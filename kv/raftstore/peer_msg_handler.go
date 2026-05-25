package raftstore

import (
	"fmt"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

// newPeerMsgHandler 把一个 peer 和共享 raftstore 上下文绑定起来。
// 这样它就能处理 Raft 消息、tick、客户端提案、split 和 snapshot 相关任务。
func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

// HandleRaftReady 是 Lab2B 中从 RawNode.Ready 接到 raftstore 的桥。
// 它应该持久化 Ready 状态、发送 Raft 消息、apply 已提交命令，
// 最后调用 RawNode.Advance。
func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	if !d.RaftGroup.HasReady() {
		return
	}

	rd := d.RaftGroup.Ready()

	applySnapResult, err := d.peerStorage.SaveReadyState(&rd)
	if err != nil {
		panic(err)
	}
	if applySnapResult != nil {
		d.applySnapshotResult(applySnapResult)
	}

	d.Send(d.ctx.trans, rd.Messages)

	for _, entry := range rd.CommittedEntries {
		d.applyEntry(entry)
	}

	d.RaftGroup.Advance(rd)
}

func (d *peerMsgHandler) applySnapshotResult(result *ApplySnapResult) {
	if result == nil {
		return
	}

	d.peerStorage.SetRegion(result.Region)

	d.ctx.storeMeta.Lock()
	defer d.ctx.storeMeta.Unlock()

	if result.PrevRegion != nil {
		d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: result.PrevRegion})
	}

	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: result.Region})
	d.ctx.storeMeta.regions[result.Region.GetId()] = result.Region
}

// applyEntry 执行一条已经被 Raft commit 的日志。
// Raft 只保证所有 peer 看到同一条日志；真正修改 KV、元信息并回调客户端是在这里发生。
func (d *peerMsgHandler) applyEntry(entry eraftpb.Entry) {
	if entry.EntryType != eraftpb.EntryType_EntryNormal {
		return
	}

	// 只有本 peer 自己 propose 的日志才会有 callback；从 leader 复制来的日志也要照常 apply。
	cb := d.findProposal(entry.Index, entry.Term)

	if len(entry.Data) == 0 {
		d.applyEmptyEntry(entry, cb)
		return
	}

	req := new(raft_cmdpb.RaftCmdRequest)
	if err := req.Unmarshal(entry.Data); err != nil {
		panic(err)
	}

	if req.GetAdminRequest() != nil {
		d.applyAdminRequest(entry, req, cb)
		return
	}

	d.applyNormalRequests(entry, req, cb)
}

// persistApplyState 记录状态机已经 apply 到哪个 Raft log index。
// 这条元信息必须和本次 KV/元信息修改一起落盘，重启后才能避免重复 apply 或漏 apply。
func (d *peerMsgHandler) persistApplyState(kvWB *engine_util.WriteBatch, index uint64) {
	d.peerStorage.applyState.AppliedIndex = index

	if err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
		panic(err)
	}
	if err := kvWB.WriteToDB(d.ctx.engine.Kv); err != nil {
		panic(err)
	}
}

// applyEmptyEntry 处理 leader noop 日志。
// 空日志不改用户数据，但仍然是 committed entry，所以必须推进 AppliedIndex。
func (d *peerMsgHandler) applyEmptyEntry(entry eraftpb.Entry, cb *message.Callback) {
	kvWB := new(engine_util.WriteBatch)
	d.persistApplyState(kvWB, entry.Index)

	if cb != nil {
		cb.Done(newCmdResp())
	}
}

// applyAdminRequest 处理 raftstore 管理命令。
// 这类命令修改 Region/Raft 元信息；当前 Lab2C 先实现 CompactLog。
func (d *peerMsgHandler) applyAdminRequest(entry eraftpb.Entry, req *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	resp := newCmdResp()
	kvWB := new(engine_util.WriteBatch)

	adminReq := req.GetAdminRequest()
	var compactIndex uint64

	switch adminReq.GetCmdType() {
	case raft_cmdpb.AdminCmdType_CompactLog:
		compactLog := adminReq.GetCompactLog()
		truncatedState := d.peerStorage.applyState.TruncatedState
		if truncatedState == nil {
			truncatedState = &rspb.RaftTruncatedState{}
			d.peerStorage.applyState.TruncatedState = truncatedState
		}

		// TruncatedState 是持久化的 compact 边界；它推进后，PeerStorage.FirstIndex/Term
		// 才会认为 compactIndex 之前的 raft log 已经不可读。
		if compactLog.GetCompactIndex() > truncatedState.GetIndex() {
			truncatedState.Index = compactLog.GetCompactIndex()
			truncatedState.Term = compactLog.GetCompactTerm()
			compactIndex = compactLog.GetCompactIndex()
		}

		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_CompactLog,
			CompactLog: &raft_cmdpb.CompactLogResponse{},
		}
	}

	d.persistApplyState(kvWB, entry.Index)

	if compactIndex > 0 {
		// 真正删除 raftdb 旧日志交给后台 worker 做，apply 线程只负责发任务。
		d.ScheduleCompactLog(compactIndex)
	}

	if cb != nil {
		cb.Done(resp)
	}
}

// applyNormalRequests 处理普通 KV 命令：Get/Put/Delete/Snap。
// 它只操作用户 KV 数据和本条日志的响应，不处理 Region/Raft 管理命令。
func (d *peerMsgHandler) applyNormalRequests(entry eraftpb.Entry, req *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	resp := newCmdResp()
	kvWB := new(engine_util.WriteBatch)

	for _, request := range req.GetRequests() {
		cmdResp := &raft_cmdpb.Response{
			CmdType: request.CmdType,
		}

		switch request.CmdType {
		case raft_cmdpb.CmdType_Get:
			get := request.GetGet()
			if err := util.CheckKeyInRegion(get.Key, d.Region()); err != nil {
				BindRespError(resp, err)
				break
			}
			value, err := engine_util.GetCF(d.ctx.engine.Kv, get.Cf, get.Key)
			if err != nil && err != badger.ErrKeyNotFound {
				BindRespError(resp, err)
				break
			}
			cmdResp.Get = &raft_cmdpb.GetResponse{Value: value}

		case raft_cmdpb.CmdType_Put:
			put := request.GetPut()
			if err := util.CheckKeyInRegion(put.Key, d.Region()); err != nil {
				BindRespError(resp, err)
				break
			}
			kvWB.SetCF(put.Cf, put.Key, put.Value)
			cmdResp.Put = &raft_cmdpb.PutResponse{}

		case raft_cmdpb.CmdType_Delete:
			del := request.GetDelete()
			if err := util.CheckKeyInRegion(del.Key, d.Region()); err != nil {
				BindRespError(resp, err)
				break
			}
			kvWB.DeleteCF(del.Cf, del.Key)
			cmdResp.Delete = &raft_cmdpb.DeleteResponse{}

		case raft_cmdpb.CmdType_Snap:
			cmdResp.Snap = &raft_cmdpb.SnapResponse{
				Region: d.Region(),
			}
			if cb != nil {
				cb.Txn = d.ctx.engine.Kv.NewTransaction(false)
			}
		}

		resp.Responses = append(resp.Responses, cmdResp)
	}

	d.persistApplyState(kvWB, entry.Index)

	if cb != nil {
		cb.Done(resp)
	}
}

func (d *peerMsgHandler) findProposal(index uint64, term uint64) *message.Callback {
	var cb *message.Callback

	for len(d.proposals) > 0 {
		p := d.proposals[0]

		if p.index == index && p.term == term {
			cb = p.cb
			d.proposals = d.proposals[1:]
			break
		}

		if p.index < index || (p.index == index && p.term != term) {
			NotifyStaleReq(term, p.cb)
			d.proposals = d.proposals[1:]
			continue
		}

		break
	}

	return cb
}

// HandleMsg 根据消息类型分发到对应的 peer 处理逻辑。
// Raft 网络消息、客户端命令、tick、split 检查、Region size 上报和 snapshot GC 都从这里进入。
func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

// preProposeRaftCommand 在请求进入 Raft 前做合法性检查。
// 它会检查 store、peer、leader、term 和 Region epoch，避免过期请求变成日志。
func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

// proposeRaftCommand 是 Lab2B 中把客户端/admin 命令提交进 Raft 的入口。
// 它会序列化已验证的命令，propose 到当前 Region 的 Raft group，
// 并保存 callback，等日志 apply 后再回调客户端。
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	data, err := msg.Marshal()
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}

	index := d.nextProposalIndex()
	term := d.Term()

	if err := d.RaftGroup.Propose(data); err != nil {
		cb.Done(ErrResp(err))
		return
	}

	d.proposals = append(d.proposals, &proposal{
		index: index,
		term:  term,
		cb:    cb,
	})
}

// onTick 推进 peer 级别的周期任务，并重新安排下一轮 tick。
func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

// startTicker 初始化周期任务，包括 Raft tick、日志 GC、split 检查和 scheduler heartbeat。
func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

// onRaftBaseTick 推进底层 RawNode 的逻辑时钟。
func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

// ScheduleCompactLog 发送异步任务，删除 apply state 已经 compact 掉的 Raft 日志。
func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

// onRaftMsg 校验外部收到的 Raft 网络消息，并把它 Step 进 RawNode。
// snapshot 消息会先做额外检查，避免过期或冲突快照进入 Raft。
func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// validateRaftMessage 检查 Raft 消息外层字段是否合法。
// 返回 false 表示消息无效，可以直接忽略。
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// checkMessage 检查消息是否发给了正确 peer，并过滤过期消息。
// 返回 true 表示消息可以静默丢弃；必要时会给过期 peer 发送 tombstone 消息。
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

// handleStaleMsg 在发送方已经过期时，按需回发 tombstone 消息让它清理自己。
func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

// handleGCPeerMsg 处理 tombstone Raft 消息。
// 如果当前 peer 的 Region 元信息已经足够旧，就销毁本地 peer。
func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// checkSnapshot 在接受快照消息前检查快照归属和 Region 重叠情况。
// 如果快照可以继续处理，返回 nil；如果应该丢弃快照文件，返回对应 snap key。
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// destroyPeer 从本地元信息中删除当前 peer，并关闭它的 router 入口。
func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

// findSiblingRegion 返回一个相邻 Region。
// 当客户端 Region epoch 过期时，可以借它刷新 split 后的 Region 信息。
func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

// onRaftGCLogTick 在已 apply 日志超过阈值时，propose 一个 CompactLog admin command。
func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

// onSplitRegionCheckTick 为过大的 leader Region 调度 split-check worker 任务。
func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

// onPrepareSplitRegion 在验证 split 请求仍然匹配当前 Region 后，向 scheduler 申请 split id。
func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

// validateSplitRegion 在请求 scheduler split 前，检查 leader 身份、split key 和 Region epoch。
func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

// onApproximateRegionSize 记录 split-check worker 上报的最新 Region 近似大小。
func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

// onSchedulerHeartbeatTick 把 leader Region 的状态上报给 scheduler。
func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

// onGCSnap 删除过期、已 compact 或已经 apply 完的 snapshot 文件。
func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

// newAdminRequest 创建 admin command 共用的请求外壳。
func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

// newCompactLogRequest 构造 CompactLog admin command。
// apply 线程后续会根据其中的 index/term compact Raft 日志。
func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
