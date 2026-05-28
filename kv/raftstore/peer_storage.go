package raftstore

import (
	"bytes"
	"fmt"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap/errors"
)

type ApplySnapResult struct {
	// PrevRegion 是应用快照前的旧 Region。
	PrevRegion *metapb.Region
	Region     *metapb.Region
}

var _ raft.Storage = new(PeerStorage)

type PeerStorage struct {
	// 当前 peer 所属 Region 的元信息。
	region *metapb.Region
	// 当前 peer 的 Raft 本地状态。
	raftState *rspb.RaftLocalState
	// 当前 peer 的 apply 状态。
	applyState *rspb.RaftApplyState

	// 当前快照生成状态。
	snapState snap.SnapState
	// regionSched 用来向 region worker 投递任务。
	regionSched chan<- worker.Task
	// 生成快照的重试次数。
	snapTriedCnt int
	// Engines 包含 raftdb 和 kvdb 两个 Badger 实例。
	Engines *engine_util.Engines
	// Tag 用于日志打印。
	Tag string
}

// NewPeerStorage 从 engines 中读取持久化 raftState，并创建 PeerStorage。
// Lab2B 会把 PeerStorage 作为某个 Region peer 背后的持久化 Storage 实现。
func NewPeerStorage(engines *engine_util.Engines, region *metapb.Region, regionSched chan<- worker.Task, tag string) (*PeerStorage, error) {
	log.Debugf("%s creating storage for %s", tag, region.String())
	raftState, err := meta.InitRaftLocalState(engines.Raft, region)
	if err != nil {
		return nil, err
	}
	applyState, err := meta.InitApplyState(engines.Kv, region)
	if err != nil {
		return nil, err
	}
	if raftState.LastIndex < applyState.AppliedIndex {
		panic(fmt.Sprintf("%s unexpected raft log index: lastIndex %d < appliedIndex %d",
			tag, raftState.LastIndex, applyState.AppliedIndex))
	}
	return &PeerStorage{
		Engines:     engines,
		region:      region,
		Tag:         tag,
		raftState:   raftState,
		applyState:  applyState,
		regionSched: regionSched,
	}, nil
}

// InitialState 为 raft.NewRawNode 返回已持久化的 HardState 和 ConfState。
// peer 启动或重启时会调用它恢复 Raft 状态。
func (ps *PeerStorage) InitialState() (eraftpb.HardState, eraftpb.ConfState, error) {
	raftState := ps.raftState
	if raft.IsEmptyHardState(*raftState.HardState) {
		y.AssertTruef(!ps.isInitialized(),
			"peer for region %s is initialized but local state %+v has empty hard state",
			ps.region, ps.raftState)
		return eraftpb.HardState{}, eraftpb.ConfState{}, nil
	}
	return *raftState.HardState, util.ConfStateFromRegion(ps.region), nil
}

// Entries 从 raftdb 读取 [low, high) 范围内的已持久化 Raft 日志。
// 当 Raft 需要读取不只在内存里的稳定日志时会调用它。
func (ps *PeerStorage) Entries(low, high uint64) ([]eraftpb.Entry, error) {
	if err := ps.checkRange(low, high); err != nil || low == high {
		return nil, err
	}
	buf := make([]eraftpb.Entry, 0, high-low)
	nextIndex := low
	txn := ps.Engines.Raft.NewTransaction(false)
	defer txn.Discard()
	startKey := meta.RaftLogKey(ps.region.Id, low)
	endKey := meta.RaftLogKey(ps.region.Id, high)
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		if bytes.Compare(item.Key(), endKey) >= 0 {
			break
		}
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		var entry eraftpb.Entry
		if err = entry.Unmarshal(val); err != nil {
			return nil, err
		}
		// May meet gap or has been compacted.
		if entry.Index != nextIndex {
			break
		}
		nextIndex++
		buf = append(buf, entry)
	}
	// If we get the correct number of entries, returns.
	if len(buf) == int(high-low) {
		return buf, nil
	}
	// Here means we don't fetch enough entries.
	return nil, raft.ErrUnavailable
}

// Term 返回指定日志 index 对应的 term。
// compact 边界使用 truncated state，普通日志则从 raftdb 读取。
func (ps *PeerStorage) Term(idx uint64) (uint64, error) {
	if idx == ps.truncatedIndex() {
		return ps.truncatedTerm(), nil
	}
	if err := ps.checkRange(idx, idx+1); err != nil {
		return 0, err
	}
	if ps.truncatedTerm() == ps.raftState.LastTerm || idx == ps.raftState.LastIndex {
		return ps.raftState.LastTerm, nil
	}
	var entry eraftpb.Entry
	if err := engine_util.GetMeta(ps.Engines.Raft, meta.RaftLogKey(ps.region.Id, idx), &entry); err != nil {
		return 0, err
	}
	return entry.Term, nil
}

// LastIndex 返回该 peer 当前最高的已持久化 Raft 日志 index。
func (ps *PeerStorage) LastIndex() (uint64, error) {
	return ps.raftState.LastIndex, nil
}

// FirstIndex 返回 compact 之后仍然可读取的第一条日志 index。
func (ps *PeerStorage) FirstIndex() (uint64, error) {
	return ps.truncatedIndex() + 1, nil
}

// Snapshot 返回当前 Region 快照，或者触发一次快照生成。
// Lab2C 中 follower 落后太多、无法靠普通日志追上时会用到它。
func (ps *PeerStorage) Snapshot() (eraftpb.Snapshot, error) {
	var snapshot eraftpb.Snapshot
	if ps.snapState.StateType == snap.SnapState_Generating {
		select {
		case s := <-ps.snapState.Receiver:
			if s != nil {
				snapshot = *s
			}
		default:
			return snapshot, raft.ErrSnapshotTemporarilyUnavailable
		}
		ps.snapState.StateType = snap.SnapState_Relax
		if snapshot.GetMetadata() != nil {
			ps.snapTriedCnt = 0
			if ps.validateSnap(&snapshot) {
				return snapshot, nil
			}
		} else {
			log.Warnf("%s failed to try generating snapshot, times: %d", ps.Tag, ps.snapTriedCnt)
		}
	}

	if ps.snapTriedCnt >= 5 {
		err := errors.Errorf("failed to get snapshot after %d times", ps.snapTriedCnt)
		ps.snapTriedCnt = 0
		return snapshot, err
	}

	log.Infof("%s requesting snapshot", ps.Tag)
	ps.snapTriedCnt++
	ch := make(chan *eraftpb.Snapshot, 1)
	ps.snapState = snap.SnapState{
		StateType: snap.SnapState_Generating,
		Receiver:  ch,
	}
	// schedule snapshot generate task
	ps.regionSched <- &runner.RegionTaskGen{
		RegionId: ps.region.GetId(),
		Notifier: ch,
	}
	return snapshot, raft.ErrSnapshotTemporarilyUnavailable
}

// isInitialized 判断当前 peer 是否已经有 Region peer 元信息。
func (ps *PeerStorage) isInitialized() bool {
	return len(ps.region.Peers) > 0
}

// Region 返回当前 peer 的 Region 元信息。
func (ps *PeerStorage) Region() *metapb.Region {
	return ps.region
}

// SetRegion 在 split 或 conf change 后更新当前 peer 的 Region 元信息。
func (ps *PeerStorage) SetRegion(region *metapb.Region) {
	ps.region = region
}

// checkRange 检查请求的日志范围是否可读，并且没有被 compact 掉。
func (ps *PeerStorage) checkRange(low, high uint64) error {
	if low > high {
		return errors.Errorf("low %d is greater than high %d", low, high)
	} else if low <= ps.truncatedIndex() {
		return raft.ErrCompacted
	} else if high > ps.raftState.LastIndex+1 {
		return errors.Errorf("entries' high %d is out of bound, lastIndex %d",
			high, ps.raftState.LastIndex)
	}
	return nil
}

// truncatedIndex 返回已经 compact 的日志边界 index。
func (ps *PeerStorage) truncatedIndex() uint64 {
	return ps.applyState.TruncatedState.Index
}

// truncatedTerm 返回 compact 边界 index 对应的 term。
func (ps *PeerStorage) truncatedTerm() uint64 {
	return ps.applyState.TruncatedState.Term
}

// AppliedIndex 返回已经 apply 到状态机的最高日志 index。
func (ps *PeerStorage) AppliedIndex() uint64 {
	return ps.applyState.AppliedIndex
}

// validateSnap 检查生成出来的快照对当前 Region 是否仍然可用。
func (ps *PeerStorage) validateSnap(snap *eraftpb.Snapshot) bool {
	idx := snap.GetMetadata().GetIndex()
	if idx < ps.truncatedIndex() {
		log.Infof("%s snapshot is stale, generate again, snapIndex: %d, truncatedIndex: %d", ps.Tag, idx, ps.truncatedIndex())
		return false
	}
	var snapData rspb.RaftSnapshotData
	if err := proto.UnmarshalMerge(snap.GetData(), &snapData); err != nil {
		log.Errorf("%s failed to decode snapshot, it may be corrupted, err: %v", ps.Tag, err)
		return false
	}
	snapEpoch := snapData.GetRegion().GetRegionEpoch()
	latestEpoch := ps.region.GetRegionEpoch()
	if snapEpoch.GetConfVer() < latestEpoch.GetConfVer() {
		log.Infof("%s snapshot epoch is stale, snapEpoch: %s, latestEpoch: %s", ps.Tag, snapEpoch, latestEpoch)
		return false
	}
	return true
}

// clearMeta 删除当前 peer 的所有持久化元信息。
func (ps *PeerStorage) clearMeta(kvWB, raftWB *engine_util.WriteBatch) error {
	return ClearMeta(ps.Engines, kvWB, raftWB, ps.region.Id, ps.raftState.LastIndex)
}

// clearExtraData 删除旧 Region 中不再属于 newRegion 的数据范围。
func (ps *PeerStorage) clearExtraData(newRegion *metapb.Region) {
	oldStartKey, oldEndKey := ps.region.GetStartKey(), ps.region.GetEndKey()
	newStartKey, newEndKey := newRegion.GetStartKey(), newRegion.GetEndKey()
	if bytes.Compare(oldStartKey, newStartKey) < 0 {
		ps.clearRange(newRegion.Id, oldStartKey, newStartKey)
	}
	if bytes.Compare(newEndKey, oldEndKey) < 0 || (len(oldEndKey) == 0 && len(newEndKey) != 0) {
		ps.clearRange(newRegion.Id, newEndKey, oldEndKey)
	}
}

// ClearMeta 删除过期元信息，比如 raftState、applyState、regionState 和 raft log entries。
func ClearMeta(engines *engine_util.Engines, kvWB, raftWB *engine_util.WriteBatch, regionID uint64, lastIndex uint64) error {
	start := time.Now()
	kvWB.DeleteMeta(meta.RegionStateKey(regionID))
	kvWB.DeleteMeta(meta.ApplyStateKey(regionID))

	firstIndex := lastIndex + 1
	beginLogKey := meta.RaftLogKey(regionID, 0)
	endLogKey := meta.RaftLogKey(regionID, firstIndex)
	err := engines.Raft.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(beginLogKey)
		if it.Valid() && bytes.Compare(it.Item().Key(), endLogKey) < 0 {
			logIdx, err1 := meta.RaftLogIndex(it.Item().Key())
			if err1 != nil {
				return err1
			}
			firstIndex = logIdx
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := firstIndex; i <= lastIndex; i++ {
		raftWB.DeleteMeta(meta.RaftLogKey(regionID, i))
	}
	raftWB.DeleteMeta(meta.RaftStateKey(regionID))
	log.Infof(
		"[region %d] clear peer 1 meta key 1 apply key 1 raft key and %d raft logs, takes %v",
		regionID,
		lastIndex+1-firstIndex,
		time.Since(start),
	)
	return nil
}

// Append 把给定 entries 写入 raft log，并更新 ps.raftState。
// Lab2B 会在这里持久化 Ready.Entries，并删除被 Raft 覆盖的旧冲突后缀。
func (ps *PeerStorage) Append(entries []eraftpb.Entry, raftWB *engine_util.WriteBatch) error {
	if len(entries) == 0 {
		return nil
	}

	firstIndex := entries[0].Index
	lastIndex := entries[len(entries)-1].Index

	if lastIndex <= ps.truncatedIndex() {
		return nil
	}

	if firstIndex <= ps.truncatedIndex() {
		entries = entries[ps.truncatedIndex()+1-firstIndex:]
	}

	oldLastIndex := ps.raftState.LastIndex
	lastEntry := entries[len(entries)-1]

	for _, entry := range entries {
		ent := entry
		if err := raftWB.SetMeta(meta.RaftLogKey(ps.region.GetId(), ent.Index), &ent); err != nil {
			return err
		}
	}

	ps.raftState.LastIndex = lastEntry.Index
	ps.raftState.LastTerm = lastEntry.Term

	for index := lastEntry.Index + 1; index <= oldLastIndex; index++ {
		raftWB.DeleteMeta(meta.RaftLogKey(ps.region.GetId(), index))
	}

	return nil
}

// ApplySnapshot 把给定快照应用到当前 peer。
// Lab2C 会在这里安装快照元信息、调度 KV 快照应用任务，并清理新 Region 外的旧数据。
func (ps *PeerStorage) ApplySnapshot(snapshot *eraftpb.Snapshot, kvWB *engine_util.WriteBatch, raftWB *engine_util.WriteBatch) (*ApplySnapResult, error) {
	log.Infof("%v begin to apply snapshot", ps.Tag)
	snapData := new(rspb.RaftSnapshotData)
	if err := snapData.Unmarshal(snapshot.Data); err != nil {
		return nil, err
	}

	// Hint: things need to do here including: update peer storage state like raftState and applyState, etc,
	// and send RegionTaskApply task to region worker through ps.regionSched, also remember call ps.clearMeta
	// and ps.clearExtraData to delete stale data
	// Your Code Here (2C).
	snapMeta := snapshot.GetMetadata()
	if snapMeta == nil {
		return nil, errors.New("snapshot metadata is nil")
	}

	prevRegion := ps.region
	prevInitialized := ps.isInitialized()
	newRegion := snapData.GetRegion()
	if newRegion == nil {
		return nil, errors.New("snapshot region is nil")
	}

	// 先清掉旧 region 的 raft log / apply state / region local state。
	// snapshot 会成为新的日志起点，旧日志不能再保留。
	if err := ps.clearMeta(kvWB, raftWB); err != nil {
		return nil, err
	}

	// replicated peer 初始只有 regionID，没有可信的 key range。
	// 只有旧 Region 已初始化时，才能用旧范围去清理不再属于新 Region 的数据。
	// 如果旧范围是占位的空范围 [, )，这里清理会误删本 store 上其他 Region 的数据。
	if prevInitialized {
		ps.clearExtraData(newRegion)
	}

	// RegionTaskApply 会真正把 snapshot 文件 ingest 到 kv engine。
	// 这里同步等待完成，保证后面写入的 applyState/raftState 不会领先于实际数据。
	// apply worker 只需要清理并导入 snapshot 覆盖的新 Region 范围。
	notifier := make(chan bool, 1)
	ps.regionSched <- &runner.RegionTaskApply{
		RegionId: newRegion.GetId(),
		Notifier: notifier,
		SnapMeta: snapMeta,
		StartKey: newRegion.GetStartKey(),
		EndKey:   newRegion.GetEndKey(),
	}

	if ok := <-notifier; !ok {
		return nil, errors.Errorf("apply snapshot failed for region %d", newRegion.GetId())
	}

	hardState := ps.raftState.HardState
	if hardState == nil {
		hardState = &eraftpb.HardState{}
	}
	if hardState.Commit < snapMeta.Index {
		hardState.Commit = snapMeta.Index
	}

	// 安装 snapshot 后，本地日志的可读起点变成 snapshot index。
	// TruncatedState 同步推进，后续 PeerStorage.FirstIndex/Term 会以它为 compact 边界。
	ps.region = newRegion
	ps.raftState = &rspb.RaftLocalState{
		HardState: hardState,
		LastIndex: snapMeta.Index,
		LastTerm:  snapMeta.Term,
	}
	ps.applyState = &rspb.RaftApplyState{
		AppliedIndex: snapMeta.Index,
		TruncatedState: &rspb.RaftTruncatedState{
			Index: snapMeta.Index,
			Term:  snapMeta.Term,
		},
	}

	meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
	if err := kvWB.SetMeta(meta.ApplyStateKey(newRegion.GetId()), ps.applyState); err != nil {
		return nil, err
	}

	log.Infof("%v apply snapshot finished, region %d, index %d, term %d",
		ps.Tag, newRegion.GetId(), snapMeta.Index, snapMeta.Term)

	return &ApplySnapResult{
		PrevRegion: prevRegion,
		Region:     newRegion,
	}, nil
}

// SaveReadyState 把 Ready 中的内存状态保存到磁盘。
// 注意：这里不能修改 ready 本身，否则后续 Advance 会拿不到正确的 Ready 信息。
// Lab2B/2C 会在这里先保存 HardState、entries 和 snapshot，再允许发送消息。
func (ps *PeerStorage) SaveReadyState(ready *raft.Ready) (*ApplySnapResult, error) {
	// Hint: you may call `Append()` and `ApplySnapshot()` in this function
	kvWB := new(engine_util.WriteBatch)
	raftWB := new(engine_util.WriteBatch)

	var applySnapResult *ApplySnapResult
	if !raft.IsEmptySnap(&ready.Snapshot) {
		// snapshot 必须先于新 entries 保存：它会重置本地 raft/apply 状态和日志边界。
		result, err := ps.ApplySnapshot(&ready.Snapshot, kvWB, raftWB)
		if err != nil {
			return nil, err
		}
		applySnapResult = result
	}

	if err := ps.Append(ready.Entries, raftWB); err != nil {
		return nil, err
	}

	if !raft.IsEmptyHardState(ready.HardState) {
		hardState := ready.HardState
		ps.raftState.HardState = &hardState
	}

	if err := raftWB.SetMeta(meta.RaftStateKey(ps.region.GetId()), ps.raftState); err != nil {
		return nil, err
	}

	if err := kvWB.WriteToDB(ps.Engines.Kv); err != nil {
		return nil, err
	}
	if err := raftWB.WriteToDB(ps.Engines.Raft); err != nil {
		return nil, err
	}

	return applySnapResult, nil
}

// ClearData 删除当前 peer 对应 Region 的 KV 数据范围。
func (ps *PeerStorage) ClearData() {
	ps.clearRange(ps.region.GetId(), ps.region.GetStartKey(), ps.region.GetEndKey())
}

// clearRange 向 region worker 发送异步销毁任务，删除指定 Region key range。
func (ps *PeerStorage) clearRange(regionID uint64, start, end []byte) {
	ps.regionSched <- &runner.RegionTaskDestroy{
		RegionId: regionID,
		StartKey: start,
		EndKey:   end,
	}
}
