// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrStepLocalMsg 表示外部错误地把本地消息传进了 RawNode.Step。
// MsgHup、MsgBeat 这类消息只能由本节点内部产生，不应该来自网络。
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound 表示响应消息来自未知 peer。
// 这样可以避免 Raft 处理过期成员或非法成员发来的 response。
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState 表示不需要持久化的易失状态。
// 上层主要用它观察 leader 和本节点角色是否发生变化。
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready 表示当前这一批已经准备好交给上层处理的 Raft 输出。
// 它里面可能包含需要持久化的日志、HardState、快照、待发送消息和已提交日志。
// 正常处理顺序是：先持久化 HardState/Entries/Snapshot，再发送 Messages，
// 再 apply CommittedEntries，最后调用 Advance 推进 RawNode 内部进度。
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

// RawNode 是对底层 Raft 状态机的封装。
// 它是 Raft 和上层存储/网络层之间的边界：上层通过 RawNode 驱动 Raft，
// 再通过 Ready/Advance 生命周期消费 Raft 的输出。
type RawNode struct {
	Raft *Raft
	// Your Data Here (2A).
	prevSoftSt *SoftState
	prevHardSt pb.HardState
}

func (rn *RawNode) softState() *SoftState {
	return &SoftState{
		Lead:      rn.Raft.Lead,
		RaftState: rn.Raft.State,
	}
}

func (rn *RawNode) hardState() pb.HardState {
	return pb.HardState{
		Term:   rn.Raft.Term,
		Vote:   rn.Raft.Vote,
		Commit: rn.Raft.RaftLog.committed,
	}
}

func isSoftStateEqual(a, b *SoftState) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}

// NewRawNode 根据配置创建 RawNode。
// 它应该初始化底层 Raft，并记录上一轮 SoftState/HardState，
// 这样 HasReady 后续才能判断状态是否发生变化。
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	rn := &RawNode{
		Raft: newRaft(config),
	}
	rn.prevSoftSt = rn.softState()
	rn.prevHardSt = rn.hardState()
	return rn, nil
}

// Tick 推进一次 Raft 逻辑时钟。
// 上层会定期调用它，Raft 用它触发选举超时和 leader 心跳。
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign 让当前节点主动发起选举。
// 它本质上是向 Raft 注入本地 MsgHup，也就是“开始选举”的信号。
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose 提议把一段业务数据追加进 Raft 日志。
// 这段数据会被包装成普通日志 entry，并通过 MsgPropose 交给 leader 复制。
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange 提议一次成员变更。
// 这里只是提出一条特殊的配置变更日志，真正修改成员要等日志提交后调用 ApplyConfChange。
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange 在配置变更日志提交后真正修改本地 peer 集合。
// 它会返回新的 ConfState，后续快照需要持久化这个配置状态。
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step 把一条外部 Raft 消息推进到底层状态机。
// 网络消息会从这里进入 Raft；本地消息会被拒绝，response 也必须来自已知 peer。
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready 返回当前时刻需要上层处理的一批 Raft 输出。
// 它会收集 unstable entries、committed entries、待发送消息、快照以及 soft/hard state 变化。
func (rn *RawNode) Ready() Ready {
	// Your Code Here (2A).

	rd := Ready{}

	softState := rn.softState()
	if !isSoftStateEqual(softState, rn.prevSoftSt) {
		rd.SoftState = softState
	}

	hardState := rn.hardState()
	if !isHardStateEqual(hardState, rn.prevHardSt) {
		rd.HardState = hardState
	}

	rd.Entries = rn.Raft.RaftLog.unstableEntries()
	rd.CommittedEntries = rn.Raft.RaftLog.nextEnts()
	if len(rn.Raft.msgs) > 0 {
		rd.Messages = rn.Raft.msgs
	}

	if rn.Raft.RaftLog.pendingSnapshot != nil && !IsEmptySnap(rn.Raft.RaftLog.pendingSnapshot) {
		rd.Snapshot = *rn.Raft.RaftLog.pendingSnapshot
	}

	return rd
}

// HasReady 用来快速判断当前是否存在待处理的 Ready。
// 如果有状态变化、未持久化日志、快照、已提交未应用日志或待发送消息，就应该返回 true。
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	if !isSoftStateEqual(rn.softState(), rn.prevSoftSt) {
		return true
	}

	if !isHardStateEqual(rn.hardState(), rn.prevHardSt) {
		return true
	}

	if len(rn.Raft.RaftLog.unstableEntries()) > 0 {
		return true
	}

	if len(rn.Raft.RaftLog.nextEnts()) > 0 {
		return true
	}

	if rn.Raft.RaftLog.pendingSnapshot != nil && !IsEmptySnap(rn.Raft.RaftLog.pendingSnapshot) {
		return true
	}

	return len(rn.Raft.msgs) > 0
}

// Advance 通知 RawNode：上一批 Ready 已经被上层处理完。
// 它会和 Ready 配套使用，在上层持久化日志、apply committed entries 后，
// 推进 stabled/applied 等内部进度，为下一批 Ready 做准备。
func (rn *RawNode) Advance(rd Ready) {
	// Your Code Here (2A).
	if len(rd.Entries) > 0 {
		lastEntry := rd.Entries[len(rd.Entries)-1]
		rn.Raft.RaftLog.stabled = lastEntry.Index
	}

	if len(rd.CommittedEntries) > 0 {
		lastEntry := rd.CommittedEntries[len(rd.CommittedEntries)-1]
		rn.Raft.RaftLog.applied = lastEntry.Index
	}

	rn.Raft.msgs = nil
	rn.prevSoftSt = rn.softState()
	rn.prevHardSt = rn.hardState()
}

// GetProgress 在当前节点是 leader 时返回所有 peer 的复制进度。
// follower 不维护权威 Progress，所以会返回空 map。
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader 尝试把 leader 身份转移给指定节点。
// 这只是发起转移请求，是否成功取决于目标节点日志是否追上等 Raft 状态。
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
