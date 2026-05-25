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
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None 表示当前没有 leader 或没有投票对象。
const None uint64 = 0

// StateType 表示一个 Raft 节点在集群里的角色。
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

// String 把 Raft 角色转成可读字符串，方便测试和调试。
func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped 表示提案被当前节点丢弃。
// 比如 follower 收到本该交给 leader 的 propose 时，可以用它快速返回失败。
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config 包含启动一个 Raft 节点需要的配置。
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

// validate 在 newRaft 初始化状态机前检查配置是否合法。
// 无效的 timeout 或 storage 配置会导致 Raft 无法正常选主或安全持久化日志。
func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress 表示 leader 视角下某个 follower 的日志复制进度。
// leader 会根据 Match/Next 决定给 follower 从哪里继续发送日志。
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int

	randomizedElectionTimeout int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft 根据配置创建一个 Raft 节点。
// 它会从 storage 恢复 HardState/ConfState，初始化 RaftLog 和 peer Progress，
// 并让节点以 follower 身份准备参与选举。
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raftLog := newLog(c.Storage)

	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	peers := c.peers
	if len(peers) == 0 {
		peers = confState.Nodes
	}
	r := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          raftLog,
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
	}
	r.resetRandomizedElectionTimeout()

	if hardState.Commit > raftLog.committed {
		raftLog.committed = hardState.Commit
	}

	if c.Applied > raftLog.applied {
		raftLog.applied = c.Applied
	}
	lastIndex := raftLog.LastIndex()
	for _, peer := range peers {
		r.Prs[peer] = &Progress{
			Match: 0,
			Next:  lastIndex + 1,
		}
	}

	return r
}

// sendAppend 给指定 peer 发送 AppendEntries 消息。
// leader 用它做正常日志复制，也用它重试那些日志落后或发生冲突的 follower。
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	r.RaftLog.maybeCompact()

	pr, ok := r.Prs[to]
	if !ok {
		return false
	}

	prevIndex := pr.Next - 1
	prevTerm, err := r.RaftLog.Term(prevIndex)
	if err != nil {
		if err == ErrCompacted {
			// follower 需要的 prev log 已经被 leader compact 掉，
			// 普通 AppendEntries 无法继续对齐，只能改发 snapshot。
			snapshot, snapErr := r.RaftLog.storage.Snapshot()
			if snapErr == ErrSnapshotTemporarilyUnavailable {
				return false
			}
			if snapErr != nil {
				panic(snapErr)
			}

			r.msgs = append(r.msgs, pb.Message{
				MsgType:  pb.MessageType_MsgSnapshot,
				From:     r.id,
				To:       to,
				Term:     r.Term,
				Snapshot: &snapshot,
			})
			return true
		}

		return false
	}

	var entries []*pb.Entry
	if pr.Next <= r.RaftLog.LastIndex() {
		offset := r.RaftLog.entries[0].Index
		for i := pr.Next - offset; i < uint64(len(r.RaftLog.entries)); i++ {
			entries = append(entries, &r.RaftLog.entries[i])
		}
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   prevIndex,
		LogTerm: prevTerm,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	})

	return true
}

// sendHeartbeat 给指定 peer 发送心跳消息。
// leader 用心跳维持自己的权威，并告诉 follower 当前最新的 committed index。
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	})
}

// tick 推进一次 Raft 内部逻辑时钟。
// 上层定期调用它；follower/candidate 用它触发选举，leader 用它触发心跳。
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			_ = r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	default:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			_ = r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	}
}

// resetRandomizedElectionTimeout 重新生成随机选举超时时间。
// 随机化可以减少多个 follower 同时发起选举、反复平票的概率。
func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// isUpToDate 判断 candidate 的日志是否至少和本地一样新。
// 投票时用它避免选出缺少已提交历史的 leader。
func (r *Raft) isUpToDate(index uint64, term uint64) bool {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		panic(err)
	}

	if term != lastTerm {
		return term > lastTerm
	}
	return index >= lastIndex
}

// quorum 返回达成多数派需要的票数或副本数。
// Raft 选主和提交日志都依赖多数派。
func (r *Raft) quorum() int {
	return len(r.Prs)/2 + 1
}

// becomeFollower 把当前节点切换成 follower。
// 它会记录 leader、更新 term、清空投票状态，并重置计时器。
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if term != r.Term {
		r.Vote = None
	}
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.votes = make(map[uint64]bool)
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()
}

// becomeCandidate 把当前节点切换成 candidate。
// 它会开启新 term、投自己一票、清空 leader，并准备统计投票响应。
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Lead = None
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()
}

// becomeLeader 把当前节点切换成 leader。
// 它会初始化每个 peer 的复制进度，并在当前 term 追加一条 no-op 日志，
// 这是 Raft 用来确立新 leader 权威的标准做法。
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.heartbeatElapsed = 0
	r.electionElapsed = 0

	lastIndex := r.RaftLog.LastIndex()
	for id := range r.Prs {
		r.Prs[id] = &Progress{
			Match: 0,
			Next:  lastIndex + 1,
		}
	}
	// leader 上任后追加一条 no-op，用来提交当前任期。
	entry := pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     lastIndex + 1,
	}
	r.RaftLog.entries = append(r.RaftLog.entries, entry)
	r.Prs[r.id].Match = entry.Index
	r.Prs[r.id].Next = entry.Index + 1
	r.maybeCommit()

}

// maybeCommit 尝试推进 leader 的 committed index。
// 只有当前 term 的日志被多数派复制后，leader 才能直接提交它；
// 一旦当前 term 日志提交，前面的旧 term 日志也会一起变成 committed。
func (r *Raft) maybeCommit() bool {
	oldCommitted := r.RaftLog.committed

	for index := r.RaftLog.LastIndex(); index > r.RaftLog.committed; index-- {
		term, err := r.RaftLog.Term(index)
		if err != nil {
			continue
		}

		// Raft 规定：leader 只能通过多数派直接提交当前 term 的日志。
		// 一旦当前 term 的某条日志提交，它前面的旧 term 日志也自然一起提交。
		if term != r.Term {
			continue
		}

		count := 0
		for _, pr := range r.Prs {
			if pr.Match >= index {
				count++
			}
		}

		if count >= r.quorum() {
			r.RaftLog.committed = index
			break
		}
	}

	return r.RaftLog.committed != oldCommitted
}

// Step 是 Raft 处理消息的统一入口。
// 本地触发和网络消息最终都会进入这里，并可能改变 term、角色、日志、
// Progress、committed index 或待发送消息。
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	switch m.MsgType {
	// MsgBeat 是 leader 本地的定时消息。
	// 它不是网络消息，作用是提醒 leader 给所有 follower 发心跳。
	case pb.MessageType_MsgBeat:
		if r.State == StateLeader {
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendHeartbeat(id)
			}
		}
		return nil

	// MsgHup 是本地选举触发消息。
	// follower/candidate 选举超时后，会用它让自己发起新一轮选举。
	case pb.MessageType_MsgHup:
		if r.State == StateLeader {
			return nil
		}

		r.becomeCandidate()

		lastIndex := r.RaftLog.LastIndex()
		lastTerm, err := r.RaftLog.Term(lastIndex)
		if err != nil {
			return err
		}

		if len(r.Prs) == 1 {
			r.becomeLeader()
			return nil
		}

		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				From:    r.id,
				To:      id,
				Term:    r.Term,
				Index:   lastIndex,
				LogTerm: lastTerm,
			})
		}
		return nil
	// MsgRequestVote 是 candidate 发给其他节点的投票请求。
	// 接收方要判断：任期是否够新、自己是否还能投票、candidate 日志是否足够新。
	case pb.MessageType_MsgRequestVote:
		granted := false
		if m.Term >= r.Term {
			canVote := r.Vote == None || r.Vote == m.From
			granted = canVote && r.isUpToDate(m.Index, m.LogTerm)
		}
		resp := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  !granted,
		}

		if granted {
			r.Vote = m.From
			r.electionElapsed = 0
			r.resetRandomizedElectionTimeout()
		}

		r.msgs = append(r.msgs, resp)
		return nil
	// MsgRequestVoteResponse 是其他节点对 candidate 的投票响应。
	// candidate 收到多数同意后成为 leader；收到多数拒绝后退回 follower。
	case pb.MessageType_MsgRequestVoteResponse:
		if r.State != StateCandidate {
			return nil
		}

		r.votes[m.From] = !m.Reject

		granted := 0
		rejected := 0
		for _, vote := range r.votes {
			if vote {
				granted++
			} else {
				rejected++
			}
		}

		if granted >= r.quorum() {
			r.becomeLeader()
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendAppend(id)
			}
		} else if rejected >= r.quorum() {
			r.becomeFollower(r.Term, None)
		}
		return nil
	// MsgHeartbeat 是 leader 发给 follower 的心跳。
	// follower 用它确认当前 leader，并同步 leader 已经提交到哪里的 commit index。
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
		return nil
	// MsgAppend 是 leader 发给 follower 的日志复制请求。
	// 里面可能带新日志，也可能只是用 prevLogIndex/prevLogTerm 做一致性检查。
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
		return nil
	// MsgPropose 是上层应用提交给 leader 的新日志提案。
	// leader 负责给 entry 填 Term/Index，追加到本地日志，再发 MsgAppend 给 followers。
	case pb.MessageType_MsgPropose:
		if r.State != StateLeader {
			return ErrProposalDropped
		}

		if len(m.Entries) == 0 {
			return nil
		}

		lastIndex := r.RaftLog.LastIndex()

		for _, e := range m.Entries {
			lastIndex++

			ent := *e
			ent.Term = r.Term
			ent.Index = lastIndex

			r.RaftLog.entries = append(r.RaftLog.entries, ent)
		}

		r.Prs[r.id].Match = lastIndex
		r.Prs[r.id].Next = lastIndex + 1

		if len(r.Prs) == 1 {
			r.RaftLog.committed = lastIndex
			return nil
		}

		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.sendAppend(id)
		}

		return nil
	// MsgAppendResponse 是 follower 对 MsgAppend 的响应。
	// leader 用它更新 follower 的复制进度，并尝试推进 committed。
	case pb.MessageType_MsgAppendResponse:
		if r.State != StateLeader {
			return nil
		}

		pr, ok := r.Prs[m.From]
		if !ok {
			return nil
		}

		if m.Reject {
			if pr.Next > 1 {
				pr.Next--
			}
			r.sendAppend(m.From)
			return nil
		}

		if m.Index > pr.Match {
			pr.Match = m.Index
		}
		pr.Next = pr.Match + 1

		if r.maybeCommit() {
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendAppend(id)
			}
		}

		return nil
	// MsgHeartbeatResponse 是 follower 对 heartbeat 的响应。
	// leader 收到后尝试发 MsgAppend，用来给落后的 follower 补日志。
	case pb.MessageType_MsgHeartbeatResponse:
		if r.State != StateLeader {
			return nil
		}
		r.sendAppend(m.From)
		return nil
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
		return nil

	}

	// 下面这些消息后续阶段会补：
	// MsgSnapshot：leader 发给落后 follower 的快照安装请求。（2C）
	// MsgTransferLeader / MsgTimeoutNow：leader transfer 相关消息。（3A）
	switch r.State {
	case StateFollower:
	case StateCandidate:
	case StateLeader:
	}
	return nil
}

// handleAppendEntries 处理 leader 发来的 AppendEntries 请求。
// follower 会在这里校验日志前缀、追加新日志、更新 committed index，
// 并向 leader 回复成功或拒绝。
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Index:   r.RaftLog.LastIndex(),
			Reject:  true,
		})
		return
	}

	r.becomeFollower(m.Term, m.From)

	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Index:   m.Index,
			Reject:  true,
		})
		return
	}

	lastNewIndex := m.Index + uint64(len(m.Entries))
	r.RaftLog.appendEntries(m.Entries)

	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, lastNewIndex)
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Index:   lastNewIndex,
	})
}

// handleHeartbeat 处理 leader 发来的心跳请求。
// follower 会在这里确认当前 term 的 leader，并回复心跳响应。
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}

	r.becomeFollower(m.Term, m.From)

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	})
}

// handleSnapshot 处理 leader 发来的快照安装请求。
// Lab2C 会在 follower 落后太多、无法靠普通日志追上时实现它。
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	if m.Snapshot == nil || IsEmptySnap(m.Snapshot) {
		return
	}
	if m.Term < r.Term {
		return
	}

	meta := m.Snapshot.Metadata
	r.becomeFollower(m.Term, m.From)

	if meta.Index <= r.RaftLog.committed {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Index:   r.RaftLog.committed,
		})
		return
	}

	r.RaftLog.pendingSnapshot = m.Snapshot
	r.RaftLog.committed = meta.Index
	r.RaftLog.applied = meta.Index
	r.RaftLog.stabled = meta.Index
	// 内存日志只保留 snapshot index 处的 dummy entry。
	// 真正的状态机数据会在上层 raftstore 处理 Ready.Snapshot 时应用。
	r.RaftLog.entries = []pb.Entry{
		{
			Index: meta.Index,
			Term:  meta.Term,
		},
	}

	r.Prs = make(map[uint64]*Progress)
	for _, id := range meta.ConfState.Nodes {
		// follower 安装 snapshot 后没有 leader 侧复制进度的权威信息。
		// 这里先用 snapshot 后的下一条日志作为后续复制起点。
		r.Prs[id] = &Progress{
			Match: 0,
			Next:  meta.Index + 1,
		}
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Index:   meta.Index,
	})
}

// addNode 把一个新节点加入 Raft group。
// Lab3A 会在配置变更日志提交后实现它，用来开始跟踪新 peer 的复制进度。
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode 从 Raft group 中移除一个节点。
// Lab3A 会在配置变更日志提交后实现它，用来停止跟踪该 peer，
// 并在必要时重新计算 commit 进度。
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
