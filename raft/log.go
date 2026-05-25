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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog 管理 Raft 日志以及 committed/applied/stabled 这些关键游标。
// 它的大致结构如下：
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// 为了简化实现，RaftLog 会在内存里管理所有还没有被 compact 的日志。
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog 根据持久化 storage 初始化 RaftLog。
// 它会把已有日志加载到内存，并在 FirstIndex-1 放一个 dummy entry，
// 这样后续从 raft index 映射到 entries 切片下标会更简单。
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	dummyTerm, err := storage.Term(firstIndex - 1)
	if err != nil {
		panic(err)
	}
	entries := []pb.Entry{
		{
			Index: firstIndex - 1,
			Term:  dummyTerm,
		},
	}
	if lastIndex >= firstIndex {
		ents, err := storage.Entries(firstIndex, lastIndex+1)
		if err != nil {
			panic(err)
		}
		entries = append(entries, ents...)
	}
	return &RaftLog{
		storage:   storage,
		committed: firstIndex - 1,
		applied:   firstIndex - 1,
		stabled:   lastIndex,
		entries:   entries,
	}
}

// maybeCompact 会在 Lab2C 中负责丢弃已经被 storage compact 掉的内存日志。
// storage 是真实 compact 边界，RaftLog.entries 是内存缓存；这里让二者对齐。
// 否则 sendAppend 可能误以为旧日志还在，错过该发送 snapshot 的时机。
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	firstIndex, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	compactIndex := firstIndex - 1
	offset := l.entries[0].Index

	if compactIndex <= offset {
		return
	}

	term, err := l.storage.Term(compactIndex)
	if err != nil {
		panic(err)
	}

	newEntries := []pb.Entry{
		{
			Index: compactIndex,
			Term:  term,
		},
	}

	if compactIndex < l.LastIndex() {
		newEntries = append(newEntries, l.entries[compactIndex-offset+1:]...)
	}

	l.entries = newEntries

	if l.stabled < compactIndex {
		l.stabled = compactIndex
	}
	if l.committed < compactIndex {
		l.committed = compactIndex
	}
	if l.applied < compactIndex {
		l.applied = compactIndex
	}
}

// allEntries 返回所有还没被 compact 的真实日志。
// 注意：返回值不包含内部 dummy entry，测试会用它检查真实日志内容。
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries[1:]
}

// unstableEntries 返回所有还没有持久化的日志。
// 这些日志位于 stabled 之后，RawNode.Ready 会把它们交给上层先写盘。
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.stabled >= l.LastIndex() {
		return []pb.Entry{}
	}
	offset := l.entries[0].Index
	return l.entries[l.stabled-offset+1:]
}

// nextEnts 返回已经 committed 但还没有 applied 的日志。
// RawNode.Ready 会把这些日志交给上层按顺序 apply 到状态机。
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.applied >= l.committed {
		return nil
	}
	offset := l.entries[0].Index
	return l.entries[l.applied-offset+1 : l.committed-offset+1]
}

// LastIndex 返回当前 RaftLog 的最后一条日志 index。
// Raft 会用它决定新日志应该追加到哪里。
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.entries[0].Index + uint64(len(l.entries)) - 1
}

// Term 返回指定日志 index 对应的 term。
// Raft 在投票和 AppendEntries 日志匹配时都会用它。
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	offset := l.entries[0].Index
	if i < offset {
		// The entry is before the in-memory compact boundary. Surface this as
		// ErrCompacted so leaders can fall back to sending a snapshot.
		return 0, ErrCompacted
	}
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}

	return l.entries[i-offset].Term, nil
}

// appendEntries 把 leader 发来的日志合并进本地内存日志。
// 已匹配的前缀会保留，冲突后缀会截断，新追加的日志会保持 unstable，
// 直到后续 Ready/Advance 标记为已持久化。
func (l *RaftLog) appendEntries(ents []*pb.Entry) {
	if len(ents) == 0 {
		return
	}

	offset := l.entries[0].Index

	for i, ent := range ents {
		if ent.Index <= l.LastIndex() {
			localTerm, err := l.Term(ent.Index)
			if err == nil && localTerm == ent.Term {
				continue
			}

			l.entries = l.entries[:ent.Index-offset]

			if l.stabled >= ent.Index {
				l.stabled = ent.Index - 1
			}

			for _, newEnt := range ents[i:] {
				l.entries = append(l.entries, *newEnt)
			}
			return
		}

		for _, newEnt := range ents[i:] {
			l.entries = append(l.entries, *newEnt)
		}
		return
	}
}
