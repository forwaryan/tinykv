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
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"sort"
	"strings"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// min 返回两个 uint64 中较小的一个，常用于日志 index 或 term 比较。
func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

// max 返回两个 uint64 中较大的一个。
func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// IsEmptyHardState 判断 HardState 是否为空。
// RawNode 用它避免在 Ready 中重复返回没有变化的 HardState。
func IsEmptyHardState(st pb.HardState) bool {
	return isHardStateEqual(st, pb.HardState{})
}

// IsEmptySnap 判断 Snapshot 是否为空。
// Ready/HasReady 用它判断是否有快照需要上层持久化或发送。
func IsEmptySnap(sp *pb.Snapshot) bool {
	if sp == nil || sp.Metadata == nil {
		return true
	}
	return sp.Metadata.Index == 0
}

// mustTerm 解包 term 查询结果。
// 在测试或辅助路径中，如果查 term 出错就直接 panic。
func mustTerm(term uint64, err error) uint64 {
	if err != nil {
		panic(err)
	}
	return term
}

// nodes 返回当前 Raft peer id 的有序列表。
// ApplyConfChange 用它构造稳定顺序的 ConfState。
func nodes(r *Raft) []uint64 {
	nodes := make([]uint64, 0, len(r.Prs))
	for id := range r.Prs {
		nodes = append(nodes, id)
	}
	sort.Sort(uint64Slice(nodes))
	return nodes
}

// diffu 返回两个字符串的 unified diff。
// 测试里用它比较期望状态和实际状态。
func diffu(a, b string) string {
	if a == b {
		return ""
	}
	aname, bname := mustTemp("base", a), mustTemp("other", b)
	defer os.Remove(aname)
	defer os.Remove(bname)
	cmd := exec.Command("diff", "-u", aname, bname)
	buf, err := cmd.CombinedOutput()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			// do nothing
			return string(buf)
		}
		panic(err)
	}
	return string(buf)
}

// mustTemp 把文本写入临时文件并返回文件名。
// diffu 会用它把字符串交给系统 diff 命令。
func mustTemp(pre, body string) string {
	f, err := ioutil.TempFile("", pre)
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(f, strings.NewReader(body))
	if err != nil {
		panic(err)
	}
	f.Close()
	return f.Name()
}

// ltoa 把 RaftLog 格式化成可读文本，方便测试失败时定位问题。
func ltoa(l *RaftLog) string {
	s := fmt.Sprintf("committed: %d\n", l.committed)
	s += fmt.Sprintf("applied:  %d\n", l.applied)
	for i, e := range l.entries {
		s += fmt.Sprintf("#%d: %+v\n", i, e)
	}
	return s
}

type uint64Slice []uint64

// Len 返回 uint64Slice 中元素数量，供 sort 使用。
func (p uint64Slice) Len() int { return len(p) }

// Less 定义 peer id 的升序排序规则。
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }

// Swap 在排序过程中交换两个 peer id。
func (p uint64Slice) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

// IsLocalMsg 判断消息是否只能由本地 Raft 节点内部产生。
// 这种消息不应该从网络侧 RawNode.Step 进入。
func IsLocalMsg(msgt pb.MessageType) bool {
	return msgt == pb.MessageType_MsgHup || msgt == pb.MessageType_MsgBeat
}

// IsResponseMsg 判断消息是否是某个 Raft RPC 的响应。
// RawNode.Step 用它拒绝未知 peer 发来的 response。
func IsResponseMsg(msgt pb.MessageType) bool {
	return msgt == pb.MessageType_MsgAppendResponse || msgt == pb.MessageType_MsgRequestVoteResponse || msgt == pb.MessageType_MsgHeartbeatResponse
}

// isHardStateEqual 比较 Raft 关心的持久化状态字段：
// term、vote 和 committed index。
func isHardStateEqual(a, b pb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}
