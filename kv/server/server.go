package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server 是 TinyKV 对外提供服务的入口，负责接收 TinySQL/客户端请求。
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

// NewServer 基于一个 Storage 实现创建 gRPC TinyKV server。
// Lab1 传入 StandAloneStorage；Lab2 会传入 RaftStorage，但上层 API 不变。
func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// 下面这些函数是 Server 的 gRPC API，实现 tinykvpb.TinyKvServer。

// Raft 转发 TinyKV 节点之间的 Raft 消息流。
// 它只在 RaftStorage 模式下使用，所以这里直接转发给 storage。
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot 转发 TinyKV 节点之间的快照流。
// 它只在 RaftStorage 模式下使用，所以这里直接转发给 storage。
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// KvGet 实现 Lab4B 的事务读路径。
// 它应该按指定版本读取 MVCC lock/write/value，并返回可见 value 或 key error。
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	return nil, nil
}

// KvPrewrite 实现 Percolator 两阶段提交的第一阶段。
// 它应该检查冲突、写 lock，并保存临时 value。
func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	return nil, nil
}

// KvCommit 实现两阶段提交的第二阶段。
// 它应该把 Prewrite 阶段写入的 lock 转成已提交的 write record。
func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	return nil, nil
}

// KvScan 使用 MVCC scanner 实现 Lab4C 的版本化范围读取。
func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// KvCheckTxnStatus 检查 primary lock 是否已提交、已回滚或已超时。
// 必要时它会清理超时 lock。
func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// KvBatchRollback 回滚同一个 start timestamp 下的一批 key。
// 它需要清理 lock 和临时 value，并写入 rollback 记录。
func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// KvResolveLock 根据 CommitVersion 决定提交或回滚某个事务留下的所有 lock。
func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// Coprocessor 处理 SQL pushdown 请求。
// 它会创建 storage reader，并把请求交给 coprocessor handler。
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
