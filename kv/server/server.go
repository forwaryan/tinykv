package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
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
	resp := new(kvrpcpb.GetResponse)

	// 每个请求创建一个 reader，用它看到同一时刻的 storage 快照。
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)

	// 读之前先检查 lock；如果有会阻塞当前版本的锁，就把 Locked 返回给客户端处理。
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}
	if lock.IsLockedFor(req.Key, req.Version, resp) {
		return resp, nil
	}

	// 没有挡路锁时，按 req.Version 去 write/default CF 找可见 value。
	value, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		resp.NotFound = true
		return resp, nil
	}

	resp.Value = value
	return resp, nil
}

// KvPrewrite 实现 Percolator 两阶段提交的第一阶段。
// 它应该检查冲突、写 lock，并保存临时 value。
func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	resp := new(kvrpcpb.PrewriteResponse)

	// Prewrite 会修改 mutation keys，本地先用 latch 防止同 key 写请求交错。
	keys := make([][]byte, 0, len(req.Mutations))
	for _, mut := range req.Mutations {
		keys = append(keys, mut.Key)
	}

	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, mut := range req.Mutations {
		// 先查 lock：如果已有事务锁住该 key，当前事务不能预写。
		lock, err := txn.GetLock(mut.Key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Locked: lock.Info(mut.Key),
			})
			continue
		}

		// 再查 write conflict：当前事务开始后如果别人提交过同一个 key，就必须失败。
		write, commitTs, err := txn.MostRecentWrite(mut.Key)
		if err != nil {
			return nil, err
		}
		if write != nil && commitTs >= req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: commitTs,
					Key:        mut.Key,
					Primary:    req.PrimaryLock,
				},
			})
			continue
		}

		// 预写阶段只写临时 value 到 default CF，并在 lock CF 挂锁；write CF 留给 Commit。
		switch mut.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(mut.Key, mut.Value)
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mut.Key)
		}

		txn.PutLock(mut.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindFromProto(mut.Op),
		})
	}

	// 只要这一批里任意 key 失败，就不落盘 txn.writes，保证 Prewrite 的批处理原子性。
	if len(resp.Errors) > 0 {
		return resp, nil
	}

	// 前面只是收集 Modify，这里才真正写入 storage。
	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	return resp, nil
}

// KvCommit 实现两阶段提交的第二阶段。
// 它应该把 Prewrite 阶段写入的 lock 转成已提交的 write record。
func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	resp := new(kvrpcpb.CommitResponse)

	// Commit 会把 lock 转成 write record，同样要用 latch 保护这些 key。
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, key := range req.Keys {
		// 重复 Commit 可能再次到达；CurrentWrite 用来保证已提交请求幂等成功。
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{
					Abort: "transaction has been rolled back",
				}
				return resp, nil
			}
			continue
		}

		// 没有当前事务的 write record 时，正常情况应该还能看到它的 lock。
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			continue
		}
		if lock.Ts != req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "lock belongs to another transaction",
			}
			return resp, nil
		}

		// Commit 不再写真正 value，只写提交记录，并删除 Prewrite 阶段留下的 lock。
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}

	server.Latches.Validate(txn, req.Keys)

	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	return resp, nil
}

// KvScan 使用 MVCC scanner 实现 Lab4C 的版本化范围读取。
func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	resp := new(kvrpcpb.ScanResponse)

	// Scan 和 Get 一样是快照读，只是从 start_key 开始连续返回多个可见 key/value。
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	// Scanner 负责在 write CF 中去重 user key，并按 req.Version 找可见版本。
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()

	// req.Limit 为 0 时循环不会进入，直接返回空 pairs。
	for uint32(len(resp.Pairs)) < req.Limit {
		key, value, err := scanner.Next()
		if err != nil {
			return nil, err
		}
		if key == nil {
			break
		}

		resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})
	}

	return resp, nil
}

// KvCheckTxnStatus 检查 primary lock 是否已提交、已回滚或已超时。
// 必要时它会清理超时 lock。
func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	resp := new(kvrpcpb.CheckTxnStatusResponse)

	// CheckTxnStatus 可能写 rollback record 或清理 primary lock，所以要 latch primary key。
	keys := [][]byte{req.PrimaryKey}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.LockTs)

	// 先看 primary key 是否已经有当前事务的 write record。
	// 如果有，说明事务状态已经确定，不需要再看 lock。
	write, commitTs, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if write != nil {
		if write.Kind != mvcc.WriteKindRollback {
			// Put/Delete write 表示事务已经提交，返回对应 commit timestamp。
			resp.CommitVersion = commitTs
		}
		// Rollback write 表示事务已经回滚；CommitVersion 保持 0 即可。
		resp.Action = kvrpcpb.Action_NoAction
		return resp, nil
	}

	// 没有 write record 时，再检查 primary lock 是否仍然存在。
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}

	if lock == nil || lock.Ts != req.LockTs {
		// primary lock 不存在，客户端会把该事务视为已回滚；这里补 rollback record 防止迟到 Commit。
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		resp.Action = kvrpcpb.Action_LockNotExistRollback
	} else if mvcc.PhysicalTime(lock.Ts)+lock.Ttl > mvcc.PhysicalTime(req.CurrentTs) {
		// lock 还没过期，不能替它做决定，只把剩余 TTL 语义返回给客户端等待/重试。
		resp.LockTtl = lock.Ttl
		resp.Action = kvrpcpb.Action_NoAction
	} else {
		// lock 已过期：回滚 primary key，清理 Prewrite 留下的 lock/default，并写 rollback record。
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		resp.Action = kvrpcpb.Action_TTLExpireRollback
	}

	server.Latches.Validate(txn, keys)

	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	return resp, nil
}

// KvBatchRollback 回滚同一个 start timestamp 下的一批 key。
// 它需要清理 lock 和临时 value，并写入 rollback 记录。
func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	resp := new(kvrpcpb.BatchRollbackResponse)

	// Rollback 会删除 lock/default 并写 rollback record，需要和 Commit/Prewrite 串行化。
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, key := range req.Keys {
		// 先看当前事务是否已经留下 write record；这是判断幂等和已提交冲突的入口。
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}

		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				// 重复 rollback 请求应该幂等成功，不需要再写一次 rollback record。
				continue
			}

			// 已经 Put/Delete 提交过的事务不能再回滚，否则会破坏已提交版本。
			resp.Error = &kvrpcpb.KeyError{
				Abort: "transaction has been committed",
			}
			return resp, nil
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}

		if lock != nil && lock.Ts == req.StartVersion {
			// 只有当前事务自己的 lock 才能删除；其它事务的 lock 必须保留。
			txn.DeleteLock(key)
			// Prewrite 阶段写入的临时 value 还没提交，回滚时要从 default CF 清掉。
			txn.DeleteValue(key)
		}

		// 即使没有 lock，也要写 rollback record，防止迟到的 Commit 把该事务重新提交。
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})
	}

	server.Latches.Validate(txn, req.Keys)

	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	return resp, nil
}

// KvResolveLock 根据 CommitVersion 决定提交或回滚某个事务留下的所有 lock。
func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	resp := new(kvrpcpb.ResolveLockResponse)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	// ResolveLock 不带具体 keys，需要扫描 lock CF 找出这个 start_ts 事务留下的全部 lock。
	locks, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		return nil, err
	}

	// 拿到所有待处理 lock 后，再 latch 这些 key，避免和并发 Commit/Rollback 交错写。
	keys := make([][]byte, 0, len(locks))
	for _, pair := range locks {
		keys = append(keys, pair.Key)
	}

	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	for _, pair := range locks {
		key := pair.Key
		lock := pair.Lock

		if req.CommitVersion == 0 {
			// CommitVersion 为 0 表示事务最终要回滚：删 lock、删临时 value、写 rollback record。
			txn.DeleteLock(key)
			txn.DeleteValue(key)
			txn.PutWrite(key, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			})
		} else {
			// CommitVersion 非 0 表示事务最终提交：保留 default value，写正式 write record，再删 lock。
			txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    lock.Kind,
			})
			txn.DeleteLock(key)
		}
	}

	server.Latches.Validate(txn, keys)

	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	return resp, nil
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
