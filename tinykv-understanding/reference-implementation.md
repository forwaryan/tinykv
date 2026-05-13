# TinyKV 参考实现阅读路线

这份文档记录旧会话里筛出来的参考实现线索。它只用于学习和对照，不建议直接复制代码。

## 推荐参考仓库

- 完整实现参考：https://github.com/waruto210/tinykv/tree/project4
- 作者实现总结：https://waruto.top/posts/tinykv-impl/
- 官方骨架仓库：https://github.com/talent-plan/tinykv

旧会话里筛过几个 fork，`waruto210/tinykv` 的 `project4` 分支最适合作为主参考。这个分支最后提交信息是 `finish project 4c`，覆盖到事务部分。

## 按 Lab 读哪些文件

| 想理解 | 优先看这些文件 |
|---|---|
| Lab1 单机 KV | `kv/storage/standalone_storage/standalone_storage.go`、`kv/server/raw_api.go` |
| Lab2 Raft | `raft/raft.go`、`raft/log.go`、`raft/rawnode.go` |
| Lab2 RaftKV | `kv/raftstore/peer_msg_handler.go`、`kv/storage/raft_storage/raft_server.go` |
| Lab3 Multi-Raft | `raft/raft.go` 里的 conf change / leader transfer，`kv/raftstore/peer_msg_handler.go` 里的 admin command / split |
| Lab3 Scheduler | `scheduler/server/cluster.go`、`scheduler/server/schedulers/balance_region.go` |
| Lab4 事务 | `kv/transaction/mvcc/transaction.go`、`kv/transaction/mvcc/scanner.go`、`kv/server/server.go` |

## 使用方式

建议先读本仓库的理解文档，知道每个 Lab 在解决什么问题；真正写代码时，再打开参考实现对照接口和流程。

```text
先看官方骨架要你实现什么
  -> 再看参考实现怎么组织流程
  -> 回到本仓库自己写
  -> 跑对应 project 测试
```

Lab1 开始时，优先关注两条线：

```text
Storage 层：
StandaloneStorage -> engine_util -> BadgerDB

API 层：
RawGet / RawPut / RawDelete / RawScan -> storage.Reader / storage.Write
```

不要一开始就跳到 Lab2 的 Raft。Lab1 的目标只是把单机存储层跑通。
