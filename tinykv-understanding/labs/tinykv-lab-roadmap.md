# TinyKV Lab 梳理

资料来源：

- Project 1 StandaloneKV: https://yunpengn.github.io/tinykv/doc/project1-StandaloneKV.html
- Project 2 RaftKV: https://yunpengn.github.io/tinykv/doc/project2-RaftKV.html
- Project 3 MultiRaftKV: https://yunpengn.github.io/tinykv/doc/project3-MultiRaftKV.html
- Project 4 Transactions: https://github.com/talent-plan/tinykv/blob/course/doc/project4-Transaction.md

说明：TinyKV 官方叫 `Project 1/2/3/4`，简历里很多人会叫 `Lab 1/2/3/4`。下面先按 Lab 来写。

## 总路线

TinyKV 的路线不是一上来就做分布式，而是逐层加能力：

```text
Lab1: 单机 KV
  -> Lab2: 单 Raft Group 的高可用 KV
  -> Lab3: 多 Raft Group / Region / 调度
  -> Lab4: MVCC / Percolator 事务
```

和 MIT 6.5840 的大致关系：

```text
TinyKV Lab1  ~= MIT Lab2 的一部分：单机 KV，但 TinyKV 更偏 Badger 存储封装
TinyKV Lab2  ~= MIT Lab3 + Lab4：Raft + RaftKV
TinyKV Lab3  ~= MIT Lab5 的一部分：分片/多组复制，但 TinyKV 更像 TiKV Region
TinyKV Lab4  = MIT 标准实验没有：MVCC + 分布式事务
```

## Lab1: StandaloneKV

一句话：实现一个单机版 KV 数据库服务。

这里还没有 Raft、没有分布式、没有副本、没有事务。客户端通过 gRPC 发请求，服务端直接读写本地 BadgerDB。

### Lab1 要实现什么

Lab1 分成两块：

1. 实现单机存储引擎 `StandaloneStorage`
2. 实现 Raw KV 的 RPC handler

### 1. 实现 StandaloneStorage

相关位置：

- `kv/storage/storage.go`
- `kv/storage/standalone_storage/standalone_storage.go`
- `kv/util/engine_util`

核心接口大概是：

```go
type Storage interface {
    Write(ctx *kvrpcpb.Context, batch []Modify) error
    Reader(ctx *kvrpcpb.Context) (StorageReader, error)
}
```

你要做的事情：

- 用 BadgerDB 作为底层本地 KV 存储。
- `Write` 接收一批修改，然后一次性写进 Badger。
- 修改类型主要是 `Put` 和 `Delete`。
- `Reader` 返回一个读视图，用来支持 `Get` 和 `Scan`。
- `Reader` 应该基于 Badger transaction/snapshot，这样一次读取过程中看到的数据是一致的。
- 用完 reader/iterator 后要正确关闭，避免资源泄漏。

这里的 `ctx *kvrpcpb.Context` 在 Lab1 基本不用，后面的 Raft/Region 才会用到。

### Column Family 是什么

Lab1 还要求支持 Column Family，简称 CF。

你可以把 CF 理解成“同一个 BadgerDB 里隔出来的多个小数据库”：

```text
default CF:
  key1 -> valueA

write CF:
  key1 -> valueB

lock CF:
  key1 -> valueC
```

同一个 `key1` 在不同 CF 里可以有不同的值。

Badger 本身不直接支持 CF，所以 TinyKV 用 `engine_util` 做了一层模拟：把 CF 前缀拼到真实 key 上。

简单理解：

```text
用户看到: cf = default, key = abc
底层存储: default_abc
```

Lab1 做 CF 的原因：Lab4 的 MVCC/事务会用到 `default`、`write`、`lock` 三个 CF。

### 2. 实现 Raw KV RPC Handler

相关位置：

- `kv/server/raw_api.go`
- `proto/proto/tinykvpb.proto`
- `proto/proto/kvrpcpb.proto`

要实现四个接口：

```text
RawGet
RawPut
RawDelete
RawScan
```

它们分别做：

```text
RawGet:
  读某个 CF 下某个 key 的当前值

RawPut:
  写某个 CF 下某个 key 的值

RawDelete:
  删除某个 CF 下某个 key

RawScan:
  从 start_key 开始，按 key 顺序扫描一批 key/value
```

### Lab1 的请求流程

以 `RawPut` 为例：

```text
client
  -> gRPC RawPut
  -> kv/server/raw_api.go
  -> StandaloneStorage.Write
  -> engine_util
  -> BadgerDB
```

以 `RawGet` 为例：

```text
client
  -> gRPC RawGet
  -> kv/server/raw_api.go
  -> StandaloneStorage.Reader
  -> reader.GetCF
  -> engine_util
  -> BadgerDB snapshot
```

### Lab1 不做什么

Lab1 不需要做：

- Raft
- leader election
- log replication
- snapshot
- Region
- 多副本一致性
- 分布式事务
- MVCC
- Percolator

所以它本质上是后面所有功能的本地存储基础。

### Lab1 和 MIT 6.5840 的关系

Lab1 最像 MIT 6.5840 的 Lab2，但只像一部分。

相同点：

- 都是 KV 服务。
- 都有 `Get/Put` 这种基本操作。
- 都还不是 RaftKV。

不同点：

- MIT Lab2 更强调 RPC、丢包重试、版本号条件写、线性一致语义。
- TinyKV Lab1 更强调 BadgerDB 封装、Column Family、Scan、存储接口抽象。
- TinyKV Lab1 是为了后面的 TiKV 风格架构铺路。

如果面试时解释，可以这么说：

> TinyKV Lab1 是单机存储层，不涉及分布式一致性。它主要实现 Badger 上的一层 Storage 抽象和 Raw KV RPC，包括 Get、Put、Delete、Scan，以及 Column Family 支持。这个 Lab 更像是在给后面的 RaftKV 和事务层打地基。

### Lab1 检查点

完成后应该能通过：

```bash
make project1
```

理解上应该能回答：

- `Storage.Write` 和 `Storage.Reader` 分别负责什么？
- 为什么 `Reader` 要用 snapshot/transaction？
- CF 是什么，Badger 不支持 CF 时 TinyKV 怎么模拟？
- `RawGet` 和 `RawScan` 怎么从 storage 里读数据？
- Lab1 为什么暂时不需要关心 `kvrpcpb.Context`？

## Lab2: RaftKV

一句话：把 Lab1 的单机 KV 变成基于 Raft 的高可用 KV。

主要分三块：

```text
Part A: 实现 Raft 本体
Part B: 用 Raft 复制 KV 请求
Part C: 做 Raft log GC 和 snapshot
```

和 MIT 的关系：

```text
TinyKV Lab2 Part A ~= MIT Lab3 Raft
TinyKV Lab2 Part B ~= MIT Lab4 RaftKV
TinyKV Lab2 Part C ~= MIT Lab3/4 的 snapshot
```

待继续细化：

- leader election
- log replication
- RawNode / Ready 接口
- PeerStorage
- RaftStorage
- raftstore ready 处理
- apply committed entries
- snapshot 发送与恢复

## Lab3: MultiRaftKV

一句话：从“一个 Raft group 管全部 key”升级成“多个 Region / 多个 Raft group 分摊 key 空间”。

主要分三块：

```text
Part A: Raft membership change 和 leader transfer
Part B: raftstore 支持 ChangePeer、TransferLeader、Region Split
Part C: Scheduler 收集 heartbeat，并做 balance region
```

和 MIT 的关系：

```text
TinyKV Lab3 和 MIT Lab5 都在做“分片 + 多组复制”
但 MIT 是固定 shard + shard controller
TinyKV 是 range region + split + scheduler，更像 TiKV
```

待继续细化：

- AddNode / RemoveNode
- TransferLeader
- RegionEpoch
- Region split
- key range `[start_key, end_key)`
- scheduler heartbeat
- balance region operator

## Lab4: Transactions

一句话：在 KV 上实现 MVCC 和 Percolator 风格事务。

主要分三块：

```text
Part A: MVCC 存储层
Part B: KvGet / KvPrewrite / KvCommit
Part C: KvScan / CheckTxnStatus / BatchRollback / ResolveLock
```

和 MIT 的关系：

```text
MIT 6.5840 标准实验基本没有这一层
这是 TinyKV 更数据库内核的部分
```

待继续细化：

- start timestamp
- commit timestamp
- snapshot isolation
- default/write/lock 三个 CF
- Prewrite
- Commit
- primary key lock
- write conflict
- rollback
- resolve lock
