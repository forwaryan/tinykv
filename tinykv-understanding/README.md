# TinyKV 理解文档

这个目录用中文白话解释 TinyKV 每个 Lab 的目标、请求路径、测试方式，以及它和 MIT 6.5840 的关系。README 只做入口页：帮你先建立全局地图，细节放在各个 Lab 文档里慢慢展开。

## 总览

TinyKV 的四个 Lab 是一层一层加能力：

```mermaid
graph LR
    L1["Lab1<br/>单机 KV<br/>把请求落到本地 Badger"] --> L2["Lab2<br/>RaftKV<br/>多副本按同一顺序执行"]
    L2 --> L3["Lab3<br/>Multi-Raft<br/>把 key 空间切成多个 Region"]
    L3 --> L4["Lab4<br/>Transactions<br/>用 MVCC 和锁提供事务语义"]
```

可以这样记：

| 层级 | 解决什么问题 | 直觉 |
|---|---|---|
| Lab1 | 存储 | 一台机器能可靠地读、写、删、扫 |
| Lab2 | 复制 | 多台机器通过 Raft 复制同一份状态 |
| Lab3 | 扩展 | 多个 Region / Raft group 分摊 key 空间和负载 |
| Lab4 | 事务 | 同一个 key 保存多个版本，事务按时间戳读写 |

前面三层主要回答“数据怎么存、怎么复制、怎么横向扩展”。Lab4 在这些能力之上回答另一个问题：多个客户端并发读写时，怎样提供事务、快照读、写冲突检测、回滚和锁清理。

## 文档索引

| 文档 | 建议先抓住什么 |
|---|---|
| [Lab1：单机 KV](./labs/lab1-standalonekv.md) | 把 `RawGet` / `RawPut` / `RawDelete` / `RawScan` 翻译成 BadgerDB 的本地读写，顺便打好 `Storage` 和 CF 的基础 |
| [Lab2：RaftKV](./labs/lab2-raftkv.md) | 写请求不再直接落盘，而是先进入 Raft log，等多数副本提交后再 apply 到 BadgerDB |
| [Lab3：Multi-Raft](./labs/lab3-multiraftkv.md) | 一个大 Raft group 变成多个 Region，每个 Region 有自己的 Peer、Raft group、split 和调度流程 |
| [Lab4：事务](./labs/lab4-transactions.md) | 在 KV 之上实现 MVCC / Percolator 风格事务，用 `default`、`write`、`lock` 三个 CF 管理版本和锁 |
| [测试指南](./labs/testing-guide.md) | 每个 Lab 跑什么 `make projectX` 命令，测试大概在检查哪些场景 |
| [参考实现阅读路线](./reference-implementation.md) | 旧会话里筛出的完整实现参考，以及每个 Lab 应该优先对照哪些源码文件 |

## 请求路径

### Lab1：单机读写

Lab1 的核心路径最短：服务端收到 Raw KV 请求后，直接通过 `StandaloneStorage` 读写本地 BadgerDB。

```mermaid
graph LR
    C["客户端 Raw KV 请求"] --> API["Raw API"]
    API --> S["StandaloneStorage"]
    S --> B["BadgerDB<br/>本地持久化"]
    B --> R["返回结果"]
```

这层先不考虑 Raft、Region、事务，只把本地存储抽象打牢。

### Lab2：先复制，再落盘

Lab2 把 Lab1 的本地写入放进 Raft。写请求先成为 Raft log，复制到多数副本并提交后，才会 apply 到状态机和 BadgerDB。

```mermaid
sequenceDiagram
    participant C as 客户端
    participant L as Region leader
    participant R as Raft
    participant F as 多数副本
    participant A as apply
    participant B as BadgerDB

    C->>L: RawPut / RawDelete
    L->>R: propose Raft command
    R->>F: 复制日志
    F-->>R: 多数确认
    R-->>A: committed entry
    A->>B: 应用到本地存储
    A-->>C: 返回结果
```

这一层的关键词是：选主、日志复制、commit、apply、snapshot。

### Lab3：先找 Region，再走它自己的 Raft

Lab3 解决扩展问题。请求先根据 key 找到对应 Region，再进入这个 Region 自己的 Raft group。不同 key range 可以由不同 Region 独立复制、分裂和调度。

```mermaid
graph LR
    Req["客户端请求<br/>key = user:42"] --> Locate["按 key 定位 Region"]
    Locate --> Leader["对应 Region 的 leader Peer"]
    Leader --> Group["该 Region 的 Raft group"]
    Group --> Commit["多数 Peer commit"]
    Commit --> Apply["apply 到对应 Store"]
    Apply --> DB["BadgerDB"]
```

Region、Peer、Store 的关系可以横着和竖着看：

```mermaid
graph TB
    subgraph S1["Store 1"]
        P11["R1 Peer"]
        P21["R2 Peer"]
    end
    subgraph S2["Store 2"]
        P12["R1 Peer"]
        P22["R2 Peer"]
    end
    subgraph S3["Store 3"]
        P13["R1 Peer"]
        P23["R2 Peer"]
    end

    P11 --- R1["Region 1<br/>Raft group<br/>[a, m)"]
    P12 --- R1
    P13 --- R1

    P21 --- R2["Region 2<br/>Raft group<br/>[m, z)"]
    P22 --- R2
    P23 --- R2
```

一句话：同一个 Region 的多个 Peer 组成一个 Raft group；同一个 Store 上可以承载很多不同 Region 的 Peer。

### Lab4：在 KV 上加事务和多版本

Lab4 的重点从“请求如何复制和分片”转到“并发读写如何保持事务语义”。它使用 MVCC：同一个 key 可以有多个历史版本，事务按照自己的 `start_ts` 读取可见版本。

事务读路径：

```mermaid
sequenceDiagram
    participant T as 事务 start_ts
    participant L as lock CF
    participant W as write CF
    participant D as default CF

    T->>L: 检查是否有冲突锁
    T->>W: 找到 commit_ts <= start_ts 的最新提交记录
    W-->>T: 返回 value 对应的 start_ts
    T->>D: 读取真实 value
    D-->>T: 返回快照可见版本
```

事务写路径是 Percolator 风格的两阶段流程：

```mermaid
graph LR
    Start["事务写入"] --> Prewrite["Prewrite<br/>检查冲突"]
    Prewrite --> Lock["lock CF<br/>写事务锁"]
    Prewrite --> Default["default CF<br/>写临时 value"]
    Lock --> Commit["Commit"]
    Default --> Commit
    Commit --> Write["write CF<br/>写提交记录 commit_ts"]
    Commit --> Unlock["删除 lock"]
    Write --> Visible["版本正式可见"]
    Unlock --> Visible
```

所以 Lab4 和前面三层的关系是：

```mermaid
graph TB
    Txn["事务 API<br/>KvGet / KvPrewrite / KvCommit / Rollback"] --> MVCC["MVCC 层<br/>版本、锁、提交记录"]
    MVCC --> KV["KV 存储抽象"]
    KV --> L1["Lab1 本地 Badger 读写"]
    KV --> L2["Lab2 Raft 复制路径"]
    KV --> L3["Lab3 Region / Multi-Raft 路由"]
```

前面三层让 KV 系统能存、能复制、能拆分；Lab4 让这个 KV 系统在并发场景下能表达“一个事务看到哪个版本、能不能提交、失败后怎么清理”。

Raw KV 和事务 KV 的差别可以这样看：

| 对比 | Raw KV | 事务 KV |
|---|---|---|
| 写入方式 | `RawPut` 直接写当前值 | `KvPrewrite` 先写锁和临时值，`KvCommit` 再让版本可见 |
| 读取方式 | 读当前值 | 按 `start_ts` 读快照版本 |
| 冲突处理 | 基本不处理事务冲突 | 检查锁、写冲突、回滚和清理锁 |
| 存储形态 | 一个 key 一个当前 value | 一个 key 多个版本，配合 `default/write/lock` 三个 CF |

```mermaid
graph TB
    subgraph Raw["Raw KV"]
        RawPut["RawPut(k, v)"] --> RawDB["Badger 当前值<br/>k -> v"]
    end

    subgraph Txn["事务 KV"]
        Begin["事务 start_ts"] --> Pre["KvPrewrite<br/>lock + default"]
        Pre --> Commit["KvCommit(commit_ts)<br/>write + delete lock"]
        Commit --> Version["可见版本<br/>k@commit_ts -> value"]
    end
```

## 推荐阅读顺序

1. 先看 [Lab1](./labs/lab1-standalonekv.md)：理解请求怎样从 Raw API 走到 Badger。
2. 再看 [Lab2](./labs/lab2-raftkv.md)：理解写请求为什么要先进入 Raft log。
3. 然后看 [Lab3](./labs/lab3-multiraftkv.md)：理解为什么要把 key 空间拆成多个 Region。
4. 最后看 [Lab4](./labs/lab4-transactions.md)：这时思维要从“复制和分片”切到“数据库事务语义”，理解为什么一个 key 要保存多个版本，以及锁、提交记录、回滚如何配合。
5. 需要跑实验时看 [测试指南](./labs/testing-guide.md)：先跑小阶段，再跑整组 `projectX`。

## 测试入口

在 TinyKV 源码根目录运行：

```bash
make project1
make project2
make project3
make project4
```

如果只想定位某个小阶段，可以看 [测试指南](./labs/testing-guide.md) 里的 `project2aa`、`project3b`、`project4c` 等命令。

## 官方资料

- TinyKV 仓库：https://github.com/talent-plan/tinykv
- Project 1 StandaloneKV：https://yunpengn.github.io/tinykv/doc/project1-StandaloneKV.html
- Project 2 RaftKV：https://yunpengn.github.io/tinykv/doc/project2-RaftKV.html
- Project 3 MultiRaftKV：https://yunpengn.github.io/tinykv/doc/project3-MultiRaftKV.html
- Project 4 Transactions：https://github.com/talent-plan/tinykv/blob/course/doc/project4-Transaction.md
- Raft 论文：https://raft.github.io/raft.pdf
- Percolator 论文：https://storage.googleapis.com/pub-tools-public-publication-data/pdf/36726.pdf

## 和 MIT 6.5840 的关系

TinyKV 和 MIT 6.5840 都适合学习分布式系统，但关注点不完全一样：

| TinyKV | MIT 6.5840 | 关系 |
|---|---|---|
| Lab1 单机 KV | KV Server 的前置练习 | 都有 KV 读写；TinyKV 更像真实数据库的存储层 |
| Lab2 RaftKV | Raft + Fault-tolerant KV | 重叠度很高，都是复制状态机和 Raft 正确性 |
| Lab3 Multi-Raft | Sharded KV | 都有分片思想；TinyKV 更贴近 TiKV 的 Region / PD 模型 |
| Lab4 Transactions | 标准 MIT 实验基本没有 | TinyKV 独有，重点是 MVCC、事务锁、两阶段提交和回滚 |

一句话：

```text
MIT 6.5840 更训练分布式系统正确性。
TinyKV 更贴近 TiKV 这类分布式数据库存储层的工程结构。
```
