# 并行与批处理设计

## 概述

本文档描述 ETL 平台的并行（分片）和批处理架构，包括每种 Source 的分片策略、可配置的批处理参数以及 checkpoint 语义。

## 并行架构

### 工作原理

当 pipeline spec 包含 `parallelism.sharding.logical_shards > 1`（或旧字段 `parallelism.count > 1`）时，平台按 logical shard 创建独立的 `Runner` 实例。每个实例：

1. 获得**深度复制后的 spec**，`shard_index` 和 `shard_total` 注入到 `source.config` 中
2. 拥有自己的**隔离 checkpoint 命名空间**：`{pipeline-name}.shard-{N}`
3. 独立运行，拥有自己的读循环、Transform 链和 Sink writer
4. 共享相同的 Sink 目标（如同一个 MySQL 表、同一个 Kafka topic）

```
Pipeline spec（logical_shards=4）
├── Shard 0：Source(shard_index=0) → Transforms → Sink  [checkpoint：name.shard-0]
├── Shard 1：Source(shard_index=1) → Transforms → Sink  [checkpoint：name.shard-1]
├── Shard 2：Source(shard_index=2) → Transforms → Sink  [checkpoint：name.shard-2]
└── Shard 3：Source(shard_index=3) → Transforms → Sink  [checkpoint：name.shard-3]
```

`parallelism.sharding.logical_shards` 表示稳定的数据归属，不应该只为了调吞吐而随意修改。`parallelism.execution.max_active_shards` 表示 standalone 进程内最多同时运行多少个 logical shard。master/worker 模式下，**所有** linear pipeline 都会派发为 shard 任务（含 `logical_shards=1` 的单分片持续任务，即 pipeline 级放置），实际并发由 worker slots 限制。

### Streaming 放置 vs 多分片扩展

- **未分片 streaming**（默认 `logical_shards=1`，kafka/cdc）：单一放置。standalone 在本进程跑；master 派 1 个 continuous worker 任务。Validate 会提示这不是多副本 HA。
- **多分片 streaming**（Kafka consumer group 分片、多表 `mysql_cdc` 等）：N 个任务/Runner。Kafka 安全默认是 `logical_shards = topic 分区数`。
- **运维层拆分（无代码）**：多个 standalone 实例各自挂载部分 pipeline YAML，共享 MySQL 元数据。
- Checkpoint：多分片用 `{name}.shard-{N}`；单分片放置保持普通 `{name}`，standalone CDC 升到 master-worker 不会丢掉旧 checkpoint。

### 每种 Source 的分片策略

| Source | 分片机制 | 读取的 Config 字段 | 说明 |
|--------|---------|-------------------|------|
| **file** | 行号取模 | `shard_index`、`shard_total` | Shard i 获取 `lineNum % total == i` 的行。所有分片读取同一个文件但输出不同行。在稳定全局位置分片实现前，恢复语义按 at-least-once 处理，可能重放。 |
| **http** | 页码取模 | `shard_index`、`shard_total` | Shard i 获取第 `i+1, i+1+total, i+1+2*total, ...` 页。每个分片独立发起 HTTP 请求。 |
| **redis** | Key hash 取模 | `shard_index`、`shard_total`、`shard_key` | 使用 `sharding.strategy: hash_modulo` 且 `sharding.key: _key` 或 `@key`。所有分片 SCAN 同一个 keyspace，但每个分片只处理 `hash(key) % total == index` 的 key。正确但扫描有重叠。 |
| **mysql_batch** | 主键取模 | `shard_index`、`shard_total` | SQL 查询中使用 `WHERE MOD(pk, total) = idx`。每个分片获取不相交的行分区。 |
| **mysql_cdc** | 表分区 + server_id 隔离 | `shard_index`、`shard_total` | Shard i 处理表 `[i, i+total, ...]`。每个分片获取唯一的 binlog `server_id`。单表 CDC 无效果。 |
| **mysql_snapshot_cdc** | 快照阶段 PK 取模 + CDC 阶段表分区 | `shard_index`、`shard_total` | 快照期间：`WHERE MOD(pk, total) = idx`。CDC 期间：表分区 + 唯一 server_id。 |
| **kafka** | 消费者组自动均衡 | （无 — 忽略分片字段） | N 个并行 runner 加入同一个 `group_id`。Kafka 的消费者组协议自动分配分区。**logical shards 应小于等于 topic partition 数**，否则多余 shard 空闲。 |
| **postgres_cdc** | 不支持分片 | — | 仅限单实例。 |

### Sharding 与 Execution 字段

新 spec 应把稳定分片和运行并发分开：

```yaml
parallelism:
  sharding:
    strategy: pk_mod
    key: id
    logical_shards: 16
  execution:
    max_active_shards: 4
    transform_workers: 1
    sink_concurrency: 1
```

`execution.sink_concurrency` 设置为正数时，会限制同一个 standalone 进程内多个 shard runner 的并发 `sink.Write` 数。它不会串行化 transform 或 checkpoint 保存，也不是跨进程的分布式 lease。

`execution.transform_workers` 大于 1 时，会在 linear runner 的当前批次内并行处理逐记录 transform，并在写入 sink 前恢复输入顺序。如果 transform 链包含 `BatchTransform`、`Flusher`、`StateSnapshotter` 或 state metrics provider，runner 会自动退回串行路径，保持 window/deduplicate/join 等状态边界有序。

`execution.source_concurrency` 仅作为前向兼容字段被解析，当前 runner 尚未激活该能力。Source 侧并行度由 `sharding.logical_shards` 和 `execution.max_active_shards` 控制。

旧字段仍兼容，并在 defaults 阶段映射：

```yaml
parallelism:
  count: 4
  shard_strategy: "id_range"
  shard_key: "id"
  shard_total: 0
```

兼容映射：

- `sharding.logical_shards = shard_total`（如设置），否则等于 `count`
- `execution.max_active_shards = count`
- `sharding.strategy = shard_strategy`
- `sharding.key = shard_key`

### DAG Execution Workers

DAG pipeline 不使用线性 `parallelism.sharding` 创建 runner shard。它可以使用顶层 `execution.workers` 在单个 DAG executor 内并行处理 route/transform 工作：

```yaml
execution:
  workers: 4
  batch_size: 1000
  backpressure_buffer: 200
```

Worker 结果会按 source 重新排序后再进入 sink 批次聚合。Sink 写入和 checkpoint 保存仍由单个聚合器完成，因此提高 `execution.workers` 可以提升 CPU/IO 型 transform 路由吞吐，但不会让 checkpoint 位置倒退。

### Kafka 并行注意事项

Kafka source 会忽略 `shard_strategy`，所有 logical shard 加入同一个 consumer group，由 Kafka 分配 topic partition。有效并行度受 topic partition 数限制：

- logical shards ≤ topic partitions：所有 shard 都能收到数据。
- logical shards > topic partitions：多余 shard 空闲，不报错。配置了 `source.config.topic_partitions` 时 validate 会警告；preflight 用**实时** broker 元数据警告，并在未分片时建议 `logical_shards = NumPartitions`。
- `source.config.topic_partitions` 仅作 broker 不可达时的静态校验提示；生产以 preflight 为准。

安全的 Kafka CDC 多分片示例：

```yaml
source:
  type: kafka
  config:
    brokers: ["kafka:9092"]
    topic: dbserver.inventory.orders
    group_id: openetl-orders-cdc
parallelism:
  sharding:
    logical_shards: 4   # ≤ PartitionCount；建议等于分区数
```

提高 `parallelism.sharding.logical_shards` 前先确认 partition 数：

```bash
kafka-topics.sh --describe --topic <topic>
# PartitionCount 需要 >= logical_shards
```

## 批处理架构

### 批次组装流程

```
readLoop ──(单条记录)──► [有界通道：backpressure_buffer]
                                   │
                              writeLoop
                                   │
                    ┌──────────────┼──────────────┐
                    │              │              │
              batch_size 达到  flush_interval_ms  通道关闭
                    │              │            （EOF/关闭）
                    └──────────────┼──────────────┘
                                   │
                              writeBatch()
                                   │
                         1. Transform 链
                         2. Sink.Write(batch)
                         3. Checkpoint 保存（节流）
```

### 可配置参数

| 参数 | YAML 字段 | 默认值 | 说明 |
|------|----------|--------|------|
| **批次大小** | `batch_size` | 1000 | 每批最大记录数。达到后立即 flush。 |
| **Flush 间隔** | `flush_interval_ms` | 1000（1秒） | Flush 之间的最大时间。即使未达到 `batch_size`，也会在此间隔 flush 部分批次。 |
| **Checkpoint 间隔** | `checkpoint_interval_sec` | 30 | Checkpoint 保存之间的最小时间。减少高吞吐量管道的 checkpoint 存储负载。EOF/关闭时始终保存。 |
| **背压缓冲** | `backpressure_buffer` | 100 | readLoop 和 writeLoop 之间的有界通道大小。满时 Source reader 阻塞，施加背压。 |

### Checkpoint 节流

Checkpoint 不会在每次批次 flush 时保存。相反：

1. 每次 `Sink.Write()` 成功后，runner 检查自上次 checkpoint 保存以来是否已过足够时间（`checkpoint_interval_sec`）
2. 如果是，或已累积 10 个未 checkpoint 的批次，则保存 checkpoint
3. EOF/关闭时**始终**保存 checkpoint（强制）
4. 这减少了高吞吐量管道的 checkpoint 存储 I/O，同时保持 at-least-once 语义

**影响**：崩溃恢复时，最多可能会重放 `checkpoint_interval_sec` 时长的批次。这对于 at-least-once 投递是可接受的；Sink 应当是幂等的（如 MySQL upsert 模式）。

### YAML 配置

```yaml
batch_size: 1000                  # 每批最大记录数
flush_interval_ms: 2000           # 每 2 秒 flush（默认 1000ms）
checkpoint_interval_sec: 60       # 每 60 秒 checkpoint（默认 30）
backpressure_buffer: 200          # 读写间缓冲 200 条记录（默认 100）
```

### 投递语义

- **At-least-once**：Checkpoint 在 Sink 写入成功**之后**保存。崩溃时，最后一个未 checkpoint 的批次会被重放。
- **Kafka offset 提交**：Offset 通过 `CheckpointForRecord()` 仅在 Sink 批次成功后提交。
- **MySQL 事务原子性**：每批次一个 `BEGIN/COMMIT` 事务。每条 INSERT 语句内部分块 500 行。
- **跨 Sink 原子性**：不支持。在扇出场景中，如果 Sink A 成功但 Sink B 失败，checkpoint 基于成功的写入推进。

## 并行 × 批处理交互

每个分片拥有独立的批处理：
- Shard 0 可能每 500ms flush（高吞吐）
- Shard 1 可能每 2s flush（慢速 Source）
- Checkpoint 按分片独立，因此慢速分片不影响快速分片的恢复

`batch_size`、`flush_interval_ms`、`checkpoint_interval_sec` 和 `backpressure_buffer` 值在所有分片间共享（同一 spec），但每个分片独立执行。
