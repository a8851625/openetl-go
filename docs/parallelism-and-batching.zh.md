# 并行与批处理设计

## 概述

本文档描述 ETL 平台的并行（分片）和批处理架构，包括每种 Source 的分片策略、可配置的批处理参数以及 checkpoint 语义。

## 并行架构

### 工作原理

当 pipeline spec 包含 `parallelism.count > 1` 时，平台创建 N 个独立的 `Runner` 实例。每个实例：

1. 获得**深度复制后的 spec**，`shard_index` 和 `shard_total` 注入到 `source.config` 中
2. 拥有自己的**隔离 checkpoint 命名空间**：`{pipeline-name}.shard-{N}`
3. 独立运行，拥有自己的读循环、Transform 链和 Sink writer
4. 共享相同的 Sink 目标（如同一个 MySQL 表、同一个 Kafka topic）

```
Pipeline spec（count=4）
├── Shard 0：Source(shard_index=0) → Transforms → Sink  [checkpoint：name.shard-0]
├── Shard 1：Source(shard_index=1) → Transforms → Sink  [checkpoint：name.shard-1]
├── Shard 2：Source(shard_index=2) → Transforms → Sink  [checkpoint：name.shard-2]
└── Shard 3：Source(shard_index=3) → Transforms → Sink  [checkpoint：name.shard-3]
```

### 每种 Source 的分片策略

| Source | 分片机制 | 读取的 Config 字段 | 说明 |
|--------|---------|-------------------|------|
| **file** | 行号取模 | `shard_index`、`shard_total` | Shard i 获取 `lineNum % total == i` 的行。所有分片读取同一个文件但输出不同行。Byte-offset checkpoint 仍然有效。 |
| **http** | 页码取模 | `shard_index`、`shard_total` | Shard i 获取第 `i+1, i+1+total, i+1+2*total, ...` 页。每个分片独立发起 HTTP 请求。 |
| **redis** | Key hash 取模 | `shard_index`、`shard_total` | 所有分片 SCAN 同一个 keyspace，但每个分片只处理 `hash(key) % total == index` 的 key。正确但扫描有重叠。 |
| **mysql_batch** | 主键取模 | `shard_index`、`shard_total` | SQL 查询中使用 `WHERE MOD(pk, total) = idx`。每个分片获取不相交的行分区。 |
| **mysql_cdc** | 表分区 + server_id 隔离 | `shard_index`、`shard_total` | Shard i 处理表 `[i, i+total, ...]`。每个分片获取唯一的 binlog `server_id`。单表 CDC 无效果。 |
| **mysql_snapshot_cdc** | 快照阶段 PK 取模 + CDC 阶段表分区 | `shard_index`、`shard_total` | 快照期间：`WHERE MOD(pk, total) = idx`。CDC 期间：表分区 + 唯一 server_id。 |
| **kafka** | 消费者组自动均衡 | （无 — 忽略分片字段） | N 个并行 runner 加入同一个 `group_id`。Kafka 的消费者组协议自动分配分区。 |
| **postgres_cdc** | 不支持分片 | — | 仅限单实例。 |

### ShardStrategy 字段

`ParallelismConfig` 中的 `shard_strategy` 字段是**建议性/文档性**的 — 它描述预期的策略，但每个 Source 直接从其 config 中读取 `shard_index`/`shard_total` 并应用自己的逻辑。字段值：

| 值 | 预期用途 |
|----|---------|
| `round_robin` | File、HTTP、Redis（行/页/key 取模） |
| `partition` | Kafka（消费者组分区再均衡） |
| `id_range` | MySQL 批量（PK 取模分片） |
| `table` | MySQL CDC（表列表分区） |

### YAML 配置

```yaml
parallelism:
  count: 4                    # 并行实例数
  shard_strategy: "id_range"  # 建议性：数据如何分割
  shard_key: "id"             # 建议性：按哪个字段分片
  shard_total: 0              # 可选：覆盖总分片空间（用于 id_range）
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
