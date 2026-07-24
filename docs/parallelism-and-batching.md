# Parallelism & Batching Design

## Overview

This document describes the parallelism (sharding) and batching architecture of the ETL platform, including per-source sharding strategies, configurable batch parameters, and checkpoint semantics.

## Parallelism Architecture

### How It Works

When a pipeline spec includes `parallelism.sharding.logical_shards > 1` (or legacy `parallelism.count > 1`), the platform creates one shard-scoped `Runner` per logical shard. Each instance:

1. Gets a **deep-copied spec** with `shard_index` and `shard_total` injected into `source.config`
2. Has its own **isolated checkpoint namespace**: `{pipeline-name}.shard-{N}`
3. Runs independently with its own read loop, transform chain, and sink writer
4. Shares the same sink target (e.g., same MySQL table, same Kafka topic)

```
Pipeline spec (logical_shards=4)
├── Shard 0: Source(shard_index=0) → Transforms → Sink  [checkpoint: name.shard-0]
├── Shard 1: Source(shard_index=1) → Transforms → Sink  [checkpoint: name.shard-1]
├── Shard 2: Source(shard_index=2) → Transforms → Sink  [checkpoint: name.shard-2]
└── Shard 3: Source(shard_index=3) → Transforms → Sink  [checkpoint: name.shard-3]
```

`parallelism.sharding.logical_shards` is stable data ownership and should not be changed just to tune throughput. `parallelism.execution.max_active_shards` controls how many logical shards may run at once in a standalone process. In master/worker mode, **all** linear pipelines are dispatched as shard tasks (including `logical_shards=1` as a single continuous placement task) and effective concurrency is bounded by worker slots.

### Streaming placement vs multi-shard scale-out

- **Unsharded streaming** (`logical_shards=1`, default for kafka/cdc): one placement. Standalone runs it locally; master dispatches one continuous worker task. Validate warns that this is not multi-active HA.
- **Multi-shard streaming** (Kafka consumer-group shards, multi-table `mysql_cdc`, etc.): N tasks / N runners. Safe Kafka default is `logical_shards = topic partition count`.
- **Ops split without code**: multiple standalone instances, each owning a subset of pipeline specs, shared MySQL metadata.
- Checkpoint keys: multi-shard uses `{name}.shard-{N}`; single-shard placement keeps the plain `{name}` key so promoting standalone CDC to master-worker does not orphan checkpoints.

### Per-Source Sharding Strategies

| Source | Sharding Mechanism | Config Fields Read | Description |
|--------|-------------------|-------------------|-------------|
| **file** | Line modulo | `shard_index`, `shard_total` | Shard i gets lines where `lineNum % total == i`. All shards read the same file but emit different lines. Treat checkpoint resume as at-least-once with possible replay until stable global-position sharding is implemented. |
| **http** | Page modulo | `shard_index`, `shard_total` | Shard i fetches pages `i+1, i+1+total, i+1+2*total, ...`. Each shard makes independent HTTP requests. |
| **redis** | Key hash modulo | `shard_index`, `shard_total`, `shard_key` | Use `sharding.strategy: hash_modulo` with `sharding.key: _key` or `@key`. All shards SCAN the same keyspace, but each shard only processes keys where `hash(key) % total == index`. Correct but scans overlap. |
| **mysql_batch** | PK modulo | `shard_index`, `shard_total` | `WHERE MOD(pk, total) = idx` in the SQL query. Each shard gets a disjoint partition of rows. |
| **mysql_cdc** | Table partition + server_id isolation | `shard_index`, `shard_total` | Shard i processes tables `[i, i+total, ...]`. Each shard gets a unique binlog `server_id`. Single-table CDC is a no-op. |
| **mysql_snapshot_cdc** | PK modulo (snapshot) + table partition (CDC) | `shard_index`, `shard_total` | During snapshot: `WHERE MOD(pk, total) = idx`. During CDC: table partition + unique server_id. |
| **kafka** | Consumer group auto-balance | (none — ignores shard fields) | N parallel runners join the same `group_id`. Kafka's consumer group protocol distributes partitions automatically. **Set logical shards ≤ topic partition count** — Kafka assigns at most one partition per consumer, so extra shards stay idle. |
| **postgres_cdc** | Not sharded | — | Single-instance only. |

### Sharding And Execution Fields

New specs should separate stable sharding from runtime concurrency:

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

`execution.sink_concurrency`, when set to a positive value, limits concurrent
`sink.Write` calls across shard runners in the same standalone process. It does
not serialize transforms or checkpoint saves, and it is not a cross-process
distributed lease.

`execution.transform_workers`, when greater than 1, parallelizes per-record
transform work inside a linear runner's current batch and restores input order
before writing to the sink. If the transform chain contains a `BatchTransform`,
`Flusher`, `StateSnapshotter`, or state metrics provider, the runner falls back
to the serial path so window/deduplicate/join style state boundaries remain
ordered.

`execution.source_concurrency` is accepted for forward compatibility but is not
active in the current runner. Source-side parallelism is driven by
`sharding.logical_shards` plus `execution.max_active_shards`.

The legacy fields are still supported and mapped during defaults:

```yaml
parallelism:
  count: 4
  shard_strategy: "id_range"
  shard_key: "id"
  shard_total: 0
```

Legacy mapping:

- `sharding.logical_shards = shard_total` when set, otherwise `count`
- `execution.max_active_shards = count`
- `sharding.strategy = shard_strategy`
- `sharding.key = shard_key`

### DAG Execution Workers

DAG pipelines do not use linear `parallelism.sharding` to create runner shards.
They can use top-level `execution.workers` to parallelize route/transform work
inside one DAG executor:

```yaml
execution:
  workers: 4
  batch_size: 1000
  backpressure_buffer: 200
```

Worker results are re-ordered per source before sink batching. Sink writes and
checkpoint saves still happen in the single aggregator, so a higher
`execution.workers` value improves CPU/IO-bound transform routing without
allowing checkpoint positions to regress.

### Kafka Parallelism Caveat

For Kafka sources, `shard_strategy` is ignored — all logical shards join the
same consumer group and Kafka assigns topic partitions to them. Because the
Kafka consumer-group protocol assigns at most one partition to each consumer
within a group, the **effective parallelism is bounded by the topic's partition
count**:

- If logical shards ≤ topic partitions: all shards receive data.
- If logical shards > topic partitions: the extra shards stay idle (no data, no
  error). Spec validate warns when `source.config.topic_partitions` is set and
  lower than `logical_shards`; preflight warns using **live** broker metadata
  and recommends `logical_shards = NumPartitions` when currently unsharded.
- Optional `source.config.topic_partitions` is a static validate-only hint when
  brokers are unreachable; production should rely on preflight metadata.

Recommended safe multi-shard Kafka CDC:

```yaml
source:
  type: kafka
  config:
    brokers: ["kafka:9092"]
    topic: dbserver.inventory.orders
    group_id: openetl-orders-cdc
    # topic_partitions: 4   # optional offline validate hint
parallelism:
  sharding:
    logical_shards: 4   # ≤ PartitionCount; prefer equal to partitions
```

Check the topic's partition count before raising `parallelism.sharding.logical_shards`:

```bash
kafka-topics.sh --describe --topic <topic>
# PartitionCount must be ≥ parallelism.sharding.logical_shards
```

To raise the partition count (Kafka only supports increasing, never decreasing):

```bash
kafka-topics.sh --alter --topic <topic> --partitions <new-count>
```

## Batching Architecture

### Batch Assembly Flow

```
readLoop ──(single records)──► [bounded channel: backpressure_buffer]
                                        │
                                   writeLoop
                                        │
                    ┌───────────────────┼───────────────────┐
                    │                   │                   │
              batch_size reached   flush_interval_ms    channel close
                    │                   │               (EOF/shutdown)
                    └───────────────────┼───────────────────┘
                                        │
                                   writeBatch()
                                        │
                              1. Transform chain
                              2. Sink.Write(batch)
                              3. Checkpoint save (throttled)
```

### Configurable Parameters

| Parameter | YAML field | Default | Description |
|-----------|-----------|---------|-------------|
| **Batch Size** | `batch_size` | 1000 | Maximum records per batch. When reached, the batch is flushed immediately. |
| **Flush Interval** | `flush_interval_ms` | 1000 (1s) | Maximum time between flushes. Partial batches are flushed at this interval even if `batch_size` isn't reached. |
| **Checkpoint Interval** | `checkpoint_interval_sec` | 30 | Minimum time between checkpoint saves. Reduces checkpoint store load for high-throughput pipelines. Always saves on EOF/shutdown regardless. |
| **Backpressure Buffer** | `backpressure_buffer` | 100 | Bounded channel size between readLoop and writeLoop. When full, the source reader blocks, exerting backpressure. |

### Checkpoint Throttling

Checkpoints are NOT saved on every batch flush. Instead:

1. After each successful `Sink.Write()`, the runner checks if enough time has passed since the last checkpoint save (`checkpoint_interval_sec`)
2. If yes, or if 10 uncheckpointed batches have accumulated, a checkpoint is saved
3. On EOF/shutdown, a checkpoint is **always** saved (forced)
4. This reduces checkpoint store I/O for high-throughput pipelines while maintaining at-least-once semantics

**Implication**: On crash recovery, up to `checkpoint_interval_sec` worth of batches may be replayed. This is acceptable for at-least-once delivery; sinks should be idempotent (e.g., MySQL upsert mode).

### YAML Configuration

```yaml
batch_size: 1000                  # Max records per batch
flush_interval_ms: 2000           # Flush every 2 seconds (default 1000ms)
checkpoint_interval_sec: 60       # Checkpoint every 60 seconds (default 30)
backpressure_buffer: 200          # Buffer 200 records between read/write (default 100)
```

### Delivery Semantics

- **At-least-once**: Checkpoints are saved AFTER successful sink writes. On crash, the last uncheckpointed batch is replayed.
- **Kafka offset commit**: Offsets are committed via `CheckpointForRecord()` only after the sink batch succeeds.
- **MySQL transaction atomicity**: One `BEGIN/COMMIT` transaction per batch. Internal chunking at 500 rows per INSERT statement.
- **Cross-sink atomicity**: NOT supported. In fanout scenarios, if Sink A succeeds but Sink B fails, the checkpoint advances based on the successful write.

## Parallelism × Batching Interaction

Each shard has its own independent batching:
- Shard 0 might flush every 500ms (high throughput)
- Shard 1 might flush every 2s (slow source)
- Checkpoints are per-shard, so a slow shard doesn't affect fast shards' recovery

The `batch_size`, `flush_interval_ms`, `checkpoint_interval_sec`, and `backpressure_buffer` values are shared across all shards (same spec), but each shard enforces them independently.
