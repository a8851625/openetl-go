# Parallelism & Batching Design

## Overview

This document describes the parallelism (sharding) and batching architecture of the ETL platform, including per-source sharding strategies, configurable batch parameters, and checkpoint semantics.

## Parallelism Architecture

### How It Works

When a pipeline spec includes `parallelism.count > 1`, the platform creates N independent `Runner` instances. Each instance:

1. Gets a **deep-copied spec** with `shard_index` and `shard_total` injected into `source.config`
2. Has its own **isolated checkpoint namespace**: `{pipeline-name}.shard-{N}`
3. Runs independently with its own read loop, transform chain, and sink writer
4. Shares the same sink target (e.g., same MySQL table, same Kafka topic)

```
Pipeline spec (count=4)
├── Shard 0: Source(shard_index=0) → Transforms → Sink  [checkpoint: name.shard-0]
├── Shard 1: Source(shard_index=1) → Transforms → Sink  [checkpoint: name.shard-1]
├── Shard 2: Source(shard_index=2) → Transforms → Sink  [checkpoint: name.shard-2]
└── Shard 3: Source(shard_index=3) → Transforms → Sink  [checkpoint: name.shard-3]
```

### Per-Source Sharding Strategies

| Source | Sharding Mechanism | Config Fields Read | Description |
|--------|-------------------|-------------------|-------------|
| **file** | Line modulo | `shard_index`, `shard_total` | Shard i gets lines where `lineNum % total == i`. All shards read the same file but emit different lines. Byte-offset checkpoints remain valid. |
| **http** | Page modulo | `shard_index`, `shard_total` | Shard i fetches pages `i+1, i+1+total, i+1+2*total, ...`. Each shard makes independent HTTP requests. |
| **redis** | Key hash modulo | `shard_index`, `shard_total` | All shards SCAN the same keyspace, but each shard only processes keys where `hash(key) % total == index`. Correct but scans overlap. |
| **mysql_batch** | PK modulo | `shard_index`, `shard_total` | `WHERE MOD(pk, total) = idx` in the SQL query. Each shard gets a disjoint partition of rows. |
| **mysql_cdc** | Table partition + server_id isolation | `shard_index`, `shard_total` | Shard i processes tables `[i, i+total, ...]`. Each shard gets a unique binlog `server_id`. Single-table CDC is a no-op. |
| **mysql_snapshot_cdc** | PK modulo (snapshot) + table partition (CDC) | `shard_index`, `shard_total` | During snapshot: `WHERE MOD(pk, total) = idx`. During CDC: table partition + unique server_id. |
| **kafka** | Consumer group auto-balance | (none — ignores shard fields) | N parallel runners join the same `group_id`. Kafka's consumer group protocol distributes partitions automatically. |
| **postgres_cdc** | Not sharded | — | Single-instance only. |

### ShardStrategy Field

The `shard_strategy` field in `ParallelismConfig` is **advisory/documentation** — it describes the intended strategy but each source reads `shard_index`/`shard_total` directly from its config and applies its own logic. The field values:

| Value | Intended Use |
|-------|-------------|
| `round_robin` | File, HTTP, Redis (line/page/key modulo) |
| `partition` | Kafka (consumer group partition rebalance) |
| `id_range` | MySQL batch (PK modulo sharding) |
| `table` | MySQL CDC (table-list partitioning) |

### YAML Configuration

```yaml
parallelism:
  count: 4                    # Number of parallel instances
  shard_strategy: "id_range"  # Advisory: how data is split
  shard_key: "id"             # Advisory: which field to shard on
  shard_total: 0              # Optional: override total shard space (for id_range)
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
