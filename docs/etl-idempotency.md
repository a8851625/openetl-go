# ETL Idempotency Contract

The runtime provides at-least-once delivery. A checkpoint is committed only after a batch is successfully written to the sink, so a crash can replay the last uncommitted records. Production pipelines must therefore choose a sink mode that can tolerate duplicates or replayed CDC events.

## Common Rules

- Every CDC/snapshot+CDC pipeline should preserve a stable primary key in `record.data`.
- Batch and snapshot jobs should either write to an idempotent target mode or write to a fresh partition/object prefix.
- Non-idempotent sinks are acceptable only for append-only audit/event streams where duplicates are expected and downstream consumers deduplicate.
- DLQ replay re-applies transforms and writes to the same sink; the sink mode must also tolerate replay.

## Sink Contracts

| Sink | Recommended Mode | Duplicate Behavior | Notes |
| --- | --- | --- | --- |
| MySQL/TiDB | `batch_mode: upsert` with `pk_columns` | Replayed rows overwrite the same primary key | Required for CDC and crash recovery when using mutable tables. Plain insert is only safe for append-only unique events. |
| ClickHouse | ReplacingMergeTree-compatible table with `_version`, or ETL `auto_create: true` | Later versions collapse with `FINAL`; raw duplicate rows may exist before merge | Queries that require exact current state should use `FINAL` or downstream materialization. Deletes rely on table design/tombstone strategy. |
| Doris | Doris Unique Key table with `batch_mode: upsert` and stable `pk_columns` | Stream Load retries use deterministic labels and replayed rows merge on the Unique Key | Production CDC/upsert requires the configured `pk_columns` to match the Doris Unique Key. DELETE uses the MySQL protocol; mixed write/delete batches are rejected unless `allow_mixed_cdc_non_atomic: true` is set. |
| Kafka sink | Producer writes are at-least-once | Duplicate messages can appear | Use deterministic message keys and consumer-side idempotency. Kafka exactly-once transactions are not implemented yet. |
| Elasticsearch | Stable document `_id` derived from primary key | Replayed documents replace the same ID | Partial bulk item errors expose failed record indexes, so the runner writes only failed records to DLQ and does not re-write accepted records. |
| S3/OSS/file sink | Content-addressed object/file key per flushed batch | Replaying the identical batch overwrites the same key within the same date prefix; changed batch boundaries produce different keys | Use a stable prefix per backfill job and treat the object set as the manifest until first-class manifests are available. |
| Local file sink | Content-addressed file path per flushed batch | Replaying the identical batch overwrites the same file within the same date prefix | Suitable for extract/debug flows; consumers must deduplicate if batches are split differently across runs. |

## Source Guidance

| Source | Recommended Sink Contract |
| --- | --- |
| `mysql_cdc` | Upsert/merge target keyed by source primary key. |
| `mysql_snapshot_cdc` | Upsert/merge target because snapshot rows can be replayed and CDC can overlap around the captured binlog position. |
| `mysql_batch` | Upsert for mutable target tables; append-only file/S3 is acceptable for extracts. |
| `file` | Depends on file semantics; if source files are reprocessed, use deterministic keys downstream. |
| `http` | Cursor/page checkpointing reduces replay, but sink must still tolerate duplicate pages after crash. |
| `kafka` | Use deterministic sink keys or consumer deduplication. |

## Runtime Guarantees

- Checkpoints advance after successful sink write.
- Filtered records can advance checkpoints because they are intentionally skipped.
- Failed records are written to DLQ with `error_class` when classification is possible.
- Transient and unknown errors are retried; config/auth/schema/data/programming errors fail fast into DLQ or fail the operation.

## Not Yet Guaranteed

- Cross-sink atomic fanout is not implemented.
- Kafka transactional exactly-once is not implemented.
- First-class S3/file manifest files are not implemented; current protection is deterministic content-addressed object/file keys.
