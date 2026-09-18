# ETL Idempotency Contract

The runtime provides at-least-once delivery. A checkpoint is committed only after a batch is successfully written to the sink, so a crash can replay the last uncommitted records. For Kafka and PostgreSQL CDC, the external source cursor is acknowledged only after that durable checkpoint save; an acknowledgement failure stops the pipeline and replays from the saved boundary. Production pipelines must therefore choose a sink mode that can tolerate duplicates or replayed CDC events.

Path-level write mode, business key, evidence scripts, and RPO/RTO declarations are catalogued in [path-contract.md](./path-contract.md) (`GET /api/v2/paths/contracts`).

## Common Rules

- Every CDC/snapshot+CDC pipeline should preserve a stable primary key in `record.data`.
- Batch and snapshot jobs should either write to an idempotent target mode or write to a fresh partition/object prefix.
- Non-idempotent sinks are acceptable only for append-only audit/event streams where duplicates are expected and downstream consumers deduplicate.
- DLQ replay re-applies transforms and writes to the same sink; the sink mode must also tolerate replay.

## Sink Contracts

| Sink | Recommended Mode | Duplicate Behavior | Notes |
| --- | --- | --- | --- |
| MySQL/TiDB | `batch_mode: upsert` with `pk_columns` | Replayed rows overwrite the same primary key | Required for CDC and crash recovery when using mutable tables. Plain insert is only safe for append-only unique events. |
| ClickHouse | `version_mode: source_order`, stable `pk_columns`, writable `_version UInt64` + `_is_deleted UInt8`, `ReplacingMergeTree(_version, _is_deleted)` | Original source positions decide the winner even when an older event arrives later; tombstones prevent an older INSERT from reviving a deleted key | Queries that require current state use `FINAL` or a proven materialization. `append` is explicit INSERT-only `MergeTree`, not a mutable-data fallback. |
| Doris | Doris Unique Key table with `batch_mode: upsert` and stable `pk_columns` | Stream Load retries use deterministic labels and replayed rows merge on the Unique Key | Production CDC/upsert requires the configured `pk_columns` to match the Doris Unique Key. DELETE uses the MySQL protocol; mixed write/delete batches are rejected unless `allow_mixed_cdc_non_atomic: true` is set. |
| Kafka sink | Producer writes are at-least-once | Duplicate messages can appear | Use deterministic message keys and consumer-side idempotency. Kafka exactly-once transactions are not implemented yet. |
| Elasticsearch | Stable document `_id` derived from primary key | Replayed documents replace the same ID | Partial bulk item errors expose failed record indexes, so the runner writes only failed records to DLQ and does not re-write accepted records. |
| S3/OSS/file sink | Content-addressed object/file key per flushed batch | Replaying the identical batch overwrites the same key within the same date prefix; changed batch boundaries produce different keys | Use a stable prefix per backfill job and treat the object set as the manifest until first-class manifests are available. |
| Local file sink | Content-addressed file path per flushed batch | Replaying the identical batch overwrites the same file within the same date prefix | Suitable for extract/debug flows; consumers must deduplicate if batches are split differently across runs. |

## Source Guidance

| Source | Recommended Sink Contract |
| --- | --- |
| `mysql_cdc` | Upsert/merge target keyed by source primary key. For ClickHouse, binlog file/position supplies the version within one binlog lineage. |
| `mysql_snapshot_cdc` | Upsert/merge target because snapshot rows can be replayed. ClickHouse versions snapshot rows with the captured binlog handoff, then uses the same order domain for CDC. |
| `mysql_batch` | Upsert for mutable target tables; append-only file/S3 is acceptable for extracts. |
| `file` | Depends on file semantics; if source files are reprocessed, use deterministic keys downstream. |
| `http` | Cursor/page checkpointing reduces replay, but sink must still tolerate duplicate pages after crash. |
| `kafka` | Use deterministic sink keys or consumer deduplication. ClickHouse order is per partition, so the same business key must stay on one stable partition. |

## ClickHouse source-order boundary

`source_order` deliberately writes every replayed record; ClickHouse chooses the maximum source-owned
version per business key. It never substitutes arrival time. This covers crash replay, DLQ replay, and
checkpoint reset only while positions remain in one comparable domain:

- MySQL `RESET MASTER`, PITR, or moving to a server whose binlog sequence/position goes backwards starts
  a new lineage. Rebuild or switch the ClickHouse target/version domain before resuming. A checkpoint
  reset or `cdc_on_binlog_purged: resnapshot` does not make old/new binlog coordinates comparable.
- Kafka `(partition, offset)` is ordered only within a partition. Route a business key with a stable
  Kafka key, keep it on the same partition, and treat partition-count changes as a target migration.
- `mysql_snapshot_cdc` pagination keys—including text keys—are not event versions. Snapshot rows use
  the binlog handoff captured before the consistent read, so a later snapshot in the same lineage can
  supersede already-written CDC state.
- Stateful derived outputs such as `window` do not own one input position. Use an explicitly INSERT-only
  append landing table unless that transform publishes and tests a separate revision contract.

Legacy tables with `Int64` wall-clock versions or one-argument ReplacingMergeTree cannot be mixed with
this contract. Build a new two-argument table, reload from one documented source lineage, reconcile
`FINAL` values by business key, and then switch. Keep the old table intact as the rollback boundary.

## ClickHouse write-ack and dedup token (CH-C1)

The ClickHouse sink exposes sink-native commit metadata for every fully acknowledged `Write` batch:
a deterministic dedup token derived from `pipeline key + first/last source position + batch sequence +
row count` (no wall-clock or random component). The same batch replayed after a crash re-derives the
same token. The token is carried with both protocols (native query setting `insert_dedup_token`, HTTP
URL parameter) when the server supports it, and is always recorded in the checkpoint envelope under
`sink_commit.native` regardless of server support (`dedup_mode: server` vs `record_only`).

Boundary semantics:

- **Crash window (sink acked, checkpoint not committed)**: replay re-sends the same batch with the same
  token. Servers that honor `insert_dedup_token` drop the duplicate block; otherwise ReplacingMergeTree
  absorbs it by source-order version. Both paths converge to an identical `FINAL` state — this
  equivalence is certified by e2e on both protocols (`ch_dedup_crash_window`).
- **Tombstones and physical mutations**: DELETE tombstones and UPDATE-driven mutations are not covered
  by insert dedup. They rely on the source-order version contract above; a replayed tombstone is
  idempotent because it re-writes the same version, not because of the token.
- **Write failures**: transport failures after a possible server commit classify as `ack unknown`
  (transient — replay-safe); explicit pre-write server rejections classify as `not acked` (auth/schema/
  data — retry or DLQ). A failed `Write` invalidates the pending boundary: `SinkCommitMetadata` then
  errors, which blocks checkpoint advancement — the runner never persists an unverifiable boundary.
- **Non-provider sinks** (MySQL, PostgreSQL, Kafka, ...): the checkpoint envelope keeps the generic
  `sink_commit` block without `native` metadata; their duplicate absorption stays as documented in
  their own sections.

## Runtime Guarantees

- Checkpoints advance after successful sink write.
- Filtered records can advance checkpoints because they are intentionally skipped.
- Failed records are written to DLQ with `error_class` when classification is possible.
- New DLQ rows freeze raw payload, primary-key declaration, format contract, before-image state, and
  source/target coordinates. Metadata-PK replay reconstructs identity only from those historical facts,
  then validates the transformed record again; unprovable rows remain repair-required/quarantined.
- DLQ replay persists `sink_acked` after the sink accepts a row and before deleting it. Failure before
  that replay checkpoint may duplicate the write; failure after it is cleanup-only on retry. The replay
  checkpoint never advances the source checkpoint.
- Transient and unknown errors are retried; config/auth/schema/data/programming errors fail fast into DLQ or fail the operation.
- ClickHouse source-order mode rejects missing/overflowed source positions and incompatible target
  schemas; it does not fall back to a wall-clock counter.

## Not Yet Guaranteed

- Cross-sink atomic fanout is not implemented.
- Kafka transactional exactly-once is not implemented.
- First-class S3/file manifest files are not implemented; current protection is deterministic content-addressed object/file keys.
