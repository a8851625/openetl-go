# sink/clickhouse

## Purpose

Write batch or CDC records into ClickHouse through native or HTTP protocol, with optional auto-create,
schema drift handling, and replay-safe source-ordered replacement for mutable rows.

## Write modes

`version_mode: source_order` is the default for mutable/replayed data. Every INSERT/UPDATE/DELETE must
carry a numeric `core.SourceOrder`; the sink writes it to a writable `UInt64` `version_column` (default
`_version`) and writes live/tombstone state to a writable `UInt8` `delete_column` (default
`_is_deleted`). The target engine must use both as its final arguments:

```sql
ENGINE = ReplacingMergeTree(_version, _is_deleted)
ORDER BY (business_key)
```

UPDATE inserts a newer live row. A primary-key-changing UPDATE inserts the new key and a same-version
tombstone for the old complete key. DELETE inserts a tombstone; it does not issue an asynchronous
mutation. Consequently, an older delayed INSERT cannot resurrect a row deleted by a newer event.
Use `FINAL` (or a correctly designed downstream materialization) when reading current state.

`version_mode: append` creates/requires a plain `MergeTree`, does not add version/tombstone columns,
and accepts INSERT only. Runtime rejects UPDATE/DELETE. Use it for append-only inputs and derived
INSERT-only outputs such as the current `window` aggregate, which has no single connector-owned order.

## Config Fields

- `host`, `database`: required target fields. `table` is optional when records carry source table metadata.
- `port`, `protocol`, `user`/`username`, `password`: connection fields.
- `version_mode`, `version_column`, `delete_column`, `pk_columns`, `pk_columns_from_metadata`:
  identity/order controls.
- `auto_create`, `schema_drift`, `ddl_policy`, `source_dialect`, `table_template`: schema and routing controls.
- `compression`, `async_insert`, `async_insert_wait`, `optimize_interval_sec`, `use_final`: write/read tuning.
  Production async inserts must keep `async_insert_wait: true` so sink acknowledgement means ClickHouse
  completed the insert; this is still at-least-once, not a source/checkpoint/sink transaction.

## Record Shape

### Source-order mapping

The sink does not read wall clock time or use a process-local counter:

- MySQL CDC: binlog file sequence + position.
- MySQL snapshot+CDC snapshot: the binlog handoff captured before the consistent snapshot; its numeric
  or text pagination cursor never becomes the version. CDC then uses the same binlog domain.
- PostgreSQL CDC: LSN (initial snapshot is rejected because it has no durable ordered row cursor).
- Kafka: partition + offset, scoped to one partition.
- MySQL batch: only a non-negative numeric cursor.

File/HTTP/Redis, ordered text batch cursors, derived window aggregates, missing/overflowed positions,
and unknown sources fail validation/preflight or the runtime write. There is no wall-clock fallback.

## Checkpoint, DLQ, Idempotency

Delivery remains checkpointed at-least-once. A crash after ClickHouse acknowledgement and before
checkpoint persistence can replay the batch; source-owned versions and tombstones make the replay
converge for one business key/order domain. Failed writes follow retry/DLQ, and a delayed DLQ replay
keeps its original source position.

Metadata-PK replay must also carry a complete declared Key. New DLQ rows freeze the raw payload,
primary-key declaration, format contract, before-image state, and target coordinates. Replay rebuilds
an incomplete Key only from those frozen facts and validates the transformed record again before this
sink. Historical rows can use the per-record `PUT .../dlq/{pipeline}/{id}/identity` gate only when
their stored declaration exactly matches an explicit static `pk_columns` safety set; partial keys and
legacy primary-key-changing UPDATEs remain quarantined. Sink acknowledgement is persisted as
`sink_acked` before DLQ deletion, so a delete failure is cleanup-only on restart. A failure before that
checkpoint may duplicate the write and remains covered by source-order replacement.

Kafka ordering is only per partition. Keep the same business key on a stable partition and do not
change partition count without a migration. MySQL binlog coordinates are comparable only within one
lineage. After `RESET MASTER`, PITR, or failover to a lower file sequence, rebuild/switch the target to
a new version domain before restarting; checkpoint reset or resnapshot alone cannot compare old and
new lineages safely.

## Existing-table migration

Opening `source_order` against legacy `_version Int64`, missing `_is_deleted`, plain/single-argument
ReplacingMergeTree, or materialized order columns fails closed. Do not `ALTER` the type in place and
mix historical wall-clock values with source positions. Create a new table with writable `UInt64` /
`UInt8` columns and two-argument ReplacingMergeTree, rebuild/backfill from a documented single source
lineage, verify `FINAL` by business key, then atomically switch the pipeline/table. Rollback switches
back to the untouched legacy table; it must not delete new tombstones.

## Fits
MySQL CDC/snapshot+CDC -> ClickHouse and Kafka detail/aggregate landing.

## Does Not Fit
Cross-sink exactly-once fanout.

## Example

```yaml
sink:
  type: clickhouse
  config:
    host: clickhouse
    port: 9000
    database: default
    table: orders
    auto_create: true
    schema_drift: add_columns
    pk_columns: [id]
    version_mode: source_order
    version_column: _version
    delete_column: _is_deleted
```

## Evidence

`hack/e2e-clickhouse-replay-ordering.sh` certifies MySQL snapshot+CDC/native and Kafka/HTTP delayed
DLQ replay plus checkpoint reset, UPDATE ordering, DELETE non-resurrection, target schema, restart,
schema drift, and ClickHouse outage/replay. `hack/e2e-kafka-multitable-clickhouse.sh` covers dynamic
multi-table keys/tombstones. `hack/e2e-kafka-canal-identity.sh` covers frozen identity context,
legacy composite-key repair, partial/key-changing/unknown-contract quarantine, and process restart.
Focused unit gates live in `clickhouse_version_test.go`, `dlq_replay_identity_test.go`, and
`clickhouse_order_preflight_test.go`. Baseline connector coverage remains in
`hack/e2e-clickhouse.sh` and `hack/e2e-snapshot-cdc-clickhouse.sh`.
