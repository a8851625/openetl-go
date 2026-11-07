# source/mysql_snapshot_cdc

## Purpose
Run an initial MySQL snapshot and continue from binlog CDC without a separate pipeline.

## Config Fields
- `host`, `user`, `database`: required source fields.
- `table` or `tables`: snapshot and CDC source tables.
- `pk_column`, `limit`: snapshot pagination.
- `server_id`, `server_id_base`, `shard_index`, `shard_total`, `consistent_snapshot_lock`: replication, sharding, and snapshot controls.
- `password`: secret.

## Record Shape
Snapshot rows and later CDC rows share the standard record shape with operation metadata.

## Schema Preflight
For multiple configured tables and `tables: ["*"]`, preflight expands and describes every base table rather than returning an empty schema or treating one table as representative. Missing/unreadable tables and explicitly listed snapshot tables without a usable single-column cursor key fail closed. Whole-database tables without such a key, or explicitly listed tables accepted with `skip_no_pk_tables: true`, produce `schema-multi-table-snapshot-skip` warnings and are declared CDC-only for historical data.

Multi-table preflight intentionally omits a single DDL preview. Dynamic per-record targets return `schema-multi-table-partial`; fixed single targets require explicit filtering/table mapping/schema normalization and still return `schema-multi-table-normalized-partial` because post-transform schemas are not inferred by this bounded contract.

## Checkpoint, DLQ, Idempotency
Snapshot pagination has a producer read-ahead cursor and a separate durable
cursor. The producer may fill the in-memory record channel ahead of the sink,
but only records represented by a successful sink boundary are included in
`CheckpointForRecords`; the durable cursor is applied after the checkpoint row
is saved. Linear and DAG execution preserve the complete source batch
represented by that boundary, which is important when one snapshot covers
multiple tables.

Numeric primary keys use a strict integer cursor. Ordered non-numeric keys such
as `VARCHAR` and `DATETIME` use a string cursor; `time.Time` values retain the
local MySQL wall-clock representation used by the connection. A snapshot
checkpoint also stores the binlog file/position captured at the snapshot
handoff. Restoring a snapshot checkpoint reuses that handoff rather than
capturing a newer master position, and CDC reconnects start from the last
acknowledged binlog position instead of handler read-ahead.

Finishing the producer-side snapshot does not by itself persist `phase: cdc`:
snapshot rows may still be buffered before the sink. The durable phase changes
to CDC only when an actual CDC record has crossed the sink/checkpoint boundary;
until then, restart safely reopens the snapshot cursor, consumes any empty tail
pages, and continues from the saved handoff.

Malformed JSON, unsupported phases, missing cursor values, invalid numeric
encodings, and missing snapshot/CDC handoff positions fail closed. A failed
checkpoint generation or acknowledgement blocks later advancement; restart
replays from the last durable boundary. Downstream replay must be absorbed by
upsert/versioned sinks.

## Fits
First-time MySQL table migration followed by continuous sync.

## Does Not Fit
Workloads where source locks are unacceptable and no consistent snapshot strategy is available.

The default delivery contract remains at-least-once. Snapshot crash/restart,
CDC restart, checkpoint reset, and heterogeneous numeric/string-PK paths are
covered by the snapshot E2E scripts listed below; exact cross-source/sink
transactions are not claimed.

## Keyless Tables and Targeted Resync

Two operational gaps used to require a full re-snapshot of the whole pipeline:

**Keyless tables** stay skipped by default (`no_pk_snapshot: skip`): their
historical rows never reach the sink, only later CDC changes. `no_pk_snapshot:
hash` instead streams the table inside the consistent snapshot transaction and
writes a synthetic `_row_hash` key (sha256 over the row's sorted column values,
with NULL hashed distinctly from the empty string). The key travels through
`Metadata.PrimaryKeyColumns` / `Metadata.Key`, so a sink with
`pk_columns_from_metadata: true` upserts on it and re-snapshot/restart is
idempotent. Trade-offs, all deliberate and documented: rows whose column values
are identical collapse into one; the scan is not resumable, so an interrupted
table restarts from its first row (correct because writes are idempotent); and
the read holds the snapshot transaction's undo log for the table's duration.
Spec validation rejects `no_pk_snapshot: hash` without
`sink.config.pk_columns_from_metadata: true`.

**Targeted resync** (`POST /api/v2/pipelines/{id}/resync` with
`{"tables":["orders"]}`) re-runs the snapshot phase for the named tables only.
The endpoint rewrites the checkpoint: the named tables' cursors are dropped, the
existing binlog anchor is reused as the new snapshot handoff, and the phase goes
back to `snapshot`. Unnamed tables keep their cursors and emit nothing. Because
the older anchor is reused, re-read rows never order after live CDC rows, so a
versioned sink can never let re-snapshot data overwrite newer state. The
pipeline must be quiescent (otherwise HTTP 409) and the sink must absorb
re-delivered rows.

## Example
```yaml
source:
  type: mysql_snapshot_cdc
  config:
    host: mysql
    user: sync
    password: "${MYSQL_PASSWORD}"
    database: app
    table: orders
    pk_column: id
```

## Evidence
Unit evidence: `internal/etl/source/mysql_snapshot_cdc_checkpoint_test.go`,
linear/DAG batch checkpoint tests, multi-table preflight tests in
`internal/etl/server/pipelines_preflight_test.go`, and source package
race/static checks.
Targeted resync and the keyless-table snapshot are covered by
`internal/etl/server/resync_test.go`,
`internal/etl/source/mysql_snapshot_cdc_nopk_test.go`, and
`hack/e2e-data-integrity.sh`.
Path evidence: `hack/e2e-snapshot-cdc.sh`,
`hack/e2e-snapshot-cdc-crash.sh`, and
`hack/e2e-snapshot-cdc-heteropk.sh`. The broader ClickHouse path matrix is
covered by `hack/e2e-snapshot-cdc-clickhouse.sh`, including safe post-reset
phase transition, schema drift, checkpoint reset, outage, and DLQ replay. All
four scripts passed on 2026-08-08. Path contract:
`mysql_snap_cdc__ch_rmt`.
