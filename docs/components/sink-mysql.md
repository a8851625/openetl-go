# sink/mysql

## Purpose
Write records into MySQL with insert or upsert batch modes.

## Config Fields
- `host`, `user`, `database`, `table`: required target fields.
- `port`, `password`, `tls`: connection fields.
- `batch_mode`, `pk_columns`, `pk_columns_from_metadata`, `auto_create`, `column_types`, `schema_drift`, `ddl_policy`: idempotency and schema controls.
- `column_types`: optional map of column → target DDL for `auto_create` / `add_columns` (e.g. `{deleted: "TINYINT(1)"}`). Highest priority over source schema and sample inference.
- Auto-create type resolution order: **column_types override → source SchemaDescriptor / Debezium field schema → sample+name inference**.
- `pre_write`: optional pre-write action block `{action, condition, params}`. Runs inside the batch transaction before inserts. `action`: `delete` (requires `condition`), `truncate`, or `truncate_partition` (requires `condition`). Idempotent for batch (delete-then-rewrite on checkpoint reset), dangerous for CDC/streaming — preflight flags truncate/truncate_partition on CDC sources as error-level.

## Record Shape
Writes record `data` columns. CDC delete/update behavior depends on operation and sink mode.

## Checkpoint, DLQ, Idempotency
Use `batch_mode: upsert` and stable `pk_columns` for replay absorption. In Debezium multi-table CDC, `pk_columns_from_metadata: true` can derive per-table keys from `record.metadata.key`, including DELETE predicates. `batch_mode: increment` is additive and checkpoint reset intentionally accumulates again. `pre_write` delete-then-rewrite runs once per pipeline start, so checkpoint reset replays the cleanup before rewriting the target partition. Batch writes are transaction-bounded.

## Fits
ODS upsert tables and MySQL-to-MySQL sync.

## Does Not Fit
Append-only CDC targets without deduplication.

## Example
```yaml
sink:
  type: mysql
  config:
    host: mysql
    user: sync
    password: "${MYSQL_PASSWORD}"
    database: ods
    table: orders
    batch_mode: upsert
    pk_columns: ["id"]
```

## Evidence
Covered by `hack/e2e-cdc-mysql.sh`, `hack/e2e-relational-write-modes.sh`, `hack/e2e-debezium-mysql.sh`, and MySQL sink tests. `hack/e2e-debezium-mysql.sh` includes a Debezium key -> `metadata.key` -> `pk_columns_from_metadata` multi-table DELETE fixture, including a composite key table; it was rerun successfully on 2026-07-07 with the local default Podman machine. `hack/e2e-relational-write-modes.sh` validates `pre_write` delete+rewrite after checkpoint reset, additive `increment` replay behavior, and MySQL VIRTUAL/STORED generated columns being skipped on write. Preflight opens the target, validates table metadata when reachable, emits DDL preview, and reports field-level schema issues.
