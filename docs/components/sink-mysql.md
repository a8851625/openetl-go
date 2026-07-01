# sink/mysql

## Purpose
Write records into MySQL with insert or upsert batch modes.

## Config Fields
- `host`, `user`, `database`, `table`: required target fields.
- `port`, `password`, `tls`: connection fields.
- `batch_mode`, `pk_columns`, `auto_create`, `schema_drift`, `ddl_policy`: idempotency and schema controls.

## Record Shape
Writes record `data` columns. CDC delete/update behavior depends on operation and sink mode.

## Checkpoint, DLQ, Idempotency
Use `batch_mode: upsert` and stable `pk_columns` for replay absorption. Batch writes are transaction-bounded.

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
Covered by `hack/e2e-cdc-mysql.sh`, `hack/e2e-debezium-mysql.sh`, and MySQL sink tests. Preflight opens the target, validates table metadata when reachable, emits DDL preview, and reports field-level schema issues.
