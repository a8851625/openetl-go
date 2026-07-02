# sink/postgres

## Purpose
Write records into PostgreSQL with insert or upsert semantics.

## Config Fields
- `host`, `user`, `database`, `table`: required target fields.
- `port`, `password`, `sslmode`: connection fields.
- `batch_mode`, `pk_columns`, `auto_create`, `schema_drift`, `ddl_policy`: idempotency and schema controls.
- `pre_write`: optional pre-write action block `{action, condition, params}`. Runs inside the batch transaction before inserts. Same semantics and CDC safety rules as MySQL sink `pre_write`.

## Record Shape
Writes record `data` columns to the target table.

## Checkpoint, DLQ, Idempotency
Use upsert mode and stable primary keys for at-least-once replay absorption. Batch writes are transaction-bounded.

## Fits
MySQL batch/CDC -> PostgreSQL and ODS sync.

## Does Not Fit
Arbitrary PostgreSQL logical replication target management.

## Example
```yaml
sink:
  type: postgres
  config:
    host: postgres
    user: sync
    password: "${PG_PASSWORD}"
    database: ods
    table: orders
    batch_mode: upsert
    pk_columns: ["id"]
```

## Evidence
Covered by `hack/e2e-mysql-postgres.sh`, `hack/e2e-cdc-postgres.sh`, and PostgreSQL sink tests. Preflight opens the target, validates table metadata when reachable, emits DDL preview, and reports field-level schema issues.
