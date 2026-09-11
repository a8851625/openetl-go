# sink/postgres

## Purpose
Write records into PostgreSQL with insert or upsert semantics.

## Config Fields
- `host`, `user`, `database`: required target fields. `table` may be omitted only for a source path that guarantees per-record `Metadata.Table`.
- `port`, `password`, `sslmode`: connection fields.
- `batch_mode`, `pk_columns`, `pk_columns_from_metadata`, `auto_create`, `column_types`, `schema_drift`, `ddl_policy`: idempotency and schema controls.
- `pre_write`: optional pre-write action block `{action, condition, params}`. Runs inside the batch transaction before inserts. Same semantics and CDC safety rules as MySQL sink `pre_write`.

## Record Shape
Writes record `data` columns to the target table.

## Checkpoint, DLQ, Idempotency
Use upsert mode and stable primary keys for at-least-once replay absorption. With `pk_columns_from_metadata: true`, each row must carry complete declared JSON-object identity; empty/partial/conflicting keys fail closed and static `pk_columns` is only the exact `legacy_verified` replay safety set. Auto-create emits the corresponding single/composite PRIMARY KEY and prioritizes `Metadata.ColumnTypes`. A key-changing UPDATE atomically upserts the new key and deletes the validated old key. Batch writes are transaction-bounded.

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
Covered by `hack/e2e-mysql-postgres.sh`, `hack/e2e-cdc-postgres.sh`, `hack/e2e-relational-write-modes.sh`, `hack/e2e-kafka-postgres-fanout.sh`, the shared metadata-PK conformance matrix, and PostgreSQL sink tests. The Kafka fan-out path covers composite keys, key-changing UPDATE, target outage/DLQ replay, and checkpoint plus consumer-group replay. Preflight opens the target, validates table metadata when reachable, emits DDL preview, and reports field-level schema issues.
