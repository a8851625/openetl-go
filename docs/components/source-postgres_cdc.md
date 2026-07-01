# source/postgres_cdc

## Purpose
Read PostgreSQL logical replication changes through pgoutput for CDC pipelines.

## Config Fields
- `host`, `user`, `database`: required PostgreSQL connection and database fields.
- `port`, `sslmode`: connection transport settings.
- `slot_name`: stable logical replication slot used for restartable CDC.
- `tables`: optional CDC table filter, and required when `enable_snapshot` is true.
- `enable_snapshot`: run an initial snapshot before CDC.
- `drop_slot_on_close`: drops the replication slot on close; keep false for restartable production pipelines.
- `password`: secret.

## Record Shape
Emits insert/update/delete CDC records with PostgreSQL row fields in `data` and operation metadata. TRUNCATE is currently skipped with a warning rather than mapped to target deletes.

## Checkpoint, DLQ, Idempotency
Checkpoint stores the PostgreSQL LSN after sink commit. Downstream replay must be absorbed by upsert or versioned sinks. Keep `drop_slot_on_close: false` so a restart can resume from the same slot.

## Fits
PostgreSQL table CDC into MySQL/PostgreSQL/ClickHouse/Doris-style idempotent sinks.

## Does Not Fit
Full PostgreSQL DDL/TRUNCATE semantic replication or PostgreSQL-to-Kafka Connect replacement.

## Example
```yaml
source:
  type: postgres_cdc
  config:
    host: postgres
    port: 5432
    user: sync
    password: "${PG_PASSWORD}"
    database: app
    slot_name: etl_slot
    tables: ["public.orders"]
    sslmode: prefer
```

## Evidence
Covered by PostgreSQL CDC unit/preflight tests for pgoutput parsing, required fields, SSL mode, table checks, `wal_level=logical`, replication role, publication, and slot readiness. `hack/e2e-postgres-cdc.sh` covers PostgreSQL CDC source insert/update/delete into MySQL and stop/restart consumption from the retained replication slot.
