# source/mysql_cdc

## Purpose
Read MySQL binlog changes for incremental CDC pipelines.

## Config Fields
- `host`, `user`, `database`, `tables`: required source fields.
- `port`, `server_id`, `server_id_base`: replication connection settings.
- `enable_gtid`, `start_from`: resume and failover controls.
- `password`: secret.

## Record Shape
Emits CDC records with operation metadata and changed row fields in `data`.

## Checkpoint, DLQ, Idempotency
Checkpoint stores binlog/GTID position after sink commit. Use upsert or versioned sinks to absorb at-least-once replay.

## Fits
MySQL -> MySQL/PostgreSQL/ClickHouse/Doris CDC sync.

## Does Not Fit
Initial full-table load without a separate snapshot; use `mysql_snapshot_cdc`.

## Example
```yaml
source:
  type: mysql_cdc
  config:
    host: mysql
    user: sync
    password: "${MYSQL_PASSWORD}"
    database: app
    tables: ["orders"]
```

## Evidence
Covered by `hack/e2e-cdc-mysql.sh`, `hack/e2e-cdc-postgres.sh`, `hack/e2e-clickhouse.sh`, and CDC crash recovery scripts.
