# source/mysql_cdc

## Purpose
Read MySQL binlog changes for incremental CDC pipelines.

## Config Fields
- `host`, `user`, `database`, `tables`: required source fields.
- `port`, `server_id`, `server_id_base`, `shard_index`, `shard_total`: replication connection and table-sharding settings.
- `enable_gtid`, `start_from`: resume and failover controls.
- `password`: secret.

## Record Shape
Emits CDC records with operation metadata and changed row fields in `data`.

## Schema Preflight
For one configured table, preflight keeps the existing flat source-schema validation and may build one target DDL preview. For multiple tables or `tables: ["*"]`, preflight enumerates every resolved base table and checks its columns independently. It never uses the first table as the schema for the whole stream and never returns a single-table DDL preview for a multi-table source.

Per-record/dynamic target routing returns a structured `schema-multi-table-partial` warning because target-specific schema validation must be performed for each routed table. A fixed ClickHouse/Doris/MaxCompute/JDBC/Elasticsearch target is blocked with `schema-multi-table-fixed-target` unless the spec explicitly declares table mapping or schema-normalizing/filtering transforms; explicit normalization remains a partial warning because preflight cannot derive the post-transform schema for every input table.

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
Covered by `hack/e2e-path-mysql-cdc-mysql.sh` (PR-2 path matrix: happy/crash/reset/outage/DLQ), `hack/e2e-cdc-mysql.sh`, `hack/e2e-cdc-postgres.sh`, `hack/e2e-clickhouse.sh`, and `hack/e2e-cdc-crash-recovery.sh`. Path contract: `mysql_cdc__mysql_upsert`.

Multi-table schema-contract evidence: `internal/etl/server/pipelines_preflight_test.go` and `hack/e2e-mysql-cdc-wide.sh`.
