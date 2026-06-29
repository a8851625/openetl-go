# sink/clickhouse

## Purpose
Write batch or CDC records into ClickHouse tables with optional auto-create and schema drift handling.

## Config Fields
- `host`, `database`, `table`: required target fields.
- `port`, `protocol`, `user`/`username`, `password`: connection fields.
- `auto_create`, `schema_drift`, `batch_mode`, `pk_columns`, `version_column`: schema and replay controls.

## Record Shape
Writes record `data` fields as columns; CDC metadata controls update/delete behavior where supported.

## Checkpoint, DLQ, Idempotency
Use ReplacingMergeTree-style tables, version columns, or explicit deduplication to absorb replay. Failed writes go through retry/DLQ.

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
```

## Evidence
Covered by `hack/e2e-clickhouse.sh`, `hack/e2e-snapshot-cdc-clickhouse.sh`, and ClickHouse sink tests.
