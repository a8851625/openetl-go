# sink/maxcompute

## Purpose
Write Kafka/ODS-style records into MaxCompute/ODPS partitioned tables through the SDK tunnel writer.

## Config Fields
- `endpoint`, `project`, `table`, `access_key_id`, `access_key_secret`: required target and credential fields.
- `tunnel_endpoint`, `quota_name`: optional tunnel routing and quota controls.
- `columns`: optional target column type map used by schema validation and DDL preview.
- `partition`, `partition_fields`, `auto_create_partition`: static or dynamic partition controls. At least one partition source is required.
- `write_mode`, `batch_size`, `max_retries`, `retry_base_ms`: write and retry controls.

## Record Shape
Writes record `data` fields to MaxCompute columns. Dynamic `partition_fields` are read from records and excluded from row payload.

## Checkpoint, DLQ, Idempotency
Default `append` mode is at-least-once and can duplicate on replay. Use controlled `partition_overwrite`, staging+merge, or downstream business-key deduplication when duplicates are not acceptable.

## Fits
Kafka ODS JSON -> project/type_convert -> MaxCompute partitioned table.

## Does Not Fit
Replacing a full MaxCompute SQL planner, uncontrolled exactly-once warehouse transactions, or non-partitioned first target paths.

## Example
```yaml
sink:
  type: maxcompute
  config:
    endpoint: https://service.cn-hangzhou.maxcompute.aliyun.com/api
    project: warehouse
    table: ods_events
    access_key_id: "${ALIYUN_ACCESS_KEY_ID}"
    access_key_secret: "${ALIYUN_ACCESS_KEY_SECRET}"
    columns:
      id: BIGINT
      payload: STRING
    partition_fields: ["dt"]
```

## Evidence
Covered by MaxCompute sink tests and server preflight tests. Real MaxCompute write/replay e2e evidence is still required before production maturity.
