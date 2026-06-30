# transform/window

## Purpose
Compute lightweight tumbling-window aggregates.

## Config Fields
- `window_sec`: tumbling window size.
- `group_by`: grouping fields.
- `aggregates`: aggregate definitions.
- `state_backend`, `state_ttl_seconds`: optional Redis-backed runtime window state. Requires `etl.state.redis.addr` or `ETL_STATE_REDIS_ADDR`.

## Record Shape
Consumes event records and emits aggregate records at window boundaries.

## Checkpoint, DLQ, Idempotency
Redis state restores in-progress window state after restart. Sliding/session windows and Flink-style late/retraction semantics are not in the production path.

## Fits
Small tumbling aggregates in Kafka -> ClickHouse detail/aggregate pipelines.

## Does Not Fit
Complex event-time stream processing.

## Example
```yaml
transforms:
  - type: window
    config:
      window_sec: 60
      group_by: ["user_id"]
      aggregates:
        amount_sum: "sum(amount)"
```

## Evidence
Covered by `hack/e2e-wide-table.sh` and window tests.
