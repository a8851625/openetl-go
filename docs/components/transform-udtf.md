# transform/udtf

## Purpose
Alias the one-to-many transform pattern for UDTF-style record expansion.

## Config Fields
- `script`: required script body.
- `language`: core path is Lua.
- `on_error`: error handling.

## Record Shape
Returns zero, one, or many output records for each input record.

## Checkpoint, DLQ, Idempotency
Failed expansion records are visible through the normal retry/DLQ path. Downstream replay absorption remains required.

## Fits
One input message to multiple business rows.

## Does Not Fit
Full Flink SQL UDTF compatibility.

## Example
```yaml
transforms:
  - type: udtf
    config:
      script: "return { record }"
```

## Evidence
Covered by flat_map/UDTF unit tests and Kafka raw ODS e2e coverage.
