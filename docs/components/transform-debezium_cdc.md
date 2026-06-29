# transform/debezium_cdc

## Purpose
Normalize Debezium Kafka CDC envelopes into OpenETL records for ODS sync.

## Config Fields
- `table_mapping`: target table naming templates.
- `include_metadata`: preserve source metadata.
- `skip_snapshot`, `skip_tombstone`: filtering controls.

## Record Shape
Parses Debezium `c/u/d/r`, `source.db`, `source.table`, `ts_ms`, tombstone, and schema-change events into standard operation and metadata fields.

## Checkpoint, DLQ, Idempotency
Kafka offsets checkpoint after sink commit. Use upsert sinks and stable keys to absorb replay.

## Fits
Replacing hand-written Debezium Kafka consumers for ODS upsert.

## Does Not Fit
Managing Debezium connectors or Kafka Connect offsets.

## Example
```yaml
transforms:
  - type: debezium_cdc
    config:
      table_mapping:
        "{source_db}.{source_table}": "ods_{source_db}__{source_table}"
```

## Evidence
Covered by `hack/e2e-debezium-mysql.sh` and Debezium transform tests.
