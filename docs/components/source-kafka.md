# source/kafka

## Purpose
Consume Kafka or Redpanda topics as JSON/text records for realtime ETL.

## Config Fields
- `brokers`, `topic`: required Kafka connection fields.
- `group_id`, `initial_offset`: consumer group and offset behavior.
- `format`, `key_column`, `value_column`: message decoding.
- `sasl_user`, `sasl_password`, `sasl_mechanism`, `tls`: auth and transport.

## Record Shape
JSON messages become record `data`; raw/text messages can be carried through configured key/value fields. Kafka message keys are preserved as `record.metadata.key`, and `key_column` can additionally copy the key into `data` for downstream transforms.

## Checkpoint, DLQ, Idempotency
Offsets are checkpointed after sink commit. Replaying offsets can duplicate append sinks; prefer deterministic keys or upsert sinks.

## Fits
Kafka JSON events, Debezium envelopes, raw protocol messages followed by parser transforms.

## Does Not Fit
Kafka Connect connector management.

## Example
```yaml
source:
  type: kafka
  config:
    brokers: ["redpanda:9092"]
    topic: orders
    group_id: openetl-orders
    format: json
```

## Evidence
Covered by `hack/e2e-kafka.sh`, `hack/e2e-kafka-raw-ods.sh`, `hack/e2e-debezium-mysql.sh`, and wide-table e2e tests.
