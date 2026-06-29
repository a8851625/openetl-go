# sink/kafka

## Purpose
Produce transformed records to Kafka or Redpanda topics.

## Config Fields
- `brokers`, `topic`: required target fields.
- `format`, `key_field`, `compression`, auth/TLS fields: output and connection controls.

## Record Shape
Serializes record `data` or the configured envelope into Kafka messages.

## Checkpoint, DLQ, Idempotency
Kafka writes are at-least-once. Duplicate messages can appear after replay; use deterministic keys and consumer-side idempotency.

## Fits
Kafka ODS output and raw -> parsed topic pipelines.

## Does Not Fit
Kafka transactional exactly-once.

## Example
```yaml
sink:
  type: kafka
  config:
    brokers: ["redpanda:9092"]
    topic: ods.orders
    format: json
    key_field: id
```

## Evidence
Covered by `hack/e2e-kafka.sh`, `hack/e2e-kafka-raw-ods.sh`, and Kafka sink tests.
