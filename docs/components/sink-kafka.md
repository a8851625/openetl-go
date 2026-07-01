# sink/kafka

## Purpose
Produce transformed records to Kafka or Redpanda topics.

## Config Fields
- `brokers`, `topic`: required target fields.
- `key_column`, `compression`, auth/TLS fields: output and connection controls.
- `auto_create_topic`: optional first-run convenience; production deployments should prefer explicitly managed topics.

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
    key_column: id
```

## Evidence
Covered by `hack/e2e-kafka.sh`, `hack/e2e-kafka-raw-ods.sh`, and Kafka sink tests. Preflight validates broker metadata and blocks a missing target topic when `auto_create_topic` is false; unreachable broker metadata remains a warning so offline validation can still show other remediation.
