# sink/elasticsearch

## Purpose
Bulk index transformed records into Elasticsearch or OpenSearch.

## Config Fields
- `hosts` or `host`, `index`: required target endpoint and index fields.
- `username`, `password`: optional basic auth; password is secret.
- `id_column`: deterministic document ID column for replay-safe overwrite semantics.
- `mappings`, `properties`, or `mapping`: optional mapping hints used by preflight schema validation.
- `chunk_size`, `max_retries`, `retry_base_ms`: bulk size and retry controls.
- `tls_skip_verify`: TLS verification control.

## Record Shape
Writes each record `data` object as one bulk index/update document. When `id_column` is present in the record, replay targets the same document ID.

## Checkpoint, DLQ, Idempotency
Delivery is at-least-once. Configure a stable `id_column` so replay overwrites the same document instead of creating duplicates. Bulk item failures are classified per item and can enter DLQ/replay.

## Fits
Operational search indexes and OpenSearch/Elasticsearch projections where eventual replay overwrite is acceptable.

## Does Not Fit
Cross-index transactions, strict exactly-once indexing, or workloads that require Elasticsearch to be the system of record.

## Example
```yaml
sink:
  type: elasticsearch
  config:
    hosts: ["http://opensearch:9200"]
    index: customers
    id_column: id
    chunk_size: 500
```

## Evidence
Covered by `hack/e2e-elasticsearch.sh`, Elasticsearch sink tests, and preflight tests for remote mapping/schema compatibility and field-level type conflicts.
