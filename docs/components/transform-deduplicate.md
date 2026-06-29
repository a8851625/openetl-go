# transform/deduplicate

## Purpose
Drop duplicate records by configured key fields.

## Config Fields
- `keys` or `key_fields`: composite key fields.
- `window_size`: memory window.
- `state_backend`, `state_path`, `state_ttl_seconds`: optional durable state.

## Record Shape
Passes first-seen records unchanged and filters duplicates.

## Checkpoint, DLQ, Idempotency
Durable state can survive restart, but replay semantics still depend on source offset and sink commit ordering.

## Fits
Kafka replay absorption and lightweight duplicate filtering.

## Does Not Fit
Global unbounded deduplication without state limits.

## Example
```yaml
transforms:
  - type: deduplicate
    config:
      keys: ["id"]
      state_backend: sqlite
```

## Evidence
Covered by `hack/e2e-wide-table.sh` and transform tests.
