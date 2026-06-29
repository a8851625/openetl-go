# transform/flat_map

## Purpose
Run a Lua one-to-many transform that expands one input record into zero, one, or many output records.

## Config Fields
- `script`: required Lua script.
- `language`: currently Lua for the core ABI.
- `on_error`: parser error behavior.

## Record Shape
Reads one record and returns an array of records or a single record. Output records inherit lineage metadata.

## Checkpoint, DLQ, Idempotency
Record-level script failures can enter DLQ. Replays can repeat multi-output records; use deterministic keys downstream.

## Fits
Protocol parsing, UDTF-style expansion, raw Kafka message parsing.

## Does Not Fit
General stream compute with timers or arbitrary state.

## Example
```yaml
transforms:
  - type: flat_map
    config:
      script: |
        return {
          { data = { id = record.data.id, value = record.data.value } }
        }
```

## Evidence
Covered by `hack/e2e-kafka-raw-ods.sh` and flat_map tests.
