# transform/project

## Purpose
Select, rename, add constants, and format fields in record data.

## Config Fields
- `fields`: selected fields.
- `mappings`: source-to-target aliases.
- `constants`: static fields to add.
- `time_formats`: time conversion rules.

## Record Shape
Outputs a projected record with explicit fields and preserved metadata.

## Checkpoint, DLQ, Idempotency
Pure transform; checkpoint and replay semantics are inherited from source/sink.

## Fits
ODS shaping, wide-record trimming, parser output normalization.

## Does Not Fit
Arbitrary SQL projection planning.

## Example
```yaml
transforms:
  - type: project
    config:
      fields: ["id", "amount", "dt"]
      constants:
        source_system: "orders"
```

## Evidence
Covered by transform tests and Kafka raw ODS e2e.
