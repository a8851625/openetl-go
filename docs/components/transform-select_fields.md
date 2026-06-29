# transform/select_fields

## Purpose
Compatibility alias for projection and field-selection workflows.

## Config Fields
- `fields`: selected fields.
- `mappings`: aliases.
- `constants`: static fields.
- `time_formats`: time conversion rules.

## Record Shape
Outputs only the selected/mapped fields in `data`.

## Checkpoint, DLQ, Idempotency
Pure transform; idempotency is determined by downstream sink behavior.

## Fits
Keeping YAML readable when the task is mostly field selection.

## Does Not Fit
General SQL select execution.

## Example
```yaml
transforms:
  - type: select_fields
    config:
      fields: ["id", "name"]
```

## Evidence
Covered by transform tests and descriptor/schema tests.
