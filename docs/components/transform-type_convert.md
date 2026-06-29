# transform/type_convert

## Purpose
Convert field values to configured primitive target types.

## Config Fields
- `conversions`: required map of field name to target type such as `int`, `float`, `bool`, `string`, or `datetime`.

## Record Shape
Mutates configured fields in record `data` to the requested types.

## Checkpoint, DLQ, Idempotency
Conversion failures are record errors and can enter retry/DLQ depending on pipeline configuration.

## Fits
Preparing parser or JSON input for typed database/OLAP sinks.

## Does Not Fit
Full schema registry or complex nested type evolution.

## Example
```yaml
transforms:
  - type: type_convert
    config:
      conversions:
        id: int
        amount: float
```

## Evidence
Covered by builtin transform tests and Kafka raw ODS e2e.
