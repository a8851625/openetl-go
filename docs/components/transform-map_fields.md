# transform/map_fields

## Purpose
Apply static dictionary mapping to declared fields. Maps enumerated code values
(1→ONLINE, 3→NOT_CHARGING) into business-readable values without Lua/JS scripts.

## Config Fields
- `fields`: list of mapping rules. Each rule:
  - `field`: field name to map (required).
  - `map`: dictionary of source-value → mapped-value (required, non-empty).
  - `default`: value used when the source value is not in `map` (optional).
  - `on_missing`: behavior when source value is not in `map` and no `default` is set. Allowed: `keep` (default, leave original value untouched) or `null` (overwrite with null).

## Record Shape
Outputs the same record with the listed fields' values replaced by the mapped value, default, or null per the rule.

## Checkpoint, DLQ, Idempotency
Pure stateless transform; checkpoint/replay/idempotency are inherited from source/sink.

## Fits
- Enum/code value standardization (status codes, type codes).
- Boolean→string expansion.
- Multi-field one-pass dictionary mapping.

## Does Not Fit
- Reverse-database lookup (use `lookup`).
- Multi-level nested mapping or conditional branches (use `filter` / Lua).
- External connection-based dictionary (use `lookup` with SQL).

## Example
```yaml
transforms:
  - type: map_fields
    config:
      fields:
        - field: status_code
          map: {"1": "ONLINE", "3": "NOT_CHARGING"}
          default: UNKNOWN
        - field: charge_state
          map: {"0": "IDLE", "1": "CHARGING"}
          on_missing: keep
```

## Evidence
Covered by `internal/etl/transform/map_fields_test.go` (hit/miss/default/null/multi-field/validation/registry).
