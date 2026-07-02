# transform/extract

## Purpose
Minimal field extraction/construction: regex extraction and template concatenation.
Replaces simple Lua/JS string manipulation for vendor/material field shaping.

## Config Fields
- `rules`: list of extraction rules. Each rule requires `target` and exactly one of:
  - `pattern`: regex applied to `source_field` (default: same as `target`). `group` selects the capture group index (default 0 = whole match).
  - `template`: Go text/template string, e.g. `"{{.material_name}}.{{.mes_optional_parts}}"`. Only `.FieldName` variables are exposed.

## Record Shape
Outputs the same record with `target` fields added or replaced. On regex miss or template error the target field is left untouched.

## Checkpoint, DLQ, Idempotency
Pure stateless transform; checkpoint/replay/idempotency are inherited from source/sink.

## Fits
- Extracting vendor prefix from `material_name`.
- Concatenating fields into a composite key.
- Normalizing simple string shapes without an expression engine.

## Does Not Fit
- Conditional branches (use `filter` / Lua).
- Multi-level nested data extraction (use Lua/JS).
- Arbitrary expression evaluation.

## Example
```yaml
transforms:
  - type: extract
    config:
      rules:
        - target: vendor
          source_field: material_name
          pattern: "^(.+?)-"
          group: 1
        - target: material_no
          template: "{{.material_name}}.{{.mes_optional_parts}}"
```

## Evidence
Covered by `internal/etl/transform/extract_test.go` (regex hit/miss/source_field/template/multi-rule/validation/registry).
