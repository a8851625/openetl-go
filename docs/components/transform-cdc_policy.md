# transform/cdc_policy

## Purpose
Apply CDC allow/deny rules, delete/snapshot/tombstone handling, and DDL guard policy.

## Config Fields
- `include_databases`, `exclude_databases`, `include_tables`, `exclude_tables`: source filters.
- `skip_delete`, `skip_snapshot`, `skip_tombstone`: event filters.
- `dangerous_ddl`, `ddl_allowlist`, `ddl_denylist`: schema-change behavior.

## Record Shape
Passes accepted records, filters configured records, and rejects dangerous DDL when configured.

## Checkpoint, DLQ, Idempotency
Rejected dangerous DDL can enter schema-class DLQ. Filtered records are counted by transform metrics.

## Fits
Debezium ODS sync safety rules.

## Does Not Fit
Automatic execution of arbitrary source DDL on targets.

## Example
```yaml
transforms:
  - type: cdc_policy
    config:
      skip_tombstone: true
      dangerous_ddl: reject
      include_tables: ["app.orders"]
```

## Evidence
Covered by `hack/e2e-debezium-mysql.sh` and CDC policy tests.
