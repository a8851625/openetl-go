# transform/lookup

## Purpose
Join streaming records with dimension data from a database or cached state.

## Config Fields
- `dsn`, `query`, `fields`: dimension lookup configuration.
- `join_key`, `dim_key`: source and dimension key mapping.
- `state_backend`, `state_path`, `state_ttl_seconds`: optional durable cache.

## Record Shape
Reads record `data`, adds configured dimension fields, and preserves metadata.

## Checkpoint, DLQ, Idempotency
Lookup misses can be configured to fail into DLQ. SQLite state can restore cached dimension rows after restart; it is not a cross-system exactly-once transaction.

## Fits
Kafka -> lookup -> OLAP/ODS detail pipelines.

## Does Not Fit
Large arbitrary stream-stream state machines.

## Example
```yaml
transforms:
  - type: lookup
    config:
      dsn: "mysql://sync:pass@tcp(mysql:3306)/dim"
      query: "select name from users where id = ?"
      join_key: user_id
      fields: ["name"]
```

## Evidence
Covered by `hack/e2e-lookup-state.sh`, `hack/e2e-wide-table.sh`, and lookup tests.
