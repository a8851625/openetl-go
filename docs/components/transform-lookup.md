# transform/lookup

## Purpose
Join streaming records with dimension data from a database or cached state.

## Config Fields
- `dsn`, `query`, `fields`: dimension lookup configuration.
- `join_key`, `dim_key`: source and dimension key mapping.
- `state_backend`, `state_ttl_seconds`: optional Redis-backed runtime cache. Requires `etl.state.redis.addr` or `ETL_STATE_REDIS_ADDR`.

## Record Shape
Reads record `data`, adds configured dimension fields, and preserves metadata.

## Checkpoint, DLQ, Idempotency
Lookup misses can be configured to fail into DLQ. Redis state can restore cached dimension rows after restart; it is not a cross-system exactly-once transaction.

## Incremental Dimension Refresh

`mode: cache` reloads the whole dimension query on every `refresh_interval_sec`, which is the usual
bottleneck for wide dimension tables. Setting `incremental_column` (plus `incremental_query`) switches
refreshes to a watermark poll: the first load is full, later refreshes ask only for rows whose monotonic
column advanced and merge them into the cache by `dim_key`.

```yaml
transforms:
  - type: lookup
    config:
      dsn: "sync:pass@tcp(mysql:3306)/dim"
      query: "select id, name, updated_at from users"
      incremental_column: updated_at
      incremental_query: "select id, name, updated_at from users where updated_at > ?"
      full_refresh_interval_sec: 3600
      join_key: user_id
      dim_key: id
      fields: ["name"]
      refresh_interval_sec: 30
```

Boundaries, all deliberate:

- hard `DELETE`s are invisible incrementally — soft-delete via `incremental_delete_column` or rely on
  `full_refresh_interval_sec`;
- a failed incremental refresh falls back to a full reload (never a partial merge);
- the watermark is per-process, so a restart starts with one full load (persisted `state_backend` cache
  is still restored);
- the queries must project `incremental_column`, otherwise the refresh fails loudly rather than looping
  without advancing.

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
Covered by `hack/e2e-lookup-state.sh`, `hack/e2e-wide-table.sh`, lookup tests, and
`hack/e2e-data-integrity.sh` scenario 5 (real MySQL `TIMESTAMP` watermark: dimension row updated mid-run
is merged incrementally, asserted through both the enriched output and the `incremental_merge` metric).
Unit evidence for the incremental path: `internal/etl/transform/lookup_incremental_test.go` and
`internal/etl/pipeline/spec_validation_incremental_test.go`.
