# sink/doris

## Purpose
Write records into Doris through Stream Load with MySQL-protocol fallback for supported operations.

## Config Fields
- `host`, `database`, `table`: required target fields.
- `port`, `http_port`, `user`, `password`: FE/MySQL and HTTP connection fields.
- `batch_mode`, `pk_columns`, `auto_create`, `schema_drift`, `ddl_policy`: idempotency and schema controls.

## Record Shape
Writes record `data` columns. Production CDC/upsert requires Unique Key tables and stable keys.

## Checkpoint, DLQ, Idempotency
Doris production path relies on Unique Key/upsert behavior. Mixed write/delete batches are constrained unless explicitly allowed.

## Fits
MySQL batch -> Doris and production-candidate Doris Unique Key upsert paths.

## Does Not Fit
Non-Unique Key CDC upsert claims.

## Example
```yaml
sink:
  type: doris
  config:
    host: doris-fe
    port: 9030
    http_port: 8030
    database: ods
    table: orders
    batch_mode: upsert
    pk_columns: ["id"]
```

## Evidence
Covered by `hack/e2e-doris.sh` and Doris sink/preflight tests.
