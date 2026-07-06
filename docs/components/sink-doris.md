# sink/doris

## Purpose
Write records into Doris through Stream Load with MySQL-protocol fallback for supported operations.

## Config Fields
- `host`, `database`, `table`: required target fields.
- `port`, `http_port`, `user`, `password`: FE/MySQL and HTTP connection fields.
- `write_mode`, `stream_load_format`, `stream_load_scheme`, `stream_load_timeout_sec`, `insert_chunk_size`: write path and batching controls.
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
Covered by `hack/e2e-doris.sh` and Doris sink/preflight tests. The e2e covers Stream Load JSON/CSV, MySQL-protocol insert fallback, Unique Key auto-create/schema typing, MySQL CDC -> Doris insert/delete, MySQL snapshot+CDC -> Doris snapshot/update/insert, app restart recovery, checkpoint reset replay absorption, schema drift add-columns, Doris BE outage -> transient DLQ, BE/FE recovery, and DLQ replay. Preflight opens the Doris MySQL protocol target, validates table/Unique Key metadata when reachable, emits DDL preview, and reports field-level schema issues.
