# source/mysql_batch

## Purpose
Batch read rows from MySQL tables or a custom SQL query for backfill and periodic sync.

## Config Fields
- `host`, `user`, `database`: required MySQL connection and database fields.
- `table`: source table when `query` is not used.
- `query`: custom SQL, including JOIN queries.
- `pk_column` / `cursor_column`: pagination cursor.
- `limit`: page size per source query.
- `password`: secret.

## Record Shape
Outputs one record per row with row columns in `data`. Operation is normally `INSERT`.

## Checkpoint, DLQ, Idempotency
Checkpoint tracks pagination progress. Sink idempotency is required when replaying a batch; prefer upsert sinks for repeatable backfills.

## Fits
MySQL table backfill, custom-query snapshots, scheduled exports.

## Does Not Fit
Low-latency binlog CDC; use `mysql_cdc` or `mysql_snapshot_cdc`.

## Example
```yaml
source:
  type: mysql_batch
  config:
    host: mysql
    user: sync
    password: "${MYSQL_PASSWORD}"
    database: app
    table: orders
    pk_column: id
```

## Evidence
Covered by `hack/e2e-mysql-postgres.sh`, `hack/e2e.sh`, and `internal/etl/source/mysql_batch_schema_test.go`.
