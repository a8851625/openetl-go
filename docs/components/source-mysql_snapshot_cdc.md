# source/mysql_snapshot_cdc

## Purpose
Run an initial MySQL snapshot and continue from binlog CDC without a separate pipeline.

## Config Fields
- `host`, `user`, `database`: required source fields.
- `table` or `tables`: snapshot and CDC source tables.
- `pk_column`, `limit`: snapshot pagination.
- `server_id`, `server_id_base`, `consistent_snapshot_lock`: replication and snapshot controls.
- `password`: secret.

## Record Shape
Snapshot rows and later CDC rows share the standard record shape with operation metadata.

## Checkpoint, DLQ, Idempotency
Checkpoint records snapshot phase and CDC position. Downstream replay must be absorbed by upsert/versioned sinks.

## Fits
First-time MySQL table migration followed by continuous sync.

## Does Not Fit
Workloads where source locks are unacceptable and no consistent snapshot strategy is available.

## Example
```yaml
source:
  type: mysql_snapshot_cdc
  config:
    host: mysql
    user: sync
    password: "${MYSQL_PASSWORD}"
    database: app
    table: orders
    pk_column: id
```

## Evidence
Covered by `hack/e2e-snapshot-cdc.sh`, `hack/e2e-snapshot-cdc-clickhouse.sh`, and snapshot+CDC crash tests.
