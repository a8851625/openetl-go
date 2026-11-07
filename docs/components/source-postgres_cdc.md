# source/postgres_cdc

## Purpose
Read PostgreSQL logical replication changes through pgoutput for CDC pipelines.

## Config Fields
- `host`, `user`, `database`: required PostgreSQL connection and database fields.
- `port`, `sslmode`: connection transport settings.
- `slot_name`: stable logical replication slot used for restartable CDC.
- `tables`: optional CDC table filter, and required when `enable_snapshot` is true.
- `enable_snapshot`: run an initial snapshot before CDC.
- `drop_slot_on_close`: drops the replication slot on close; keep false for restartable production pipelines.
- `on_truncate`: `error` (default) fails closed when TRUNCATE is observed; `skip` only continues after explicit `allow_unsafe: true` and leaves residual sink rows (PR-2.3).
- `password`: secret.

## Record Shape
Emits insert/update/delete CDC records with PostgreSQL row fields in `data` and operation metadata,
including `Metadata.PrimaryKeyColumns` and `Metadata.Key` derived from `pg_index.indisprimary`, so a
metadata-PK sink (`pk_columns_from_metadata: true`) can absorb replays by business key.

Before images follow what pgoutput actually published, recorded in `Metadata.BeforeImageState`:

| pgoutput old tuple | BeforeImageState | Before content |
| --- | --- | --- |
| `O` (REPLICA IDENTITY FULL) | `full` | every column |
| `K` (key only) | `partial` | replica-identity columns, named from the Relation message flags |
| `N` (no old tuple, key unchanged) | `partial` | key columns reconstructed from the after image |

A key-unchanged UPDATE under the default replica identity (`d`) publishes no old tuple at all. The
reader reconstructs the key-only before image from the after image in that case — pgoutput only omits
the old tuple when the identity columns did not change, so nothing is guessed — which is what keeps
such updates out of the DLQ with reason `update_before_image_missing`.

TRUNCATE is **not** mapped to target deletes: default policy is `on_truncate: error`, which now really
fails closed (the earlier parser read the relation count one byte late, so TRUNCATE frames were
silently ignored while reporting the safe policy).

## Checkpoint, DLQ, Idempotency
Checkpoint stores the PostgreSQL LSN after sink commit. `CheckpointForRecord` only constructs the candidate LSN; after the durable checkpoint save, `AckCheckpoint` sends the replication flush/apply status and advances the in-memory committed LSN only after that send succeeds. Keepalives with no durable LSN use `0/0` rather than the server read-ahead end, and connection recovery resumes from the durable/acknowledged LSN rather than the latest received frame. An external-ack failure fails closed so restart can replay from the durable checkpoint. Downstream replay must be absorbed by upsert or versioned sinks. Keep `drop_slot_on_close: false` so a restart can resume from the same slot.

## Fits
PostgreSQL table CDC into MySQL/PostgreSQL/ClickHouse/Doris-style idempotent sinks.

## Does Not Fit
Full PostgreSQL DDL/TRUNCATE semantic replication or PostgreSQL-to-Kafka Connect replacement.

## Example
```yaml
source:
  type: postgres_cdc
  config:
    host: postgres
    port: 5432
    user: sync
    password: "${PG_PASSWORD}"
    database: app
    slot_name: etl_slot
    tables: ["public.orders"]
    sslmode: prefer
```

## Evidence
Covered by PostgreSQL CDC unit/preflight tests for pgoutput parsing, required fields, SSL mode, table checks, `wal_level=logical`, replication role, publication, and slot readiness. `hack/e2e-postgres-cdc.sh` runs against a real PostgreSQL 16 (`wal_level=logical`) and MySQL sink and proves, in one pass:

- publication `etl_pub` and logical slot creation;
- INSERT/UPDATE/DELETE propagation with the metadata primary key driving an upsert (no duplicate row);
- the slot staying active across `wal_sender_timeout` while idle, then resuming without a reconnect gap;
- stop/start resuming from the durable LSN checkpoint with no duplicate replay of already-acknowledged rows;
- TRUNCATE aborting the stream instead of silently leaving stale sink rows.

Regression tests for the three decoding bugs found by that path live in
`internal/etl/source/postgres_cdc_tablepk_test.go` (`TestLoadTablePKsQueryShape`,
`TestParseUpdateKeyTupleNamesColumnsFromReplicaIdentity`,
`TestParseUpdateWithoutOldTupleReconstructsKeyBeforeImage`, `TestHandleTruncateMsgFailsClosed`).
