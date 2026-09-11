# Reliability Certification Matrix

This matrix is the evidence source for OpenETL-Go production-candidate recovery boundaries. The default guarantee is at-least-once: a sink acknowledgement happens before the corresponding source checkpoint is persisted. For sources with an external cursor (Kafka consumer offsets or PostgreSQL WAL), that external acknowledgement happens only after the same checkpoint is durably saved. A crash or checkpoint-store failure in between can replay records; it must not silently skip them.

Path-level production contracts (source → transforms → sink write mode + business key + storage/runtime + RPO/RTO) live in [path-contract.md](./path-contract.md) and `GET /api/v2/paths/contracts`. Connector maturity alone does not certify a path.

## Checkpoint Boundary

Successful stateful checkpoints use the v1 envelope:

```json
{
  "version": 1,
  "source": {"topic": "orders", "offsets": {"0": 41}},
  "state": {"window-3": "snapshot-version"},
  "sink_commit": {
    "acknowledged": true,
    "sink": "clickhouse",
    "record_count": 10,
    "last_batch_sha256": "..."
  },
  "delivery_mode": "at_least_once"
}
```

The fields describe one durable recovery boundary; they are not a distributed transaction:

1. Transform state is durably snapshotted.
2. The sink acknowledges the batch.
3. Sink acknowledgement metadata, state versions, and source position are saved together.
4. If state metadata, sink commit metadata, or checkpoint persistence fails, the source checkpoint does not advance and the range replays.
5. For Kafka/PostgreSQL CDC, the source external acknowledgement is sent after step 3. An external-ack failure blocks further checkpoint advancement and fails/stops the pipeline; restart reopens from the durable checkpoint and may replay the range.
6. If a failed record cannot be written to the DLQ, later successful batches cannot advance past it during the same run. Restart reopens the source from the last durable checkpoint.
7. A checkpoint-throttled sink acknowledgement is retained as a pending boundary. The write-loop timer persists it after `checkpoint_interval_sec` even when the stream becomes idle; Stop/EOF force the same boundary to durable storage.
8. Kafka offset `0` is stored explicitly. Missing partition state is not conflated with the valid zero offset.
9. Checkpoint storage/read or envelope validation errors fail the pipeline before its source is opened. They are never treated as an empty/new source position.
10. A valid JSON payload must also pass the source codec's semantic validator before `Source.Open`: required fields, non-negative cursors, source/topic identity, phase, binlog handoff, and LSN/GTID syntax are checked explicitly.

## Production-Candidate Matrix

| Path / `path_id` | Happy path | Replay absorption | Failure / DLQ | Restart / crash | Broker / rebalance | Residual boundary |
| --- | --- | --- | --- | --- | --- | --- |
| **MySQL CDC -> MySQL upsert** `mysql_cdc__mysql_upsert` (PR-2 forced) | `hack/e2e-path-mysql-cdc-mysql.sh`, `hack/e2e-cdc-mysql.sh` | MySQL `batch_mode: upsert` + stable PK | Sink privilege/outage -> DLQ -> replay | SIGKILL + checkpoint resume (`hack/e2e-cdc-crash-recovery.sh`, path matrix) | Not applicable | Source binlog and sink are not a distributed transaction |
| **MySQL snapshot+CDC -> ClickHouse** `mysql_snap_cdc__ch_rmt` (PR-2 forced) | `hack/e2e-clickhouse-replay-ordering.sh` (native path) | `UInt64` binlog-handoff/source-position versions + two-argument ReplacingMergeTree absorb delayed DLQ and checkpoint-reset replay | ClickHouse outage DLQ/replay | Snapshot and CDC crash recovery (`hack/e2e-snapshot-cdc-crash.sh`) | Not applicable | Source binlog and sink are not a transaction; order is valid only within one binlog lineage; use `FINAL` |
| **Kafka envelope -> ClickHouse** (IT-2 source-order certification) | `hack/e2e-clickhouse-replay-ordering.sh` (HTTP path), `hack/e2e-kafka-multitable-clickhouse.sh` | Partition/offset version + tombstone preserve newer UPDATE/DELETE during delayed replay | Constraint failure -> DLQ -> delayed replay | OpenETL checkpoint + external consumer-group reset to offset 0 | One business key must remain on one stable partition | No cross-partition order; partition-count/key routing change requires migration |
| **Kafka Canal -> ClickHouse metadata PK** `kafka_canal__ch_metadata_pk` | `hack/e2e-kafka-canal-identity.sh` | Complete composite `pkNames`, before-key UPDATE and partition/offset version | Frozen raw/PK/contract/old/target context; exact legacy repair; partial/key-change/unknown quarantine | Process restart preserves repair state; sink-acked/delete crash order has fault-injection tests | Single stable partition in certification | Replay remains at-least-once before the durable sink-ack marker |
| **Kafka envelope -> PostgreSQL metadata PK** (IT-2 GAP-7.3 certification, 2026-09-05) | `hack/e2e-kafka-postgres-fanout.sh` | Per-table PK derived from metadata Key; composite key and key-changing UPDATE remove old key in one transaction | PostgreSQL outage -> DLQ -> restart -> controlled replay | Checkpoint + consumer-group reset replay leaves business-key state unchanged (no resurrection) | Same-partition ordering as ClickHouse path | Per-record upsert atomicity only; no cross-sink transaction |
| **Kafka envelope -> ClickHouse multi-table** (IT-2 GAP-7.3 certification, 2026-09-05) | `hack/e2e-kafka-multitable-clickhouse.sh` | `table_template` fan-out to three tables; composite ORDER BY; key-change tombstone | ClickHouse outage -> DLQ -> restart -> controlled replay | Checkpoint + group reset replay absorbed by source-ordered ReplacingMergeTree | One business key per stable partition | No cross-partition order; business-key rerouting requires migration |
| Kafka -> file `kafka__file_unsafe` | `hack/e2e-kafka.sh` (`allow_unsafe: true` is explicit in the fixture) | Content-addressed file key keeps object count stable after offset replay | Runner and file sink tests | Wait for source offset + sink commit, SIGKILL, produce while down, restart from checkpoint | Redpanda restart and same-group join/leave | Changed batch boundaries may produce different objects; production specs remain blocked by default without explicit opt-in |
| Kafka raw -> lookup -> Kafka ODS | `hack/e2e-kafka-raw-ods.sh` | Kafka append duplicates are explicitly visible after offset replay | Parser and lookup miss DLQ | Source checkpoint restart coverage inherited from Kafka tests | Covered by ordinary Kafka and Debezium paths | Kafka transactions/exactly-once are not claimed |
| Debezium Kafka -> MySQL `debezium_kafka__mysql` | `hack/e2e-debezium-mysql.sh` | MySQL upsert and stable keys absorb replay | Data/schema DLQ and replay | App restart | Broker restart and consumer-group rebalance | Debezium connector lifecycle remains external |
| Kafka -> lookup/deduplicate/window -> ClickHouse | `hack/e2e-wide-table.sh` | ReplacingMergeTree/deduplicate absorb replay | Lookup miss and ClickHouse outage DLQ/replay | SIGKILL with Redis state restore | Kafka boundary certified by `hack/e2e-kafka.sh` | Offset/state/sink are bound by an envelope, not atomically committed |
| Kafka -> lookup -> ClickHouse | `hack/e2e-lookup-state.sh` | ClickHouse business key/version strategy | Dimension query unavailable after restart uses Redis cache | App SIGKILL/restart | Kafka boundary certified separately | Cache TTL expiry follows configured miss/error policy |
| MySQL CDC/snapshot+CDC -> Doris `mysql_snap_cdc__doris_uk` | `hack/e2e-doris.sh` | Unique Key/upsert with stable PK | BE outage -> DLQ -> recovery replay | App restart | Not applicable | Mixed write/delete batches remain constrained |
| MySQL batch -> Elasticsearch | `hack/e2e-elasticsearch.sh` | Stable document ID | Item-level mapping conflict DLQ/replay | Repeatable batch restart | Not applicable | Bulk request is only item-aware, not cross-item atomic |
| File/batch -> S3 `file_batch__s3_content_key` | `hack/e2e-s3-minio.sh` | Deterministic content-addressed object key | MinIO outage -> transient DLQ -> replay | Checkpoint reset | Not applicable | First-class manifests are not implemented |

## Required Unit Gates

- Linear and DAG checkpoints include source position, state snapshot versions when present, and sink acknowledgement metadata.
- State snapshot or sink commit metadata failure prevents checkpoint advancement.
- DLQ persistence failure blocks later checkpoint advancement past the unsafe record.
- Sink write failure never advances the checkpoint.
- Legacy source checkpoints continue to open; envelope source positions are unwrapped before source startup.
- Corrupt legacy JSON, corrupt envelopes, unknown envelope versions, and envelopes without a source position fail startup and remain visible as `failed`.
- Syntactically valid but semantically incomplete source positions (`{}`, missing offsets/pages/cursors, negative values, topic mismatch, invalid LSN/GTID/phase, or missing snapshot handoff) fail before source `Open`. Nil checkpoints and valid legacy positions remain accepted; Kafka's `-1` committed-offset sentinel remains valid for replaying offset zero.
- Kafka `CheckpointForRecord` does not mark/commit offsets; auto-commit is disabled and `AckCheckpoint` marks/commits only after durable checkpoint save.
- PostgreSQL CDC `CheckpointForRecord` does not advance `committedLsn`; `AckCheckpoint` sends the WAL status update first and publishes the committed LSN only after a successful send. Keepalives without a durable LSN use 0/0, and reconnects use the durable marker rather than the server/read-ahead end.
- MySQL snapshot+CDC keeps producer pagination cursors separate from acknowledged snapshot cursors. Linear and DAG writers pass the complete source batch to the snapshot checkpointer; numeric and ordered string cursors are applied only after the checkpoint row is saved. Snapshot checkpoints retain the original CDC handoff position, and CDC reconnects use the last acknowledged binlog file/position rather than handler read-ahead.
- MySQL snapshot+CDC malformed numeric cursors, missing cursor columns, unsupported phases, and missing snapshot/CDC binlog handoff positions fail closed. A checkpoint-generation error blocks later advancement and marks the linear/DAG pipeline failed so restart replays from the last durable boundary.
- ClickHouse `source_order` accepts only a connector-owned numeric position, writes writable
  `UInt64` version + `UInt8` tombstone columns through native and HTTP protocols, and rejects missing/
  overflowed positions, derived `window` outputs, and incompatible legacy target engines/types. Its
  append mode is INSERT-only.
- Kafka offset zero is retained and an idle stream flushes the latest throttled checkpoint boundary after the configured interval.
- Metadata-PK pipelines reject incapable source/format combinations in validate/preflight. The runner validates complete declared Key/Data/before identity after transforms; source parse rejections and identity failures reach durable DLQ before checkpoint acknowledgement, while DLQ failure fences later checkpoint progress.
- Metadata-PK DLQ replay uses only the context frozen with the failed row, revalidates identity after transforms, and returns structured HTTP 409 repair/quarantine outcomes. A persisted `sink_acked` marker precedes DLQ deletion; restart after delete failure performs cleanup without another sink write.
- CDC/Kafka to file/S3 remains rejected unless `allow_unsafe: true` explicitly acknowledges the documented duplicate boundary.
- DAG DLQ records without `dag_node` remain stored and replay returns HTTP 400.

Primary unit evidence:

- `internal/etl/checkpoint/*_test.go`
- `internal/etl/pipeline/runner_test.go`
- `internal/etl/orchestrator/orchestrator_test.go`
- `internal/etl/server/dlq_test.go`
- `internal/etl/source/kafka_test.go`
- `internal/etl/source/checkpoint_validation_test.go`
- `internal/etl/server/checkpoint_error_visibility_test.go`

Validated commands for the 2026-07-13 closure:

```sh
go test ./internal/etl/... -count=1
./hack/e2e-kafka.sh
E2E_SKIP_BUILD=1 ./hack/e2e-wide-table.sh
E2E_SKIP_BUILD=1 ./hack/e2e-lookup-state.sh
```

### PR-2.4.1 checkpoint restore fail-closed evidence (2026-08-08)

The recovery boundary now has explicit fault-injection coverage for the
standalone linear runner and DAG executor:

```text
go test ./internal/etl/... -count=1                 PASS
go test -race ./internal/etl/checkpoint ./internal/etl/pipeline ./internal/etl/orchestrator -count=1  PASS
go vet ./internal/etl/checkpoint ./internal/etl/pipeline ./internal/etl/orchestrator  PASS
```

Covered cases include checkpoint-store load errors, malformed JSON, unknown
envelope versions, missing envelope source payloads, valid legacy positions,
and DAG source-open failures. A load/validation failure sets the pipeline to
`failed`, cancels the DAG context where applicable, and does not call source
`Open`. At the time of this evidence capture, external source acknowledgement
ordering and source-specific cursor validation were listed as bounded
follow-ups; they are now recorded in the `PR-2.4.2` and `PR-2.4.3` sections
below.

### PR-2.4.2 external acknowledgement ordering evidence (2026-08-08)

The source cursor lifecycle is now explicitly split into candidate generation,
durable checkpoint persistence, and external acknowledgement:

```text
CheckpointForRecord (no external side effect)
    -> CheckpointStore.Save
    -> AckCheckpoint (Kafka offset / PostgreSQL WAL status)
```

Evidence:

```text
go test ./internal/etl/source ./internal/etl/pipeline ./internal/etl/orchestrator -count=1  PASS
go test -race ./internal/etl/source ./internal/etl/pipeline ./internal/etl/orchestrator -count=1  PASS
go vet ./internal/etl/core ./internal/etl/source ./internal/etl/pipeline ./internal/etl/orchestrator  PASS
CONTAINER_CLI=docker ./hack/e2e-kafka.sh  PASS
CONTAINER_CLI=docker E2E_SKIP_BUILD=1 ./hack/e2e-postgres-cdc.sh  PASS
```

The tests cover Kafka auto-commit disabled, no mark/commit during
`CheckpointForRecord`, active-session acknowledgement, PostgreSQL send failure
without `committedLsn` advancement, keepalive 0/0 and durable reconnect behavior
before the first acknowledged LSN, and linear/DAG save-before-ack fault ordering.
The bounded `mysql_snapshot_cdc` cursor and handoff follow-up is covered in
the PR-2.4.3 evidence below. Sarama's
`ConsumerGroupSession.Commit()` does not return a broker result; synchronous
session loss is rejected before commit, while broker commit errors remain
visible through Sarama's consumer-group error channel.

### PR-2.4.3 snapshot cursor and handoff evidence (2026-08-08)

The snapshot source now has two explicit cursor layers:

```text
snapshot producer read-ahead (in-memory only)
    -> records reaching the sink
    -> CheckpointForRecords (candidate)
    -> durable CheckpointStore.Save
    -> AckCheckpoint (apply durable snapshot/CDC cursor)
```

The durable snapshot position contains per-table numeric/string cursors plus
the binlog file/position captured at the snapshot handoff. A restart or canal
reconnect discards producer read-ahead and starts from that durable position;
an acknowledgement failure therefore replays rather than skips rows. The
linear runner and DAG executor both preserve the complete source batch
represented by the sink boundary when building this candidate, including
multi-table snapshot batches.

Producer completion alone does not persist `phase: cdc`, because snapshot rows
may still be buffered before the sink. The durable phase changes only when an
actual CDC record is sink-acknowledged; a restart before that point reopens the
snapshot cursor, drains any empty tail pages, and continues from the original
handoff without skipping buffered rows.

Unit and static evidence:

```text
go test ./internal/etl/... -count=1
go test -race ./internal/etl/source ./internal/etl/pipeline ./internal/etl/orchestrator -count=1
go vet ./internal/etl/core ./internal/etl/source ./internal/etl/pipeline ./internal/etl/orchestrator
git diff --check
```

All four commands passed. The targeted tests cover producer read-ahead
isolation, numeric and ordered string cursors, local-wall-clock `time.Time`
encoding, legacy `LastID` positions, multi-table batches,
snapshot-to-CDC transition, durable reconnect position, missing/invalid
cursor and handoff fail-closed behavior, channel EOF, linear/DAG batch
checkpoint propagation (including throttled pending-batch accumulation), and
checkpoint-generation failure blocking.

Path evidence:

```text
CONTAINER_CLI=docker ./hack/e2e-snapshot-cdc.sh
CONTAINER_CLI=docker ./hack/e2e-snapshot-cdc-crash.sh
CONTAINER_CLI=docker E2E_SKIP_BUILD=1 ./hack/e2e-snapshot-cdc-clickhouse.sh
CONTAINER_CLI=docker ./hack/e2e-snapshot-cdc-heteropk.sh
```

All four scripts passed. The local container environment already had the
shared `etl-mysql-source` and ClickHouse containers, so compose printed
name-in-use warnings before reusing the healthy services. The scripts still
exited 0 and verified snapshot rows, post-handoff CDC, snapshot crash/restart,
CDC restart, schema drift, checkpoint reset replay absorption, ClickHouse
outage to DLQ/replay, and numeric/string heterogeneous-PK behavior. The
ClickHouse script now asserts `phase: cdc` only after a real CDC record is
acknowledged, including after reset, rather than treating producer completion
as a durable phase transition. The path remains at-least-once: use stable
business keys/upsert or an equivalent deduplication strategy at the sink.

### IT-2/T2.4 ClickHouse source-order evidence (2026-09-05)

ClickHouse no longer assigns versions from sink wall-clock arrival. In
`version_mode: source_order`, snapshot rows use the MySQL binlog handoff captured
before the consistent read; CDC uses binlog file/position; Kafka uses
partition/offset. Native and HTTP writes preserve the full UInt64 value and add
an UInt8 tombstone consumed by `ReplacingMergeTree(version, deleted)`.

Targeted gates:

```text
go test ./internal/etl/core/... ./internal/etl/source ./internal/etl/sink ./internal/etl/server -count=1
go test -race ./internal/etl/core/... ./internal/etl/source ./internal/etl/sink ./internal/etl/server -count=1
```

Container evidence against `openetl-go-etl:dev` image
`8ded68e84e7434b591cc83d4fbb80f8171bdeb190f61e1182e574a489f4fa7c9`:

```text
CONTAINER_CLI=docker E2E_SKIP_BUILD=1 ./hack/e2e-clickhouse-replay-ordering.sh  PASS
CONTAINER_CLI=docker ./hack/e2e-binlog-purged-resnapshot.sh                  PASS
CONTAINER_CLI=docker E2E_SKIP_BUILD=1 ./hack/e2e-kafka-multitable-clickhouse.sh PASS
CONTAINER_CLI=docker E2E_SKIP_BUILD=1 ./hack/e2e-wide-table.sh               PASS
CONTAINER_CLI=docker E2E_SKIP_BUILD=1 ./hack/e2e-clickhouse-autocreate.sh     PASS
CONTAINER_CLI=docker E2E_SKIP_BUILD=1 ./hack/e2e-clickhouse.sh                PASS
CONTAINER_CLI=docker E2E_SKIP_BUILD=1 ./hack/e2e-snapshot-cdc-crash.sh        PASS
```

The first script proves that a newer UPDATE remains visible after an older DLQ
record is replayed, a newer DELETE is not resurrected by an older INSERT, and an
offset-0 replay leaves `FINAL` business state unchanged. It also validates
`_version UInt64`, `_is_deleted UInt8`, the two-argument engine, schema drift,
restart, outage, and DLQ recovery. Multi-table metadata keys and tombstones are
covered separately. Window aggregates explicitly use append/MergeTree because a
derived aggregate has no single connector-owned position; preflight rejects that
combination in source-order mode.

Residual boundary: source positions are comparable only inside one lineage.
MySQL `RESET MASTER`, PITR, or failover to a lower binlog coordinate requires a
new/rebuilt ClickHouse target version domain; Kafka requires a stable partition
for each business key. Legacy `Int64` wall-clock versions and one-argument
ReplacingMergeTree tables are rejected and must be migrated by rebuild/switch,
not mixed in place. This remains checkpointed at-least-once, not exactly-once.

### IT-2/T2.6 frozen DLQ identity and controlled replay (2026-09-05)

Every new SQL-backed DLQ row now freezes the source payload, original
primary-key declaration, failure/missing columns, format contract,
before-image state, source/target coordinates, and replay provenance. Legacy
rows without this column normalize to `legacy_unknown` + `repair_required`;
they are never interpreted with the current source configuration. Non-UTF-8
source bytes are retained losslessly as base64.

Metadata-PK replay reconstructs a Key only from the frozen contract and
validates identity again after transforms. The single-row repair API requires
the operator's declaration to match both the historical Record and the
explicit target `pk_columns`; partial composite keys, an absent original
declaration, static-set drift, and legacy key-changing UPDATEs remain stored.
Sink acknowledgement is then persisted as `sink_acked` before deletion. A
checkpoint-write failure leaves the row pending and permits an at-least-once
retry; a delete failure leaves `sink_acked`, so restart performs cleanup only.

Evidence against `openetl-go-etl:dev`
`sha256:0076a91ddef463046bb97e604cecd25b335c97d99c66b62dfd4eaaee6519d928`:

```text
CONTAINER_CLI=docker E2E_SKIP_BUILD=1 ./hack/e2e-kafka-canal-identity.sh  PASS
CONTAINER_CLI=docker ./hack/e2e-storage-mysql.sh                         PASS (MySQL 8)
CONTAINER_CLI=docker ./hack/e2e-storage-postgres.sh                      PASS (PostgreSQL 16)
go test -race ./internal/etl/core/... ./internal/etl/pipeline ./internal/etl/server ./internal/etl/storage/... -count=1  PASS
go test ./... -count=1                                                   PASS
go vet ./internal/etl/core/... ./internal/etl/pipeline ./internal/etl/server ./internal/etl/storage/...  PASS
```

The focused container path validates API-visible frozen context, process
restart, successful legacy composite-key repair, and durable quarantine for
partial composite, key-changing, and unknown-contract records. Fault-injection
unit tests cover both sides of the sink-ack replay checkpoint and backup/restore
tests preserve the full context. The remaining GAP-7 work is T2.7 cross-sink
conformance and path certification, not a broader exactly-once claim.

### PR-2.4.4 source position semantic validation and recovery visibility (2026-08-08)

Every built-in checkpoint codec now participates in the optional
`core.CheckpointValidator` contract. The runner loads and unwraps the persisted
position, then invokes this validator before `Source.Open`; the DAG source path
uses the same order. Source `Open` methods also repeat the local validation as a
defensive SDK/direct-call boundary.

Covered codecs and compatibility rules:

- Kafka requires a non-empty partition-offset map, rejects negative partitions,
  offsets below the valid `-1` replay sentinel, and a checkpoint topic that does
  not match the configured topic.
- HTTP and REST require their mode-specific page/offset/page-count shape and
  reject negative or out-of-range numeric positions; terminal empty cursor or
  page-token values remain valid when the page count is present.
- Redis and demo positions remain compatible with the historical plain decimal
  representation while rejecting malformed or negative counters.
- MySQL batch requires a non-negative `last_id`; MySQL CDC requires a verified
  file/positive position or a valid enabled GTID, and understands legacy
  `binlog_file`/`binlog_pos` names.
- PostgreSQL CDC accepts empty LSN only in an enabled snapshot phase; CDC LSNs
  and phases are parsed before the replication connection opens.
- File positions require a non-negative record offset and optional non-negative
  byte offset. MySQL snapshot+CDC validates phase, durable binlog handoff, and
  per-table numeric/string cursor maps. Feishu Sheet explicitly rejects a
  persisted cursor because that source does not yet implement durable row resume.

Checkpoint startup failures populate `last_error`, `last_error_code`, and
`last_error_remediation` in linear and DAG stats. Pipeline start returns HTTP
422 instead of a successful error-shaped response. `/api/v2/health` keeps raw
connector errors out of the flattened probe response while exposing a
`pipeline_issues` JSON map containing status, stable error code, and safe
remediation. The WebUI pipeline detail/issues view renders that remediation and
offers retry-start and log inspection; it does not suggest checkpoint reset as
the default repair.

Evidence:

```text
go test ./internal/etl/... -count=1  PASS
go test -race ./internal/etl/source ./internal/etl/pipeline ./internal/etl/orchestrator ./internal/etl/server -count=1  PASS
go vet ./internal/etl/core ./internal/etl/source ./internal/etl/pipeline ./internal/etl/orchestrator ./internal/etl/server ./internal/etl/telemetry  PASS
cd web && npm run typecheck && npm run build  PASS
bash hack/e2e-ui.sh  PASS (117 passed, 0 failed)
git diff --check  PASS
```

The E2E permanently seeds a file pipeline with `{}` as its persisted source
position, verifies the start request returns 422 and the stable
`checkpoint.file.invalid` code appears in API stats, then verifies the WebUI
recovery panel, remediation text, and safe retry action.

## RPO / RTO (release declaration)

| Metric | Declaration |
| --- | --- |
| **RPO** | Last durable checkpoint. If sink ack succeeds but checkpoint persistence fails, the batch replays; it is never silently skipped. |
| **RTO** | Process restart + source/sink reconnect + at most one `checkpoint_interval_sec` recovery window (standalone). |
| **Duplicate upper bound** | Uncheckpointed last batches; absorbed by upsert / ReplacingMergeTree / content-addressed keys, or explicitly visible on append sinks. |

## Boundary policies (PR-2.3)

- PostgreSQL CDC `TRUNCATE`: default `on_truncate: error` fails closed; `skip` requires `allow_unsafe: true` and leaves residual sink rows.
- CDC → file/S3 append: blocked unless `allow_unsafe: true`.
- DAG multi-sink fanout: blocked unless `allow_unsafe: true` (non-atomic across sinks).

## Non-Claims

- No Kafka transaction exactly-once guarantee.
- No atomic transaction across source offset, Redis state, and an external sink.
- No cross-sink atomic fanout.
- A replay-safe result depends on the documented sink strategy: upsert/key/version, ReplacingMergeTree, deterministic object key, or explicit deduplication.
