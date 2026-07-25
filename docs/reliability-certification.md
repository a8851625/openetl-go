# Reliability Certification Matrix

This matrix is the evidence source for OpenETL-Go production-candidate recovery boundaries. The default guarantee is at-least-once: a sink acknowledgement happens before the corresponding source checkpoint is persisted. A crash or checkpoint-store failure in between can replay records; it must not silently skip them.

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
5. If a failed record cannot be written to the DLQ, later successful batches cannot advance past it during the same run. Restart reopens the source from the last durable checkpoint.
6. A checkpoint-throttled sink acknowledgement is retained as a pending boundary. The write-loop timer persists it after `checkpoint_interval_sec` even when the stream becomes idle; Stop/EOF force the same boundary to durable storage.
7. Kafka offset `0` is stored explicitly. Missing partition state is not conflated with the valid zero offset.

## Production-Candidate Matrix

| Path / `path_id` | Happy path | Replay absorption | Failure / DLQ | Restart / crash | Broker / rebalance | Residual boundary |
| --- | --- | --- | --- | --- | --- | --- |
| **MySQL CDC -> MySQL upsert** `mysql_cdc__mysql_upsert` (PR-2 forced) | `hack/e2e-path-mysql-cdc-mysql.sh`, `hack/e2e-cdc-mysql.sh` | MySQL `batch_mode: upsert` + stable PK | Sink privilege/outage -> DLQ -> replay | SIGKILL + checkpoint resume (`hack/e2e-cdc-crash-recovery.sh`, path matrix) | Not applicable | Source binlog and sink are not a distributed transaction |
| **MySQL snapshot+CDC -> ClickHouse** `mysql_snap_cdc__ch_rmt` (PR-2 forced) | `hack/e2e-snapshot-cdc-clickhouse.sh` | ReplacingMergeTree absorbs checkpoint reset replay | ClickHouse outage DLQ/replay | Snapshot and CDC crash recovery (`hack/e2e-snapshot-cdc-crash.sh`) | Not applicable | Source binlog and sink are not a distributed transaction; use `FINAL` for exact current state |
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
- Kafka offset zero is retained and an idle stream flushes the latest throttled checkpoint boundary after the configured interval.
- CDC/Kafka to file/S3 remains rejected unless `allow_unsafe: true` explicitly acknowledges the documented duplicate boundary.
- DAG DLQ records without `dag_node` remain stored and replay returns HTTP 400.

Primary unit evidence:

- `internal/etl/checkpoint/*_test.go`
- `internal/etl/pipeline/runner_test.go`
- `internal/etl/orchestrator/orchestrator_test.go`
- `internal/etl/server/dlq_test.go`
- `internal/etl/source/kafka_test.go`

Validated commands for the 2026-07-13 closure:

```sh
go test ./internal/etl/... -count=1
./hack/e2e-kafka.sh
E2E_SKIP_BUILD=1 ./hack/e2e-wide-table.sh
E2E_SKIP_BUILD=1 ./hack/e2e-lookup-state.sh
```

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
