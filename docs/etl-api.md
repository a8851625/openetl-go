# ETL API v2

## Authentication
- Set `ETL_API_TOKEN` to protect ETL API routes.
- Clients may pass `X-API-Token: <token>` or `Authorization: Bearer <token>`.
- `GET /api/v2/health` remains unauthenticated for liveness checks.

## Secret fields

Connection responses and linear/DAG pipeline specs use connector descriptors to
mask secret fields, including JDBC/dbt/enricher/lookup DSNs. Connections return
`******`; specs retain the historical first/last-character mask for longer values.
Submitting a returned placeholder for an existing secret preserves its stored value. The same
descriptor policy drives encryption at rest; explicit non-secret fields remain
readable. Undeclared fields use legacy secret-name matching with a warning.

`secret_encryption` health reports stored plaintext connection/settings findings.
See [the runbook](./ops-runbook.md#11b-existing-plaintext-secrets) for the offline
`--check-secrets` and idempotent `--remediate-secrets` commands.

## Restore-failed pipelines

`GET /api/v2/pipelines` and `GET /api/v2/pipelines/{id}` continue to return a
stored pipeline when startup cannot reconstruct its runner. Its status is
`restore_failed`, and `restore_error` contains the failure `stage`, stable
`code`, `message`, operator `remediation`, `previous_status`, and `failed_at`.
Starting or resuming that pipeline returns HTTP `409` with the same diagnostic
instead of a misleading `404`.

`GET /api/v2/health` reports the per-pipeline value `restore_failed`, overall
status `degraded`, `restore_failed_count`, and a JSON `pipeline_issues` map.
Repair the stored spec or referenced connection and restart; restore-state
updates never reset the checkpoint. See [runtime-modes.md](./runtime-modes.md)
for strict/non-strict startup configuration and recovery procedure.

## DLQ APIs

Dead-letter records include `error_class` when the runtime can classify the failure. Current classes are `transient`, `data`, `schema`, `auth`, `config`, `programming`, and `unknown`. Retry policy uses the same classifier: transient and unknown errors are retried, while data/schema/auth/config/programming errors fail fast into DLQ or fail the operation.

### List DLQ Records
`GET /api/v2/dlq/{pipeline}`

Query parameters:
- `limit`: page size. Defaults to `100`; `0` also means the default. Use with `offset` to page.
- `offset`: number of rows to skip. Listings are newest-first; replay uses the same filters but drains oldest-first.
- `from`: include records at or after this RFC3339Nano timestamp.
- `until`: include records at or before this RFC3339Nano timestamp.
- `contains`: substring match against the serialized failed record payload (LIKE wildcards in the value are escaped).
- `error_contains`: substring match against the DLQ error string (wildcards escaped).
- `error_class`: exact match on the indexed `error_class` column (`transient`, `data`, `schema`, `auth`, `config`, `programming`, `unknown`).

SQL-backed DLQ responses include stable `id` values for per-record delete/replay, `attempt` (how many times this exact record has been captured; a repeated capture refreshes the existing row instead of appending a duplicate), and `record_hash`. DAG DLQ responses also include `dag_node` when the failure was recorded with node context. New rows also expose `identity_context`: the raw source payload (UTF-8 in `source_bytes`, otherwise base64 in `source_bytes_base64`), original primary-key declaration, identity failure/missing columns, frozen format-contract ID and before-image state, source/target coordinates, replay provenance/state, and replay acknowledgement. `raw_payload` is retained as forensic evidence for the exact source bytes; automatic replay reconstructs identity from the frozen primary-key declaration, format contract and before-image state rather than re-parsing it. `pipeline_version` is always 0 today (no authoritative runtime spec-version source). Treat the raw payload as production data and protect this API with authentication and normal data-access controls.

`GET /api/v2/pipelines` exposes `dlq_backlog` (current DLQ row count for the pipeline) and `dlq_backlog_known`. `stats.records_dlq` is a cumulative per-process counter: it resets on restart and never decreases after replay, so it must not be used as a backlog. `dlq_backlog_known=false` means the count query failed; the backlog is unknown, not zero.

Examples:
```sh
curl -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/dlq/orders?limit=20'

curl -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/dlq/orders?contains=customer_id'

curl -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/dlq/orders?error_contains=Duplicate'
```

### Replay DLQ Records
`POST /api/v2/dlq/{pipeline}/replay`
`POST /api/v2/dlq/{pipeline}/{id}/replay`

Replay uses the same query parameters as list. Replayed records are transformed again and written to the configured sink. For a metadata-PK pipeline, replay first proves a complete identity from the record or its frozen format contract and validates it again after transforms. It never derives historical key semantics from the current source configuration. Unprovable rows remain in `repair_required` or `quarantined` and return HTTP `409` with `id`, `replay_state`, and `reason`.

After a sink acknowledges a replay, the SQL-backed row is first updated to `sink_acked`, then deleted. A crash or storage failure before that replay checkpoint may write the row again (the documented at-least-once boundary); a crash or delete failure after it causes the next request to perform cleanup only, without another sink write. This replay checkpoint does not advance the source checkpoint.

Use the ID endpoint for deterministic one-record replay and UI/API feedback such as `{"replayed":1}`. Linear pipeline DLQ replay is supported. DAG pipeline DLQ replay is supported for records that include `dag_node`: sink-node failures are written back to that sink, and transform-node failures resume at that transform and route downstream. Legacy DAG DLQ records without `dag_node` return HTTP `400` with `{"error":"...dag_node...","replayed":0}` and are not deleted. DAG replay enforces the same metadata-PK identity gate as linear replay for every metadata-PK sink reachable from the failing node.

Replay is serialised per pipeline instance: a second concurrent replay request returns HTTP `400` (`another DLQ replay is already running`). Replay runs detached from the request context with a 10-minute cap, so a client disconnect does not abort a batch mid-way. A pipeline that pairs a CDC source with a non-idempotent sink and does not set `allow_unsafe` refuses replay (HTTP `400`), because replay writes directly to the sink.

`POST /api/v2/dlq/{pipeline}/replay?dry_run=1` (also `dry-run=1`) returns a preview instead of writing:

```json
{"dry_run":true,"replayable":2,"blocked":1,"items":[{"id":1,"replayable":true},{"id":2,"replayable":false,"state":"repair_required","reason":"..."}]}
```

The preview applies the replay identity gate per row and mutates nothing: no sink write, no DLQ state change. Transform-output validation and sink idempotency are still only proven by a real replay.

Examples:
```sh
curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/dlq/orders/replay?contains=9901'

curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/dlq/orders/123/replay'

curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/dlq/orders/replay?from=2026-06-06T00:00:00Z&until=2026-06-07T00:00:00Z'
```

### Repair one legacy identity declaration

`PUT /api/v2/dlq/{pipeline}/{id}/identity`

This is a single-row, explicit compatibility gate for historical metadata-PK DLQ rows. It is available when the linear sink has `pk_columns_from_metadata: true` and an explicit static `pk_columns` safety set, and for DAG rows when a metadata-PK sink is reachable from the row's `dag_node`. `primary_key_columns` in the request must exactly match both the declaration already stored in the historical record and that static target set. The server verifies that every value exists and rejects partial keys, invented declarations, and every legacy primary-key-changing UPDATE. A successful repair durably stores the reconstructed Key with `legacy_verified` provenance before replay is allowed. There is no bulk or automatic repair endpoint.

```sh
curl -X PUT -H "X-API-Token: $ETL_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"primary_key_columns":["tenant_id","id"]}' \
  'http://127.0.0.1:8001/api/v2/dlq/orders/123/identity'
```

### Delete DLQ Records
`DELETE /api/v2/dlq/{pipeline}`

Delete uses the same query parameters as list (`limit`/`offset` are ignored for deletes; `error_class` selects). If no selective filter is provided, every DLQ row for the pipeline is removed and the response reports the real removed count, e.g. `{"deleted":7}`. The response never uses a sentinel such as `-1`, and `dlq_delete_count` only increases.

Deleting a pipeline also deletes its DLQ rows in the same transaction: once the spec is gone, replay is impossible and the rows would otherwise stay invisible while consuming the global DLQ retention budget.

Examples:
```sh
curl -X DELETE -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/dlq/orders?error_contains=unknown%20column'

curl -X DELETE -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/dlq/orders'
```

## Checkpoint APIs

Pipeline responses expose three lifecycle fields: `desired_state` is the
durable operator intent (`running`, `stopped`, or `paused`), `observed_state`
is the last persisted runtime state, and `generation` is the checkpoint
fencing token. Start/resume persists `desired_state=running` before opening a
source. Stop/pause persists the quiescent intent before changing the runner;
if that persistence fails, the request is non-2xx and the runner is unchanged.

Checkpoint `set` and `reset` are administrative operations and are accepted
only while the pipeline is stopped or paused. A running pipeline receives HTTP
`409`, code `pipeline_not_quiescent`, and remediation. A successful operation
atomically advances `generation` and changes the checkpoint. Any in-flight
write from an older generation is rejected, logged, and counted by
`checkpoint_fenced_total`; it is never retried with the new token. Delivery
therefore remains at-least-once, and the sink must absorb replay duplicates.

### Set Kafka Replay Offset
`POST /api/v2/pipelines/{pipeline}/checkpoint/set`

For Kafka sources, use structured checkpoint requests instead of hand-writing the internal checkpoint JSON. `offset` and `replay_from_offsets` mean "start reading from this offset on the next start"; OpenETL-Go stores `offset-1` internally because Kafka commits the next offset after a successful sink write.

Examples:
```sh
curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"source":"kafka","topic":"debezium.orders","partition":0,"offset":42}' \
  'http://127.0.0.1:8001/api/v2/pipelines/orders/checkpoint/set'

curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"source":"kafka","topic":"debezium.orders","replay_from_offsets":{"0":42,"1":1000}}' \
  'http://127.0.0.1:8001/api/v2/pipelines/orders/checkpoint/set'
```

Use `{"mode":"last_committed","offsets":{"0":41}}` when setting the stored committed offsets directly. Legacy raw checkpoints remain supported with `{"position":{...}}`.

### Reset Source Checkpoint

`POST /api/v2/pipelines/{pipeline}/checkpoint/reset`

The response includes `generation` plus `reset_semantics` (`effect`,
`external_boundary`, and `operator_action`). Reset only changes OpenETL's
durable checkpoint; its exact next-start boundary is source-specific:

| Source | Next-start boundary after reset |
| --- | --- |
| `kafka` | OpenETL offsets are removed, but broker consumer-group offsets and topic retention are unchanged. Reset the group separately for older replay. |
| `mysql_cdc` | The connector discovers the current MySQL master position; reset is not “replay from the oldest binlog”. Use `checkpoint/set` with a retained position for controlled replay. |
| `mysql_snapshot_cdc` | Snapshot/handoff state is removed and the full snapshot runs again before CDC; every source row can be redelivered. |
| `postgres_cdc` | The saved LSN is removed, while the replication slot and retained WAL remain the external boundary. Reset does not reposition or recreate the slot. |

Stop or pause the pipeline and wait for it to quiesce before invoking either
checkpoint endpoint. Neither endpoint resets an external broker group,
replication slot, or database log position automatically.

### Re-sync a Single Table (snapshot+CDC)

`POST /api/v2/pipelines/{pipeline}/resync`

Re-runs the snapshot phase for **named tables only** on a `mysql_snapshot_cdc`
pipeline, instead of resetting the whole checkpoint and re-delivering every
table.

```sh
curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"tables":["orders"]}' \
  'http://127.0.0.1:8001/api/v2/pipelines/shop-sync/resync'
```

How it works: the per-table snapshot cursors already live in the checkpoint, so
the endpoint drops the cursors of the named tables, keeps the existing binlog
anchor as the new snapshot handoff, and sets the checkpoint phase back to
`snapshot`. Tables that were not named keep their cursor and emit nothing on the
next start.

Requirements and boundaries:

| Item | Behavior |
| --- | --- |
| Source type | `mysql_snapshot_cdc` only; other sources have no per-table snapshot cursor (use `checkpoint/reset`). |
| Pipeline state | Must be quiescent (stopped or paused); otherwise HTTP 409 `pipeline_not_quiescent`. |
| Sink | Must absorb re-delivered rows (upsert / business key / version column). A plain-insert sink duplicates the re-synced rows. |
| Ordering | The older binlog position is reused as the handoff, so re-read rows never order after live CDC rows. |
| Table names | Must appear in the pipeline's `source.config.tables` (or `table`); unknown names are rejected with HTTP 400. |

### Compact File / Object-Store Output

`POST /api/v2/pipelines/{id}/output/compact`

Folds the committed data objects of one date partition into a single object and publishes a
compaction manifest that retires the replaced keys. Readers resolve the committed view from the
manifest set (`CommittedObjectKeys`), so every row is visible exactly once; the replaced objects are
kept on disk, which makes the pass reversible. Requires `manifest: true` on an `s3` / `file_sink`
sink.

```bash
curl -X POST "$BASE/api/v2/pipelines/$ID/output/compact" \
  -H "X-API-Token: $TOKEN" -H 'Content-Type: application/json' \
  -d '{"day":"2026/11/01/","min_objects":8,"dry_run":true}'
```

Body (all optional): `day`, `min_objects` (default 4), `max_objects` (default 32),
`max_object_bytes` (default 1 MiB), `dry_run`.

Response: `groups[]` with `merged_key`, `manifest_key`, `replaced_keys`, `source_batch_ids`, `rows`,
`bytes_before`, `bytes_after`, plus totals and `skipped_groups`.

Two related guarantees come with manifests:

- **Cross-day replay**: a batch whose content was already committed is not committed twice. A replay
  on another day writes its data object under that day's prefix but publishes no second manifest, so
  the duplicate stays uncommitted (invisible to readers, safe to delete).
- **Reversible compaction**: the pass never deletes data; it only rewrites which objects count as
  committed.

This is not a lakehouse transaction: there is no atomic multi-object commit, no cross-table snapshot
isolation, and no automatic retry of a half-finished pass (re-run it — the pass is idempotent).

## Reconcile Source and Sink (Data Verification)

`POST /api/v2/pipelines/{pipeline}/reconcile` runs an on-demand comparison of the
source and target tables; `GET` on the same path returns the supported matrix
and limits.

```sh
curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"tables":["orders"],"deep":true,"buckets":16,"sample_rows":20}' \
  'http://127.0.0.1:8001/api/v2/pipelines/shop-sync/reconcile'
```

The comparison is per table: row count, then (unless `deep:false`) a content
digest over the columns both sides share, then — when the sides disagree and the
table has a single numeric key — a bucketed digest comparison that localises
which key range drifted.

Verdicts: `match`, `count_mismatch`, `content_mismatch`, `schema_mismatch`,
`missing_target_table`, `error`. Every response carries `limits`, so the report
cannot be mistaken for an exactly-once proof.

`sample_rows` (0 = off, max 100) adds a **row-level diff**: inside the first mismatching buckets
(up to 3, or the head of the table when no bucket key is available) the reconciler reads up to
`sample_rows` rows per side with a per-row digest and reports `row_diffs[]` as
`{key, kind}` where `kind` is `missing_in_target`, `extra_in_target` or `value_mismatch`.
`row_diff_scope` says whether the sample covered buckets or only the table head, and
`row_diff_truncated` flags a sample that hit its bound. It identifies what to fix; it is not a
full table diff.

Supported pairs (both ends need a SQL query surface):

| Source | Sinks |
| --- | --- |
| `mysql_batch`, `mysql_cdc`, `mysql_snapshot_cdc` | `mysql`, `doris`, `clickhouse`, `postgres` |
| `postgres_cdc` | `postgres` |

Kafka / file / http / object-store / Elasticsearch ends are rejected with an
explicit reason. Pipelines that declare `transforms` are also rejected: the
comparison aligns raw source and target columns, which transformed fields would
silently invalidate.

Reconciliation is a point-in-time read-only comparison. It does not lock the
source, so running it while the pipeline is live can report in-flight
differences; re-run after the checkpoint goes quiet for a decision-grade result.

## Plugin Metadata

Discover registered plugins and their basic capabilities.

```sh
curl -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/plugins'
```

Response includes legacy lists plus `metadata`:

```json
{
  "sources": ["file", "mysql_cdc"],
  "sinks": ["file_sink", "clickhouse"],
  "transforms": ["identity", "lua"],
  "metadata": {
    "sources": {
      "mysql_cdc": {
        "required": ["host", "user", "database", "tables"],
        "capabilities": ["cdc", "checkpoint", "schema_descriptor_single_table"],
        "maturity": "production"
      }
    }
  }
}
```

## Plugin Dry Run

Run an installed transform plugin against one sample record. Multi-output
plugins return every output in `records` and the count in `output_count`; `record`
and `output` keep the first output for older clients.

```sh
curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name":"raw-parser","record":{"operation":"INSERT","data":{"id":1},"metadata":{"source":"ui","table":"sample"}}}' \
  'http://127.0.0.1:8001/api/v2/plugins/dry-run'
```

```json
{
  "name": "raw-parser",
  "kind": "transform",
  "filtered": false,
  "output_count": 2,
  "records": [
    {"operation": "INSERT", "data": {"id": 1, "idx": 1}, "metadata": {"source": "ui", "table": "sample"}},
    {"operation": "INSERT", "data": {"id": 1, "idx": 2}, "metadata": {"source": "ui", "table": "sample"}}
  ]
}
```

## AI Context And Generation

AI-assisted DAG generation uses the same connector descriptors, plugin schema,
component docs, and validate/preflight path as the UI and YAML flows. It only
drafts ordinary pipeline/DAG specs; it does not start pipelines or bypass user
confirmation.

```sh
curl -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/ai/context'
```

```sh
curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"Read Debezium orders from Kafka and upsert to MySQL ODS."}' \
  'http://127.0.0.1:8001/api/v2/ai/generate'
```

The generation response includes `yaml`, `context_pack_version`, `validation`,
and `review` fields. Resolve or explicitly accept `review.missing_fields`,
`review.risk_flags`, and `review.requires_confirmation` before applying and
starting the generated spec.

## Spec Validation

Validate a pipeline spec without creating a runtime pipeline.

```sh
curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"spec":{"name":"example","source":{"type":"file","config":{}},"sink":{"type":"file_sink","config":{}}}}' \
  'http://127.0.0.1:8001/api/v2/specs/validate'
```

Response:

```json
{
  "valid": true,
  "warnings": [],
  "spec": {
    "name": "example",
    "batch_size": 1000,
    "checkpoint_interval_sec": 30,
    "backpressure_buffer": 100
  }
}
```

When preflight has enough context, the response also includes
`preflight.recommendations`: operator-reviewed config patches such as
`sink.config.batch_mode=upsert`, `sink.config.pk_columns=["id"]`,
`sink.config.schema_drift=add_columns`, `transforms=[{type:type_convert,...}]`,
`sink.config.prefix=orders/`, `sink.config.key_column=id`,
`sink.config.auto_create_topic=true`, `batch_size=500`, or `dlq.enable=true`.
The Web wizard can apply these patches to the draft spec before creation.

## Connection Test

Build and optionally open one source, sink, or transform config without creating a pipeline.

```sh
curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"kind":"source","type":"file","config":{"path":"/app/data/input/customers.jsonl","format":"json"},"open":true}' \
  'http://127.0.0.1:8001/api/v2/connections/test'
```

Response:

```json
{
  "ok": true,
  "kind": "source",
  "type": "file",
  "opened": true
}
```

## Saved Connection Context

Fetch a saved connection together with its descriptor, health, recommended runtime parameters, and best-effort source/sink introspection.

```sh
curl -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/connections/file-source/context'
```

Response:

```json
{
  "connection": {
    "name": "file-source",
    "kind": "source",
    "type": "file",
    "last_status": "ok"
  },
  "recommendations": [
    {"field": "schedule.type", "value": "once"},
    {"field": "batch_size", "value": 1000},
    {"field": "checkpoint_interval_sec", "value": 30}
  ],
  "introspection": {
    "ok": true,
    "type": "file",
    "schema": [
      {"name": "id", "data_type": "string"},
      {"name": "name", "data_type": "string"}
    ],
    "sample": [
      {"operation": "INSERT", "data": {"id": "1", "name": "Alice"}}
    ]
  }
}
```

Current built-in adapters cover file/HTTP/demo sampling, MySQL and PostgreSQL table/schema metadata, Kafka topic/partition metadata, and sink target metadata for MySQL, PostgreSQL, ClickHouse, Doris, Kafka, Elasticsearch/OpenSearch, File, and S3/local-fallback output targets. File/S3 context returns `introspection.targets` with the resolved directory or bucket, prefix, format, and writability/bucket-existence status. Introspection is advisory control-plane context; pipeline startup still relies on `spec validate` and preflight as the enforcement gate.

## Transform Dry Run

Execute a transform chain on one sample record without starting a pipeline.

```sh
curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"transforms":[{"type":"identity","config":{}}],"record":{"operation":"INSERT","data":{"id":1},"before":{},"metadata":{"source":"ui","table":"sample"}}}' \
  'http://127.0.0.1:8001/api/v2/transforms/dry-run'
```

Response:

```json
{
  "filtered": false,
  "output_count": 1,
  "record": {
    "operation": "INSERT",
    "data": {"id": 1}
  },
  "records": [
    {
      "operation": "INSERT",
      "data": {"id": 1}
    }
  ]
}
```

For `BatchTransform` implementations such as `flat_map` / `udtf`, `records` contains every output record and `record` is the first output for backward compatibility. Record-level parser errors are returned as `errors` with `partial_error: true` instead of hiding successful outputs.

## Specs Reload

Load new pipeline specs from the configured `etl.specsDir` without replacing already-loaded pipelines.

```sh
curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/specs/reload'
```

Response:

```json
{
  "loaded": ["new-pipeline"],
  "skipped": {"existing.yaml": "pipeline existing already loaded"},
  "errors": {}
}
```

## Audit Events

Returns recent mutation events persisted in the configured SQL storage backend. Audit logging can be disabled with `ETL_AUDIT_ENABLED=false`, `etl.audit.enabled: false`, or `--audit-enabled=false`.

```sh
curl -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/audit?limit=50'
```

Response:

```json
{
  "events": [
    {
      "timestamp": "2026-06-07T00:00:00Z",
      "action": "specs.reload",
      "target": "./pipes",
      "method": "POST",
      "path": "/api/v2/specs/reload",
      "remote": "127.0.0.1:52100"
    }
  ]
}
```
