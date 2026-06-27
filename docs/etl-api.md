# ETL API v2

## Authentication
- Set `ETL_API_TOKEN` to protect ETL API routes.
- Clients may pass `X-API-Token: <token>` or `Authorization: Bearer <token>`.
- `GET /api/v2/health` remains unauthenticated for liveness checks.

## DLQ APIs

Dead-letter records include `error_class` when the runtime can classify the failure. Current classes are `transient`, `data`, `schema`, `auth`, `config`, `programming`, and `unknown`. Retry policy uses the same classifier: transient and unknown errors are retried, while data/schema/auth/config/programming errors fail fast into DLQ or fail the operation.

### List DLQ Records
`GET /api/v2/dlq/{pipeline}`

Query parameters:
- `limit`: maximum records to return. Defaults to `100`. Use `0` for no limit.
- `timestamp`: exact RFC3339Nano DLQ timestamp to match.
- `from`: include records at or after this RFC3339Nano timestamp.
- `until`: include records at or before this RFC3339Nano timestamp.
- `contains`: substring match against the serialized failed record payload.
- `error_contains`: substring match against the DLQ error string.

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

Replay uses the same query parameters as list. Replayed records are transformed again and written to the configured sink. Successfully replayed records are deleted from the DLQ file.

Linear pipeline DLQ replay is supported. DAG pipeline DLQ replay is explicitly not supported yet because node-level replay has not been implemented. DAG replay attempts return HTTP `501` with `code: "dag_dlq_replay_unsupported"` and do not delete DLQ records.

Examples:
```sh
curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/dlq/orders/replay?contains=9901'

curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/dlq/orders/replay?from=2026-06-06T00:00:00Z&until=2026-06-07T00:00:00Z'
```

### Delete DLQ Records
`DELETE /api/v2/dlq/{pipeline}`

Delete uses the same query parameters as list. If no selective filter is provided, the entire DLQ file for the pipeline is removed.

Examples:
```sh
curl -X DELETE -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/dlq/orders?error_contains=unknown%20column'

curl -X DELETE -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/dlq/orders'
```

## Checkpoint APIs

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
        "maturity": "beta"
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
