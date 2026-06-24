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
        "required": ["host", "database", "table", "server_id"],
        "capabilities": ["cdc", "checkpoint"],
        "maturity": "stable"
      }
    }
  }
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
  "record": {
    "operation": "INSERT",
    "data": {"id": 1}
  }
}
```

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

Returns recent mutation events written to `ETL_AUDIT_LOG` or the default `data/audit.log`.

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
