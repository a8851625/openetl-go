# ETL YAML Config Reference

Pipeline specs are YAML files under `pipes/` or the configured `etl.specsDir`. Environment variables are expanded before parsing with `${VAR}` or `${VAR:-default}`.

## Top-Level Spec

```yaml
name: example-pipeline
source:
  type: file
  config: {}
transforms:
  - type: identity
    config: {}
sink:
  type: file_sink
  config: {}
batch_size: 1000
checkpoint_interval_sec: 30
backpressure_buffer: 100
retry:
  max_attempts: 3
  initial_interval_ms: 1000
  max_interval_ms: 30000
dlq:
  enable: true
```

| Field | Required | Description |
| --- | --- | --- |
| `name` | yes | Unique pipeline name. Duplicate runtime creates return `409`; duplicate files are skipped. |
| `source.type` | yes | Registered source plugin name. |
| `source.config` | no | Source-specific settings. Defaults to `{}`. |
| `transforms` | no | Ordered transform chain. Omit for no transforms. |
| `sink.type` | yes | Registered sink plugin name. |
| `sink.config` | no | Sink-specific settings. Defaults to `{}`. |
| `batch_size` | no | Sink flush size. Default `1000`. |
| `checkpoint_interval_sec` | no | Reserved interval setting; checkpoints currently advance after successful writes. Default `30`. |
| `backpressure_buffer` | no | Internal record channel size. Default `100`. |
| `retry` | no | Retry policy. Defaults shown above. |
| `dlq.enable` | no | Enable file DLQ. |

## Sources

### `file`

```yaml
source:
  type: file
  config:
    path: /app/data/input/customers.jsonl
    format: json
    delimiter: ","
    has_header: true
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `path` | yes | | File path inside the container. Mount input files under `/app/data/input` for Docker deployments. |
| `format` | no | `csv` | `json` (JSON Lines) or `csv`. |
| `delimiter` | no | `,` | CSV delimiter. |
| `has_header` | no | `true` | Whether CSV first row contains column names. |

### `http`

```yaml
source:
  type: http
  config:
    url: http://fixture:8080/items
    method: GET
    headers:
      X-API-Key: ${HTTP_API_KEY}
    pagination: page
    page_param: page
    size_param: size
    page_size: 100
    max_pages: 10
    result_key: data
    auth_token: ${HTTP_BEARER_TOKEN}
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `url` | yes | | Base URL. |
| `method` | no | `GET` | HTTP method. |
| `headers` | no | | Request headers map. |
| `pagination` | no | | `page` or empty. |
| `page_param` | no | | Page query parameter name. |
| `size_param` | no | | Page size query parameter name. |
| `page_size` | no | `100` | Page size. |
| `max_pages` | no | `100` | Maximum pages to read. |
| `result_key` | no | auto | JSON array key. Auto-detects `data`, `items`, `results`. |
| `auth_token` | no | | Bearer token (**secret**). Prefer env interpolation. |

### `mysql_batch`

```yaml
source:
  type: mysql_batch
  config:
    host: mysql
    port: 3306
    user: sync_user
    password: ${MYSQL_PASSWORD}
    database: app
    table: customers
    pk_column: id
    limit: 5000
    columns: [id, name, email]
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `host` | yes | | MySQL host. |
| `port` | no | `3306` | MySQL port. |
| `user` | yes | | MySQL user. |
| `password` | no | | MySQL password (**secret**). |
| `database` | yes | | Source database. |
| `table` | yes | | Source table. |
| `pk_column` | no | `id` | Primary key column for pagination. |
| `limit` | no | `5000` | Rows per query page. |
| `columns` | no | `*` | Specific columns to SELECT. |

### `mysql_cdc`

```yaml
source:
  type: mysql_cdc
  config:
    host: mysql
    port: 3306
    user: sync_user
    password: ${MYSQL_PASSWORD}
    database: app
    tables: [customers, orders]
    server_id: 1001
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `host` | yes | | MySQL host. |
| `port` | no | `3306` | MySQL port. |
| `user` | yes | | MySQL user. |
| `password` | no | | MySQL password (**secret**). |
| `database` | yes | | Source database. |
| `tables` | yes | | Array of table names to watch. |
| `server_id` | no | `1001` | Unique replication server ID. |

Requires MySQL binlog `ROW` format and `FULL` row image.

### `mysql_snapshot_cdc`

```yaml
source:
  type: mysql_snapshot_cdc
  config:
    host: mysql
    port: 3306
    user: sync_user
    password: ${MYSQL_PASSWORD}
    database: app
    table: customers
    pk_column: id
    limit: 1000
    server_id: 1101
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `host` | yes | | MySQL host. |
| `port` | no | `3306` | MySQL port. |
| `user` | yes | | MySQL user. |
| `password` | no | | MySQL password (**secret**). |
| `database` | yes | | Source database. |
| `table` | yes | | Source table (singular). |
| `pk_column` | no | `id` | Primary key column for snapshot pagination. |
| `limit` | no | `1000` | Rows per snapshot query page. |
| `server_id` | no | `1101` | Unique replication server ID. |

Snapshots by primary-key chunks, records binlog position, then switches to CDC. Checkpoints survive crash during both phases.

### `kafka`

```yaml
source:
  type: kafka
  config:
    brokers: [redpanda:9092]
    topic: events
    group_id: openetl-go
    format: json
    key_column: key
    value_column: payload
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `brokers` | yes | `["localhost:9092"]` | Kafka broker addresses. |
| `topic` | yes | | Kafka topic to consume. |
| `group_id` | no | `etl-consumer` | Consumer group ID. |
| `format` | no | `json` | Message format: `json` or `text`. |
| `key_column` | no | | Column name for message key. |
| `value_column` | no | | Column name for raw message value. |

### `postgres_cdc`

```yaml
source:
  type: postgres_cdc
  config:
    host: postgres
    port: 5432
    user: sync_user
    password: ${PG_PASSWORD}
    database: app
    slot_name: etl_slot
    tables: [customers, orders]
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `host` | yes | | PostgreSQL host. |
| `port` | no | `5432` | PostgreSQL port. |
| `user` | yes | | PostgreSQL user. |
| `password` | no | | PostgreSQL password (**secret**). |
| `database` | yes | | Source database. |
| `slot_name` | no | `etl_slot` | Logical replication slot name. |
| `tables` | no | | Tables to watch. |

Uses pgoutput logical replication protocol. Creates publication and slot if missing.

## Sinks

### `file_sink`

```yaml
sink:
  type: file_sink
  config:
    output_dir: /app/data/output/customers
    format: jsonl
    prefix: "batch_"
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `output_dir` | yes | `/tmp/etl-output` | Output directory path. |
| `format` | no | `json` | `json`, `jsonl`, `csv`, or `parquet`. |
| `prefix` | no | | File name prefix. |

Parquet format writes all columns as strings in a flat schema.

### `s3`

```yaml
sink:
  type: s3
  config:
    endpoint: http://minio:9000
    bucket: etl-bucket
    access_key: ${S3_ACCESS_KEY}
    secret_key: ${S3_SECRET_KEY}
    region: us-east-1
    format: jsonl
    prefix: "batch_"
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `endpoint` | no | | S3-compatible endpoint URL (e.g., MinIO). |
| `region` | no | | S3 region. |
| `bucket` | yes | | S3 bucket name. |
| `access_key` | no | | Access key (**secret**). |
| `secret_key` | no | | Secret key (**secret**). |
| `output_dir` | no | `/tmp/etl-output` | Local fallback directory. |
| `format` | no | `json` | `json`, `jsonl`, `csv`, or `parquet`. |
| `prefix` | no | | Object key prefix. |

Uses MinIO-compatible API when endpoint/bucket are configured, otherwise local file fallback.

### `mysql`

```yaml
sink:
  type: mysql
  config:
    host: mysql
    port: 3306
    user: sync_user
    password: ${MYSQL_PASSWORD}
    database: target
    table: customers
    batch_mode: upsert
    pk_columns: [id]
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `host` | yes | | MySQL host. |
| `port` | no | `3306` | MySQL port. |
| `user` | yes | | MySQL user. |
| `password` | no | | MySQL password (**secret**). |
| `database` | yes | | Target database. |
| `table` | yes | | Target table. |
| `batch_mode` | no | `insert` | `insert` or `upsert`. |
| `pk_columns` | no | `["id"]` | Primary key columns for upsert mode. |

Use `batch_mode: upsert` for CDC/snapshot+CDC idempotency.

### `clickhouse`

```yaml
sink:
  type: clickhouse
  config:
    host: clickhouse
    port: 9000
    database: analytics
    table: customers
    user: default
    password: ${CLICKHOUSE_PASSWORD:-}
    auto_create: true
    schema_drift: add_columns
    pk_columns: [id]
    version_column: _version
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `host` | yes | | ClickHouse host. |
| `port` | no | `9000` | ClickHouse native port. |
| `user` | no | `default` | ClickHouse user. |
| `password` | no | | ClickHouse password (**secret**). |
| `database` | yes | | Target database. |
| `table` | yes | | Target table. |
| `pk_columns` | no | `["id"]` | Primary key columns for auto-create. |
| `version_column` | no | `_version` | Version column for ReplacingMergeTree. |
| `auto_create` | no | `false` | Auto-create table if missing. |
| `schema_drift` | no | `ignore` | `ignore`, `fail`, or `add_columns`. |

### `kafka`

```yaml
sink:
  type: kafka
  config:
    brokers: [redpanda:9092]
    topic: events
    key_column: id
    compression: gzip
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `brokers` | yes | `["localhost:9092"]` | Kafka broker addresses. |
| `topic` | yes | | Kafka topic to produce to. |
| `key_column` | no | | Column for message key. |
| `compression` | no | `none` | `none`, `gzip`, `snappy`, or `lz4`. |

### `elasticsearch` / `es`

```yaml
sink:
  type: elasticsearch
  config:
    hosts: [http://opensearch:9200]
    username: admin
    password: ${ES_PASSWORD}
    index: customers
    id_column: id
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `hosts` | yes | `["http://localhost:9200"]` | Elasticsearch/OpenSearch host URLs. |
| `username` | no | | ES username (**secret**). |
| `password` | no | | ES password (**secret**). |
| `index` | yes | | Target index name. |
| `id_column` | no | `id` | Column for document ID (enables upsert). |

## Transforms

### `identity`

Passes records through unchanged. No config required.

### `rename`

```yaml
transforms:
  - type: rename
    config:
      mappings:
        old_name: new_name
        foo: bar
```

| Field | Required | Description |
| --- | --- | --- |
| `mappings` | yes | Map of old_name → new_name. |

### `drop_field`

```yaml
transforms:
  - type: drop_field
    config:
      fields: [password_hash, internal_id]
```

| Field | Required | Description |
| --- | --- | --- |
| `fields` | yes | Array of field names to remove. |

### `add_field`

```yaml
transforms:
  - type: add_field
    config:
      field: source_system
      value: crm
```

| Field | Required | Description |
| --- | --- | --- |
| `field` | yes | Field name to add. |
| `value` | yes | Field value. Supports `{{now}}` (RFC3339), `{{ts}}` (unix timestamp). |

### `type_convert`

```yaml
transforms:
  - type: type_convert
    config:
      conversions:
        id: int
        amount: float
        active: bool
```

| Field | Required | Description |
| --- | --- | --- |
| `conversions` | yes | Map of field → target type: `int`, `float`, `bool`, `string`, `datetime`. |

### `filter`

```yaml
transforms:
  - type: filter
    config:
      expression: "deleted_at != nil"
```

| Field | Required | Description |
| --- | --- | --- |
| `expression` | yes | Filter expression. Supports `deleted_at != nil` and `status == 'value'` patterns. |

Filtered records are not DLQ errors and can advance checkpoint.

### `normalize_envelope` / `debezium_envelope`

Normalizes plain JSON or Debezium-like Kafka envelopes before `lookup` / `window` processing. Plain JSON passes through. Debezium payloads are flattened from `after` / `before` and mapped to `operation`, `metadata.table`, and `metadata.timestamp`.

```yaml
transforms:
  - type: normalize_envelope
    config:
      keep_metadata: true
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `keep_metadata` | no | `true` | Keep `_op`, `_source_table`, and `_event_time` fields in the record. |

### `lookup`

Performs stream-table join by loading a MySQL/PostgreSQL dimension query into memory and copying selected dimension fields by `join_key`.

```yaml
transforms:
  - type: lookup
    config:
      dsn: root:root123456@tcp(mysql:3306)/app
      query: SELECT id, name, tier FROM dim_users
      join_key: user_id
      dim_key: id
      fields: [name, tier]
      refresh_interval_sec: 300
      max_cache_entries: 100000
      on_miss: "null"
      on_refresh_error: error
      state_backend: sqlite
      state_path: ./data/etl-state.db
      state_pipeline: orders-wide-table
      state_node: lookup-users
      state_ttl_seconds: 86400
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `dsn` | yes | | Dimension database DSN. |
| `query` | yes | | SQL used to load the dimension table. |
| `join_key` | no | `id` | Field in the input record. |
| `dim_key` | no | `id` | Field in the dimension result. |
| `fields` | yes | | Dimension fields copied into the record. |
| `refresh_interval_sec` | no | `300` | Full refresh interval, `0` means load once. |
| `max_cache_entries` | no | `0` | Maximum distinct dimension cache entries, `0` means unlimited. Exceeding the cap fails the refresh/restore and increments `cache_limit_exceeded`. |
| `on_miss` | no | `pass` | Action when no dimension row is found: `pass` keeps the record unchanged, `null` writes the configured fields with null values, `dlq`/`error` returns an error so the runner routes the record to DLQ. |
| `on_refresh_error` | no | `pass` | Action when dimension refresh fails and no usable cache can be loaded: `pass` keeps the record unchanged, `error` returns an error so the runner routes the record to DLQ. |
| `state_backend` | no | | Durable lookup cache backend. Currently supports `sqlite`. |
| `state_path` | no | `./data/etl-state.db` | SQLite state database path when `state_backend=sqlite`. |
| `state_pipeline` | no | pipeline name | Pipeline namespace for persisted lookup cache. Runtime injects the pipeline name when omitted. |
| `state_node` | no | transform node id | Node namespace for persisted lookup cache. Runtime injects the transform node id when omitted. |
| `state_ttl_seconds` | no | `0` | TTL for persisted lookup rows, `0` means no expiry. |

When `state_backend` is enabled, every successful dimension refresh is persisted
to `StateStore`. If the dimension query later fails, lookup can restore the last
non-expired cache snapshot and continue enriching records with stale-but-known
dimension values.

For production wide-table pipelines, prefer `on_miss: "null"` when a left-join
miss is acceptable and explicit null dimension fields are useful downstream.
Use `on_miss: dlq` or `on_refresh_error: error` when dimension misses or cache
refresh failures should stop the current record and enter the normal DLQ path.

### `join`

Performs a stream-stream interval join by buffering records for `join_window_sec`
and matching later records on `join_key`. For production-like recovery tests,
enable the SQLite `StateStore` backend so buffered records can be restored after
process restart.

```yaml
transforms:
  - type: join
    config:
      join_type: left
      join_key: user_id
      join_window_sec: 60
      join_fields: [amount, status]
      join_prefix: prev_
      on_miss: dlq
      state_backend: sqlite
      state_path: ./data/etl-state.db
      state_pipeline: orders-wide-table
      state_node: join-orders
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `join_type` | no | `inner` | Join type. Supported values: `inner`, `left`. |
| `join_key` | yes | | Field name to join on. |
| `join_window_sec` | no | `60` | How long to keep buffered records in join state. |
| `join_fields` | yes | | Fields copied from the matched buffered record. |
| `join_prefix` | no | `joined_` | Prefix for copied fields. |
| `where` | no | | Optional filter expression for buffered records. |
| `on_miss` | no | `drop` | Action for an inner-join miss: `drop`, `dlq`, or `error`. |
| `max_buffered_keys` | no | `0` | Maximum distinct join keys kept in memory, `0` means unlimited. |
| `max_buffered_records` | no | `0` | Maximum total join records kept in memory, `0` means unlimited. |
| `state_backend` | no | | Durable join buffer backend. Currently supports `sqlite`. |
| `state_path` | no | `./data/etl-state.db` | SQLite state database path when `state_backend=sqlite`. |
| `state_pipeline` | no | pipeline name | Pipeline namespace for persisted join buffers. Runtime injects the pipeline name when omitted. |
| `state_node` | no | transform node id | Node namespace for persisted join buffers. Runtime injects the transform node id when omitted. |
| `state_ttl_seconds` | no | `0` | TTL for persisted join buffers. `0` uses `join_window_sec`. |

When `state_backend` is enabled, `join` persists each join key's buffered
records after every update and restores non-expired records on startup. This
improves crash/restart recovery for stream-stream joins, but it does not by
itself provide exactly-once semantics across Kafka offsets, state snapshots, and
sink commits.

When `max_buffered_keys` or `max_buffered_records` is exceeded, `join` rejects
the current record with an error instead of growing unbounded state. The existing
pipeline retry/DLQ path handles that error, and `state_limit_exceeded` is exposed
through transform metrics.

### `window`

Windowed aggregation. The production configuration path currently exposes only `tumbling`; `sliding` / `session` remain roadmap items and should not be used in production specs.

```yaml
transforms:
  - type: window
    config:
      window_type: tumbling
      window_size_seconds: 60
      allowed_lateness_seconds: 10
      group_by: [region, tier]
      aggregates:
        order_count:
          func: count
        total_amount:
          func: sum
          field: amount
      state_backend: sqlite
      state_path: ./data/etl-state.db
      state_pipeline: orders-wide-table
      state_node: window-orders
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `window_type` | no | `tumbling` | Only `tumbling` is supported. |
| `window_size_seconds` | no | `60` | Fixed window size. |
| `allowed_lateness_seconds` | no | `0` | Allowed event-time lateness. |
| `group_by` | no | | Group-by fields. |
| `aggregates` | yes | | Aggregation definitions. Supports `count`, `sum`, `avg`, `min`, `max`, `first`, `last`. |
| `state_backend` | no | | Durable tumbling-window state backend. Currently supports `sqlite`. |
| `state_path` | no | `./data/etl-state.db` | SQLite state database path when `state_backend=sqlite`. |
| `state_pipeline` | no | pipeline name | Pipeline namespace for persisted window state. Runtime injects the pipeline name when omitted. |
| `state_node` | no | transform node id | Node namespace for persisted window state. Runtime injects the transform node id when omitted. |
| `state_ttl_seconds` | no | `0` | TTL for persisted window state, `0` means no expiry. |

When `state_backend` is enabled, `window` persists buffered tumbling-window
aggregate state and restores it on startup, so records accumulated before a
restart can still contribute to the final aggregate. This is a recovery aid for
at-least-once pipelines; it does not make window emission transactional with
Kafka offsets or downstream sink commits.

### `deduplicate`

Drops repeated records by a composite key. By default it keeps the recent key
set in memory. For crash/restart recovery, enable the SQLite `StateStore`
backend.

```yaml
transforms:
  - type: deduplicate
    config:
      keys: [order_id]
      window_size: 10000
      state_backend: sqlite
      state_path: ./data/etl-state.db
      state_pipeline: orders-wide-table
      state_node: dedup-orders
      state_ttl_seconds: 86400
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `keys` | yes | | Fields forming the dedup key. |
| `window_size` | no | `10000` | Process-local ring size for recently seen keys. |
| `state_backend` | no | | Durable state backend. Currently supports `sqlite`. |
| `state_path` | no | `./data/etl-state.db` | SQLite state database path when `state_backend=sqlite`. |
| `state_pipeline` | no | pipeline name | Pipeline namespace for persisted dedup keys. Runtime injects the pipeline name when omitted. |
| `state_node` | no | transform node id | Node namespace for persisted dedup keys. Runtime injects the transform node id when omitted. |
| `state_ttl_seconds` | no | `0` | TTL for persisted dedup keys, `0` means no expiry. |

### `lua`

```yaml
transforms:
  - type: lua
    config:
      script: |
        record.data.name = string.upper(record.data.name)
        return record
```

| Field | Required | Description |
| --- | --- | --- |
| `script` | yes | Lua script code. Receives `record` table and `metadata` table. |

## Runtime APIs For Config Workflows

- Validate spec: `POST /api/v2/specs/validate` — returns idempotency warnings for dangerous source/sink combos
- Connection catalog: `GET/POST /api/v2/connections`, `GET/PUT/DELETE /api/v2/connections/{name}`, `POST /api/v2/connections/{name}/test` — stores reusable source/sink/transform configs, masks secret fields in responses, and records last health status
- Test ad-hoc connection: `POST /api/v2/connections/test`
- Connector descriptors: `GET /api/v2/connectors/descriptors` — returns Connector Descriptor v1 records merged from registry, config schema, secret markers, capabilities, and maturity metadata
- Transform dry-run: `POST /api/v2/transforms/dry-run`
- Wide-table preview: `POST /api/v2/wide-table/preview` — returns envelope/lookup/window/sink preview, sample field types, proposed ClickHouse DDL, and preflight result
- Reload specs: `POST /api/v2/specs/reload`
- Plugin schema: `GET /api/v2/plugins/schema` — returns typed field schemas with secret markers

## Stateful Processing Foundation

Stateful transforms use the `StateStore` v1 contract in `internal/etl/state`. It currently has:

- `MemoryStore` for tests and development.
- `SQLiteStore` for durable local/standalone state with TTL, snapshot/restore, state size stats, and expired-key cleanup.
- `checkpoint.Envelope` for stateful checkpoint payloads. It groups the source position, per-node state snapshot versions, sink commit metadata, and the documented `at_least_once` delivery mode in one JSON payload while remaining distinguishable from legacy source positions.

`lookup`, `join`, `window`, and `deduplicate` can now use `StateStore` through
`state_backend: sqlite`. `lookup` persists refreshed dimension-cache rows and
can restore the latest non-expired snapshot when the dimension query fails;
`join` persists buffered interval-join records by join key; `window` persists
buffered tumbling-window aggregates; `deduplicate` persists seen keys across
restarts. Complex window semantics such as sliding/session, side outputs, and
transactional emission remain roadmap items.

When a pipeline is built by the linear runner or DAG executor, stateful
transforms with `state_backend` enabled automatically receive `state_pipeline`
from the pipeline name and `state_node` from the transform/node id. Set these
fields only when you need an explicit shared or migrated state namespace.

The linear runner and DAG executor now save checkpoint envelopes for stateful
transforms that implement `StateSnapshotter`: after a successful sink write they
store the source position together with the latest state snapshot versions and
unwrap the source position again before reopening the source. If state snapshot
collection fails, the source checkpoint is not advanced. Sink commit metadata is
still a roadmap item.

`deduplicate`, `lookup`, `join`, and `window` also expose basic state metrics
when `state_backend` is enabled. `/api/v2/metrics` includes `state_metrics`
entries per node, and Prometheus `/metrics` exports:

- `etl_state_keys{pipeline,node}`
- `etl_state_bytes{pipeline,node}`
- `etl_state_updated_timestamp_seconds{pipeline,node}`

`deduplicate` exposes `processed`, `passed`, `duplicate_dropped`,
`memory_duplicate_dropped`, `state_duplicate_dropped`, and `evicted_keys` in
`/api/v2/metrics` as `transform_metrics` and in Prometheus as
`etl_transform_metric_total{pipeline,node,transform,metric}`.

`lookup` exposes `processed`, `hit`, `miss`, `missing_key`, `miss_null`,
`miss_dlq`, `miss_error`, `refresh_success`, `refresh_error`,
`refresh_error_dlq`, `restore_success`, `scan_error`, and
`cache_limit_exceeded` through the same `transform_metrics` and Prometheus
counter family. These counters make dimension-cache hit ratio, miss handling,
stale-state restore, external dependency failures, row scan failures, and
cache-cap pressure visible.

`join` also exposes domain counters in `/api/v2/metrics` as `transform_metrics`
and in Prometheus as `etl_transform_metric_total{pipeline,node,transform,metric}`.
Current join metrics are `hit`, `miss`, `miss_dropped`, `miss_dlq`, and
`miss_error`, and `state_limit_exceeded`.

`window` exposes `accumulated`, `late_dropped`, `emitted_records`,
`emitted_windows`, and `flushed_records` through the same `transform_metrics`
and Prometheus counter family.

## Connector Descriptor And Plugin ABI

`GET /api/v2/connectors/descriptors` returns Connector Descriptor v1 records. Each descriptor includes:

- `kind`, `type`, `version`, `registered`
- typed config `fields`
- `required` and `secret_fields`
- `capabilities`
- evidence-driven `maturity`

WASM plugins use Plugin ABI v1 metadata in `internal/etl/plugin/pluginsystem`:

- ABI string: `openetl.plugin.abi/v1`
- Minimum runtime contract: `openetl-runtime/v1`
- Required entrypoints: source plugins export `read`, sink plugins export `write`, transform plugins export `transform`
- Config field types: `string`, `int`, `bool`, `float`, `string_array`, `map`

## Idempotency Warnings

Spec validation (`POST /api/v2/specs/validate`) returns warnings for potentially dangerous combinations:

- CDC source + file/S3 sink: no deduplication, risk of duplicates on restart
- `mysql_batch` + `mysql` sink with `batch_mode: insert`: duplicate key errors on re-run
- CDC source + append-only sink: UPDATE/DELETE written as rows, not mutations
