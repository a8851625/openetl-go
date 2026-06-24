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
- Test connection: `POST /api/v2/connections/test`
- Transform dry-run: `POST /api/v2/transforms/dry-run`
- Reload specs: `POST /api/v2/specs/reload`
- Plugin schema: `GET /api/v2/plugins/schema` — returns typed field schemas with secret markers

## Idempotency Warnings

Spec validation (`POST /api/v2/specs/validate`) returns warnings for potentially dangerous combinations:

- CDC source + file/S3 sink: no deduplication, risk of duplicates on restart
- `mysql_batch` + `mysql` sink with `batch_mode: insert`: duplicate key errors on re-run
- CDC source + append-only sink: UPDATE/DELETE written as rows, not mutations
