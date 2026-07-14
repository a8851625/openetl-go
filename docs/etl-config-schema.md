# ETL YAML Config Reference

Pipeline specs are YAML files under `pipes/` or the configured `etl.specsDir`. They express OpenETL-Go's core model: lightweight self-hosted `Source -> Transform -> Sink` pipelines for data synchronization, cleansing, and aggregation. Environment variables are expanded before parsing with `${VAR}` or `${VAR:-default}`.

The config surface should serve common CDC/ETL paths, checkpoints, DLQ, idempotent writes, and lightweight aggregation. Full stream-processing semantics such as arbitrary keyed state, timers, SQL planners, and savepoints are outside the current production target. See [product positioning](./positioning.md).

## Top-Level Spec

```yaml
name: example-pipeline
allow_unsafe: false
source:
  type: file
  connection: saved-source
  config: {}
transforms:
  - type: identity
    connection: saved-transform
    config: {}
sink:
  type: file_sink
  connection: saved-sink
  config: {}
schedule:
  type: once
batch_size: 1000
checkpoint_interval_sec: 30
backpressure_buffer: 100
parallelism:
  sharding:
    strategy: pk_mod
    key: id
    logical_shards: 16
  execution:
    max_active_shards: 4
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
| `allow_unsafe` | no | Explicit opt-in for a CDC/stream source writing to a non-idempotent `file_sink`/`s3` target. Default `false`; use only when duplicate handling and replay boundaries are documented and tested. |
| `source.type` | yes unless `source.connection` is set | Registered source plugin name. If omitted with `connection`, it is inferred from the saved connection. |
| `source.connection` / `source.connection_ref` | no | Saved connection catalog entry to use as the base source config. Inline `source.config` overrides fields from the saved connection. |
| `source.config` | no | Source-specific settings. Defaults to `{}`. |
| `transforms` | no | Ordered transform chain. Omit for no transforms. Each transform can also use `connection` / `connection_ref`. |
| `sink.type` | yes unless `sink.connection` is set | Registered sink plugin name. If omitted with `connection`, it is inferred from the saved connection. |
| `sink.connection` / `sink.connection_ref` | no | Saved connection catalog entry to use as the base sink config. Inline `sink.config` overrides fields from the saved connection. |
| `sink.config` | no | Sink-specific settings. Defaults to `{}`. |
| `schedule.type` | no | Trigger type. If omitted, the source `default_schedule` is applied. Built-in CDC/stream sources (`mysql_cdc`, `postgres_cdc`, `mysql_snapshot_cdc`, `kafka`) currently support only `streaming`; batch/pull sources (`mysql_batch`, `file`, `http`) support `once`, `cron`, `periodic`, and `dependency`. `spec validate` rejects schedule types outside the source descriptor's `supported_schedules`. |
| `schedule.cron` | for `cron` | Cron expression used when `schedule.type: cron`. |
| `schedule.interval_sec` | for `periodic` | Positive interval in seconds used when `schedule.type: periodic`. |
| `schedule.depends_on` | for `dependency` | Upstream pipeline names that trigger this pipeline after completion. |
| `batch_size` | no | Sink flush size. Default `1000`. |
| `checkpoint_interval_sec` | no | Reserved interval setting; checkpoints currently advance after successful writes. Default `30`. |
| `backpressure_buffer` | no | Internal record channel size. Default `100`. |
| `parallelism` | no | Optional sharding and runtime concurrency. New specs should use `parallelism.sharding.logical_shards` for stable data ownership and `parallelism.execution.max_active_shards` for current process concurrency. Legacy `parallelism.count`, `shard_strategy`, `shard_key`, and `shard_total` are still accepted and mapped for compatibility. |
| `retry` | no | Retry policy. Defaults shown above. |
| `dlq.enable` | no | Enable file DLQ. |
| `table_mapping` | no | Pipeline-level source→target table rename applied before transforms. Used for multi-table A→B sync. |

### Table mapping (multi-table A→B)

Pipeline-level `table_mapping` rewrites `record.metadata.table` before transforms and preserves the original name in `data._source_table` (and `data._source_database` when present). Leave `sink.config.table` empty so each mapped table routes dynamically.

```yaml
name: multi-table-map-to-mysql
source:
  type: mysql_snapshot_cdc
  config:
    database: src_db
    tables: [customers, products]
    pk_column: id
table_mapping:
  template: "ods_{source_table}"
  # or:
  # rules:
  #   customers: ods_customers
  #   products: ods_products
  # regex:
  #   - {pattern: "^(.*)$", replacement: "ods_$1"}
sink:
  type: mysql
  config:
    database: tgt_db
    # table omitted → Metadata.Table after mapping
    batch_mode: upsert
    pk_columns: [id]
    auto_create: true
    schema_drift: add_columns
```

| Field | Required | Description |
| --- | --- | --- |
| `table_mapping.template` | no | Default target template when no rule matches. Supports `{source_table}`, `{source_db}`, `{table}`, `{db}`. |
| `table_mapping.rules` | no | Glob map of source pattern → target (target may use the same template tokens). Prefers `db.table` keys when metadata.database is set. |
| `table_mapping.regex` | no | List of `{pattern, replacement}` regex rewrites. |

Evidence: `hack/e2e-multi-table-map.sh`, `hack/e2e-mysql-cdc-wide.sh`.

### Parallelism

```yaml
parallelism:
  sharding:
    strategy: pk_mod        # pk_mod, id_range, hash_modulo, round_robin, partition, table
    key: id                 # field/key used by hash or PK strategies
    logical_shards: 16      # stable shard namespace, also used in checkpoint keys
  execution:
    max_active_shards: 4    # standalone in-process concurrency
    transform_workers: 1    # batch-local workers for stateless per-record transforms
    sink_concurrency: 1     # max concurrent sink writes across standalone shard runners
```

`parallelism.execution.source_concurrency` is still parsed for forward
compatibility but is not active in the current runner. Use
`parallelism.sharding.logical_shards` and
`parallelism.execution.max_active_shards` for source-side parallelism.

Legacy form remains valid:

```yaml
parallelism:
  count: 4
  shard_strategy: id_range
  shard_key: id
  shard_total: 0
```

### DAG Execution Workers

DAG specs use the top-level `execution` block instead of linear `parallelism.execution`:

```yaml
execution:
  workers: 4
  batch_size: 1000
  backpressure_buffer: 200
```

`execution.workers` parallelizes DAG route/transform work for records that have already been read from source nodes. Sink batching, sink writes, and checkpoint advancement still pass through a single ordered aggregator, so checkpoints advance in source record order and retain at-least-once semantics. The default is `1`, which preserves the previous single-router behavior.

### Reusing Saved Connections

Connections saved through `GET/POST /api/v2/connections` can be referenced by linear pipeline endpoints and DAG nodes:

```yaml
source:
  connection: orders-mysql
  config:
    table: orders
sink:
  connection_ref: warehouse-clickhouse
  config:
    table: orders_wide
```

The saved connection supplies `kind`, `type`, and base `config`. The pipeline endpoint must use a connection with the matching kind (`source`, `sink`, or `transform`). Inline `config` wins over saved config, so shared credentials can live in the catalog while per-pipeline fields such as `table`, `topic`, or `query` remain in the spec.

### Connection field scope

Connection Catalog entries should store **connection-scope** fields only. Pipeline endpoints own **behavior-scope** fields.

| Scope | Lives in | Typical fields |
| --- | --- | --- |
| `connection` | Connection Catalog | `host`, `port`, `user`, `password`, `database`, `brokers`, `endpoint`, `bucket`, `access_key`, `secret_key`, TLS/SASL credentials |
| `behavior` | pipeline `source.config` / `sink.config` | `table`, `tables`, `topic`, `query`, `batch_mode`, `pk_columns`, `pre_write`, `schema_drift`, `ddl_policy`, `format`, `partition`, `increment_columns` |

Descriptor and `/api/v2/plugins/schema` fields expose `scope: connection|behavior`. The UI Connection Catalog form filters to connection-scope fields; wizard/DAG forms show behavior-only fields when a saved connection is selected.

Legacy connections that still store behavior fields continue to merge for one compatibility window, but `POST /api/v2/specs/validate` emits a deprecation warning asking operators to move those fields into the endpoint config.


### Post-Commit Trigger (dependency schedule)

For the common warehouse pattern "after CDC lands into ODS, recompute an aggregate table", use `schedule.type: dependency`. The downstream pipeline is triggered once each time the upstream pipeline completes a run.

```yaml
# 1) Upstream: CDC continuously lands rows into MySQL ODS
name: cdc-orders-to-ods
source:
  type: mysql_cdc
  config: {host: mysql, user: sync, password: "${MYSQL_PASSWORD}", database: src, server_id: 100}
sink:
  type: mysql
  config: {host: mysql, user: sync, password: "${MYSQL_PASSWORD}", database: ods, table: orders, batch_mode: upsert, pk_columns: [id]}
schedule: {type: streaming}

# 2) Downstream: recompute daily issue_count whenever the upstream CDC run finishes
name: recompute-issue-count
source:
  type: mysql_batch
  config: {host: mysql, user: sync, password: "${MYSQL_PASSWORD}", database: ods, table: orders, query: "SELECT dt, COUNT(*) AS issue_count FROM orders GROUP BY dt"}
sink:
  type: mysql
  config:
    host: mysql
    user: sync
    password: "${MYSQL_PASSWORD}"
    database: dws
    table: issue_count_daily
    batch_mode: upsert
    pk_columns: [dt]
    pre_write: {action: delete, condition: "dt IN (SELECT DISTINCT dt FROM ods.orders WHERE updated_at >= NOW() - INTERVAL 1 DAY)"}
schedule:
  type: dependency
  depends_on: [cdc-orders-to-ods]
```

Notes:
- The downstream re-computation runs every time the upstream finishes a run, so the downstream sink MUST be idempotent — `batch_mode: upsert` with stable `pk_columns`, or `pre_write: {action: delete, condition: ...}` to scope a delete-then-rewrite. `spec validate` warns when a dependency-scheduled pipeline targets an append-only sink (kafka/file/s3) or a relational sink in non-upsert mode.
- This scheme replaces sink-side post-commit hooks (`hooks.on_batch_written`); the pipeline model stays single-direction `source -> transform -> sink`.

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
| `schema` | no | | Optional preflight-only schema hint, as `[{name,data_type,nullable}]` or `{field: type}`. |
| `sample` | no | | Optional preflight-only sample record used to infer schema when the file path is not readable during validation. |

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
| `schema` | no | | Optional preflight-only schema hint, as `[{name,data_type,nullable}]` or `{field: type}`. |
| `sample` | no | | Optional preflight-only sample response record used to infer schema without calling the remote API. |

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
| `enable_gtid` | no | `false` | Enable GTID-based replication. |
| `server_id_base` | no | | Base replication server ID used with sharding. |
| `shard_index` | no | | Shard index for table partitioning. |
| `shard_total` | no | | Total shard count for table partitioning. |
| `start_from` | no | | CDC start point: `timestamp`, `binlog:<file>:<pos>`, or `gtid:<set>`. |

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
| `table` | no | | Source table (singular). Required when `tables` is not set. |
| `tables` | no | | Source tables for multi-table snapshot+CDC. Required when `table` is not set. |
| `pk_column` | no | `id` | Primary key column for snapshot pagination. |
| `limit` | no | `1000` | Rows per snapshot query page. |
| `server_id` | no | `1101` | Unique replication server ID. |
| `server_id_base` | no | | Base replication server ID used with sharding. |
| `consistent_snapshot_lock` | no | `true` | Use table locks for consistent snapshot capture. |
| `shard_index` | no | | Shard index for snapshot partitioning. |
| `shard_total` | no | | Total shard count for snapshot partitioning. |

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
| `schema` | no | | Optional preflight-only schema hint, as `[{name,data_type,nullable}]` or `{field: type}`. |
| `sample` | no | | Optional preflight-only sample message used to infer schema without consuming Kafka. |

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
| `tables` | no | | Tables to watch. Required when `enable_snapshot: true`; empty means publication for all tables in CDC-only mode. Entries may be unqualified (`orders`) or schema-qualified (`public.orders`). |
| `sslmode` | no | `prefer` | PostgreSQL SSL mode: `disable`, `allow`, `prefer`, `require`, `verify-ca`, or `verify-full`. |
| `enable_snapshot` | no | `false` | Run an initial table snapshot before switching to logical replication. Requires `tables`. |
| `drop_slot_on_close` | no | `false` | Drop the replication slot when the source closes. Keep `false` for restartable CDC. |

Uses pgoutput logical replication protocol. Creates publication and slot if missing.
Preflight validates required connection fields, port, slot name, SSL mode,
snapshot table list, `wal_level=logical`, replication role, publication
readiness, configured table existence, and replication slot ownership.

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
| `endpoint` | yes | | S3-compatible endpoint URL (e.g., MinIO). |
| `region` | no | | S3 region. |
| `bucket` | yes | | S3 bucket name. |
| `access_key` | no | | Access key (**secret**). |
| `secret_key` | no | | Secret key (**secret**). |
| `output_dir` | no | `/tmp/etl-output` | Local staging/fallback directory used only by file-compatible writes; use `file_sink` for intentional local output. |
| `format` | no | `json` | `json`, `jsonl`, `csv`, or `parquet`. |
| `prefix` | no | | Object key prefix. |

Uses MinIO-compatible API. `endpoint` and `bucket` are required; use `file_sink` for local file output.

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
| `batch_mode` | no | `insert` | `insert`, `upsert`, or `increment`. |
| `pk_columns` | no | `["id"]` | Primary key columns for upsert mode. |
| `pk_columns_from_metadata` | no | `false` | Derive per-table primary key columns from `record.metadata.key` for Debezium multi-table CDC. |
| `increment_columns` | no | | Target column -> source field map for additive `batch_mode: increment`. |
| `pre_write` | no | | Pre-write action block: `delete`, `truncate`, or `truncate_partition` with optional `params`. |
| `auto_create` | no | `false` | Auto-create table if missing. |
| `schema_drift` | no | `ignore` | `ignore`, `fail`, or `add_columns`. |
| `ddl_policy` | no | `reject` | `reject`, `ignore`, or `apply`. |
| `insert_chunk_size` | no | `500` | Rows per INSERT statement. |

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
| `port` | no | `9000` | ClickHouse port (`9000` native, `8123` HTTP). |
| `protocol` | no | `native` | `native` or `http`. |
| `user` | no | `default` | ClickHouse user. |
| `password` | no | | ClickHouse password (**secret**). |
| `database` | yes | | Target database. |
| `table` | no | | Target table. Empty uses the source table name dynamically. |
| `pk_columns` | no | `["id"]` | Primary key columns for ORDER BY, DELETE, and UPDATE conditions. |
| `version_column` | no | `_version` | Version column for ReplacingMergeTree. |
| `auto_create` | no | `false` | Auto-create table if missing. |
| `schema_drift` | no | `ignore` | `ignore`, `fail`, `add_columns`, or `sync`. |
| `ddl_policy` | no | `apply` | `reject`, `ignore`, or `apply`. |
| `source_dialect` | no | | Source SQL dialect for DDL translation: `mysql`, `postgres`, `postgresql`, or `clickhouse`. |
| `optimize_interval_sec` | no | `0` | Periodic `OPTIMIZE TABLE FINAL` interval; `0` disables it. |
| `use_final` | no | `false` | Append `FINAL` to internal deduplicated reads. |
| `tls` | no | `false` | Enable TLS for ClickHouse connection. |
| `tls_skip_verify` | no | `false` | Skip TLS certificate verification. |
| `compression` | no | `LZ4` | `LZ4` or `ZSTD`. |
| `async_insert` | no | `false` | Enable ClickHouse `async_insert`. |
| `async_insert_wait` | no | `true` | Wait for async insert completion. |
| `ttl` | no | | TTL expression for auto-created tables. |

### `maxcompute` / `odps`

Experimental connector for Kafka ODS JSON -> MaxCompute partitioned table. The sink is backed by the Aliyun ODPS SDK batch tunnel writer and validates config/schema/partition fields, but maturity remains experimental until a real MaxCompute integration environment provides repeatable write, replay, and failure-injection evidence.

```yaml
sink:
  type: maxcompute
  config:
    endpoint: https://service.cn-hangzhou.maxcompute.aliyun.com/api
    tunnel_endpoint: https://dt.cn-hangzhou.maxcompute.aliyun.com
    project: warehouse
    table: ods_events
    access_key_id: ${ALIYUN_ACCESS_KEY_ID}
    access_key_secret: ${ALIYUN_ACCESS_KEY_SECRET}
    columns:
      id: BIGINT
      event_time: TIMESTAMP
      payload: STRING
    partition_fields: [dt]
    write_mode: append
    auto_create_partition: true
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `endpoint` | yes | | MaxCompute endpoint URL. |
| `tunnel_endpoint` | no | | MaxCompute Tunnel endpoint URL. If omitted, the SDK resolves it from the project. |
| `project` | yes | | MaxCompute project name. |
| `table` | yes | | Target table name. |
| `access_key_id` | yes | | Alibaba Cloud access key ID (**secret**). |
| `access_key_secret` | yes | | Alibaba Cloud access key secret (**secret**). |
| `quota_name` | no | | MaxCompute quota name used by tunnel upload sessions. |
| `columns` | no | | Target column type map. Supported first-pass types: `STRING`, `BIGINT`, `DOUBLE`, `DECIMAL`, `BOOLEAN`, `DATETIME`, `TIMESTAMP`. |
| `partition` | no | | Static partition values, for example `{dt: "2026-06-26"}`. |
| `partition_fields` | no | | Record fields used as dynamic partition values, for example `[dt]`. |
| `write_mode` | no | `append` | `append` or `partition_overwrite`. Append is at-least-once and can duplicate on replay. |
| `auto_create_partition` | no | `true` | Ask the Tunnel SDK to create missing target partitions during upload. |
| `batch_size` | no | `500` | Rows per batch. |
| `max_retries` | no | `3` | Retry attempts for transient writes. |
| `retry_base_ms` | no | `500` | Base retry delay in milliseconds. |

Use `project` / `type_convert` before this sink so the record schema matches the declared MaxCompute `columns` and contains every dynamic `partition_fields` value. Preflight loads the remote table and validates table/partition/permission reachability. `append` is at-least-once and can duplicate after checkpoint reset or replay; use business keys, staging+merge, or controlled `partition_overwrite` flows when duplicates are not acceptable.

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
| `hosts` | yes | | Elasticsearch/OpenSearch host URLs. |
| `username` | no | | ES username (**secret**). |
| `password` | no | | ES password (**secret**). |
| `index` | yes | | Target index name. |
| `id_column` | no | `id` | Column for document ID (enables upsert). |
| `mappings` | no | | Optional Elasticsearch mapping used by preflight schema validation. If omitted, preflight reads `/{index}/_mapping` from the target when reachable. |
| `properties` | no | | Optional mapping properties shorthand, for example `{id: {type: long}, status: {type: keyword}}`. |
| `chunk_size` | no | `500` | Records per bulk request. |
| `max_retries` | no | `3` | Retry attempts for transient bulk failures. |
| `retry_base_ms` | no | `500` | Base retry delay in milliseconds. |
| `tls_skip_verify` | no | `false` | Skip TLS certificate verification. |

Preflight validates source fields against configured or remote mapping properties when available. It reports field-level type mismatches such as string data targeting a `long` mapping before the first bulk write.

### `postgres`

```yaml
sink:
  type: postgres
  config:
    host: postgres
    port: 5432
    user: sync_user
    password: ${PG_PASSWORD}
    database: target
    schema: public
    table: customers
    batch_mode: upsert
    pk_columns: [id]
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `host` | yes | | PostgreSQL host. |
| `port` | no | `5432` | PostgreSQL port. |
| `user` | yes | | PostgreSQL user. |
| `password` | no | | PostgreSQL password (**secret**). |
| `database` | yes | | Target database. |
| `schema` | no | `public` | Target schema. |
| `table` | yes | | Target table. |
| `batch_mode` | no | `insert` | `insert`, `upsert` (`INSERT ... ON CONFLICT`), or `increment`. |
| `pk_columns` | no | `["id"]` | Primary key columns for upsert mode. |
| `increment_columns` | no | | Target column -> source field map for additive `batch_mode: increment`. |
| `pre_write` | no | | Pre-write action block: `delete`, `truncate`, or `truncate_partition` with optional `params`. |
| `auto_create` | no | `false` | Auto-create table if missing. |
| `schema_drift` | no | `ignore` | `ignore`, `fail`, or `add_columns`. |
| `ddl_policy` | no | `reject` | `reject`, `ignore`, or `apply`. |
| `sslmode` | no | `prefer` | PostgreSQL SSL mode: `disable`, `allow`, `prefer`, `require`, `verify-ca`, or `verify-full`. |
| `insert_chunk_size` | no | `500` | Rows per INSERT statement. |

### `doris`

```yaml
sink:
  type: doris
  config:
    host: doris-fe
    port: 9030
    http_port: 8030
    user: root
    password: ${DORIS_PASSWORD}
    database: analytics
    table: customers
    write_mode: stream_load
    stream_load_format: json
    batch_mode: upsert
    pk_columns: [id]
    auto_create: true
    schema_drift: add_columns
    ddl_policy: reject
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `host` | yes | | Doris FE host. |
| `port` | no | `9030` | Doris MySQL protocol port used for DDL, fallback INSERT, and DELETE. |
| `http_port` | no | `8030` | Doris Stream Load HTTP port. |
| `user` | no | `root` | Doris user. |
| `password` | no | | Doris password (**secret**). |
| `database` | yes | | Target database name. |
| `table` | yes | | Target table name. Leave empty only for dynamic CDC table routing where `record.metadata.table` is always present. |
| `write_mode` | no | `stream_load` | `stream_load` or MySQL-protocol `insert` fallback. |
| `batch_mode` | no | `insert` | `insert` or `upsert`. Production CDC/upsert requires a Doris Unique Key table and stable `pk_columns`. |
| `pk_columns` | no | | Key columns for DELETE, auto-created Unique Key tables, and replay-safe upsert validation. |
| `stream_load_format` | no | `json` | `json` or `csv`. |
| `stream_load_scheme` | no | `http` | `http` or `https`. |
| `stream_load_timeout_sec` | no | `30` | Stream Load HTTP timeout in seconds. |
| `insert_chunk_size` | no | `500` | Rows per INSERT statement when `write_mode: insert` is used. |
| `tls_skip_verify` | no | `false` | Skip TLS certificate verification. |
| `auto_create` | no | `false` | Auto-create missing Doris Unique Key tables. If no `pk_columns` are set, an `id` column is required. |
| `schema_drift` | no | `ignore` | `ignore`, `fail`, or `add_columns`. |
| `ddl_policy` | no | `reject` | `reject`, `ignore`, or `apply`. The production default rejects source DDL; Doris `apply` is limited to safe `ALTER TABLE ... ADD COLUMN` statements. |
| `allow_mixed_cdc_non_atomic` | no | `false` | Allow mixed write/delete CDC batches even though Stream Load and MySQL DELETE are not atomic together. |

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

### `project` / `select_fields`

Projects a record into an explicit output shape. `select_fields` is an alias for the same transform.

```yaml
transforms:
  - type: project
    config:
      fields: [id, amount]
      mappings:
        user_name: customer_name
        created_at: dt
      constants:
        source_system: crm
      time_formats:
        dt: "2006-01-02"
```

| Field | Required | Description |
| --- | --- | --- |
| `fields` | no | Source fields to keep with the same output names. Missing fields are ignored. |
| `mappings` | no | Map of source field → output field alias. Missing source fields are ignored. |
| `constants` | no | Constant output fields added after fields and mappings. |
| `time_formats` | no | Map of output field → time format. Supports `unix`, `unix_ms`, `rfc3339`, or any Go time layout such as `2006-01-02`. |
| `keep_unmapped` | no | Preserve input fields not listed in `fields` or `mappings` (default `false`). Mapped source fields are renamed to their target names. |

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

### `flat_map` / `udtf`

Expands one input record into zero, one, or many output records. `udtf` is an alias for the same transform. The first core ABI is Lua-backed: the script receives a full `record` table (`record.data`, `record.metadata`, `record.before`, `record.operation`) and returns `nil`/`false`, one record or data map, or an array of records/data maps. Output records inherit the input operation and metadata unless the returned record overrides them.

```yaml
transforms:
  - type: flat_map
    config:
      language: lua
      on_error: dlq
      script: |
        local out = {}
        for i, item in ipairs(record.data.items) do
          out[i] = {
            data = {
              order_id = record.data.id,
              sku = item.sku,
              qty = item.qty,
            },
            metadata = {
              table = "order_items",
            },
          }
        end
        return out
```

| Field | Required | Description |
| --- | --- | --- |
| `language` | no | Script language. Only `lua` is implemented in the first core ABI. |
| `script` | yes | Lua script returning `nil`, one output record/data map, or an array of output records/data maps. |
| `code` | no | Alias for `script`. |
| `on_error` | no | Parse/script error policy: `dlq` (default, record-level DLQ), `drop`, or `error` (batch-level failure). |
| `timeout_ms` | no | Per-input-record script timeout in milliseconds (default `5000`). |

Metrics exposed through `transform_metrics`: `input_records`, `output_records`, `dropped_records`, and `parse_errors`.

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

### `debezium_cdc`

Normalizes Debezium Kafka CDC messages for ODS-style replication. It parses `c/u/d/r`, `source.db`, `source.table`, `ts_ms`, tombstones, and DDL-like schema-change events into `operation`, `metadata.database`, `metadata.table`, `metadata.timestamp`, and optional metadata fields.

```yaml
transforms:
  - type: debezium_cdc
    config:
      keep_metadata: true
      skip_tombstone: true
      table_mapping:
        template: "ods_{source_db}__{source_table}"
  - type: cdc_policy
    config:
      include_databases:
        - dl_vls_dev
      include_tables:
        - dl_vls_dev.vehicle_charge
      skip_delete: false
      skip_snapshot: true
      dangerous_ddl: reject
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `keep_metadata` | no | `true` | Keep `_debezium_op`, `_debezium_snapshot`, `_source_database`, `_source_table`, `_op`, and `_event_time` fields. |
| `skip_tombstone` | no | `true` | Filter Debezium tombstone messages. |
| `target_table_template` | no | | Target table template. Supports `{source_db}`, `{source_table}`, `{YYYYMMDD}`, and `{YYYY-MM-DD}`. |
| `table_mapping` | no | | String template, rules map, or `{template, rules}` map for source db/table to target table mapping. |

### `cdc_policy` / `ddl_guard`

Applies explicit CDC migration policy after `debezium_cdc`. `ddl_guard` is an alias focused on schema-change events.

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `include_databases` | no | | Source database exact/glob allowlist. |
| `exclude_databases` | no | | Source database exact/glob denylist. |
| `include_tables` | no | | Source table or `db.table` exact/glob allowlist. Matches the Debezium source table, not the mapped target table. |
| `exclude_tables` | no | | Source table or `db.table` exact/glob denylist. |
| `skip_delete` | no | `false` | Filter DELETE events. Use only when losing deletes is an explicit migration choice. |
| `skip_snapshot` | no | `false` | Filter Debezium snapshot events (`op=r` or snapshot marker). |
| `skip_tombstone` | no | `true` | Filter Debezium tombstone markers. |
| `dangerous_ddl` | no | `reject` | Action for dangerous DDL: `reject`, `drop`, or `pass`. Rejected DDL goes through normal transform error/DLQ handling. |
| `ddl_allowlist` | no | | DDL patterns allowed to pass. |
| `ddl_denylist` | no | | DDL patterns always treated as dangerous. |

Metrics exposed through `transform_metrics`: `processed`, `skipped_filter`, `skipped_delete`, `skipped_snapshot`, `skipped_tombstone`, `ddl_rejected`, `ddl_dropped`, and `ddl_passed`.

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
      state_backend: redis
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
| `state_backend` | no | | Runtime lookup cache backend. Only `redis` is allowed and it requires deployment Redis config. |
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
enable the Redis `StateStore` backend so buffered records can be restored after
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
      state_backend: redis
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
| `state_backend` | no | | Runtime join buffer backend. Only `redis` is allowed and it requires deployment Redis config. |
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

Windowed aggregation. The production configuration path exposes only `tumbling`; `sliding` / `session` are not supported by the pipeline spec.

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
      state_backend: redis
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
| `state_backend` | no | | Runtime tumbling-window state backend. Only `redis` is allowed and it requires deployment Redis config. |
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
set in memory. For crash/restart recovery, enable the Redis `StateStore`
backend.

```yaml
transforms:
  - type: deduplicate
    config:
      keys: [order_id]
      window_size: 10000
      state_backend: redis
      state_pipeline: orders-wide-table
      state_node: dedup-orders
      state_ttl_seconds: 86400
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `keys` | yes | | Fields forming the dedup key. |
| `window_size` | no | `10000` | Process-local ring size for recently seen keys. |
| `state_backend` | no | | Runtime deduplicate state backend. Only `redis` is allowed and it requires deployment Redis config. |
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

### `ts` / `javascript` / `js`

Requires a CGO build with QuickJS enabled. The function receives a full
`core.Record` JSON object and may return `null` / `undefined` / `false` to drop
the input, one full record object, one plain data object, or an array of record
objects / data objects for one-to-many parser flows.

```yaml
transforms:
  - type: javascript
    config:
      script: |
        function transform(record) {
          return record.data.items.map(function(item) {
            return {
              data: {
                order_id: record.data.id,
                sku: item.sku,
                qty: item.qty
              },
              metadata: {
                table: "order_items"
              }
            }
          })
        }
```

| Field | Required | Description |
| --- | --- | --- |
| `script` | yes | TypeScript/JavaScript function body or function declaration. |
| `code` | no | Alias for `script`. |
| `timeout_ms` | no | Per-input-record script timeout in milliseconds. QuickJS timeout granularity is seconds, with sub-second values rounded up. |

## Runtime APIs For Config Workflows

- Validate spec: `POST /api/v2/specs/validate` — returns idempotency warnings for dangerous source/sink combos
- Connection catalog: `GET/POST /api/v2/connections`, `GET/PUT/DELETE /api/v2/connections/{name}`, `POST /api/v2/connections/{name}/test` — stores reusable source/sink/transform configs, masks secret fields in responses, and records last health status
- Test ad-hoc connection: `POST /api/v2/connections/test`
- Connector descriptors: `GET /api/v2/connectors/descriptors` — returns Connector Descriptor v1 records merged from registry, config schema, secret markers, capabilities, maturity metadata, and readiness gates
- Transform dry-run: `POST /api/v2/transforms/dry-run` — returns `records`/`output_count` for multi-output transforms such as `flat_map` / `udtf` / `javascript`
- Reload specs: `POST /api/v2/specs/reload`
- Plugin schema: `GET /api/v2/plugins/schema` — returns typed field schemas with secret markers

## Stateful Processing Foundation

Stateful transforms use the `StateStore` v1 contract in `internal/etl/state`. It currently has:

- `MemoryStore` for tests and development.
- `RedisStore` for runtime state/cache with TTL, snapshot/restore, and state size stats.
- `SQLiteStore` remains only as a local test/reference implementation; SQLite/MySQL/PostgreSQL runtime storage is for checkpoint/metadata and must not be configured as state/cache backends.
- `checkpoint.Envelope` for stateful checkpoint payloads. It groups the source position, per-node state snapshot versions, sink commit metadata, and the documented `at_least_once` delivery mode in one JSON payload while remaining distinguishable from legacy source positions.

`lookup`, `join`, `window`, and `deduplicate` can use `StateStore` through
`state_backend: redis` when `etl.state.redis.addr` or `ETL_STATE_REDIS_ADDR` is configured. `lookup` persists refreshed dimension-cache rows and
can restore the latest non-expired snapshot when the dimension query fails;
`join` persists buffered interval-join records by join key; `window` persists
buffered tumbling-window aggregates; `deduplicate` persists seen keys across
restarts. Complex window semantics such as sliding/session, side outputs, and
transactional emission are outside the current ETL runtime boundary.

When a pipeline is built by the linear runner or DAG executor, stateful
transforms with `state_backend` enabled automatically receive `state_pipeline`
from the pipeline name and `state_node` from the transform/node id. Set these
fields only when you need an explicit shared or migrated state namespace.

The linear runner and DAG executor now save checkpoint envelopes that bind the
source position, latest state snapshot versions, and sink acknowledgement
metadata after a successful sink write. They unwrap the source position again
before reopening the source. If state snapshot or sink acknowledgement metadata
collection fails, the source checkpoint is not advanced.

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
- evidence-driven `maturity`: `production`, `beta`, `experimental`, or `dev-only`
- `readiness`: machine-readable gates for registry/config schema, schema/preflight, checkpoint or replay absorption, and e2e evidence. This helps UI/wizard flows explain production gaps without replacing the maturity value.

WASM plugins use Plugin ABI v1 metadata in `internal/etl/plugin/pluginsystem`:

- ABI string: `openetl.plugin.abi/v1`
- Minimum runtime contract: `openetl-runtime/v1`
- Required entrypoints: source plugins export `read`, sink plugins export `write`, transform plugins export `transform`
- Transform output contract: empty output, `null`, or `false` drops the input; a JSON record object or plain data object emits one record; an array of record/data objects emits multiple records through the batch transform path
- Config field types: `string`, `int`, `bool`, `float`, `string_array`, `map`
- `/api/v2/plugins/install` accepts an optional multipart `manifest` JSON field. Explicit manifests are validated before the WASM file is loaded; legacy uploads without a manifest are reported as `manifest_validated=false`.
- `/api/v2/plugins/compile` is transform-only. Source and sink plugins must be compiled offline and installed through `/api/v2/plugins/install`.

See `docs/plugin-abi-v1.md` for the manifest shape, compatibility matrix, and deprecation policy.

## Idempotency Warnings

Spec validation (`POST /api/v2/specs/validate`) returns warnings for potentially dangerous combinations:

- CDC source + file/S3 sink: no deduplication, risk of duplicates on restart
- `mysql_batch` + `mysql` sink with `batch_mode: insert`: duplicate key errors on re-run
- CDC source + append-only sink: UPDATE/DELETE written as rows, not mutations
