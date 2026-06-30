# ETL YAML 配置参考

Pipeline spec 是 `pipes/` 或配置的 `etl.specsDir` 下的 YAML 文件。它表达 OpenETL-Go 的核心模型：轻量自托管的 `Source -> Transform -> Sink` 数据同步、清洗和汇聚管道。环境变量在解析前通过 `${VAR}` 或 `${VAR:-default}` 展开。

配置能力应服务常见 CDC/ETL 链路、checkpoint、DLQ、幂等写入和轻量聚合；复杂 keyed state、timer、SQL planner、savepoint 等完整流计算语义不属于当前生产配置目标。产品边界见[产品定位](./positioning.zh.md)。

## 顶层 Spec

```yaml
name: example-pipeline
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
flush_interval_ms: 1000
checkpoint_interval_sec: 30
backpressure_buffer: 100
retry:
  max_attempts: 3
  initial_interval_ms: 1000
  max_interval_ms: 30000
dlq:
  enable: true
```

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `name` | 是 | 唯一管道名称。重复的运行时创建返回 `409`；重复文件被跳过。 |
| `source.type` | 设置 `source.connection` 时可省略 | 已注册的 Source 插件名称。使用 `connection` 且省略 `type` 时，会从已保存连接推断。 |
| `source.connection` / `source.connection_ref` | 否 | 引用连接目录中的 Source 配置作为基础配置。内联 `source.config` 会覆盖连接配置中的同名字段。 |
| `source.config` | 否 | Source 专属设置。默认为 `{}`。 |
| `transforms` | 否 | 有序 Transform 链。无 Transform 可省略。每个 Transform 也可使用 `connection` / `connection_ref`。 |
| `sink.type` | 设置 `sink.connection` 时可省略 | 已注册的 Sink 插件名称。使用 `connection` 且省略 `type` 时，会从已保存连接推断。 |
| `sink.connection` / `sink.connection_ref` | 否 | 引用连接目录中的 Sink 配置作为基础配置。内联 `sink.config` 会覆盖连接配置中的同名字段。 |
| `sink.config` | 否 | Sink 专属设置。默认为 `{}`。 |
| `schedule.type` | 否 | 触发类型。省略时使用 source descriptor 的 `default_schedule`。内置 CDC/流式 source（`mysql_cdc`、`postgres_cdc`、`mysql_snapshot_cdc`、`kafka`）当前只支持 `streaming`；批/拉取 source（`mysql_batch`、`file`、`http`）支持 `once`、`cron`、`periodic` 和 `dependency`。`spec validate` 会拒绝不在 source descriptor `supported_schedules` 内的调度类型。 |
| `schedule.cron` | `cron` 时必填 | `schedule.type: cron` 使用的 Cron 表达式。 |
| `schedule.interval_sec` | `periodic` 时必填 | `schedule.type: periodic` 使用的正整数秒级间隔。 |
| `schedule.depends_on` | `dependency` 时必填 | 上游 pipeline 名称列表，上游完成后触发当前 pipeline。 |
| `batch_size` | 否 | Sink 每批最大记录数。默认 `1000`。 |
| `flush_interval_ms` | 否 | 批次 flush 间隔（毫秒）。默认 `1000`。 |
| `checkpoint_interval_sec` | 否 | 检查点保存间隔（秒）。默认 `30`。 |
| `backpressure_buffer` | 否 | 内部记录通道大小。默认 `100`。 |
| `retry` | 否 | 重试策略。默认值见上方。 |
| `dlq.enable` | 否 | 启用死信队列（DLQ）。默认 `true`。 |

### 复用已保存连接

通过 `GET/POST /api/v2/connections` 保存的连接，可以被线性 pipeline 的 source/sink/transform 和 DAG node 引用：

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

已保存连接提供 `kind`、`type` 和基础 `config`。管道端点必须使用 kind 匹配的连接（`source`、`sink` 或 `transform`）。内联 `config` 优先级更高，因此账号、地址等共享配置可以放在连接目录中，而 `table`、`topic`、`query` 等任务级字段仍留在 pipeline spec 中。

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

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `path` | 是 | | 容器内文件路径。Docker 部署建议把输入文件挂载到 `/app/data/input`。 |
| `format` | 否 | `csv` | `json`（JSON Lines）或 `csv`。 |
| `delimiter` | 否 | `,` | CSV 分隔符。 |
| `has_header` | 否 | `true` | CSV 首行是否为列名。 |
| `schema` | 否 | | 仅用于 preflight 的 schema hint，格式为 `[{name,data_type,nullable}]` 或 `{field: type}`。 |
| `sample` | 否 | | 仅用于 preflight 的样例记录；当校验阶段无法读取文件路径时用于推断 schema。 |

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

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `url` | 是 | | 基础 URL。 |
| `method` | 否 | `GET` | HTTP 方法。 |
| `headers` | 否 | | 请求头映射。 |
| `pagination` | 否 | | `page` 或留空。 |
| `page_param` | 否 | | 页码查询参数名。 |
| `size_param` | 否 | | 每页大小查询参数名。 |
| `page_size` | 否 | `100` | 每页大小。 |
| `max_pages` | 否 | `100` | 最大读取页数。 |
| `result_key` | 否 | 自动 | JSON 数组 key。自动检测 `data`、`items`、`results`。 |
| `auth_token` | 否 | | Bearer 令牌（**密钥**）。建议使用环境变量插值。 |
| `schema` | 否 | | 仅用于 preflight 的 schema hint，格式为 `[{name,data_type,nullable}]` 或 `{field: type}`。 |
| `sample` | 否 | | 仅用于 preflight 的样例响应记录；不调用远端 API 时用于推断 schema。 |

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

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `host` | 是 | | MySQL 主机。 |
| `port` | 否 | `3306` | MySQL 端口。 |
| `user` | 是 | | MySQL 用户。 |
| `password` | 否 | | MySQL 密码（**密钥**）。 |
| `database` | 是 | | 源数据库。 |
| `table` | 是 | | 源表。 |
| `pk_column` | 否 | `id` | 用于分页的主键列。 |
| `limit` | 否 | `5000` | 每页查询行数。 |
| `columns` | 否 | `*` | 要 SELECT 的特定列。 |

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

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `host` | 是 | | MySQL 主机。 |
| `port` | 否 | `3306` | MySQL 端口。 |
| `user` | 是 | | MySQL 用户。 |
| `password` | 否 | | MySQL 密码（**密钥**）。 |
| `database` | 是 | | 源数据库。 |
| `tables` | 是 | | 要监听的表名数组。 |
| `server_id` | 否 | `1001` | 唯一复制 server ID。 |

需要 MySQL binlog `ROW` 格式和 `FULL` row image。

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

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `host` | 是 | | MySQL 主机。 |
| `port` | 否 | `3306` | MySQL 端口。 |
| `user` | 是 | | MySQL 用户。 |
| `password` | 否 | | MySQL 密码（**密钥**）。 |
| `database` | 是 | | 源数据库。 |
| `table` | 是 | | 源表（单数）。 |
| `pk_column` | 否 | `id` | 快照分页的主键列。 |
| `limit` | 否 | `1000` | 每次快照查询的行数。 |
| `server_id` | 否 | `1101` | 唯一复制 server ID。 |

按主键分块快照，记录 binlog 位置，然后切换到 CDC。两个阶段的 checkpoint 都可以在崩溃后恢复。

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

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `host` | 是 | | PostgreSQL 主机。 |
| `port` | 否 | `5432` | PostgreSQL 端口。 |
| `user` | 是 | | PostgreSQL 用户。 |
| `password` | 否 | | PostgreSQL 密码（**密钥**）。 |
| `database` | 是 | | 源数据库。 |
| `slot_name` | 否 | `etl_slot` | 逻辑复制槽名称。 |
| `tables` | 否 | | 要监听的表。 |

使用 pgoutput 逻辑复制协议。如果缺失会自动创建发布和复制槽。

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

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `brokers` | 是 | `["localhost:9092"]` | Kafka broker 地址。 |
| `topic` | 是 | | 要消费的 Kafka topic。 |
| `group_id` | 否 | `etl-consumer` | 消费者组 ID。 |
| `format` | 否 | `json` | 消息格式：`json` 或 `text`。 |
| `key_column` | 否 | | 消息 key 的列名。 |
| `value_column` | 否 | | 原始消息 value 的列名。 |
| `schema` | 否 | | 仅用于 preflight 的 schema hint，格式为 `[{name,data_type,nullable}]` 或 `{field: type}`。 |
| `sample` | 否 | | 仅用于 preflight 的样例消息；不消费 Kafka 时用于推断 schema。 |

### `redis`

```yaml
source:
  type: redis
  config:
    host: redis
    port: 6379
    password: ${REDIS_PASSWORD}
    db: 0
    pattern: "user:*"
    batch_size: 500
```

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `host` | 是 | | Redis 主机。 |
| `port` | 否 | `6379` | Redis 端口。 |
| `password` | 否 | | Redis 密码（**密钥**）。 |
| `db` | 否 | `0` | Redis 数据库编号。 |
| `pattern` | 否 | `*` | SCAN 的 key 匹配模式。 |
| `batch_size` | 否 | `500` | 每批 SCAN 返回的 key 数量。 |

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

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `output_dir` | 是 | `/tmp/etl-output` | 输出目录路径。 |
| `format` | 否 | `json` | `json`、`jsonl`、`csv` 或 `parquet`。 |
| `prefix` | 否 | | 文件名前缀。 |

Parquet 格式将所有列写入为字符串的扁平 schema。

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

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `endpoint` | 否 | | S3 兼容端点 URL（如 MinIO）。 |
| `region` | 否 | | S3 区域。 |
| `bucket` | 是 | | S3 桶名称。 |
| `access_key` | 否 | | 访问密钥（**密钥**）。 |
| `secret_key` | 否 | | 秘密密钥（**密钥**）。 |
| `output_dir` | 否 | `/tmp/etl-output` | 本地回退目录。 |
| `format` | 否 | `json` | `json`、`jsonl`、`csv` 或 `parquet`。 |
| `prefix` | 否 | | 对象 key 前缀。 |

使用 MinIO 兼容 API（当配置了 endpoint/bucket 时），否则回退到本地文件。

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
    auto_create: true
```

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `host` | 是 | | MySQL 主机。 |
| `port` | 否 | `3306` | MySQL 端口。 |
| `user` | 是 | | MySQL 用户。 |
| `password` | 否 | | MySQL 密码（**密钥**）。 |
| `database` | 是 | | 目标数据库。 |
| `table` | 是 | | 目标表。 |
| `batch_mode` | 否 | `insert` | `insert` 或 `upsert`。 |
| `pk_columns` | 否 | `["id"]` | Upsert 模式的主键列。 |
| `auto_create` | 否 | `false` | 自动建表。 |

CDC/snapshot+CDC 幂等性请使用 `batch_mode: upsert`。

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

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `host` | 是 | | ClickHouse 主机。 |
| `port` | 否 | `9000` | ClickHouse 原生端口。 |
| `user` | 否 | `default` | ClickHouse 用户。 |
| `password` | 否 | | ClickHouse 密码（**密钥**）。 |
| `database` | 是 | | 目标数据库。 |
| `table` | 是 | | 目标表。 |
| `pk_columns` | 否 | `["id"]` | 用于自动建表的主键列。 |
| `version_column` | 否 | `_version` | ReplacingMergeTree 的版本列。 |
| `auto_create` | 否 | `false` | 表缺失时自动创建。 |
| `schema_drift` | 否 | `ignore` | `ignore`、`fail` 或 `add_columns`。 |

### `maxcompute` / `odps`

Kafka ODS JSON -> MaxCompute 分区表的 experimental connector。当前 sink 已接入阿里云 ODPS SDK batch tunnel writer，并校验配置、schema 和分区字段；但在真实 MaxCompute 集成环境补齐可重复写入、重放和故障注入证据前，maturity 仍保持 experimental。

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

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `endpoint` | 是 | | MaxCompute endpoint URL。 |
| `tunnel_endpoint` | 否 | | MaxCompute Tunnel endpoint URL；不填时由 SDK 从 project 解析。 |
| `project` | 是 | | MaxCompute project 名称。 |
| `table` | 是 | | 目标表名。 |
| `access_key_id` | 是 | | 阿里云 AccessKey ID（**密钥**）。 |
| `access_key_secret` | 是 | | 阿里云 AccessKey Secret（**密钥**）。 |
| `quota_name` | 否 | | Tunnel upload session 使用的 MaxCompute quota 名称。 |
| `columns` | 否 | | 目标字段类型映射。第一版支持 `STRING`、`BIGINT`、`DOUBLE`、`DECIMAL`、`BOOLEAN`、`DATETIME`、`TIMESTAMP`。 |
| `partition` | 否 | | 静态分区值，例如 `{dt: "2026-06-26"}`。 |
| `partition_fields` | 否 | | 从 record 字段取值的动态分区字段，例如 `[dt]`。 |
| `write_mode` | 否 | `append` | `append` 或 `partition_overwrite`。append 是 at-least-once，重放可能产生重复。 |
| `auto_create_partition` | 否 | `true` | 上传时通过 Tunnel SDK 创建缺失目标分区。 |
| `batch_size` | 否 | `500` | 每批写入行数。 |
| `max_retries` | 否 | `3` | 临时写入失败重试次数。 |
| `retry_base_ms` | 否 | `500` | 重试基础退避毫秒数。 |

使用该 sink 前应通过 `project` / `type_convert` 让 record schema 匹配声明的 MaxCompute `columns`，并包含所有动态 `partition_fields` 字段。Preflight 会加载远端表并校验 table/partition/权限可达性。`append` 是 at-least-once，checkpoint reset 或 replay 后可能重复；不能接受重复时应使用业务键、staging+merge 或受控的 `partition_overwrite` 流程。

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

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `brokers` | 是 | `["localhost:9092"]` | Kafka broker 地址。 |
| `topic` | 是 | | 要生产的 Kafka topic。 |
| `key_column` | 否 | | 消息 key 的列名。 |
| `compression` | 否 | `none` | `none`、`gzip`、`snappy` 或 `lz4`。 |

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

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `hosts` | 是 | `["http://localhost:9200"]` | Elasticsearch/OpenSearch 主机 URL。 |
| `username` | 否 | | ES 用户名（**密钥**）。 |
| `password` | 否 | | ES 密码（**密钥**）。 |
| `index` | 是 | | 目标索引名称。 |
| `id_column` | 否 | `id` | 文档 ID 的列名（启用 upsert）。 |
| `mappings` | 否 | | 可选 Elasticsearch mapping，用于 preflight schema 校验。未配置时，preflight 会在目标可达时读取 `/{index}/_mapping`。 |
| `properties` | 否 | | mapping properties 简写，例如 `{id: {type: long}, status: {type: keyword}}`。 |
| `chunk_size` | 否 | `500` | 每个 bulk 请求的记录数。 |
| `max_retries` | 否 | `3` | 临时 bulk 失败的重试次数。 |
| `retry_base_ms` | 否 | `500` | 基础重试延迟，单位毫秒。 |
| `tls_skip_verify` | 否 | `false` | 跳过 TLS 证书校验。 |

当配置或远端索引存在 mapping properties 时，preflight 会校验 source 字段与 mapping 类型是否兼容，并在首次 bulk 写入前返回字段级类型问题，例如字符串写入 `long` 字段。

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
    table: customers
    batch_mode: upsert
    pk_columns: [id]
```

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `host` | 是 | | PostgreSQL 主机。 |
| `port` | 否 | `5432` | PostgreSQL 端口。 |
| `user` | 是 | | PostgreSQL 用户。 |
| `password` | 否 | | PostgreSQL 密码（**密钥**）。 |
| `database` | 是 | | 目标数据库。 |
| `table` | 是 | | 目标表。 |
| `batch_mode` | 否 | `insert` | `insert` 或 `upsert`（INSERT … ON CONFLICT）。 |
| `pk_columns` | 否 | `["id"]` | Upsert 模式的主键列。 |

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

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `host` | 是 | | Doris FE 主机。 |
| `port` | 否 | `9030` | Doris MySQL 协议端口，用于 DDL、insert fallback 和 DELETE。 |
| `http_port` | 否 | `8030` | Stream Load HTTP 端口。 |
| `user` | 否 | `root` | Doris 用户。 |
| `password` | 否 | | Doris 密码（**密钥**）。 |
| `database` | 是 | | 目标数据库。 |
| `table` | 是 | | 目标表。只有 CDC 动态表路由且每条记录都有 `metadata.table` 时才可留空。 |
| `write_mode` | 否 | `stream_load` | `stream_load` 或 MySQL 协议 `insert` fallback。 |
| `batch_mode` | 否 | `insert` | `insert` 或 `upsert`。生产 CDC/upsert 需要 Doris Unique Key 表和稳定 `pk_columns`。 |
| `pk_columns` | 否 | | DELETE、自动创建 Unique Key 表和 replay-safe upsert 校验使用的业务主键列。 |
| `stream_load_format` | 否 | `json` | `json` 或 `csv`。 |
| `stream_load_scheme` | 否 | `http` | `http` 或 `https`。 |
| `tls_skip_verify` | 否 | `false` | 跳过 TLS 证书校验。 |
| `auto_create` | 否 | `false` | 自动创建 Doris Unique Key 表。未配置 `pk_columns` 时必须存在 `id` 字段。 |
| `schema_drift` | 否 | `ignore` | `ignore`、`fail` 或 `add_columns`。 |
| `ddl_policy` | 否 | `reject` | `reject`、`ignore` 或 `apply`。生产默认拒绝源端 DDL；Doris `apply` 仅允许安全的 `ALTER TABLE ... ADD COLUMN` 子集。 |
| `allow_mixed_cdc_non_atomic` | 否 | `false` | 允许混合 write/delete CDC 批次，需接受 Stream Load 与 MySQL DELETE 非原子语义。 |

### `redis`

```yaml
sink:
  type: redis
  config:
    host: redis
    port: 6379
    password: ${REDIS_PASSWORD}
    db: 0
    mode: hash
    key_column: id
```

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `host` | 是 | | Redis 主机。 |
| `port` | 否 | `6379` | Redis 端口。 |
| `password` | 否 | | Redis 密码（**密钥**）。 |
| `db` | 否 | `0` | Redis 数据库编号。 |
| `mode` | 否 | `hash` | 写入模式：`hash`、`string` 或 `list`。 |
| `key_column` | 否 | `id` | Hash/String 模式的 key 列名。 |

## Transforms

### `identity`

原样传递记录。无需 config。

### `rename`

```yaml
transforms:
  - type: rename
    config:
      mappings:
        old_name: new_name
        foo: bar
```

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `mappings` | 是 | old_name → new_name 映射表。 |

### `drop_field`

```yaml
transforms:
  - type: drop_field
    config:
      fields: [password_hash, internal_id]
```

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `fields` | 是 | 要移除的字段名数组。 |

### `add_field`

```yaml
transforms:
  - type: add_field
    config:
      field: source_system
      value: crm
```

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `field` | 是 | 要添加的字段名。 |
| `value` | 是 | 字段值。支持 `{{now}}`（RFC3339）、`{{ts}}`（Unix 时间戳）。 |

### `project` / `select_fields`

把记录投影成明确的输出字段集合。`select_fields` 是同一 transform 的别名。

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

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `fields` | 否 | 保留并同名输出的源字段；缺失字段会被忽略。 |
| `mappings` | 否 | 源字段 → 输出字段别名映射；缺失源字段会被忽略。 |
| `constants` | 否 | 在 fields 和 mappings 之后写入的常量输出字段。 |
| `time_formats` | 否 | 输出字段 → 时间格式映射。支持 `unix`、`unix_ms`、`rfc3339`，或 `2006-01-02` 这类 Go time layout。 |
| `keep_unmapped` | 否 | 保留未被 `fields` 或 `mappings` 列出的输入字段（默认 `false`）。被映射的源字段会重命名为目标字段。 |

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

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `conversions` | 是 | 字段 → 目标类型映射：`int`、`float`、`bool`、`string`、`datetime`。 |

### `filter`

```yaml
transforms:
  - type: filter
    config:
      expression: "deleted_at != nil"
```

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `expression` | 是 | 过滤表达式。支持 `deleted_at != nil`、`status == 'value'`、`amount > 100 && status == 'paid'` 等模式。 |

被过滤的记录不算 DLQ 错误，可以推进 checkpoint。

### `flat_map` / `udtf`

把一条输入记录展开为 0、1 或多条输出记录。`udtf` 是同一 transform 的别名。第一版核心 ABI 使用 Lua：脚本会收到完整 `record` 表（`record.data`、`record.metadata`、`record.before`、`record.operation`），并返回 `nil`/`false`、一条 record/data map，或 record/data map 数组。输出记录默认继承输入的 operation 和 metadata，除非返回的 record 显式覆盖。

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

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `language` | 否 | 脚本语言。第一版核心 ABI 仅实现 `lua`。 |
| `script` | 是 | Lua 脚本，返回 `nil`、一条输出 record/data map，或输出 record/data map 数组。 |
| `code` | 否 | `script` 的别名。 |
| `on_error` | 否 | 解析/脚本错误策略：`dlq`（默认，记录级 DLQ）、`drop` 或 `error`（批级失败）。 |
| `timeout_ms` | 否 | 单条输入记录的脚本超时时间，毫秒，默认 `5000`。 |

通过 `transform_metrics` 暴露指标：`input_records`、`output_records`、`dropped_records` 和 `parse_errors`。

### `normalize_envelope` / `debezium_envelope`

将 Kafka 中的普通 JSON 或 Debezium-like envelope 标准化为后续 `lookup` / `window` 可处理的记录。普通 JSON 会原样通过；Debezium payload 会展开 `after` / `before`，并设置 `operation`、`metadata.table`、`metadata.timestamp`。

```yaml
transforms:
  - type: normalize_envelope
    config:
      keep_metadata: true
```

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `keep_metadata` | 否 | `true` | 在记录中保留 `_op`、`_source_table`、`_event_time`。 |

### `debezium_cdc`

面向 ODS 同步的 Debezium Kafka CDC 标准化 transform。它解析 `c/u/d/r`、`source.db`、`source.table`、`ts_ms`、tombstone 和 DDL-like schema change 事件，并输出标准 `operation`、`metadata.database`、`metadata.table`、`metadata.timestamp` 以及可选元数据字段。

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

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `keep_metadata` | 否 | `true` | 保留 `_debezium_op`、`_debezium_snapshot`、`_source_database`、`_source_table`、`_op` 和 `_event_time` 字段。 |
| `skip_tombstone` | 否 | `true` | 过滤 Debezium tombstone 消息。 |
| `target_table_template` | 否 | | 目标表模板。支持 `{source_db}`、`{source_table}`、`{YYYYMMDD}` 和 `{YYYY-MM-DD}`。 |
| `table_mapping` | 否 | | 源库/表到目标表的模板、规则表，或 `{template, rules}` 映射。 |

### `cdc_policy` / `ddl_guard`

在 `debezium_cdc` 之后应用显式 CDC 迁移策略。`ddl_guard` 是偏向 schema-change 事件的同能力别名。

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `include_databases` | 否 | | 源库精确/通配 allowlist。 |
| `exclude_databases` | 否 | | 源库精确/通配 denylist。 |
| `include_tables` | 否 | | 源表或 `db.table` 精确/通配 allowlist；匹配 Debezium 源表，不匹配映射后的目标表。 |
| `exclude_tables` | 否 | | 源表或 `db.table` 精确/通配 denylist。 |
| `skip_delete` | 否 | `false` | 过滤 DELETE 事件。只有明确接受丢失删除语义时才应开启。 |
| `skip_snapshot` | 否 | `false` | 过滤 Debezium snapshot 事件（`op=r` 或 snapshot 标记）。 |
| `skip_tombstone` | 否 | `true` | 过滤 Debezium tombstone 标记。 |
| `dangerous_ddl` | 否 | `reject` | 危险 DDL 处理方式：`reject`、`drop` 或 `pass`。reject 会走正常 transform error/DLQ 路径。 |
| `ddl_allowlist` | 否 | | 允许通过的 DDL 模式。 |
| `ddl_denylist` | 否 | | 一律视为危险的 DDL 模式。 |

通过 `transform_metrics` 暴露指标：`processed`、`skipped_filter`、`skipped_delete`、`skipped_snapshot`、`skipped_tombstone`、`ddl_rejected`、`ddl_dropped` 和 `ddl_passed`。

### `lookup`

用于 stream-table join：启动后从 MySQL/PostgreSQL 维表加载缓存，并按 `join_key` 补充维表字段。

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

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `dsn` | 是 | | 维表数据库 DSN。 |
| `query` | 是 | | 加载维表的 SQL。 |
| `join_key` | 否 | `id` | 输入记录中的关联字段。 |
| `dim_key` | 否 | `id` | 维表查询结果中的关联字段。 |
| `fields` | 是 | | 要复制到记录中的维表字段。 |
| `refresh_interval_sec` | 否 | `300` | 维表全量刷新间隔，`0` 表示只加载一次。 |
| `max_cache_entries` | 否 | `0` | 维表缓存最多保留的不同 key 数，`0` 表示不限。超过上限会让刷新/恢复失败，并递增 `cache_limit_exceeded` 指标。 |
| `on_miss` | 否 | `pass` | 维表未命中时的动作：`pass` 保持记录不变，`null` 将配置的维表字段显式写成 null，`dlq`/`error` 返回错误并进入 runner 的 DLQ 路径。 |
| `on_refresh_error` | 否 | `pass` | 维表刷新失败且没有可用缓存时的动作：`pass` 保持记录不变，`error` 返回错误并进入 runner 的 DLQ 路径。 |
| `state_backend` | 否 | | 运行时 lookup 缓存后端；仅允许 `redis`，并要求部署配置 Redis。 |
| `state_pipeline` | 否 | pipeline name | 持久化 lookup 缓存的 pipeline 命名空间；省略时由运行时注入 pipeline 名称。 |
| `state_node` | 否 | transform node id | 持久化 lookup 缓存的 node 命名空间；省略时由运行时注入 transform/node ID。 |
| `state_ttl_seconds` | 否 | `0` | 持久化 lookup 行的 TTL，`0` 表示不过期。 |

启用 `state_backend` 后，每次维表刷新成功都会写入 `StateStore`。如果后续维表查询失败，lookup 会尝试恢复最近一份未过期缓存，用已知维表值继续补充记录。

生产宽表链路中，如果 left join 未命中是可接受场景，建议使用 `on_miss: "null"`，让下游明确看到维表字段为空；如果维表未命中或缓存刷新失败必须人工处理，则使用 `on_miss: dlq` 或 `on_refresh_error: error`，让当前记录进入标准 DLQ 路径。

### `join`

执行 stream-stream interval join：在 `join_window_sec` 时间窗口内缓存记录，并用后续记录的 `join_key` 匹配已缓存记录。如果需要 crash/restart 后恢复 join 缓冲区，可以启用 Redis `StateStore` 后端。

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

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `join_type` | 否 | `inner` | Join 类型，支持 `inner`、`left`。 |
| `join_key` | 是 | | 用于关联的字段名。 |
| `join_window_sec` | 否 | `60` | 记录在 join 状态中保留的时间窗口。 |
| `join_fields` | 是 | | 从匹配到的缓存记录中复制的字段。 |
| `join_prefix` | 否 | `joined_` | 复制字段的前缀。 |
| `where` | 否 | | 针对缓存记录的可选过滤表达式。 |
| `on_miss` | 否 | `drop` | inner join 未命中时的动作：`drop`、`dlq` 或 `error`。 |
| `max_buffered_keys` | 否 | `0` | 内存中最多保留的 join key 数，`0` 表示不限制。 |
| `max_buffered_records` | 否 | `0` | 内存中最多保留的 join 记录数，`0` 表示不限制。 |
| `state_backend` | 否 | | 运行时 join 缓冲后端；仅允许 `redis`，并要求部署配置 Redis。 |
| `state_pipeline` | 否 | pipeline name | 持久化 join 缓冲的 pipeline 命名空间；省略时由运行时注入 pipeline 名称。 |
| `state_node` | 否 | transform node id | 持久化 join 缓冲的 node 命名空间；省略时由运行时注入 transform/node ID。 |
| `state_ttl_seconds` | 否 | `0` | 持久化 join 缓冲的 TTL，`0` 表示使用 `join_window_sec`。 |

启用 `state_backend` 后，`join` 会在每次更新后按 join key 持久化缓冲记录，并在启动时恢复未过期记录。这能改善 stream-stream join 的 crash/restart 恢复，但不等价于 Kafka offset、状态快照和 sink commit 之间的 exactly-once 事务。

当 `max_buffered_keys` 或 `max_buffered_records` 超出上限时，`join` 会拒绝当前记录并返回错误，而不是继续扩大无界状态；该错误会进入现有 pipeline retry/DLQ 路径，并通过 transform metrics 暴露 `state_limit_exceeded` 计数。

### `window`

窗口聚合。当前生产配置路径只暴露 `tumbling` window；pipeline spec 不支持 `sliding` / `session`。

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

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `window_type` | 否 | `tumbling` | 仅支持 `tumbling`。 |
| `window_size_seconds` | 否 | `60` | 固定窗口大小。 |
| `allowed_lateness_seconds` | 否 | `0` | 允许的 event-time 迟到时间。 |
| `group_by` | 否 | | 分组字段。 |
| `aggregates` | 是 | | 聚合定义，支持 `count`、`sum`、`avg`、`min`、`max`、`first`、`last`。 |
| `state_backend` | 否 | | 运行时 tumbling window 状态后端；仅允许 `redis`，并要求部署配置 Redis。 |
| `state_pipeline` | 否 | pipeline name | 持久化 window 状态的 pipeline 命名空间；省略时由运行时注入 pipeline 名称。 |
| `state_node` | 否 | transform node id | 持久化 window 状态的 node 命名空间；省略时由运行时注入 transform/node ID。 |
| `state_ttl_seconds` | 否 | `0` | 持久化 window 状态的 TTL，`0` 表示不过期。 |

启用 `state_backend` 后，`window` 会持久化 tumbling window 的聚合缓冲状态，并在启动时恢复；重启前已经累计但尚未输出的记录仍可参与最终聚合。这是 at-least-once pipeline 的恢复辅助能力，不等价于 Kafka offset、window 输出和下游 sink commit 之间的事务。

### `deduplicate`

按复合 key 过滤重复记录。默认只在进程内保存最近 key；如果需要 crash/restart 后仍能去重，可以启用 Redis `StateStore` 后端。

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

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `keys` | 是 | | 组成去重 key 的字段。 |
| `window_size` | 否 | `10000` | 进程内最近 key ring 大小。 |
| `state_backend` | 否 | | 运行时去重状态后端；仅允许 `redis`，并要求部署配置 Redis。 |
| `state_pipeline` | 否 | pipeline name | 持久化去重 key 的 pipeline 命名空间；省略时由运行时注入 pipeline 名称。 |
| `state_node` | 否 | transform node id | 持久化去重 key 的 node 命名空间；省略时由运行时注入 transform/node ID。 |
| `state_ttl_seconds` | 否 | `0` | 持久化去重 key 的 TTL，`0` 表示不过期。 |

### `lua`

```yaml
transforms:
  - type: lua
    config:
      script: |
        record.data.name = string.upper(record.data.name)
        return record
```

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `script` | 是 | Lua 脚本代码。接收 `record` 表和 `metadata` 表。 |

### `ts` / `javascript` / `js`

需要启用 CGO 的 QuickJS 构建。函数接收完整的 `core.Record` JSON 对象，
可以返回 `null` / `undefined` / `false` 丢弃当前输入，返回一条完整 record
对象、一条普通 data 对象，或返回 record/data 对象数组来承载一进多出的解析链路。

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

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `script` | 是 | TypeScript/JavaScript 函数体或函数声明。 |
| `code` | 否 | `script` 的别名。 |
| `timeout_ms` | 否 | 单条输入记录的脚本超时时间，毫秒。QuickJS 的超时粒度是秒，亚秒值会上取整。 |

## 运行时配置 API

- 校验 spec：`POST /api/v2/specs/validate` — 返回危险 Source/Sink 组合的幂等性警告
- 连接目录：`GET/POST /api/v2/connections`、`GET/PUT/DELETE /api/v2/connections/{name}`、`POST /api/v2/connections/{name}/test` — 保存可复用 source/sink/transform 配置，响应中脱敏密钥字段，并记录最近健康状态
- 临时测试连接：`POST /api/v2/connections/test`
- Connector Descriptor：`GET /api/v2/connectors/descriptors` — 返回 Connector Descriptor v1，聚合 registry、配置 schema、secret 标记、capabilities、maturity metadata 和 readiness gates
- Transform 试运行：`POST /api/v2/transforms/dry-run` — 对 `flat_map` / `udtf` / `javascript` 等多输出 transform 返回 `records` / `output_count`
- 重载 spec：`POST /api/v2/specs/reload`
- 插件 schema：`GET /api/v2/plugins/schema` — 返回带 secret 标记的类型化字段 schema

## 状态化处理基础

状态化 transform 使用 `internal/etl/state` 中的 `StateStore` v1 契约。当前已经具备：

- `MemoryStore`：用于测试和开发。
- `RedisStore`：用于运行时 state/cache，支持 TTL、snapshot/restore 和状态大小统计。
- `SQLiteStore`：仅保留为本地测试/参考实现；SQLite/MySQL/PostgreSQL runtime storage 只用于 checkpoint/metadata，不得配置为 state/cache 后端。
- `checkpoint.Envelope`：用于状态化 checkpoint payload，把 source position、每个 node 的 state snapshot version、sink commit metadata 和已文档化的 `at_least_once` 交付模式放在同一个 JSON payload 中，同时可与旧 source position 区分。

`lookup`、`join`、`window` 和 `deduplicate` 可以在配置 `etl.state.redis.addr` 或 `ETL_STATE_REDIS_ADDR` 后通过 `state_backend: redis` 使用 `StateStore`。`lookup` 会持久化刷新成功的维表缓存，并在维表查询失败时恢复最近一份未过期快照；`join` 会按 join key 持久化 interval join 缓冲记录；`window` 会持久化 tumbling window 聚合缓冲；`deduplicate` 会在进程重启后继续识别已见 key。sliding/session、side output 和事务化输出等复杂 window 语义不属于当前 ETL runtime 边界。

当 pipeline 由 linear runner 或 DAG executor 构建时，只要 transform 启用了 `state_backend`，运行时会自动把 `state_pipeline` 设置为 pipeline 名称，并把 `state_node` 设置为 transform/node ID。只有需要显式共享状态命名空间或迁移旧状态时，才需要手动配置这两个字段。

linear runner 与 DAG executor 现在都会为实现 `StateSnapshotter` 的状态化 transform 保存 checkpoint envelope：sink 写入成功后，把 source position 与最新 state snapshot version 一起保存；source 重新打开前会自动把 envelope 解回原始 source position。如果状态快照采集失败，source checkpoint 不会前进。sink commit metadata 仍是 roadmap 项。

`deduplicate`、`lookup`、`join` 和 `window` 在启用 `state_backend` 后也会暴露基础状态指标。`/api/v2/metrics` 会按 node 返回 `state_metrics`，Prometheus `/metrics` 会导出：

- `etl_state_keys{pipeline,node}`
- `etl_state_bytes{pipeline,node}`
- `etl_state_updated_timestamp_seconds{pipeline,node}`

`deduplicate` 会在 `/api/v2/metrics` 的 `transform_metrics` 中暴露 `processed`、`passed`、`duplicate_dropped`、`memory_duplicate_dropped`、`state_duplicate_dropped` 和 `evicted_keys`，并在 Prometheus 中输出 `etl_transform_metric_total{pipeline,node,transform,metric}`。

`lookup` 会通过同一组 `transform_metrics` 和 Prometheus counter 暴露 `processed`、`hit`、`miss`、`missing_key`、`miss_null`、`miss_dlq`、`miss_error`、`refresh_success`、`refresh_error`、`refresh_error_dlq`、`restore_success`、`scan_error` 和 `cache_limit_exceeded`。这些指标用于观察维表缓存命中率、miss 处理方式、旧状态恢复、外部依赖失败、行扫描失败和缓存上限压力。

`join` 还会在 `/api/v2/metrics` 的 `transform_metrics` 中暴露业务计数，并在 Prometheus 中输出 `etl_transform_metric_total{pipeline,node,transform,metric}`。当前 join 指标包括 `hit`、`miss`、`miss_dropped`、`miss_dlq`、`miss_error` 和 `state_limit_exceeded`。

`window` 也会通过同一组 `transform_metrics` 和 Prometheus counter 暴露 `accumulated`、`late_dropped`、`emitted_records`、`emitted_windows` 和 `flushed_records`。

## Connector Descriptor 与 Plugin ABI

`GET /api/v2/connectors/descriptors` 返回 Connector Descriptor v1。每个 descriptor 包含：

- `kind`、`type`、`version`、`registered`
- 类型化配置 `fields`
- `required` 和 `secret_fields`
- `capabilities`
- 基于测试证据的 `maturity`：`production`、`beta`、`experimental` 或 `dev-only`
- `readiness`：机器可读 gate，覆盖 registry/config schema、schema/preflight、checkpoint 或 replay 吸收能力、e2e 证据。它用于让 UI/向导解释生产缺口，不替代 maturity。

WASM 插件使用 `internal/etl/plugin/pluginsystem` 中的 Plugin ABI v1 元数据：

- ABI 字符串：`openetl.plugin.abi/v1`
- 最低运行时契约：`openetl-runtime/v1`
- 必要 entrypoint：source 插件导出 `read`，sink 插件导出 `write`，transform 插件导出 `transform`
- Transform 输出约定：空输出、`null` 或 `false` 表示丢弃输入；JSON record 对象或普通 data 对象输出一条记录；record/data 对象数组通过 batch transform 路径输出多条记录
- 配置字段类型：`string`、`int`、`bool`、`float`、`string_array`、`map`

## 幂等性警告

Spec 校验（`POST /api/v2/specs/validate`）会对潜在危险的组合返回警告：

- CDC 源 + file/S3 sink：无去重，重启时有重复风险
- `mysql_batch` + `mysql` sink（`batch_mode: insert`）：重新运行时会触发重复 key 错误
- CDC 源 + 追加型 sink：UPDATE/DELETE 被写入为行而非变更操作
