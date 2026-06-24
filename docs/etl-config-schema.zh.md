# ETL YAML 配置参考

Pipeline spec 是 `pipes/` 或配置的 `etl.specsDir` 下的 YAML 文件。环境变量在解析前通过 `${VAR}` 或 `${VAR:-default}` 展开。

## 顶层 Spec

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
| `source.type` | 是 | 已注册的 Source 插件名称。 |
| `source.config` | 否 | Source 专属设置。默认为 `{}`。 |
| `transforms` | 否 | 有序 Transform 链。无 Transform 可省略。 |
| `sink.type` | 是 | 已注册的 Sink 插件名称。 |
| `sink.config` | 否 | Sink 专属设置。默认为 `{}`。 |
| `batch_size` | 否 | Sink 每批最大记录数。默认 `1000`。 |
| `flush_interval_ms` | 否 | 批次 flush 间隔（毫秒）。默认 `1000`。 |
| `checkpoint_interval_sec` | 否 | 检查点保存间隔（秒）。默认 `30`。 |
| `backpressure_buffer` | 否 | 内部记录通道大小。默认 `100`。 |
| `retry` | 否 | 重试策略。默认值见上方。 |
| `dlq.enable` | 否 | 启用死信队列（DLQ）。默认 `true`。 |

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
    port: 8030
    mysql_port: 9030
    user: root
    password: ${DORIS_PASSWORD}
    database: analytics
    table: customers
    auto_create: true
```

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `host` | 是 | | Doris FE 主机。 |
| `port` | 否 | `8030` | Stream Load HTTP 端口。 |
| `mysql_port` | 否 | `9030` | MySQL 协议端口（DELETE 用）。 |
| `user` | 是 | | Doris 用户。 |
| `password` | 否 | | Doris 密码（**密钥**）。 |
| `database` | 是 | | 目标数据库。 |
| `table` | 是 | | 目标表。 |
| `auto_create` | 否 | `false` | 自动建表。 |

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

## 运行时配置 API

- 校验 spec：`POST /api/v2/specs/validate` — 返回危险 Source/Sink 组合的幂等性警告
- 测试连接：`POST /api/v2/connections/test`
- Transform 试运行：`POST /api/v2/transforms/dry-run`
- 重载 spec：`POST /api/v2/specs/reload`
- 插件 schema：`GET /api/v2/plugins/schema` — 返回带 secret 标记的类型化字段 schema

## 幂等性警告

Spec 校验（`POST /api/v2/specs/validate`）会对潜在危险的组合返回警告：

- CDC 源 + file/S3 sink：无去重，重启时有重复风险
- `mysql_batch` + `mysql` sink（`batch_mode: insert`）：重新运行时会触发重复 key 错误
- CDC 源 + 追加型 sink：UPDATE/DELETE 被写入为行而非变更操作
