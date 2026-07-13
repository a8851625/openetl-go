# ETL API v2 参考

## 认证
- 设置 `ETL_API_TOKEN` 来保护 ETL API 路由。
- 客户端通过 `X-API-Token: <token>` 或 `Authorization: Bearer <token>` 传递令牌。
- `GET /api/v2/health` 不需要认证，用于存活检查。

## DLQ API

死信记录包含 `error_class` 字段，表示运行时对失败原因的分类。当前分类包括 `transient`（瞬时错误）、`data`（数据错误）、`schema`（模式错误）、`auth`（认证错误）、`config`（配置错误）、`programming`（编程错误）和 `unknown`（未知错误）。重试策略使用相同的分类：transient 和 unknown 错误会重试，而 data/schema/auth/config/programming 错误会快速失败进入 DLQ 或直接使操作失败。

### 列出 DLQ 记录
`GET /api/v2/dlq/{pipeline}`

查询参数：
- `limit`：最大返回记录数。默认 `100`。使用 `0` 表示无限制。
- `timestamp`：精确匹配 RFC3339Nano 格式的 DLQ 时间戳。
- `from`：包含此 RFC3339Nano 时间戳及之后的记录。
- `until`：包含此 RFC3339Nano 时间戳及之前的记录。
- `contains`：对序列化的失败记录负载进行子串匹配。
- `error_contains`：对 DLQ 错误字符串进行子串匹配。

SQL-backed DLQ 响应会包含稳定 `id`，用于按记录删除和重放。DAG DLQ 响应在失败记录带有节点上下文时还会包含 `dag_node`。

示例：
```sh
curl -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/dlq/orders?limit=20'

curl -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/dlq/orders?contains=customer_id'

curl -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/dlq/orders?error_contains=Duplicate'
```

### 重放 DLQ 记录
`POST /api/v2/dlq/{pipeline}/replay`
`POST /api/v2/dlq/{pipeline}/{id}/replay`

重放使用与列表相同的查询参数。重放的记录会重新经过 Transform 链并写入配置的 Sink。成功重放的记录会优先按稳定 DLQ ID 从 SQL-backed DLQ 存储中删除。

按 ID 端点用于确定性地重放单条记录，响应会包含 `{"replayed":1}` 这类结果反馈。线性 pipeline 支持 DLQ 重放。DAG pipeline 对包含 `dag_node` 的 DLQ 记录支持节点级重放：sink 节点失败会直接写回该 sink，transform 节点失败会从该 transform 重新执行并继续向下游路由。缺少 `dag_node` 的旧 DAG DLQ 记录会返回 HTTP `400` 和 `{"error":"...dag_node...","replayed":0}`，且不会删除 DLQ 记录。

示例：
```sh
curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/dlq/orders/replay?contains=9901'

curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/dlq/orders/123/replay'

curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/dlq/orders/replay?from=2026-06-06T00:00:00Z&until=2026-06-07T00:00:00Z'
```

### 删除 DLQ 记录
`DELETE /api/v2/dlq/{pipeline}`

删除使用与列表相同的查询参数。如果不提供选择性过滤条件，则删除该管道的整个 DLQ 文件。

示例：
```sh
curl -X DELETE -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/dlq/orders?error_contains=unknown%20column'

curl -X DELETE -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/dlq/orders'
```

## Checkpoint API

### 设置 Kafka 重放 Offset
`POST /api/v2/pipelines/{pipeline}/checkpoint/set`

Kafka source 推荐使用结构化 checkpoint 请求，而不是手写内部 checkpoint JSON。`offset` 和 `replay_from_offsets` 表示“下次启动从这个 offset 开始读取”；OpenETL-Go 内部会保存 `offset-1`，因为 Kafka 在 sink 写入成功后提交的是下一条 offset。

示例：
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

如果要直接设置已提交 offset，可使用 `{"mode":"last_committed","offsets":{"0":41}}`。旧的原始 checkpoint 形态 `{"position":{...}}` 仍然兼容。

## 插件元数据

发现已注册的插件及其基本能力。

```sh
curl -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/plugins'
```

响应包含旧版列表以及 `metadata`：

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

## 插件试运行

对已安装的 transform 插件运行一条样例记录。多输出插件会在 `records`
中返回全部输出，并在 `output_count` 中返回数量；`record` 和 `output`
为兼容旧客户端保留第一条输出。

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

## AI 上下文与生成

AI 辅助 DAG 生成使用与 UI/YAML 相同的 connector descriptor、插件 schema、
组件文档和 validate/preflight 路径。它只生成普通 pipeline/DAG spec 草稿；
不会启动管道，也不能绕过人工确认。

```sh
curl -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/ai/context'
```

```sh
curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"从 Kafka 读取 Debezium orders 并 upsert 到 MySQL ODS。"}' \
  'http://127.0.0.1:8001/api/v2/ai/generate'
```

生成响应包含 `yaml`、`context_pack_version`、`validation` 和 `review`。
应用并启动前需要处理或明确接受 `review.missing_fields`、
`review.risk_flags` 和 `review.requires_confirmation`。

## Spec 校验

校验 pipeline spec 而不创建运行时管道。

```sh
curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"spec":{"name":"example","source":{"type":"file","config":{}},"sink":{"type":"file_sink","config":{}}}}' \
  'http://127.0.0.1:8001/api/v2/specs/validate'
```

响应：

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

当 preflight 有足够上下文时，响应还会包含
`preflight.recommendations`：需要操作员审阅的配置补丁，例如
`sink.config.batch_mode=upsert`、`sink.config.pk_columns=["id"]`、
`sink.config.schema_drift=add_columns`、`transforms=[{type:type_convert,...}]`、
`sink.config.prefix=orders/`、`sink.config.key_column=id`、
`sink.config.auto_create_topic=true`、`batch_size=500` 或 `dlq.enable=true`。
Web 向导可以在创建前把这些补丁应用到草稿 spec。

## 连接测试

构建并可选择性地打开一个 Source、Sink 或 Transform 配置，而不创建管道。

```sh
curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"kind":"source","type":"file","config":{"path":"/app/data/input/customers.jsonl","format":"json"},"open":true}' \
  'http://127.0.0.1:8001/api/v2/connections/test'
```

响应：

```json
{
  "ok": true,
  "kind": "source",
  "type": "file",
  "opened": true
}
```

## 保存连接上下文

读取保存连接，同时返回 descriptor、健康状态、推荐运行参数和尽力而为的 source/sink introspection。

```sh
curl -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/connections/file-source/context'
```

响应：

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

当前内置 adapter 覆盖 file/HTTP/demo 采样、MySQL/PostgreSQL 表和字段元数据、Kafka topic/partition 元数据，以及 MySQL、PostgreSQL、ClickHouse、Doris、Kafka、Elasticsearch/OpenSearch、File、S3/local-fallback sink 目标元数据。File/S3 context 会返回 `introspection.targets`，包含解析后的目录或 bucket、prefix、format、可写状态或 bucket 是否存在。Introspection 是控制面提示；真正的启动拦截仍由 `spec validate` 和 preflight 执行。

## Transform 试运行

在单条样本记录上执行 Transform 链，不启动管道。

```sh
curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"transforms":[{"type":"identity","config":{}}],"record":{"operation":"INSERT","data":{"id":1},"before":{},"metadata":{"source":"ui","table":"sample"}}}' \
  'http://127.0.0.1:8001/api/v2/transforms/dry-run'
```

响应：

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

对于 `flat_map` / `udtf` 这类 `BatchTransform`，`records` 包含全部输出记录；`record` 为兼容旧调用保留第一条输出。记录级解析错误会以 `partial_error: true` 和 `errors` 返回，不会隐藏已经成功生成的输出。

## Spec 重载

从配置的 `etl.specsDir` 加载新的 pipeline spec，不替换已加载的管道。

```sh
curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/specs/reload'
```

响应：

```json
{
  "loaded": ["new-pipeline"],
  "skipped": {"existing.yaml": "pipeline existing already loaded"},
  "errors": {}
}
```

## 审计事件

返回持久化在当前 SQL storage backend 中的最近变更事件。可通过 `ETL_AUDIT_ENABLED=false`、`etl.audit.enabled: false` 或 `--audit-enabled=false` 禁用 audit 写入。

```sh
curl -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/audit?limit=50'
```

响应：

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
