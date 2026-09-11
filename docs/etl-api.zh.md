# ETL API v2 参考

## 认证
- 设置 `ETL_API_TOKEN` 来保护 ETL API 路由。
- 客户端通过 `X-API-Token: <token>` 或 `Authorization: Bearer <token>` 传递令牌。
- `GET /api/v2/health` 不需要认证，用于存活检查。

## Secret 字段

Connection 响应和线性/DAG pipeline spec 统一根据 connector descriptor 掩码 secret
字段，包括 JDBC/dbt/enricher/lookup DSN。Connection 返回 `******`；spec 保留旧的
首尾字符掩码格式（较长值）。更新已有 secret 时回传该
占位符会保留存储中的真实值。落盘加密使用同一 descriptor；显式非 secret 字段保持
可读。未声明字段采用旧字段名匹配兜底，并产生 WARN。

`secret_encryption` health 反映存量 connection/settings 明文检测结果。离线
`--check-secrets` 和可幂等重跑的 `--remediate-secrets` 用法见
[运维手册](./ops-runbook.md#11b-existing-plaintext-secrets)。

## 恢复失败的 Pipeline

启动时无法重建 runner 的持久化 pipeline 仍会出现在
`GET /api/v2/pipelines` 和 `GET /api/v2/pipelines/{id}` 中，状态为
`restore_failed`。`restore_error` 包含失败 `stage`、稳定 `code`、
`message`、可操作的 `remediation`、`previous_status` 与 `failed_at`。
对该 pipeline 执行 start/resume 会返回 HTTP `409` 及同一诊断，不再误报 `404`。

`GET /api/v2/health` 会返回 pipeline 级 `restore_failed`、总体
`degraded`、`restore_failed_count` 和 JSON 格式的 `pipeline_issues`。
修复 spec 或引用的 connection 后重启即可恢复；restore 状态更新不会重置
checkpoint。strict/non-strict 启动配置与处置流程见
[runtime-modes.md](./runtime-modes.md)。

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

SQL-backed DLQ 响应会包含稳定 `id`，用于按记录删除和重放。DAG DLQ 响应在失败记录带有节点上下文时还会包含 `dag_node`。新记录还会返回 `identity_context`：原始 source payload（合法 UTF-8 使用 `source_bytes`，否则以 `source_bytes_base64` 保存 base64）、原始主键声明、身份失败原因/缺列、冻结的 format-contract ID 与 before-image 状态、源/目标坐标、replay provenance/state 及认可 checkpoint。原始 payload 属于生产数据；该 API 必须启用认证并沿用数据访问控制。

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

重放使用与列表相同的查询参数。重放的记录会重新经过 Transform 链并写入配置的 Sink。metadata-PK pipeline 会先依据记录本身或冻结的 format contract 证明完整身份，并在 Transform 后再次校验；绝不会从当前 source 配置猜测历史键语义。不能证明的记录继续保持 `repair_required` 或 `quarantined`，并返回 HTTP `409` 以及结构化 `id`、`replay_state`、`reason`。

Sink 确认 replay 后，SQL-backed DLQ 行会先持久化为 `sink_acked`，再删除。若在该 replay checkpoint 前崩溃或落盘失败，下次可能再次写入（即公开的 at-least-once 边界）；若 checkpoint 已成功而进程崩溃或删除失败，下次只清理 DLQ，不再写 Sink。此 replay checkpoint 不推进 source checkpoint。

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

### 修复单条 legacy 身份声明

`PUT /api/v2/dlq/{pipeline}/{id}/identity`

这是历史 metadata-PK DLQ 行的单条显式兼容门禁，仅适用于线性 sink 同时配置了 `pk_columns_from_metadata: true` 和静态 `pk_columns` 安全集合的情况。请求中的 `primary_key_columns` 必须与历史 Record 已保存的原始声明及目标静态集合都精确一致；服务端还会验证全部值，拒绝部分键、凭当前配置补造声明，以及所有 legacy 主键变更 UPDATE。修复成功后，完整 Key 与 `legacy_verified` provenance 会先持久化，之后才允许 replay；不提供批量或自动修复。

```sh
curl -X PUT -H "X-API-Token: $ETL_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"primary_key_columns":["tenant_id","id"]}' \
  'http://127.0.0.1:8001/api/v2/dlq/orders/123/identity'
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

Pipeline 响应会暴露三个生命周期字段：`desired_state` 是持久化的操作意图
（`running`、`stopped` 或 `paused`），`observed_state` 是最后一次持久化的运行态，
`generation` 是 checkpoint fencing token。start/resume 会先持久化
`desired_state=running` 再打开 source；stop/pause 会先持久化静止意图再改变 runner。
若 desired state 持久化失败，请求返回非 2xx，runner 保持原状态。

checkpoint `set` 与 `reset` 都是管理操作，只允许在 pipeline 已 stopped 或 paused
时执行。运行态调用返回 HTTP `409`、错误码 `pipeline_not_quiescent` 和处置建议。
成功操作会在同一事务中提升 `generation` 并修改 checkpoint；旧 generation 的在途
写入会被拒绝、记录日志并计入 `checkpoint_fenced_total`，绝不会借用新 token 重试。
交付语义仍是 at-least-once，sink 必须能够吸收 replay 重复。

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

### 重置 Source Checkpoint

`POST /api/v2/pipelines/{pipeline}/checkpoint/reset`

响应包含 `generation` 和 `reset_semantics`（`effect`、`external_boundary`、
`operator_action`）。reset 只修改 OpenETL 的持久化 checkpoint；下次启动位置取决于
source：

| Source | reset 后的下次启动边界 |
| --- | --- |
| `kafka` | 删除 OpenETL offset，但不会改变 broker consumer group offset 或 topic retention；需要更早重放时必须单独重置 consumer group。 |
| `mysql_cdc` | 重新发现当前 MySQL master position；这不是“从最旧 binlog 重放”。受控重放应使用 `checkpoint/set` 指定仍被保留的 file/position。 |
| `mysql_snapshot_cdc` | 删除 snapshot/CDC handoff 状态，重新执行完整 snapshot 后再进入 CDC；所有源行都可能再次投递。 |
| `postgres_cdc` | 删除 OpenETL 保存的 LSN，但 replication slot 与 WAL retention 仍是外部边界；reset 不会移动或重建 slot。 |

调用任一 checkpoint 管理端点前，先 stop 或 pause 并等待 pipeline 静止。系统不会自动
重置外部 broker group、replication slot 或数据库日志位置。

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
