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

重放使用与列表相同的查询参数。重放的记录会重新经过 Transform 链并写入配置的 Sink。成功重放的记录会从 DLQ 文件中删除。

示例：
```sh
curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  'http://127.0.0.1:8001/api/v2/dlq/orders/replay?contains=9901'

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
        "required": ["host", "database", "table", "server_id"],
        "capabilities": ["cdc", "checkpoint"],
        "maturity": "stable"
      }
    }
  }
}
```

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
  "record": {
    "operation": "INSERT",
    "data": {"id": 1}
  }
}
```

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

返回写入 `ETL_AUDIT_LOG` 或默认 `data/audit.log` 的最近变更事件。

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
