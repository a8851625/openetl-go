# Sync Canal ETL — 快速入门 / Quick Start

> 轻量级声明式 ETL/CDC 数据同步平台。YAML 定义管道，支持 MySQL CDC、Kafka、ClickHouse、PostgreSQL、Doris、Elasticsearch、S3 等 20+ 连接器。

---

## 1. 环境准备 (5 分钟)

### Docker Compose 一键启动

```bash
# 克隆项目
git clone <repo-url> openetl-go
cd openetl-go

# 启动全部依赖 (MySQL, ClickHouse, MinIO, Redpanda) + ETL 服务
podman-compose -f docker-compose.dev.yml up -d

# 验证服务
curl http://localhost:8000/api/v2/health
# → {"status":"ok","storage":"ok",...}
```

### 配置 API Token (生产必选)

```bash
# 生成随机 token
export ETL_API_TOKEN=$(openssl rand -hex 16)

# (可选) 生成 spec 加密密钥
export ETL_SPEC_ENCRYPTION_KEY=$(openssl rand -base64 32)

# 重启服务使配置生效
podman-compose -f docker-compose.dev.yml restart openetl-go
```

---

## 2. 创建第一个管道 (3 分钟)

### 方式一：Web UI 设计器

访问 `http://localhost:8000/#/designer`，可视化拖拽构建管道。

### 方式二：YAML 声明式

创建文件 `pipes/my-first-pipeline.yaml`：

```yaml
name: mysql-to-clickhouse

source:
  type: mysql_cdc
  config:
    host: etl-mysql-source
    port: 3306
    user: sync_user
    password: sync_password_123
    database: dzh3136_go
    tables: ["orders"]

transforms:
  - type: add_field
    config:
      field: _synced_at
      value: "now()"

sink:
  type: clickhouse
  config:
    host: etl-clickhouse
    port: 9000
    user: default
    password: dzh123456
    database: dzh3136_go
    auto_create: true
    schema_drift: add_columns

batch_size: 1000
flush_interval_ms: 1000
checkpoint_interval_sec: 30
```

管道文件会被 **热加载**（无需重启），几秒后自动出现在 `http://localhost:8000/#/pipelines`。

### 方式三：API 创建

```bash
curl -X POST http://localhost:8000/api/v2/pipelines \
  -H "Content-Type: application/json" \
  -H "X-API-Token: $ETL_API_TOKEN" \
  -d '{"spec": {"name": "my-pipeline", "source": {"type": "file", "config": {"path": "/tmp/in.jsonl"}}, "sink": {"type": "file_sink", "config": {"path": "/tmp/out.jsonl"}}}}'
```

---

## 3. 管道管理

```bash
# 列出所有管道
curl -H "X-API-Token: $TOKEN" http://localhost:8000/api/v2/pipelines

# 启动/停止/暂停/恢复
curl -X POST -H "X-API-Token: $TOKEN" http://localhost:8000/api/v2/pipelines/my-pipeline/start
curl -X POST -H "X-API-Token: $TOKEN" http://localhost:8000/api/v2/pipelines/my-pipeline/stop
curl -X POST -H "X-API-Token: $TOKEN" http://localhost:8000/api/v2/pipelines/my-pipeline/pause
curl -X POST -H "X-API-Token: $TOKEN" http://localhost:8000/api/v2/pipelines/my-pipeline/resume

# 查看指标
curl -H "X-API-Token: $TOKEN" http://localhost:8000/api/v2/metrics

# Prometheus 格式
curl http://localhost:8000/metrics
```

---

## 4. 连接器一览

| 类型 | 连接器 | 说明 |
|------|--------|------|
| **Source** | `mysql_cdc` | MySQL binlog 增量 (支持 GTID) |
| | `mysql_snapshot_cdc` | MySQL 全量+增量 |
| | `mysql_batch` | MySQL 批量读取 |
| | `postgres_cdc` | PostgreSQL 逻辑复制 |
| | `kafka` | Kafka 消费者 |
| | `file` | 文件读取 (JSONL/CSV) |
| | `http` | HTTP API 轮询 |
| **Sink** | `clickhouse` | ClickHouse (自动建表/Schema漂移) |
| | `mysql` | MySQL (INSERT/UPSERT/DELETE) |
| | `postgres` | PostgreSQL (INSERT/UPSERT) |
| | `kafka` | Kafka 生产者 (幂等) |
| | `elasticsearch` | ES 批量索引 |
| | `doris` | Doris (Stream Load + MySQL协议) |
| | `s3` | S3/MinIO (Parquet/JSON) |
| | `file_sink` | 本地文件 |
| **Transform** | `filter`, `rename`, `add_field`, `drop_field`, `type_convert` | 基础转换 |
| | `lua` | Lua 脚本 (内联) |
| | `validate` | 数据校验 (8种规则) |
| | `join` | 流流 JOIN |
| | `window` | 窗口聚合 |
| | `router`, `fanout` | 路由分发 |
| | `enricher`, `lookup` | 数据增强 |

---

## 5. 关键配置说明

| 配置 | 默认值 | 说明 |
|------|--------|------|
| `batch_size` | 1000 | 每批最大记录数 |
| `flush_interval_ms` | 1000 | 多久 flush 一次 |
| `checkpoint_interval_sec` | 30 | 检查点保存频率 |
| `backpressure_buffer` | 100 | Source↔Sink 缓冲区 |
| `parallelism.count` | 1 | 并行实例数 |
| `parallelism.shard_strategy` | round_robin | 分片策略 |

---

## 6. 生产部署检查清单

- [ ] 设置 `ETL_API_TOKEN` 环境变量
- [ ] 设置 `ETL_SPEC_ENCRYPTION_KEY` 加密 spec
- [ ] 配置 TLS (`ETL_TLS_CERT`, `ETL_TLS_KEY`)
- [ ] 配置告警渠道 (`ALERT_DINGTALK_WEBHOOK` / `ALERT_FEISHU_WEBHOOK` / `ALERT_SLACK_WEBHOOK`)
- [ ] 设置 DLQ 过期 (`ETL_DLQ_TTL=168h`)
- [ ] 验证所有 CDC 管道使用幂等 sink (UPSERT 模式)
- [ ] 数据库用户授予复制权限 (`REPLICATION SLAVE`, `REPLICATION CLIENT`)
- [ ] MySQL binlog 配置 `ROW` 格式 + `FULL` row image

---

## 7. API 文档

- **Swagger UI**: `http://localhost:8000/api/v2/docs`
- **OpenAPI Spec**: `http://localhost:8000/api/v2/openapi.yaml`
- **完整 API 文档**: `docs/etl-api.md`
- **配置 Schema**: `docs/etl-config-schema.md`
- **幂等性说明**: `docs/etl-idempotency.md`

---

## 8. 常见问题

### Q: 管道创建失败提示 "unsafe pipeline"?
CDC 源 + 非幂等 sink (file_sink/s3) 会被拦截。请使用 MySQL/ClickHouse/Doris 的 UPSERT 模式。

### Q: 如何从特定时间点回填数据?
```yaml
source:
  type: mysql_cdc
  config:
    start_from: "2026-06-01T00:00:00Z"  # RFC3339 时间戳
    # 或: start_from: "binlog:mysql-bin.000003:12345"
    # 或: start_from: "gtid:3E11FA47-...:1-100"
```

### Q: 如何暂停管道不丢数据?
使用 `pause` (而非 `stop`)。Pause 暂停源读取但保留 checkpoint，`resume` 从同一位置继续。

### Q: DLQ 中的记录如何查看内容?
DLQ 页面点击每条的 "▼ Data" 展开查看完整 JSON 数据，支持单条 replay。

---

## 9. 示例管道

`pipes/` 目录包含 9 个完整示例：
- `file-to-file.yaml` — 最简 file→file
- `mysql-batch-to-mysql.yaml` — MySQL 批量同步
- `kafka-to-clickhouse.yaml` — Kafka 消费到 ClickHouse
- `order-analytics.yaml` — 窗口聚合 + JOIN 示例

---

## 10. 获取帮助

- **GitHub Issues**: 报告 Bug / 功能需求
- **API Docs**: `/api/v2/docs` (Swagger UI)
- **示例管道**: `pipes/` 目录
