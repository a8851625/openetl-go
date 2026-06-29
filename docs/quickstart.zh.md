# ETL/CDC 快速入门

> 轻量、自托管、开源的 CDC/ETL 数据同步、清洗、汇聚运行时。用 YAML、API 或 Web UI 定义 `Source -> Transform -> Sink` 管道，支持 MySQL CDC、Kafka、ClickHouse、PostgreSQL、Doris、Elasticsearch、S3 等连接器。

OpenETL-Go 适合常见同步、清洗、补维、去重和 tumbling window 汇聚；不定位为 Flink/Spark 级复杂流计算、Airflow 级通用调度器或 Airbyte 级 SaaS ELT 连接器目录。完整边界见[产品定位](./positioning.zh.md)。

---

## 1. 环境准备（5 分钟）

### 容器 Compose 一键启动

```bash
# 克隆项目
git clone <repo-url> openetl-go
cd openetl-go

# 启动全部依赖（MySQL、ClickHouse、MinIO、Redpanda）+ ETL 服务
CONTAINER_CLI="${CONTAINER_CLI:-$(command -v podman || command -v docker)}"
"$CONTAINER_CLI" compose -f docker-compose.quickstart.yml up -d

# 验证服务
curl http://localhost:8000/api/v2/health
# → {"status":"ok","storage":"ok",...}
```

### 配置 API Token（生产必选）

```bash
# 生成随机 token
export ETL_API_TOKEN=$(openssl rand -hex 16)

# （可选）生成 spec 加密密钥
export ETL_SPEC_ENCRYPTION_KEY=$(openssl rand -base64 32)

# 重启服务使配置生效
"$CONTAINER_CLI" compose -f docker-compose.quickstart.yml restart etl
```

---

## 2. 创建第一个管道（3 分钟）

### 方式一：Web UI 向导

访问 `http://localhost:8000`，进入 **Pipelines**，点击 **Create from wizard**。

向导会生成普通 pipeline spec，覆盖数据库同步、Kafka 明细/聚合、Debezium CDC、Kafka 报文解析、文件/HTTP 落地等常见任务。选择模板后，先查看由 descriptor/schema 驱动的 source/sink 字段摘要，执行 **Transform dry-run**，再执行 **Validate + preflight**。如果 preflight 指出字段或连接器问题，修正表单或 YAML 面板后重新校验。只有 preflight 通过后再点击 **Create and start**。

生成的 YAML 会一直显示且可编辑；使用 **Sync YAML to form** 可把 YAML 修改同步回表单，再创建管道。

### 方式一补充：Web UI 设计器

访问 `http://localhost:8000/#/designer`，可视化拖拽构建高级 DAG。
在 Settings 配置 OpenAI 兼容 LLM 后，设计器的 AI 面板可以根据提示词生成普通
YAML 草稿。应用到画布前需要先审阅 diff、缺失字段、风险标记和确认项；最终仍
必须通过 **Validate + preflight** 后才能创建。

### 方式二：YAML 声明式

创建文件 `pipes/my-first-pipeline.yaml`：

```yaml
name: mysql-to-clickhouse

source:
  type: mysql_cdc
  config:
    host: quickstart-mysql
    port: 3306
    user: root
    password: root123
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
    host: quickstart-clickhouse
    port: 8123
    protocol: http
    user: default
    password: clickhouse
    database: dzh3136_go
    auto_create: true
    schema_drift: add_columns

batch_size: 1000
flush_interval_ms: 1000
checkpoint_interval_sec: 30
```

管道文件会被**热加载**（无需重启），几秒后自动出现在 `http://localhost:8000/#/pipelines`。

### 方式三：API 创建

```bash
curl -X POST http://localhost:8000/api/v2/pipelines \
  -H "Content-Type: application/json" \
  -H "X-API-Token: $ETL_API_TOKEN" \
  -d '{"spec": {"name": "my-pipeline", "source": {"type": "file", "config": {"path": "/tmp/in.jsonl"}}, "sink": {"type": "file_sink", "config": {"output_dir": "/tmp"}}}}'
```

---

## 3. 管道管理

```bash
# 列出所有管道
curl -H "X-API-Token: $TOKEN" http://localhost:8000/api/v2/pipelines

# 启动 / 停止 / 暂停 / 恢复
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
| **Source** | `mysql_cdc` | MySQL binlog 增量（支持 GTID） |
| | `mysql_snapshot_cdc` | MySQL 全量+增量无缝衔接 |
| | `mysql_batch` | MySQL 批量读取（支持自定义 SQL） |
| | `postgres_cdc` | PostgreSQL 逻辑复制（pgoutput） |
| | `kafka` | Kafka 消费者组 |
| | `redis` | Redis SCAN 全量读取 |
| | `file` | 文件读取（JSONL/CSV） |
| | `http` | HTTP API 分页轮询 |
| **Sink** | `clickhouse` | ClickHouse（自动建表 / Schema 漂移 / DDL 翻译） |
| | `mysql` | MySQL（INSERT / UPSERT / DELETE，自动建表） |
| | `postgres` | PostgreSQL（INSERT / UPSERT，自动建表） |
| | `kafka` | Kafka 生产者（支持幂等） |
| | `elasticsearch` | ES 批量索引（多 host 轮询，429 重试） |
| | `doris` | Doris（Stream Load + MySQL 协议 DELETE） |
| | `redis` | Redis（HASH/STRING/LIST 三种模式） |
| | `s3` | S3/MinIO（Parquet/JSON，分片上传） |
| | `jdbc` | 任意 JDBC 数据库 |
| | `file_sink` | 本地文件输出 |
| **Transform** | `filter`、`project`、`select_fields`、`flat_map`、`udtf`、`rename`、`add_field`、`drop_field`、`type_convert` | 基础转换 |
| | `deduplicate`、`validate` | 数据清洗 |
| | `lua` | Lua 脚本（内联，gopher-lua 纯 Go） |
| | `normalize_envelope`、`debezium_cdc`、`cdc_policy`、`ddl_guard`、`lookup`、`window` | Kafka envelope/CDC 策略 / 维表 JOIN / tumbling 窗口聚合 |
| | `join` | 流流 interval JOIN，可选 SQLite 状态恢复；生产级 crash/rebalance 认证仍在 roadmap 中 |
| | `router`、`fanout`、`tap` | 条件路由 / 扇出 / 旁路 |
| | `enricher`、`lookup` | 数据增强 / 维表查找 |
| | `rate_limiter` | 流量控制 |

---

## 5. 关键配置说明

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `batch_size` | 1000 | 每批最大记录数 |
| `flush_interval_ms` | 1000 | 批次 flush 间隔（毫秒） |
| `checkpoint_interval_sec` | 30 | 检查点保存间隔（秒） |
| `backpressure_buffer` | 100 | Source↔Sink 缓冲区大小 |
| `parallelism.count` | 1 | 并行分片实例数 |
| `parallelism.shard_strategy` | round_robin | 分片策略 |
| `retry.max_attempts` | 3 | 最大重试次数 |
| `retry.initial_interval_ms` | 1000 | 初始重试间隔 |
| `retry.max_interval_ms` | 30000 | 最大重试间隔 |
| `dlq.enable` | true | 是否启用死信队列 |

---

## 6. 生产部署检查清单

- [ ] 设置 `ETL_API_TOKEN` 环境变量
- [ ] 设置 `ETL_SPEC_ENCRYPTION_KEY` 加密 spec
- [ ] 配置 TLS（`ETL_TLS_CERT`、`ETL_TLS_KEY`）
- [ ] 配置告警渠道（`ALERT_DINGTALK_WEBHOOK` / `ALERT_FEISHU_WEBHOOK` / `ALERT_SLACK_WEBHOOK`）
- [ ] 设置 DLQ 过期（`ETL_DLQ_TTL=168h`）
- [ ] 验证所有 CDC 管道使用幂等 sink（UPSERT 模式）
- [ ] 数据库用户授予复制权限（`REPLICATION SLAVE`、`REPLICATION CLIENT`）
- [ ] MySQL binlog 配置 `ROW` 格式 + `FULL` row image
- [ ] PostgreSQL 配置 `wal_level=logical`
- [ ] 设置资源限制（Docker 或 systemd 的 CPU/内存限制）

---

## 7. 常见问题

### Q: 管道创建失败提示 "unsafe pipeline"？
CDC 源 + 非幂等 sink（file_sink/s3）会被拦截。请使用 MySQL/ClickHouse/Doris 的 UPSERT 模式，或在 spec 中显式设置 `allow_unsafe: true`。

### Q: 如何从特定时间点回填数据？
```yaml
source:
  type: mysql_cdc
  config:
    start_from: "2026-06-01T00:00:00Z"  # RFC3339 时间戳
    # 或指定 binlog 位置:
    # start_from: "binlog:mysql-bin.000003:12345"
    # 或指定 GTID:
    # start_from: "gtid:3E11FA47-...:1-100"
```

### Q: 如何暂停管道不丢数据？
使用 `pause`（而非 `stop`）。Pause 暂停源读取但保留 checkpoint，`resume` 从同一位置继续。

### Q: DLQ 中的记录如何查看和重放？
DLQ 页面点击每条的 "▼ Data" 展开查看完整 JSON 数据。支持按条件过滤重放：
```bash
# 按错误信息过滤重放
curl -X POST -H "X-API-Token: $TOKEN" \
  'http://localhost:8000/api/v2/dlq/my-pipeline/replay?error_contains=Duplicate'

# 按时间范围重放
curl -X POST -H "X-API-Token: $TOKEN" \
  'http://localhost:8000/api/v2/dlq/my-pipeline/replay?from=2026-06-01T00:00:00Z'

# 按稳定 DLQ ID 单条重放或删除
curl -X POST -H "X-API-Token: $TOKEN" \
  'http://localhost:8000/api/v2/dlq/my-pipeline/123/replay'
curl -X DELETE -H "X-API-Token: $TOKEN" \
  'http://localhost:8000/api/v2/dlq/my-pipeline/123'
```

### Q: 管道启动后无数据？
检查 preflight 结果：
```bash
curl -X POST -H "X-API-Token: $TOKEN" \
  -H "Content-Type: application/json" \
  -d @pipes/my-pipeline.yaml \
  http://localhost:8000/api/v2/specs/validate
```
常见原因：binlog 格式错误、复制权限不足、源表不存在、网络不通。

### Q: 如何监控管道运行状态？
- **Web UI**：Dashboard 页面查看实时指标
- **Prometheus**：`curl http://localhost:8000/metrics`
- **API**：`curl -H "X-API-Token: $TOKEN" http://localhost:8000/api/v2/metrics`
- **日志**：设置 `LOGGER_FORMAT=json` 输出结构化 JSON 日志

### Q: ClickHouse 自动建表的字段类型不对？
自动建表通过采样数据值推断类型。如需精确类型映射，请先在 ClickHouse 中手动建表，然后设置 `auto_create: false`。

### Q: 分布式部署如何配置？
```yaml
# Master 节点
etl:
  role: master
  storage:
    type: mysql
    # ... MySQL 连接配置

# Worker 节点
etl:
  role: worker
  storage:
    type: mysql
    # ... 相同的 MySQL 连接配置
```
Master 负责任务调度，Worker 执行分片任务。Worker 崩溃后分片自动重分配到其他 Worker。

---

## 8. 示例管道

`pipes/` 目录包含完整示例：
- `file-to-file.yaml` — 最简 file→file
- `mysql-batch-to-mysql.yaml` — MySQL 批量同步
- `mysql-cdc-to-clickhouse.yaml` — MySQL CDC→ClickHouse（自动建表）
- `order-realtime-analytics.yaml` — 窗口聚合 + JOIN 实时分析
- `ultimate-complex-demo.yaml` — DAG 多源多汇复杂场景

---

## 9. 获取帮助

- **GitHub Issues**：报告 Bug / 功能需求
- **API Docs**：`/api/v2/docs`（Swagger UI）
- **示例管道**：`pipes/` 目录
- **配置参考**：[`docs/etl-config-schema.zh.md`](./etl-config-schema.zh.md)
- **API 参考**：[`docs/etl-api.zh.md`](./etl-api.zh.md)
