# OpenETL-Go — 轻量级 ETL/CDC 引擎（单二进制）

> **OpenETL-Go** 是一个单二进制、插件化的 ETL/CDC 引擎：`Source → Transform → Sink`。
> 数据源支持 MySQL binlog / PostgreSQL 逻辑复制 / Kafka / 文件 / HTTP / Redis，
> 数据汇支持 ClickHouse / MySQL / PostgreSQL / Doris / Elasticsearch / Kafka / Redis / S3 / JDBC。
> 单机模式使用 SQLite（零外部依赖）；扩展模式使用 MySQL/PostgreSQL 共享存储 + master-worker 分片。

## 能力一览

| 阶段 | 连接器 / 算子 |
|------|--------------|
| **Source** | `mysql_cdc`（binlog）、`mysql_snapshot_cdc`（快照+增量衔接）、`postgres_cdc`（逻辑复制）、`mysql_batch`、`kafka`、`redis`（SCAN）、`http`（分页+断点续传）、`file`（JSON/CSV） |
| **Transform** | `filter`（表达式引擎）、`deduplicate`、`validate`（8 种校验规则）、`rename`/`drop_field`/`add_field`、`type_convert`、`enricher`、`lookup`、`join`、`window`、`router`（条件路由）、`fanout`、`tap`、`rate_limiter`；脚本扩展：`lua`（默认，gopher-lua）、`javascript`/`typescript`（QuickJS，需 CGO）、WASM 插件（extism） |
| **Sink** | `clickhouse`（自动建表+DDL 翻译）、`mysql`/`postgres`（批量+幂等 upsert）、`doris`（Stream Load）、`kafka`（幂等生产者）、`elasticsearch`（bulk API，轮询）、`redis`（HASH/STRING/LIST）、`s3`/minio（分片上传，Parquet）、`jdbc`（任意 JDBC 数据库）、`file` |
| **可靠性** | at-least-once + 幂等 sink + DLQ 死信队列 + 三态断路器 + 指数退避重试 + checkpoint；**零静默数据丢失**（SPEC §6.1） |
| **执行模式** | 线性 pipeline / DAG 多源多汇 / ParallelRunner 分片 / master-worker 真分布式（A11-redo 已验证） |
| **运维** | REST API `/api/v2/*`、preflight 预检、Prometheus `/metrics`、JSON 结构化日志、SQLite/MySQL/PG 元存储、Web 管理界面 |

## 🚀 5 分钟快速开始

```bash
# 1. 启动依赖（MySQL 源 + ClickHouse 目标）
podman compose -f docker-compose.quickstart.yml up -d

# 2. 示例管道 MySQL CDC → ClickHouse（自动建表）在 pipes-quickstart/ 中
#    也可以直接使用：pipes/mysql-cdc-to-clickhouse.yaml

# 3. 打开管理界面：http://localhost:8000   （REST API：/api/v2/*）
```

- **示例 spec**：`pipes-quickstart/mysql-to-clickhouse.yaml`、`pipes/mysql-cdc-to-clickhouse.yaml`
- **完整入门**：[`docs/quickstart.zh.md`](./docs/quickstart.zh.md)

## 安装

### 从 Release 下载（推荐）

前往 [Releases](../../releases) 下载对应平台的压缩包（Linux/macOS/Windows × amd64/arm64），解压后：

```bash
tar -xzf openetl-go_*.tar.gz
./openetl-go                           # 默认读取 manifest/config/config.yaml
```

### Docker

```bash
docker run -d --name openetl-go -p 8000:8000 -p 8001:8001 \
  -v "$PWD/pipes:/app/pipes" \
  ghcr.io/a8851625/openetl-go:latest
```

### 从源码构建

```bash
go build -o openetl-go .                # 默认构建（纯 Go，含 Lua）
# 可选运行时：
go build -tags=extism -o openetl-go .   # 启用 WASM 插件（wazero，纯 Go）
go build -tags=nolua -o openetl-go .    # 剥离 Lua 运行时，进一步瘦身
CGO_ENABLED=1 go build -o openetl-go .  # 启用 JS/TS transform（QuickJS，需 CGO）
```

## 构建标签

| 标签 | 效果 | 默认 |
|------|------|------|
| *(无)* | 纯 Go 核心 + 全部 sink/source + Lua（gopher-lua）；无 WASM、无 QuickJS、无 CGO | ✅ |
| `-tags=extism` | + WASM 插件运行时（wazero，纯 Go） | — |
| `-tags=nolua` | 剥离 Lua 运行时（`type:lua` 返回清晰错误），更小的二进制 | — |
| `CGO_ENABLED=1` | + JavaScript/TypeScript transform（QuickJS，需 C 工具链） | — |

## 运行模式

- **单机（默认）**：SQLite 存储，零外部依赖，一个进程跑全部管道。
- **可扩展**：将 `etl.storage.type` 切换为 `mysql` 或 `postgresql`，使用 `etl.role=master` + 多个 `etl.role=worker` 实现真分布式分片（线性 spec 跨 worker 不重叠分发，worker 崩溃后分片自动重分配）。

最小配置见 [`manifest/config/config.yaml`](./manifest/config/config.yaml)；详细字段参考 [`docs/etl-config-schema.zh.md`](./docs/etl-config-schema.zh.md)。

## 架构

### Pipeline 模型

```
┌──────────┐     ┌──────────────────┐     ┌──────────┐
│  Source   │ ──► │  Transform Chain  │ ──► │   Sink   │
│ （读取）   │     │ （过滤/增强/…）    │     │ （写入）  │
└──────────┘     └──────────────────┘     └──────────┘
       │                                        │
       └──────────── checkpoint ◄───────────────┘
                         │
                    DLQ（失败记录）
```

### 执行模式

| 模式 | 适用场景 | 配置方式 |
|------|---------|---------|
| **线性 Pipeline** | 单源 → 转换 → 单汇 | `spec.source` + `spec.sink` |
| **DAG** | 多源、多汇、条件边 | `spec.dag.nodes` + `spec.dag.edges` |
| **ParallelRunner** | 单源 N 个并行分片独立写入 | `parallelism.count: N` |
| **Master-Worker** | 跨多进程/节点分布式 | `etl.role: master\|worker` |

### 存储后端

| 后端 | 适用场景 | 配置方式 |
|------|---------|---------|
| **SQLite** | 单机、零依赖部署 | `etl.storage.type: sqlite`（默认） |
| **MySQL** | 多节点共享状态 | `etl.storage.type: mysql` |
| **PostgreSQL** | 多节点共享状态 | `etl.storage.type: postgresql` |

### 可靠性栈

1. **At-least-once 投递**：checkpoint 仅在 sink 写入成功后推进
2. **幂等 Sink**：MySQL/PostgreSQL upsert、ClickHouse ReplacingMergeTree、ES 文档 `_id`
3. **DLQ 死信队列**：失败记录持久化存储，含错误分类；支持 list/replay/delete API
4. **断路器**：每个 sink 独立的三态断路器（closed→open→half-open），防止级联故障
5. **指数退避**：`retry.Do` 支持可配置的初始/最大重试间隔
6. **Panic 恢复**：readLoop/writeLoop 逐 goroutine 恢复；panic 路由到 DLQ

## 安全配置

### API 认证
```bash
# 启用 Token 认证（生产必选）
export ETL_API_TOKEN=$(openssl rand -hex 32)

# 客户端通过 Header 传递 Token
curl -H "X-API-Token: $ETL_API_TOKEN" http://localhost:8000/api/v2/pipelines
curl -H "Authorization: Bearer $ETL_API_TOKEN" http://localhost:8000/api/v2/pipelines
```

### Spec 加密
```bash
# 加密数据库中存储的 pipeline spec
export ETL_SPEC_ENCRYPTION_KEY=$(openssl rand -base64 32)
```

### TLS
```bash
# 启用 API 服务器 TLS
export ETL_TLS_CERT=/path/to/cert.pem
export ETL_TLS_KEY=/path/to/key.pem
```

### 告警
```bash
# 配置 DLQ 溢出/断路器跳闸的告警渠道
export ALERT_DINGTALK_WEBHOOK=https://oapi.dingtalk.com/robot/send?access_token=...
export ALERT_FEISHU_WEBHOOK=https://open.feishu.cn/open-apis/bot/v2/hook/...
export ALERT_SLACK_WEBHOOK=https://hooks.slack.com/services/...
```

## 文档

- [`docs/quickstart.zh.md`](./docs/quickstart.zh.md)（中文）| [`docs/quickstart.md`](./docs/quickstart.md) (EN) — 5 分钟入门
- [`docs/etl-api.zh.md`](./docs/etl-api.zh.md)（中文）| [`docs/etl-api.md`](./docs/etl-api.md) (EN) — REST API 参考
- [`docs/etl-config-schema.zh.md`](./docs/etl-config-schema.zh.md)（中文）| [`docs/etl-config-schema.md`](./docs/etl-config-schema.md) (EN) — 配置字段参考
- [`docs/etl-idempotency.zh.md`](./docs/etl-idempotency.zh.md)（中文）| [`docs/etl-idempotency.md`](./docs/etl-idempotency.md) (EN) — 幂等与 exactly-once 语义
- [`docs/parallelism-and-batching.zh.md`](./docs/parallelism-and-batching.zh.md)（中文）| [`docs/parallelism-and-batching.md`](./docs/parallelism-and-batching.md) (EN) — 并行与批处理
- [`docs/GITHUB_ACTIONS_DEPLOY.zh.md`](./docs/GITHUB_ACTIONS_DEPLOY.zh.md)（中文）| [`docs/GITHUB_ACTIONS_DEPLOY.md`](./docs/GITHUB_ACTIONS_DEPLOY.md) (EN) — GitHub Actions 发布与部署
- [`docs/SPEC-v2-reaudit-2026-06.zh.md`](./docs/SPEC-v2-reaudit-2026-06.zh.md)（中文）| [`docs/SPEC-v2-reaudit-2026-06.md`](./docs/SPEC-v2-reaudit-2026-06.md) (EN) — v2 生产就绪复审
- [`docs/ROADMAP.zh.md`](./docs/ROADMAP.zh.md)（中文）— 产品能力缺口与后续改进路线图
- [`SPEC.md`](./SPEC.md) — 架构与生产就绪标准
- [`CHANGELOG.zh.md`](./CHANGELOG.zh.md)（中文）| [`CHANGELOG.md`](./CHANGELOG.md) (EN) — 发布说明

## 贡献

参见 [`CONTRIBUTING.zh.md`](./CONTRIBUTING.zh.md)（中文）| [`CONTRIBUTING.md`](./CONTRIBUTING.md) (EN)。改动前请先阅读 SPEC 的代码风格与测试约定（§3-§5）；默认 `-race` 跑测试。

## License

MIT，详见 [`LICENSE`](./LICENSE)。
