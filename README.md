# OpenETL-Go — 轻量级 ETL/CDC 引擎(单二进制)

> **OpenETL-Go** 是一个单二进制、插件化的 ETL/CDC 引擎:`Source → Transform → Sink`。
> 支持 MySQL binlog / PostgreSQL 逻辑复制 / Kafka / 文件 / HTTP / Redis 作为源,
> ClickHouse / MySQL / PostgreSQL / Doris / Elasticsearch / Kafka / Redis / S3 / JDBC 作为目标。
> 单机用 SQLite(零外部依赖);可扩展用 MySQL/PostgreSQL 共享存储 + master-worker 分片。

## 能力一览

| 阶段 | 支持的连接器 / 算子 |
|------|---------------------|
| **Source** | `mysql_cdc`(binlog)、`mysql_snapshot_cdc`(快照+增量衔接)、`postgres_cdc`(逻辑复制)、`mysql_batch`、`kafka`、`redis`(SCAN)、`http`(分页+断点)、`file`(JSON/CSV) |
| **Transform** | `filter`(表达式)、`deduplicate`、`validate`、`rename`/`drop_field`/`add_field`、`type_convert`、`enricher`、`lookup`、`join`、`window`、`router`(条件路由)、`fanout`、`tap`、`rate_limiter`、`ts`;脚本扩展:`lua`(默认)、`javascript`(QuickJS)、WASM 插件 |
| **Sink** | `clickhouse`(自动建表+DDL 翻译)、`mysql`/`postgres`(批量+幂等 upsert)、`doris`(Stream Load)、`kafka`(幂等生产者)、`elasticsearch`、`redis`、`s3`/minio、`jdbc`、`file` |
| **可靠性** | at-least-once + 幂等 sink + DLQ 死信队列 + 三态断路器 + 指数退避重试 + checkpoint;**零静默数据丢失**(SPEC §6.1) |
| **执行模式** | 线性 pipeline / DAG 多源多汇 / ParallelRunner 分片 / master-worker 真分布式(A11-redo 验证) |
| **运维** | REST API `/api/v2/*`、preflight 预检、Prometheus `/metrics`、JSON 结构化日志、SQLite/MySQL/PG 元存储、Web 管理界面 |

## 🚀 5 分钟快速开始

```bash
# 1. 启动依赖(MySQL 源 + ClickHouse 目标)
podman compose -f docker-compose.quickstart.yml up -d
# 2. 示例管道 MySQL CDC -> ClickHouse(自动建表)已在 pipes-quickstart/ 提供
#    也可以直接用: pipes/mysql-cdc-to-clickhouse.yaml
# 3. 打开管理界面: http://localhost:8000   (REST API: /api/v2/*)
```

- **示例 spec**:`pipes-quickstart/mysql-to-clickhouse.yaml`、`pipes/mysql-cdc-to-clickhouse.yaml`
- **完整入门**:[`docs/quickstart.md`](./docs/quickstart.md)

## 安装

### 从 Release 下载(推荐)

到 [Releases](../../releases) 下载对应平台的压缩包(Linux/macOS/Windows × amd64/arm64),解压后:

```bash
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
go build -o openetl-go .                # 默认构建(纯 Go,含 Lua)
# 可选运行时:
go build -tags=extism -o openetl-go .   # 启用 WASM 插件(wazero,纯 Go)
go build -tags=nolua -o openetl-go .    # 剥离 Lua 运行时,进一步瘦身
CGO_ENABLED=1 go build -o openetl-go .  # 启用 JavaScript/TypeScript transform(QuickJS,需 CGO)
```

## 构建标签

| 标签 | 效果 | 默认 |
|------|------|------|
| *(无)* | 纯 Go 核心 + 全部 sink + Lua;无 WASM、无 QuickJS、无 CGO | ✅ |
| `-tags=extism` | + WASM 插件运行时(wazero,纯 Go) | — |
| `-tags=nolua` | 剥离 Lua 运行时(`type:lua` 返回清晰错误),进一步瘦身 | — |
| `CGO_ENABLED=1` | + JavaScript/TypeScript transform(QuickJS,需 C 工具链) | — |

## 运行模式

- **单机(默认)**:SQLite 存储,零外部依赖,一个进程跑全部管道。
- **可扩展**:把 `etl.storage.type` 切到 `mysql`/`postgresql`,用 `etl.role=master` + 多个 `etl.role=worker` 跑真分布式分片(线性 spec 跨 worker 不重叠分发,worker 崩溃后分片重分配)。

最小配置见 [`manifest/config/config.yaml`](./manifest/config/config.yaml);详细字段见 [`docs/etl-config-schema.md`](./docs/etl-config-schema.md)。

## 文档

- [`docs/quickstart.md`](./docs/quickstart.md) — 5 分钟入门
- [`docs/etl-api.md`](./docs/etl-api.md) — REST API
- [`docs/etl-config-schema.md`](./docs/etl-config-schema.md) — 配置字段
- [`docs/etl-idempotency.md`](./docs/etl-idempotency.md) — 幂等与 exactly-once 语义
- [`docs/parallelism-and-batching.md`](./docs/parallelism-and-batching.md) — 并行与批处理
- [`SPEC.md`](./SPEC.md) — 架构与生产就绪标准

## 贡献

见 [`CONTRIBUTING.md`](./CONTRIBUTING.md)。改动前请先阅读 SPEC 的代码风格与测试约定(§3-§5);默认 `-race` 跑测试。

## License

MIT,见 [`LICENSE`](./LICENSE)。
