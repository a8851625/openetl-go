# OpenETL-Go Release Notes

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 格式。

## [Unreleased]

## [v0.2.0] — Pipeline orchestration and reliability release

### Highlights
- Fixed React production bundle blank-page regressions caused by undefined runtime variables in routed pages, and refreshed the packed `resource/public` assets used by the Go server.
- Added a pipeline orchestration path around Kafka facts, lookup dimensions, tumbling aggregation, and ClickHouse output, including UI entries for orchestration preview, Connections, and Schedules.
- Added stable DLQ IDs for replay/delete flows, improved stateful transform metrics, and introduced state/checkpoint envelopes for state-backed deduplicate, lookup, join, and window paths.
- Added connector/roadmap maturity guidance so source, sink, transform, storage, and plugin capabilities are presented with explicit maturity instead of over-claiming production readiness.

### Pipeline Validation
- Added `hack/e2e-wide-table.sh` for Podman-based Redpanda + MySQL + ClickHouse validation.
- Covered Kafka -> lookup -> ClickHouse detail pipelines, Kafka -> deduplicate -> lookup -> tumbling aggregate -> ClickHouse pipelines, duplicate Kafka message absorption, schema drift DLQ, lookup miss DLQ and replay, lookup refresh failure DLQ, and ClickHouse outage DLQ/replay.

### Release Boundary
- This is a 0.2.0 release. Kafka orchestration-based aggregation, ClickHouse sink usage, lookup stream-table joins, tumbling aggregation, and SQLite-backed state are available as validated building blocks, not a blanket production-ready guarantee.
- Default delivery semantics remain at-least-once. Exactly-once, Kafka rebalance/crash guarantees, DAG/stateful replay, stream-stream production joins, complex windows, and full connector certification remain roadmap items.

### Verification
- `./hack/e2e-wide-table.sh`
- `./hack/e2e-ui.sh` — 73 passed, 0 failed
- Podman: `go test -timeout 120s ./internal/etl/...`

## [v0.1.0-beta2] — Phase 5 reliability and usability release

### Highlights
- Closed the beta2 P0/P1 reliability bar: standalone runner creation, file-source resume, zero-survivor checkpoint safety, Postgres CDC pgoutput parsing, worker slot accounting, sink error metrics, and preflight rejection for hard pipeline misconfigurations.
- Reworked the public quickstart surface around OpenETL-Go: canonical MySQL CDC -> ClickHouse examples, aligned Podman compose settings, richer `/api/v2/plugins/schema` metadata, and updated README/quickstart/deployment docs.
- Improved the lightweight release shape by excluding test fixtures from runtime images and publishing `-tags=nolua` as the Lua-free build option while keeping default Lua compatibility.

### Verification
- Added/updated focused tests for server preflight behavior, plugin schema coverage, runner checkpoint safety, Postgres CDC non-row messages, and worker slot limits.
- Verified affected packages with `go test -race -count=1 -timeout=120s ./internal/etl/server ./internal/etl/pipeline ./internal/etl/source ./internal/etl/worker`.

## [v0.1.0-beta] — 首个公开测试版

### 亮点
- **单二进制 ETL/CDC 引擎**,纯 Go 默认构建,零外部运行时依赖
- 8 种 Source + 9 种 Sink + 19 种 Transform,覆盖主流数据同步/清洗/轻度加工场景
- MySQL CDC (binlog) + PostgreSQL CDC (逻辑复制) + 快照增量衔接
- JDBC Sink (支持任意 JDBC 数据库,含 Oracle/SQL Server/DB2 等)
- 22 个 E2E 脚本验证(CDC 崩溃恢复 / DLQ / 分布式分片 / ClickHouse 自动建表 …)
- 单机 SQLite(零依赖) / 可扩展 MySQL·PG + master-worker 真分布式

### 连接器 (Sources)
- `mysql_cdc` — MySQL binlog CDC (行级增删改,含 GTID/position checkpoint)
- `mysql_snapshot_cdc` — MySQL 快照(全量) + 增量 CDC 无缝衔接
- `postgres_cdc` — PostgreSQL 逻辑复制 (pgoutput)
- `mysql_batch` — MySQL 全量批量读取
- `kafka` — Kafka 消费者组 (at-least-once,offset 断点)
- `redis` — Redis SCAN 全量
- `http` — HTTP API 分页读取(断点续传,429/5xx 指数退避)
- `file` — JSON Lines / CSV 文件(byte-offset checkpoint)

### 连接器 (Sinks)
- `clickhouse` — 原生协议 + HTTP 协议,自动建表(DDL 翻译),ReplacingMergeTree 裁剪
- `mysql` — 批量 INSERT / upsert(INSERT … ON DUPLICATE KEY UPDATE),幂等,自动建表
- `postgres` — 批量 INSERT / upsert(INSERT … ON CONFLICT),自动建表
- `doris` — Stream Load + MySQL DELETE,auto-create,DDL 翻译
- `kafka` — 同步生产者(支持幂等),auto-create topic
- `elasticsearch` — Bulk API,动态索引,多 host 轮询,429 Retry-After
- `redis` — HASH/STRING/LIST 三种模式
- `s3` — MinIO/S3 对象存储(分片上传,断点重试)
- `jdbc` — 任意 JDBC 数据库 (MySQL/PostgreSQL/Oracle/SQL Server/DB2/…)

### 转换 (Transforms)
- **清洗**: `filter`(表达式)、`deduplicate`、`validate`、`type_convert`
- **加工**: `rename`/`drop_field`/`add_field`、`enricher`、`lookup`、`join`、`window`
- **路由**: `router`(条件分流)、`fanout`(一对多) `tap`(旁路) `rate_limiter`
- **脚本**: `lua`(默认,gopher-lua)、`javascript`/`ts`(QuickJS,CGO)、WASM 插件(extism,wazero)

### 执行模式
- 线性 Pipeline — 串行 Source→Transform→Sink
- DAG — 多源多汇有向无环图,条件边路由
- ParallelRunner — 单源表分片并行写入
- master-worker 分布式 — MySQL/PG 共享存储,分片跨 worker 不重叠分发,worker 崩溃重分配

### 可靠性
- at-least-once + 幂等 sink (upsert / 版本列)
- DLQ 死信队列 (SQLite/MySQL/PG,`/api/v2/dlq/*` 查看重放删除)
- 三态断路器 (closed→open→half-open),基于 sink 独立隔离
- 指数退避重试 (`retry.Do` + 可重试错误分类)
- `-race` 默认跑测试;零静默数据丢失 (SPEC §4.2/§6.1)

### 运维
- REST API `/api/v2/*` (CRUD pipeline,上传下载 YAML,启停,查看状态/DLQ/preflight)
- Prometheus `/metrics` (每 sink 指标:rows/batches/errors/latency,断路器状态,lineage)
- JSON 结构化日志 (`LOGGER_FORMAT=json`)
- SQLite / MySQL / PostgreSQL 存储后端 (pipeline 定义/checkpoint/DLQ/audit)
- Web 管理界面 (Svelte,GoFrame resource-pack)
- `make test` / `make test-quick` / `make test-integration`

### 平台
- Linux (amd64, arm64)
- macOS (amd64, arm64 / Apple Silicon)
- Windows (amd64)

### 构建标签
| 标签 | 效果 | 默认? |
|------|------|------|
| *(无)* | 纯 Go 核心 + 全部 Sink/Source + Lua(gopher-lua,纯 Go) | ✅ |
| `-tags=extism` | + WASM 插件运行时(wazero,纯 Go) | — |
| `-tags=nolua` | 剥离 Lua 运行时,进一步瘦身 | — |
| `CGO_ENABLED=1` | + JavaScript/TypeScript transform(QuickJS,CGO) | — |

### 文档
- `docs/quickstart.md` — 5 分钟入门
- `docs/etl-api.md` — REST API
- `docs/etl-config-schema.md` — 配置字段
- `docs/etl-idempotency.md` — 幂等与 exactly-once 语义
- `docs/parallelism-and-batching.md` — 并行与批处理
- `SPEC.md` — 架构与生产就绪标准 (Phase 0-5 全部完成)
