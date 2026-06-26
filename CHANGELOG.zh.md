# OpenETL-Go 发布说明

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 格式。

## [Unreleased]

## [v0.2.1] — Pipeline 编排口径收敛与连接复用

### 亮点
- 移除独立宽表 preview API 和专用前端页面。明细宽表与聚合表场景统一通过普通 pipeline/DAG 编排表达，由 source、transform、state 和 sink 组合实现。
- 为线性 pipeline spec 和 DAG node 增加 `connection` / `connection_ref` 引用能力，可以把账号、地址等共享连接配置放入连接目录，任务级 table、topic、query 等字段继续保留在 spec 内。
- 重整英文和中文 README，收敛为快速开始、最小 spec、连接复用、编排式宽表汇聚、连接器能力面、运行模型和文档入口，避免把“已注册能力”误读成“独立产品模块”。

### 验证
- `go test ./internal/etl/server ./internal/etl/pipeline ./internal/etl/orchestrator`
- `web/` 下执行 `npm run build`

## [v0.2.0] — Pipeline 编排与可靠性正式版

### 亮点
- 修复 React 生产 bundle 中 routed page 因运行时未定义变量导致的前端空白页回归，并刷新 Go 服务内嵌的 `resource/public` 产物。
- 新增围绕 Kafka 事实流、维表 lookup、tumbling 聚合和 ClickHouse 输出的 pipeline 编排路径，并补齐编排预览、Connections、Schedules 等 UI 入口。
- 新增 DLQ 稳定 ID replay/delete 流程，补强状态化 transform 指标，并为 deduplicate、lookup、join、window 等状态路径引入 state/checkpoint envelope。
- 收束 connector/source/sink/transform/storage/plugin 成熟度口径，按 beta / production-candidate / production-ready 边界表达能力，避免把“已注册”误读为“生产承诺”。

### 编排验证
- 新增 `hack/e2e-wide-table.sh`，基于 Podman 编排 Redpanda + MySQL + ClickHouse。
- 覆盖 Kafka -> lookup -> ClickHouse 明细 pipeline、Kafka -> deduplicate -> lookup -> tumbling aggregate -> ClickHouse 聚合 pipeline、重复 Kafka 消息吸收、schema drift 入 DLQ、lookup miss 入 DLQ 并修复后 replay、lookup refresh failure 入 DLQ、ClickHouse 下线入 DLQ 并恢复后 replay。

### 发布边界
- 这是 0.2.0 正式版。Kafka 编排式聚合、ClickHouse sink 使用方式、lookup stream-table join、tumbling 聚合、SQLite-backed state 可以作为已验证积木使用，但不宣称任意复杂链路或连接器矩阵 production-ready。
- 默认交付语义仍是 at-least-once。Exactly-once、Kafka rebalance/crash 保证、DAG/stateful replay、stream-stream production join、复杂 window、完整 connector certification 仍是 roadmap 项。

### 验证
- `./hack/e2e-wide-table.sh`
- `./hack/e2e-ui.sh` — 73 passed, 0 failed
- Podman：`go test -timeout 120s ./internal/etl/...`

## [v0.1.0-beta2] — Phase 5 可靠性与易用性发布

### 亮点
- 关闭 beta2 的 P0/P1 可靠性门槛：standalone runner 创建、文件源恢复、零幸存批次 checkpoint 安全、Postgres CDC pgoutput 解析、worker slot 限流、sink error metrics，以及 pipeline 硬性 preflight 错误拦截。
- 重整公开 quickstart 体验：规范 MySQL CDC -> ClickHouse 示例、对齐 Podman compose 配置、补全 `/api/v2/plugins/schema` 元数据，并更新 README / quickstart / 部署文档。
- 改善轻量发布形态：运行时镜像不再携带测试夹具，新增 `-tags=nolua` Lua-free 构建选项，同时保持默认 Lua 兼容。

### 验证
- 新增/更新 server preflight、插件 schema 覆盖、runner checkpoint 安全、Postgres CDC 非行消息、worker slot 限流等测试。
- 已验证受影响包：`go test -race -count=1 -timeout=120s ./internal/etl/server ./internal/etl/pipeline ./internal/etl/source ./internal/etl/worker`。

## [v0.1.0-beta] — 首个公开测试版

### 亮点
- **单二进制 ETL/CDC 引擎**，纯 Go 默认构建，零外部运行时依赖
- 8 种 Source + 9 种 Sink + 19 种 Transform，覆盖主流数据同步/清洗/轻度加工场景
- MySQL CDC（binlog）+ PostgreSQL CDC（逻辑复制）+ 快照增量衔接
- JDBC Sink（支持任意 JDBC 数据库，含 Oracle/SQL Server/DB2 等）
- 22 个 E2E 脚本验证（CDC 崩溃恢复 / DLQ / 分布式分片 / ClickHouse 自动建表 …）
- 单机 SQLite（零依赖）/ 可扩展 MySQL·PG + master-worker 真分布式

### 连接器（Sources）
- `mysql_cdc` — MySQL binlog CDC（行级增删改，含 GTID/position checkpoint）
- `mysql_snapshot_cdc` — MySQL 快照（全量）+ 增量 CDC 无缝衔接
- `postgres_cdc` — PostgreSQL 逻辑复制（pgoutput）
- `mysql_batch` — MySQL 全量批量读取
- `kafka` — Kafka 消费者组（at-least-once，offset checkpoint）
- `redis` — Redis SCAN 全量
- `http` — HTTP API 分页读取（断点续传，429/5xx 指数退避）
- `file` — JSON Lines / CSV 文件（byte-offset checkpoint）

### 连接器（Sinks）
- `clickhouse` — 原生协议 + HTTP 协议，自动建表（DDL 翻译），ReplacingMergeTree 裁剪
- `mysql` — 批量 INSERT / upsert（INSERT … ON DUPLICATE KEY UPDATE），幂等，自动建表
- `postgres` — 批量 INSERT / upsert（INSERT … ON CONFLICT），自动建表
- `doris` — Stream Load + MySQL DELETE，auto-create，DDL 翻译
- `kafka` — 同步生产者（支持幂等），auto-create topic
- `elasticsearch` — Bulk API，动态索引，多 host 轮询，429 Retry-After
- `redis` — HASH/STRING/LIST 三种模式
- `s3` — MinIO/S3 对象存储（分片上传，断点重试，Parquet 支持）
- `jdbc` — 任意 JDBC 数据库（MySQL/PostgreSQL/Oracle/SQL Server/DB2/…）

### 转换（Transforms）
- **清洗**：`filter`（表达式引擎）、`deduplicate`、`validate`（8 种校验规则）、`type_convert`
- **加工**：`rename`/`drop_field`/`add_field`、`enricher`、`lookup`、`join`、`window`
- **路由**：`router`（条件分流）、`fanout`（一对多）、`tap`（旁路）、`rate_limiter`
- **脚本**：`lua`（默认，gopher-lua）、`javascript`/`typescript`（QuickJS，CGO）、WASM 插件（extism，wazero）

### 执行模式
- 线性 Pipeline — 串行 Source→Transform→Sink
- DAG — 多源多汇有向无环图，条件边路由
- ParallelRunner — 单源表分片并行写入
- master-worker 分布式 — MySQL/PG 共享存储，分片跨 worker 不重叠分发，worker 崩溃重分配

### 可靠性
- at-least-once + 幂等 sink（upsert / 版本列）
- DLQ 死信队列（SQLite/MySQL/PG，`/api/v2/dlq/*` 查看重放删除）
- 三态断路器（closed→open→half-open），基于 sink 独立隔离
- 指数退避重试（`retry.Do` + 可重试错误分类）
- `-race` 默认跑测试；零静默数据丢失（SPEC §6.1）

### 运维
- REST API `/api/v2/*`（CRUD pipeline，上传下载 YAML，启停，查看状态/DLQ/preflight）
- Prometheus `/metrics`（每 sink 指标：rows/batches/errors/latency，断路器状态）
- JSON 结构化日志（`LOGGER_FORMAT=json`）
- SQLite / MySQL / PostgreSQL 存储后端（pipeline 定义/checkpoint/DLQ/audit）
- Web 管理界面（Svelte，GoFrame resource-pack）

### 平台
- Linux（amd64、arm64）
- macOS（amd64、arm64 / Apple Silicon）
- Windows（amd64）

### 构建标签
| 标签 | 效果 | 默认？ |
|------|------|------|
| *(无)* | 纯 Go 核心 + 全部 Sink/Source + Lua（gopher-lua，纯 Go） | ✅ |
| `-tags=extism` | + WASM 插件运行时（wazero，纯 Go） | — |
| `-tags=nolua` | 剥离 Lua 运行时，进一步瘦身 | — |
| `CGO_ENABLED=1` | + JavaScript/TypeScript transform（QuickJS，CGO） | — |

### 文档
- `docs/quickstart.md` — 5 分钟入门（中英文双语）
- `docs/etl-api.md` — REST API 参考
- `docs/etl-config-schema.md` — 配置字段参考
- `docs/etl-idempotency.md` — 幂等与 exactly-once 语义
- `docs/parallelism-and-batching.md` — 并行与批处理
- `SPEC.md` — 架构与生产就绪标准（Phase 0-5 全部完成）
