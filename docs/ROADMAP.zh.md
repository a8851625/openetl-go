# OpenETL 后续改进 Roadmap

本文面向项目目标：成为一个开放、易用、可扩展的数据同步与数据处理工具。结论来自当前代码、文档与已发布能力的抽样审计，优先关注产品能力缺口、设计风险和可验证的落地路径。

## 当前基线

OpenETL 已经具备较好的雏形：单二进制部署、GoFrame HTTP 服务、React UI、SQL 存储后端、Pipeline/DAG 执行、DLQ、checkpoint、审计、指标、插件安装、主从 worker、较丰富的 source/sink/transform 注册体系，以及多条 Podman e2e 脚本。

当前更像“能力广、端到端可运行”的工程平台，但距离“开放、易用、可扩展”的产品目标还有几类关键缺口：

- 可靠性契约仍需收紧：部分错误路径仍可能跳过记录或缺少 DAG/stateful replay 语义，跨 sink fanout、Kafka 事务、S3/file deterministic manifest、ES item-level DLQ 等也在现有幂等性文档中被列为未保证能力。
- 易用性还偏工程化：UI 已有 dashboard、pipeline、designer、DLQ、plugin、worker、audit，但缺少一等公民的连接管理、向导式建管道、schema 预览、运行前影响评估、错误修复建议和调度入口。
- 扩展接口没有闭环：`SchemaDescriptor` / `SchemaValidator` 已定义但内置 connector 尚未实现，插件元数据与真实能力可能漂移，插件编译仍可在请求时走 `npx` fallback。
- 数据处理能力偏基础：窗口 transform 实际主要支持 tumbling，watermark/late data 只是部分支持；sliding/session window、schema registry、Avro/Protobuf、join miss policy、lineage 还未产品化。
- 开放生态建设不足：README 与真实成熟度之间需要自动化矩阵校准，贡献指南、兼容性策略、插件认证、公开 roadmap 和 issue 模板还不足以支撑外部贡献者规模化参与。

## 首要产品方向：Kafka 驱动的聚合宽表

近期 roadmap 的第一优先级应从“继续扩 connector 数量”调整为“把 Kafka 实时数据接入、多源 join、聚合计算、宽表落地做成可生产使用的完整场景”。这是最能体现 OpenETL 同时具备同步、轻量流处理和可视化编排价值的场景，也能倒逼可靠性、schema、状态和运维能力成熟。

当前能力判断：OpenETL 已经具备搭出 Kafka -> 维表 lookup -> ClickHouse 宽表/聚合宽表 MVP 的基础能力，也已经有预览、向导、示例和 Podman happy-path e2e 的雏形；但还不能把“任意 Kafka 流 + 任意外部数据 join + 宽表聚合”直接声明为 production-ready。生产化的关键差距不在于 connector 是否存在，而在于状态恢复、offset/checkpoint/sink commit 边界、join/window 语义、失败路径 DLQ、schema/preflight 和运维指标是否能被自动化测试证明。

目标场景：

- Kafka 事实流作为主输入，支持 JSON、Debezium envelope，并逐步支持 Avro/Protobuf + Schema Registry。
- 事实流可与 MySQL/PostgreSQL 维表、Kafka 维表 topic、HTTP/Redis 等外部数据 join，形成明细宽表。
- 明细宽表可继续按时间窗口、业务维度做实时聚合，输出到 ClickHouse/MySQL/PostgreSQL/Doris/S3 等 sink。
- UI 能通过向导完成连接、topic/sample 预览、字段映射、join 规则、聚合指标、目标表 DDL、preflight、发布和回放。

必须补齐的核心能力：

| 能力域 | 当前基础 | Production-ready 缺口 | 优先交付 |
|--------|----------|------------------------|----------|
| Kafka 输入语义 | `kafka` source 已可消费，已有 Redpanda e2e。 | topic 多分区顺序、consumer group rebalance、offset 与 sink checkpoint 关系、重复消息处理边界需要固化；缺 Debezium/Schema Registry 一等解析。 | Kafka crash + rebalance + duplicate e2e；增加 envelope/parser 层，支持 event time、op、key、source metadata 标准化。 |
| 维表 join | `lookup`、`enricher`、Lua 内存状态、`join` transform 已有基础；`lookup` 已支持刷新持久化、状态恢复、缓存上限、miss 显式 null/DLQ/error 策略、刷新失败 error 策略和 hit/miss/刷新/恢复指标；宽表 Podman e2e 已覆盖维表 miss 进入 DLQ、补维表后按稳定 DLQ ID replay 写入 sink，以及维表刷新失败进入 DLQ。 | CDC 维表更新、crash e2e 和 backpressure 仍未闭环。 | 先做 stream-table join production path：MySQL/PostgreSQL 维表 snapshot + 定时刷新 + miss DLQ/left join；再做 CDC 维表增量更新。 |
| 流流 join | `join` 支持 keyed interval join，已可选接入 SQLite `StateStore` 恢复基础缓冲状态；runner/DAG executor 已可自动注入 pipeline/node 状态命名空间；linear runner 与 DAG executor checkpoint envelope 已记录 state snapshot version；已支持 `max_buffered_keys`/`max_buffered_records` 状态上限。 | 仍缺 Kafka rebalance/crash e2e、event-time interval、watermark、late data 和恢复边界证明。 | 保持 beta；补 interval join e2e 和 Kafka crash/rebalance 认证后再作为生产路径。 |
| 窗口聚合 | `window` 主要支持 tumbling，已有基础聚合函数，并可选接入 SQLite `StateStore` 恢复未关闭窗口的聚合缓冲。 | sliding/session 未实现；watermark、allowed lateness、late side-output、retraction/update/delete CDC 聚合语义和输出事务边界不完整。 | 先生产化 tumbling + event-time + allowed lateness + 状态恢复；再补 sliding/session。 |
| 宽表 sink | ClickHouse/MySQL/PostgreSQL/S3/Kafka 等 sink 已实现；ClickHouse 宽表已使用 ReplacingMergeTree/version 策略，宽表 Podman e2e 已覆盖明细宽表重复 Kafka 消息在 `FINAL` 视图下按业务主键去重、聚合宽表通过 `deduplicate(id,_version)` 避免重复累计、ClickHouse schema drift 写入失败进入 DLQ，以及 ClickHouse 容器下线时明细宽表写入失败进入 DLQ 并在恢复后按稳定 DLQ ID replay 写回。 | delete/update CDC、distributed 表、真实超时注入、自动恢复边界和更完整 schema drift 回滚测试仍未闭环。 | ClickHouse 作为第一生产目标；继续补 ClickHouse timeout injection、update/delete/final consistency、distributed table 和 schema evolution 测试。 |
| 状态与一致性 | checkpoint、DLQ、run history 已有基础；`StateStore` v1 已起步，deduplicate/lookup/join/window 可选接入 SQLite 状态；linear runner 与 DAG executor 会为启用 `state_backend` 的 transform 注入状态命名空间；linear runner 与 DAG executor checkpoint envelope 已保存 source position + state snapshot versions。 | Kafka offset、状态快照、sink commit 之间仍缺一致性协议和恢复测试；sink commit metadata 仍未接入。 | 补 sink commit metadata、Kafka crash/rebalance 和 state replay e2e。 |
| 可观测与修复 | pipeline metrics、DLQ、audit 已有基础；状态化 transform 已通过 `/api/v2/metrics` 和 `/metrics` 暴露 node-level state keys/bytes/updated_at；`lookup` 已暴露 processed/hit/miss/missing_key/miss_null/miss_dlq/miss_error/refresh_success/refresh_error/refresh_error_dlq/restore_success/scan_error/cache_limit_exceeded；`join` 已暴露 hit/miss/miss_dropped/miss_dlq/miss_error；`window` 已暴露 accumulated/late_dropped/emitted_records/emitted_windows/flushed_records；linear lookup miss 已有按稳定 DLQ ID replay 修复 e2e。 | 仍缺 node-level lag/watermark lag、late side-output/DLQ 指标；DLQ replay 对 DAG/状态型任务不完整。 | 增加聚合宽表专用指标、node-level DLQ、按时间/offset 重放和从 checkpoint 重建宽表的 runbook。 |
| UI 与开发体验 | DAG editor、schema API、插件 schema 已有基础。 | 用户仍需要写 YAML；缺 topic sample、join 预览、聚合预览、目标 DDL 预览、影响分析和字段血缘。 | 新增 Wide Table Wizard：source sample -> dimension join -> metrics -> sink DDL -> preflight -> deploy。 |
| 测试认证 | 已有多条 Podman e2e；宽表脚本已覆盖 Redpanda + MySQL 维表 + ClickHouse 的明细/聚合 happy path、ClickHouse 明细宽表重复消息幂等、聚合宽表重复消息不重复累计、ClickHouse schema drift 写入失败进入 DLQ、ClickHouse 容器下线时明细宽表写入失败进入 DLQ 并在恢复后 replay、lookup miss 进入 DLQ、补维表后按稳定 DLQ ID replay 写入 sink、lookup 维表刷新失败进入 DLQ。 | 仍缺 crash、rebalance、late data、stream-stream join miss、ClickHouse 真实超时注入、DAG/stateful replay 等异常路径认证。 | 扩展宽表 Podman e2e：Redpanda + MySQL 维表 + ClickHouse 宽表，覆盖 crash、rebalance、late data、stream-stream join miss、ClickHouse timeout injection、DAG/stateful DLQ replay。 |

首个生产候选链路建议定义为：

```text
Kafka(JSON/Debezium order events)
  -> normalize envelope/event_time
  -> lookup MySQL/PostgreSQL dimension tables
  -> build order detail wide table
  -> tumbling window aggregate by business keys
  -> ClickHouse ReplacingMergeTree tables
```

该链路进入 production-ready 的最低门槛：

- 至少一条明细宽表和一条聚合宽表 Podman e2e，依赖 Redpanda + MySQL/PostgreSQL + ClickHouse。
- 覆盖 Kafka consumer crash、worker restart、consumer group rebalance、重复消息、join miss、维表刷新失败、ClickHouse 写入失败。
- 所有记录级失败进入 DLQ 或明确计数审计，不允许静默跳过。
- checkpoint 能说明并验证 Kafka offset、join/window 状态、sink 写入之间的恢复边界。
- 文档明确交付语义：默认 at-least-once，exactly-once 不承诺；推荐使用业务主键、版本列和 sink 幂等策略消除重复影响。

因此，短期产品口径应是：支持 Kafka 驱动的宽表 MVP 和 production candidate 链路建设；只有当上面的状态恢复、失败路径和指标认证通过后，才把它升级为 production-ready。

## 关键设计不足

### 1. 可靠同步：必须优先保证“没有静默丢数”

已发现的高优先级风险：

- Elasticsearch sink 在 bulk 构造时，action/doc marshal 失败会打印 warning 后 `continue`，该记录不会进入 DLQ，也不会让 pipeline 失败。这和数据同步工具的默认可靠性预期冲突。
- DLQ replay 已支持 linear pipeline 按稳定 ID 精确操作，但 DAG pipeline replay 和状态型 replay 语义仍未支持。
- 幂等性文档仍列出未保证项：跨 sink 原子 fanout、Kafka transactions、S3/file deterministic manifests、ES partial bulk item-level DLQ。
- 部分 SPEC 中的旧问题已经修复，需要重新校准状态，避免团队继续基于过期风险列表决策。

设计方向：

- 所有 sink 的单条记录失败必须三选一：写入 DLQ、返回错误触发 retry、或有显式配置允许丢弃并计数审计。
- DLQ 记录需要稳定 ID、原始 pipeline/version/node、record hash、错误分类、首次失败时间、最近失败时间、重放次数。
- DAG 与 linear pipeline 使用统一失败语义，node-level DLQ 可追踪来源和目标 sink。
- 发布前必须有“异常路径 e2e”，不仅验证 happy path。

### 2. 易用性：从“会写 YAML 才能用”升级到“可引导完成任务”

当前 UI 已能监控和编辑，但用户完成常见任务仍需要理解 connector 配置字段、YAML 结构、运行时错误和部署方式。

主要缺口：

- 缺少 Connection Catalog：用户不能先创建、测试、复用 MySQL/Postgres/Kafka/S3/ES 等连接。
- 缺少 Pipeline Wizard：没有从 source 选择、字段预览、目标映射、同步模式、幂等策略、调度/CDC 策略到提交的一条路径。
- 缺少 schema preview 与 sample preview：用户难以在启动前知道会同步哪些列、类型是否兼容、目标表会怎样创建。
- `SchedulesPage.tsx` 存在，但主路由/导航没有把 schedule 作为一等页面暴露。
- 错误文案偏底层，缺少错误分类、修复建议、配置字段定位和文档链接。

设计方向：

- UI 以“连接 -> 数据集 -> 同步任务 -> 运行/恢复”为主流程，而不是只以 pipeline YAML 为中心。
- 所有 connector 暴露统一的 typed descriptor，UI 表单、文档、校验、OpenAPI 都从同一份元数据生成。
- preflight 不仅返回 pass/fail，还返回风险等级、影响范围、建议修复动作。

### 3. 可扩展性：插件能力需要 ABI、元数据和测试套件

项目已有 WASM/TypeScript SDK、插件安装 API 和 connector registry，但平台化程度还不够。

主要缺口：

- 插件 ABI/SDK 兼容策略不明确，缺少版本协商和 deprecation policy。
- 插件编译路径仍可依赖请求时 `npx --yes -p @extism/js-pdk@...`，这对生产环境的可用性、供应链安全和可复现构建都不理想。
- connector metadata 部分仍是手写聚合，容易和真实配置、能力、成熟度漂移。
- 缺少 connector certification：新 source/sink/transform 没有统一合约测试、失败注入、幂等性验证和性能基准。
- `SchemaDescriptor` / `SchemaValidator` 已预留，但内置 connector 未实现，导致 schema 校验链路实际无效。

设计方向：

- 插件体系要从“可以运行自定义代码”升级为“可发布、可测试、可认证、可升级的扩展生态”。
- 内置 connector 与插件 connector 使用同一套 descriptor、schema、preflight、metrics、DLQ 合约。
- 默认生产镜像不应在请求路径拉取 npm 包；编译器应预安装、离线缓存，或把编译移到 CLI/CI。

### 4. 数据处理：流处理语义还不完整

OpenETL 的 transform 种类丰富，但有些高级处理能力尚未达到可预期的生产语义。

主要缺口：

- window transform 接受 `window_type`，但 sliding/session 仍未实现，当前主要是 tumbling。
- watermark、allowed lateness、late record 处理需要明确输出语义和 DLQ/side-output 策略。
- join/lookup/window 的状态存储、过期、恢复、内存上限与 checkpoint 关系需要产品化。
- 缺少 schema registry、Avro/Protobuf、Debezium envelope、字段血缘和类型演进策略。
- JavaScript/TypeScript/Lua 脚本 transform 已有预算控制，但仍需要更系统的沙箱、依赖、日志和调试体验。

设计方向：

- 把 batch sync、CDC sync、stream processing 分层定义清楚，避免所有能力都塞进同一个 pipeline 语义。
- 高级 transform 必须定义状态模型、时间模型、失败模型和资源模型。
- 对处理类能力提供本地 dry-run、样例输入输出、单元测试模板和可视化调试。

### 5. 开放生态：需要从“代码开源”走向“协作开放”

主要缺口：

- 缺少公开 roadmap 与 connector 成熟度矩阵，外部用户难以判断哪些能力可生产使用。
- README 能力矩阵和实现成熟度需要自动化校验，避免宣传超过代码现实。
- 贡献者缺少 issue 模板、插件开发教程、connector 合约测试说明、版本兼容策略和发布流程说明。
- API/OpenAPI、配置 schema、UI 表单、插件 schema 还需要更强的一致性。

设计方向：

- 每个 connector 明确 maturity：experimental、beta、production。
- 每个能力有 owner、测试门槛、兼容性说明和文档入口。
- 文档默认提供 Podman 调试路径，与现有 e2e 脚本保持一致。

## Connector 与插件 Production-ready 检视

当前 `pluginMetadata()` 中大量 source/sink 被标记为 `stable`，但 SPEC 已明确：能力成熟度不能只看 metadata 字符串，必须同时满足代码实现、真实测试、文档与运行时行为一致。下面的矩阵用于重新校准生产可用性，后续应把它转成自动生成的 connector maturity matrix。

### 分级标准

- **Production-ready**：有真实依赖的 Podman e2e；覆盖 happy path、重启/checkpoint、DLQ/错误路径、幂等或重复处理语义；有 preflight、metrics、配置文档；不会静默丢数。
- **Beta**：核心路径可用，有单测或部分 e2e，但生产语义仍有缺口，例如 schema 校验、异常路径、重放、资源上限、兼容性或文档不足。
- **Experimental**：已注册或已有原型，但缺少真实 e2e、生产安全边界、兼容性承诺或关键语义。
- **Dev-only**：仅用于示例、测试或开发演示，不应进入生产能力矩阵。

### Source 成熟度

| Source | 当前判断 | 证据与主要缺口 | Roadmap 跟进 |
|--------|----------|----------------|--------------|
| `mysql_batch` | Beta / 准生产 | 已有 MySQL batch -> file/MySQL e2e；但未实现 `SchemaDescriptor`，schema preview/preflight 无法基于真实列信息闭环。 | 实现 schema 描述、增量 cursor 边界测试、重复运行幂等测试，补 descriptor 驱动的 UI 表单。 |
| `mysql_cdc` | Beta / 准生产 | 已有 CDC -> MySQL、CDC -> ClickHouse、CDC crash recovery e2e；仍需更严格的 DDL、删除、断点、GTID/binlog purge 场景验证。 | 增加 binlog 位点异常、DDL drift、delete/update、长时间断线恢复 e2e，并和 sink idempotency matrix 绑定。 |
| `mysql_snapshot_cdc` | Beta / 准生产 | 已有 snapshot+CDC 和 crash recovery e2e；快照与增量衔接是高风险语义。 | 固化 low/high watermark 语义、重复快照重放测试、schema drift during snapshot 测试。 |
| `postgres_cdc` | Beta | 有实现与单测；SPEC 仍记录 TRUNCATE/DDL 语义不完整，缺少 PostgreSQL CDC -> sink 的真实 e2e。 | 增加 PostgreSQL logical replication e2e，明确 TRUNCATE/DDL 同步策略，补 slot restart 与 publication 配置检查。 |
| `kafka` | Beta / 准生产 | 有 Redpanda e2e 覆盖 source/sink；但 Kafka transactions、consumer group 重平衡、offset commit 与 sink checkpoint 关系仍需强化。 | 增加 crash + rebalance + duplicate message e2e，明确 exactly-once 不承诺边界和推荐 key 策略。 |
| `http` | Beta / 准生产 | 有分页/auth/checkpoint e2e；HTTP API 类型差异大，错误重试、限流、游标语义需更多模板。 | 增加 429/5xx Retry-After、cursor 过期、schema inference、sample preview。 |
| `file` | Beta | 有 file -> file e2e 和 CSV/JSON 单测；文件重扫、移动/截断、glob、多文件 checkpoint 语义需明确。 | 增加多文件、文件轮转、崩溃重扫、CSV 类型推断测试。 |
| `redis` | Experimental | 有配置/单测，但未见 Redis source 真实 e2e；SCAN/stream/list/hash 语义、checkpoint 和重复处理策略需要澄清。 | 补 Redis source/sink e2e，区分 SCAN 快照与 Stream 消费模式，定义 checkpoint 与幂等建议。 |
| `demo` | Dev-only | 注册为 source，但用于演示/测试。 | 从公开生产矩阵隐藏，或明确标为 dev-only。 |

### Sink 成熟度

| Sink | 当前判断 | 证据与主要缺口 | Roadmap 跟进 |
|------|----------|----------------|--------------|
| `mysql` | Beta / 准生产 | 有 batch -> MySQL、CDC -> MySQL、snapshot+CDC -> MySQL e2e；但未实现 `SchemaValidator`，upsert/delete/DDL drift 组合需矩阵化。 | 实现 schema validation，补 delete/update/upsert/insert mode 异常路径测试。 |
| `clickhouse` | Beta / 准生产 | 有 CDC -> ClickHouse、auto-create、schema drift e2e；宽表 e2e 已验证 ReplacingMergeTree 明细表重复消息在 `FINAL` 视图下按业务主键去重、聚合宽表重复消息不重复累计、schema drift 写入失败进入 DLQ，以及 ClickHouse 容器下线时明细宽表写入失败进入 DLQ 并在恢复后按稳定 DLQ ID replay 写回。ClickHouse DELETE/UPDATE、真实超时注入和 distributed 表语义仍复杂。 | 增加 update/delete/final consistency、distributed、optimize、schema drift 回滚和 ClickHouse timeout injection 测试。 |
| `s3` / `file_sink` | Beta | 有 MinIO JSONL/CSV/Parquet e2e；现有幂等性文档仍指出 deterministic object manifest 未保证。 | 增加 deterministic object naming/manifest、崩溃后 multipart 清理、重复写入去重策略。 |
| `kafka` | Beta | 有 Redpanda e2e；Kafka transactions 未保证，exactly-once 只能依赖 key/idempotent producer 和下游去重。 | 明确 transaction 不承诺；补 partition key、producer retry、broker restart e2e。 |
| `postgres` / `postgresql` | Beta | 有代码和存储后端 e2e，但缺直接 pipeline -> PostgreSQL sink 的真实 e2e 覆盖。 | 增加 MySQL batch/CDC -> PostgreSQL sink e2e，补 schema validation 和 truncate/delete 语义文档。 |
| `elasticsearch` / `es` | Beta，Phase 0 阻塞 | 有 OpenSearch bulk e2e；但 bulk 构造 marshal 错误会 warning 后跳过，ES partial bulk item-level DLQ 仍未完成。 | 先修 silent skip，再做 item-level DLQ、429 Retry-After、mapping conflict、bulk partial retry e2e。 |
| `redis` | Experimental | 有实现与配置文档，但缺真实 Redis sink e2e；HASH/STRING/LIST 的幂等与 delete/update 语义需要明确。 | 补 Redis sink e2e，定义 key strategy、TTL、delete/update 行为和重放语义。 |
| `doris` | Experimental / Beta | 有实现与单测；未见 Podman e2e，Stream Load、事务标签、delete/upsert、schema drift 需要真实集群验证。 | 增加 Doris e2e 或文档标为 experimental；补 label 幂等、partial failure、schema drift 测试。 |
| `jdbc` | Experimental | 通用 JDBC 能力面太宽，当前缺少按数据库的 e2e 和兼容矩阵。 | 拆分认证矩阵：MySQL/PostgreSQL 可通过原生 sink 覆盖，JDBC 先针对 1-2 个数据库做 certification。 |

### Transform 与脚本插件成熟度

| Transform / Plugin | 当前判断 | 证据与主要缺口 | Roadmap 跟进 |
|--------------------|----------|----------------|--------------|
| `identity`、`rename`、`drop_field`、`add_field`、`type_convert`、`filter` | Production candidate | 基础无状态算子，单测覆盖较好；仍需和 schema descriptor 联动。 | 作为第一批 production transform，补配置 schema 与错误定位。 |
| `lua` | Beta | 默认构建可用，已有超时和沙箱；但无强内存上限，脚本依赖、调试和资源审计还不完整。 | 增加资源指标、脚本 dry-run、错误定位；生产文档说明内存边界。 |
| `javascript` / `typescript` / `ts` | Experimental / Beta | 依赖 CGO + QuickJS，非默认构建；已有 timeout/memory limit，但部署复杂度高。 | 明确构建矩阵，补 CGO 镜像、脚本兼容测试、资源耗尽 e2e。 |
| `deduplicate` | Beta / 准生产 | LRU 去重可用，已可选接入 SQLite `StateStore`，覆盖重启后识别重复 key 和 TTL 过期测试；runner/DAG executor 已可注入状态命名空间；linear runner 与 DAG executor checkpoint envelope 已记录 state snapshot version；基础 state keys/bytes/updated_at 与 processed/passed/duplicate_dropped/memory_duplicate_dropped/state_duplicate_dropped/evicted_keys 指标已接入。 | 下一步补 crash e2e。 |
| `validate` | Beta | 数据质量规则可用；失败策略、规则报告和 DLQ 分类需要产品化。 | 增加 `on_fail: dlq|drop|error`、质量报告、样例导出。 |
| `router` / `fanout` | Beta | 已修复 provenance 方向问题；但多 sink fanout 原子性仍未保证。 | 明确 fanout delivery contract，补 partial sink failure 与回滚/补偿策略。 |
| `tap` / `rate_limiter` | Beta | 运维辅助算子；需要指标、全局/分片限流语义。 | 补 Prometheus 指标、分布式限流说明和 e2e。 |
| `enricher` / `lookup` | Beta / 准生产 | HTTP/SQL enrichment 可用；`lookup` 已可选接入 SQLite `StateStore`，刷新成功后持久化维表缓存，维表查询失败时可恢复最近未过期快照；已支持 `max_cache_entries` 上限、`on_miss: pass|null|dlq|error`、`on_refresh_error: pass|error`，并暴露 processed/hit/miss/missing_key/miss_null/miss_dlq/miss_error/refresh_success/refresh_error/refresh_error_dlq/restore_success/scan_error/cache_limit_exceeded 指标；`hack/e2e-wide-table.sh` 已验证 lookup miss 入 DLQ、补维表后按稳定 DLQ ID replay 写入 sink，以及坏维表 query 进入 DLQ。 | 补 CDC 维表增量更新和 crash e2e。 |
| `join` | Beta | `on_miss` 已出现，且可选 SQLite `StateStore` 已能按 join key 持久化/恢复基础 interval join 缓冲；runner/DAG executor 已可注入状态命名空间；linear runner 与 DAG executor checkpoint envelope 已记录 state snapshot version；基础 state keys/bytes/updated_at 与 join hit/miss/miss policy 指标已接入；`max_buffered_keys`/`max_buffered_records` 可限制内存状态，上限触发时进入现有错误/DLQ 路径并暴露 `state_limit_exceeded`；但缺 Kafka crash/rebalance 验证。 | 补 interval join e2e。 |
| `window` | Beta（tumbling）/ Experimental（复杂窗口） | 当前主要是 tumbling；可选 SQLite `StateStore` 已能恢复未关闭窗口的聚合缓冲；runner/DAG executor 已可注入状态命名空间；linear runner 与 DAG executor checkpoint envelope 已记录 state snapshot version；基础 state keys/bytes/updated_at 与 accumulated/late_dropped/emitted_records/emitted_windows/flushed_records 指标已接入；`window_type` 生产 schema 只暴露 tumbling。sliding/session、late side-output、retraction/update/delete CDC 聚合语义仍不完整。 | 补 watermark lag、late side-output/DLQ 和 window crash e2e；实现 sliding/session 后再提升复杂窗口成熟度。 |
| WASM `plugin_*` source/sink/transform | Experimental | 需要 `-tags=extism`，默认构建为 no-op；source checkpoint 目前是通用时间戳，sink 无 schema/idempotency 合约，服务端 TS 编译仍可能走请求时 `npx` fallback。 | 定义 Plugin ABI v1、manifest、schema/preflight、checkpoint/idempotency contract、离线构建和 certification test kit。 |

### 需要立即修正的 maturity 偏差

- `pluginMetadata()` 不应把所有 source/sink 默认标为 `stable`；应改为 `production|beta|experimental|dev-only`，并由测试证据驱动。
- README 能力一览应增加 maturity 列或链接到本矩阵，避免用户把“已注册”理解为“生产承诺”。
- `GET /api/v2/plugins/schema`、UI 插件能力矩阵、配置文档和 README 必须使用同一事实源。
- 每个 connector 进入 Production-ready 前必须绑定至少一条 Podman e2e、一个异常路径测试和一段幂等/重放语义文档。

## 分阶段 Roadmap

### Phase 0：聚合宽表语义与可靠性底线，0-2 周

目标：先把 Kafka 聚合宽表的生产语义定义清楚，同时消除会破坏结果可信度的静默丢数和状态恢复风险。

交付项：

- 编写聚合宽表 ADR：明确支持的第一条链路、默认 at-least-once 语义、Kafka offset/checkpoint/sink commit 边界、重复数据消除建议。
- 定义宽表 pipeline spec 模板：`kafka -> envelope normalize -> lookup -> detail wide table -> window aggregate -> clickhouse`。
- 明确 transform maturity：`lookup` 可作为第一批生产候选，`join` stream-stream 与复杂 `window` 暂不承诺 production-ready。
- 将 `window_type` 中未实现的 sliding/session 从 UI/schema 的生产路径降级，避免用户误以为已经可用。
- 设计 `StateStore` v1 接口：至少覆盖 join/window/deduplicate 状态 key、TTL、snapshot、restore、metrics。
- 修复 Elasticsearch sink marshal/action 错误静默跳过问题，改为 DLQ 或显式失败，并补 item-level bulk error DLQ。
- 为 DLQ 增加稳定 ID、record hash、pipeline version、DAG node 信息；API 支持按 ID replay/delete。当前 SQL 存储与 UI/API 精确单条操作已完成，后续补 DAG node 传播覆盖更多 transform/source 失败点。
- 明确 DAG DLQ replay 策略：要么实现 node-level replay，要么 UI/API 明确禁用并提供替代恢复路径。
- 禁用生产默认请求时 `npx` 编译 fallback；提供离线预装镜像、CLI 编译或 CI 编译路径。
- 重新审计 `docs/SPEC-v2-reaudit-2026-06*.md`，把已修复项标记完成，把仍存在项转成 issue/里程碑。
- 把 `pluginMetadata()`、README、UI 插件矩阵中的 maturity 从笼统 `stable` 改为 production/beta/experimental/dev-only，并以测试证据为准。
- 增加错误路径 e2e 设计：ES marshal/bulk partial failure、DLQ ID replay、Kafka crash/rebalance、join miss、DAG 失败恢复、插件编译离线环境。

验收指标：

- 聚合宽表第一条 production candidate 链路有明确 spec、语义文档、失败模型和测试清单。
- 所有 sink 的跳过记录路径都有 DLQ/失败/显式 allow-drop 配置。
- `make test-quick` 与关键 Podman e2e 通过。
- README、SPEC、OpenAPI、配置文档、UI 插件矩阵中的能力状态一致。
- 每个 connector 都有明确 maturity、owner、缺口和进入下一等级的测试门槛。

### Phase 1：聚合宽表 MVP，2-6 周

目标：交付一条可演示、可测试、可恢复的 Kafka 实时宽表链路，以 ClickHouse 作为第一生产 sink。

当前实现进展：

- Kafka envelope normalize 已支持普通 JSON 和 Debezium-like envelope，并纳入配置 schema。
- 宽表预览 API 已提供：`POST /api/v2/wide-table/preview`，返回 envelope/lookup/window/sink 预览、样例字段类型、ClickHouse DDL 建议和 preflight 结果。
- Wide Table Wizard 已有前端预览与发布入口，可生成 Kafka -> normalize_envelope -> lookup -> tumbling window -> ClickHouse 的候选 spec，展示字段类型、标准化样例、告警和 ClickHouse DDL，并通过 `POST /api/v2/pipelines` 创建 pipeline。
- Connection Catalog 已有后端持久化、健康测试、密钥脱敏和前端页面入口，可复用 Connector Descriptor v1 生成 connector 类型提示。
- Schedule 页面已接入主导航，并已通过专用 schedule API 支持 cron/periodic/streaming/once 的保存、禁用、立即运行和运行历史查看。
- Redpanda + MySQL 维表 + ClickHouse 的宽表 Podman e2e 已加入 `hack/e2e-wide-table.sh` 和 `hack/e2e-all.sh`，覆盖明细宽表、tumbling 聚合宽表、ClickHouse 明细宽表重复消息幂等、聚合宽表重复消息不重复累计、ClickHouse schema drift 写入失败进入 DLQ、ClickHouse 容器下线时明细宽表写入失败进入 DLQ 并在恢复后 replay、lookup miss 进入 DLQ、补维表后按稳定 DLQ ID replay 写入 sink，以及 lookup 维表刷新失败进入 DLQ。
- DLQ 记录已具备稳定数据库 ID、`record_hash`、`pipeline_version`、`dag_node` 字段，API/UI 支持按 ID 精确单条 replay/delete，避免时间窗口误删/误重放。

交付项：

- Kafka envelope normalize：支持普通 JSON 和 Debezium-like envelope，标准化 `op`、`event_time`、`source_table`、业务 key。
- Stream-table lookup：支持 MySQL/PostgreSQL 维表 snapshot 加载、定时刷新、字段前缀、left join、join miss 进入 DLQ 或显式置空。
- 明细宽表构建：字段映射、字段重命名、类型转换、目标主键、版本列、delete/update CDC 处理策略。
- Tumbling 聚合宽表：按 event-time、固定窗口、业务维度输出 count/sum/avg/min/max/first/last，支持 allowed lateness，并通过 `deduplicate(id,_version)` 避免同一业务事件重复累计。
- ClickHouse 宽表落地：自动建表/DDL 预览、ReplacingMergeTree/version 策略、schema drift add-column、明细宽表重复写入幂等验证、聚合重复消息去重验证、schema drift 写入失败 DLQ 验证、ClickHouse 容器下线写入失败 DLQ 验证和恢复后 replay 验证。
- 新增 Podman e2e：Redpanda + MySQL/PostgreSQL 维表 + ClickHouse，已覆盖正常链路、ClickHouse 明细宽表重复消息幂等、聚合宽表重复消息不重复累计、ClickHouse schema drift 写入失败进入 DLQ、ClickHouse 容器下线时明细宽表写入失败进入 DLQ 并在恢复后 replay、lookup miss 进入 DLQ、补维表后按稳定 DLQ ID replay 写入 sink 和维表刷新失败进入 DLQ；后续继续补 stream-stream join miss、ClickHouse 真实超时注入、DAG/stateful replay。
- Connection Catalog：连接创建、测试、复用、密钥脱敏、连接健康状态。
- Wide Table Wizard：Kafka topic sample -> envelope 解析 -> 维表 join -> 明细字段 -> 聚合指标 -> ClickHouse DDL -> preflight -> deploy。
- Schema/sample preview：启动前展示字段、类型、样例数据、目标表 DDL/自动建表计划。
- 将 Schedule 页面接入主导航，并补齐 create/edit/enable/disable/run now/history。
- 错误分类体系：配置错误、连接错误、权限错误、schema 错误、运行时错误、下游限流、数据质量错误。
- UI 中展示 preflight 风险、DLQ 修复建议、checkpoint 恢复点和 replay 影响范围。

验收指标：

- 用户可在 UI 中完成 Kafka -> 维表 lookup -> ClickHouse 明细宽表 -> ClickHouse 聚合宽表。
- 聚合宽表 MVP 有 Podman e2e 和 Playwright UI e2e。
- crash/restart 后可以按文档边界恢复，不产生不可解释的丢数；重复写入由业务 key/version 策略吸收。

### Phase 2：状态化 join/window 与扩展平台化，6-10 周

目标：把 MVP 中的内存状态和单一窗口语义推进到可恢复、可扩展、可认证的状态化处理能力。

当前实现进展：

- `StateStore` v1 已有 `MemoryStore` 与 SQLite-backed `SQLiteStore`，覆盖 TTL、snapshot/restore、状态大小统计和过期 key 清理。
- checkpoint v1 envelope 已定义，可在同一 payload 中携带 source position、node state snapshot version、sink commit metadata 和 `at_least_once` 交付模式。
- Connector Descriptor v1 API 已提供：`GET /api/v2/connectors/descriptors`，聚合 registry、配置 schema、secret 标记、capabilities 和 maturity metadata。
- Plugin ABI v1 manifest 校验已提供，固定 ABI 为 `openetl.plugin.abi/v1`，并约束 source/read、sink/write、transform/transform 最低 entrypoint。
- `deduplicate` 已支持可选 SQLite `StateStore`，可通过 `state_backend/state_path/state_pipeline/state_node/state_ttl_seconds` 持久化去重 key，并已补重启恢复与 TTL 单测。
- `lookup` 已支持可选 SQLite `StateStore`，可通过同一组 `state_backend/state_path/state_pipeline/state_node/state_ttl_seconds` 配置持久化维表缓存，并已补缓存恢复与 TTL 单测；同时支持 `max_cache_entries` 控制维表缓存 key 上限，超限会拒绝刷新/恢复并递增指标；`on_miss: pass|null|dlq|error` 可把维表 miss 显式置空或送入 DLQ，`on_refresh_error: pass|error` 可把无可用缓存的刷新失败送入 DLQ。
- `join` 已支持可选 SQLite `StateStore`，可通过同一组 `state_backend/state_path/state_pipeline/state_node/state_ttl_seconds` 配置按 join key 持久化 interval join 缓冲，并已补重启恢复与 TTL 单测。
- `window` 已支持可选 SQLite `StateStore`，可通过同一组 `state_backend/state_path/state_pipeline/state_node/state_ttl_seconds` 配置持久化 tumbling window 聚合缓冲，并已补重启恢复与 TTL 单测。
- Linear runner 与 DAG executor 已能在 transform 启用 `state_backend` 且未显式配置命名空间时，自动注入 `state_pipeline=<pipeline name>` 与 `state_node=<transform/node id>`，减少状态命名冲突和 YAML 样板。
- Linear runner 与 DAG executor 已在 checkpoint 保存前采集实现 `StateSnapshotter` 的 transform state snapshot version，并用 checkpoint envelope 保存 `source position + state versions + at_least_once`；恢复 source 时会自动解出 legacy source position；状态快照采集失败时不会推进 source checkpoint。
- `deduplicate`、`lookup`、`join`、`window` 已实现 `StateMetricsProvider`，linear/parallel/DAG runner 会汇总基础 state keys/bytes/updated_at，并通过 `/api/v2/metrics` JSON 与 Prometheus `/metrics` 暴露。
- `deduplicate` 已实现 `TransformMetricsProvider`，linear/parallel/DAG runner 会汇总 processed/passed/duplicate_dropped/memory_duplicate_dropped/state_duplicate_dropped/evicted_keys 计数，并通过 `/api/v2/metrics` JSON 与 Prometheus `etl_transform_metric_total` 暴露。
- `lookup` 已实现 `TransformMetricsProvider`，linear/parallel/DAG runner 会汇总 processed/hit/miss/missing_key/miss_null/miss_dlq/miss_error/refresh_success/refresh_error/refresh_error_dlq/restore_success/scan_error/cache_limit_exceeded 计数，并通过 `/api/v2/metrics` JSON 与 Prometheus `etl_transform_metric_total` 暴露。
- `join` 已实现 `TransformMetricsProvider`，linear/parallel/DAG runner 会汇总 hit/miss/miss_dropped/miss_dlq/miss_error 计数，并通过 `/api/v2/metrics` JSON 与 Prometheus `etl_transform_metric_total` 暴露。
- `window` 已实现 `TransformMetricsProvider`，linear/parallel/DAG runner 会汇总 accumulated/late_dropped/emitted_records/emitted_windows/flushed_records 计数，并通过 `/api/v2/metrics` JSON 与 Prometheus `etl_transform_metric_total` 暴露。
- SQL 存储后端当前已有 SQLite/MySQL/PostgreSQL 实现，但仍存在较多重复 CRUD、migration 和方言分支，后续应收敛为统一 SQL storage engine。

SQL 抽象选型原则：

- 推荐路线：以 Go 标准库 `database/sql` 为连接与事务底座，叠加 `sqlx` 或轻量 SQL builder（如 Squirrel/goqu）减少 scan、placeholder 和动态查询样板；项目内部保留一个很薄的 `Dialect` 层处理 SQLite/MySQL/PostgreSQL 的真实差异。
- 不建议把存储层整体交给重 ORM：GORM/Bun/ent 可以减少 CRUD 样板，但会引入模型生命周期、migration、hook、方言行为和性能调优复杂度；OpenETL 的存储表偏平台元数据，手写可控 SQL + builder 更容易证明可靠性。
- `sqlc` 适合强类型固定 SQL，但跨 SQLite/MySQL/PostgreSQL 时通常仍要维护多份 query 或方言条件；可以作为局部候选，不作为统一存储抽象的默认路线。
- 配置目标是 `driver + dsn`：用户通过更换 DSN 切换 SQLite/MySQL/PostgreSQL 存储后端，但这不是“任意 SQL 数据库完全透明”。每个受支持数据库仍要通过 conformance test、migration smoke test 和方言兼容认证。

交付项：

- `StateStore` v1 落地：本地/SQL 后端、snapshot/restore、TTL、状态大小指标、状态清理。
- 统一 SQL storage engine：基于 `database/sql` + `sqlx`/SQL builder 收敛 SQLite/MySQL/PostgreSQL 存储实现，保留极薄 `Dialect` 层处理 placeholder、DDL、upsert、JSON/time 类型、锁语义和 `RETURNING` 差异。
- 存储配置收敛为 `driver + dsn`：用户通过更换数据库 driver/DSN 切换 SQLite/MySQL/PostgreSQL 后端；文档明确这不是“所有 SQL 方言完全透明”，而是由内部 dialect 适配可支持的存储语义，并由 conformance test 定义生产承诺。
- Stateful lookup/join：维表 CDC 增量更新、join/window 状态 backpressure 和 crash e2e。
- Window 语义升级：event-time watermark、allowed lateness、late side-output/DLQ、窗口状态 checkpoint。当前已完成 tumbling window 基础状态恢复、运行时状态命名空间注入，以及 linear/DAG checkpoint state version，后续补输出事务边界。
- Kafka 恢复强化：consumer group rebalance、broker restart、offset replay、重复消息与 sink 幂等组合测试。
- Connector Descriptor v1：字段 schema、必填项、默认值、secret 标记、能力、限制、成熟度、示例。
- 内置 source/sink 实现 `SchemaDescriptor` / `SchemaValidator`，UI 和 preflight 复用同一套 schema。
- Plugin ABI v1：版本协商、兼容矩阵、deprecation policy、最小运行时版本。
- Plugin/connector test kit：open/close、preflight、schema、read/write、retry、DLQ、idempotency、metrics 合约测试。
- Connector certification：MySQL、ClickHouse、Kafka、S3 先进入 production candidate；PostgreSQL、Redis、Doris、JDBC、ES 按缺口补齐后再晋级。
- 插件 registry manifest：签名、checksum、license、作者、版本、兼容性、下载源。
- 将插件编译从服务请求路径移出，提供 `openetl plugin build/test/package` 或等价脚本。

验收指标：

- Kafka 聚合宽表链路的状态可以在进程重启后恢复，恢复边界有自动化测试证明。
- 同一套 storage conformance 测试可以在 SQLite/MySQL/PostgreSQL 上通过；新增存储表或字段时只需改通用 SQL store 和少量 dialect/migration。
- 新增一个第三方 sink 不需要修改 UI 表单代码即可生成配置界面。
- 每个 production connector 都有合约测试和成熟度声明。
- 插件 SDK 升级有兼容性测试，破坏性变更可被 CI 阻断。

### Phase 3：生产运维与分布式，10-16 周

目标：把单机可用能力推进到可运维、可恢复、可扩容的生产平台。

交付项：

- 多进程/多容器 master-worker e2e：任务分配、worker crash、slot 恢复、任务重试、重复调度防护。
- Worker history 与 task history：状态转换、失败原因、重试次数、分片归属、运行耗时。
- 存储 retention/janitor：audit、run history、DLQ、task、pipeline versions 的保留策略。
- SQL storage 运维闭环：统一迁移工具、schema version、备份/恢复 runbook、跨 SQLite/MySQL/PostgreSQL 的 upgrade/downgrade smoke test。
- HA/DR 文档：数据库备份恢复、checkpoint 恢复、worker 扩缩容、版本升级、回滚。
- 资源治理：pipeline 级并发、batch、内存、水位、下游限流、backpressure 策略。
- 可观测性升级：结构化 JSON log、trace id、pipeline/node/record 关联、Prometheus dashboard 模板。

验收指标：

- Podman 环境可稳定跑 master + 2 worker + MySQL + ClickHouse 的 crash recovery e2e。
- worker 异常退出后任务可自动重新分配，且不会突破幂等性约束。
- 生产部署文档覆盖单机、standalone+SQL、master-worker 三种拓扑。
- SQLite/MySQL/PostgreSQL 三种 storage 后端的迁移、retention、备份恢复和版本升级均有 Podman smoke test。

### Phase 4：高级处理与生态，持续推进

目标：在聚合宽表 production path 稳定后，扩展到更复杂的实时处理、质量治理和生态集成。

交付项：

- 完整 window 语义：sliding、session、复杂 trigger、watermark、late data side output。
- Stateful stream-stream join：event-time interval join、多流 join、状态 TTL、miss policy、缓存后端、checkpoint 恢复。
- Schema registry：Avro/Protobuf/JSON Schema、兼容性检查、schema evolution。
- CDC envelope 标准化：Debezium-compatible envelope、before/after/op/ts/source 元数据。
- Lineage 与影响分析：pipeline、node、field-level lineage，变更影响预览。
- Data quality：规则模板、失败策略、质量报告、异常样本导出。
- Connector marketplace：成熟度、下载量、版本、认证状态、示例 pipeline。

验收指标：

- 复杂处理能力都有语义文档、样例、失败测试和资源上限测试。
- schema 变更可以在 preflight 阶段给出兼容/不兼容结论。
- 外部贡献 connector 可以通过认证流程进入 marketplace。

## 优先级建议

近期不要继续单纯扩 connector 数量。首要目标应是把 Kafka 聚合宽表做成一条 production candidate 链路，并用这条链路反向校准可靠性、元数据、状态处理、UI 向导和扩展合约。

建议排序：

1. 先定义 Kafka 聚合宽表 production candidate：输入格式、join 类型、聚合类型、sink、交付语义和不承诺边界。
2. 修所有会破坏宽表结果可信度的路径：静默丢数、join miss 不可见、late data 重复聚合、checkpoint/replay 语义不清。
3. 交付 Kafka -> lookup -> ClickHouse 明细宽表和 tumbling 聚合宽表的 Podman e2e。
4. 再统一 connector descriptor/schema/preflight，让 Wide Table Wizard、文档、API 从同一事实源生成。
5. 接着固化 StateStore、Plugin ABI/Test Kit 和 connector certification，把能力扩展到更多 sink 与复杂 join/window。

## 发布评估

当前状态适合发布一个新的 **beta / production-candidate 修复版本**，但不适合把 Kafka 聚合宽表或整个连接器矩阵宣称为 production-ready。

本轮收束结论：发布 `v0.2.0-beta1`，定位为 **wide-table beta / reliability preview**。该版本以“前端空白页修复 + Kafka 宽表 production-candidate 示例 + DLQ/状态化可靠性增强 + roadmap 成熟度收敛”为主，不把复杂流处理、完整 Kafka 恢复语义或连接器认证包装成已完成能力。

建议发布口径：

- 可以发布：前端空白页修复、Wide Table Wizard/preview、Connection Catalog、Schedule 页面入口、DLQ 稳定 ID replay/delete、状态化 transform 指标、Kafka -> lookup -> ClickHouse 明细/聚合宽表 production candidate 示例，以及宽表 Podman e2e 的异常路径覆盖。
- 可以在 release note 中标为 beta：Kafka 宽表链路、ClickHouse 宽表 sink、lookup stream-table join、tumbling window 聚合、SQLite-backed StateStore。
- 不应宣称 production-ready：任意 Kafka 多源 join、stream-stream join、复杂 window、DAG/stateful replay、Kafka rebalance/crash 恢复、ClickHouse 真实超时注入与自动恢复边界、delete/update CDC 聚合语义、connector marketplace/ABI 认证。

本轮已完成的最小检查：

- `./hack/e2e-wide-table.sh` 已通过，覆盖宽表 happy path、重复消息、schema drift、lookup miss/replay、lookup refresh failure、ClickHouse 容器下线 DLQ 和恢复后 replay。
- `./hack/e2e-ui.sh` 已通过，73 个 Playwright 断言全部成功，证明修复后的生产前端构建不会再出现已知空白页回归。
- Podman 内 `go test -timeout 120s ./internal/etl/...` 已通过。
- `docs/ROADMAP.zh.md`、README 链接和 release note 已对新增能力与 beta 边界保持一致。
- release note 已明确默认交付语义是 at-least-once，exactly-once 不承诺；推荐业务主键、版本列、deduplicate 和 sink 幂等策略吸收重复。

发布结论：可以发 `v0.2.0-beta1`，版本名称和文案使用 **wide-table beta / reliability preview**，不使用 production-ready 表述。下一版本再以 Kafka crash/rebalance、ClickHouse 真实超时注入、DAG/stateful replay 和 connector certification 作为晋级 production-ready 的发布门槛。

## 推荐跟踪指标

- 聚合宽表：端到端延迟、Kafka lag、watermark lag、join hit/miss rate、late record count、aggregate emit count、宽表重复写入消除率。
- 可靠性：异常路径 DLQ 覆盖率、replay 成功率、crash recovery e2e 通过率、每个 sink 的 silent-skip 路径数量。
- 易用性：首次成功任务耗时、wizard 完成率、preflight 拦截率、错误修复后成功率。
- 可扩展性：descriptor 覆盖率、connector 合约测试覆盖率、插件 ABI 兼容测试覆盖率。
- 生态开放：production connector 数量、外部插件数量、文档示例可运行率、issue 到 release 的闭环比例。
- 性能运维：吞吐、端到端延迟、CDC lag、worker crash 后恢复时间、DLQ 积压时间。

## 下一步可执行清单

- 建立 `wide-table-mvp`、`reliability`、`stateful-processing`、`usability`、`plugin-sdk` 五个 GitHub milestone。
- 为 Kafka 聚合宽表写 ADR：明确第一生产候选链路、默认 at-least-once、重复处理、状态恢复、DLQ/replay 边界。
- 新增 `testdata/pipes-wide-table/` 示例：Kafka 订单事件、MySQL/PostgreSQL 维表、ClickHouse 明细宽表、ClickHouse 聚合宽表。
- 新增 `hack/e2e-wide-table.sh`，默认使用 Podman 编排 Redpanda + MySQL/PostgreSQL + ClickHouse，覆盖正常链路和关键失败路径。
- 把 Phase 0 每个交付项拆成 issue，并绑定测试要求。
- 把 README connector 矩阵改为从 connector descriptor 生成，至少先手动增加 maturity 列。
- 以 Kafka -> lookup -> ClickHouse 宽表为样板，打通 Wide Table Wizard、schema preview、preflight、运行、DLQ、replay、指标的完整闭环。
- 设计 `internal/etl/storage/sqlstore`：把 checkpoint、DLQ、pipeline spec、audit、run history、plugins、connection catalog 等通用 SQL CRUD 合并到统一实现，只保留 SQLite/MySQL/PostgreSQL dialect 与 migration 差异。
- 做一轮 SQL 抽象 spike：对比 `sqlx`、Squirrel/goqu、Bun/GORM/ent、`sqlc` 在现有 storage 表上的样板量、方言逃逸点、migration 策略和 conformance test 成本，默认优先验证 `database/sql + sqlx/SQL builder + thin Dialect`。
- 所有新增验证默认使用 Podman，本地命令与现有 `hack/e2e-*.sh` 保持一致。
