# OpenETL-Go Roadmap

本文是项目当前阶段的产品和工程路线图。它围绕一个收束后的定位展开：

> OpenETL-Go 是轻量、自托管、开源的 CDC/ETL 数据同步、清洗、汇聚运行时。

详细定位见 [docs/positioning.zh.md](./positioning.zh.md)。Roadmap 的目标不是把项目做成 Flink、Airbyte、Airflow 或 Kafka Connect 的非完整替代品，而是把常见自托管数据管道做到可信、易用、可扩展、轻量。

## 当前基线

OpenETL-Go 已经具备较宽的能力面：

- 数据路径：`Source -> Transform -> Sink` 线性 pipeline，DAG 节点/边，条件路由，fanout，并行分片，定时和流式执行。
- 数据来源和目标：MySQL batch/CDC/snapshot+CDC、PostgreSQL CDC、Kafka、file、HTTP、Redis、ClickHouse、MySQL、PostgreSQL、Doris、Elasticsearch、Kafka、Redis、S3、JDBC、file sink。
- 数据处理：filter、validate、type_convert、project/select_fields、flat_map/udtf（Lua 第一版 ABI）、debezium_cdc、cdc_policy/ddl_guard、rename/drop/add field、normalize_envelope、lookup、enricher、join、deduplicate、tumbling window、router、fanout、tap、rate_limiter、Lua/JS/TS/WASM。
- 运行治理：Web UI、REST API、Connection Catalog、spec validate、connection test、transform dry-run、checkpoint、DLQ list/replay/delete、audit、metrics、health。
- 运行形态：SQLite 单机，MySQL/PostgreSQL 共享存储，master-worker 分布式调度，build-tag gated WASM/QuickJS，Lua 可裁剪。

必须持续保持的边界：

- 默认投递语义是 at-least-once，不承诺 exactly-once。
- 生产链路通过业务主键、版本列、upsert、ReplacingMergeTree、deduplicate 或显式补偿吸收重放影响。
- SQLite 是轻量单机入口，不宣称分布式保证。
- DAG、复杂 join/window、插件生态和部分连接器必须按成熟度标注，不用文案包装成 production-ready。
- 不追求完整流计算语义：任意/通用 keyed state、通用 processing-time timer、CoProcessFunction、SQL planner、Flink savepoint、复杂 sliding/session window、late side-output、retraction 不进入近期核心。
- 对 Flink SQL 类同步任务，只迁移其数据流语义：source、解析/展开、lookup 补维、投影转换、sink。不要为了兼容 SQL 语法而引入通用 SQL planner。
- 状态数据和缓存型运行时能力必须和 metadata/checkpoint 持久层分开：SQLite/MySQL/PostgreSQL 只作为 pipeline spec、checkpoint、DLQ、audit、worker/task 等持久化存储，不适合作为维表缓存、deduplicate/window/join 运行时状态等高频缓存后端；如果当前服务未配置 Redis，则相关内置 state/cache 能力必须在 spec validate/preflight 阶段禁用或报错，不能退化使用 SQLite/MySQL/PostgreSQL 充当缓存。

## 竞品边界

Roadmap 按以下差异化推进：

| 参照项目 | 它们更强的地方 | OpenETL-Go 应坚持的差异 |
| --- | --- | --- |
| Airbyte / Meltano / dlt | SaaS/数据库到数仓的 ELT 连接器目录 | 更轻，偏 CDC/实时同步、DLQ replay、幂等写入、自托管小闭环 |
| Airflow / Dagster / Prefect | 通用工作流调度 | 内置数据读写、checkpoint、DLQ、preflight；也可被这些调度器调用 |
| SeaTunnel / ChunJun | 更重的批流一体和大规模分布式执行 | 单二进制/小集群优先，部署和运维成本低 |
| Kafka Connect / Debezium | CDC connector runtime 和 Kafka 标准生态 | 一体化 transform、sink、UI/API、DLQ；不夸大 CDC 协议成熟度 |
| DataX | 稳定离线批同步 | 更实时，包含 CDC、checkpoint、DLQ、UI/API 和轻量汇聚 |
| Flink / Spark Streaming | 复杂有状态流计算 | 不替代，只覆盖轻量清洗、补维、去重和 tumbling window 聚合 |

## 当前主要缺口

### 1. 可靠性仍需证明

已有 checkpoint、DLQ、幂等建议和部分 e2e，但还需要把“不会静默丢数”变成端到端证据：

- Kafka crash/rebalance/offset replay 认证不足。
- Stateful transform 的 e2e 恢复边界还不够系统，尤其是 `Kafka offset + state snapshot + sink commit` 的关系。
- DAG DLQ replay 当前不支持，必须持续显式暴露，不让用户误以为可用。
- Elasticsearch partial bulk 已能按失败条目进入 DLQ；S3/File 重放幂等、跨 sink fanout 非原子等边界仍需要更清楚的测试和文档。
- Connector maturity 需要由测试证据驱动，而不是手写字符串。
- Doris sink 的 production gate 已关闭，但 maturity 必须持续由真实 Doris e2e、schema/model preflight、DDL 安全策略、幂等语义验证，以及文档、descriptor 和实现一致性共同约束。

### 2. 上手路径仍偏工程化

用户仍需要理解较多 YAML 和运行时细节：

- 常见任务缺少固定向导：数据库同步、Kafka 实时明细/聚合、文件/HTTP 落地。
- 启动前 schema/sample/DDL/幂等策略预览还不完整。
- 调度配置和 source 能力已经建立第一版显式绑定：source descriptor 暴露 `supported_schedules` / `default_schedule`，spec validate/preflight 拒绝不支持的 `schedule.type`，UI 按当前 source 过滤调度类型；后续需要继续把调度重跑风险和 sink 幂等性 warning 串起来。
- preflight 错误需要更明确地指向 pipeline、node、字段、风险、修复动作和是否可 replay。
- UI 应降低“必须手写 YAML”的比例，但 YAML 仍保留为高级和可审计入口。
- UI 配置页需要复用更多上下文，而不是只暴露静态表单：
  - connector descriptor、字段 schema、secret 标记、默认值、示例值和 maturity。
  - Connection Catalog 中已保存的连接、最近健康状态和权限测试结果。
  - source sample、schema introspection、topic/table/partition 列表、目标表 DDL preview。
  - transform dry-run、preflight 结果、幂等风险、DLQ/replay 策略和推荐修复动作。
  - 现有 pipeline/DAG spec、模板、Quickstart 示例和核心组件文档。
- AI/LLM 生成 DAG 只能作为辅助入口；它需要结构化上下文包，否则容易生成不可执行或越界的 DAG。

### 3. 扩展生态还没有认证闭环

项目已有 registry、WASM runtime、TypeScript SDK、插件安装和 descriptor API，但扩展生态还缺少硬约束：

- 内置 connector 和插件需要共享 descriptor、schema、preflight、metrics、DLQ 合约。
- 缺少 connector/plugin certification test kit。
- Plugin ABI v1 需要版本协商、兼容矩阵、deprecation policy 和最小运行时版本说明。
- maturity metadata 应由实现、测试和文档边界共同校验。

### 4. 插件化报文解析和一进多出 transform 需要产品化

Kafka 设备协议、行业报文、日志 envelope 等任务常见形态是：

```text
Kafka raw message
  -> parse protocol payload
  -> flat_map / UDTF 展开为 0..N 条业务记录
  -> lookup 维表补全
  -> project/type_convert
  -> Kafka/OLAP/warehouse sink
```

这属于 OpenETL-Go 的同步、清洗、汇聚主线，不属于完整流计算。具体协议解析不应硬编码进核心；GB32960 这类行业协议应作为 Lua/JS/TS/WASM 插件或外部 parser 插件承载。`flat_map` / `udtf` 已有 Lua-backed 第一版核心 ABI，可返回 0..N 条记录，核心 transform dry-run 已能预览多输出；Kafka raw protocol -> Lua `flat_map` parser -> MySQL `lookup` -> `project` / `type_convert` -> Kafka ODS 已补第一条 Docker e2e，覆盖解析失败 DLQ、维表 miss DLQ、一进多出写入和 offset replay 后 Kafka append 重复边界；Kafka sink 已补 producer 失败注入单测，runner 层已有失败进入 DLQ 且不静默推进 checkpoint 的覆盖。JS/TS transform 已支持返回 record/data 对象数组，并通过 dry-run 预览多输出；WASM/plugin transform 已补空输出、单条和数组输出 ABI，`/api/v2/plugins/dry-run` 已返回 `records` / `output_count`；后续仍需补真实 WASM e2e 验证。

需要补齐：

- `flat_map` / `udtf` transform：Lua-backed 第一版核心 ABI 已支持单条或一批输入输出 0..N 条记录、记录级 DLQ、metrics 和继承式 record lineage；JS/TS transform 已补数组返回 ABI 与 dry-run；WASM/plugin transform 已补数组返回 ABI 和 dry-run 多输出响应，后续补真实 WASM e2e。
- 脚本级输出数组约定：Lua/JS/TS/WASM plugin 已可返回多条 record，而不是只能修改当前 record；真实插件链路 e2e 仍待补齐。
- 插件化协议解析样板：Lua parser fixture、GB32960 实时上报测试夹具和 Kafka raw -> ODS Kafka e2e 已证明核心能承载一进多出报文解析、补维、投影和 replay 边界；后续仍需提供 JS/TS/WASM 示例 parser 插件和真实 WASM e2e。
- `project` / `select_fields` transform 已进入核心：对解析后宽字段做显式投影、重命名、常量填充和时间转换；后续需在 Kafka ODS / MaxCompute 链路中补 e2e 验证。

### 5. ODPS/MaxCompute 生态需要按连接器规划

阿里云 ODPS/MaxCompute 常见于维表和数仓落地。它可以进入连接器规划，但定位是 source/lookup/sink connector，不是 Flink SQL 兼容层：

- `odps_lookup` 或 `lookup` 的 ODPS/MaxCompute driver：支持按 key 维表查询或周期性快照缓存。
- `odps` / `maxcompute` sink：experimental 第一版已注册 connector descriptor、鉴权/endpoint/project/table/partition 配置、schema validator、动态分区字段校验；SDK-backed batch tunnel writer、远端表/分区/权限 preflight、失败分类、sink-local retry/backoff 和 metrics 已接入，真实 MaxCompute 环境的写入/replay/DLQ 集成证据仍待补齐。
- 首个 sink 目标链路：Kafka ODS JSON -> `project` / `type_convert` / schema validate -> MaxCompute 分区表，支持从记录字段（如 `dt`）生成目标分区。
- 若 ODPS 维表查询成本或延迟不适合流式逐条 lookup，优先推荐把 ODPS 维表镜像到 MySQL/PostgreSQL/Redis，再用现有 `lookup` 补维。

### 6. Debezium Kafka CDC 迁移需要产品化

很多团队已经在使用 Debezium 把 MySQL binlog 写入 Kafka，再用自研 FastAPI/Java/Go consumer 做 ODS 同步。这类链路是 OpenETL-Go 应优先覆盖的替代场景：

```text
MySQL -> Debezium -> Kafka -> OpenETL-Go -> MySQL/PostgreSQL/ClickHouse/Doris/ODS
```

它不需要完整流计算语义，核心是 Debezium envelope 解析、规则过滤、表名映射、幂等 upsert、成功后提交 offset、失败可见和可重放。当前 `kafka` source、`normalize_envelope`、`debezium_cdc`、`cdc_policy` / `ddl_guard`、模板化表名、Lua/TS/WASM、MySQL upsert 和 checkpoint 已覆盖主链路的配置级骨架；Debezium Kafka CDC -> MySQL ODS upsert 已补首个 fixture/e2e 入口，控制面已支持按 Kafka partition/offset 设置 replay checkpoint，e2e 脚本已覆盖 offset replay 后 upsert 吸收重复、Redpanda broker restart 后继续消费写入、同组消费者 join/leave rebalance 后继续消费、MySQL lock wait 临时错误 retry 后写入成功且不进 DLQ、MySQL 值范围写入失败进入 data-class DLQ 并在修复后 replay 补写，以及危险 DDL reject 进入 schema-class DLQ 且不落到 sink。后续还需要更多临时故障类型注入。

- `debezium_cdc` preset transform 已落地第一版：解析 Debezium `c/u/d/r`、`source.db`、`source.table`、`ts_ms`、tombstone、schema-change/DDL 事件，输出标准 `operation`、`metadata.table`、`metadata.database` 和保留原始元数据的可选字段。
- 模板化 `table_mapping` 已在 `debezium_cdc` 配置中支持 `{source_db}`、`{source_table}`、`{YYYYMMDD}`、`{YYYY-MM-DD}` 等变量，覆盖 `dl_vls_dev.vehicle_charge -> ods_dl_vls_dev__vehicle_charge` 这类 ODS 命名规则；pipeline 顶层 table_mapping 的更深整合后续继续。
- `cdc_policy` / `ddl_guard` transform 已支持配置化表达源库/表 `include_*` / `exclude_*`、`skip_delete`、`skip_snapshot`、`skip_tombstone`、`dangerous_ddl=reject|drop|pass`、`ddl_allowlist`、`ddl_denylist`，并暴露跳过/拦截计数。
- `/pipelines/{name}/checkpoint/set` 已支持 Kafka `partition` / `offset` 和 `replay_from_offsets` 请求，内部转换为 Kafka source 使用的 committed-offset checkpoint；旧 `position` 原始形态保持兼容。

这些能力必须保持在 ETL 配置层，不引入 Kafka Connect runtime 或 Debezium connector 管理系统。推荐替代路径是先保留 Debezium 和 Kafka，只替换自研 consumer；若用户希望减少组件，再评估改用内置 `mysql_cdc` 直连 binlog。

### 7. 轻量运行需要产品化

- 默认二进制包含很多内置能力，长期需要更清楚的裁剪策略。
- API-only、master-only、worker-only、headless 模式需要文档和 smoke test。
- 后端启动命令已完成第一版参数化：`--config`、本地数据/日志/插件/schema/spec 目录、HTTP/ETL API host/port、storage、TLS、API token、SQL-backed audit 开关、master/worker mode 等运行参数可通过 CLI flags 指定，并与环境变量和 `config.yaml` 保持 `CLI flags > 环境变量 > 配置文件 > 内置默认值` 的优先级。
- SQLite/MySQL/PostgreSQL storage 已存在，但升级、回滚、retention、备份恢复、worker 扩缩容 runbook 还不够系统。

## Roadmap 主线

近期只保留四条主线：

1. 可靠同步底线。
2. 首次任务闭环。
3. 扩展合约与认证。
4. 轻量运行与生产运维。

新增连接器不是主线。只有当它服务上述主线，且能带测试、preflight、schema、DLQ、metrics 和成熟度说明时，才应进入核心。

## Phase 0：定位和事实源收束，0-2 周

目标：公开口径、配置文档、UI metadata、测试证据和代码能力一致。

交付项：

- 完成产品定位文档，并在 README、Quickstart、配置参考、AGENTS 中引用。
- 建立统一 maturity 规则：`production`、`beta`、`experimental`、`dev-only`，README、UI、descriptor、配置文档使用同一事实源。
- 明确 production candidate 链路：
  - MySQL snapshot/CDC -> ClickHouse。
  - MySQL batch/CDC -> MySQL/PostgreSQL。
  - Debezium Kafka CDC -> `debezium_cdc` / `cdc_policy` / `table_mapping` -> MySQL ODS upsert。
  - Kafka JSON/Debezium -> lookup -> deduplicate -> tumbling window -> ClickHouse。
  - Kafka raw message -> parser plugin/flat_map -> lookup -> project -> Kafka ODS。
  - Kafka ODS JSON -> project/type_convert -> MaxCompute partitioned table。
  - file/HTTP -> file/S3。
- 把 DAG DLQ replay 当前不支持的行为保持在 API/UI/文档中显式可见。
- 建立“失败记录必须可见”规则：失败只能进入 DLQ、返回错误触发 retry，或由显式 allow-drop 配置并计数审计。

验收指标：

- README、Quickstart、配置参考、Roadmap 和 `GET /api/v2/connectors/descriptors` 的定位和成熟度口径不冲突。
- 文档不再暗示 OpenETL-Go 是 Flink/Airbyte/Airflow/Debezium 的全量替代。
- `make test-quick` 和相关 server/schema/DLQ 单测通过。

## Phase 1：可信同步与轻量汇聚 MVP，2-6 周

目标：把最核心的同步、清洗、汇聚链路做成可推荐的 production candidate。

交付项：

- 高优先级增补（置顶 backlog，2026-06-29）：
  1. ODPS / MaxCompute Sink 完成实现：
     - 现状：`odps` / `maxcompute` 已有 experimental descriptor、配置 schema、partition validator、SDK-backed batch writer、远端权限/表/分区 preflight、错误分类、sink-local retry/backoff 和 metrics；真实 MaxCompute 环境下的 DLQ/replay/e2e 证据尚未补齐。
     - 目标：优先完成 sink 写入闭环，覆盖 Kafka ODS JSON -> `project` / `type_convert` -> MaxCompute 分区表。
     - 验收：SDK-backed batch writer、schema/partition preflight、权限错误分类、retry/backoff、DLQ/replay、metrics、at-least-once 重放边界文档，以及真实或可替代集成环境 e2e；未有真实写入证据前 maturity 保持 experimental/beta，不提升 production。
  2. 异步 I/O 维表查询增强：
     - 现状：`enricher` 和 `lookup` 支持 HTTP/SQL 查询，但缺少并发控制、队列上限、超时、背压和缓存失效策略。
     - 目标：补齐并发度、in-flight 上限、超时、重试、失败分类、背压和 metrics；缓存能力必须显式依赖 Redis，未配置 Redis 时只能使用无缓存查询或直接阻断要求缓存的配置。
     - 验收：配置字段、默认安全值、preflight 校验、lookup miss / timeout / 429 / SQL 临时错误 e2e、缓存命中率和背压指标；SQLite/MySQL/PostgreSQL 不作为维表缓存后端。
- 首要任务：Doris sink production-ready gate 已关闭。证据和边界：
  - 已补 `hack/e2e-doris.sh` 并纳入 `hack/e2e-all.sh`：使用 Podman 启动官方 Doris FE/BE 2.1.11 镜像，已实跑通过 MySQL batch -> Doris 的 Stream Load JSON、Stream Load CSV、MySQL 协议 insert fallback、auto-create Unique Key、decimal 类型推断和零失败记录断言。
  - 已实现 Doris `SchemaValidator` 和 preflight 接入：校验目标表存在、字段缺失、类型兼容、`pk_columns` 与 Doris Unique Key 模型一致；接入已有非 Unique Key 表时，不允许把 `batch_mode=upsert` 宣称为幂等。
  - 已修正 DDL 策略：Doris 默认 `reject`，支持明确的 `reject` / `ignore` / `apply` 语义；`apply` 仅允许安全的 `ALTER TABLE ... ADD COLUMN` 子集，不 raw apply 任意源端 DDL。
  - 已修复 auto-create / schema drift 类型推断：`ensureTablesAndColumns` 传入代表性 field values，避免首跑建表退化为列名/默认 STRING 推断；文档中的 Doris DDL 配置口径已和实际建表策略对齐。
  - 已明确 Doris 幂等边界：production CDC/upsert 只支持 Doris Unique Key 表和稳定业务主键；DELETE 使用 MySQL 协议，混合 write/delete 批次默认拒绝，除非显式设置 `allow_mixed_cdc_non_atomic`。
  - 已统一配置契约：修正 `port` / `http_port` 文档、API schema、UI descriptor 和示例 YAML，移除未实现的 `mysql_port` 口径；maturity metadata 已提升为 `production`，对外口径限定为已验证的 Doris Unique Key/upsert、Stream Load/insert fallback、schema/preflight/DDL 安全边界。
  - 已补 observability 和错误分类：Stream Load HTTP 5xx/429/timeout 按 transient，auth/schema/data 类错误进入对应 DLQ 分类；Doris sink 暴露 `SinkMetricsProvider`。
  - 本轮验证：`E2E_SKIP_BUILD=1 ./hack/e2e-doris.sh` 通过；`go test ./internal/cmd ./internal/etl/server ./internal/etl/sink` 通过。
  - 持续认证项不再阻塞 production gate，但需要继续补强：MySQL CDC/snapshot+CDC -> Doris、checkpoint restart/reset replay、DLQ/replay、schema drift add-columns、Doris FE/BE outage/recover 的扩展 e2e。
- 建立 source 与调度类型的第一版绑定规则：
  - 已落地每个内置 source descriptor 的 `supported_schedules` 和 `default_schedule`，第一版只使用这两个字段，不引入额外运行分类。
  - `mysql_cdc`、`postgres_cdc`、`mysql_snapshot_cdc`、`kafka` 默认只允许 `streaming`；`mysql_batch`、`file`、`http` 默认允许 `once` / `cron` / `periodic` / `dependency`；`redis` 按现有模式保守声明为 `once`。
  - `spec validate` 和 preflight 已拒绝不在 `supported_schedules` 内的 `schedule.type`；缺省 schedule 会按 source 的 `default_schedule` 回填，并校验 `cron`、`periodic`、`dependency` 的必填字段。
  - UI 创建/编辑 pipeline 时已按当前 source 支持的 schedule 类型过滤选项，并在切换 source 后重新校验已有 schedule；多 source DAG 使用支持类型交集。
  - 调度校验仍需结合 sink 幂等性给出重跑风险 warning，但第一版不把 sink 风险混入 source capability 字段。
- 为三条主路径补齐 e2e：
  - MySQL snapshot/CDC -> ClickHouse，覆盖 schema drift、重启恢复、重复吸收、DLQ/replay。MySQL snapshot+CDC -> ClickHouse 已补 Docker e2e，覆盖 auto-create、DDL/schema drift add-column、CDC update/delete/insert、checkpoint restart、checkpoint reset replay 后 ReplacingMergeTree 吸收重复，以及 ClickHouse 下线 DLQ/replay。
  - MySQL batch/CDC -> MySQL/PostgreSQL，覆盖 upsert/delete、事务批写、checkpoint 恢复。MySQL batch custom-query/JOIN -> PostgreSQL upsert 已补 Docker e2e，覆盖 schema preflight 拦截和 checkpoint reset replay 后 upsert 吸收重复；MySQL CDC -> PostgreSQL 已补 Docker e2e，覆盖 insert/update/delete 和 stop/restart checkpoint 恢复。
  - Kafka JSON/Debezium -> lookup -> deduplicate -> tumbling window -> ClickHouse，覆盖重复消息、lookup miss、状态恢复、ClickHouse 下线和 replay。wide-table Docker e2e 迁移为 Redis StateStore 路径后应继续覆盖重复吸收、lookup miss DLQ/replay、lookup refresh failure、ClickHouse 下线 DLQ/replay，以及 SIGKILL 后 deduplicate/window 恢复；lookup StateStore 独立 Docker e2e 应覆盖维表查询不可用后从 Redis cache 恢复。
- 增加 Kafka 报文解析到 ODS Kafka 的生产候选链路：
  - Kafka raw message -> parser plugin + `flat_map` / `udtf` -> lookup 维表 -> `project` / `type_convert` -> Kafka JSON sink（Lua parser fixture 和 Docker e2e 已覆盖第一版链路）。
  - 第一版维表优先使用 MySQL/PostgreSQL 作为查询源，缓存和状态恢复只允许 Redis；ODPS/MaxCompute 作为后续 connector 增强。
  - 覆盖解析失败进入 DLQ、维表 miss 策略、解析一进多出、Kafka sink 写入失败、offset replay 后幂等/重复边界（当前 e2e 已覆盖解析失败、lookup miss、一进多出和 Kafka append 重复边界；Kafka sink producer 失败注入已由单测覆盖，runner 层已有 DLQ/checkpoint 语义覆盖）。
- 增加 Debezium Kafka CDC 到 ODS MySQL 的生产候选链路：
  - Kafka Debezium topic -> `debezium_cdc` -> `cdc_policy` -> 模板化 `table_mapping` -> MySQL `batch_mode=upsert`（核心 transform、fixture 和 e2e 脚本已补，且覆盖 MySQL schema 写入失败进入 DLQ、修复后 replay）。
  - 覆盖 include/exclude、跳过 DELETE、跳过 snapshot `op=r`、跳过 tombstone、危险 DDL drop/reject policy、目标表自动建表或 schema drift、offset replay、broker restart、consumer group rebalance、MySQL lock wait retry（checkpoint set 已支持 Kafka partition/offset，Debezium MySQL e2e 脚本已覆盖 replay 后 upsert 去重、Redpanda 重启后继续消费、同组消费者 join/leave 后继续消费和锁等待重试后写入成功）；更多临时故障类型仍待扩展。
  - 控制面覆盖 start/stop/pause/resume、checkpoint set/reset、按 Kafka partition/offset 回放和 DLQ replay（checkpoint set + start/stop + offset replay 已进入 e2e；DLQ replay 已通过 MySQL schema 失败注入进入 Debezium e2e）。
  - 明确迁移边界：不管理 Debezium connector，不承诺 Kafka transaction exactly-once，不自动执行危险 DDL。
- 增加 Kafka ODS 到 MaxCompute 的生产候选链路：
  - Kafka ODS JSON -> `project` / `select_fields` -> `type_convert` -> `odps` / `maxcompute` sink（experimental sink 合约和 SDK-backed writer 已落地；真实环境 e2e 仍待凭据和测试表验证）。
  - 支持 MaxCompute 表字段映射、`STRING` / `BIGINT` / `DOUBLE` / `TIMESTAMP` 等基础类型转换、按 `dt` 等字段写分区（配置 schema、validator 和 SDK tunnel writer 已覆盖）。
  - preflight 校验目标 project/table/partition 权限、字段缺失、类型兼容和分区字段存在性（本地字段/分区合约与远端表加载已覆盖；真实权限矩阵待集成环境验证）。
  - 明确 at-least-once 重放边界：默认 append 可能重复；推荐事件唯一键、分区 staging + merge/overwrite 或 sink 侧可证明的幂等提交策略。
- Kafka 链路增加 consumer crash、broker restart、consumer group rebalance、offset replay 测试。
- StateStore 恢复扩展到 e2e，明确 lookup、deduplicate、window 的恢复边界；lookup、deduplicate、window 均已补 Docker e2e 恢复证据。
- Elasticsearch item-level bulk DLQ 已接入 runner/sink；mapping conflict Docker e2e 已覆盖好记录写入、失败记录进入 schema DLQ、修复 mapping 后按 ID replay。
- S3/File 只补回填所需 deterministic prefix/manifest，不承诺通用 exactly-once 文件输出；当前 S3/File sink 已使用 content-addressed key 吸收相同批次 replay，S3 MinIO e2e 已覆盖 checkpoint reset 后同一对象 key 不重复，first-class manifest 文件仍待补。
- Preflight 输出风险等级、字段级错误、幂等建议和目标 DDL 预览；当前已补 sink reachability 真实 `Open` 检查、warning 不阻断创建、结构化 `field_issues` 和关系型/MaxCompute `ddl_preview` 的单测证据；幂等建议与更广泛 connector DDL 预览仍需继续完善。
- `flat_map` / `udtf` transform 已进入核心 transform 集合，Lua 第一版 ABI 支持返回多条记录，并暴露 `input_records`、`output_records`、`dropped_records`、`parse_errors` 等 metrics；JS/TS transform 已支持返回 record/data 数组并可通过 dry-run 预览；WASM/plugin transform 已支持通用数组返回 ABI，插件 dry-run API 已返回多输出；GB32960 fixture 已覆盖 Lua parser 样板，真实 WASM e2e 后续补齐。
- `project` / `select_fields` transform 已进入核心 transform 集合，用于宽字段投影、常量字段、字段别名和基础时间转换；后续补 Kafka ODS / MaxCompute e2e。

验收指标：

- 主路径 e2e 覆盖 happy path、重复、失败、DLQ/replay、重启恢复。
- 失败路径不会静默推进 checkpoint。
- 文档清楚说明 at-least-once、重复吸收策略和不承诺边界。

## Phase 2：首次任务闭环，6-10 周

目标：让新用户不用先读完整 YAML schema，也能完成常见任务。

交付项：

- UI 配置体验先解决“上下文不足”和“表单不可解释”：
  - 每个 source/sink/transform 表单由 connector descriptor、schema、secret 标记、默认值、示例、maturity 和文档链接驱动。
  - 选择连接后自动带出已保存连接、最近健康状态、可见 database/table/topic/partition、权限检查结果和推荐 batch/checkpoint 参数。
  - 配置时提供 sample preview、schema preview、目标 DDL preview、transform dry-run、preflight 风险和幂等建议。
  - 对危险选项给出明确后果：如 CDC + append-only sink、跳过 DELETE、自动 apply DDL、MaxCompute append 重放重复。
  - YAML/DAG 编辑器保留，但要和表单双向同步；高级字段可折叠，不隐藏最终 spec。
- UI 只做少量高频任务向导：
  - 数据库同步：source -> table/schema preview -> sink -> idempotency -> preflight -> start。
  - Kafka 实时明细/聚合：topic sample -> envelope -> lookup -> deduplicate/window -> ClickHouse DDL -> start。
  - Debezium CDC 同步：topic sample -> Debezium envelope -> cdc policy -> table mapping -> MySQL/ClickHouse/Doris sink -> preflight -> start。
  - Kafka 报文解析：topic sample -> parser/flat_map dry-run（核心 API 已支持多输出 records）-> lookup -> field projection -> Kafka/OLAP sink -> start。
  - 文件/HTTP 落地：input sample -> transform dry-run -> file/S3 output -> manifest/idempotency 提示。
- 向导生成普通 pipeline/DAG spec，不引入专用执行路径。
- Schema/sample/DDL preview 使用真实 connector descriptor、source/sink introspection 和 preflight，不依赖静态表单猜测。
- 错误体验标准化：什么失败、在哪个 pipeline/node/字段失败、原因、修复动作、是否可 replay。
- Quickstart 保持 5 分钟内可跑通，并覆盖 UI 创建任务路径。
- AI/LLM DAG 辅助入口只生成普通 DAG spec，必须使用同一套 validate/preflight：
  - 构建 `AI context pack`：核心组件 Markdown、connector descriptor、transform 使用方法、DAG 节点/边语义、示例 pipeline、常见错误、产品边界和 maturity。
  - 为每个内置 source/sink/transform 补一页简洁组件文档：用途、配置字段、输入输出 record 形态、幂等/重放边界、示例 YAML、适用/不适用场景。
  - AI 生成结果必须展示 diff、风险解释、缺失字段、需要用户确认的 secret/权限/危险 DDL，不允许直接绕过 preflight 启动。
  - AI 上下文从文档和 descriptor 自动生成或校验，避免代码能力、UI 表单和 LLM 知识漂移。

验收指标：

- 新用户可在 UI 完成三条任务之一，且无需手写 YAML。
- Playwright e2e 覆盖向导创建、preflight 失败、修复后启动、DLQ 查看与 replay。
- 同一 pipeline 可在 UI 与 YAML 间往返，不产生隐藏配置。
- AI 生成的 DAG 在无 secret 的前提下能通过 spec validate；失败时能指出具体 node/字段/修复动作。

当前证据（2026-06-27）：

- `web/src/main.tsx` 已补 `FirstTaskWizard`：覆盖数据库同步、Kafka 明细/聚合、Debezium CDC、Kafka 报文解析、文件/HTTP 落地五类固定模板；向导生成普通 linear pipeline spec，不引入专用执行路径。
- Source/sink/transform 配置区已复用 descriptor/schema 驱动的 `ConfigForm`，带默认值、示例、secret 标记、maturity/capability 摘要；保留 Advanced JSON、transform JSON 和 Generated YAML，支持 YAML -> 表单同步。
- 向导同屏接入 sample record、transform dry-run、spec validate/preflight、DDL preview、field issue/remediation 和 `/api/v2/docs` 入口；MaxCompute remote preflight 用于 endpoint/project/table/partition/权限失败修复路径验证。
- `web/src/DagEditorPage.tsx` 已支持 DAG/YAML 与 canvas/form 往返：YAML drawer 可编辑并同步回节点/表单；DAG Editor 内置 Validate + preflight 结果面板，创建前阻断 `valid:false`，并展示错误、warning、preflight issue、field issue 和 remediation。
- `docs/quickstart.md` / `docs/quickstart.zh.md` 已把 Web UI Wizard 放到首选路径，覆盖 dry-run、Validate + preflight、修复、Create and start，以及 YAML 可见/可同步。
- `hack/e2e-ui.sh` 已覆盖：五类向导入口可见、schema-driven 表单、docs 入口、preflight 失败、修复后创建启动、向导 YAML -> 表单同步、DAG YAML -> canvas/form 同步、DAG validation 错误定位、DLQ 查看与 replay。验证命令：`E2E_SKIP_BUILD=1 ./hack/e2e-ui.sh`，结果 `88 passed, 0 failed`。

连接上下文闭环证据（2026-06-29）：

- `/api/v2/connections/{name}/context`：返回保存连接、connector descriptor、推荐 `schedule.type` / `batch_size` / `checkpoint_interval_sec`，以及尽力而为的 source/sink introspection。
- Source introspection 第一版覆盖：file/HTTP/demo sample 与 schema 推断、MySQL/PostgreSQL database/table/column/primary key 元数据、Kafka topic/partition 元数据；sink introspection 已补 MySQL/PostgreSQL/ClickHouse/Doris 目标表 schema、Kafka topic/partition 元数据、Elasticsearch/OpenSearch index mapping，以及 File/S3/local-fallback 输出 target、prefix、format、可写/bucket 存在性提示；真实启动拦截仍走 spec validate 与 preflight。
- `web/src/main.tsx` 向导已接入保存连接选择：source/sink 可选择 Connection Catalog，生成 YAML 使用普通 `connection` 引用，展示最近健康状态、schema/sample/topic/table/target 上下文和推荐参数；选择 saved connection 时默认清空旧 inline config，推荐 batch/checkpoint 会进入生成 spec，`source.config.*` / `sink.config.*` 类推荐可一键应用到表单/YAML。
- `web/src/DagEditorPage.tsx` 节点属性已复用保存连接 context，DAG/YAML 继续使用普通 `connection` 字段，不引入专用执行路径；选择 saved connection 时清空旧节点 config，节点级 `source.config.*` / `sink.config.*` 推荐可直接应用为当前节点的 inline override。
- `docs/etl-api.md` / `docs/etl-api.zh.md` / `docs/openapi.yaml` 已补保存连接上下文接口。
- 本轮已验证：`go test ./internal/etl/server -count=1`、`npm run build`、`./hack/pack.sh`、`./hack/e2e-ui.sh`，UI e2e 结果 `92 passed, 0 failed`。

剩余缺口：

- 复杂 transform chain 的增删、排序和跨 transform 错误定位仍需从 JSON 辅助编辑继续产品化。
- Connector/plugin certification test kit、真实 WASM e2e、更多生产候选链路的故障注入证据仍需继续补齐。

### 已交付：v0.2.5，组件文档与 AI context pack

目标：把已经落地的 connector descriptor、schema、dry-run、preflight 和示例 pipeline 收束成可复用事实源，让 UI、静态文档和 AI 辅助生成 DAG 使用同一套组件知识。

范围：

- 为 production candidate 主路径涉及的核心组件补第一批 Markdown 组件文档：`mysql_batch`、`mysql_cdc`、`mysql_snapshot_cdc`、`kafka`、`file`、`http`、`clickhouse`、`mysql`、`postgres`、`kafka` sink、`s3`/`file_sink`、`lookup`、`deduplicate`、`window`、`flat_map`/`udtf`、`project`/`select_fields`、`type_convert`、`debezium_cdc`、`cdc_policy`。
- 每个组件文档必须包含：用途、配置字段、输入/输出 record 形态、checkpoint/DLQ/幂等边界、适用/不适用场景、最小 YAML 示例、相关 e2e 或单测证据。
- 建立 AI context pack 生成入口，把定位边界、DAG 语义、组件文档、connector descriptors、transform dry-run 约定、常见错误和成熟度说明打包成稳定 Markdown/JSON 产物。
- AI 生成 DAG 仍只输出普通 pipeline/DAG spec；生成结果必须展示 diff、缺失字段、风险和需要用户确认的 secret/权限/危险 DDL，并走现有 validate/preflight。
- 增加文档/descriptor 一致性检查的第一版脚本或单测，至少校验组件文档覆盖到 descriptor 中的核心 source/sink/transform 名称，避免 UI、文档和 LLM context 漂移。
- 继续把复杂 transform chain 的 UI 产品化拆成后续任务；本迭代只补文档事实源、context pack 和最小校验，不引入新的 transform 执行语义。

明确不做：

- 不新增通用 SQL planner、Flink SQL 兼容层或 AI 直启 pipeline 的专用执行路径。
- 不把 MaxCompute、WASM plugin e2e、connector certification test kit 扩大进本迭代主范围；这些继续留在 Phase 1/Phase 3 对应条目。
- 不把未有真实 e2e 证据的 connector maturity 提升为 production。

验收指标：

- `docs/components/` 或等价目录中存在第一批核心组件文档，字段、maturity 和示例与 descriptor/API 文档不冲突。
- AI context pack 可以从仓库内容生成或校验，并包含产品边界、组件清单、DAG 规则、示例 spec、常见错误和成熟度信息。
- 至少补一个后端或脚本级测试，验证核心组件文档覆盖率和 context pack 产物结构。
- Quickstart 或 API 文档给出 AI/DAG 辅助入口的边界说明：AI 只辅助生成普通 spec，不能绕过 validate/preflight 和人工确认。
- `go test ./internal/etl/server` 以及新增文档/context pack 校验通过；如涉及 UI 展示，需同步运行 `npm run build` 和 `./hack/e2e-ui.sh`。

当前证据（2026-06-29）：

- 已新增 `internal/etl/server/ai_context.go`：AI context pack 从 connector descriptor、plugin schema、maturity metadata、组件文档、产品边界、DAG 规则、示例和常见错误生成；`/api/v2/ai/context` 暴露该事实源。
- `/api/v2/ai/generate` 已改为使用 context pack system prompt，不再使用硬编码 “Flink-like” 口径；响应包含 `context_pack_version`、`validation` 和 `review`，其中 review 覆盖缺失字段、secret 确认、非 production maturity、CDC -> append sink、MaxCompute remote preflight/experimental maturity、DDL apply、脚本 transform 和 DLQ disabled 等风险。
- `web/src/DagEditorPage.tsx` 已将 AI 生成改为“审阅后应用”：展示 validation、缺失字段、风险、确认项、当前 YAML 与生成 YAML，用户点击 Apply 后才写入 canvas。
- `web/src/main.tsx` 首次任务向导的 transform chain 已支持增删、排序、切换 transform 类型和逐阶段 dry-run，仍然生成普通 `transforms` 数组。
- `docs/components/` 已补第一批核心组件文档，覆盖 MySQL/Kafka/File/HTTP sources，ClickHouse/MySQL/PostgreSQL/Doris/Kafka/S3/File sinks，以及 lookup/deduplicate/window/flat_map/udtf/project/select_fields/type_convert/debezium_cdc/cdc_policy。
- `internal/etl/server/ai_context_test.go` 已补 context pack、组件文档覆盖和 AI 风险审阅测试；API/OpenAPI/Quickstart 和中英文 changelog 已同步。
- 已验证：`npm --prefix web run build` 通过，生成新的 `resource/public`。
- 已验证：临时 Go toolchain 执行 `go test ./internal/etl/server ./internal/etl/transform -count=1` 通过；Podman 容器路径执行 `go test ./internal/etl/server ./internal/etl/transform -count=1` 通过。
- 已验证：`./hack/e2e-ui.sh` 通过，结果 `92 passed, 0 failed`。
- 已验证：`./hack/pack.sh` 通过，`internal/packed/packed.go` 已重新打包当前 UI 资源。

## Phase 3：扩展合约与认证，10-14 周

目标：把“开放”和“可扩展”做成可维护机制。

交付项：

- Connector/plugin certification test kit：
  - open/close。
  - schema descriptor/validator。
  - preflight。
  - read/write。
  - retry/backoff。
  - DLQ。
  - idempotency。
  - metrics。
  - resource limit。
- Plugin ABI v1 文档和兼容性测试：manifest、entrypoint、host function、版本协商、deprecation policy。
- 插件构建移出服务请求主路径，提供 CLI/CI/离线镜像；服务端 compile 只保留受控开发模式。
- Descriptor 由代码、schema 和测试证据共同生成或校验，阻止 metadata 与实现漂移。
- 第三方 connector 能在不改 UI 代码的情况下提供表单、preflight、metrics 和 DLQ 行为。
- 核心组件文档进入扩展合约：
  - 每个 connector/transform 的 Markdown 文档必须能被 UI、AI context pack 和静态文档复用。
  - 文档字段、descriptor 字段、schema validator 和 dry-run 示例需要一致性校验。
  - 新增组件如果缺少配置说明、输入输出 record 约定、错误分类和幂等边界，不能标注为 production。
- ODPS/MaxCompute connector 认证与扩展：
  - sink 写入闭环已提升到 Phase 1 高优先级增补；Phase 3 继续承担 certification test kit、maturity 证据、插件/connector 合约和后续 lookup/source 方向的认证。
  - connector descriptor、鉴权、endpoint/project/table/partition 配置、schema/partition validator、SDK batch writer、远端表加载和错误分类已定义；真实 MaxCompute 写入/replay/DLQ 证据待补。
  - 优先实现 `odps` / `maxcompute` sink，满足 Kafka ODS 落 MaxCompute 分区表；随后实现 `odps_lookup` 或 lookup driver，满足维表补全。
  - sink 必须接入 DLQ、retry/backoff、schema validation、partition preflight、metrics 和重放语义文档。
  - 需要专门的集成测试策略；没有真实环境时只能标注为 `experimental`。

验收指标：

- production connector 必须通过 certification test kit。
- 插件 ABI 有兼容性测试和最小运行时版本说明。
- 新 connector 的 maturity 由测试证据驱动。

## Phase 4：轻量运行与生产运维，14-18 周

目标：降低部署、升级、回滚和日常运维成本。

交付项：

- 文档化 API-only/headless、standalone、master-only、worker-only 运行形态，并补 smoke test。
- 后端启动命令参数化已完成第一版：
  - 已支持 `--config` 指定配置文件路径，`--data-dir` / `--log-dir` / `--plugins-dir` / `--schemas-dir` / `--specs-dir` 指定本地存储、日志、插件、schema registry 和 pipeline spec 目录。
  - 已支持 `--host` / `--port` / `--etl-api-host` / `--etl-api-port` 等绑定参数，覆盖本地开发、容器、内网部署和反向代理场景。
  - 已支持 storage、TLS、API token、SQL-backed audit 开关、worker/master mode 等关键运行参数通过 CLI flags 指定，并与环境变量、`config.yaml` 字段对齐。
  - 已明确配置优先级：CLI flag > 环境变量 > 配置文件 > 内置默认值；启动日志输出最终生效的非敏感配置摘要。
  - 已支持 `--help` / `-h` 查看使用手册，包含参数说明、对应环境变量、示例命令和敏感字段提示。
  - 已补单测和 smoke test：`go test ./internal/cmd ./internal/etl/server ./internal/etl/sink`、`go run . --help`、非法 `--role` 启动前失败检查。
- 评估 source/sink build tags，优先裁剪重依赖或低频连接器。
- SQL storage 收敛：保留 SQLite/MySQL/PostgreSQL，减少重复 CRUD/migration，补升级/回滚 smoke test。
- 生产 runbook：
  - 备份恢复。
  - retention。
  - DLQ 积压处理。
  - worker 扩缩容。
  - 版本升级和回滚。
  - 指标面板。
- 建立最小资源基线：镜像大小、启动耗时、空闲内存、典型吞吐和 checkpoint 延迟。

验收指标：

- 最小部署、单机部署、master-worker 部署都有明确命令和 smoke test。
- 后端 CLI flags、环境变量和配置文件字段保持一致，`--help` 能作为可执行使用手册。
- 升级/回滚路径有文档和自动化验证。
- 运维指标能定位 source lag、sink latency、DLQ 积压、checkpoint age、worker health。

## 明确暂缓或不做

以下内容不进入近期主线：

- 为了数量新增大量数据库、消息队列或 SaaS connector。
- Kafka exactly-once transactions。
- 跨 sink 原子 fanout。
- 完整流处理引擎：不提供任意 keyed state、通用 processing-time/event-time timer、CoProcessFunction、多流状态计算。
- Flink/Spark 兼容 savepoint。
- 通用 SQL planner 或 Flink SQL 迁移层。
- 但支持把 Flink SQL 中的数据流拆成 pipeline：Kafka source、UDTF/flat_map、lookup、project、sink。
- 复杂 trigger、late side-output、retraction/update/delete 聚合语义；sliding/session window 不进入近期核心。
- Connector marketplace、插件商店、下载量和评分系统。
- Kubernetes operator、etcd/Zookeeper 等新的基础设施依赖。
- AI 自动生成 pipeline 作为核心路径。它可以作为辅助入口，但生成结果必须走同一套 validate/preflight。

## 推荐跟踪指标

- 可靠性：失败记录可见率、DLQ 写入失败次数、DLQ replay 成功率、crash/rebalance e2e 通过率、checkpoint 恢复耗时。
- 易用性：首次成功任务耗时、向导完成率、preflight 拦截率、错误修复后成功率。
- 数据处理：Kafka lag、CDC lag、lookup hit/miss、late record count、window emit count、重复吸收率。
- 扩展性：descriptor 覆盖率、schema validator 覆盖率、connector certification 通过率、插件 ABI 兼容测试通过率。
- 轻量性：默认镜像大小、最小二进制大小、启动耗时、空闲内存、外部依赖数量。

## 下一步清单

1. 补 `odps` / `maxcompute` 真实环境证据：用真实 MaxCompute 凭据跑 Kafka ODS -> `project` / `type_convert` -> MaxCompute 分区表 e2e，补 DLQ/replay、checkpoint reset/restart、权限失败和操作文档；没有真实写入证据前保持 experimental。
2. 完成异步 I/O 维表查询增强：为 `lookup` / `enricher` 补并发控制、in-flight 上限、超时、背压、失败分类和 metrics；需要缓存时只允许 Redis，未配置 Redis 必须 validate/preflight 阻断。
3. 补 production candidate 链路的失败注入：优先覆盖 Kafka crash/rebalance/offset replay、Debezium 临时故障、ClickHouse/Doris/ES/S3 目标故障、DLQ replay 和 checkpoint 恢复边界。
4. 扩展 schema/preflight 覆盖面：优先为 production candidate source/sink 补 `SchemaDescriptor` / `SchemaValidator`、DDL preview 和字段级 remediation，驱动 UI 表单和 preflight。
5. 建立 connector certification test kit 第一版：先认证 MySQL、ClickHouse、Kafka、S3/File，maturity 必须由测试证据、descriptor、文档和实现共同约束。
6. 补轻量运行 smoke：容器入口命令、standalone/master/worker 三形态启动、非法参数失败输出，以及升级/回滚/备份恢复 runbook 的最小自动化验证。
