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
- 不追求完整流计算语义：任意 keyed state、processing-time timer、CoProcessFunction、SQL planner、Flink savepoint、复杂 sliding/session window、late side-output、retraction 不进入近期核心。
- 对 Flink SQL 类同步任务，只迁移其数据流语义：source、解析/展开、lookup 补维、投影转换、sink。不要为了兼容 SQL 语法而引入通用 SQL planner。

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

### 2. 上手路径仍偏工程化

用户仍需要理解较多 YAML 和运行时细节：

- 常见任务缺少固定向导：数据库同步、Kafka 实时明细/聚合、文件/HTTP 落地。
- 启动前 schema/sample/DDL/幂等策略预览还不完整。
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
- `odps` / `maxcompute` sink：experimental 第一版已注册 connector descriptor、鉴权/endpoint/project/table/partition 配置、schema validator、动态分区字段校验和 writer-disabled preflight；真实批量写入、权限探测、失败重试、DLQ 和集成测试仍待 SDK client 接入。
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

- 为三条主路径补齐 e2e：
  - MySQL snapshot/CDC -> ClickHouse，覆盖 schema drift、重启恢复、重复吸收、DLQ/replay。
  - MySQL batch/CDC -> MySQL/PostgreSQL，覆盖 upsert/delete、事务批写、checkpoint 恢复。
  - Kafka JSON/Debezium -> lookup -> deduplicate -> tumbling window -> ClickHouse，覆盖重复消息、lookup miss、状态恢复、ClickHouse 下线和 replay。
- 增加 Kafka 报文解析到 ODS Kafka 的生产候选链路：
  - Kafka raw message -> parser plugin + `flat_map` / `udtf` -> lookup 维表 -> `project` / `type_convert` -> Kafka JSON sink（Lua parser fixture 和 Docker e2e 已覆盖第一版链路）。
  - 第一版维表优先使用 MySQL/PostgreSQL 缓存；ODPS/MaxCompute 作为后续 connector 增强。
  - 覆盖解析失败进入 DLQ、维表 miss 策略、解析一进多出、Kafka sink 写入失败、offset replay 后幂等/重复边界（当前 e2e 已覆盖解析失败、lookup miss、一进多出和 Kafka append 重复边界；Kafka sink producer 失败注入已由单测覆盖，runner 层已有 DLQ/checkpoint 语义覆盖）。
- 增加 Debezium Kafka CDC 到 ODS MySQL 的生产候选链路：
  - Kafka Debezium topic -> `debezium_cdc` -> `cdc_policy` -> 模板化 `table_mapping` -> MySQL `batch_mode=upsert`（核心 transform、fixture 和 e2e 脚本已补，且覆盖 MySQL schema 写入失败进入 DLQ、修复后 replay）。
  - 覆盖 include/exclude、跳过 DELETE、跳过 snapshot `op=r`、跳过 tombstone、危险 DDL drop/reject policy、目标表自动建表或 schema drift、offset replay、broker restart、consumer group rebalance、MySQL lock wait retry（checkpoint set 已支持 Kafka partition/offset，Debezium MySQL e2e 脚本已覆盖 replay 后 upsert 去重、Redpanda 重启后继续消费、同组消费者 join/leave 后继续消费和锁等待重试后写入成功）；更多临时故障类型仍待扩展。
  - 控制面覆盖 start/stop/pause/resume、checkpoint set/reset、按 Kafka partition/offset 回放和 DLQ replay（checkpoint set + start/stop + offset replay 已进入 e2e；DLQ replay 已通过 MySQL schema 失败注入进入 Debezium e2e）。
  - 明确迁移边界：不管理 Debezium connector，不承诺 Kafka transaction exactly-once，不自动执行危险 DDL。
- 增加 Kafka ODS 到 MaxCompute 的生产候选链路：
  - Kafka ODS JSON -> `project` / `select_fields` -> `type_convert` -> `odps` / `maxcompute` sink（experimental sink 合约已落地；当前 build 明确拦截未启用 writer 的 pipeline）。
  - 支持 MaxCompute 表字段映射、`STRING` / `BIGINT` / `DOUBLE` / `TIMESTAMP` 等基础类型转换、按 `dt` 等字段写分区（配置 schema 和 validator 已覆盖；真实写入待接 SDK）。
  - preflight 校验目标 project/table/partition 权限、字段缺失、类型兼容和分区字段存在性（当前已覆盖本地字段/分区合约和 writer-disabled error；远端权限/表探测待集成环境）。
  - 明确 at-least-once 重放边界：默认 append 可能重复；推荐事件唯一键、分区 staging + merge/overwrite 或 sink 侧可证明的幂等提交策略。
- Kafka 链路增加 consumer crash、broker restart、consumer group rebalance、offset replay 测试。
- StateStore 恢复扩展到 e2e，明确 lookup、deduplicate、window 的恢复边界。
- Elasticsearch item-level bulk DLQ 已接入 runner/sink；后续补 mapping conflict e2e。
- S3/File 只补回填所需 deterministic prefix/manifest，不承诺通用 exactly-once 文件输出。
- Preflight 输出风险等级、字段级错误、幂等建议和目标 DDL 预览。
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
- Schema/sample/DDL preview 使用真实 connector descriptor、source introspection 和 preflight，不依赖静态表单猜测。
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
- ODPS/MaxCompute connector 进入评估和设计：
  - connector descriptor、鉴权、endpoint/project/table/partition 配置、schema/partition validator 和 writer-disabled preflight 已定义；批写 client 和远端错误分类待实现。
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
- 升级/回滚路径有文档和自动化验证。
- 运维指标能定位 source lag、sink latency、DLQ 积压、checkpoint age、worker health。

## 明确暂缓或不做

以下内容不进入近期主线：

- 为了数量新增大量数据库、消息队列或 SaaS connector。
- Kafka exactly-once transactions。
- 跨 sink 原子 fanout。
- 完整流处理引擎：任意 keyed state、timer、CoProcessFunction、多流状态计算。
- Flink/Spark 兼容 savepoint。
- 通用 SQL planner 或 Flink SQL 迁移层。
- 但支持把 Flink SQL 中的数据流拆成 pipeline：Kafka source、UDTF/flat_map、lookup、project、sink。
- sliding/session window、复杂 trigger、late side-output、retraction/update/delete 聚合语义。
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

1. 为四条主线建立 milestone：`reliability-baseline`、`first-task-flow`、`extension-contract`、`lightweight-ops`。
2. 为 production candidate 链路补 e2e 和失败注入测试。
3. 统一 connector maturity 事实源，并让 UI、descriptor、README、配置文档使用同一数据。
4. 扩展 `SchemaDescriptor` / `SchemaValidator` 覆盖面，用它驱动 preflight、DDL preview 和 UI 表单。
5. 扩展 `flat_map` / `udtf`：在已落地的 Lua 第一版 ABI、JS/TS 数组返回 ABI、WASM/plugin 数组返回 ABI、核心多输出 dry-run 和插件 dry-run 多输出响应基础上，补真实 WASM/plugin e2e。
6. 扩展插件化报文解析链路：第一条 Kafka raw -> Lua `flat_map` -> lookup -> Kafka ODS e2e 已覆盖解析失败、维表 miss、一进多出和 Kafka replay append 重复边界；Kafka sink producer 失败注入、JS/TS transform 数组返回和 dry-run、WASM/plugin 数组返回 ABI、GB32960 Lua parser fixture 已补；下一步补真实 WASM/plugin e2e 和更贴近生产的协议插件样板。
7. 继续实现 `odps` / `maxcompute` sink：experimental descriptor/schema/partition 合约和 writer-disabled preflight 已落地；下一步接入 SDK-backed batch writer、远端权限/表/分区 preflight、DLQ/retry 和 Kafka ODS e2e。
8. 为已落地的 Debezium Kafka CDC 迁移 preset 继续补更多临时故障类型注入；ODS MySQL upsert、offset replay、broker restart、consumer group rebalance、MySQL lock wait retry、MySQL 值范围失败 data-class DLQ replay、危险 DDL reject 已有 e2e 覆盖。
9. 改造 UI 配置上下文：descriptor/schema/sample/preflight/dry-run/docs 统一驱动表单、向导和 YAML/DAG 编辑器。
10. 建立 AI DAG context pack：核心组件 Markdown、使用方法、示例、边界和 maturity 进入可校验事实源。
11. 为 connector certification test kit 写第一版，先认证 MySQL、ClickHouse、Kafka、S3/File。
