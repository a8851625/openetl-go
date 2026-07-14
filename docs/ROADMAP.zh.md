# OpenETL-Go Roadmap

> 当前基线：v0.2.10-beta.1（2026-07-14）
>
> 最后核对：2026-07-14

本文只维护尚未完成、可以验收的产品和工程工作。已经交付的功能、测试命令和版本说明进入 [CHANGELOG.zh.md](../CHANGELOG.zh.md)；本文末尾只保留必要的证据索引，不再重复完整实现日志。

## 产品定位与边界

OpenETL-Go 是轻量、自托管、开源的 CDC/ETL 数据同步、清洗和汇聚运行时。核心产品模型始终是：

```text
Source -> Transform -> Sink
```

YAML、API 和 UI 操作同一份 pipeline/DAG spec。DAG、调度、并行分片和 master-worker 是该模型的扩展，不形成独立产品线。

近期工作必须优先服务以下能力：

- 数据库、Kafka、文件、HTTP、对象存储、OLAP/搜索等常见数据路径。
- checkpointed at-least-once、失败可见、DLQ replay、sink 幂等和 schema/preflight 安全。
- connector/plugin 合约、认证证据和小团队可运维性。
- 首次任务闭环和 YAML/API/UI 等价性。

明确边界：

- 默认语义是 at-least-once，不承诺跨 sink exactly-once。
- 生产链路依赖业务主键、版本列、upsert、ReplacingMergeTree、显式 deduplicate 或补偿吸收重放。
- 不建设通用 keyed state、任意 processing-time timer、Flink savepoint、完整 SQL planner、复杂 sliding/session window、late side-output 或 retraction 语义。
- metadata/checkpoint 存储与高频运行时 state/cache 分离；需要缓存或可恢复状态的内置能力使用 Redis，不退化到 SQLite/MySQL/PostgreSQL 充当高频缓存。
- 新 connector 只有在同时具备 schema、preflight、DLQ、metrics、重放边界和测试证据时才进入核心。

详细定位见 [positioning.zh.md](./positioning.zh.md)，at-least-once 与幂等边界见 [etl-idempotency.md](./etl-idempotency.md)。

## 当前已交付基线

以下能力已进入当前基线，不再作为未来 roadmap 项重复实施：

- 线性 pipeline、DAG 文件加载/hot-reload、条件路由、fanout、并行分片、cron/periodic/streaming/dependency 调度。
- MySQL batch/CDC/snapshot+CDC、PostgreSQL CDC、Kafka、file、HTTP、Redis、Feishu Sheet 等 source；ClickHouse、MySQL、PostgreSQL、Doris、Elasticsearch、Kafka、Redis、S3/file、JDBC、MaxCompute experimental contract 等 sink。
- lookup/enricher 异步 I/O 第一轮闭环：并发、in-flight 上限、超时、retry/backoff、背压、metrics、局部失败 DLQ、Redis-only cache gate。
- `flat_map`/`udtf` 一进多出、Debezium CDC policy、投影/类型转换、map_fields、extract、deduplicate、lookup、tumbling window 等同步和轻量汇聚能力。
- Web UI 固定任务向导、Connection Catalog 上下文、schema/sample/DDL preview、transform dry-run、validate/preflight、YAML/表单往返和 DLQ replay。
- DAG 节点级 DLQ replay；没有 `dag_node` 上下文的旧记录显式拒绝并保留。
- Connector certification kit 第一版、Plugin ABI v1 manifest/兼容边界、TypeScript SDK 和 Feishu source plugin 样板。
- 可靠性认证矩阵：source/state/sink checkpoint envelope、DLQ persistence gate、普通 Kafka crash/restart/rebalance/replay，以及 lookup/deduplicate/window StateStore 恢复证据。
- 真实 WASM transform 认证链路：固定工具链编译、ABI manifest 安装、0/1/N 输出、secret config、DLQ/replay、升级和 restart reload。
- standalone/master/worker/headless 运行文档、CLI smoke 和最小生产 runbook。
- MySQL/PostgreSQL `pre_write`、`increment`、生成列跳过、Debezium metadata PK 提取，以及多表映射/CDC 宽表生产候选链路。

当前公开成熟度必须继续以 descriptor/readiness、组件文档和可重复测试证据为准。没有真实环境证据的 MaxCompute、Feishu 和第三方插件不得提升为 production。

## 执行规则

Roadmap 状态只使用以下值：

| 状态 | 含义 |
| --- | --- |
| `active` | 当前唯一主任务，正在实现或验证 |
| `blocked_external` | 实现已具备，但缺少凭据、外部服务或人工授权 |
| `queued` | 已排序但尚未开始 |
| `delivered` | 已达到验收标准，应迁入 changelog/证据索引 |
| `deferred` | 明确不进入近期主线 |

执行纪律：

- 同一时间只推进一个 `active` 主任务。
- 外部阻塞必须写明所缺输入和解除条件；不得把被动等待包装成开发进度。
- 后续项不能静默改变当前最高优先级；需要调整时必须显式说明原因并获得用户确认。
- 每个任务必须有范围、验收标准和证据位置。完成后更新证据并从活动 backlog 移出。
- 实现过程中发现的相邻需求进入“有界后续”，不扩大当前验收标准。

## 当前主任务

### P0：MaxCompute 真实环境认证

状态：`blocked_external`

这是现有最高优先级，不改变原 roadmap 排序。MaxCompute/ODPS sink 的 SDK batch writer、partition/schema validator、远端 preflight、错误分类、retry/backoff、metrics 和环境门控 e2e 脚本已经存在；当前缺口不是继续实现 writer，而是真实 MaxCompute 环境中的认证证据。

解除阻塞所需输入：

- `MAXCOMPUTE_ENDPOINT`
- `MAXCOMPUTE_PROJECT`
- `MAXCOMPUTE_TABLE`
- `MAXCOMPUTE_ACCESS_KEY_ID`
- `MAXCOMPUTE_ACCESS_KEY_SECRET`
- 可选的 tunnel endpoint、quota 和用于失败注入的受控权限/测试表

验收标准：

- 实跑 Kafka ODS JSON -> `project` / `type_convert` -> MaxCompute 分区表。
- 验证正常写入、动态/静态分区、权限失败分类和远端 schema/partition preflight。
- 验证 sink 暂时失败进入 DLQ、修复后 replay 写回。
- 验证应用 restart、checkpoint reset/replay，并记录 append 模式可能重复的边界。
- 更新组件文档、connector readiness 和 certification evidence。
- 在上述证据完成前，`maxcompute` / `odps` maturity 保持 `experimental`。

现有入口：[e2e-maxcompute.sh](../hack/e2e-maxcompute.sh)、[sink-maxcompute.md](./components/sink-maxcompute.md)。

## 排队中的工作

以下工作按原顺序排队。P1、P2 已于 2026-07-13 达到验收标准并迁入 CHANGELOG/证据索引；P0 未解除阻塞时，是否切换到 P3 仍需显式确认。

### P3：成熟度事实源与认证覆盖扩展

状态：`queued`

目标：减少手写 maturity 字符串与测试证据之间的漂移。

范围：

- 将现有 certification kit 从首批 MySQL/ClickHouse/Kafka/S3/File 扩展到所有标记为 production 的内置 connector，优先补 HTTP、PostgreSQL sink 和 Doris。
- maturity 提升必须同时满足 descriptor/schema、注册、preflight/readiness、组件文档和可重复 e2e evidence。
- connector/plugin 的 partial readiness 必须携带具体 evidence 和 remediation。
- 第三方 connector 在不修改专用 UI 代码的情况下提供类型化表单；preflight、metrics、DLQ 等行为继续通过统一合约暴露。

验收标准：

- 任一 connector 被标记为 production 时，认证测试自动将其纳入或显式拒绝未知 production 项。
- descriptor、schema required 字段、secret/scope、组件文档和实现注册之间有一致性测试。
- 不再仅靠人工修改 metadata 字符串提升成熟度。

### P4：首次任务体验残留收口

状态：`queued`

现有向导和上下文闭环已经交付，本项只处理有证据的残留，不重新建设一套 UI。

范围：

- 补齐仍为 partial 的 connector schema/sample/DDL preview 和字段级 remediation。
- 将 schedule 重跑风险与 sink 幂等性 warning 串联，而不污染 source capability 定义。
- 统一 pipeline、DAG node、字段、风险、修复动作和是否可 replay 的错误表达。
- 使用 Playwright 保持向导、YAML 往返、preflight 修复、创建启动和 DLQ replay 的关键路径。

验收标准：

- 新增 UI 工作必须由真实 connector descriptor/introspection/preflight 驱动，不使用独立静态执行语义。
- 同一 spec 在 UI、YAML 和 API 间往返不丢失隐藏字段。
- 错误提示可以定位到具体 pipeline/node/field，并给出可执行 remediation。

### P5：轻量运行与生产运维收口

状态：`queued`

现有运行模式文档和最小 runbook 已交付；剩余工作聚焦自动化和资源基线。

范围：

- 评估 source/sink build tags，优先裁剪重依赖或低频 connector。
- 为 SQLite/MySQL/PostgreSQL storage 补 migration 升级、回滚和备份恢复 smoke。
- 建立默认镜像大小、启动耗时、空闲内存、典型吞吐和 checkpoint 延迟基线。
- 补指标面板或等价查询说明，覆盖 source lag、sink latency、DLQ backlog、checkpoint age 和 worker health。

验收标准：

- standalone、master-worker 和 headless 路径有可重复 smoke。
- storage schema 变更具备前向升级和受控回滚/恢复证据。
- 发布说明记录资源基线及显著回归阈值。

## 有界后续

这些事项只有在上方当前任务完成或被明确重新排序后才进入执行：

- S3/File first-class manifest；当前 content-addressed key 只吸收相同 batch 边界的重放，不宣称通用 exactly-once 文件输出。
- ODPS/MaxCompute lookup/source 方向；必须在 MaxCompute sink 真实认证后再评估，优先推荐将维表镜像到 MySQL/PostgreSQL/Redis。
- Feishu 内置 source 和插件样板的真实环境、429/rate-limit、token failure 和 restart 证据；完成前保持 beta/dev-only。
- JS/TS/WASM parser 示例扩展；不得将具体行业协议硬编码进核心。
- 更复杂的多事实实时 merge、CDC dimension update 和 late-data 策略；只在不引入 Flink 级状态计算语义的前提下评估。

## 明确暂缓或不做

- 为数量新增大量数据库、消息队列或 SaaS connector。
- Kafka exactly-once transaction 和跨 sink 原子 fanout。
- 任意 keyed state、通用 timer、CoProcessFunction、多流状态机。
- Flink/Spark savepoint 兼容。
- 通用 SQL planner 或 Flink SQL 迁移层；只支持将其数据流语义拆成普通 pipeline。
- sliding/session window、复杂 trigger、late side-output、retraction/update 聚合语义。
- connector marketplace、下载量/评分系统。
- Kubernetes operator、etcd、Zookeeper 等新的基础设施依赖。
- AI 直接绕过 validate/preflight 启动 pipeline。

## 交付证据索引

| 已交付领域 | 主要证据 |
| --- | --- |
| v0.2.9 多表映射、CDC 宽表、UI 场景和 connection scope | [CHANGELOG.zh.md](../CHANGELOG.zh.md)、`hack/e2e-multi-table-map.sh`、`hack/e2e-mysql-cdc-wide.sh`、`hack/e2e-ui.sh` |
| DAG 声明式加载与节点级 DLQ replay | `internal/etl/server/dag_load_test.go`、`internal/etl/server/dlq_test.go`、`internal/etl/orchestrator/replay.go` |
| lookup/enricher 异步 I/O | `hack/e2e-lookup-query.sh`、`hack/e2e-enricher.sh`、transform/pipeline/server 单测 |
| 关系型 pre_write/increment、生成列与 metadata PK | `hack/e2e-relational-write-modes.sh`、`hack/e2e-debezium-mysql.sh` |
| Connector certification 与 Plugin ABI v1 | [connector-certification.md](./connector-certification.md)、[plugin-abi-v1.md](./plugin-abi-v1.md)、`internal/etl/server/connector_certification_test.go` |
| P1 可靠性认证矩阵 | [reliability-certification.md](./reliability-certification.md)、`hack/e2e-kafka.sh`、`hack/e2e-wide-table.sh`、`hack/e2e-lookup-state.sh`、checkpoint/pipeline/orchestrator 单测 |
| P2 真实 WASM 插件链路 | `hack/e2e-wasm-plugin.sh`、`hack/wasm-compiler.Dockerfile`、`web/plugin-sdk/examples/replay-matrix-transform/`、`TestWASMPluginCertificationFixture` |
| Feishu source plugin 样板 | `web/plugin-sdk/examples/feishu-sheet-source/` |
| 运行模式与生产 runbook | [runtime-modes.md](./runtime-modes.md)、`hack/e2e-runtime-smoke.sh` |
| UI 首次任务闭环与 AI context pack | `web/src/main.tsx`、`web/src/DagEditorPage.tsx`、`internal/etl/server/ai_context_test.go`、`hack/e2e-ui.sh` |

## 跟踪指标

- 可靠性：失败记录可见率、DLQ 写入失败次数、replay 成功率、crash/rebalance e2e 通过率、checkpoint 恢复耗时。
- 易用性：首次成功任务耗时、向导完成率、preflight 拦截率、修复后成功率。
- 数据处理：Kafka/CDC lag、lookup hit/miss、window emit、重复吸收率。
- 扩展性：descriptor/schema/preflight 覆盖率、production connector 认证率、Plugin ABI 兼容测试通过率。
- 轻量性：镜像/二进制大小、启动耗时、空闲内存、外部依赖数量和 checkpoint 延迟。
