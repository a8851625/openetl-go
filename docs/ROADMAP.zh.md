# OpenETL-Go Roadmap

> 当前基线：v0.2.11-beta.2（2026-07-22）
>
> 最后核对：2026-07-22

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

状态：`in_progress`（2026-07-21 原型对齐批次已落地主路径；见 `docs/UI-REDESIGN-TODO.zh.md` residual）

现有向导和上下文闭环已经交付，但 2026-07-20 产品走查确认当前 Web UI 仍更接近“能力完整的工程控制台”，尚未完全收敛为围绕“创建成功、稳定运行、快速修复”的任务型产品。主要证据包括：一级导航平铺构建、运维和系统对象；首次任务向导在单个长弹窗中同时暴露模板、连接、descriptor、运行参数、transform、样例、YAML 和 preflight；运行状态、健康度和累计指标存在口径混用；页面状态没有可分享 URL；部分危险操作、国际化和无障碍表达不一致。

**2026-07-21 已交付（证据）**：全宽管道列表 + URL 筛选；`#/pipelines/new` 全页三段式向导 + 草稿；DLQ 聚合主视图 + Replay 确认面板；详情写入语义/生命周期；总览时间范围切换；Connections 抽屉；问题中心固定排序；顶栏用户菜单与扩展分组；e2e/文档区分「路由可达」与「原型对齐」。Residual：DAG 空画布模板、小屏信息行、截图刷新、多 run 历史。

本项在现有 React UI、connector descriptor/introspection/preflight 和同一份 pipeline spec 上渐进收口，不另建独立 UI 语义、设计器模型或服务端执行模型。内部按以下顺序实施；同一时间只推进一个子阶段：

#### P4.1：状态语义与交互可信度

- 建立统一的展示状态：`healthy`、`degraded`、`failed`、`paused`、`scheduled`、`completed`，由期望状态、实际运行状态、lag、checkpoint、DLQ 和最近错误共同派生；不得再使用“running 数量 / pipeline 总数”作为健康度。
- 区分失败 pipeline 数、失败记录累计值、当前 DLQ backlog 和历史 DLQ/replay 计数；所有卡片、列表和详情使用相同口径并标明时间范围。
- `failed` 不计入 `stopped`，主动暂停、等待调度和一次性完成不得显示为不健康。
- 统一批量启动/停止、立即运行、禁用调度、checkpoint reset、连接删除、worker deregister、DLQ 删除/replay 等高影响操作的目标数量、风险说明、确认和结果反馈；连接删除需提示被引用的 pipeline。
- 补齐关键页面中硬编码的中英文混用，统一 Lucide 图标、文本标签和状态颜色；可点击行、图标按钮和状态提示具备键盘、ARIA 和非颜色表达。
- API Token 默认遮挡；AI/LLM 明确为可选能力，任何 AI 入口仍必须经过 validate/preflight，不能成为创建或启动 pipeline 的旁路。

#### P4.2：首次任务分步闭环

- 将现有长弹窗重组为同一向导内的渐进步骤：场景选择 -> Source 连接与数据选择 -> Sink 与写入语义 -> 可选 Transform -> 安全检查 -> 确认并启动。
- 默认流程只展示完成当前步骤所需字段；connector maturity/readiness、原始 JSON、完整 YAML、批量和 checkpoint 等高级参数使用渐进披露，但不得因此丢失或重写隐藏字段。
- Source 步骤展示真实 connection health、库表/topic、schema 和 sample；Sink 步骤展示目标、auto-create/DDL preview、主键、insert/upsert/pre_write 等写入语义和 replay 重复边界。
- Transform 默认可跳过；新增、排序、删除和逐阶段 dry-run 保留，并把失败定位到具体 stage/field。
- Preflight 问题靠近对应步骤和字段展示，并提供可执行 remediation；修复后可重新验证。生产 UI 移除 `Failure demo`、`Repair to file_sink` 等 e2e/demo 专用控制。
- 最终确认页以 `Source -> Transform -> Sink` 摘要展示连接、数据范围、调度、幂等策略、DDL、checkpoint、DLQ 和已知重放风险，再执行创建和启动。

#### P4.3：任务型信息架构与可分享上下文

- 一级信息架构收敛为总览、管道、运维、资源和系统等任务分组；Designer 作为创建/编辑 pipeline 的入口，Schedule 作为 pipeline 生命周期配置，同时保留必要的全局运维视图。
- Connector 能力/成熟度目录与已保存 Connection 实例分开表达；WASM 编辑/编译归入扩展或开发者能力，不与日常运行入口同权展示。
- standalone 模式不突出 worker 实现细节；worker/cluster 管理只在对应运行模式或系统分组下展示。
- 引入可刷新、可返回、可分享的 URL，至少覆盖 `/pipelines`、`/pipelines/:id`、`/pipelines/:id/runs`、`/pipelines/:id/dlq` 和 `/connections/:name`；刷新和浏览器前进/后退不得丢失选择上下文。
- 总览从累计数字陈列收敛为可操作的待处理事项入口，优先呈现 failed/degraded pipeline、DLQ backlog、CDC lag、过期 checkpoint、异常 connection 和离线 worker；点击后定位到对应对象和修复上下文。
- Pipeline 列表直接展示 Source -> Transform -> Sink 摘要、batch/CDC 模式、schedule、sink 写入模式和最近错误；详情按 Overview、Runs、Issues、Checkpoints、Spec/Versions 组织。
- DLQ 按 error class、DAG node 和时间范围聚合，并形成“定位问题 -> 编辑修复 -> replay -> 核对剩余记录”的闭环；replay 前继续明确 at-least-once 和可能重复的边界。

范围：

- 补齐仍为 partial 的 connector schema/sample/DDL preview 和字段级 remediation。
- 将 schedule 重跑风险与 sink 幂等性 warning 串联，而不污染 source capability 定义。
- 统一 pipeline、DAG node、字段、风险、修复动作和是否可 replay 的错误表达。
- 使用 Playwright 保持分步向导、URL/deep-link、YAML 往返、preflight 修复、创建启动、状态口径和 DLQ replay 的关键路径。

验收标准：

- 新增 UI 工作必须由真实 connector descriptor/introspection/preflight 驱动，不使用独立静态执行语义。
- 同一 spec 在 UI、YAML 和 API 间往返不丢失隐藏字段。
- 错误提示可以定位到具体 pipeline/node/field，并给出可执行 remediation。
- 主动暂停、等待调度、一次性完成、运行失败和 degraded 状态在总览、列表、详情和指标中口径一致，并有自动化覆盖证明失败记录数、失败 pipeline 数和 DLQ backlog 未混用。
- 用户可以从空环境沿分步向导完成 connection 选择、schema/sample 确认、transform dry-run、sink 幂等/DDL 检查、preflight 修复、创建启动；默认路径不要求编辑 JSON/YAML。
- 关键对象具有稳定 URL，刷新、前进/后退和直接打开 deep link 后仍能恢复同一 pipeline/tab/filter 上下文。
- failed/degraded/DLQ 入口能从总览或 pipeline 详情定位到具体错误，并完成修复后的 replay 或重启；结果反馈包含成功数、失败数和剩余 backlog。
- 高影响操作具备一致确认和影响说明；关键中文路径不出现未翻译的产品文案，图标按钮与可点击行通过键盘和无障碍检查。
- standalone 与 distributed 模式分别验证导航和系统入口，日常 pipeline 用户不需要理解 worker/plugin 编译等实现细节即可完成首次任务和故障处理。

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
- 易用性：首次成功任务耗时、向导完成率、preflight 拦截率、修复后成功率、deep-link 上下文恢复率、从 failed/degraded/DLQ 入口到修复或 replay 的耗时。
- 数据处理：Kafka/CDC lag、lookup hit/miss、window emit、重复吸收率。
- 扩展性：descriptor/schema/preflight 覆盖率、production connector 认证率、Plugin ABI 兼容测试通过率。
- 轻量性：镜像/二进制大小、启动耗时、空闲内存、外部依赖数量和 checkpoint 延迟。
