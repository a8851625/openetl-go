# OpenETL-Go 发布说明

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 格式。

## [Unreleased]

### 新增

- **dbt transform（Phase 1）**：`type: dbt` 将 batch 写入 staging 表 → `dbt run --select <model>` → 读取输出表；支持 `postgres` / `duckdb` adapter。dbt 为可选能力，不引入核心依赖。
- 配置 schema / connector descriptor 注册 `dbt`；组件文档 `docs/components/transform-dbt.md`；可跳过 E2E `hack/e2e-dbt.sh`。

### 验证

- `go test ./internal/etl/transform/ -run 'DBT|ParsePostgres'`
- `go test ./internal/etl/server/ -run 'PluginSchema'`

## [v0.2.11-beta.2] — 2026-07-22 — UI 原型对齐与信息架构收口

### 亮点

- **管道列表**：全宽列表（去掉 master-detail 右栏）；hash 筛选；批量选择工具条；行操作右对齐；Start/Stop 在更多菜单。
- **新建向导**：`#/pipelines/new` 全页 6 步 + 摘要 + 草稿；e2e 跳过草稿恢复。
- **DLQ 闭环**：三栏布局；左侧管道筛选/仅积压/排序；右侧 Replay 确认面板（目标数/幂等/dry-run）；Lucide + aria-label。
- **管道详情**：写入语义/生命周期卡；**Logs** 页签（卡片式日志，非终端风格）；**Topology** 只读 DAG；调度在详情弹窗编辑；拓扑写入口只保留设计器。
- **IA 收口**：去掉列表「DAG+日志」复合弹窗；连接器目录合并内置矩阵；调度总览共用弹窗；减少重复编辑入口。
- **AppShell**：顶栏搜索/自动刷新/语言切换/重载锚点；扩展区仅 WASM 我的插件。

### 验证

- `npm --prefix web run build`
- `./hack/e2e-ui.sh` → **108 passed, 0 failed**

### Residual

- DAG 空画布模板、小屏表格信息行、截图刷新、多 run 历史深度。详见 `docs/UI-REDESIGN-TODO.zh.md`。

## [v0.2.11-beta.1] — 2026-07-21 — 任务型 Web UI 重构（P4 落地）

### 亮点

- 一级导航收敛为任务分组：**总览 / 运行 / 资源 / 系统**；「新建管道」成为一级主动作。
- **Designer/DAG 降级为高级编辑入口**（非删除）：日常创建走 Source → Transform → Sink 向导；多源/路由/扇出仍经同一 pipeline/DAG spec 的画布编辑。
- 统一管道健康视图：`healthy` / `degraded` / `failed` / `paused` / `scheduled` / `completed` 等，由运行态、lag、checkpoint、DLQ、最近错误共同派生；总览改为问题优先，不再用「running/总数」冒充健康度。
- Hash 可分享路由：`#/overview`、`#/pipelines`、`#/pipelines/new`、`#/pipelines/:id/:tab`、`#/issues`、`#/dlq`、`#/connections`、`#/connectors`、`#/designer` 等；刷新/直达不丢上下文。
- 新增问题中心、连接器目录（与 Connection 实例分离）、管道详情 tabs（Overview / Runs / Issues / Checkpoints / Spec）。
- DLQ 按 error class / DAG node 聚合；空 backlog 隐藏批量危险操作；replay 反馈剩余积压。
- 视觉 token：冷灰 canvas + 青绿主色；中英 i18n 覆盖新 IA；standalone 默认不突出 Workers，distributed 再展示集群入口。
- 设计基线文档：`docs/UI-REDESIGN.zh.md`、`docs/UI-REDESIGN-PROTOTYPE.html`。

### 验证

- `npm --prefix web run typecheck`
- `npm --prefix web run build`
- `./hack/e2e-ui.sh` → **107 passed, 0 failed**

### 边界

- 默认语义仍是 **at-least-once**；UI 不引入新的执行模型或独立 UI spec。
- P4 分步向导重组、部分 connector 字段级 remediation 与完整无障碍矩阵仍可继续收口（见 ROADMAP P4 子阶段）。

## [v0.2.10-beta.1] — 2026-07-14 — 可靠性认证与真实 WASM 插件链路

### P1：可靠性认证矩阵收口

- 统一线性 pipeline 与 DAG checkpoint envelope：绑定 source position、StateStore snapshot version 和 sink acknowledgement metadata；明确保持 at-least-once，而非跨系统事务。
- DLQ 持久化失败会阻断后续 checkpoint；sink commit metadata/state snapshot/checkpoint store 任一失败均不会静默推进 source position。
- 修复 Kafka offset `0` 未进入 checkpoint 的零值判断缺陷。
- 修复 checkpoint 节流后流转为空闲时，最后一个 sink-acknowledged batch 可能长期不落 checkpoint 的问题；pending boundary 现在由定时器以及 Stop/EOF 强制持久化。
- `allow_unsafe` 成为可执行 spec 字段：Kafka/CDC 写 file/S3 仍默认阻断，只允许测试并记录过 replay 重复边界的链路显式 opt-in。
- 新增 [可靠性认证矩阵](docs/reliability-certification.md)，并扩展 `hack/e2e-kafka.sh`、`hack/e2e-wide-table.sh`，覆盖普通 Kafka crash、broker restart、consumer group rebalance、offset replay、state restore 与 sink commit envelope。

### P2：真实 WASM 插件链路 E2E

- 新增真实 TypeScript transform fixture 和 `hack/e2e-wasm-plugin.sh`，验证真实 WASM 编译、ABI v1 manifest 安装、0/1/N 输出、secret config、错误入 DLQ、升级后 replay 和 restart reload。
- 新增带架构校验和的 compiler image：固定 esbuild 0.25.6、Extism JS PDK 1.6.0 和 Binaryen 130，并在构建期检查 `wasm-merge`/`wasm-opt`。
- 修正 Extism JS SDK bridge：使用当前 `Host`、`Config`、`Var` globals，启用 WASI、调用级配置更新、状态桥接和并发安全的 install/unload/exec。
- 修正服务端 transform-only 编译入口和公开文档：TypeScript 先经 esbuild 打包为 CommonJS，再使用当前 `extism-js input.js -i interface.d.ts -o output.wasm` CLI；source/sink 继续离线编译安装。
- 新增真实 WASM fixture/manifest/compiler 静态认证门闩；未完成独立故障/replay 证据的第三方插件仍保持 beta/dev-only。

### 验证

- `go test ./... -count=1`
- `go test -tags=extism ./internal/etl/plugin/pluginsystem ./internal/etl/server -count=1`
- `npm --prefix web/plugin-sdk run build`
- `npm --prefix web run build`
- `./hack/e2e-kafka.sh`
- `E2E_SKIP_BUILD=1 ./hack/e2e-wide-table.sh`
- `E2E_SKIP_BUILD=1 ./hack/e2e-lookup-state.sh`
- `./hack/e2e-wasm-plugin.sh`

## [v0.2.9] — 2026-07-13 — 多表映射同步、CDC 宽表链路、UI 场景入口、connection scope

### 亮点
- **多表 A→B 同步 + 表名映射**：
  - 管道级 `table_mapping` 支持 `template` / `rules` / `regex`，模板变量含 `{source_table}`、`{source_db}`。
  - 映射前保留 `_source_table` / `_source_database`。
  - `mysql_cdc` / `mysql_snapshot_cdc` 写入 `Metadata.Database`，便于限定库表映射与 `cdc_policy` 过滤。
  - snapshot checkpoint 在表映射后仍按源表名维护游标。
  - 新增 e2e：`hack/e2e-multi-table-map.sh` + `testdata/pipes-multi-table-map/`。
- **多表 binlog → 宽表**：
  - 生产候选路径：`mysql_cdc` + `cdc_policy` + `lookup` + rename/type_convert → ClickHouse 宽表。
  - 新增 e2e：`hack/e2e-mysql-cdc-wide.sh` + `testdata/pipes-mysql-cdc-wide/`。
- **两个核心场景的 UI 产品化**：
  - 向导新增推荐模板：多表库同步 + 表映射、CDC 宽表（lookup 补维）。
  - 向导可编辑 `table_mapping`，生成普通 pipeline YAML。
  - Connection Catalog / Wizard / DAG 表单按连接字段 vs 任务参数分流，文案更清晰。
  - Designer 工具栏文字标签、空态文案、管道/连接/DLQ/审计/WASM 空状态增强。
  - 修复 WASM Plugins 与 Workers 的 i18n 裸 key（中英文）。
- **扩展与运维包装**：
  - 官方飞书表格 source 插件样板：`web/plugin-sdk/examples/feishu-sheet-source/`（beta/dev-only）。
  - 轻量运行形态文档与 smoke：`docs/runtime-modes.md`、`hack/e2e-runtime-smoke.sh`。
  - descriptor/schema 字段 `scope` 标注，以及认证 kit 对插件样板的检查扩展。
- **数仓 ETL 残留证据**（主线已有能力一并纳入本版面）：
  - 关系型写入模式、生成列跳过、Debezium metadata PK、DAG 加载/DLQ 重放等相关 e2e 仍属发布面。

### 发布边界
- 默认交付语义仍是 **at-least-once**。生产链路请用 upsert、稳定业务键、版本列、ReplacingMergeTree 或显式去重吸收重放。
- MaxCompute/ODPS 在无真实环境写入/DLQ/replay 证据前保持 experimental。
- 内置 `feishu_sheet` 与飞书 WASM 插件样板在补真实环境故障注入前保持 beta/dev-only。
- 复杂多事实实时 merge / Flink 级宽表语义仍不在范围内；已认证宽表路径是事实流 + 维表 lookup（+ 可选 tumbling 聚合）。

### 验证
- `go test ./internal/etl/server ./internal/etl/pipeline ./internal/etl/source ./internal/cmd -count=1`
- `npm --prefix web run build`
- `./hack/pack.sh`（UI 已构建时可用 `SKIP_UI=1`）
- `bash hack/e2e-runtime-smoke.sh`
- `E2E_SKIP_BUILD=1 bash hack/e2e-multi-table-map.sh`
- `E2E_SKIP_BUILD=1 bash hack/e2e-mysql-cdc-wide.sh`
- Playwright UI 抽查：Wizard 模板、table_mapping 面板、WASM/Workers 中文 i18n

## [v0.2.8] — 2026-07-06 — lookup query-mode 认证、Plugin ABI v1 生产边界、Doris/UI 收尾发布

### 亮点
- **lookup query-mode 与状态恢复认证**：
  - 完成 lookup 异步 I/O 第一轮闭环，覆盖 query-mode、Redis-only cache gate、preflight/schema/spec 校验和 `hack/e2e-lookup-query.sh`。
  - 新增 lookup query fixture，覆盖成功命中、miss、timeout、lock-wait/replay 行为。
  - 新增 runner DLQ 上下文回归，确保 DLQ 写入失败不会静默推进 checkpoint。
- **Connector certification kit 扩展**：
  - 扩展 descriptor/schema/readiness/e2e evidence/组件文档一致性认证。
  - 补 MySQL、ClickHouse、Kafka、S3/File 生产候选证据，并继续增强 Doris 持续认证。
  - 认证文档新增插件 ABI 规则和生产插件准入 gate。
- **Plugin ABI v1 生产边界**：
  - 在 `internal/etl/plugin/pluginsystem` 统一插件名、kind、manifest 校验。
  - `/api/v2/plugins/install` 支持可选 Plugin ABI v1 `manifest` 字段，显式 manifest 会在写入/加载 WASM 前校验。
  - 插件元数据持久化 ABI、最低运行时版本、manifest JSON 和 `manifest_validated`。
  - `/api/v2/plugins` 与 `/api/v2/plugins/schema` 暴露当前 `plugin_abi` 合约。
  - TypeScript SDK 导出 ABI 常量、manifest 类型和 `definePluginManifest`；VIP 示例插件同步声明 manifest。
  - 新增 `docs/plugin-abi-v1.md`，记录 manifest 形状、兼容矩阵、deprecation policy 和认证边界。
- **Doris 生产候选认证增强**：
  - `hack/e2e-doris.sh` 改为独立 MySQL source 端口，并覆盖 MySQL CDC -> Doris 与 MySQL snapshot+CDC -> Doris。
  - 补 restart/replay 证据：app restart 后继续消费、checkpoint reset replay 吸收、schema drift add-column、Doris BE outage -> DLQ -> 恢复后 replay。
- **Phase 1 验证与 UI 产品化收尾**：
  - 修复 PostgreSQL CDC e2e 中 MySQL client host 口径。
  - Wizard transform chain 完成增删、类型切换、排序、逐阶段 dry-run 和 partial error 阶段定位。
  - UI e2e 覆盖 transform-chain 控件，保持 99 项通过。
- **运行打磨**：
  - 补分布式 worker label HTTP e2e 覆盖。
  - 补日志回归测试。
  - 刷新内嵌 UI 资产和发布版本元数据。

### 发布边界
- Plugin ABI v1 基础设施可作为生产扩展边界使用；单个第三方插件只有在具备 manifest、文档、测试和运行证据后才可声明 production-certified。
- Feishu/Lark 电子表格插件集成已写入 roadmap，作为下一步官方插件样板；现有内置 `feishu_sheet` source 在补更多真实环境证据前仍保持 beta。
- 默认交付语义仍是 at-least-once；生产建议继续依赖 upsert、稳定业务键、版本列和 sink 侧 replay 吸收策略。

### 验证
- `go test ./internal/etl/plugin/pluginsystem ./internal/etl/server ./internal/etl/storage/... -count=1`
- `go test ./internal/etl/... ./internal/cmd -count=1`
- `go test ./... -count=1`
- `npm --prefix web/plugin-sdk run build`
- `npm --prefix web run build`
- `SKIP_UI=1 ./hack/pack.sh`
- `CONTAINER_CLI=podman ./hack/e2e-ui.sh` — 99 passed, 0 failed
- `git diff --check`

## [v0.2.7] — 2026-07-03 — Debezium CDC preflight 修复、enricher 异步 I/O 增强、Phase 1 数仓 ETL 场景闭环

### 亮点
- **Debezium CDC preflight 修复**：新增 `hasDebeziumCDCTransform()` 辅助函数；`checkRelationalSinkConfig` 和 `checkDorisSinkConfig` 在检测到 `debezium_cdc` transform + `auto_create: true` / `pk_columns_from_metadata: true` 时，跳过静态 `table` 和 `pk_columns` 必填检查；对 CDC 管道抑制 `pk_columns` recommendation。
- **enricher 异步 I/O 增强**（Phase 1 "异步 I/O 维表查询增强"）：
  - `concurrency` / `max_in_flight` 并发控制 + `BatchTransform` 实现 batch 内并行。
  - `max_retries` / `retry_base_ms` 指数退避重试（仅 transient 类错误：HTTP 429/5xx、网络超时）。
  - HTTP 429 `Retry-After` 响应头在重试时优先使用服务端要求的退避时间。
  - 显式失败分类：HTTP 429/5xx → `transient`、401/403 → `auth`、其他 4xx → `data`。
  - 完整 `TransformMetricsProvider`：10 个计数器（processed/hits/misses/cache_hits/cache_misses/timeouts/retries/errors/succeeded/in_flight）。
  - SQL mode 现在也受 `timeout_seconds` context deadline 保护（之前仅 HTTP mode 有独立超时）。
  - 新增 `hack/e2e-enricher.sh`，覆盖 4 个场景：happy path、429+Retry-After 重试、timeout→DLQ、batch partial failure→DLQ。
- **Phase 1 数仓 ETL 场景闭环**完成交付：
  - pre_write action（MySQL/PostgreSQL sink：delete/truncate/truncate_partition + 参数化 condition）。
  - map_fields transform（声明式枚举/码值映射）。
  - Post-Commit Trigger（通过 `schedule.type: dependency` 实现 CDC→重算）。
  - increment batch_mode（MySQL/PostgreSQL 累加写入模式）。
  - extract transform（正则 `pattern`+`group` 提取 + `template` 拼接）。
  - feishu_sheet source（OAuth2 client_credentials + 飞书表格拉取）。
  - HTTP source OAuth2 client_credentials 认证增强。
  - Connection 配置职责收束（behavior 字段 deprecation warning+向后兼容）。
  - Sink 元数据驱动列集：生成列自动跳过 + `pk_columns_from_metadata` Debezium key PK 推导。

### 验证
- `go test -count=1 -run TestRunPreflight ./internal/etl/server/`
- `go test -count=1 -run TestEnricher ./internal/etl/transform/`
- `go test ./internal/etl/transform/ ./internal/etl/server/ ./internal/cmd -count=1`
- `go vet ./internal/etl/... ./internal/cmd`
- `E2E_SKIP_BUILD=1 ./hack/e2e-enricher.sh` — 4 场景通过
- `go build -buildvcs=false ./...`

## [v0.2.6-beta-2] — 2026-07-01 — 运行时调度接入 Server

### 亮点
- 将已存在的 `orchestrator.Scheduler`（cron/periodic/dependency 调度引擎）接入 `Server.StartAll`，使得延迟调度的 pipeline 不再在启动时立即执行，而是注册到调度器，由调度器在指定时间触发。
- `Server` 结构体新增 `s.scheduler` 字段，在 `NewServer` 中初始化；`StartAll` 遍历所有 pipeline，对有延迟 schedule 的调用 `s.scheduler.RegisterExecutor(id, runner, sched)`，然后 `go s.scheduler.Run(ctx)`。
- 所有运行时 API 路径（create、update、import、schedule PUT/DELETE、pipeline delete）都会在操作同时注册或注销调度条目，无需重启。
- 新增 `schedulerScheduleFor` 辅助函数，将 `depends_on` 中的 pipeline 名称解析为稳定 ID，确保依赖调度在内部使用 ID 作为 key 时仍能正确触发。
- 重构 `Scheduler` 接口从 `*DAGExecutor` 改为 `pipeline.RunnerInterface`，线性 Runner、ParallelRunner、DAGRunnerWrapper 均可被调度。
- 新增集成测试覆盖：cron schedule 在启动时不立即执行（状态为 `scheduled`）、periodic schedule 真正触发 runner。

### 验证
- `go test ./internal/etl/... ./internal/cmd -count=1`

## [v0.2.6-beta-1] — 2026-07-01 — Phase 1 收尾：connector preflight 全面补齐与连接上下文闭环

### 亮点
- 把 Phase 1（可信同步与轻量汇聚 MVP）剩余的 preflight 缺口收齐：为全部内置 source/sink 补第一版静态字段级 remediation 和真实远端 reachability 检查，preflight 不再只覆盖 schema validator，避免非法配置静默回退默认值后才在运行时暴露为行为差异。
- Source 侧补独立 preflight：Kafka（broker metadata、topic/partition 存在性）、MySQL CDC / snapshot+CDC（静态字段、shard、`start_from`、远端连接/权限/binlog/表）、MySQL batch（`table|query`、cursor column、表/列存在）、PostgreSQL CDC（静态字段、`wal_level=logical`、replication role、publication/slot）、File（`path`/`format`/CSV delimiter、可解析性）、HTTP（`url`/method/pagination、首个分页 sample、auth、JSON 响应、`result_key`）。
- Sink 侧补字段级 static preflight 和真实远端检查：File/S3（`format` 白名单、显式 `endpoint`/`bucket`、retry 非负、bucket reachability）、MySQL/PostgreSQL（`batch_mode`、upsert `pk_columns`、`schema_drift`、`ddl_policy`、`sslmode`、目标表/列 metadata、DDL preview）、ClickHouse（`protocol`、`source_dialect`、`optimize_interval_sec`、`compression`、`version_column`、目标 schema、DDL preview）、Doris（`write_mode`、Stream Load `format`/`scheme`/`timeout`、Unique Key metadata、DDL preview）、Kafka（`compression` 白名单、`retry_backoff_ms`、topic metadata、`auto_create_topic` 降级）、Elasticsearch/OpenSearch（`hosts`/`index`/`chunk_size`/retry 参数，运行时拒绝空值隐式回退 localhost）、MaxCompute/ODPS（endpoint/project/table/access key、partition 冲突、`columns` 类型，真实远端走现有 `maxcompute-preflight`）。
- PostgreSQL CDC source 重写 preflight 和 readiness：静态失败时不继续远端探测，避免首跑 validate 被连接错误掩盖真正缺失字段；新增 `hack/e2e-postgres-cdc.sh` 覆盖 insert/update/delete -> MySQL upsert/delete，以及 stop 后通过保留 replication slot 在 restart 后继续消费。
- Source/Sink runtime 配置补常见数组形态兼容：Kafka `brokers`、MySQL/PG CDC `tables`、MySQL batch `columns`、ES `hosts`、各 sink `pk_columns` 现在同时接受 `[]any` 和 `[]string`，避免 UI/API 生成的数组字段被静默忽略。
- 首次任务向导把 `batch_size` / `checkpoint_interval_sec` / `dlq.enable` 提升为可见的 Runtime safety 表单控制，并修正 preflight / saved-connection recommendation Apply 的状态闭环：顶层运行参数现在写入 wizard 状态源，与 YAML sync 和生成 spec 保持一致。
- Connector readiness 暴露 source 侧 `remote_preflight` gate 和 sink 侧真实 Open + schema metadata 证据；缺少远端检查的 connector 会在 readiness guidance 中显式暴露缺口，不再隐式标 pass。
- 组件文档事实源补齐 PostgreSQL CDC source、Elasticsearch sink、MaxCompute sink 三页，覆盖 descriptor/schema/preflight/readiness/maturity 一致性。

### 边界与不做
- 本迭代只收尾 preflight、连接上下文和 runtime safety 表单，不新增 connector、不改变 transform 执行语义、不引入通用 SQL planner 或 Flink 兼容层。
- MaxCompute/ODPS sink 在没有真实环境 DLQ/replay/e2e 证据前 maturity 继续保持 experimental/beta，不提升 production。
- DAG DLQ replay 当前不支持的行为继续在 API/UI/文档中显式可见。

### 验证
- `podman run --rm -v "$PWD:/workspace" -v openetl-go_go-cache:/go -v openetl-go_go-build-cache:/root/.cache/go-build -w /workspace etl-go-dev:latest sh -c 'go test ./internal/etl/server -count=1'`
- `podman run --rm -v "$PWD:/workspace" -v openetl-go_go-cache:/go -v openetl-go_go-build-cache:/root/.cache/go-build -w /workspace etl-go-dev:latest sh -c 'go test ./internal/etl/source ./internal/etl/sink -count=1'`
- `podman run --rm -v "$PWD:/workspace" -v openetl-go_go-cache:/go -v openetl-go_go-build-cache:/root/.cache/go-build -w /workspace etl-go-dev:latest sh -c 'go test ./internal/etl/... ./internal/cmd -count=1'`
- `npm --prefix web run build`
- `SKIP_UI=1 ./hack/pack.sh`

## [v0.2.5] — 2026-07-01 — 首次任务闭环、Redis 状态约束与 MaxCompute sink

### 亮点
- 将 `0.2.5-beta.1` / `0.2.5-beta.2` 的 AI context pack、受控 DAG 生成、组件文档和保存连接上下文能力收敛为正式版。
- 首次任务向导和 DAG 编辑器继续生成普通 pipeline/DAG spec，不引入专用执行路径；UI 展示 validate/preflight、field issue、readiness、guidance、recommendation 和 DDL preview，并支持对 preflight 推荐配置执行 Apply。
- 保存连接 context 扩展到 source/sink 双向：file/HTTP/demo sample、MySQL/PostgreSQL schema、Kafka topic/partition、MySQL/PostgreSQL/ClickHouse/Doris/Elasticsearch/Kafka sink 目标元数据，以及 File/S3/local-fallback 输出 target、prefix、format、可写或 bucket 存在性提示。
- 明确 runtime state/cache 与 SQL metadata storage 分离：Redis 是内置 state/cache 能力的唯一运行时后端；未配置 Redis 时，依赖缓存/状态的 lookup/enricher/deduplicate/window/join 配置会在 validate/preflight 阶段阻断，SQLite/MySQL/PostgreSQL 只作为 checkpoint、DLQ、audit、pipeline spec、worker/task 等持久化存储。
- MaxCompute/ODPS sink 从 writer-disabled 合约推进到 SDK-backed batch tunnel writer、远端表/分区/权限 preflight、错误分类、sink-local retry/backoff 和 metrics；由于仍缺真实 MaxCompute 环境的 DLQ/replay/e2e 证据，maturity 继续保持 experimental/beta 边界，不提升 production。
- Connector readiness 和 preflight recommendation 进入 API/UI：用户能在启动前看到 maturity gate、schema/preflight 缺口、幂等与 replay 建议、字段级 remediation 和安全修复动作。
- 继续清理 roadmap/spec 中偏 Flink 流计算平台的内容，保持项目定位在轻量、自托管、Source -> Transform -> Sink 的 CDC/ETL 同步、清洗和汇聚运行时。

### 验证
- `podman run --rm -v "$PWD:/workspace" -v openetl-go_go-cache:/go -v openetl-go_go-build-cache:/root/.cache/go-build -w /workspace etl-go-dev:latest sh -c 'go test ./internal/etl/server -count=1'`
- `podman run --rm -v "$PWD:/workspace" -v openetl-go_go-cache:/go -v openetl-go_go-build-cache:/root/.cache/go-build -w /workspace etl-go-dev:latest sh -c 'go test ./internal/etl/... -count=1'`
- `podman run --rm -v "$PWD:/workspace" -v openetl-go_go-cache:/go -v openetl-go_go-build-cache:/root/.cache/go-build -w /workspace etl-go-dev:latest sh -c 'go test ./internal/cmd -count=1'`
- `npm --prefix web run build`
- `./hack/pack.sh`
- `CONTAINER_CLI=podman ./hack/e2e-ui.sh` 当前回归已覆盖 93 个通过项；新增跨模板 saved-connection recommendation Apply 断言被撤回，不作为本次 release gate。

## [v0.2.5-beta.1] — 2026-06-29 — AI context pack 与受控 DAG 生成

### 亮点
- 新增由 connector descriptor、插件 schema、maturity metadata、组件文档、产品边界、DAG 规则、示例和常见错误生成的 AI context pack。
- 新增 `GET /api/v2/ai/context`，并将 `POST /api/v2/ai/generate` 改为使用 context pack，不再依赖硬编码 prompt；生成结果返回 `context_pack_version`、`validation` 和 `review`。
- AI review 会标记缺失必填字段、secret 确认、experimental/dev-only 成熟度、CDC 写 append sink 的重放风险、MaxCompute/ODPS writer-disabled、DDL apply、脚本 transform 和未启用 DLQ 等问题。
- DAG 编辑器 AI 面板会在应用到画布前展示 validation 状态、缺失字段、风险、确认项，以及当前 YAML 与生成 YAML 对照。
- 首次任务向导的 transform chain 支持增删、排序、切换 transform 类型和逐阶段 dry-run，同时仍生成普通 `transforms` 数组。
- 在 `docs/components/` 下补齐第一批核心 production-candidate source/sink/transform 组件文档，包含用途、字段、record 形态、checkpoint/DLQ/幂等边界、示例和证据。
- 更新 API/OpenAPI/Quickstart 文档和内嵌 UI 资源，明确 AI 辅助生成不能绕过 validate/preflight 和人工确认。

### 验证
- `npm --prefix web run build`
- `go test ./internal/etl/server ./internal/etl/transform -count=1`
- `podman run --rm -v "$PWD:/workspace" -v openetl-go_go-cache:/go -v openetl-go_go-build-cache:/root/.cache/go-build -w /workspace etl-go-dev:latest sh -c 'go test ./internal/etl/server ./internal/etl/transform -count=1'`
- `./hack/e2e-ui.sh` — 92 passed, 0 failed
- `./hack/pack.sh`

## [v0.2.4-beta.1] — 2026-06-29 — 连接上下文与 schema introspection

### 亮点
- 新增 `GET /api/v2/connections/{name}/context`，返回保存连接、connector descriptor、推荐调度/batch/checkpoint 参数，以及尽力而为的 source introspection。
- Source introspection 第一版覆盖 file/HTTP/demo 采样、MySQL/PostgreSQL database/table/column/primary key 元数据、Kafka topic/partition 元数据。
- 首次任务向导支持选择保存的 source/sink 连接，展示健康状态、schema/sample/topic/table 上下文，并生成带 `connection` 引用和推荐 batch/checkpoint 参数的普通 spec。
- DAG 编辑器节点属性支持展示保存连接 context，同时保持 DAG spec 使用现有 `connection` 字段。
- 更新 API 文档、OpenAPI metadata、内嵌 UI 资源，并扩展 UI e2e 的保存连接上下文覆盖。

### 验证
- `go test ./internal/etl/server -count=1`
- `web/` 下执行 `npm run build`
- `./hack/pack.sh`
- `./hack/e2e-ui.sh` — 92 passed, 0 failed

## [v0.2.3-beta-1] — 2026-06-27 — 首次任务 UI 与运行参数

### 亮点
- React UI 新增首次任务向导，覆盖数据库同步、Kafka 明细/聚合、Debezium CDC 同步、Kafka 报文解析、文件/HTTP 落地。向导生成普通 pipeline spec，YAML 仍作为可审计事实源。
- 向导支持由 schema 驱动的 source/sink/transform 配置表单、生成 YAML 编辑、YAML 回填表单、transform dry-run、validate + preflight，以及创建后启动。
- DAG 编辑器支持 YAML 与 canvas/form 往返、validate + preflight 操作，并结构化展示错误、warning、preflight issue、field issue、修复建议和 DDL preview。
- 后端新增 runtime CLI flags，覆盖配置文件、本地 data/log/plugin/schema/spec 目录、HTTP 与 ETL API 绑定地址、storage、TLS、API token、audit、日志格式，以及 standalone/master/worker 运行角色。运行配置优先级明确为 CLI flags > 环境变量 > 配置文件 > 内置默认值。
- 新增 `hack/container-cli.sh` 统一检测 Podman/Docker，并同步更新 e2e 脚本和文档中的容器运行时选择。

### 验证
- `go test ./internal/cmd ./internal/etl/server ./internal/etl/sink`
- `go run . --help`
- 非法 `--role` 启动前失败检查
- `E2E_SKIP_BUILD=1 ./hack/e2e-ui.sh` — 88 passed, 0 failed

## [v0.2.3-beta] — Doris 验证与调度约束

### 亮点
- 收紧 Doris sink 合约并补真实 FE/BE 验证：`ddl_policy` 默认改为 `reject`，schema validation 会校验目标表存在性、字段兼容性、Unique Key 与 `pk_columns` 是否一致，`ddl_policy=apply` 只允许安全的 add-column 变更。
- 修正 Doris 2.1 写入和 DDL 细节：Stream Load label 改为确定性生成，JSON/CSV header 显式设置，错误按 retry/DLQ 语义分类，auto-create 要求稳定主键，生成的 Unique Key DDL 使用 Doris 兼容的列顺序和类型推断。
- 新增 `hack/e2e-doris.sh` 并纳入 `hack/e2e-all.sh`；脚本支持 Podman 或 Docker，使用官方 Doris FE/BE 2.1.11 镜像验证 MySQL batch -> Doris 的 Stream Load JSON、Stream Load CSV、MySQL insert fallback、auto-create Unique Key、decimal 推断和零失败记录。
- 增加 source 绑定的调度元数据：source descriptor 暴露 `supported_schedules` 和 `default_schedule`，spec 会回填默认调度，并拒绝不支持的 `schedule.type`，同时校验 `cron`、`periodic`、`dependency` 的必填字段。
- DAG 编辑器会加载 connector descriptor，按当前 source 集合过滤 schedule 类型，支持 dependency schedule，并在切换 source 后重置不再支持的调度选择。

### 验证
- `CONTAINER_CLI="${CONTAINER_CLI:-$(command -v podman || command -v docker)}"; "$CONTAINER_CLI" run --rm -v "$PWD:/workspace" -v openetl-go_go-cache:/go -v openetl-go_go-build-cache:/root/.cache/go-build -w /workspace localhost/etl-go-dev:latest sh -c 'go test ./internal/etl/...'`
- `web/` 下执行 `npm run build`
- `E2E_SKIP_BUILD=1 ./hack/e2e-doris.sh`

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
- 新增 `hack/e2e-wide-table.sh`，基于 Docker 编排 Redpanda + MySQL + ClickHouse。
- 覆盖 Kafka -> lookup -> ClickHouse 明细 pipeline、Kafka -> deduplicate -> lookup -> tumbling aggregate -> ClickHouse 聚合 pipeline、重复 Kafka 消息吸收、schema drift 入 DLQ、lookup miss 入 DLQ 并修复后 replay、lookup refresh failure 入 DLQ、ClickHouse 下线入 DLQ 并恢复后 replay。

### 发布边界
- 这是 0.2.0 正式版。Kafka 编排式聚合、ClickHouse sink 使用方式、lookup stream-table join、tumbling 聚合、SQLite-backed state 可以作为已验证积木使用，但不宣称任意复杂链路或连接器矩阵 production-ready。
- 默认交付语义仍是 at-least-once。Exactly-once、Kafka rebalance/crash 保证、DAG/stateful replay、stream-stream production join、复杂 window、完整 connector certification 仍是 roadmap 项。

### 验证
- `./hack/e2e-wide-table.sh`
- `./hack/e2e-ui.sh` — 73 passed, 0 failed
- Docker：`go test -timeout 120s ./internal/etl/...`

## [v0.1.0-beta2] — Phase 5 可靠性与易用性发布

### 亮点
- 关闭 beta2 的 P0/P1 可靠性门槛：standalone runner 创建、文件源恢复、零幸存批次 checkpoint 安全、Postgres CDC pgoutput 解析、worker slot 限流、sink error metrics，以及 pipeline 硬性 preflight 错误拦截。
- 重整公开 quickstart 体验：规范 MySQL CDC -> ClickHouse 示例、对齐 Docker compose 配置、补全 `/api/v2/plugins/schema` 元数据，并更新 README / quickstart / 部署文档。
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
