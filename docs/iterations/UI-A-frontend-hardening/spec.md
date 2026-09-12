# UI-A：前端交互与向导逻辑加固（2026-09-12 审计）

## WHY

v0.2.12-beta.19 发布后对 Web UI 做了两轮深度审计：

1. **页面交互审计**（易用性/美观/逻辑三维度，基于真实运行实例 + 390px 移动端实测）。
2. **创建向导六 Step 表单逻辑审计**（逐 Step 核对表单字段来源、步骤跳转、配置同步、预检/创建语义）。

结论：UI 的组件能力（descriptor 驱动字段、连接复用、预检、字段级错误、分阶段 dry-run）已经具备，
但存在一批 **事实失真、静默数据丢失、状态分裂与危险默认** 缺陷。核心问题是"界面展示的运行事实、
操作含义与后端实际语义不一致"，直接损害 CDC/ETL 控制台的核心价值：准确判断"哪里坏了、影响什么、
下一步做什么"。

本迭代不新增 connector、不改变 runtime 执行语义、不重构信息架构（那是 P4.3 的既有范围），
只修复两份审计确认的缺陷。

## 审计发现清单（合并去重，按优先级）

来源标记：[页面]=页面交互审计，[向导]=向导 Step 审计。

### P0（事实失真 / 静默数据丢失 / 安全）

| # | 发现 | 来源 | 位置 |
| --- | --- | --- | --- |
| P0-1 | Dashboard 时间范围切换是假控件：24h 显示 all-time 累计值，15m 显示 all-time×0.08 硬编码估算 | [页面] | `web/src/pages/DashboardPage.tsx` |
| P0-2 | 管道启动失败（如 sink Error 1045）时 detail Issues tab 显示 "No open issues"：渲染条件只看 records_failed/records_dlq/last_error，启动期失败三者皆空；Logs 有真实错误但 Issues/Overview 无。后端 stats 缺 last_error 是根因 | [页面] | `web/src/pages/pipelines/PipelineDetailPage.tsx`、`internal/etl/server` stats 契约 |
| P0-3 | 向导草稿把完整 secret（password/DSN/access key）自动写入 localStorage，与 API token"不得持久化"的策略矛盾 | [两份] | `first-task-wizard.tsx` auto-save |
| P0-4 | 移动端 390px 向导横向溢出（scrollWidth 729），Save draft/步骤按钮在可视区外 | [页面] | wizard 布局 |
| P0-5 | 表单 state 与 YAML 双真相源：确认页渲染表单值，实际提交 YAML.parse(yamlText)；YAML 手改后不 Sync 则确认页展示 A、创建 B；表单任意变化又会整体覆盖 YAML | [向导] | `buildSpec`/`setYamlText` effect/`createAndStart` |
| P0-6 | JSON 解析失败静默降级：source/sink/transforms/tableMapping 文本非法时 parseJSONText fallback {} / [] / null，字段被静默删除，无任何提示 | [向导] | `lib/format.ts` fallback + wizard |
| P0-7 | "Add transform" 默认插入空 `project`（fields 空 + keep_unmapped=false）→ 运行时把记录投影为 {}，静默清空所有字段 | [向导] | `addTransform` + `ProjectTransform.Apply` |
| P0-8 | "Open advanced DAG" 不传递向导配置：DAG editor 的 editTarget 只能加载已存在管道，草稿被丢弃，用户看到空画布 | [向导] | confirm step + `DagEditorPage` |
| P0-9 | source/sink 连接不可达时界面显示 "Preflight passed"，下一步直接 "Create and start" | [向导] | Safety step 结果渲染 |

### P1（操作语义 / 状态口径 / 引导正确性）

| # | 发现 | 来源 |
| --- | --- | --- |
| P1-1 | 管道路径/模式靠 tags 猜测：mysql_cdc 管道显示为 batch、`Source → — → Sink`；API 摘要缺真实 source/sink descriptor | [页面] |
| P1-2 | 列表 "View issues" 主按钮实际打开 Overview 而非 Issues tab | [页面] |
| P1-3 | stoppedCount = status!=='running'，failed/completed/scheduled 全被计入 "stopped" 并进入 Start all 目标；Dashboard 把 paused+stopped 合并显示为 Paused | [页面] |
| P1-4 | 批量启停无确认、forEach 并发、无结果汇总、toast 风暴 | [页面] |
| P1-5 | pipeline-health.ts 硬编码中文（'运行失败' 等）混入英文界面 | [页面] |
| P1-6 | 保存连接下拉不按模板允许类型过滤，选择后静默改写 sourceType/sinkType，破坏模板语义 | [向导] |
| P1-7 | 切换模板无确认，直接销毁全部已填配置（含连接、运行参数、预检结果） | [向导] |
| P1-8 | 连接 introspection（databases/tables/columns/PK/topics/writable）基本未被消费，用户仍手填 database/table/topic/pk_columns | [向导] |
| P1-9 | 选择保存连接时静默自动覆盖用户已设置的 batch_size/checkpoint_interval_sec；Apply 按钮形同虚设 | [向导] |
| P1-10 | Safety 参数只有 3 个可见，retry/backpressure 硬编码不可见 | [向导] |
| P1-11 | navigateToIssue 把 batch_/checkpoint_/retry 字段错误导航到 Scenario（实际在 Safety） | [向导] |
| P1-12 | createAndStart 中 Create 成功但 Start 失败时误报 "Pipeline creation failed"，用户被引导重复创建 | [向导] |
| P1-13 | Confirm 页摘要不含实际 endpoint/连接/mapping/写入语义/preflight 新旧度，不足以支撑生产确认 | [向导] |
| P1-14 | 实验性 maxcompute 出现在普通 sink 下拉，直到第 5 步预检才阻断 | [向导] |
| P1-15 | kafka-detail 模板用 deduplicate `key_fields`，schema 表单要求 `keys`，模板/表单/后端 alias 三方不一致 | [向导] |
| P1-16 | multi-table 模板 buildSpec 静默 delete sink.table；table mapping 是裸 JSON 无预览无冲突检查 | [向导] |
| P1-17 | Schedules 页 N+1 请求（每管道一个 /schedule），API 失败伪装成 disabled；"next run" 列实际是 schedule 描述 | [页面] |
| P1-18 | Run now / Stop all / checkpoint reset 等高危操作无确认（共享 confirmAction 用 window.confirm，自有 ConfirmDialog 闲置） | [页面] |
| P1-19 | "Production runtime" 眉头静态文案不随真实 profile 变化 | [页面] |

### P2（一致性 / 可访问性 / 视觉收尾）

| # | 发现 | 来源 |
| --- | --- | --- |
| P2-1 | 顶栏 Bell 按钮无 onClick 的死控件 | [页面] |
| P2-2 | Geist/Geist Mono 字体声明但无加载资源，跨机回退不一致 | [页面] |
| P2-3 | DLQ "Dry run" 实为"当前页 ≤50 条影响预览"，命名越权 | [页面] |
| P2-4 | transform 下拉平铺 39 项不分组不标注风险/成熟度 | [向导] |
| P2-5 | ConfigForm label 无 htmlFor 绑定、字段名裸露内部命名、移动/删除 transform 无风险提示 | [向导] |
| P2-6 | 关闭 DLQ 无风险确认 | [向导] |
| P2-7 | 全局搜索/自动刷新缺控制反馈；未知 hash 静默回退 Dashboard（#/pipeline-new 不被识别） | [页面] |

## 不做什么（Non-goals）

- 不新增 connector、不改 runtime 执行/checkpoint/DLQ 语义。
- 不做 P4.3 的信息架构重构（任务分组导航、可分享 URL 体系）。
- 不引入 metrics 历史时序 API（P0-1 的修复是"诚实展示累计值/隐藏假切换"，不是造时序数据）。
- 不重写向导为全新框架；在现有组件上修复。

## 验收（总体）

- 上述 P0 项全部修复并有自动化或 Playwright 证据。
- P1 项按增量交付，每个增量有领取记录与验收矩阵。
- `npm run typecheck` / `npm run build` / `npm run lint`（无新增 warning）通过。
- `hack/e2e-ui.sh` 全量通过（若断言依赖被修复的旧行为，同步更新测试并说明）。
- 后端变更（仅 P0-2 的 last_error 契约）单独成增量，push 前按证据门禁流程重跑认证。
