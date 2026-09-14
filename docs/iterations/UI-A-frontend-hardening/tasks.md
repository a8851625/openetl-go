# UI-A 任务分解

 Rounds 按依赖排序；每轮一个可领取增量，验收可独立验证。

## Round 1：向导配置完整性（P0-5/6/7/8）

```text
Round: 1/5
Roadmap item: UI-A.1
Profile/path: standalone Web UI wizard
Objective: 向导表单成为唯一真相源；JSON/YAML 解析失败可见且阻断；Add transform 不再默认清空字段；DAG 编辑器入口不丢配置。
Scope: web/src/pages/pipelines/first-task-wizard.tsx、web/src/DagEditorPage.tsx、web/src/i18n.ts、web/src/main.tsx、hack/e2e-ui.sh
Non-goals: secret 草稿、连接过滤、预检分层（Round 2+）；runtime 语义。
Acceptance:
  1) source/sink/transforms JSON 非法时显示行级错误，Next/Validate/Create 被禁用；
  2) YAML 手改后表单冻结只读，Apply/Discard 二选一；解析失败禁止继续；
  3) 确认页摘要与实际提交 spec 逐字段一致（YAML dirty 时从 YAML 渲染并提示来源）；
  4) Add transform 默认 identity；空 project 配置显示阻断错误；
  5) Open in DAG editor 携带当前配置（sessionStorage seed），画布 3 节点非空且名字保留；
  6) hack/e2e-ui.sh 全绿。
Evidence: npm run typecheck/build 通过；npm run lint 32 warnings 全为既有；CONTAINER_CLI=podman IMAGE=openetl-go-etl:ui-a2 E2E_SKIP_BUILD=1 bash hack/e2e-ui.sh → 127 passed / 0 failed（含新增 A1.1–A1.4b 共 10 条断言）。
Result: delivered
Residual/follow-up: 无（A1 中发现的 evaljs 嵌套引号陷阶已通过专属 JSON 编辑器 testid + fill 规避）。
```

验收矩阵：

| Criterion | Evidence | Result | Residual/blocker |
| --- | --- | --- | --- |
| JSON 非法显示错误且阻断 Next/Validate/Create | e2e A1.1/A1.1b/A1.1c（fill invalid → wizard-json-parse-error + Next disabled；恢复后解锁） | passed | — |
| YAML dirty 冻结表单，Apply/Discard 二选一 | e2e A1.2（dirty banner + discard 按钮）；A1.2b（confirm 显示 YAML 值 321 而非表单 100 + 来源提示）；A1.2c（discard 后恢复 batch_size:100 且 banner 消失） | passed | — |
| 确认页与提交 spec 一致 | submissionSpec 直接从 yamlDirty?YAML:buildSpec() 派生，e2e A1.2b | passed | — |
| Add transform 默认 identity；空 project 阻断 | e2e A1.3（默认 identity）、A1.3b（wizard-transform-project-danger 警示） | passed | — |
| DAG 入口携带草稿 | e2e A1.4/A1.4b（#/designer + ≥3 节点 + 名字保留；sessionStorage etl_dag_seed_v1 消费后即清） | passed | — |
| 既有行为回归 | e2e-ui.sh 全量 127 passed / 0 failed；lint/typecheck 无新增告警 | passed | — |

## Round 2：向导语义与安全（P0-3/9 + P1-6/7/9/14）

```text
Round: 2/5
Roadmap item: UI-A.2
Profile/path: standalone Web UI wizard + connections
Objective: 草稿不再持久化 secret；连接选择、模板切换、推荐应用、实验 connector 均显式化；预检结果分层；Create/Start 失败分离反馈。
Scope: first-task-wizard.tsx、i18n.ts、hack/e2e-ui.sh
Non-goals: 页面级问题（Round 3/4）。
Acceptance:
  1) localStorage 草稿中 secret 字段（含 yamlText 镜像）为哨兵值，恢复后留空并提示重新输入；
  2) 保存连接下拉按模板类型过滤，不兼容项归入显式 optgroup 并标注模板名；
  3) dirty draft 切模板弹 ConfirmDialog，确认后才重置；
  4) 连接推荐不自动覆盖用户已改（touched）的 batch/checkpoint；
  5) maxcompute 选项显示 Experimental 标注，选中后显示阻断警示 banner；
  6) source/sink 不可达时预检显示 not ready to start；Confirm 主按钮变为 Create without starting，Start despite warnings 需勾选；
  7) Create 成功但 Start 失败时明确区分反馈，不误导重复创建。
Evidence: npm run typecheck/build/lint；CONTAINER_CLI=podman IMAGE=openetl-go-etl:ui-a2 E2E_SKIP_BUILD=1 bash hack/e2e-ui.sh → 133 passed / 0 failed（含 A2.1–A2.4 新断言）。
Result: delivered
Residual/follow-up: 无。
```

验收矩阵：

| Criterion | Evidence | Result | Residual/blocker |
| --- | --- | --- | --- |
| 草稿不存明文 secret（含 YAML 镜像） | e2e A2.1（JSON scrub + YAML 行级 scrub，哨兵替换，明文不出现） | passed | — |
| 连接按模板过滤 | 实测 optgroup "Incompatible with this template (switches source type)"，不兼容项标注 (not in <template>) | passed | — |
| 模板切换确认 | e2e A2.2（dirty 弹框且 name 保留）/ A2.2b（确认后重置为模板默认） | passed | — |
| 推荐不覆盖用户值 | runtimeTouched 标记，loadConnectionContext 仅在未 touched 时应用推荐 | passed | 单元级断言待后续如有需要 |
| 实验 connector 显式化 | e2e A2.4（option 标注 + 选中后 warning banner） | passed | — |
| 预检分层 | e2e A2.3（not ready to start）/ A2.3b（Create without starting + 勾选门控 Start） | passed | — |
| Create/Start 失败分离 | createPipeline(start) 拆分，start 失败 toast 明示 "created, but start failed" | passed | 代码路径覆盖，e2e 断言待后续 |
| 既有行为回归 | e2e-ui.sh 全量 133 passed / 0 failed | passed | — |

## Round 3：页面事实一致性（P0-1/2/4 + P1-2/3/5/19）

```text
Round: 3/5
Roadmap item: UI-A.3
Profile/path: standalone Web UI pages + 后端 stats/health 契约
Objective: Dashboard 不伪造时间范围；启动失败在 Issues 可见；状态分桶正确；View issues 直达；i18n 化 issue 文案；runtime 徽标真实化；移动端向导无溢出。
Scope: DashboardPage、PipelinesPage、PipelineDetailPage、pipeline-health、i18n、internal/etl/pipeline/pipeline.go（markStartFailed）、internal/etl/server/server.go（health profile）、wizard 布局、hack/e2e-ui.sh
Non-goals: 批量操作/调度页/高危确认（Round 4）。
Acceptance:
  1) Dashboard 无假时间切换；展示值与 API 累计值一致；
  2) 启动失败管道 Issues tab 显示错误（后端 stats 补 last_error/startup_failed，不覆盖 checkpoint 类错误码）；
  3) 列表/总览状态分桶不再把 failed/completed 计入 stopped；Start all 只针对 stopped；
  4) View issues 直达 issues tab；
  5) 英文界面无中文硬编码 issue 文案（titleKey + issueTitle）；
  6) Production runtime 徽标反映真实 profile/role（health 暴露 profile + insecure_dev）；
  7) 390px 视口向导无横向溢出；
  8) go test -race ./internal/etl/pipeline ./internal/etl/server + e2e 全绿。
Evidence: go test -race（pipeline 63s / server 98s ok）；CONTAINER_CLI=podman IMAGE=openetl-go-etl:ui-a3 E2E_SKIP_BUILD=1 bash hack/e2e-ui.sh → 139 passed / 0 failed（含 A3.1–A3.4）。
Result: delivered
Residual/follow-up: 无。
```

验收矩阵：

| Criterion | Evidence | Result | Residual/blocker |
| --- | --- | --- | --- |
| Dashboard 假时间范围移除 | e2e A3.2b（dash-scope-badge 存在，无 Last 24 hours 切换；×0.08 估算代码删除） | passed | — |
| 启动失败进入 Issues | 后端 markStartFailed(stage, err) 写 stats；实测 open sink 错误在 Issues tab 可见（A3.1/A3.1b）；checkpoint 类错误码不被覆盖（D2.7a 回归通过） | passed | — |
| 状态分桶 | e2e A3.3（failed 不计 stopped，other states 徽标）；Start all 目标只含 status==stopped | passed | — |
| View issues 直达 | 实测点击后 hash 落在 /issues tab（带 pipeline id） | passed | — |
| issue 文案 i18n | titleKey + issueTitle() 翻译；英文环境无中文拼接 | passed | — |
| runtime 徽标真实化 | health 暴露 profile/insecure_dev；eyebrow 显示 development · standalone（A3.2） | passed | — |
| 移动端无溢出 | e2e A3.4（390px scrollWidth ≤ innerWidth+2） | passed | — |
| Go/前端回归 | go test -race 两包 ok；typecheck/build/lint 无新增告警；e2e 139 passed | passed | — |

## Round 4：页面操作安全与收尾（P1-4/17/18 + P2-1/2/3/4/6/7）

```text
Round: 4/5
Roadmap item: UI-A.4
Profile/path: standalone Web UI pages
Objective: 高危操作统一确认+结果汇总；Schedules 请求与状态诚实；P2 视觉/命名/可访问性收尾。
Scope: PipelinesPage、SchedulesPage、DLQPage、app-shell、styles、routing、main.tsx、configFields、first-task-wizard、i18n、hack/e2e-ui.sh
Non-goals: 新 API；transform 风险标注全量。
Acceptance:
  1) 批量启停有 ConfirmDialog 确认（显示目标数量/名单）与结果汇总面板（成功/失败/原因），受控并发 4；Start/Stop all 无目标时禁用；
  2) Run now 有确认；Schedules 去除 selected 依赖的 N+1 重拉，失败行显示 unavailable；
  3) DLQ 预览按钮改名 Preview impact (≤50 loaded)；向导关闭 DLQ 有风险确认（非阻塞 ConfirmDialog）；
  4) 死 Bell 按钮移除；Geist 未加载字体声明移除；transform 下拉按意图分组（6 组 + 未分组）；
  5) 未知路由显示 not-found 页；legacy #/pipeline-new 别名进入向导；ConfigForm label htmlFor 绑定；
  6) selection Start 与 Start-all 口径统一（只针对 stopped）。
Evidence: npm typecheck/build/lint（32 warnings 低于基线 34）；CONTAINER_CLI=podman IMAGE=openetl-go-etl:ui-a4 E2E_SKIP_BUILD=1 bash hack/e2e-ui.sh → 145 passed / 0 failed（含 A4.1–A4.3）。
Result: delivered

```text
Round: 5/5
Roadmap item: UI-B.1（原 UI-A 残留 P1-1 真实管道摘要契约）
Profile/path: standalone API + Web UI
Objective: 列表/详情的模式与拓扑来自后端事实而非 tags 猜测。
Scope: internal/etl/server/server.go（buildSpecSummary + spec_summary 字段）、web/src/lib/types.ts、web/src/lib/pipeline-health.ts、hack/e2e-ui.sh（B1 断言块）
Non-goals: 完整 spec 展开；P1-8 introspection 消费；write_mode 深层校验。
Acceptance:
  1) GET /api/v2/pipelines 每条记录携带 spec_summary（source/source_mode/transforms/sink/write_mode/schedule/dag_*），config 值（密码等）绝不出现；
  2) source_mode 分类：mysql_cdc 等为 cdc；kafka/http/redis 为 streaming；cron/periodic/dependency 为 scheduled；once/streaming 默认值不误判为 scheduled；DAG 为 dag；
  3) 前端 deriveModeLabel/derivePipelinePath 优先消费 spec_summary，无字段时回退 tags（旧后端兼容）；
  4) 单测覆盖分类矩阵与脱敏；e2e B1.1-B1.3 断言列表真实渲染。
Evidence: go test ./internal/etl/server/ 全绿（含 TestBuildSpecSummary 5 例）；CONTAINER_CLI=podman IMAGE=openetl-go-etl:ui-b1 E2E_SKIP_BUILD=1 bash hack/e2e-ui.sh → 148 passed / 0 failed；live 验证 batch/scheduled/streaming 三管道 mode+path 正确。
Result: delivered

```text
Round: 2/5（UI-B 窗口）
Roadmap item: UI-B.2（向导逻辑小项打包：P1-10/11/13/15/16 + demo 凭据 + 模板诚实标注）
Profile/path: standalone Web UI wizard
Objective: Safety 隐藏参数显式化；错误导航修正；deduplicate 字段归一；多表 sink.table 诚实处理；凭据/模板/文案诚实。
Scope: web/src/pages/pipelines/first-task-wizard.tsx、web/src/i18n.ts、web/src/main.tsx（移除未用 plugins prop）、hack/e2e-ui.sh（B2 断言块）
Non-goals: P1-8 introspection 消费；window.confirm 全量替换；i18n 全量覆盖。
Acceptance:
  1) P1-10 Safety 新增折叠式 Retry & backpressure 区（max_attempts/initial_ms/max_ms/buffer 可编辑，持久化进草稿与 YAML）；
  2) P1-11 navigateToIssue 将 schedule/retry/batch_/checkpoint_/backpressure_ 类字段错误路由到 Safety（原误路由 Scenario）；
  3) P1-15 deduplicate 配置归一：提交时 key_fields→keys（后端只认 keys，旧字段被静默忽略导致全记录去重）；模板自身改用 keys；
  4) P1-16 多表模板不再静默丢 sink.table：mapping 存在时才丢弃并显示琥珀提示（wizard-sink-table-mapping-hint）；无 mapping 时保留用户输入；
  5) demo 凭据（sync_password_123/dzh123456/minioadmin/dzh3136_go 库名）全部清空为空串占位，首填界面不再展示示例密码；
  6) cdc-wide-table 模板卡显示 Needs completion 徽标（lookup dsn/query 未预填）；transform dry-run 按钮旁新增作用域提示（仅单样例，不验 source/sink/checkpoint）；
  7) Confirm 页新增 Write mode、Retry/buffer、Schedule 行（提交语义可见）。
Evidence: npm typecheck 0 错误；lint 30 warnings（低于 34 基线，净减 2：清除 plugins/recommendationValue 未用变量）；build 2.33s；CONTAINER_CLI=podman IMAGE=openetl-go-etl:ui-b2 E2E_SKIP_BUILD=1 bash hack/e2e-ui.sh → 153 passed / 0 failed（新增 B2.1–B2.5）。
Result: delivered

```text
Round: 3/5（UI-B 窗口）
Roadmap item: UI-B.3（window.confirm 统一替换 + 自动刷新控制）
Profile/path: standalone Web UI
Objective: 破坏性操作全部走共享 ConfirmDialog；自动刷新可暂停并带新度/失败反馈。
Scope: web/src/{SchedulesPage,DLQPage,main.tsx,i18n.ts,pages/pipelines/{PipelinesPage,PipelineDetailPage,pipeline-modals}.tsx,components/{layout/app-shell,shared/confirm-dialog}.tsx}、hack/e2e-ui.sh（B3 断言块）
Non-goals: ConfirmDialog pending/error 态（后续）；P1-8；i18n 全量。
Acceptance:
  1) 全部 6 处 window.confirm/confirmAction 调用点（Schedules Run now、DLQ Delete all、版本回滚×2、checkpoint reset、管道删除）替换为声明式 ConfirmDialog；confirmAction 导出删除，仓库 0 残留；
  2) 自动刷新：topbar 控件可暂停/恢复（aria-pressed），label 显示上次刷新时间或 paused/failed 态；暂停后 5s interval 停止；
  3) 新增 i18n 键（ui.cancel、各确认标题、autorefresh 态）双语；
  4) e2e B3.1–B3.2b + L1 适配全绿。
Evidence: npm typecheck 0 错误；lint 29 warnings（再减 1）；build 2.50s；CONTAINER_CLI=podman IMAGE=openetl-go-etl:ui-b3 E2E_SKIP_BUILD=1 bash hack/e2e-ui.sh → 156 passed / 0 failed。
Result: delivered

```text
Round: 4/5（UI-B 窗口）
Roadmap item: UI-B.4（P1-8 连接 introspection 消费）
Profile/path: standalone Web UI
Objective: 向导消费连接探测数据——库/表/topic 选择器与列/PK/目标事实展示，替代手填。
Scope: web/src/{pages/pipelines/first-task-wizard.tsx,i18n.ts,lib/types.ts}、hack/e2e-ui.sh（B4 断言块）
Non-goals: 连接管理页改造；introspection 后端扩展（现有 API 已足够）。
Acceptance:
  1) mysql/pg/CH/doris 家族 source/sink：database 下拉 → 表下拉（显示 PK），选中写回 config（source 且有 PK 时自动填 pk_columns）；
  2) kafka：topic 下拉（分区数提示）；
  3) file/S3 sink：目标事实（kind/location/prefix/exists/writable）展示；
  4) file/http/demo source：schema chips（列名+类型）展示；
  5) 选择写回通过 mergeConfigText 保留用户其余手填项。
Evidence: npm typecheck 0 错误；lint 29 warnings 持平；build 2.63s；CONTAINER_CLI=podman IMAGE=openetl-go-etl:ui-b4 E2E_SKIP_BUILD=1 bash hack/e2e-ui.sh → 158 passed / 0 failed（B4.1 schema chips、B4.2 sink target facts）。
Result: delivered
Residual/follow-up: db/table picker 对真实 MySQL/CH 连接的 e2e 留待有依赖容器的环境；wizard i18n 全量、全局搜索按钮、Dashboard 密度、行交互模型为小项池。
```

验收矩阵：

| Criterion | Evidence | Result | Residual/blocker |
| --- | --- | --- | --- |
| 批量确认与汇总 | batchAction → ConfirmDialog（数量+前 5 名）+ runBatch 受控并发 4 + 结果面板；e2e A4.1/A4.1a（无目标禁用） | passed | 真实多目标交互流待依赖服务 e2e |
| Run now 确认 | confirmAction(sched.confirmRunNow) | passed | — |
| Schedules N+1/状态诚实 | load effect 去除 selected 依赖；失败行 unavailable 徽标 | passed | — |
| DLQ 命名/关闭确认 | i18n dlq.dryRun 改名；pendingDlqDisable ConfirmDialog（非阻塞） | passed | e2e D2.1g 已适配 |
| 死控件/字体/分组/路由 | Bell 移除；system 字体栈；TRANSFORM_GROUPS 6 组；not-found 页 + pipeline-new 别名（A4.2/A4.2b） | passed | — |
| ConfigForm 可访问性 | Label htmlFor + input id（全部输入类型） | passed | — |
| 回归 | e2e-ui.sh 145 passed / 0 failed；lint 32（<基线 34） | passed | — |

## Round 5：缓冲（溢出项/回归收口）

预留给前四轮溢出的修复、e2e 断言修缮与最终验收矩阵汇总。
