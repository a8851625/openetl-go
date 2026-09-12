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
Objective: 草稿不再持久化 secret；连接选择、模板切换、推荐应用、实验 connector 均显式化；预检结果分层。
Scope: first-task-wizard.tsx、connections 相关、ConfirmDialog
Non-goals: 页面级问题（Round 3/4）。
Acceptance:
  1) localStorage 草稿中 secret 字段为哨兵值，恢复后留空并提示重新输入；
  2) 保存连接下拉按模板类型过滤；不兼容选择需确认；
  3) dirty draft 切模板有确认；
  4) 连接推荐不自动覆盖用户已改的 batch/checkpoint；
  5) maxcompute 显示实验徽标且默认不可选；
  6) source/sink 不可达时预检显示 "not ready to start"，主按钮 Create without starting；
  7) e2e-ui.sh 全绿。
Evidence: 同上 + 针对性 Playwright
Result: pending
```

## Round 3：页面事实一致性（P0-1/2/4 + P1-2/3/5/19）

```text
Round: 3/5
Roadmap item: UI-A.3
Profile/path: standalone Web UI pages + 后端 last_error 契约
Objective: Dashboard 不再伪造时间范围；启动失败在 Issues 可见；状态分桶正确；移动端向导可用。
Scope: DashboardPage、PipelinesPage、PipelineDetailPage、pipeline-health、i18n、internal/etl/server（stats last_error）、wizard 布局
Non-goals: 批量操作/调度页/高危确认（Round 4）。
Acceptance:
  1) Dashboard 无假时间切换；展示值与 API 累计值一致；
  2) 启动失败管道 Issues tab 显示错误（含 last_error 或 status=failed 派生）；
  3) 列表/总览状态分桶不再把 failed/completed 计入 stopped；Start all 只针对 stopped；
  4) View issues 直达 issues tab；
  5) 英文界面无中文硬编码 issue 文案；
  6) Production runtime 徽标反映真实 profile；
  7) 390px 视口向导无横向溢出（scrollWidth ≤ 视口宽）；
  8) go test ./internal/etl/... + 受影响路径证据重绑通过。
Evidence: go test、Playwright、e2e-ui.sh、check-connector-evidence -strict
Result: pending
```

## Round 4：页面操作安全与收尾（P1-4/17/18 + P2-1/2/3/4/6/7）

```text
Round: 4/5
Roadmap item: UI-A.4
Profile/path: standalone Web UI pages
Objective: 高危操作统一确认+结果汇总；Schedules 请求与状态诚实；P2 视觉/命名/可访问性收尾。
Scope: PipelinesPage、SchedulesPage、DLQPage、app-shell、styles、routing、configFields
Non-goals: 新 API（除 schedules 列表端点复用既有），transform 风险标注全量（只做分组）。
Acceptance:
  1) 批量启停/Run now/Stop all/checkpoint reset/连接删除/DLQ delete/replay 均有 ConfirmDialog 确认；
  2) 批量操作结果有汇总（成功/失败/原因）；
  3) Schedules 无 N+1（列表页 ≤2 请求/刷新），失败显示 unavailable；
  4) DLQ 按钮改名；Bell 移除；字体栈修正；transform 分组；未知 hash 有提示；
  5) DLQ 关闭有风险确认；ConfigForm label 绑定；
  6) e2e-ui.sh 全绿。
Evidence: Playwright + e2e-ui.sh
Result: pending
```

## Round 5：缓冲（溢出项/回归收口）

预留给前四轮溢出的修复、e2e 断言修缮与最终验收矩阵汇总。
