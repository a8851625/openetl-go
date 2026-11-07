# UI-F 任务分解

> 状态取值：`todo` / `active` / `done` / `blocked`。实施时一次只允许一个 `active`。`P0/P1/P2` 是问题严重度，不自动改变 Roadmap 的领取顺序。2026-09-27：UI-F.1–UI-F.7 全部 delivered（收口轮 265/265）；UI-E.1 仍准确记录为 `blocked_external`（缺目标用户研究输入）。

## Round 分组

| Round | 包含 task | 目标 |
| --- | --- | --- |
| 1/5 | UI-F.1 | 让失效 Pipeline URL 不能把高影响动作打到错误对象。`done` 2026-09-27 |
| 2/5 | UI-F.2、UI-F.3 | 让 route-owned dialog 与破坏性删除在动作前表达真实意图。UI-F.2 `done` 2026-09-27；UI-F.3 `done` 2026-09-27。 |
| 3/5 | UI-F.4、UI-F.6 | 让控制面读取失败、空数据、任务 telemetry、运行快照和用户文案不互相冒充。UI-F.4、UI-F.6 `done` 2026-09-27。 |
| 4/5 | UI-F.5 | 在 UI-D.3 共享基线已冻结后，收口未覆盖页面的小屏 containment。`done` 2026-09-27。 |
| 5/5 | UI-F.7 | 真实 app 容器回归、证据回填和交付收口。`done` 2026-09-27。 |

## 任务表

| ID | 任务 | 依赖 | 状态 | 证据落点 |
| --- | --- | --- | --- | --- |
| UI-F.1 | 阻断无效 detail route 回退与错误对象 lifecycle 请求 | — | `done`（2026-09-27 Round 1） | `main.tsx`、`PipelineDetailPage.tsx`、routing tests、`hack/e2e-ui.sh` |
| UI-F.2 | 使 Settings dialog 的可见性、hash、焦点和关闭行为一致 | UI-F.1 | `done`（2026-09-27 Round 2） | `main.tsx`、Settings/Dialog、Playwright route/focus tests |
| UI-F.3 | 为 Worker/Connection/Plugin 删除建立确认与影响未知性基线 | UI-F.2 | `done`（2026-09-27 Round 2b） | 三个页面、ConfirmDialog、i18n、DELETE request tests |
| UI-F.4 | 分离 loading/error/empty/data，并修正 task/run 状态事实表达 | — | `done`（2026-09-27 Round 3） | Audit/Connectors/MyPlugins/Workers/Detail、503 browser tests |
| UI-F.5 | 协同 UI-D.3 修复未覆盖页面的 390px containment 与局部可操作性 | UI-D.3 约束冻结、UI-F.4 | `done`（2026-09-27 Round 4） | AppShell/table/详情/Workers/Audit/Catalog/DAG、viewport tests |
| UI-F.6 | 修复 Extensions i18n 泄漏和稳定字段 label 语义 | UI-F.3、UI-F.4 | `done`（2026-09-27 Round 3） | i18n、MyPlugins/Settings/Schedules/DAG、EN/ZH a11y tests |
| UI-F.7 | 在真实 app container 复验并回填 Roadmap / 原始实测报告 | UI-F.1–UI-F.6 | `done`（2026-09-27 Round 5） | typecheck/lint/build、`hack/e2e-ui.sh`、ROADMAP、review report |

## 任务明细

### UI-F.1 Detail route identity guard（PW-R1，P0）

**验收**：

1. 直接打开、刷新、back/forward 到 `#/pipelines/not-a-real-pipeline/<tab>` 时，页面显示明确 not-found；不能把列表第一条或之前 selected pipeline 呈现为目标对象。
2. 无效/加载失败 detail 中不存在 Start、Stop、Reset checkpoint、Open DLQ、Open designer 等会引用其他 pipeline 的可执行入口；网络观察中零 `start`/`stop`/`checkpoint/reset`/DLQ 操作请求。
3. 有效 ID 的 overview/runs/issues/checkpoints/logs/topology/spec 仍直达；标题、crumb、metric 和 action target 均来自 route 匹配对象。
4. route key 与 display name 的兼容匹配规则显式、单一且测试化；不因为空列表、刷新或 selected state 变化回退到 `pipelinesList[0]`。

**允许范围**：`web/src/main.tsx`、`web/src/pages/pipelines/PipelineDetailPage.tsx`、必要的 `routing.ts`/route tests、`hack/e2e-ui.sh`。

**非目标**：不扩展 DLQ/Issue/Connection 的 URL context（归 UI-E.3）；不改变 pipeline API、状态派生、checkpoint 或 replay 语义。

**证据落点**：direct-route browser request interception、route unit test、真实 app container UI E2E。

### UI-F.2 Settings route/dialog 单一真相源（PW-R2）

**验收**：

1. `#/settings` 只在该 route 下显示 Settings；任何 `next.page !== 'settings'` 的 hash change 都使 dialog 关闭。
2. `#/settings → #/schedules` 后，Schedules 的 Run now 等主操作可被点击；不存在遮罩/overlay 截获 pointer/focus。
3. Close、Esc、back、forward 后 hash、visible dialog 和焦点恢复行为符合已记录规则，且不进入 history loop。
4. settings token/LLM/worker settings 表单当前保存、读取、语言切换与 token persistence 行为不回归。

**允许范围**：`web/src/main.tsx`、`web/src/pages/SettingsPage.tsx`、必要 shared Dialog、`i18n.ts`、Playwright / `hack/e2e-ui.sh`。

**非目标**：不新增设置路由层级、不改 settings API、token 存储安全边界或 LLM 配置语义。

**证据落点**：hash、keyboard、focus、pointer-interaction 浏览器断言；真实 app UI 回归。

### UI-F.3 删除确认与影响未知性（PW-R3）

**验收**：

1. Worker deregister、Connection delete、Plugin uninstall 首击只显示当前语言的 ConfirmDialog；Cancel、Esc、overlay/Close 都不发 DELETE。
2. Confirm 后只对显示对象发起一次 DELETE；成功、失败和刷新结果对用户可见，失败不被 success toast 覆盖。
3. 连接确认框由权威 API reference facts 驱动；当前无此数据时写明“引用关系不可确认”，不得虚构 0 个受影响 pipeline。
4. Dialog 含对象名、操作后果/恢复边界、可访问名称；EN/ZH 文案无 raw i18n key 或英文 fallback。

**允许范围**：`web/src/WorkersPage.tsx`、`web/src/ConnectionsPage.tsx`、`web/src/MyPluginsPage.tsx`、shared ConfirmDialog/Dialog、`i18n.ts`、相关 UI E2E。

**非目标**：不改变 DELETE endpoint、服务端授权、connection 引用查询契约或 plugin/worker 生命周期；权威引用 API 另按 UI-E.5 / 经批准后端 task 实现。

**证据落点**：Playwright dialog + request count/target 断言、容器 UI E2E、必要 API contract test。

### UI-F.4 控制面状态真实性（PW-R5、PW-R7、PW-R8）

**验收**：

1. Audit、Connector catalog、Extensions 的 loading/error/empty/data 分支互斥：HTTP 503 显示可解释 error + retry，不显示“无记录/未安装/未找到”；成功空响应才显示 empty。
2. Workers 任务区区分 registered workers、online workers、暂无 task、task telemetry unavailable；有 worker 时不得称“没有 worker”。不造假 task rows。
3. Detail 的当前 runtime snapshot 与 storage-backed run history 区分命名、帮助文本和空态；未接历史数据时不暗示已有完整历史。
4. error 说明当前无法判断的范围并保留可读 API details；不以客户端刷新时间、空数组或累计值推断 runtime 事实。

**允许范围**：`web/src/pages/AuditPage.tsx`、`web/src/pages/ConnectorsPage.tsx`、`web/src/MyPluginsPage.tsx`、`web/src/WorkersPage.tsx`、`web/src/pages/pipelines/PipelineDetailPage.tsx`、`web/src/main.tsx`、`i18n.ts`、UI E2E。

**非目标**：不新增任务 telemetry、run history 或 incident API；不修改 polling/retry、audit 存储或 runtime metrics 契约。

**证据落点**：浏览器 503/success-empty/data route interception、应用容器 API failure test（可用时）、文案/状态 snapshot。

### UI-F.5 小屏 containment 与局部操作（PW-R4）

**验收**：

1. 在 UI-D.3 的共享布局策略已冻结后，390px 下 Detail Runs、Workers、Audit、Connector catalog、DAG Designer 满足 `scrollWidth <= innerWidth`；同一矩阵包含 UI-D.3 的 Connections/Schedules/My Plugins，不允许回退。
2. AppShell main flex child 可收缩，但不通过全局 `overflow-x:hidden` 隐藏内容；宽表仅在可识别的 local scroller 中横滚，画布只在自身 pan/zoom 边界内扩展。
3. Schedules 仍按 UI-D.3 处理其表列；DAG palette 的 Router/Tap/Rate Limiter/Lookup/Enricher 等不会撑开 document，仍可用键盘/触屏选择，画布可查看和编辑。
4. 1280px 保持桌面密度、表列与 DAG interaction；关键状态和危险操作有文本或等价 accessible name。

**允许范围**：`web/src/components/layout/app-shell.tsx`、必要 `table.tsx`/styles、Detail/Workers/Audit/Connectors/DagEditor 及与 UI-D.3 协调的页面、`i18n.ts`、UI E2E。

**非目标**：不把移动端变成第二套数据/操作模型，不删除关键列/危险操作以满足断言，不重写 DAG 业务逻辑。

**阻塞条件**：UI-D.3 未冻结其 AppShell/table responsive 合同时，记录 `blocked` 并协调用户决定；不得并行做冲突的全局样式改动。

**证据落点**：390px/1280px `scrollWidth`、local scroller、focus/action、DAG palette/canvas 交互断言；真实 app UI E2E。

### UI-F.6 i18n 与字段语义（PW-R6、PW-R9）

**验收**：

1. Extensions installed plugins 表和 destructive button 不显示任何 raw i18n key；EN/ZH 的 kind/version/uninstall/confirm/cancel/close 都正确。
2. Settings API token、Schedules cron、DAG pipeline name 及本 task 改动的其他输入都有稳定 label relation（`id + htmlFor` 或 `aria-labelledby`）；placeholder 只作例子。
3. 有错误/帮助的字段通过 `aria-describedby` 将文本与 input 关联；输入后、语言切换后、键盘焦点路径中名称仍可理解。
4. 自动化扫描或测试会失败于已知 key 模式泄漏，避免仅靠人工发现。

**允许范围**：`web/src/i18n.ts`、`web/src/MyPluginsPage.tsx`、`web/src/pages/SettingsPage.tsx`、`web/src/SchedulesPage.tsx`、`web/src/DagEditorPage.tsx`、相关 UI test。

**非目标**：不更改 token storage、cron parser、DAG spec schema、plugin ABI 或 API payload。

**证据落点**：EN/ZH browser text、ARIA snapshot/role-name assertion、i18n regression test、容器 UI E2E。

### UI-F.7 真实容器验证与证据收口

**验收**：

1. 在实际 app container（非 fixture）中执行 UI-F.1–UI-F.6 的关键路径；无效 detail、Settings route、每类删除确认、至少一个 API 读取失败/拒绝、390px 和 EN/ZH 均有覆盖。
2. `cd web && npm run typecheck && npm run lint && npm run build`、受影响 Playwright 测试、`CONTAINER_CLI=<docker|podman> bash hack/e2e-ui.sh` 均记录实际输出；未具备容器/故障注入条件时准确标 `blocked_external`/`skipped`。
3. ROADMAP UI-F、UI-F tasks、[原始补充实测报告](../../ui-playwright-supplemental-review-2026-09-25.zh.md) 回填完成项、失败证据和残留；不删除或改写原始结论以制造“全绿”。
4. 审查最终 diff：无 API mock、临时截图、生成 bundle、token/secret；`git diff --check` 通过。

**允许范围**：验证脚本、受影响前端/测试、`docs/ROADMAP.zh.md`、UI-F 文档、原始实测报告的状态回填。

**非目标**：不借验收轮加入新的产品能力、incident 聚合、connection reference 后端 API 或大规模 IA 改造。

**证据落点**：命令日志、app-container UI E2E、Roadmap 验收矩阵、最终 diff。

## Round 1 Claim 记录

```text
Round: 1/5（UI-F 窗口）
Roadmap item: UI-F / UI-F.1
Profile/path: standalone production Web UI + hash route identity safety
Objective: 无效 pipeline detail route 绝不显示或向其他 pipeline 发出 lifecycle/checkpoint/DLQ 动作；有效 detail 的七个 tab 保持可达
Why eligible: UI-D.3 的共享小屏基线已 delivered；排在 UI-F 前的 UI-E.1 已明确 blocked_external（外部 3–5 用户研究输入缺失），用户授权持续处理可实施 roadmap 项
Scope: web/src/main.tsx、web/src/pages/pipelines/PipelineDetailPage.tsx（仅必要 loading/error/not-found 标识）、必要 routing test、hack/e2e-ui.sh、UI-F 文档/ROADMAP 证据
Non-goals: UI-E.3 深链上下文、pipeline API/status/checkpoint/DLQ 语义、React Router 迁移、任何后端改动
Acceptance: 1) 无效 direct/reload/back-forward 显示 not-found；2) 零错误对象 lifecycle/reset/DLQ request；3) 有效对象七 tab 可达且 title/crumb/metric/action 均 route-match；4) app-container e2e
Evidence: 先添加失败 browser assertion，随后 typecheck/lint/build、真实 app container `hack/e2e-ui.sh`、最终 diff/doc matrix
Result: delivered（2026-09-27）
Residual/follow-up: UI-F.2/UI-F.3；UI-E 仍 blocked_external，解除条件见 research-protocol.md
```

### Round 1 验收矩阵（2026-09-27）

| Criterion | Evidence | Result | Residual / blocker |
| --- | --- | --- | --- |
| direct invalid detail 不回退为 selected/list 首项 | `hack/e2e-ui.sh` F1.1；`PipelineDetailPage` route-tied `data-route-id` | PASS | none |
| reload、back、forward 保持 route identity | F1.1b、F1.4b、F1.4c；真实 app container | PASS | none |
| 无效 route 零 lifecycle/checkpoint 请求且无可执行入口 | F1.2、F1.3；browser `fetch` interceptor 仅阻断测试期潜在非 GET，未观察到请求 | PASS | none |
| valid route 七个 detail tab 保持对象绑定 | F1.4、F1.5；`main.tsx` only uses `resolvePipelineRoute` for detail props | PASS | none |
| 静态 UI 校验 | `cd web && npm run typecheck && npm run lint && npm run build` | PASS（lint 0 errors、29 pre-existing warnings） | bundle-size warning only |
| 真实 app 容器回归 | `CONTAINER_CLI=podman ./hack/e2e-ui.sh` | PASS（217/217，非 `E2E_SKIP_BUILD`） | none |
| diff hygiene | `git diff --check` | pending final-round check | no known blocker |

**Round 1/5 result：`UI-F.1 delivered`。** 本 slice 仅使 detail identity fail-closed：`selected` 继续供列表/Dashboard 使用，不能作为 detail action target。下一可领取项为 UI-F.2 + UI-F.3；UI-E 继续保持 `blocked_external`，不得以 UI-only 假设越过研究/API gate。

## Round 2 Claim 记录（UI-F.2）

```text
Round: 2/5（UI-F 窗口）
Roadmap item: UI-F / UI-F.2
Profile/path: standalone production Web UI + hash route / Settings dialog
Objective: Settings 的可见性只能由 #/settings 决定；离开 hash 后不可遗留 overlay，并定义 close/Esc/history 的无循环返回规则
Why eligible: UI-F.1 已 delivered；UI-D.3 基线已 delivered；UI-E 仍为 documented blocked_external
Scope: web/src/main.tsx、web/src/lib/routing.ts（如需 replace navigation）、web/src/pages/SettingsPage.tsx、hack/e2e-ui.sh、UI-F evidence docs
Non-goals: Settings API、token storage/persistence、LLM/worker settings payload、设置 IA 或后端路由
Acceptance: 1) route-owned open/close；2) settings→schedules 无遮罩且 Run now 可操作；3) Close/Esc/back/forward 无 history loop 且焦点不困在 overlay；4) 现有 settings 交互回归
Evidence: 先写 F2 browser assertions（现状失败），再 typecheck/lint/build、真实 app container `hack/e2e-ui.sh`
Result: delivered（2026-09-27）
Residual/follow-up: UI-F.3（同 Round，F2 已关闭后可单独 active）
```

### Round 2a 验收矩阵（UI-F.2，2026-09-27）

| Criterion | Evidence | Result | Residual / blocker |
| --- | --- | --- | --- |
| `#/settings` 是唯一 dialog open source | `main.tsx` route-derived `open={route.page === 'settings'}`；F2.1 | PASS | none |
| 离开 settings 立即卸载 overlay，Schedules 可操作 | F2.2 / F2.3，真实 app container | PASS | none |
| Esc、Close、back、forward 的 route/UI 对齐 | F2.4–F2.7；close 后 active element 仍连接且不在 dialog subtree | PASS | close 目标明确为 `#/overview`，不伪称返回任意来源 route |
| Settings 既有 token/language 行为 | legacy I1–I4、S1–S5 | PASS | no Settings API/storage semantic change |
| static checks | `cd web && npm run typecheck && npm run lint && npm run build` | PASS（lint 0 errors、29 pre-existing warnings） | bundle-size warning only |
| real app container | `CONTAINER_CLI=podman ./hack/e2e-ui.sh` 223/223 non-skip；随后 `E2E_SKIP_BUILD=1` 224/224 含 F2.7 | PASS | F2.7 only changed test script; image was the immediately preceding real build |

**Round 2a result：`UI-F.2 delivered`。** `showSettings` 已移除；hash 是唯一真相。接下来仅领取 UI-F.3（删除确认）；不得与 F3 后的 F4/F6 或 UI-E blocked research 混做。

## Round 2b Claim 记录（UI-F.3）

```text
Round: 2/5（UI-F 窗口；Round 2b，F2 已 delivered）
Roadmap item: UI-F / UI-F.3
Profile/path: standalone production Web UI + destructive control-plane actions
Objective: Worker deregister、Connection delete、Plugin uninstall 首击仅确认，不猜测影响；confirm 后仅请求显示对象一次
Why eligible: UI-F.2 已 delivered；现有三处 DELETE 都绕过确认，且 UI-E.5 的权威 connection reference API 尚未可用
Scope: web/src/WorkersPage.tsx、web/src/ConnectionsPage.tsx、web/src/MyPluginsPage.tsx、shared ConfirmDialog/Dialog、i18n.ts、hack/e2e-ui.sh、UI-F docs
Non-goals: DELETE endpoint/auth/lifecycle、connection reference API、worker/task telemetry、plugin ABI、UI-E context/model
Data/impact semantics: connection reference facts unavailable 时明确显示“引用关系不可确认”；绝不显示假造的 0 impact；取消/Esc/overlay/Close 零 DELETE；confirm 保留 server success/failure truth
Acceptance: 1) EN/ZH named confirm with object + impact uncertainty/recovery boundary; 2) Cancel/Esc/Close zero DELETE; 3) Confirm exactly one target DELETE and outcome visible; 4) real app-container E2E
Evidence: browser assertions written before fix, then typecheck/lint/build, targeted request interception and app-container `hack/e2e-ui.sh`
Result: delivered
Residual/follow-up: UI-F.4/F.6 remain later; UI-E.5 reference API stays out of scope
```

### Round 2b 验收矩阵（UI-F.3，2026-09-27）

| Criterion | Evidence | Result |
| --- | --- | --- |
| EN/ZH named confirm（对象 + 影响未知性/恢复边界，无 raw i18n key 泄漏） | F3.2/F3.7/F3.13 EN、F3.14 ZH；文案含 `worker.confirmDeregister`、`conn.confirmDelete`（“引用关系不可确认”）、`myplugin.confirmUninstall`；F3.7/F3.13 断言 `!/conn\.|myplugin\./` | pass |
| Cancel/Esc/overlay/Close 零 DELETE | F3.3/F3.4（Worker Esc）、F3.8/F3.9/F3.10（Connection 首击/Cancel/Close）、F3.15（Plugin overlay 指针序列取消） | pass |
| Confirm 恰一次目标 DELETE + 真实结果 | F3.5 Worker（真实 deregister 后行消失）、F3.11 Connection（真实删除后行消失 + notice）、F3.16 受控 503 可见失败、F3.17 受控成功 + notice；请求拦截仅覆盖三个目标 shape | pass |
| 前端静态检查 | `npm run typecheck`/`build` PASS；lint 0 error / 29 pre-existing warnings | pass |
| 真实 app-container E2E | `CONTAINER_CLI=podman E2E_SKIP_BUILD=1 ./hack/e2e-ui.sh` 242/242；最终非 skip 全量重建 `CONTAINER_CLI=podman ./hack/e2e-ui.sh` 242/242 | pass |
| 契约边界 | DELETE endpoint/服务端未改；connection reference 明示不可确认（无假造 0 impact）；plugin 行为 browser fixture（标准镜像无 Extism WASM runtime），未声称真实插件安装覆盖 | pass |

**Round 2b result：`UI-F.3 delivered`。** 下一可领取项为 UI-F.4 + UI-F.6（同窗口 Round 3），UI-F.5 随后（UI-D.3 共享基线已 delivered），最后 UI-F.7 收口；UI-E 仍保持 `blocked_external`。


## Round 3 Claim 记录（UI-F.4 + UI-F.6）

```text
Round: 3/5（UI-F 窗口；F.1/F.2/F.3 已 delivered）
Roadmap item: UI-F / UI-F.4 + UI-F.6
Profile/path: standalone production Web UI + 控制面读取状态真实性/i18n-a11y
Objective: 读取失败（503）不再伪装成空态；Workers 任务区诚实表达 telemetry 不可用；关键输入有稳定 label 关联
Why eligible: F.3 已 delivered；PW-R5/R6/R7/R8 复现于已构建 UI 且不需要新后端 API
Scope: web/src/pages/AuditPage.tsx、pages/ConnectorsPage.tsx、MyPluginsPage.tsx、WorkersPage.tsx、pages/pipelines/PipelineDetailPage.tsx、pages/SettingsPage.tsx、SchedulesPage.tsx、DagEditorPage.tsx、main.tsx（onRetry 接线）、i18n.ts、hack/e2e-ui.sh
Non-goals: 不新增 task telemetry、run history、incident API；不修改 polling/retry 节奏、audit 存储、runtime metrics 契约、token storage、cron parser、DAG spec schema
Data/impact semantics: error 分支与 empty 分支互斥；503 显示可解释错误 + retry；成功空响应才显示 empty；Workers 任务区在有 worker 时明确 telemetry unavailable，不显示“暂无工作节点”；Detail Runs 注明当前快照来源
Acceptance: F4.1–F4.6（503/空态互斥、retry、任务区诚实、Runs 注记）；F6.1–F6.4（无 raw key、token/cron/DAG name label 关联）
Evidence: browser assertions 先于修复写入并确认失败（11 failed pre-fix）；typecheck/lint/build PASS（29 pre-existing warnings/0 errors）；E2E_SKIP_BUILD=1 254/254；最终非 skip 全量重建 CONTAINER_CLI=podman ./hack/e2e-ui.sh 254/254
Result: delivered
Residual/follow-up: UI-F.5（390px containment，UI-D.3 基线已冻结）→ UI-F.7 收口
```

### Round 3 验收矩阵（UI-F.4 + UI-F.6，2026-09-27）

| Criterion | Evidence | Result |
| --- | --- | --- |
| Audit 503 → error（含状态），非“无记录”空态；成功空响应才 empty | F4.1/F4.4（受控 503 与受控空响应互斥验证） | pass |
| Connector catalog 503 → error + retry 控件，非 empty | F4.2/F4.2b（`connectors-load-error`/`connectors-load-retry`，onRetry 接线至共享 refreshKey） | pass |
| Extensions 读取失败 → error，非“未安装” | F4.3（`plugin-load-error`；refresh 不再吞错误） | pass |
| Workers 任务区：有 worker 时不说“没有 worker”，telemetry 缺席如实声明 | F4.5/F4.5b（`worker-task-area` + `worker.tasksUnavailable` 文案） | pass |
| Detail Runs 区分 runtime snapshot 与 storage-backed history | F4.6（`detail-runs-note` = pipe.runsNote） | pass |
| Extensions 无 raw i18n key 泄漏 | F6.1（EN 全页扫描；F.3 已补齐 kind/version/uninstall/confirm/cancel/close 的 EN/ZH） | pass |
| Settings token / Schedules cron / DAG pipeline name 稳定 label 关联 | F6.2（`htmlFor=settings-api-token`）、F6.3（`htmlFor=schedules-cron-input` sr-only label）、F6.4（`aria-label=design.name`） | pass |
| 前端静态检查 | typecheck/build PASS；lint 0 error / 29 pre-existing warnings | pass |
| 真实 app-container E2E | `CONTAINER_CLI=podman E2E_SKIP_BUILD=1 ./hack/e2e-ui.sh` 254/254；最终非 skip 全量 `CONTAINER_CLI=podman ./hack/e2e-ui.sh` 254/254 | pass |

**Round 3 result：`UI-F.4 + UI-F.6 delivered`。** 下一可领取项为 UI-F.5（390px containment；UI-D.3 共享基线已 delivered，阻塞条件不成立），最后 UI-F.7 收口；UI-E 仍保持 `blocked_external`。



## Round 4 Claim 记录（UI-F.5）

```text
Round: 4/5（UI-F 窗口；F.1–F.4、F.6 已 delivered）
Roadmap item: UI-F / UI-F.5
Profile/path: standalone production Web UI + 390px 小屏 containment
Objective: Detail Runs、Workers、Audit、Connector catalog、DAG Designer 在 390px 无整页横滚；宽内容仅在局部 scroller 内滚动；1280px 桌面密度不回归
Why eligible: UI-D.3 共享基线（AppShell min-w-0/table overflow-auto）已 delivered，阻塞条件不成立；实测仅 DAG Designer 溢出（713px），其余四页已被共享基线覆盖
Scope: web/src/DagEditorPage.tsx（工具栏 palette/drawer-tabs 行 overflow-x-auto + min-w-0 + shrink-0）、hack/e2e-ui.sh（F5 断言）
Non-goals: 不用全局 overflow-x:hidden；不删除节点/列/危险操作；不改 DAG 业务逻辑、canvas pan/zoom 实现
Data/impact semantics: 无数据面影响（纯布局）；palette 行保留键盘 focus + scrollIntoView 可达性
Acceptance: F5.1 五页 390px containment；F5.2 UI-D.3 三页无回退；F5.3 palette 局部滚动 + 焦点可见；F5.4 canvas 存在且不撑开 document；F5.5 1280px 桌面回归
Evidence: 修复前实测仅 #/designer 713px 溢出（palette 行 flex 不收缩）；修复后 390/390；E2E_SKIP_BUILD=1 265/265；最终非 skip 全量 CONTAINER_CLI=podman ./hack/e2e-ui.sh 265/265；typecheck/build PASS；lint 0 error/29 pre-existing warnings
Result: delivered
Residual/follow-up: UI-F.7（最终真实容器复验与证据收口）
```

### Round 4 验收矩阵（UI-F.5，2026-09-27）

| Criterion | Evidence | Result |
| --- | --- | --- |
| 390px `scrollWidth <= innerWidth`（Runs/Workers/Audit/Connectors/Designer） | F5.1 五项（实测仅 Designer 曾溢出 713px，其余由 UI-D.3 共享基线覆盖） | pass |
| UI-D.3 页面（Connections/Schedules/My Plugins）无回退 | F5.2 三项 | pass |
| DAG palette 局部滚动、键盘焦点可见、不撑开 document | F5.3（palette scrollWidth 684 > clientWidth 332，focus+scrollIntoView 后 right ≤ viewport） | pass |
| DAG canvas 存在且 390px contained | F5.4 | pass |
| 1280px 桌面密度/工具栏/命名输入不回归 | F5.5 | pass |
| 修复手段合规 | 仅 palette/drawer-tabs 行 `overflow-x-auto + min-w-0 + max-w-full + shrink-0`；无全局 hidden、无功能删减 | pass |
| 真实 app-container E2E | `E2E_SKIP_BUILD=1` 265/265；最终非 skip 全量重建 265/265 | pass |

**Round 4 result：`UI-F.5 delivered`。** 仅剩 UI-F.7（真实容器复验 + ROADMAP/实测报告证据收口）。



## Round 5 Claim 记录（UI-F.7 收口）

```text
Round: 5/5（UI-F 窗口；F.1–F.6 已 delivered）
Roadmap item: UI-F / UI-F.7（迭代收口）
Profile/path: standalone production Web UI + 全量真实容器复验与证据回填
Objective: 在真实 app container 一次非 skip 运行中复验 UI-F.1–UI-F.6 全部关键路径，回填 ROADMAP/board/spec/原始实测报告，审查最终 diff
Why eligible: F.1–F.6 全部 delivered，无遗留 active 子任务
Scope: hack/e2e-ui.sh（已含 F1–F6 断言）、docs/ROADMAP.zh.md、docs/iterations/README.md、UI-F 三件套、ui-playwright-supplemental-review 回填
Non-goals: 不借验收轮新增产品能力、incident 聚合、connection reference 后端 API 或 IA 改造
Acceptance: 1) 真实容器复验覆盖无效 detail、Settings route、三类删除确认、503/空态互斥、390px/1280px、EN/ZH；2) 静态检查记录实际输出；3) 四处文档回填且不改写原始发现证据；4) 最终 diff 无 API mock/临时截图/秘密，git diff --check 通过
Evidence: CONTAINER_CLI=podman ./hack/e2e-ui.sh（非 skip、全量镜像重建）265 passed / 0 failed，单次运行覆盖 F1.1–F1.5、F2.1–F2.7、F3.0–F3.17、F4.1–F4.6、F6.1–F6.4、F5.1–F5.5；cd web && npm run typecheck（PASS）、npm run lint（0 error/29 pre-existing warnings）、npm run build（PASS）；git diff --check 通过；internal/packed/packed.go 构建产物已还原为已提交版本
Result: delivered（UI-F 迭代整体 delivered，5/5 rounds）
Residual/follow-up: UI-E.5 权威 connection reference API 仍归 UI-E（blocked_external）；DAG canvas 深度移动编辑体验超出本轮 containment 契约，未新增需求
```

### Round 5 验收矩阵（UI-F.7，2026-09-27）

| Criterion | Evidence | Result |
| --- | --- | --- |
| 真实容器一次运行覆盖全部 UI-F 面 | 非 skip 全量 `CONTAINER_CLI=podman ./hack/e2e-ui.sh` 265/265：F1（route identity）、F2（Settings route）、F3（三类删除确认 + 503 受控失败）、F4（503/空态互斥 + retry + telemetry 诚实）、F5（390px/1280px）、F6（raw-key + label 关联） | pass |
| 静态检查实际输出 | typecheck PASS；lint 0 error / 29 pre-existing warnings；vite build PASS | pass |
| 文档回填完整且不篡改原始证据 | ROADMAP（F.1–F.6 delivered + 矩阵证据）、board（UI-F 状态）、spec（6 条问题全部 strike-through + 修复回填）、supplemental review（PW-R1/R2/R3/R4/R5/R6 状态改 fixed 且保留原始发现） | pass |
| 最终 diff 卫生 | 无 API mock 残留（受控 fetch override 均在测试脚本内并自恢复）、无临时截图、无 token/secret；resource/public 为项目约定的已提交前端构建产物；internal/packed/packed.go 已还原 | pass |
| git diff --check | 通过 | pass |

**Round 5 result：`UI-F.7 delivered`。UI-F 迭代整体 `delivered`（5/5 rounds，265/265 最终证据）。** 下一可领取项：仅剩 UI-E（blocked_external，等待外部用户研究输入）与 UI-G（queued，restart_policy UI 配置与 spec 往返保持）。


## 领取记录模板

```text
Round: <n>/5
Roadmap item: UI-F / UI-F.<n>
Profile/path: standalone production Web UI + <route safety|control-plane truth|responsive containment>
Objective: <one observable interaction outcome>
Scope: <explicit UI/API/test/doc files>
Non-goals: <no UI-only runtime inference; no data-plane semantic change; no unrelated IA work>
Dependencies: <UI-D.3 contract / container runtime / server-backed reference facts if applicable>
Acceptance: <numbered checks from this task>
Evidence: <typecheck/lint/build/Playwright/app-container E2E/docs matrix>
Result: <delivered|active|blocked_external>
Residual/follow-up: <bounded item or none>
```
