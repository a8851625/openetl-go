# OpenETL-Go UI 补充实测复审（2026-09-25，Playwright CLI）

> 性质：这是在既有 [UI 产品与交互设计复审](./ui-product-design-review-2026-09-24.zh.md) 之后的**补充实测**，专门覆盖上一轮未深入操作的详情页、系统页、深链、失败态、移动布局与高影响操作。
>
> 本文不修改 UI-D/UI-E 的状态、范围或优先级。用户随后授权将这些新增发现沉淀为独立、`queued` 的 [UI-F 后续迭代](./iterations/UI-F-control-plane-truthfulness/spec.md)；该映射不抢占或重排 IT-6、UI-D、UI-E，也不改写本文的原始实测结论。

## 1. 测试边界与可信度

### 实际运行方式

- 实际打开的工件：仓库已构建的生产前端 [resource/public](../resource/public/index.html)，以静态服务器提供。
- 自动化方式：`playwright-cli`，浏览器真实执行 hash 导航、点击、键盘、视口缩放、请求拦截和 DOM/accessibility tree 检查。
- API：浏览器层 fixture，覆盖 5 条管道（含 running / failed / DAG）、连接、DLQ、metrics、workers、audit、scheduler、plugin、DAG spec 和 log。
- 视口：桌面 `1280×900`；小屏 `390×844`。

### 本轮覆盖

| 范围 | 实测内容 |
| --- | --- |
| Pipeline detail | `overview/runs/issues/checkpoints/logs/topology/spec` 7 个 tab 的直接 URL、内容挂载、checkpoint reset 保护、无效 ID 深链与 Stop 操作。 |
| Schedules | Run now 确认、错误 cron 反馈、双表小屏行为。 |
| Workers / Connections / Extensions | 删除请求、确认层、worker 状态文案、503 失败态、插件表与 i18n。 |
| Audit / Connector catalog | 503 时的空态与错误态。 |
| Settings / 路由 | `#/settings` 打开、hash 跳转、关闭、页面拦截。 |
| DAG Designer | 编辑深链、connection test、preflight 入口、节点工具库小屏行为。 |
| 基础体验 | theme 切换、关键 input accessibility tree、移动布局 document width。 |

### 不能在本轮宣布的结论

- fixture 证明的是**前端表达、导航、请求发起、状态分支与响应式布局**；它不证明真实后端是否真正停止/删除管道、重放数据、持久化审计或执行 DAG。
- 没有运行 `hack/e2e-ui.sh`，因此不能把本轮作为 app container / real API 的 E2E 证据。
- 未覆盖真实认证、真实 connector outage、长列表/高数据量、浏览器兼容性、真实屏幕阅读器朗读和实际移动设备触摸性能。

---

## 2. 已确认可用的能力（避免只报问题）

| 路径 | 实测结果 | 说明 |
| --- | --- | --- |
| Detail 深链 tabs | 7 个 tab 都可由 `#/pipelines/orders-cdc/<tab>` 直达并挂载主内容。 | 对已有有效 pipeline ID 成立；无效 ID 的行为见 PW-R1。 |
| Checkpoint reset | 未输入准确管道名时 Reset disabled；输入 `orders-cdc` 后打开明确 replay/duplicate 风险确认框。 | 这是高风险操作应有的意图确认模式。 |
| Schedule Run now | 点击后打开确认框，说明“立即启动”及 sink replay 吸收风险。 | 比 Workers/Connections/Extensions 删除流程更符合风险操作基线。 |
| Cron 输入 | 输入非法 cron 后出现 `Invalid cron expression`。 | 有即时反馈，不会静默接受。 |
| DAG connection test | 编辑 DAG 的 Test connection 可显示 `source/mysql_cdc connection OK`。 | 后端真实连通性仍未认证。 |
| Theme | Account 菜单内 Theme 可在 dark/light 间来回切换，根元素 class 正确变化。 | 基础主题状态无异常。 |
| Mobile navigation | 小屏抽屉可到达 Audit；从抽屉导航后会自动关闭；焦点位于 Sheet 时 Escape 会关闭抽屉；底栏 Pipelines 可正确跳转。 | 仅代表该 fixture 下的基础导航，不能替代真实设备触摸测试。 |
| Workers / Connections 503 | 这两个页面显示 API error，而不是完全伪装成正常空态。 | 但错误以原始 JSON 形式呈现，仍可优化。 |

---

## 3. 新增问题清单

优先级含义：`P0` 可能把高影响动作打到错误对象；`P1` 阻断主要任务或隐藏重要运行事实；`P2` 造成明显误解、可靠性下降或运营成本；`P3` 为可访问性/语义质量改进。

### PW-R1 · P0：无效 pipeline 深链静默回退为默认管道，且可直接对默认管道执行 Stop

**浏览器复现**：

1. 直接打开 `#/pipelines/not-a-real-pipeline/overview`；
2. 页面并没有显示“不存在”，而显示 fixture 的默认 `orders-cdc`，标题和路径也变成 `/pipelines/orders-cdc`；
3. 点击 `Stop` 后，浏览器真实发出 `POST /api/v2/pipelines/orders-cdc/stop`。

**用户风险**：用户从告警、书签、同事分享或旧 pipeline id 进入一个失效 URL 时，表面上仍像在查看请求对象，但操作实际落到默认列表第一条管道。这是“对象身份”与“操作目标”脱钩，尤其不应出现在 Start/Stop、checkpoint reset、DLQ/replay 等运行控制界面。

**建议修复**：

- route id 不存在时，`PipelineDetailPage` 必须收到 `undefined` 并显示明确的 not-found state，不能从全局默认 `selected` 回退；
- 不渲染或禁用一切 lifecycle / reset / DLQ 操作，直到 route id 与 API 返回对象一致；
- 回到列表应保留用户原来的筛选/来源上下文；
- 新增 Playwright 断言：无效 detail URL 下**不存在**对任意有效 pipeline 的 `start/stop/reset` 请求。

**相关实现入口**：[main.tsx](../web/src/main.tsx)、[routing.ts](../web/src/lib/routing.ts)、[PipelineDetailPage.tsx](../web/src/pages/pipelines/PipelineDetailPage.tsx)。

**交付回填（2026-09-27，UI-F.1）**：已以 `resolvePipelineRoute` 建立 detail 的唯一 identity source，移除 `selected` / 列表首项回退；无效 URL 现在渲染 route-tied not-found，且没有 lifecycle、checkpoint 或 DLQ 操作入口。真实 app container `CONTAINER_CLI=podman ./hack/e2e-ui.sh` 在 217/217 PASS 中覆盖 direct、reload、back/forward、零 lifecycle/checkpoint request 和有效对象的七个 detail tabs（F1.1–F1.5）。原始 2026-09-25 复现保留为问题发现证据，不改写为当日通过。

---

### PW-R2 · P1：Settings 弹层与 hash 路由不同步，离开 `#/settings` 后仍遮挡后续页面

**浏览器复现**：

1. 在已加载页面内改为 `#/settings`，Settings dialog 打开；
2. 不点 Close，直接改 hash 到 `#/schedules`；
3. URL 已是 `#/schedules`，但 Settings dialog 仍 visible；
4. 尝试点击 Schedules 的 `Run now`，Playwright 发生点击 timeout（弹层覆盖并截获交互）。

**用户影响**：浏览器前进/后退、侧栏跳转、深链、程序化导航后，视觉内容和实际可交互层不一致；用户会看到“调度页 URL”，却只能操作设置弹窗。这是路由状态与局部 React state 双来源造成的交互死角。

**建议修复**：

- 让 `showSettings` 由 route 派生，或在任何 `next.page !== 'settings'` 时显式关闭；
- `onClose` 再导航到 overview 时，定义并测试 back/forward 语义，避免循环；
- E2E 覆盖 `#/settings → #/schedules`、back、forward、Esc、Close button。

**相关实现入口**：[main.tsx](../web/src/main.tsx)、[SettingsPage.tsx](../web/src/pages/SettingsPage.tsx)。

**交付回填（2026-09-27，UI-F.2）**：`showSettings` local state 已移除，Dialog `open` 仅由 `route.page === 'settings'` 派生。真实 app container 的 F2.1–F2.7 覆盖 direct settings、settings → schedules 无遗留 dialog/overlay 且 Run now 可操作、Esc、Close、back/forward 与关闭后的焦点不困在 overlay；完整构建回归 223/223 PASS，新增 Close 断言后的同镜像真实容器复验 224/224 PASS。原始 2026-09-25 复现保留为发现证据。

---

### PW-R3 · P1：多种删除操作绕过确认，风险模型与已有 pipeline/DLQ 操作不一致

| 页面 / 行为 | 实测网络行为 | 是否出现确认 dialog |
| --- | --- | --- |
| Workers → Deregister `worker-1` | `DELETE /api/v2/workers/worker-1/deregister` | 否 |
| Connections → Delete `commerce-mysql` | `DELETE /api/v2/connections/commerce-mysql` | 否 |
| Extensions → Uninstall `mask-pii` | `DELETE /api/v2/plugins/mask-pii` | 否 |

**风险**：连接可能被正在运行的管道引用；worker deregister 可能影响任务分配；plugin uninstall 可使现有 spec 无法再加载。它们的破坏性不低于已经使用 `ConfirmDialog` 的 checkpoint reset、Run now 或 DLQ purge。

**建议修复**：

- 使用同一 `ConfirmDialog` 契约：`对象 → 影响 → 不可逆/恢复方式 → 确认`；
- Connections 删除前必须由后端返回引用关系或明确写“引用关系不可确认”，不能在 UI 里猜；
- worker/plugin 删除确认应包含受影响 pipeline / task 数（API 无数据时标明 unavailable）；
- 成功后给可回到受影响对象的结果反馈，失败不要只靠 toast。

**相关实现入口**：[WorkersPage.tsx](../web/src/WorkersPage.tsx)、[ConnectionsPage.tsx](../web/src/ConnectionsPage.tsx)、[MyPluginsPage.tsx](../web/src/MyPluginsPage.tsx)。

**修复状态（2026-09-27，UI-F.3 delivered）**：三处首击均改走共享 `ConfirmDialog`（对象名 + 影响未知性/恢复边界，EN/ZH）；connection 确认文案明确“引用关系不可确认”，未假造影响数据；worker/plugin 影响同样标注 unavailable；成功后页面内 notice 呈现结果、失败保留可见错误（不再只靠 toast）；取消/Esc/overlay/Close 零 DELETE。受影响 pipeline/task 计数需权威 API（UI-E.5），不在本轮猜测。真实 app container `hack/e2e-ui.sh` F3.1–F3.17 全部 PASS（242/242）。

---

### PW-R4 · P1：390px 下多个补充页面发生整页横向滚动，不能只靠已有 table wrapper 解决

**实际测量（未注入 CSS）**：

| 路由 | `documentElement.scrollWidth` | 视口 |
| --- | ---: | ---: |
| `#/pipelines/orders-cdc/runs` | 623px | 390px |
| `#/schedules` | 540px | 390px |
| `#/cluster` | 683px | 390px |
| `#/audit` | 499px | 390px |
| `#/connectors` | 420px | 390px |
| `#/extensions` | 874px | 390px |
| `#/designer?edit=orders-dag` | 742px | 390px |

**根因实验（不是产品改动）**：在浏览器临时注入 `#root > .flex > .flex-1 { min-width: 0 }` 后，Runs/Workers/Audit/Connectors/Extensions 的 document width 可回到 390px，说明主 flex item 的 min-content 宽度会放大局部内容并带动整页横滚。但 Schedules 仍为 524px，DAG 仍为 713px：

- Schedules：Schedule 表和 Run History 的 `Schedule / written / DLQ / Edit` 列仍在内容区外；
- DAG：节点库的 `Router / Tap / Rate Limiter / Lookup / Enricher` 工具按钮保持单行宽度。

**建议修复**：

1. AppShell 主内容 flex child 加 `min-w-0`，作为正确的外层收缩基线；
2. 宽表只在**表格容器内**滚动，或在 ≤640px 变为状态优先卡片；不得用全局 `overflow-x:hidden` 遮住数据；
3. Schedules 列表和 Run History 分别设计移动卡片/summary，`Edit` 不应要求整页拖动；
4. DAG 节点工具库在小屏采用折叠分类、可横滚的局部 palette 或底部 drawer；画布需要独立的 pan/zoom 边界，不应撑开 AppShell；
5. 新增 390px 断言：`documentElement.scrollWidth <= innerWidth`，同时单独允许指定局部 scroller 的 `scrollWidth > clientWidth`。

这是对已有 UI-D 小屏结论的**覆盖扩展**：本轮不变更 UI-D 范围，但不能再只测试 Connections/Schedules/My Plugins。

**相关实现入口**：[app-shell.tsx](../web/src/components/layout/app-shell.tsx)、[table.tsx](../web/src/components/ui/table.tsx)、[SchedulesPage.tsx](../web/src/SchedulesPage.tsx)、[WorkersPage.tsx](../web/src/WorkersPage.tsx)、[AuditPage.tsx](../web/src/pages/AuditPage.tsx)、[DagEditorPage.tsx](../web/src/DagEditorPage.tsx)。

---

### PW-R5 · P1：Audit / Connector catalog / Extensions 在 503 时显示“正常空态”，掩盖运行面失败

**浏览器复现**：分别将 `/api/v2/audit` 或 `/api/v2/plugins` 拦截为 HTTP 503：

| 页面 | 503 时实际文案 | 应有语义 |
| --- | --- | --- |
| Audit | `No audit events` | 审计读取失败，无法判断是否无事件 |
| Connector catalog | `No connectors found` | connector catalog 暂不可用 |
| Extensions | `No plugins installed yet` | plugin registry 暂不可用 |

Workers 和 Connections 在同类 503 下能显示错误分支（虽是原始 JSON），表明错误状态并非不可实现。

**风险**：自托管运维工具中，“没有数据”与“无法读取数据”必须可区分。特别是 Audit 的 false empty 会让操作者误判不存在操作记录，Extensions false empty 会误导为未安装插件。

**建议修复**：

- 传播并展示 `useApi` 的 `error`，提供 retry；
- 空态只在请求成功且数据确为空时显示；
- 失败态说明影响的判断范围，例如“审计不可用，不能据此确认近期操作”；
- 错误信息做用户可读映射，保留可展开的 API details，而不是直接渲染 JSON。

**相关实现入口**：[main.tsx](../web/src/main.tsx)、[AuditPage.tsx](../web/src/pages/AuditPage.tsx)、[ConnectorsPage.tsx](../web/src/pages/ConnectorsPage.tsx)、[MyPluginsPage.tsx](../web/src/MyPluginsPage.tsx)。

---

### PW-R6 · P2：Extensions 已安装插件表直接泄漏 i18n key，且危险按钮文本不可理解

**浏览器实测（英文 UI）**：已安装插件表显示：

```text
Name | common.kind | common.version | ABI | Status | WASM path | Actions
…
common.uninstall
```

这不是翻译语句的未翻译，而是原始 key 被直接呈现。`common.uninstall` 同时是 destructive action 文案，会降低用户判断风险的能力。

**建议修复**：补齐/使用正确的 `kind`、`version`、`uninstall` i18n keys，并为缺 key 设置开发期检测（例如 test 扫描页面 text 中的 `^[a-z]+\.[a-z_]+$` 风格 key）；确认后再补 EN/ZH 组件测试。

**相关实现入口**：[MyPluginsPage.tsx](../web/src/MyPluginsPage.tsx)、[i18n.ts](../web/src/i18n.ts)。

---

### PW-R7 · P2：Workers 的“Running tasks”在已有 worker 时固定宣称没有 worker

fixture 中 Workers 页面明确显示 `2 registered / 1 online / 6 slots`，但下面的 `Running tasks` 卡片仍显示“没有 worker/standalone runs in-process”性质的空态。这把“当前没有 task telemetry”误报成“没有 worker”。

**建议修复**：

- 若没有 task API，文案应为“任务分配 telemetry 暂不可用”而非“没有 worker”；
- 若有 task API，展示 task count、pipeline、worker、状态与更新时间；
- 把 `registered workers = 0`、`online = 0`、`no running tasks`、`task API unavailable` 四种状态分开。

**相关实现入口**：[WorkersPage.tsx](../web/src/WorkersPage.tsx)。

---

### PW-R8 · P2：Detail 的 “Runs / Run history” 实际只是当前 runtime snapshot

在 `Runs` tab 中，界面显示一行当前指标并声明：`Run history is derived from the current runtime snapshot; full run timeline ships with storage-backed history.` 这意味着它不是用户会理解的真正 run history，也没有开始/结束/attempt/restart/trigger/recovery 的时序记录。

**风险**：页面标题承诺“历史”，实现却提供单一当前快照。值守人员可能据此误判“上一轮运行成功”或无法判断 restart 后的恢复过程。

**建议修复**：在真正接入 storage-backed history 前，将 tab 改名为“Current runtime snapshot / 当前运行快照”，并把完整历史明确列为 unavailable；接入后区分 trigger、attempt、duration、checkpoint、write/DLQ、failure/retry、operator action。

**相关实现入口**：[PipelineDetailPage.tsx](../web/src/pages/pipelines/PipelineDetailPage.tsx)。

---

### PW-R9 · P3：表单可访问名称可由 placeholder 回退，但 visual label 与控件没有语义绑定

Playwright accessibility tree 能读出：

- Settings API Token：`textbox "API Token"`；
- Schedules cron：`textbox "*/5 * * * *"`；
- DAG pipeline name：`textbox "Pipeline name"`。

因此它们**不是无名称控件**，不应误报为 A11y blocker。但 DOM 中这些 input 的 `labels.length=0` 且没有 `aria-label`；当前名称主要来自 placeholder。placeholder 在输入后消失，且不会表达持续语义/帮助/错误关联。

**建议修复**：每个真实字段使用稳定 `id + htmlFor` 或 `aria-labelledby`，错误使用 `aria-describedby`；尤其 DAG 的 pipeline name 与 schedule cron 需要独立 label。不要仅把 placeholder 当作字段说明。

---

## 4. 建议的处理顺序（已沉淀为 queued UI-F，不改变既有优先级）

| 顺序 | 最小可验证 slice | 原因 |
| --- | --- | --- |
| 1 | [UI-F.1](./iterations/UI-F-control-plane-truthfulness/tasks.md)：无效 pipeline 深链不再 fallback，任何高风险请求必须引用 URL 所请求的有效 ID | 唯一会把动作直接打到错误对象的 P0；P0 为严重度，不自动改变 IT-6/UI-D/UI-E 的领取顺序。 |
| 2 | PW-R2 + PW-R3：route/dialog 状态统一；所有 destructive delete 先确认影响 | 解决遮挡死角与运行面破坏性操作不一致。 |
| 3 | PW-R5：所有 `useApi` 页面区分 `loading / error / empty / data` | 防止 self-hosted 控制台把 API 故障伪装成空系统。 |
| 4 | PW-R4：AppShell min-width 基线 + 逐页局部滚动/卡片设计 | 移动端要恢复整页可用性，不能用全局遮挡修补。 |
| 5 | PW-R6/R7/R8/R9：i18n、worker truthfulness、Run snapshot 命名、字段语义 | 提升任务理解与长期可维护性。 |

## 5. 明确不建议的修法

- 不用“总是选中第一条 pipeline”掩盖无效 detail URL；
- 不用全局 `overflow-x: hidden` 掩盖数据表/工具库超宽；
- 不在浏览器端猜测 connection reference、worker impact 或 incident 关系；
- 不把 API 失败转成普通 empty state，也不把原始 JSON 当作唯一用户错误文案；
- 不在没有真实 server-backed run history 前把当前 metrics snapshot 称为历史；
- 不因本轮 fixture 成功就宣称真实 container/runtime/replay 通过。

## 6. 本轮验证记录摘要

| 验证 | 结果 | 证据性质 |
| --- | --- | --- |
| 有效 detail 7 tabs / reset guard | `passed` | 浏览器实际导航与交互，fixture API |
| 无效 detail → 默认 pipeline Stop | `fixed`（UI-F.1，2026-09-27） | 原始浏览器 POST 保留为发现证据；真实 app container F1.1–F1.5 217/217 PASS，零错误对象 action request |
| Settings route → 后续页面弹层拦截 | `fixed`（UI-F.2，2026-09-27） | 真实 app container F2.1–F2.7 223/223→224/224 PASS |
| worker/connection/plugin delete confirmation | `fixed`（UI-F.3，2026-09-27） | 原始无确认 DELETE 保留为发现证据；真实 app container F3.1–F3.17 PASS（含受控 503 失败与真实 Worker/Connection 删除后行消失），242/242 |
| 390px document overflow | `fixed`（UI-D.3 + UI-F.5，2026-09-27） | Connections/Schedules/My Plugins 由 UI-D.3 交付；其余五页（Runs/Workers/Audit/Connectors/Designer）由 UI-F.5 交付（DAG palette 局部滚动）；F5.1–F5.5 真实 app container 265/265 |
| 503 false empty | `fixed`（UI-F.4，2026-09-27） | 原始 503 伪装空态保留为发现证据；受控 503/空响应互斥断言 F4.1–F4.4 + retry 控件通过（254/254） |
| Extensions raw keys | `fixed`（UI-F.3/UI-F.6，2026-09-27） | F.3 补齐 EN/ZH 文案；F6.1 全页 raw-key 扫描通过（254/254） |
| Schedule Run now confirmation / invalid cron | `passed` | 浏览器实际 dialog / validation |
| Theme toggle / DAG test feedback | `passed` | 浏览器 DOM / interaction |
| 真正 runtime、删除持久化、认证、容器 E2E | `not run` | 需要真实 app container |
