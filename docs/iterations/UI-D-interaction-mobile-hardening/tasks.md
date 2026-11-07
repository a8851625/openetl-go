# UI-D 任务分解

> 状态取值：`todo` / `active` / `done` / `blocked`。实施时一次只允许一个 `active`；截至 2026-09-26，UI-D.1–UI-D.6 已全部完成并收口。

## Round 分组

| Round | 包含 task | 目标 |
| --- | --- | --- |
| 1/5 | UI-D.1、UI-D.2 | 让批量启停在确认前、确认中、完成后都表达真实且可理解的操作结果。✅ 2026-09-26 |
| 2/5 | UI-D.3 | 让手机/小屏能完成连接、调度和扩展的核心值守任务。✅ 2026-09-26 |
| 3/5 | UI-D.4 | 让移动端创建向导的所有步骤和进度可发现。✅ 2026-09-26 |
| 4/5 | UI-D.5 | 让刷新控制与 icon-only 操作具备清晰状态和可访问名称。✅ 2026-09-26 |
| 5/5 | UI-D.6 | 在真实应用容器中回归、记录证据并收口。✅ 2026-09-26 |

## 任务表

| ID | 任务 | 依赖 | 状态 | 证据落点 |
| --- | --- | --- | --- | --- |
| UI-D.1 | 修复批量启停确认中的 action 替换和 EN/ZH 完整本地化 | — | `done`（2026-09-26 Round 1） | `PipelinesPage.tsx`、`confirm-dialog.tsx`、`ui/dialog.tsx`、`i18n.ts`、`hack/e2e-ui.sh` D1.1–D1.2 |
| UI-D.2 | 让批量 Start/Stop 与所选目标匹配，并消除静默 no-op | UI-D.1 | `done`（2026-09-26 Round 1） | `PipelinesPage.tsx`、`i18n.ts`、`hack/e2e-ui.sh` D2.1–D2.3 |
| UI-D.3 | 修复 Connections、Schedules、My Plugins 的 390px 整页横向溢出 | — | `done`（2026-09-26 Round 2） | `app-shell.tsx`、`ConnectionsPage.tsx`、`SchedulesPage.tsx`、`hack/e2e-ui.sh` D3.1–D3.9 |
| UI-D.4 | 增强移动端向导步骤进度、自动可见性和发现性 | — | `done`（2026-09-26 Round 3） | `first-task-wizard.tsx`、`i18n.ts`、`hack/e2e-ui.sh` D4.1–D4.5 |
| UI-D.5 | 明确自动刷新状态并补齐 icon-only 可访问名称 | — | `done`（2026-09-26 Round 4） | `api.ts`/`types.ts` refresh in-flight state、`main.tsx`、`app-shell.tsx`、`i18n.ts`、`hack/e2e-ui.sh` D5.1–D5.9 |
| UI-D.6 | 在真实 app 容器执行完整 UI 回归并回填 Roadmap 证据 | UI-D.1–UI-D.5 | `done`（2026-09-26 Round 5） | `CONTAINER_CLI=podman ./hack/e2e-ui.sh` 209/209、ROADMAP/board/review 收口、`git diff --check` |

## 任务明细

### UI-D.1 批量启停确认可信度

**验收**：

1. `start` / `stop` 的标题与描述都替换 `{action}`，并始终包含选中数量、目标名和 replay 风险。
2. Confirm、Cancel、Close 在 EN/ZH 都为当前语言；Dialog close 具备本地化 accessible name。
3. 提交后成功、失败、未执行结果可见，失败原因不丢失。
4. 对四个语言×操作组合加入浏览器断言，且断言文本没有 `{` / `}` 遗留 token。

**允许范围**：`web/src/pages/pipelines/PipelinesPage.tsx`、`web/src/components/shared/confirm-dialog.tsx`、`web/src/components/ui/dialog.tsx`、`web/src/i18n.ts`、`hack/e2e-ui.sh`。

**非目标**：不改 batch API、并发策略、status 派生或 at-least-once/replay 语义。

**证据落点**：新增 e2e `UI-D.1.*` 断言；`npm run typecheck/build/lint`；容器 `hack/e2e-ui.sh`。

### UI-D.2 批量操作可供性

**验收**：

1. empty / running-only / stopped-only / mixed 选择集的 Start/Stop enabled 状态与可执行目标一致。
2. 不适用的动作显示可读取原因；键盘用户也能获得原因。
3. 无可执行目标时不发 POST；有可执行目标时仍出现 UI-D.1 的确认和结果汇总。
4. 从 selection toolbar 和全局 start-all / stop-all 两条路径验证一致的原因文案。

**允许范围**：`web/src/pages/pipelines/PipelinesPage.tsx`、现有 tooltip/button 支撑（如必要）、`web/src/i18n.ts`、`hack/e2e-ui.sh`。

**非目标**：不把 failed、completed、paused、scheduled 重新归类为 stopped；不改批量并发上限。

**证据落点**：浏览器请求拦截、disabled/ARIA/tooltip 断言及真实容器回归。

### UI-D.3 小屏值守任务布局

**验收**：

1. 390px 下 Connections、Schedules、My Plugins 的 `scrollWidth` 不超过 viewport。
2. 每页的名称、状态、必要上下文、主操作和危险操作均无需整页横向拖动即可到达。
3. Connections 至少覆盖 Load、Test、Delete；Schedules 和 My Plugins 覆盖各自当前主操作。
4. 1280px 保持桌面表格/密度，不引入 duplicate action handler 或状态分叉。
5. 关键操作可按键盘顺序获取焦点，危险操作不变成没有文字/accessible name 的纯图标。

**允许范围**：`web/src/ConnectionsPage.tsx`、`web/src/SchedulesPage.tsx`、`web/src/MyPluginsPage.tsx`、必要共享组件/样式、`i18n.ts`、`hack/e2e-ui.sh`。

**非目标**：不重做桌面信息架构，不隐藏 Delete/Replay 等危险动作以换取更窄的布局。

**证据落点**：390px/1280px Playwright 测试、截图（如现有 harness 保留）、完整 UI E2E。

### UI-D.4 小屏向导步骤发现

**验收**：

1. 390px 首屏可读到总步数和当前步骤，或有语义等价的进度表达。
2. 通过 Next、Previous 或点击步骤后，当前步骤按钮自动滚入可视区域。
3. 任意 step 可通过触屏和键盘访问；隐藏部分有明确滚动 affordance 或不再依赖水平猜测。
4. 草稿、YAML/表单单真相源、validation gating、hash 路由行为不回归。

**允许范围**：`web/src/pages/pipelines/first-task-wizard.tsx`、`web/src/styles.css`（必要时）、`web/src/i18n.ts`、`hack/e2e-ui.sh`。

**非目标**：不改成不允许回跳的强制线性 wizard，不增加新的 wizard step。

**证据落点**：390px box/scroll/focus 断言、既有向导回归 E2E。

### UI-D.5 刷新状态与 icon-only 无障碍

**验收**：

1. 自动刷新运行、暂停、失败三态都以独立的状态文本表达；上次成功刷新时间只作为辅助信息。
2. 若提供手动刷新，它有独立、可访问的按钮和 busy/failure 反馈，不与 auto-pause 状态混淆。
3. EN/ZH 下受影响的 icon-only 控件均有正确 `aria-label` 和 tooltip/title。
4. 自动刷新周期、失败重试、现有 API refresh 语义保持不变。

**允许范围**：`web/src/components/layout/app-shell.tsx`、`web/src/main.tsx`（仅必要时）、`web/src/i18n.ts`、`hack/e2e-ui.sh`。

**非目标**：不新增监控仪表盘，不修改后端刷新接口或引入新的 polling 系统。

**证据落点**：EN/ZH 状态切换与辅助技术语义断言；完整 UI E2E。

### UI-D.6 容器级验证与 Roadmap 收口

**验收**：

1. 使用实际应用容器，而非浏览器 API fixture，执行所有 UI-D 覆盖的关键路径。
2. 每个验收标准在 Roadmap UI-D 下记录命令、证据和结果；任何缺失的运行时/容器输入须记录 `blocked`，不能以 mock 结果代替。
3. `npm run typecheck`、`npm run lint`、`npm run build`、`hack/e2e-ui.sh` 均记录实际输出。
4. `git diff --check` 通过，且没有 mock route、临时截图、生成 bundle 或 secret 进入项目变更。

**允许范围**：验证命令、`docs/ROADMAP.zh.md`、本迭代文档、必要的 `hack/e2e-ui.sh`。

**非目标**：不借验证轮顺带实现新的事故聚合、信息架构或 connector 特性。

**证据落点**：Roadmap 验收矩阵、构建/E2E 日志、最终 git diff。

## Round 1 Claim 记录

```text
Round: 1/5（UI-D 窗口）
Roadmap item: UI-D / UI-D.1 + UI-D.2
Profile/path: standalone production Web UI（真实 app 容器验证）
Objective: 批量启停确认在 EN/ZH 完整本地化（无 {token} 泄漏、Cancel/Close 本地化），批量按钮与可执行目标匹配并有可见禁用原因
Scope: web/src/pages/pipelines/PipelinesPage.tsx、web/src/components/shared/confirm-dialog.tsx、web/src/components/ui/dialog.tsx、web/src/i18n.ts、hack/e2e-ui.sh
Non-goals: 批量 API/并发策略、status 派生、at-least-once 语义、其他页面
Acceptance: 1) 确认框 EN/ZH 无未替换 {action}/{count}/{names}，描述含目标数/名称/replay 风险；2) ConfirmDialog 默认 Cancel/Confirm 与 Dialog Close 按钮本地化（aria-label）；3) selection Start/Stop disabled 状态与可执行目标一致，禁用原因可见；4) 清空选择移除批量栏；5) 结果面板/toast 本地化
Evidence: npm run typecheck/lint/build 通过；CONTAINER_CLI=podman ./hack/e2e-ui.sh 全量 186/186 PASS（新增 D0/D1.1a-c/D1.2/D2.1-D2.3 十项断言，真实 app 容器 + API 预置 stopped fixture）；D1.1b 断言无 "{" token、含 Start/名称/replay/Cancel/Start all；D1.2 断言 ZH 启动/重放/取消/关闭
Result: UI-D.1、UI-D.2 delivered
Residual/follow-up: UI-D.3（390px 小屏布局）→ UI-D.4 → UI-D.5 → UI-D.6
```

### Round 1 验收矩阵

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| EN/ZH 确认框无 {token}、含目标数/名称/replay 风险 | e2e D1.1b（EN：Start+ui-d-stopped-fixture+replay+Cancel+Start all，正则拒绝 `{word}` token）、D1.2（ZH：启动/重放/取消/关闭）；`batchConfirmDesc` ZH 文案补 replay 重复风险 | passed | — |
| ConfirmDialog 默认标签本地化 | `confirm-dialog.tsx` confirmLabel/cancelLabel 回退 `ui.confirm`/`ui.cancel`（getLang）；调用方显式传参仍可覆盖 | passed | — |
| Dialog Close 可访问名称 | `ui/dialog.tsx` DialogPrimitive.Close 加 aria-label+sr-only `ui.close`（EN Close / ZH 关闭）；e2e D1.1c/D1.2 断言 | passed | — |
| 批量按钮与目标匹配 + 可见原因 | selection Start/Stop disabled + data-testid 原因文本（noStopped/noRunning/noExecutable）；Start-all/Stop-all 已有 disabled+title；e2e D2.1/D2.1b/D2.2 | passed | title 在 disabled 按钮上不可悬停（以可见文本兜底） |
| 清空选择移除批量栏 | e2e D2.3 | passed | — |
| 结果反馈本地化 | runBatch toast 改用 `pipe.startAll/stopAll`+`ui.ok/ui.failed`；结果面板 ✕ 按钮加 aria-label/title | passed | — |
| 前端回归 | typecheck/lint/build 通过；e2e 既有 176 项无回归（186-10 新增） | passed | — |

## Round 2 Claim 记录

```text
Round: 2/5（UI-D 窗口）
Roadmap item: UI-D / UI-D.3
Profile/path: standalone production Web UI（真实 app 容器验证）
Objective: 在 390px 让 Connections、Schedules、My Plugins 无整页横向溢出，同时保留键盘可达的核心/危险操作与 1280px 桌面表格密度
Scope: web/src/components/layout/app-shell.tsx、web/src/ConnectionsPage.tsx、web/src/SchedulesPage.tsx、hack/e2e-ui.sh、此任务记录
Non-goals: 重做桌面信息架构、隐藏 Delete/Replay、修改连接/调度/插件 API 或运行时语义
Acceptance: 1) 三页 document scrollWidth ≤ viewport；2) Connections 的 Load/Test/Delete 以本地 table scroller 可达且 Delete 可 focus；3) Schedules 编辑/运行入口与 My Plugins template/install 入口可 focus；4) 1280px 仍为 table 桌面布局；5) 无 duplicate action handler
Evidence: npm run typecheck + npm run build 通过；CONTAINER_CLI=podman E2E_SKIP_BUILD=1 ./hack/e2e-ui.sh 195/195 PASS（D3.1–D3.9，真实 app 容器）
Result: UI-D.3 delivered
Residual/follow-up: UI-D.4（390px 向导步骤发现）→ UI-D.5 → UI-D.6
```

### Round 2 验收矩阵

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| 三页 390px 无整页横向溢出 | `min-w-0` 应用于 app-shell 主列、Connections/Schedules grid item；e2e D3.1（Connections）、D3.3（Schedules）、D3.6（My Plugins） | passed | — |
| Connections 核心/危险操作可达 | `Table` 仍局部横滚，不扩展 document；D3.2 断言 Load/Test/Delete 存在且 DOM 顺序正确，focus Delete 后 table `scrollLeft` 自动移动、按钮在 viewport 内、document 仍不溢出 | passed | 未将危险动作缩成无名称图标 |
| Schedules 主操作可达 | D3.4 选中真实 b1-cron-probe 后断言 Edit in dialog/Run now enabled+focus；D3.5 打开 editor dialog 时仍无溢出 | passed | — |
| My Plugins 主工作流可达 | D3.7 切换 Editor，Load template + Install 存在，focus template 仍在 390px viewport 内 | passed | UI-F.6 后续处理该页面既有 raw i18n key，不改变本轮布局范围 |
| 1280px 桌面密度保留 | D3.8/D3.9 断言 Connections/Schedules 保留 table 且 document 不溢出；`xl` grid 定义未改 | passed | — |
| 真实容器全量回归 | `CONTAINER_CLI=podman E2E_SKIP_BUILD=1 ./hack/e2e-ui.sh`：195 passed / 0 failed | passed | — |

## Round 3 Claim 记录

```text
Round: 3/5（UI-D 窗口）
Roadmap item: UI-D / UI-D.4
Profile/path: standalone production Web UI（真实 app 容器验证）
Objective: 390px 向导首屏明确显示当前/总步骤，隐藏步骤有可见滑动提示，Next/Previous、触摸/点击和键盘选择都会把当前步骤带入 rail 可视区域
Scope: web/src/pages/pipelines/first-task-wizard.tsx、web/src/i18n.ts、hack/e2e-ui.sh、此任务记录
Non-goals: 改变草稿/YAML 单真相、validation gating、hash 路由、向导步骤数或线性/可回跳策略
Acceptance: 1) 当前/总步数可读；2) Next 到 Confirm 自动本地滚动 rail；3) 键盘 focus+Enter 选择任意隐藏 step 并可见；4) hash 仍为 `#/pipelines/new?step=<id>`；5) 390px 无整页溢出
Evidence: 先在旧 UI 镜像运行新增 D4 断言得到 196 passed / 4 failed（D4.1–D4.4，确认缺少 progress/hint/自动滚动）；实施后 npm run typecheck/lint/build 通过（lint 29 个既有 warnings、0 errors）；CONTAINER_CLI=podman ./hack/e2e-ui.sh 200/200 PASS
Result: UI-D.4 delivered
Residual/follow-up: UI-D.5（刷新状态与 icon-only a11y）→ UI-D.6
```

### Round 3 验收矩阵

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| 首屏可读当前/总步骤 | `wizard-step-progress` + `wizard.progressCurrent`（EN `Step 1 of 6: Scenario` / ZH `第 1 / 6 步：场景`），active button `aria-current="step"`；e2e D4.1 | passed | — |
| 隐藏步骤有明确发现提示 | mobile-only `wizard-step-scroll-hint`（Swipe/scroll sideways / 左右滑动或滚动）；e2e D4.2 | passed | desktop `xl` 竖向 rail 不显示不必要提示 |
| Next/Previous/route 改步自动可见 | `stepRailRef` + per-step refs，在 step effect 中以本地 rail `scrollTo` 居中；e2e D4.3 确认 Confirm rail 可见且 document 未溢出 | passed | — |
| 键盘可达 | native button + onFocus 同一 local-scroll 行为；D4.4 focus Source + Enter 后 hash/current/位置均正确 | passed | — |
| 草稿/YAML/hash 不回归 | D4.5 保留 `#/pipelines/new?step=source`；完整既有 wizard YAML/validation/DAG e2e 均包含于 200/200 | passed | — |

## Round 4 Claim 记录

```text
Round: 4/5（UI-D 窗口）
Roadmap item: UI-D / UI-D.5
Profile/path: standalone production Web UI（真实 app 容器验证）
Objective: 把自动刷新状态、上次成功时间和立即刷新操作分开表达；运行/暂停/失败均有可读状态，app-shell 的 icon-only 控件在 EN/ZH 有当前语言 aria-label/title
Scope: web/src/lib/types.ts、web/src/lib/api.ts、web/src/main.tsx、web/src/components/layout/app-shell.tsx、web/src/i18n.ts、hack/e2e-ui.sh、此任务记录
Non-goals: 改 5 秒 cadence、后端 refresh API/重试策略、增加 dashboard、改 pipeline 运行语义
Acceptance: 1) active/paused/failed 三态独立文本；2) 上次成功时间只作次级文本；3) 立即刷新独立按钮、busy/disabled/失败恢复反馈；4) EN/ZH icon-only aria/title；5) 既有 auto-pause 语义不变
Evidence: 新增 D5.1–D5.9；npm run typecheck/lint/build 通过（lint 29 个既有 warnings、0 errors）；CONTAINER_CLI=podman E2E_SKIP_BUILD=1 ./hack/e2e-ui.sh 209/209 PASS
Result: UI-D.5 delivered
Residual/follow-up: UI-D.6（真实容器回归、Roadmap/复审文档收口）
```

### Round 4 验收矩阵

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| active/paused/failed 三态清晰 | `auto-refresh-label` 主文本为 `Live updates active`、`Auto-refresh paused`、`Refresh failed · retrying`（ZH 同理）；D5.1/D5.5/D5.6 | passed | — |
| 上次成功时间不是操作标签 | `last-refresh-at` 以 `Last successful refresh: {time}` 单独渲染；D5.1 和既有 B3.2b | passed | 小屏隐藏辅助时间以避免拥挤，主状态仍可见 |
| 立即刷新独立且有 busy | `manual-refresh` icon Button、aria/title/aria-busy/disabled/spinner；`useApi.refreshing` 覆盖所有 in-flight request；D5.2–D5.4 使用延迟但真实 app `/api/v2/pipelines` 请求 | passed | — |
| 失败和真实恢复 | D5.6 用浏览器受控 transport reject 走既有 API error path（无生产 mock），D5.7 恢复真实 fetch 后立即刷新恢复 active | passed | 容器 API 成功路径仍覆盖 D5.1–D5.4/D5.7 |
| EN/ZH icon-only 可访问性 | `manual-refresh`、`toggle-lang`、mobile Open navigation、状态 toggle 都用 i18n 名称；D5.8/D5.9 | passed | 全项目历史 raw i18n key 归 UI-F.6，不扩大本轮范围 |
| cadence/auto-pause 不变 | `setInterval(..., 5000)` 与 `autoPaused` effect 未改；D5.5/B3.2/B3.2b 回归 | passed | — |

## Round 5 Claim 记录（UI-D 收口）

```text
Round: 5/5（UI-D 窗口）
Roadmap item: UI-D / UI-D.6
Profile/path: standalone production Web UI（真实 app 容器）
Objective: 在重建的应用容器内回归 UI-D.1–D.5，记录实际命令和证据，并同步 roadmap/迭代/复审结论
Scope: hack/e2e-ui.sh、docs/ROADMAP.zh.md、docs/iterations/UI-D-interaction-mobile-hardening/、docs/iterations/README.md、docs/ui-product-design-review-2026-09-24.zh.md
Non-goals: 新增事故聚合、信息架构、connector/runtime 特性；不以 test mock 代替成功路径 API 认证
Acceptance: 1) typecheck/lint/build；2) 无 E2E_SKIP_BUILD 的真实 app container 全量回归；3) 每个 UI-D criterion 有证据；4) diff hygiene；5) 仅将已执行事实回填文档
Evidence: `cd web && npm run typecheck && npm run lint && npm run build`（0 errors，29 existing warnings，Vite build success）；`CONTAINER_CLI=podman ./hack/e2e-ui.sh`（重建 image、真实 /api/v2、209 passed / 0 failed）；`bash -n hack/e2e-ui.sh`；`git diff --check`
Result: UI-D.1–UI-D.6 delivered（5/5 rounds）
Residual/follow-up: 无；任务型用户研究/API 契约项仍独立留在 queued UI-E，控制面后续项留在 UI-F/UI-G
```

### Round 5 验收矩阵（迭代总收口）

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| UI-D.1 EN/ZH 危险批量确认 | D0/D1.1a–D1.2：真实 API 创建 stopped streaming fixture，EN/ZH 文本、replay 边界、Cancel/Close、无 token | passed | — |
| UI-D.2 批量可供性 | D2.1–D2.3：stopped selection 下 Stop 禁用+可见原因、Start 可用、清空选择 | passed | completed/failed 等 status 不重新归类为 stopped |
| UI-D.3 小屏核心值守 | D3.1–D3.9：390px containment/local scroller/focus 与 1280px table 回归 | passed | — |
| UI-D.4 小屏向导发现性 | D4.1–D4.5：1/6 进度、scroll hint、Next/keyboard local rail auto-scroll、hash | passed | — |
| UI-D.5 刷新/a11y | D5.1–D5.9：active/paused/failure、last-success 分层、manual busy/recovery、EN/ZH names | passed | 失败分支用浏览器 transport reject 驱动现有客户端 error path；成功/恢复均打真实 app API |
| 静态和容器验证 | `npm run typecheck` passed；`npm run lint` 0 errors / 29 pre-existing warnings；`npm run build` passed；无 skip 的 `CONTAINER_CLI=podman ./hack/e2e-ui.sh` 209/209 | passed | — |
| hygiene | `bash -n hack/e2e-ui.sh`、文档回填后 `git diff --check` | passed | — |

## 领取记录模板

```text
Round: <n>/5
Roadmap item: UI-D / UI-D.<n>
Profile/path: standalone production Web UI
Objective: <one observable interaction outcome>
Scope: <explicit UI source/test/doc files>
Non-goals: <runtime semantics, unrelated information architecture, etc.>
Acceptance: <numbered checks from this task>
Evidence: <typecheck/lint/build/Playwright/app-container e2e/doc matrix>
Result: <delivered|active|blocked_external>
Residual/follow-up: <bounded item or none>
```
