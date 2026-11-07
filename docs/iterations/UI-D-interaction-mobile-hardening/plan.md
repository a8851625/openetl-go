# UI-D 技术方案

> 本文是实现输入，不是验收标准。验收以 [spec.md](./spec.md) 与 [ROADMAP UI-D](../../ROADMAP.zh.md) 为准。状态：`delivered`（2026-09-26；实现与证据已回填 tasks.md）。

## 方案总览

UI-D 采用**小而独立的前端垂直 slice**：每个 slice 先将 2026-09-24 Playwright 走查的可复现缺口编码为浏览器断言，再在现有 React/Tailwind/Radix 组件上最小修复。不得引入新的运行时端点、页面数据模型或静态 UI 执行语义。浏览器 fixture 用于高覆盖交互验证；最终在 app 容器内以真实 `/api/v2` 路径复验。

不选择全局响应式重构或重做 UI 信息架构：它们会混淆“可用性缺陷修复”和未验证的产品设计假设，并扩大现有稳定 UI 的回归面。

## 分项设计

### UI-D.1：批量确认的意图、风险与本地化

**现状**：`PipelinesPage` 对 `pipe.batchConfirmTitle` 替换了 `{action}`，但 `pipe.batchConfirmDesc` 只替换 `{count}` / `{names}`。共享 `ConfirmDialog` 的默认 `cancelLabel` 为英文，`components/ui/dialog.tsx` 的 sr-only Close 也为英文。

**目标**：由调用点显式提供本地化 Cancel/确认文案；描述和标题都使用同一个 action display label；共享 Dialog 支持可本地化 close label。批量结果继续使用当前受控并发和结果面板，且改为翻译后的统计文本。

**关键接口与文件**：

| 文件 | 改动性质 |
| --- | --- |
| `web/src/pages/pipelines/PipelinesPage.tsx` | 统一生成 action label；替换 description 参数；为 ConfirmDialog 提供 cancelLabel；完善无目标反馈。 |
| `web/src/components/shared/confirm-dialog.tsx` | 默认英文移除或调用点显式化，避免隐式语言泄露。 |
| `web/src/components/ui/dialog.tsx` | 为 close accessible label 提供可传入的本地化机制，兼容其他 Dialog 调用者。 |
| `web/src/i18n.ts` | 补齐 close、tooltip、batch result、excluded target 等键。 |
| `hack/e2e-ui.sh` | EN/ZH start/stop confirm 断言，检查文本中没有未替换 token。 |

**数据语义**：不改变 POST `/api/v2/pipelines/:id/start|stop`、并发上限、checkpoint 或 replay 行为。文案必须继续说明流式管道中断在途批次与可能重放的边界。

**设计权衡**：不在确认框堆入全部管道名或大量统计；默认展示前 5 个目标和总数，更多目标以“其余 N 条”表达，避免确认框自身成为不可扫读的日志。

### UI-D.2：批量动作可供性与结果反馈

**现状**：`batchAction` 对空 target 直接 return；选择态工具栏可能提供可点击但无结果的 Start/Stop。

**目标**：根据 selected targets 计算 `startable` 与 `stoppable`，不适用动作禁用；给 `disabled` 按钮提供可访问原因（包装 Tooltip 或 visible helper），并保持有 target 时现有确认/结果汇总链路。

**关键接口与文件**：

| 文件 | 改动性质 |
| --- | --- |
| `web/src/pages/pipelines/PipelinesPage.tsx` | 从选择集派生可启动/可停止目标、disabled/title/tooltip、fallback toast；不修改 status 判定。 |
| `web/src/components/ui/tooltip.tsx`（仅必要时） | 保证 disabled 控件的原因仍能聚焦/可读取。 |
| `web/src/i18n.ts` | `noStoppedTargets` / `noRunningTargets` 及选择态提示统一。 |
| `hack/e2e-ui.sh` | 拦截请求并验证 empty/running/stopped/mixed 集合。 |

**数据语义**：不改变批量请求列表、受控并发或失败收集；无目标必须保证不发 API 请求。

**设计权衡**：优先禁用 + 原因，而不是允许点击后 toast；对高影响操作，执行前看出不能执行优于执行后才获悉无结果。

### UI-D.3：小屏关键运维页的任务化布局

**现状**：Connections 表使用桌面多列表格；390px 下整页宽度超过 viewport。Schedules 和 My Plugins 也有整页溢出。全局 `Table` 的 `overflow-auto` 不意味着当前页核心内容可用。

**目标**：保持桌面 table，`< md` 或明确断点下将关键条目改成单列任务卡或紧凑行：首层显示名称/状态/类型，次层显示最后检测/错误摘要，操作层显示 Load/Test 与 More；Delete 位于显式危险菜单或确认入口。Schedules/My Plugins 按同样原则隐藏低优先列而不隐藏关键状态与可操作入口。

**关键接口与文件**：

| 文件 | 改动性质 |
| --- | --- |
| `web/src/ConnectionsPage.tsx` | 桌面表格保持，增加移动端卡片/行或受控滚动表格；保障 Test/Load/Delete 可达。 |
| `web/src/SchedulesPage.tsx` | 消除移动端整页溢出，保持 run/enable/disable 等关键任务。 |
| `web/src/MyPluginsPage.tsx` | 消除移动端整页溢出，保持状态、卸载/查看等关键任务。 |
| `web/src/components/ui/table.tsx`（仅必要时） | 容器级响应式支持；禁止用全局 CSS 强行改变所有表格。 |
| `web/src/i18n.ts`、`hack/e2e-ui.sh` | 移动卡片标签和 390px regression 断言。 |

**数据语义**：只改变呈现层；连接测试、加载、删除、调度和插件 API 调用不变。

**设计权衡**：不以隐藏危险操作换取“看上去不拥挤”；危险操作保留带文字的可发现入口并通过 ConfirmDialog 确认。

### UI-D.4：移动端向导进度与步骤发现

**现状**：步骤容器允许横向滚动，但初始屏只显示前 3 步，且没有当前/总步数或可滚动提示。

**目标**：增加可访问进度（例如“第 2 / 6 步 · Source”）；点击/Next/Previous 后当前 step 自动滚动至可见区。保留横向步骤条时增加右侧渐隐/提示或等价 affordance；不要改变 hash step、草稿保存或验证阻断。

**关键接口与文件**：

| 文件 | 改动性质 |
| --- | --- |
| `web/src/pages/pipelines/first-task-wizard.tsx` | step nav ref、scrollIntoView、progress label、移动端 affordance。 |
| `web/src/i18n.ts` | 当前步骤/总步数/滑动提示本地化。 |
| `web/src/styles.css`（仅必要时） | 极小的渐隐/滚动视觉提示，遵守 reduced motion。 |
| `hack/e2e-ui.sh` | 390px 当前步骤可见、Next 后 scroll 位置与 ARIA 断言。 |

**数据语义**：不改变 step hash、草稿 localStorage、YAML/表单单真相源或 create/preflight 请求。

**设计权衡**：不改成强制线性步骤（用户可回看已有配置）；进度只是增强定位和发现，不减少专家用户的跳步能力。

### UI-D.5：刷新状态和 icon-only a11y

**现状**：`AppShell` 用 `lastRefreshAt` 作为非暂停时的按钮文本，自动刷新状态、最后刷新时间和操作语义混合；部分 icon-only 操作依赖 title 或固定英文 aria。

**目标**：控件主文本表达状态（实时更新、已暂停、刷新失败）；最近成功时间为辅助信息；若新增手动刷新，语义、加载状态和失败回退均明确。所有 icon-only 操作提供当前语言的 accessible name。

**关键接口与文件**：

| 文件 | 改动性质 |
| --- | --- |
| `web/src/components/layout/app-shell.tsx` | 分离 status label 与 last refresh metadata；本地化 aria-label/title。 |
| `web/src/main.tsx` | 仅当需要主动刷新时接入既有 refresh key，保留当前 auto-pause 行为。 |
| `web/src/i18n.ts` | 状态、最近刷新、立即刷新、可访问名称。 |
| `hack/e2e-ui.sh` | EN/ZH 三态、pressed、accessible name 断言。 |

**数据语义**：不改 5 秒刷新 cadence、API 请求集合或失败重试策略；手动刷新（若实现）只触发已有 `refreshKey`。

**设计权衡**：不为状态设计加入额外仪表盘；把一个模糊按钮变成一个可理解的状态控件即可。

## 架构约束

- 不创建 UI-only API、mock 数据开关或第二套 pipeline 状态派生逻辑。
- 不引入新的前端框架、状态管理库或替换 Radix/Tailwind 组件。
- 不把 Connector、Worker、DAG/Plugin 开发者能力提升为日常移动端一级操作。
- 不改变高影响操作的 API、checkpoint 边界、DLQ 删除/replay 或 at-least-once 合同。

## 风险与回滚

| 风险 | 影响 | 缓解 | 回滚路径 |
| --- | --- | --- | --- |
| 全局 Dialog 可访问名称改动影响既有弹窗 | 文案/aria 回归 | 保持可选参数和默认兼容；覆盖所有现有调用 | 单独 revert UI-D.1 shared-component commit |
| 响应式分支令移动/桌面状态分裂 | 操作入口不一致 | 共用 action handler，只替换布局；同一 fixture 覆盖两个断点 | 单独 revert page layout commit |
| 批量按钮过度禁用 | 用户无法理解筛选后为什么不可操作 | 辅助文本/tooltip、混合选择回归覆盖 | revert UI-D.2 derived state commit |
| 手动刷新带来重复 API 请求 | UI 负载上升 | 复用 refreshKey、loading 状态防抖/disable | 不新增端点，revert UI-D.5 UI commit |
| fixture 与真实后端契约漂移 | mock 绿但容器失败 | 交付门禁要求 app-container `hack/e2e-ui.sh` | 保持 queued / 不宣布 delivered |

## 测试策略

按 [AGENTS.md 步骤 6](../../../AGENTS.md) 由窄到宽：

| 层级 | 范围 | 命令 |
| --- | --- | --- |
| 1 静态与前端检查 | 受影响 TSX/i18n | `web: npm run typecheck && npm run lint && npm run build` |
| 2 浏览器交互 | EN/ZH、高影响确认、批量选择、390px/1280px、键盘/ARIA | 扩展 `hack/e2e-ui.sh` 或受控 Playwright 测试 |
| 3 真实 app API | 反向代理、真实 API、容器内 UI | `CONTAINER_CLI=<docker|podman> bash hack/e2e-ui.sh` |
| 4 回归 | 既有 UI 路由、向导、DLQ、连接 | 完整 `hack/e2e-ui.sh`；若代码触及通用组件则覆盖相关页面 |
| 5 外部环境认证 | 不适用 | 不需要外部 connector credential；容器应用为最终门槛 |
