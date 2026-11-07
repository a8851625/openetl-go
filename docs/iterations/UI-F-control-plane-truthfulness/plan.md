> 状态：`delivered`（2026-09-27：F.1–F.7 全部交付，收口 265/265）

# UI-F 技术方案

> 本文是实现输入，不是验收标准。验收以 [spec.md](./spec.md) 与 [ROADMAP UI-F](../../ROADMAP.zh.md) 为准。

## 方案总览

UI-F 按“**先阻断错误对象操作，再统一状态真实性，最后扩展小屏边界**”拆成最小垂直 slice。现有 React + hash router + Radix/Tailwind + `useApi` 是唯一前端基础；不迁移路由框架、不新增 UI-only runtime state、不改变 `/api/v2` 运行语义。

browser fixture 用来稳定复现 route、dialog、DELETE、503 和 viewport 行为。每个实现 slice 必须先将对应 PW-R 发现写成失败的浏览器断言，最终再以真实应用容器中的 `hack/e2e-ui.sh` 验证。UI-F.5 与 UI-D.3 都会触碰响应式基础边界，因此必须先冻结 UI-D.3 的约束，或经用户批准后在同一有界改动中协调，不能各自做全局 CSS 修补。

## 分项设计

### UI-F.1：Pipeline Detail 的 route identity guard（PW-R1，P0）

**现状**：`main.tsx` 同时维护 `selectedPipeline` 和 hash route。`selected` 对未命中的选择回退到 `pipelinesList[0]`，而 detail 的 `pipeline` prop 又使用 `route.id` 查找结果 `|| selected`。所以失效 detail URL 会把列表第一条当作当前对象，动作请求由该错误对象的 `pipelineRef` 发出。

**目标**：对 detail route 单独解析 `resolvedPipeline`：只有 key/name 与 `route.id` 精确匹配时才传入详情页。加载中显示 loading；加载成功但无匹配时传 `undefined`，复用 `PipelineDetailPage` 的明确 not-found 分支；读失败时显示读取失败，不以列表第一条填补。所有 detail 的 lifecycle/reset/DLQ/designer action 都只能引用已验证的 resolved object。

**关键接口与文件**：

| 文件 | 改动性质 |
| --- | --- |
| `web/src/main.tsx` | 将全局列表选择与 detail route resolution 分离；移除 detail 的 `|| selected` fallback；仅对已验证对象派生 metric、crumb、page title 和操作回调。 |
| `web/src/pages/pipelines/PipelineDetailPage.tsx` | 必要时区分 loading / read error / not-found，确保 undefined 时不保留操作按钮。 |
| `web/src/lib/routing.ts` | 仅在需补 route/unit helpers 时改动；不扩展 URL schema。 |
| `hack/e2e-ui.sh` 或受控 Playwright 测试 | 覆盖有效/无效 detail URL、reload、back/forward 和网络请求 target。 |

**数据语义**：不改 start/stop/reset/DLQ API、checkpoint 或 at-least-once。安全原则是“无法证明 route 对象存在时，零生命周期请求”，而不是把操作转给默认对象。

**设计权衡**：不自动重定向到 Pipelines 列表，因为失效 URL 对排障/书签用户仍有诊断价值；not-found 页面应给清晰返回入口。UI-E.3 以后可扩展跨页上下文，但不能以它为前置来修复此 P0。

### UI-F.2：Settings 的 route-owned dialog state（PW-R2）

**现状**：route listener 进入 `settings` 时只调用 `setShowSettings(true)`，离开时没有对应关闭。侧栏 Settings 打开还能先写局部 state；于是 hash 和 dialog open state 有两个真相源。

**目标**：Settings 可见性由当前 route 单向派生，或者每次 route 改变都显式同步；离开 `settings` 必定关闭。Close/Esc 的目标路由、焦点恢复和 browser history 规则在实施前确定并由测试冻结。新的 URL 页面不能被已离开的 dialog overlay 拦截。

**关键接口与文件**：

| 文件 | 改动性质 |
| --- | --- |
| `web/src/main.tsx` | 收敛 `showSettings` 和 route state；将 `onOpenSettings`/`onClose` 统一经过 `navigate`。 |
| `web/src/pages/SettingsPage.tsx`、shared Dialog | 仅在需要显式 focus return、close label 或 controlled dialog 适配时调整。 |
| `hack/e2e-ui.sh` | `#/settings → #/schedules`、Close、Esc、back/forward、主操作点击和焦点回归。 |

**数据语义**：不改 token 持久化选择、LLM/worker settings API 或 refresh cadence。设置读取失败仍应由既有失败分支可见，不得因为收敛 dialog state 而吞掉。

**设计权衡**：不引入“modal stack”或自定义 history state。Settings 是一个 hash route；任何临时 UI state 都不能与该 route 的可见性冲突。

### UI-F.3：破坏性控制操作的确认基线（PW-R3）

**现状**：Workers、Connections、My Plugins 直接在按钮 handler 中 DELETE；已有 checkpoint reset、Run now 和部分 DLQ 路径却使用 `ConfirmDialog`，风险模式不一致。

**目标**：复用共享 `ConfirmDialog`，使 worker deregister、connection delete、plugin uninstall 都经过 `对象 → 已知影响/未知性 → 不可逆或恢复路径 → confirm`。请求只能在 confirm 回调中发出；取消、Escape、关闭都零请求。Connection 如无 server-backed reference 数据，必须明确“引用关系不可确认”；UI-F 不在浏览器中猜引用，UI-E.5 才消费未来获批的服务端事实。

**关键接口与文件**：

| 文件 | 改动性质 |
| --- | --- |
| `web/src/WorkersPage.tsx` | 将 deregister 变为 pending target + ConfirmDialog，保留 refresh/error 流。 |
| `web/src/ConnectionsPage.tsx` | 将 delete 变为确认路径，表达 reference facts 或不可确认状态。 |
| `web/src/MyPluginsPage.tsx` | 将 uninstall 变为确认路径，保留 install/editor state。 |
| `web/src/components/shared/confirm-dialog.tsx`、`web/src/i18n.ts` | 复用本地化、危险风格、confirm/cancel/close 文案；不复制 dialog。 |
| `hack/e2e-ui.sh` | 三类对象的取消/确认/请求数/EN-ZH 文案断言。 |

**数据语义**：只改变请求前的用户确认与失败/成功反馈，不改变 DELETE endpoint、服务端删除判定、审计或恢复语义。connection 的最终删除授权仍由后端决定。

**设计权衡**：不使用浏览器原生 `confirm()`，以保证本地化、可访问焦点控制和影响说明一致；不承诺 undo，除非服务端提供真实可恢复契约。

### UI-F.4：loading / error / empty / data 的事实分层（PW-R5、PW-R7、PW-R8）

**现状**：Audit 和 Connector catalog 只从 `data` 推导空数组，忽略 `ApiState.error`；My Plugins 将 plugin GET 失败收敛成空列表。Workers 的 Running tasks 区无 task telemetry 时仍使用 `worker.noWorkers`；Detail 把一行 runtime snapshot 表达成 Run history。

**目标**：页面先按 `loading → error → data/empty` 渲染。error state 要说明当前无法做出的判断并提供 retry；empty 只在成功响应且集合确为空时出现。Workers 以“无任务 telemetry/暂无运行任务/无 worker”表达不同事实；Details 在未接 storage-backed history 前改名为当前快照或明确历史不可用。

**关键接口与文件**：

| 文件 | 改动性质 |
| --- | --- |
| `web/src/pages/AuditPage.tsx` | 消费 `audit.loading/error`，增加 error/retry（由父层传 callback 或约定的 refresh key）。 |
| `web/src/pages/ConnectorsPage.tsx`、`web/src/main.tsx` | 消费 plugins/schema error，避免失败空态；保留 cards/matrix 选择。 |
| `web/src/MyPluginsPage.tsx` | 分开 local loading/error/empty，失败不归零为“未安装”。 |
| `web/src/WorkersPage.tsx` | 区分 worker 注册状态与 task telemetry 状态；不造 task 数据。 |
| `web/src/pages/pipelines/PipelineDetailPage.tsx`、`web/src/i18n.ts` | 修正 snapshot/history 文案与可解释的 unavailable 状态。 |
| `hack/e2e-ui.sh` | 503、loading、successful empty、data 四分支断言。 |

**数据语义**：不改变 retry、polling 或 API payload。错误文案不能用当前客户端刷新时间、累计值或空集合推断 runtime 健康。

**设计权衡**：不把原始 JSON 当作唯一用户文案，也不隐藏可展开的技术详情；ErrorBox 的用户说明和 API detail 可以分层呈现。

### UI-F.5：UI-D 之外的小屏 containment 与局部操作（PW-R4）

**现状**：390px 下 Detail Runs、Workers、Audit、Connector catalog 和 DAG Designer 都会将整个 document 撑宽。临时在浏览器加入 AppShell 主 flex child `min-width: 0` 后，多个表页恢复局部滚动，但 Schedules/DAG 仍有自身最小宽度问题。UI-D.3 已负责 Connections/Schedules/My Plugins，因此不能在本项重写其范围。

**目标**：先建立 AppShell 可收缩的共享基线，再只处理 UI-D 之外的页面：Detail Runs、Workers、Audit、Connector catalog、DAG palette/canvas。宽表应在局部容器滚动或变为任务优先的窄屏布局；DAG palette/画布有自己的可见、可操作边界。UI-D.3 所属页的回归必须被一起测量。

**关键接口与文件**：

| 文件 | 改动性质 |
| --- | --- |
| `web/src/components/layout/app-shell.tsx` | 主内容 flex child 的收缩边界；不全局隐藏 overflow。 |
| `web/src/components/ui/table.tsx` | 仅在确保 local scroller containment 所必需时调整。 |
| `web/src/pages/pipelines/PipelineDetailPage.tsx`、`web/src/WorkersPage.tsx`、`web/src/pages/AuditPage.tsx`、`web/src/pages/ConnectorsPage.tsx` | 保持桌面密度的小屏局部表/卡片设计。 |
| `web/src/DagEditorPage.tsx`、必要样式 | palette 折叠/局部滚动或 drawer 与画布 pan/zoom 边界。 |
| `hack/e2e-ui.sh` | 390px document width + local scroller + focus/action；1280px 回归。 |

**数据语义**：纯呈现和可达性改动；不得新建移动端独立数据、隐藏危险操作或改变 DAG/pipeline 保存、preflight、connection test。

**设计权衡**：不采用全局 `overflow-x:hidden`；它会让不可见列或画布内容无法访问。只允许指定的 table/canvas 容器横向滚动或平移。

### UI-F.6：i18n 和稳定字段语义（PW-R6、PW-R9）

**现状**：My Plugins 调用了 `common.kind`、`common.version`、`common.uninstall`，但字典未定义，导致 raw key 可见。Settings token、Schedules cron、DAG pipeline name 的 accessibility name 可由 placeholder 回退，但其 visual Label 没有绑定到 input。

**目标**：补齐或改用正确的 EN/ZH key，建立开发期检测避免 raw key 回归。为受影响字段设置稳定 `id + htmlFor` 或 `aria-labelledby`，并在错误/帮助存在时设置 `aria-describedby`。placeholder 只作为示例，不作为唯一语义。

**关键接口与文件**：

| 文件 | 改动性质 |
| --- | --- |
| `web/src/i18n.ts`、`web/src/MyPluginsPage.tsx` | 修复表头/卸载文案和 EN/ZH 回归。 |
| `web/src/pages/SettingsPage.tsx`、`web/src/SchedulesPage.tsx`、`web/src/DagEditorPage.tsx` | 建立 label / input / help / error 关系。 |
| 现有或新增前端测试、`hack/e2e-ui.sh` | raw-key scan、accessibility snapshot、EN/ZH 文案断言。 |

**数据语义**：不改变 token 储存、cron parser、DAG spec field 或任何 connector/config payload。

**设计权衡**：不把 placeholder 移除；保留它作为示例，同时通过稳定 label 让输入后与辅助技术路径仍可理解。

## 架构约束

- 只复用 `api` / `useApi`、hash router、shared ConfirmDialog、ErrorBox、i18n 和现有 Tailwind/Radix 组件。
- route、selected pipeline、modal、error 和 empty 不能各自另建隐式状态真相源。
- 不建立前端 incident/task/reference 推断模型；服务端还未提供的事实必须显示 unavailable/unknown。
- 不改变任何 source position、checkpoint commit、DLQ、replay、worker/connector/plugin API 的数据面或持久化语义。
- 任何 responsive 修复必须保持危险操作的文字标签或等价 accessible name。

## 风险与回滚

| 风险 | 影响 | 缓解 | 回滚路径 |
| --- | --- | --- | --- |
| route guard 与慢加载交错 | 误显 not-found 或短暂错误 UI | 明确 loading/error/not-found 三态，针对初始加载/refresh 测试 | 单独 revert UI-F.1 route-resolution commit；不改后端数据 |
| Settings route 收敛破坏 close/back 预期 | 用户卡在设置或 history 循环 | 实施前冻结 close route/focus 规则，覆盖 Esc/Close/back/forward | 单独 revert UI-F.2 controlled-state commit |
| 删除确认仅是视觉层、影响事实不完整 | 用户误以为已做 dependency 分析 | 无权威数据时明确 unknown；服务端终审；UI-E.5 后续补事实 | revert confirmation UI，不改变 DELETE API |
| error state 改动把成功空态误报失败 | 运维噪音 | 用 status/ApiState 分支测试 loading/error/empty/data | 单页 revert，API/polling 不变 |
| 共享 min-width 修复影响 UI-D 页面 | 其他页面的表格或画布退化 | UI-D.3 协调、390px 与 desktop 矩阵、局部 scroller 断言 | revert shared shell change，保留页面级修复 |
| i18n key 仅在一个语言修复 | 另一语言仍泄漏或 fallback | EN/ZH text scan + accessibility assertion | revert key/caller paired commit |
| fixture 与真实 API / container 漂移 | mock 绿而运行时回归 | `hack/e2e-ui.sh` 为 delivered 门槛 | 保持 queued / 记录 blocker，不能宣称完成 |

## 测试策略

按 [AGENTS.md 步骤 6](../../../AGENTS.md) 由窄到宽：

| 层级 | 范围 | 命令 / 证据 |
| --- | --- | --- |
| 1 前端静态 | 路由类型、TSX、i18n、label 关系 | `cd web && npm run typecheck && npm run lint && npm run build` |
| 2 定向浏览器 | 无效/有效 detail、dialog route、DELETE confirm、503、390px、EN/ZH、ARIA | 新增或扩展 Playwright 断言；请求拦截只作为 UI 分支测试 |
| 3 route / API 契约 | route parsing、connection reference 已知性、error payload | 对应前端 unit 与后端 API contract 测试；无 contract 时明示 unavailable |
| 4 应用容器 | 真实反向代理、真实 API、全部受影响页面 | `CONTAINER_CLI=<docker|podman> bash hack/e2e-ui.sh` |
| 5 故障 / 回归 | API 读取失败、删除拒绝、路由刷新、小屏和 DAG | 可注入的容器路径；完整 UI E2E 与 `git diff --check` |
