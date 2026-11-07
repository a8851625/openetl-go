# UI-D：实测交互可信度与小屏可用性收口

> 状态：`delivered`（2026-09-26，UI-D.1–UI-D.6；真实 app 容器 `hack/e2e-ui.sh` 209/209 PASS） | 依赖：无（应用容器真实 API 联动已复验） | 归属 roadmap 条目：[UI-D](../../ROADMAP.zh.md)

## 问题陈述

2026-09-24 的 Playwright 实机走查覆盖总览、问题中心、管道列表/详情、创建向导、DLQ、Connections 和设置；页面由已构建 React UI 渲染，并以浏览器层代表性 API fixture 驱动。走查发现以下可复现缺口：

1. 管道批量启停确认描述遗漏 `{action}` 参数；中文场景仍露出 `Cancel`/`Close`，使高影响操作的意图与结果边界不可信。
2. 选择状态不匹配的管道时，批量 Start/Stop 仍显示为可点；内部因空目标直接返回，用户没有确认、toast 或禁用理由。
3. 390px 视口中 Connections 页面 `documentElement.scrollWidth` 达约 690px，Extensions/My Plugins 达约 540px，Schedules 也有轻微溢出；核心读状态与操作需要整页横向拖动。
4. 390px 视口中创建向导步骤 4–6 位于可横向滚动容器外，虽技术上可达但没有明确进度或滚动提示。
5. 顶栏把最近刷新时间作为自动刷新开关主文本，状态、时间与操作意图混在一起；部分 icon-only 控件缺少本地化可访问名称。

这些问题不改变 ETL runtime 语义，却会降低用户在启动、停止、值守和故障恢复时对系统的信任，并使手机场景难以完成核心运维任务。

## 可观察结果

1. 操作者在任一 EN/ZH 批量启停确认框中能读到正确动作、目标、风险和本地化按钮，且操作结束后能核对结果。
2. 操作者只能触发对所选目标实际适用的批量动作；不适用动作有可理解的禁用原因或明确反馈。
3. 在 390px 小屏上，用户无需横向拖动整个文档即可读 Connections、Schedules、Extensions/My Plugins 的关键状态并完成核心操作。
4. 小屏用户能立刻看出向导共 6 步、当前在第几步，并能到达所有步骤。
5. 自动刷新运行、暂停、失败和手动刷新（若保留）在视觉、文字与辅助技术上都可区分。

## 验收标准

| # | 验收标准 | 判定方式 |
| --- | --- | --- |
| 1 | EN/ZH 的批量 start/stop 确认框都无未替换 `{…}` token，标题、描述、取消、关闭和确认按钮均使用当前语言；描述包含目标数、目标名和 at-least-once/replay 风险。 | `hack/e2e-ui.sh` / Playwright：四个组合快照与文本断言；人工复核。 |
| 2 | 选择 empty、running-only、stopped-only、mixed 四种集合时，Start/Stop 的 enabled 状态与可执行目标一致；无目标不会发送请求，用户能看见禁用理由或明确 toast。 | Playwright 拦截请求 + DOM/ARIA/tooltip 断言。 |
| 3 | 390px 下 Connections、Schedules、My Plugins 页面均满足 `document.documentElement.scrollWidth <= window.innerWidth`；状态、主要动作和危险操作无需整页横向滚动即可访问；桌面布局不退化。 | Playwright 390px/1280px 断言；键盘 Tab 路径检查。 |
| 4 | 390px 下向导初始屏显示当前/总步数或等价信息；所有步骤可发现、当前步骤会滚入可视区域；触屏和键盘路径可达。 | Playwright 390px：元素 box、scrollLeft、焦点顺序与进度文本断言。 |
| 5 | 顶栏刷新控件的三种状态（运行、暂停、失败）使用不同且可理解的文字；每个 icon-only 交互有 EN/ZH 的 aria-label/tooltip；不会误把上次刷新时间表达为唯一主操作。 | Playwright EN/ZH + a11y 语义断言。 |
| 6 | 前端 typecheck/build/lint 与现有 `hack/e2e-ui.sh` 回归通过；在可用容器环境中，真实应用 API 下复验相同关键路径。 | CI 命令、应用容器 E2E 日志与 Roadmap 证据矩阵。 |

## 非目标

- 不重做整个信息架构、视觉体系或组件库。
- 不改变 pipeline status 派生、Source → Transform → Sink spec、connector descriptor、checkpoint、DLQ/replay 或 at-least-once 语义。
- 不把移动端所有复杂桌面表格完整复制到卡片；只保证核心值守任务可完成。
- 不在没有后端 incident 关联数据的前提下，UI-only 合并 Issues 或猜测根因。
- 不新增独立 UI 执行语义、模拟运行数据或绕过 preflight。

## 交付约束

- 先写失败的浏览器断言，再修复相应 UI；fixture 可用于组件交互走查，但交付前须在 `hack/e2e-ui.sh` 的真实 app 容器上复验。
- 任何危险动作的文案调整不得弱化“可能重放/可能重复”的 at-least-once 边界。
- 不适用的批量动作不得静默返回；禁用状态也必须对键盘用户和屏幕阅读器可解释。
- 小屏改造必须保留桌面内容密度，并使 Delete/Replay 等危险操作保持文本标签或同等可访问名称。
- 复用 `i18n.ts`、共享 Dialog/Tooltip/Button，禁止为单页引入第二套翻译、确认或状态模型。

## 依赖与前置

| 类型 | 内容 | 状态 |
| --- | --- | --- |
| 迭代依赖 | UI-A/UI-B/UI-C 已交付的共享 ConfirmDialog、i18n、responsive shell 与 UI E2E 基线 | 已满足 |
| 外部输入 | 容器运行时、应用镜像与 Playwright CLI，用于真实 API 下的最终验证 | 当前本地环境可能缺失；交付前必需 |
| 待决策 | UI-D 后的事故聚合 / 任务型信息架构是否立项 | 不阻塞 UI-D；需用户授权 |

## 完成定义（DoD）

迭代置 `delivered` 需同时满足：

1. `tasks.md` 的 UI-D.1–UI-D.6 均为 `done`，每项都有真实执行的证据。
2. 上方 6 条验收标准全部 `passed`；容器缺失不能标记 delivered，而应记录具体 blocker。
3. [ROADMAP UI-D 条目](../../ROADMAP.zh.md) 的逐项证据与状态已更新。
4. [UI 产品与交互设计复审](../../ui-product-design-review-2026-09-24.zh.md) 中已交付项同步标注，未验证的任务流建议保持后续项。
5. `git diff --check` 通过；不引入 secret、临时 demo 开关或 API mock 到生产 UI。嵌入式 `resource/public` 前端 bundle 是本项目受版本控制的发布资产，仅由正式 `web` build 更新。

## 交付回填（2026-09-26）

- UI-D.1–D.5 按五个小切片完成：批量确认/可供性、390px 值守页面、向导步骤发现、刷新状态和 app-shell a11y。
- `hack/e2e-ui.sh` 新增 D0、D1.1–D1.2、D2.1–D2.3、D3.1–D3.9、D4.1–D4.5、D5.1–D5.9；最终由重建的真实 app 容器执行，**209 passed / 0 failed**。
- 前端 `npm run typecheck`、`npm run build` 通过；`npm run lint` 0 errors、29 个既有 warnings。完整逐项证据见 [tasks.md](./tasks.md)。
- 无残留 follow-up：产品验证/API 契约相关工作仍是独立的 UI-E，未扩张本迭代范围。
