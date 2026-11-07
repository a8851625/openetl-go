# UI-F：控制面路由安全与运行状态真实性

> 状态：`delivered`（2026-09-27：UI-F.1–UI-F.7 全部完成；收口轮真实容器非 skip 全量 265/265） | 依赖：UI-D.3 的共享小屏基线已冻结；真实 app 容器用于最终验证 | 归属 roadmap 条目：[UI-F](../../ROADMAP.zh.md)

## 问题陈述

2026-09-25 的 [Playwright CLI 补充实测复审](../../ui-playwright-supplemental-review-2026-09-25.zh.md) 覆盖了此前未充分操作的 Pipeline Detail、Settings、Workers、Audit、Connector catalog、Extensions 与 DAG Designer。走查使用已构建前端和浏览器层代表性 API fixture；以下是可复现的**前端控制面表达/交互缺口**，并不等同于真实 runtime 已验证的后端语义：

1. ~~无效 Pipeline Detail URL 会回退为列表默认管道。~~ **UI-F.1 delivered（2026-09-27）**：`#/pipelines/not-a-real-pipeline/overview` 现呈现 route-tied not-found；detail props 只来自 `resolvePipelineRoute`，不再回退到列表 selection。真实 app container 的 direct/reload/back/forward、零 lifecycle/checkpoint request 和有效七 tab 断言均通过（217/217）。
2. ~~`#/settings` 打开由局部 state 维护的 dialog；离开该 hash 后 dialog 仍覆盖新页面并阻断后续按钮。~~ **UI-F.2 delivered（2026-09-27）**：移除 `showSettings`，Settings open 仅派生自 hash route；F2.1–F2.7 在真实 app container 覆盖 direct、离开 settings 后的 Schedules Run now、Esc、Close、back/forward 和焦点未困在 overlay（223/223 non-skip，224/224 final script run）。
3. ~~Worker deregister、Connection delete、Plugin uninstall 首击直接 DELETE，无确认、无对象/影响表达。~~ **UI-F.3 delivered（2026-09-27）**：三处首击均弹共享 `ConfirmDialog`（对象名 + 影响未知性/恢复边界，EN/ZH；connection 明确“引用关系不可确认”）；取消/Esc/overlay/Close 零 DELETE；confirm 恰一次目标请求且成功/失败结果页面内可见。真实 app container F3.1–F3.17 全部 PASS（242/242）。
4. ~~Audit/Connector catalog/Extensions 读取失败（503）伪装成“无记录/未安装”空态。~~ **UI-F.4 delivered（2026-09-27）**：error 与 empty 分支互斥；503 显示可解释错误 + retry（catalog 接线共享 refreshKey）；Workers 任务区在有 worker 时明确 telemetry unavailable 而非“暂无工作节点”；Detail Runs 注明当前快照来源。F4.1–F4.6 于真实 app container 全部 PASS（254/254）。
5. ~~Extensions 显示 raw i18n key；Settings token、Schedules cron、DAG pipeline name 仅靠 placeholder 提供语义。~~ **UI-F.6 delivered（2026-09-27）**：F.3 补齐 Extensions EN/ZH 文案；token/cron 输入有 `id+htmlFor` 稳定 label，DAG name 输入有 `aria-label`；F6.1–F6.4 通过（254/254）。
6. ~~390px 下 Detail Runs、Workers、Audit、Connector catalog、DAG Designer 整页横滚（DAG 713px）。~~ **UI-F.5 delivered（2026-09-27）**：palette/drawer-tabs 行改局部 `overflow-x-auto`（不收缩内容、不用全局 hidden）；F5.1–F5.5（五页 containment、UI-D.3 无回退、palette 键盘可达、canvas contained、1280px 回归）真实 app container 全 PASS（265/265）。
7. **UI-F.7 delivered（2026-09-27）**：收口轮以非 skip 全量镜像重建在真实 app container 单次运行 265/265，覆盖 F1–F6 全部断言；ROADMAP/board/spec/review 四处证据回填且保留原始发现；git diff --check 通过。
3. Worker deregister、Connection delete、Plugin uninstall 都直接发送 DELETE，缺少与 checkpoint reset / Run now 一致的意图确认；Connection 引用影响不能由客户端猜测。
4. Audit、Connector catalog、Extensions 的 HTTP 503 被渲染成普通“无数据”空态，用户无法区分系统为空和控制面读取失败；Workers 的 Running tasks 在已有 workers 时仍称“没有 workers”；Detail 的 Run history 实际为当前 runtime snapshot。
5. UI-D.3 未覆盖的 390px 页面也发生整页横向溢出：Pipeline Detail Runs、Workers、Audit、Connector catalog、DAG Designer。浏览器临时 CSS 实验表明 AppShell main flex item 的 min-content 收缩是共因之一，但 Schedules/DAG 等页面还存在自身的宽表/节点库超宽，不能以全局隐藏 overflow 修复。
6. Extensions 已安装插件表泄漏 `common.kind`、`common.version`、`common.uninstall` 等 i18n key；部分字段的可访问名称仅来自 placeholder，缺少稳定的 label 关系。

这些问题不会改变 Source → Transform → Sink、checkpoint、DLQ/replay 或 at-least-once 语义，但会使操作者在失效链接、删除、故障读取和小屏值守时无法确定“当前对象是什么、操作是否会发生、信息是否可信”。

## 可观察结果

1. 用户直接打开或刷新无效 pipeline detail URL 时，看到明确 not-found 状态；页面不渲染任何其他管道的控制按钮，且不会对其他对象发出 start/stop/reset/DLQ 动作请求。
2. Settings 只在路由为 `settings` 时可见；任意 hash 离开、浏览器前进/后退、Esc 和 Close 后，URL、焦点与可交互页面一致。
3. Worker、Connection、Plugin 的破坏性删除在发出请求前明确说明对象、影响已知性和不可逆性，并要求用户确认；无引用事实时系统明确“不确定”，而不是猜测安全。
4. Audit、Catalog、Extensions、Workers、Run snapshot 在 loading、error、empty、data 四种状态中使用不同、可解释且符合事实的文案；用户不会把 503 当成“没有记录/没有插件/没有 connector”。
5. 在 390px 小屏，UI-F 覆盖页面不会让整个 document 超过 viewport；表/画布可在局部容器内滚动或平移，且核心状态、主操作与危险操作可达。UI-D.3 的 Connections/Schedules/My Plugins 约束不被回退。
6. Extensions 表和受影响字段显示当前语言的产品文案，并有稳定、可读的辅助技术名称；placeholder 不再是字段唯一长期说明。

## 验收标准

| # | 验收标准 | 判定方式 |
| --- | --- | --- |
| 1 | 无效 `#/pipelines/<id>/<tab>` 显示 not-found；不会渲染其他 pipeline 名称/路径作为当前对象；对 Start、Stop、Reset、DLQ 等入口不产生其他 pipeline 的请求。有效 ID 的全部 7 个 detail tab 仍可直达。 | Playwright 请求拦截 + direct URL / reload / back-forward 断言；route/unit 测试；真实 app 容器回归。 |
| 2 | `#/settings → #/schedules`、back/forward、Esc、Close button 后，dialog visibility、hash、focus target 和下一页主操作一致；不存在遗留 overlay 拦截点击。 | Playwright hash / keyboard / focus / pointer-interaction 断言。 |
| 3 | Worker deregister、Connection delete、Plugin uninstall 都先出现本地化 ConfirmDialog；只有确认后才发 DELETE。Connection 引用无权威数据时明确标示不可确认；有已批准 API 事实时才显示具体影响。 | Playwright 网络与 dialog 文本断言；EN/ZH；相关 API contract / 容器 E2E。 |
| 4 | Audit、Connector catalog、Extensions 在 HTTP 4xx/5xx 时显示 error + retry，而不是 empty；请求成功且集合为空时才显示 empty。Workers 任务区和 Detail Runs 的标题/空态准确表达“无 tasks / telemetry unavailable / current snapshot / history unavailable”。 | Playwright route interception；组件/浏览器状态断言；真实 API outage 回归（若 harness 可注入）。 |
| 5 | 390px 下 Pipeline Detail Runs、Workers、Audit、Connector catalog、DAG Designer 满足 `document.documentElement.scrollWidth <= window.innerWidth`；局部宽表/画布仍可操作。与 UI-D.3 的共用 AppShell/table 改动协同，不回退 UI-D 已覆盖页面。 | 390px / 1280px Playwright `scrollWidth`、焦点与局部 scroller / canvas interaction 断言；视觉人工复核。 |
| 6 | Extensions 不显示 i18n key；受影响 EN/ZH 卸载及表头文案正确。Settings token、cron、DAG pipeline name（以及本 slice 新改字段）通过 `id + htmlFor` 或 `aria-labelledby` 有稳定 label，错误/提示能通过 `aria-describedby` 关联。 | EN/ZH Playwright text / accessibility snapshot；静态 i18n key scan。 |
| 7 | 前端 typecheck、lint、build、受影响的 Playwright 和 `hack/e2e-ui.sh` 都记录真实结果；不把 fixture 成功替代真实 app container 结论。 | 命令日志、ROADMAP 证据矩阵、最终 `git diff --check`。 |

## 非目标

- 不迁移到 React Router，不建立第二套路由/管道选择 state，也不把 transient secret、完整 YAML、token 或错误 payload 写入 hash。
- 不改变 Pipeline API、worker 分配、plugin 生命周期、checkpoint、DLQ/replay、sink acknowledgement 或 at-least-once 的数据语义。
- 不在前端根据名称、错误字符串或时间接近程度推断 Connection 引用、任务影响或事故根因；UI-E.3/UI-E.5 继续负责有 server-backed 契约的跨对象上下文。
- 不将每张复杂桌面表完整复制到手机卡片，也不通过全局 `overflow-x: hidden` 或隐藏危险操作来“修复”溢出。
- 不承诺完整 storage-backed run history；本迭代只确保当前页面名称与现有事实一致。
- 不借本项重做 Dashboard、首次成功向导或全局信息架构。

## 交付约束

- **路由对象身份是动作安全边界**：高影响 API 的目标必须来自已验证、与 route ID 一致的对象；加载中、无效 ID、权限/读取失败时均不可 fallback 至列表首项。
- Dialog open state 与 hash 的单向关系必须可推导；关闭后的 URL/focus 行为须在实现前冻结并测试，避免 browser history 循环。
- 删除确认要保持 at-least-once/replay 风险说明的真实性；不允许因未有 impact 数据就声称“没有影响”。
- empty 仅表示“请求成功且结果为空”；error 要说明“哪些判断目前不可作出”，并提供重试或明确的下一步。
- UI-F.5 与 UI-D.3 共享 AppShell / table responsive 边界；实施前必须先确认 UI-D.3 的最终约束或在同一经批准的最小 slice 中一致更新，避免两个 queued 迭代各自修改基础布局。
- fixture 用于复现和定向断言；交付前必须在实际 app container 上执行 `hack/e2e-ui.sh`。容器/运行时缺失时保持 `queued` 或准确记录 blocker，不能以 mock 宣布 delivered。

## 依赖与前置

| 类型 | 内容 | 状态 |
| --- | --- | --- |
| 迭代依赖 | UI-D.3 的小屏共享基线（对 UI-F.5 是前置；UI-F.1–UI-F.4/UI-F.6 可独立预研） | `delivered`（2026-09-26，UI-D 209/209） |
| 相关后续 | UI-E.3/UI-E.5 的稳定 deep-link、connection reference 和恢复事实契约 | `blocked_external`（UI-E.1 缺目标用户研究）；UI-F.1 不依赖它，也不替代其范围 |
| 外部输入 | 可运行 app container、Playwright CLI；若要显示 connection impact，则需权威 API contract | 交付前必需 / 待核对 |
| 待决策 | 是否按报告顺序领取 UI-F.1 作为下一项 UI 安全修复；不自动抢占 IT-6、UI-D 或 UI-E | 用户授权后决定 |

## 完成定义（DoD）

迭代置 `delivered` 需同时满足：

1. `tasks.md` 的 UI-F.1–UI-F.7 均为 `done`，每项有可追溯的真实执行证据。
2. 上方 7 条验收标准均为 `passed`；容器、权威 impact API 或其他外部输入缺失时必须精确标记 `blocked_external`，不得宣布 delivered。
3. [ROADMAP UI-F](../../ROADMAP.zh.md) 的逐项证据和状态已回填；与 UI-D/UI-E 的边界及任何新增后端 prerequisite 已同步。
4. [Playwright 补充实测复审](../../ui-playwright-supplemental-review-2026-09-25.zh.md) 标明哪些发现已解决、哪些仍为后续，不重写原始实测结论。
5. `git diff --check` 通过；不引入 token、真实凭据、mock route、临时截图、生成 bundle 或 UI-only 运行状态。
