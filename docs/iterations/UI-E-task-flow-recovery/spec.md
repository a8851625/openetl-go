# UI-E：首次成功与故障恢复的任务型产品闭环

> 状态：`blocked_external`（2026-09-26：UI-E.1 缺 3–5 名目标用户任务走查；匿名协议见 [research-protocol.md](./research-protocol.md)） | 依赖：UI-D 已交付；部分实现仍依赖后端关联/引用契约 | 归属 roadmap 条目：[UI-E](../../ROADMAP.zh.md)

## 问题陈述

OpenETL-Go 已具备 Source → Transform → Sink 向导、连接、preflight、Issues、DLQ 与 replay，但用户面对的是多个工程对象和技术信号，而非连续的任务结果。

2026-09-24 的产品/交互复审结合 Playwright 实操与实现核对，确认以下问题：

1. **首次成功的认知负担偏高**：六步向导的能力强，但模板、连接、descriptor、运行参数、transform、样例、YAML 与 preflight 的呈现仍会让新用户难以判断当前必做项。默认路径的核心价值应该是“建一条安全且已验证的交付链路”，而不是展示全部配置能力。
2. **一个故障可被拆为多条技术信号**：`deriveIssues` 会独立生成 DLQ、failed records、lag/checkpoint 与 last error 等项；同一个 sink outage 可能让用户先后处理多个表象。前端不能仅凭错误文本猜测根因，需要可靠关联契约。
3. **修复上下文不可稳定分享或恢复**：`openDLQ(key)` 只把 key 存在组件 state 并导航到 `#/dlq`；hash router 的 DLQ route 没有 pipeline/issue/filter 字段。因此刷新、直接打开链接或前进/后退不能保证恢复同一条修复对象。
4. **观察口径仍会让用户混淆**：总览已经将累计指标放到次级区，但用户仍需在 `running`、health、最近成功推进、checkpoint age、DLQ 与累计 records 之间自行推断“是否正在交付”。
5. **连接管理缺少业务影响上下文**：当前 Connections 主要显示类型、测试状态、时间及操作；若删除一个连接，操作者需要知道受影响管道与安全下一步，但 UI 不应自行推测引用关系。

## 可观察结果

1. 目标用户可以沿默认路径完成并验证第一条安全管道，不需要编辑 YAML 或理解低频工程对象。
2. 一次常见 sink 故障呈现为一个可解释的恢复任务：根因/影响/下一步，以及修复到 replay 再到恢复核对的连续状态。
3. 用户可用稳定 URL 分享并在刷新后恢复管道、Issue、DLQ filter 与修复上下文，不暴露 secret。
4. 用户可正确回答“该管道是否在推进、如何知道、现在该处理什么”；总览、列表、详情的健康/推进/累计口径一致。
5. 用户在删除连接前能理解被影响的管道；无法建立可靠引用时，系统明确告知而不是假装安全。

## 验收标准

| # | 验收标准 | 判定方式 |
| --- | --- | --- |
| 1 | 对 3–5 名目标画像用户完成首次任务、故障修复、小屏值守、文案理解、指标口径五个任务走查；记录原始完成时间、错误、辅助需求和任务后访谈。 | 可复核的匿名研究记录与观察矩阵；不以满意度替代实际任务行为。 |
| 2 | 以用户走查为准确定义向导默认/高级信息分层；默认成功路径无需 YAML，隐藏字段不会在 YAML/UI/API 往返中丢失。 | 设计决策记录 + Playwright create/preflight/submit 回归；真实 connector descriptor 驱动。 |
| 3 | 若后端能提供可靠关联 ID/错误类别，则一个 sink outage + DLQ 场景以一个 recovery task 展示根因、影响、下一步和修复状态；若不能，明确记录后端 blocker 而非 UI 猜测聚合。 | API 契约测试 + 注入故障 E2E + 用户走查；从 Issue 到 remediation/人工升级路径用时中位 ≤10 分钟或记录 blocker。 |
| 4 | DLQ/Issue/Connection 的有效上下文通过稳定 hash URL 表达；刷新、直接链接、前进/后退后恢复同一对象、tab/filter，且 URL 不含 token/secret。 | Playwright 深链回归、路由 unit/浏览器测试、安全审查。 |
| 5 | Dashboard、列表、详情可清楚区分当前行动、持续推进证据、健康和历史累计；用户任务测试中的状态判断正确率相对 UI-E 前基线提高，并保持真实时间口径。 | 指标理解测试 + `/metrics`/API 契约对照 + UI 自动化。 |
| 6 | 被引用的 connection 删除前显示影响的 pipeline 与下一步；后端无引用数据时，删除流程明确告知“引用关系不可确认”。 | connection reference API/前端 E2E + 删除确认路径；无 UI 猜测。 |
| 7 | UI-D 已通过其容器级真实 API 验收；UI-E 改动通过 typecheck、lint、build、受影响 UI E2E 和相关 runtime/connector E2E。 | Roadmap 证据矩阵、命令日志、`git diff --check`。 |

## 非目标

- 不把 OpenETL-Go 变成 Airflow/Dagster 工作流系统、Flink/Spark 流计算平台或 Airbyte 式 connector marketplace。
- 不引入第二份 UI-only pipeline/incident 状态模型，或依据错误字符串进行不可靠的根因推断。
- 不迁移到新路由框架，也不将 secret、完整 spec、凭据或 token 写入 URL。
- 不用 AI 自动修改 spec、自动恢复或自动 replay 代替可解释的 preflight/用户确认。
- 不在用户研究前进行全局视觉重做、复杂仪表盘或大规模导航重构。

## 交付约束

- UI-E.1 的研究观察先于产品结构实现；结论必须包含反例和未证实假设。
- UI-E.2 / UI-E.5 的任何跨对象关系必须来自服务端或已证实的 spec reference，不能由前端字符串匹配杜撰。
- 恢复路径保持 at-least-once：每个 replay 操作继续说明可能重复、sink 幂等模式、范围与结果。
- URL 只承载可分享的对象 identity、tab、filter 和 issue identity；敏感参数必须拒绝/剥离。
- 若相关后端契约缺失，task 标为 `blocked` 并把最小 API contract 记录到 ROADMAP；不得把静态 mock 当作闭环证据。

## 依赖与前置

| 类型 | 内容 | 状态 |
| --- | --- | --- |
| 迭代依赖 | UI-D.1–UI-D.6 交付（确认可信度、小屏基线、真实 app UI E2E） | `delivered`（2026-09-26，209/209） |
| 研究输入 | 3–5 名符合“小团队兼任数据运维的应用工程师”画像的参与者及隔离测试环境 | `blocked_external`：参与者待产品 owner/研究协调人安排；协议与解除条件见 `research-protocol.md` |
| 后端契约 | Issue/错误关联 ID、connection 引用关系、当前推进/最近成功写入字段的可用性核对 | 待独立只读 preflight；不替代研究输入 |
| 待决策 | 若研究显示当前 6 步结构不适配，是否允许调整步骤/信息架构 | 仅在研究结论后由用户决定 |

## 完成定义（DoD）

迭代置 `delivered` 需同时满足：

1. `tasks.md` 中 UI-E.1–UI-E.6 全部 `done`，研究证据、API 契约和实现证据可追溯。
2. 上方 7 条验收标准全部 `passed`；外部参与者或后端契约缺失须精确 `blocked_external`，不能宣布 delivered。
3. [ROADMAP UI-E](../../ROADMAP.zh.md) 更新每项证据、结论和残留；任何后端 prerequisite 另立有界任务。
4. 用户旅程复审文档同步实际验证结果，清楚区分“已验证”与“假设”。
5. `git diff --check` 通过；无研究参与者 PII、secret、模拟事故数据或临时 demo 控制进入仓库。
