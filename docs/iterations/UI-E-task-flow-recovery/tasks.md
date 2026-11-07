# UI-E 任务分解

> 状态取值：`todo` / `active` / `done` / `blocked`。一次只允许一个 `active`。2026-09-26 已领取 UI-E.1 进行研究前置；因缺少合格外部参与者，按本文件阻塞条件转为 `blocked`，不得用内部自测宣称完成。

## Round 分组

| Round | 包含 task | 目标 |
| --- | --- | --- |
| 1/5 | UI-E.1 | 用真实目标用户任务建立首次成功、故障恢复与移动值守基线。`blocked_external` 2026-09-26：已备好匿名协议，等待 3–5 名目标参与者。 |
| 2/5 | UI-E.2 | 冻结首次任务默认/高级信息分层，并用真实 descriptor/spec 往返验证。 |
| 3/5 | UI-E.3 | 对 issue/connection/推进事实做 API 契约 preflight；决定能否实施可靠 recovery task。 |
| 4/5 | UI-E.4、UI-E.5 | 实现有事实依据的恢复闭环、稳定 deep-link 和健康/推进表达。 |
| 5/5 | UI-E.6 | 容器故障路径、用户回访和 Roadmap 证据收口。 |

## 任务表

| ID | 任务 | 依赖 | 状态 | 证据落点 |
| --- | --- | --- | --- | --- |
| UI-E.1 | 进行五类目标用户任务走查并记录行为基线 | UI-D delivered、测试环境、3–5 名目标参与者 | `blocked`（2026-09-26 external） | `research-protocol.md`；待匿名研究汇总、决策记录、ROADMAP UI-E |
| UI-E.2 | 将研究结论转为首条管道的渐进披露最小 slice | UI-E.1 | `todo` | `first-task-wizard.tsx`、i18n、wizard Playwright、spec round-trip 证据 |
| UI-E.3 | 核对并冻结 issue 关联、connection references、推进事实的最小 API 契约 | UI-E.1 | `todo` | API/Go contract test、存储/connector 兼容矩阵、ROADMAP blocker 或 decision |
| UI-E.4 | 建立可靠 recovery task 与 DLQ/Issue/Connection 稳定 URL 上下文 | UI-E.2、UI-E.3 | `todo` | routing/main/pages、UI E2E、故障注入 E2E |
| UI-E.5 | 用真实指标分层总览的行动/推进/累计，并呈现连接删除影响 | UI-E.3 | `todo` | Dashboard/Pipelines/Connections、metrics/reference contract、任务测试 |
| UI-E.6 | 执行容器级恢复回归、用户回访并回填证据 | UI-E.2–UI-E.5 | `todo` | `hack/e2e-ui.sh`、connector E2E、研究 follow-up、ROADMAP matrix |

## 任务明细

### UI-E.1 用户任务走查基线

**验收**：

1. 招募 3–5 位目标用户：小团队中兼任数据运维的应用/后端工程师，且至少多数未使用过 OpenETL。
2. 每人完成：首条 `orders → ClickHouse`；sink timeout + 20 DLQ 修复/重放；390px 值守；`Diagnose/Locate` 文案首击；运行/健康/推进/累计解释。
3. 记录完成时间、完成/放弃、首个错误、协助次数、YAML 使用、错误 replay、误解与原话证据；移除姓名、凭据、业务数据与录屏中的敏感内容。
4. 产出明确假设清单：哪些摩擦经验证，哪些未验证/被推翻；不根据偏好问卷直接排期。

**允许范围**：测试环境与匿名研究资料；`docs/` 的聚合结论；ROADMAP UI-E。 

**非目标**：不改生产 UI，不将参与者 PII、录屏、真实配置或 credential 提交到仓库。

**阻塞条件**：没有目标用户或安全隔离测试环境时标 `blocked_external`，记录所需人数、owner、可恢复条件；不可用内部熟练贡献者替代后宣称完成。

### UI-E.2 首次任务渐进披露

**验收**：

1. 研究结论定义每个 step 的“必须回答的问题”、默认可见字段、高级入口、风险说明与退出/返回行为。
2. 默认路径完成 connection → source/sink → safety/preflight → create/start → 首批验证，不要求直接编辑 JSON/YAML。
3. 隐藏/展开高级字段在 structured draft、YAML、API submit 来回后逐字段保真；非法 YAML/JSON 仍阻断。
4. connector descriptor、introspection、preflight 是字段和提示的唯一真实来源；不静态写死 connector 语义。
5. 针对默认与专家路径新增 Playwright 断言，且 UI-D 的小屏/确认路径不回归。

**允许范围**：`web/src/pages/pipelines/first-task-wizard.tsx`、共享 config form（仅必要）、`i18n.ts`、UI E2E、研究决策文档。

**非目标**：不新增 connector、不改 pipeline spec schema、不强制用户按顺序走完所有 step。

### UI-E.3 服务端事实契约 preflight

**验收**：

1. 列出当前 `/api/v2` 与存储层能提供的 issue error/incident identity、pipeline/node/connection reference、last success/progress、checkpoint、DLQ、connection references 事实字段。
2. 对同一个 sink outage + DLQ 的 lifecycle 执行一次最小容器/fixture 验证，确认哪些字段可稳定关联、哪些仅为独立信号。
3. 若不够，提出最小 server-backed contract：字段、稳定 ID、生命周期、存储 backend 兼容、API 版本兼容、测试和 rollback；获得用户批准后才实现。
4. 若 sufficient，冻结消费规则并明确 UI 不可推断的边界。

**允许范围**：实现/测试/API docs 的只读 preflight；只有用户明确批准后才在一个新 claim 中修改后端。

**非目标**：不根据 `last_error` 文本、时间接近或 pipeline 名称在客户端聚合事故；不改变 DLQ/replay 语义。

**证据落点**：preflight matrix、API contract test/文档或准确 `blocked_external` 记录。

### UI-E.4 恢复任务与深链上下文

**验收**：

1. 仅当 UI-E.3 有可靠契约时，故障显示根因、影响、推荐下一步和当前恢复阶段；没有契约时保留多信号列表但改善动作命名。
2. `#/dlq?pipeline=…&contains=…`、`#/issues?issue=…`、`#/connections?connection=…`（或经批准的等价 typed route）在直接打开、刷新、back/forward 后还原上下文。
3. route parser 拒绝/忽略 token、secret、完整 spec 与未知敏感参数；失效 ID 显示明确状态，不回退到第一条 pipeline。
4. replay 后 result 显示 success/failed/remaining，连接到当前 recovery context，仍需用户确认与 at-least-once 风险说明。

**允许范围**：`web/src/lib/routing.ts`、`main.tsx`、Issues/DLQ/Connections/Dashboard 相关页面、i18n、UI E2E；后端仅限已批准的 UI-E.3 contract。

**非目标**：不迁移 router、不自动 replay、不改变 checkpoint 或 DLQ endpoint。

### UI-E.5 行动/推进/累计与连接影响

**验收**：

1. Dashboard/列表/详情对行动、健康、推进、累计采用同一真实指标来源和时间范围；没有 last-success 事实时显示 unavailable 而非推测。
2. 用户任务测试中，参与者能够正确区分 running、healthy/degraded、checkpoint age、CDC lag、last successful write 和 cumulative counts；记录与 UI-E.1 基线的变化。
3. 若 API 有可靠 references，Connections 显示引用摘要，删除确认显示受影响 pipeline/下一步；否则明确引用不可确认。
4. 390px 与桌面均可完成相关判断和操作，UI-D.3 的不溢出约束不回归。

**允许范围**：Dashboard/Pipelines/Details/Connections、metrics/reference API consumer、i18n、UI E2E；最小批准后端 contract。

**非目标**：不伪造趋势图、时间窗口、health 规则或 connection reference。

### UI-E.6 验证与收口

**验收**：

1. 在真实 app container 执行 UI 深链、创建、preflight、故障、DLQ dry-run/replay/recovery、connection delete impact 的关键路径。
2. 受影响的 connector/runtime E2E 覆盖 outage、restart/replay、duplicate boundary；未具备外部输入的路径准确标 `skipped`/`blocked`。
3. 与 UI-E.1 同类的至少 3 名用户回访或等效新用户验证，确认主要假设变化，不以内部自测代替。
4. Roadmap UI-E、iteration task 状态、用户旅程复审更新实际证据；`git diff --check` 通过。

**允许范围**：验证脚本、docs、受影响 UI/API 测试。

**非目标**：不借收口轮新增页面、connector 或未验证的产品能力。

## Round 1 Claim 记录（blocked_external）

```text
Round: 1/5（UI-E 窗口）
Roadmap item: UI-E / UI-E.1
Profile/path: standalone production Web UI + 用户任务研究
Objective: 以 3–5 名目标画像用户的实际行为建立首条交付、故障恢复、小屏值守、文案和指标理解基线
Scope: docs/iterations/UI-E-task-flow-recovery/research-protocol.md、tasks.md、spec.md、ROADMAP UI-E、iterations board
Non-goals: 生产 UI/API 改动；内部熟练贡献者替代用户样本；提交 PII/录屏/凭据/真实业务数据
Dependencies: UI-D delivered（已满足）；隔离环境（已有容器路径可复用）；外部参与者（缺失）
Acceptance: 3–5 名合格参与者各完成 T1–T5；匿名记录完成/耗时/首个错误/帮助/YAML/replay/指标理解；结论含反例和未证实假设
Evidence: `research-protocol.md` 已冻结招募、隔离、五任务、匿名表与解除阻塞条件；尚无参与者记录
Result: blocked_external
Missing input / owner / unblock test: 产品 owner/研究协调人需安排至少 3 位（目标 3–5）此前未使用 OpenETL 的小团队应用/后端工程师，并提供已约时间与可重置隔离环境；每人完成 T1–T5，交付去标识观察矩阵即解除 UI-E.1 阻塞
Residual/follow-up: UI-E.2/E.4/E.5 继续不可实施；UI-E.3 只读 API 契约 preflight 可在独立、明确的下一 claim 进行，但不替代用户研究
```

## 领取记录模板

```text
Round: <n>/5
Roadmap item: UI-E / UI-E.<n>
Profile/path: standalone production Web UI + <research|API contract|recovery path>
Objective: <one user-observable outcome>
Scope: <explicit UI/API/test/doc files>
Non-goals: <no UI-only incident guesses, no runtime semantic changes, etc.>
Dependencies: <UI-D status, research participants, API contract>
Acceptance: <numbered checks>
Evidence: <research matrix / tests / app-container e2e / docs>
Result: <delivered|active|blocked_external>
Residual/follow-up: <bounded next item or none>
```
