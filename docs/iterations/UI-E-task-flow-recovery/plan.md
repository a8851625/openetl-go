# UI-E 技术与产品验证方案

> 本文是 discovery + 实现前置输入，不是验收标准。验收以 [spec.md](./spec.md) 与 [ROADMAP UI-E](../../ROADMAP.zh.md) 为准。状态：`blocked_external`（2026-09-26，等待 UI-E.1 的 3–5 名目标用户走查；协议见 `research-protocol.md`）。

## 方案总览

UI-E 不从页面清单或审美重构出发，而从两个端到端结果出发：

1. **首条已验证交付**：选择连接和数据范围 → 明确写入语义/风险 → preflight → 创建启动 → 验证首批数据与 checkpoint；
2. **安全恢复**：发现异常 → 理解根因和影响 → 修复/验证 → dry-run → replay → 核对 remaining backlog 与推进。

先运行低样本定性任务走查，建立基线；再核对服务端是否能支撑可靠的 issue/connection 关联和推进事实；最后只实现能改善已验证瓶颈的最小 vertical slice。不能以 UI-only 分组、模拟 timeline 或错误字符串相似性取代事实契约。

## 分项设计

### UI-E.1：首次任务研究与渐进披露

**现状**：`FirstTaskWizard` 已有场景、连接、source/sink、transform、安全和确认 step，也有 draft、YAML/表单一致性与 preflight。但首层仍暴露较多低频配置和专家术语。

**研究设计**：

- 参与者：3–5 位能够描述一个业务同步需求、但未使用过 OpenETL 的应用/后端工程师；不把 OpenETL 贡献者作为唯一样本。
- 任务：把 `orders` 从测试 MySQL 同步至 ClickHouse，选择适合的写入模式，完成 preflight，并证明首批数据已写入。
- 记录：完成/放弃、分钟数、首个误点、请求帮助次数、是否打开/编辑 YAML、何时能说出重放边界、屏幕/行为笔记。
- 后访谈：只问已发生的行为与证据，例如“刚才你为什么点这里”，不以“你会不会喜欢”引导。

**目标实现方向（由研究确认后选择）**：

- 每 step 只回答一个问题：读什么、写到哪里/怎样写、是否需要处理、是否安全、怎样验证；
- 将 descriptor 的低频 runtime 参数、原始 YAML、样例编辑等置入可发现的高级区域；
- 复用现有 structured draft / YAML override，任何隐藏字段仍原样随 UI/API/YAML 往返；
- 确认页增加基于真实数据的“首批验证计划”，而不是只堆配置摘要。

**关键接口与文件（预测，研究后冻结）**：

| 文件 | 改动性质 |
| --- | --- |
| `web/src/pages/pipelines/first-task-wizard.tsx` | 信息层级、默认/高级分组与首批验证摘要；不引入新的 spec state。 |
| `web/src/components/shared/config-form.tsx` | 必要时支持 descriptor 驱动的 progressive disclosure。 |
| `web/src/i18n.ts` | 结果导向文本与风险说明。 |
| `hack/e2e-ui.sh` | 默认路径与隐藏字段往返回归。 |
| `docs/` 研究记录（匿名汇总） | 任务基线、结论、决策日志。 |

**设计权衡**：不强制线性化所有专家用户；高级能力仍可访问。选择“默认收起”而不是删除，前提是隐藏字段安全保真且有明确入口。

### UI-E.2：从技术 Issue 到恢复任务

**现状**：`deriveIssues` 直接从 pipeline stats/metrics 独立发出 `failed`、`dlq`、`rec-failed`、`lag`、`checkpoint`、`last error`。前端无法可靠判断它们是否同一根因。

**前置契约核对**：

- 核查 API 是否已有/可最小新增：稳定 error/incident ID、发生窗口、关联 pipeline/node/connection、当前影响指标、推荐 remediation category、验证状态；
- 对一个真实 sink outage + DLQ 场景，确认状态迁移：检测 → 归类 → 修复证据 → dry-run → replay → resolved/remaining；
- 若 server-side 不能建立可靠关系，只改动作命名和上下文连续性，不合并列表项。

**目标实现方向（以后端证据为前提）**：一个 recovery card 显示根因、受影响记录/DLQ、推进风险、下一步和详情；将 `Locate` 降为辅助操作，主操作用结果命名，如“查看失败原因”“测试连接”“验证修复”。

**关键接口与文件（预测）**：

| 文件 | 改动性质 |
| --- | --- |
| `internal/etl/...` / API v2（仅契约确实缺失时） | 最小关联/引用事实字段与测试；不产生 UI-only incident state。 |
| `web/src/lib/pipeline-health.ts` | 消费服务端分类；不再从文本猜测根因。 |
| `web/src/pages/IssuesPage.tsx`、`DashboardPage.tsx` | recovery task 表达、上下文导航。 |
| `web/src/pages/DLQPage.tsx` | recovery 验证、replay 后结果链路。 |
| 故障注入 E2E | sink outage、DLQ、修复、replay、recovery。 |

**数据语义**：保持 source position、checkpoint、sink acknowledgement 和 DLQ/replay 语义不变。UI 只能呈现 server 已知的关联；恢复不等于自动清空 backlog，结果须显示 replayed/failed/remaining。

### UI-E.3：可分享修复上下文

**现状**：hash router 的 DLQ route 为 `#/dlq`，`openDLQ(key)` 仅设置 in-memory `selectedPipeline`；Issue 页用 `history.replaceState` 追加 `?issue=`，但 route parser 不消费该 query。刷新/深链难以恢复选择和高亮。

**目标**：采用最小、非敏感 URL 参数，例如：

```text
#/dlq?pipeline=<stable-id>&contains=<filter>
#/issues?issue=<stable-issue-id>
#/connections?connection=<name>
```

在 `parseHash` / `routeToHash` 中建立 typed route 语义，而不是让页面各自解析字符串。调用者接受 identity 和可分享 filter；页面优先从 route 恢复 state，用户交互再同步 URL。token、secret、完整 YAML、错误 payload、预签名 URL 一律不放入 hash。

**关键接口与文件（预测）**：

| 文件 | 改动性质 |
| --- | --- |
| `web/src/lib/routing.ts` | 扩展 typed AppRoute、parse/serialize URL 参数与参数白名单。 |
| `web/src/main.tsx` | route → selected context 同步；移除仅内存的跨页面选择依赖。 |
| `web/src/pages/IssuesPage.tsx` | 读取/高亮 stable issue ID，保留返回上下文。 |
| `web/src/pages/DLQPage.tsx` | 从 route 读取并写回 pipeline/filter，处理失效 ID 的明确 empty state。 |
| `web/src/ConnectionsPage.tsx` | 从 route 恢复 connection 选择（仅有稳定 identity 时）。 |
| `hack/e2e-ui.sh` | direct-link / reload / back-forward / sensitive-param rejection。 |

**设计权衡**：不引入 React Router；现有 hash router 已足够，只需把隐式 state 显式化、typed 化。若 ID 无效，显示可解释的 not-found/empty，而非 silently choosing the first pipeline。

### UI-E.4：行动、推进、健康、累计的口径

**现状**：Dashboard 已把 cumulative metrics 放在次级区域；`derivePipelineHealth` 由 status、failed/DLQ/last_error、CDC lag、checkpoint age 共同派生。但最近成功写入/推进证据是否能由当前 API 稳定表达须核对。

**目标**：

- 总览第一层：现在要处理的 recovery tasks；
- 第二层：是否持续推进（last successful write/heartbeat、checkpoint age、CDC lag，附时间范围）；
- 第三层：累计处理量（明确 all-time/current backlog）；
- 详情：为什么判定 degraded/failed，逐项链接至事实指标；
- 列表：最少的 path、状态、模式、推进风险和最近错误，不将累计 records 当 health 代理。

**前置**：确认 metrics API 每种 connector 是否存在可比的 `last_successful_write_at` 或等价事实。缺失时记录最小后端数据契约；不得用客户端刷新时间或累计 records 伪造。

### UI-E.5：Connection 健康与引用影响

**现状**：Connections 显示 `last_status`、错误、last tested 和 Load/Test/Delete，但没有稳定的引用关系信息。

**目标**：若服务端可从 persisted spec/connection catalog 返回 references，则在连接行/详情和删除确认中呈现受影响管道、引用种类、删除后下一步；无此数据时明确“无法确认引用关系”，不提供虚假的安全结论。

**数据语义**：删除阻断/允许策略需由后端权威决定；UI 只传达真实 impact。无论是否有引用，删除依旧走 ConfirmDialog 和可审计 API。

## 架构约束

- 复用 current React/hash routing、pipeline spec、descriptor、introspection、preflight 与 API v2。
- 不为 UI-E 创建静态任务/incident/replay simulator，不依赖 client-side error-string matching。
- 不改变 at-least-once、checkpoint checkpoint、sink acknowledgement、DLQ retention/replay endpoint 语义。
- URL 的 identity 必须是稳定、编码、安全的 server identifier；不要把显示名称当唯一安全 ID。

## 风险与回滚

| 风险 | 影响 | 缓解 | 回滚路径 |
| --- | --- | --- | --- |
| 低样本研究被个别熟练用户偏置 | 错误改造默认流程 | 招募目标画像、记录反例、与行为日志交叉验证 | 不进入实现；仅保留研究结论和下一轮假设 |
| incident 聚合错误关联 | 用户修错对象/误 replay | 仅消费 server-backed ID/类别；无契约则不合并 | revert recovery card，保留原始 issue 列表 |
| URL 参数造成 state 泄露或坏链接 | 安全/支持成本 | typed allow-list、encode/decode、敏感参数拒绝、invalid state 明示 | revert new route serialization；已保存页仍打开 base route |
| 渐进披露误覆盖隐藏字段 | 配置静默损失 | 沿用 structured draft/YAML tests，确认提交 spec diff | revert wizard display-only commit；不迁移 storage |
| connection references 过期 | 错误删除提示 | server 返回 revision/time，删除 API 终审 | 保留通用 warning；不假称影响已核对 |

## 测试策略

| 层级 | 范围 | 命令 / 证据 |
| --- | --- | --- |
| 0 研究 | 首次任务/故障/移动/文案/指标理解 | 匿名任务观察表、录屏或笔记（不入仓库 PII） |
| 1 静态 | route typing、UI、i18n | `npm run typecheck && npm run lint && npm run build`（`web/`） |
| 2 浏览器 | 深链、wizard、issues/DLQ、390px、a11y | 扩展 `hack/e2e-ui.sh` 与 Playwright 定向断言 |
| 3 API/后端 | incident/reference/metrics 真相 | 对应 Go 单测、API contract test、存储 backend 覆盖 |
| 4 故障路径 | sink outage → DLQ → fix → dry-run → replay → recovery | 受影响 connector 容器 E2E；记录 duplicate/replay 边界 |
| 5 回归 | standalone 生产 UI | `CONTAINER_CLI=<docker|podman> bash hack/e2e-ui.sh` + `git diff --check` |
