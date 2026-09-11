# 迭代规划总览（SDD）

> 规划基线：`c2ac396`（v0.2.12-beta.17，2026-08-29）
>
> 唯一执行 backlog 仍是 [docs/ROADMAP.zh.md](../ROADMAP.zh.md)。本目录不复制 roadmap 条目，
> 只把**已经进入当前执行 backlog、尚未实现**的条目组织成可领取、可验证的迭代，并补充
> roadmap 条目层面没有的**技术方案**与**交付约束**。两者冲突时以 ROADMAP 的验收标准为准。
>
> ROADMAP 中标为“候选规划/待决策”的内容（当前包括 ClickHouse `CH-C1`–`CH-C8`）不属于
> 本目录的覆盖范围，也不计入“未实现条目已覆盖”的承诺。候选项只有在用户确认范围与优先级、
> 并新增或更新对应的 `spec.md`/`plan.md`/`tasks.md` 后，才可进入本目录和状态看板。

## 这是什么

本目录是 spec-driven development（SDD）产物。每个迭代一个目录，包含三件套：

| 文件 | 回答什么 | 对应 [AGENTS.md](../../AGENTS.md) 步骤 |
| --- | --- | --- |
| `spec.md` | **WHAT / WHY** —— 问题陈述、可观察结果、验收标准、交付约束、完成定义 | 步骤 1-2（sync + claim 的内容源） |
| `plan.md` | **HOW** —— 技术方案、关键接口与文件、数据语义、风险与回滚、测试策略 | 步骤 4（preflight 的输入） |
| `tasks.md` | **可领取增量** —— 带 ID 的任务表、round 分组、领取记录模板、证据落点 | 步骤 3、7、8（split / reconcile / close） |

`_template/` 是新迭代的模板。

## 迭代总览

| 迭代 | 主题 | 归属 roadmap 条目 | 依赖 | 状态 |
| --- | --- | --- | --- | --- |
| [IT-1](./IT-1-verification-substrate/) | 验证基座与存量收口 | RA-4、RA-7、BUG-1/2/6、GAP-1/3/4、P4 follow-up | 无 | `complete` |
| [IT-2](./IT-2-correctness/) | 正确性（控制面真值 + 数据面身份与顺序） | RA-1、RA-2、RA-3、GAP-7.1/.2/.3 | IT-1 | `complete`（2026-09-05；T2.1–T2.8 全部 done，16 项验收 passed） |
| [IT-3](./IT-3-integrity-capacity/) | 完整性与容量 | RA-5、RA-6、RA-8、PR-1.3 残留 | IT-1 | `active`（2026-09-09：T3.1–T3.4、T3.6-A 已交付；Round 5/5 推进 T3.6-B；CH 专项待决策） |
| [IT-4](./IT-4-ga-closeout/) | GA 收口评估 | 证据刷新、maturity 对齐、移除 beta 判定 | IT-1 + IT-2 + IT-3 | `queued` |
| [PT-A](./PT-A-maxcompute-certification/) | MaxCompute 真实环境认证（并行轨） | P0 | 外部凭据 | `blocked_external` |

### 依赖关系

```text
                    ┌──────────────────────────────────────────┐
                    │ IT-1 验证基座与存量收口                    │
                    │ 发布门禁 + e2e 自动化 + 清空 active backlog │
                    └───────────────┬──────────────────────────┘
                                    │ 解锁：其余迭代的验收可在 CI 复现
                    ┌───────────────┴───────────────┐
                    ▼                               ▼
        ┌───────────────────────┐      ┌───────────────────────┐
        │ IT-2 正确性            │      │ IT-3 完整性与容量      │
        │ 控制面真值 + 身份/顺序  │      │ secret + backup + 基线 │
        └───────────┬───────────┘      └───────────┬───────────┘
                    └───────────────┬───────────────┘
                                    ▼
                    ┌──────────────────────────────┐
                    │ IT-4 GA 收口评估              │
                    │ 证据刷新 + 移除 beta 判定      │
                    └──────────────────────────────┘

  PT-A MaxCompute 认证（blocked_external）───▶ 凭据到位后可插入任意时点，不阻塞主线
```

IT-2 与 IT-3 之间无代码依赖，文件域基本不重叠（IT-2 集中在 `server`/`sink`/`source`/`orchestrator`，
IT-3 集中在 `storage`/`backup`/CI 基准），在满足「同一时间只推进一个 `active` 主任务」的前提下
可按需调整先后。**IT-2 优先**的理由是它包含唯一会产生静默错值的 RA-1。

## Roadmap 覆盖映射

本轮迭代覆盖 roadmap 中**已进入当前执行 backlog 的全部**未实现条目。逐条对照：

| Roadmap 条目 | 当前状态 | 归属 |
| --- | --- | --- |
| P0：MaxCompute 真实环境认证 | `blocked_external` | PT-A |
| BUG-1：`mysql_batch` 字符串主键游标不推进 | `delivered`（2026-09-01 复核：容器 e2e 补全） | IT-1 |
| BUG-2：MySQL CDC binlog 断裂（ERROR 1236）无自动恢复 | `delivered`（三策略容器级闭合） | IT-1 |
| BUG-6：`snapshot_cdc` CDC 阶段不填 `ColumnTypes` | `delivered`（声明类型 e2e 闭合） | IT-1 |
| GAP-1：`postgres_cdc` Metadata 契约 | `delivered`（PG 实例 e2e 闭合） | IT-1 |
| GAP-3：`postgres` sink `pk_columns_from_metadata` | `delivered`（扇出 e2e + auto-create PK 修复） | IT-1 |
| GAP-4：`elasticsearch` mapping-conflict 策略 | `delivered`（mapping-conflict e2e + 校验语义对齐） | IT-1 |
| P4：Doris/Kafka 事实核验 follow-up | bounded follow-up | IT-1 |
| RA-4：release 流水线无测试门禁 | `delivered`（2026-09-03；skip/fail/normal tag run URL 已补齐） | IT-1 |
| RA-7：e2e 证据自动化 | `delivered`（结构化证据+commit 绑定+CI 全绿：run 33767847369；篡改拒绝 33769319891/33769344338） | IT-1 |
| RA-2：`RestoreFromDB` 静默跳过 | `delivered`（2026-09-05；IT-2/T2.1） | IT-2 |
| RA-3：`StartAll` 无视 desired state | `delivered`（2026-09-05；IT-2/T2.2） | IT-2 |
| RA-1：ClickHouse `_version` 非源事件序 | `delivered`（2026-09-05；IT-2/T2.4） | IT-2 |
| GAP-7.1：普通流完整 Key 与组合预检 | `delivered`（2026-09-05；T2.5） | IT-2 |
| GAP-7.2：受控 DLQ replay | `delivered`（2026-09-05；T2.6） | IT-2 |
| GAP-7.3：metadata-PK sink 认证 | `delivered`（2026-09-05；T2.7） | IT-2 |
| RA-5：secret 两套真值来源 | `delivered`（复核 Round 2/5） | IT-3 |
| RA-6：backup 硬截断 100000 | `delivered`（复核 Round 3/5） | IT-3 |
| PR-1.3 残留：非原子 restore、version/ID 保真、WASM artifact、Redis state | `delivered`（复核 Round 1/5） | IT-3 |
| RA-8：实测 resource baseline | `active`（复核 Round 5/5，T3.6-A 已交付） | IT-3 |
| 决策记录：RA-1 优先级 | 已按用户持续交付授权在 IT-1 后完成（2026-09-05） | IT-2 |
| schema evolution：additive-only 单独排期 | 2026-09-06 用户确认 | IT-3 |
| 待决策：ClickHouse 写入吞吐是否立项 | 未决 | IT-3（与 RA-8 合并测定） |
| 项目级发布门槛 / 移除 beta 判定 | — | IT-4 |
| ClickHouse 迭代启发候选 `CH-C1`–`CH-C8` | —（候选，待用户决策；不计入状态机） | 尚未纳入；晋级后新增/更新迭代 |

**显式不纳入本轮**：ROADMAP「有界后续」及 ClickHouse `CH-C1`–`CH-C8` 候选项（包括
S3/File first-class manifest、ODPS lookup/source 方向、Feishu 真实环境证据、JS/TS/WASM
parser 示例扩展、复杂多事实 merge）与「明确暂缓或不做」全部条目。它们保持原状态，需要
显式重新排序并补齐迭代三件套才进入执行。

## 怎么用（SDD 循环）

本目录不替代 [AGENTS.md 的 Roadmap-to-Delivery Workflow](../../AGENTS.md#roadmap-to-delivery-workflow)，
而是为它提供输入。一轮完整循环：

```text
1. 选迭代      读 IT-N/spec.md，确认可观察结果、验收标准、交付约束、依赖是否满足
2. 读方案      读 IT-N/plan.md，确认技术路线、允许触碰的文件、数据语义与回滚边界
3. 领取任务    从 IT-N/tasks.md 取当前 round 中最靠前的未完成 task，复制其领取记录模板
               同步把 ROADMAP 对应条目置 active（一次只一个）
4. 实现         AGENTS.md 步骤 4-5：preflight -> 最小垂直切片
5. 验证         AGENTS.md 步骤 6：按 task 的「验收」与「证据落点」执行，记录
               passed/failed/skipped/blocked，skip 与 blocked 不得计为 pass
6. 回填         更新 tasks.md 的任务状态与实际证据；更新 ROADMAP 条目的验收矩阵
7. 收口         一个 round 的全部 task 完成后核对 spec.md 的完成定义；
               全部 round 完成后迭代置 delivered
```

### 与 round 预算的关系

AGENTS.md 默认一次执行请求上限 **5 rounds**。每个迭代的 `tasks.md` 已按此预算把 task 分组为
`Round 1/5` .. `Round 5/5`，一个 round 是一次完整的 claim-to-close 循环。跨迭代继续需要显式
新指令，不从沉默推断授权。

### 状态取值

沿用 ROADMAP 的状态机，不新增：`active` / `blocked_external` / `queued` / `delivered` / `deferred`。
迭代级状态是其全部 task 的聚合：任一 task 未闭合则迭代不得 `delivered`。

## 全局交付约束

以下约束对所有迭代生效，迭代级 `spec.md` 只补充增量约束，不得放宽这些：

1. **语义不倒退**：默认交付语义是 checkpointed at-least-once。任何 retry、replay、checkpoint 或
   sink 改动不得把「可能重复」悄悄变成「可能丢失」。
2. **证据即事实**：`skipped`、`blocked`、「代码存在」、「脚本存在」、maturity 字符串都不等于通过。
   每项声明必须有**当前版本实际执行**的记录。
3. **验收不可中途改写**：实现中发现的相邻工作进入有界后续，不修改当前 task 的验收标准。
4. **不扩大 roadmap**：本目录不新增产品能力。发现新缺口先记入 ROADMAP，再由用户决定是否排期。
5. **优先级不静默变更**：任何提前、插队或重排都需要显式用户决策并在 ROADMAP 留痕。
6. **失败必须可见**：持久化、生命周期、checkpoint、迁移、任务状态错误必须影响 API 结果与
   overall health，不得只写日志或返回零值。
7. **maturity 保守**：证据不完整时使用 `beta` / `experimental` / `production_with_review`，
   不提前改成 `production`。

## 状态看板

在每次收口时更新此表（`—` 表示尚未开始）。

| 迭代 | Round 1 | Round 2 | Round 3 | Round 4 | Round 5+ | 迭代状态 |
| --- | --- | --- | --- | --- | --- | --- |
| IT-1 | T1.1/T1.2 完成 | T1.3/T1.4 完成 | T1.5/T1.6/T1.7/T1.8 完成 | T1.9/T1.10/T1.11 完成（Doris 外部镜像问题保留为 bounded follow-up） | T1.12 完成 | `complete` |
| IT-2 | T2.1 完成：restore failure 持久化/API/health/strict gate | T2.2 完成：desired/observed + reset fencing | T2.3 完成：identity/order 共享契约 | T2.4/T2.5/T2.6 完成 | T2.7 完成：metadata-PK 跨路径认证；T2.8 完成：16 项验收核对 + 证据重绑；迭代 `complete` | `complete` |
| IT-3 | T3.1–T3.4 复核交付；T3.6 通用基线修复中 | — | — | — | — | `active`（见复核交付记录） |
| IT-4 | — | — | — | — | — | `queued` |
| PT-A | — | — | — | — | — | `blocked_external` |
