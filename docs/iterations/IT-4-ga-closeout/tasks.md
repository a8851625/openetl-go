# IT-4 任务分解

> 状态取值：`todo` / `active` / `done` / `blocked`。一次只允许一个 `active`。
>
> **阶段顺序不可调换**：T4.1 → T4.2；T4.3 独立但须早于 T4.4。

## Round 分组

| Round | 包含 task | 目标 | 可独立发版 |
| --- | --- | --- | --- |
| 1/5 | T4.1 | 知道真实证据状态（声明→证据→新鲜度对照表） | 是 |
| 2/5 | T4.2 | 公开 maturity 与证据一致（不一致者降级） | 是 |
| 3/5 | T4.3 | 三 backend 升级与回滚 drill 完成 | 是 |
| 4/5 | T4.4 | 两条主推荐链路当前版本全矩阵认证 | 是 |
| 5/5 | T4.5 | 显式 GA 判定 + 发布说明 + 迭代收口 | 是 |

## 任务表

| ID | 任务 | 依赖 | 状态 | 证据落点 |
| --- | --- | --- | --- | --- |
| T4.1 | 证据全量复核与对照表 | IT-1/T1.5 | `todo` | `check-connector-evidence.sh` 输出 |
| T4.2 | maturity 对齐（降级优先） | T4.1 | `todo` | connector-certification / path-contract / README / positioning |
| T4.3 | 三 backend 升级与回滚 drill | IT-1、IT-3/T3.4 | `todo` | upgrade drill 脚本与耗时记录 |
| T4.4 | 两条主推荐链路全矩阵认证 | IT-2 | `todo` | 证据 JSON |
| T4.5 | GA 判定 + 发布说明 + 收口 | T4.1..T4.4 | `todo` | `docs/ga-assessment-<date>.md` |

## 任务明细

### T4.1 证据全量复核与对照表

**验收**（spec 验收 5、6 的前置）：

1. 产出「声明 → 证据 → 新鲜度」对照表，覆盖 `connector-certification.md`、
   `path-contract.md`、`reliability-certification.md`、`README(.zh).md`、
   `positioning(.zh).md` 中全部 maturity 声明。
2. 核验规则生效：证据 `commit` 是 HEAD 或其祖先且不早于相关源码最近改动；
   `result != "passed"` 的 path 标记为不得声明 production；`skipped`/`blocked` 的 check
   标记为需体现为边界。
3. 所有 `PathContract.LastCertified` 无空值、无过期。
4. 对照表由 `hack/check-connector-evidence.sh` **工具化产出**，不是人工整理
   （人工整理正是历史漂移的原因）。

**证据落点**：`hack/check-connector-evidence.sh` 输出归档。

### T4.2 maturity 对齐

**验收**（spec 验收 6）：

1. 按「降级优先于解释」处理每条不一致：证据过期/缺失 → 降级；含 skipped/blocked →
   降级或显式标注边界。
2. 声明 beta/experimental 但证据充分者**不自动升级**；升级需单独判定并有用户确认。
3. 四类文档与 descriptor maturity 字段同步更新。
4. 更新后重跑 T4.1 的对照表，无剩余不一致。

**证据落点**：各文档 diff；descriptor maturity 字段 diff；复核后的对照表。

### T4.3 三 backend 升级与回滚 drill

**验收**（spec 验收 3、4）：

1. 对 SQLite / MySQL / PostgreSQL 各执行一次：上一稳定版本创建代表性数据集
   （线性 + DAG pipeline、加密 spec、connection、DLQ、audit、run history、checkpoint）
   → 备份 → 升级 → 启动。
2. 断言：spec 可读、checkpoint 续跑、DLQ 可查可 replay、desired state 保持、secret 可解密。
3. 回滚到上一稳定版本，断言数据未被单向破坏；**若迁移不可逆则在此暴露**并写入发布说明。
4. 记录升级窗口与回滚窗口耗时，供 RPO/RTO 声明引用。
5. 无「用 SQLite 通过替代其他 backend」的情况。

**证据落点**：upgrade drill 脚本、三次执行记录、耗时数据。

### T4.4 两条主推荐链路全矩阵认证

**验收**（spec 验收 2）：

1. MySQL CDC → MySQL upsert：crash / checkpoint reset / 依赖中断 / DLQ replay /
   重复吸收，在**当前 commit** 通过。
2. MySQL snapshot+CDC → ClickHouse：同上，并含 IT-2 交付的乱序 replay 矩阵。
3. 两条链路的证据 JSON 绑定当前 commit，`LastCertified` 同步刷新。

**证据落点**：证据 JSON；CI run URL。

### T4.5 GA 判定 + 发布说明 + 收口

**验收**（spec 验收 1、7、8、9、10）：

1. 五条项目级发布门槛逐条核验，每条给出 pass/fail + 证据 + 缺口。
2. `hack/check-release-assets.sh` 通过：无空 token、`change-me`、浮动 `latest`。
3. 发布说明列出全部残余边界：at-least-once、可能重复、RPO/RTO、单点、
   非原子 fanout、未认证 connector。
4. 按发布声明分级**分别**判定：项目级 / standalone / distributed / connector-path。
   distributed 默认保持 beta，除非 PR-D1 有当前版本证据。
5. 产出 `docs/ga-assessment-<date>.md`，**含否定结论的表达位**；若判定为不可移除 beta，
   则剩余阻断项作为新条目写入 ROADMAP 并标注建议归属迭代。
6. `docs/iterations/README.md` 状态看板更新。

**证据落点**：`docs/ga-assessment-<date>.md`；ROADMAP diff；`release-checklist.md`。

## 领取记录模板

```text
Round: <n>/5
Roadmap item: <项目级发布门槛 | 证据治理> (IT-4/T4.<n>)
Profile/path: <project | standalone | distributed | connector path>
Objective: <one observable outcome>
Scope: <files/components allowed>
Non-goals: <explicit exclusions —— 本迭代不实现功能、不就地修复缺口>
Acceptance: <numbered checks，直接引用本文件对应 task 的验收>
Evidence: <commands, run URL, drill 记录, docs>
Result: <delivered|active|blocked_external>
Residual/follow-up: <bounded next item or none>
```
