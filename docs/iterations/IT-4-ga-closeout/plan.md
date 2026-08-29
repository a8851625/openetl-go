# IT-4 技术方案

> 本文是实现输入，不是验收标准。验收以 [spec.md](./spec.md) 为准。
>
> 本迭代以**核验与判定**为主，代码改动仅限于 maturity 元数据、文档与发布配置。

## 方案总览

四个阶段，顺序不可调换：

```text
1. 证据全量复核   —— 先知道现在的真实状态，再谈判定
2. maturity 对齐  —— 不一致者降级，使公开声明与证据一致
3. 升级 drill     —— 补上唯一未验证的生产动作（跨版本升级与回滚）
4. 显式判定       —— 按发布声明分级分别给出结论
```

阶段 1 必须先于阶段 2：在不知道真实证据状态的情况下调整 maturity 会产生新的漂移。
阶段 3 独立于 1/2，但必须先于阶段 4，因为升级失败会直接否决 GA。

## 分项设计

### A. 证据全量复核

**目标**：产出一张「声明 → 证据 → 新鲜度」对照表，覆盖所有对外 maturity 声明。

**数据来源**：

| 来源 | 内容 |
| --- | --- |
| `docs/evidence/*.json`（IT-1 产出） | 各 path 的实际运行结果与 commit 绑定 |
| `internal/etl/server/path_contract*.go` | `PathContract.LastCertified` |
| `docs/connector-certification.md` | connector maturity 字符串 |
| `docs/path-contract.md` | path 级契约与 write mode |
| `docs/reliability-certification.md` | 可靠性矩阵 |
| `README(.zh).md`、`docs/positioning(.zh).md` | 对外能力声明 |

**核验规则**：

- 证据的 `commit` 必须是当前 HEAD 或其祖先，且不早于该 path 相关源码的最近改动。
- `result != "passed"` 的 path 不得声明为 production。
- `skipped` / `blocked` 的 check 必须在声明中体现为边界或降级。

**工具化**：优先扩展 `hack/check-connector-evidence.sh` 输出该对照表，而不是人工整理 —— 
人工整理正是审计发现漂移的原因。

### B. maturity 对齐

**原则**：**降级优先于解释**（交付约束 4）。

| 情形 | 动作 |
| --- | --- |
| 声明 production，证据 passed 且新鲜 | 保持 |
| 声明 production，证据过期或缺失 | 降级为 `beta` 或 `production_with_review`，记录原因 |
| 声明 production，证据含 skipped/blocked check | 降级或在声明中显式标注该边界 |
| 声明 beta/experimental，证据充分 | **不自动升级**；升级需单独判定并有用户确认 |

**关键文件**：`docs/connector-certification.md`、`docs/path-contract.md`、
`internal/etl/server/` 的 descriptor maturity 字段、`README(.zh).md`、`docs/positioning(.zh).md`。

### C. 升级与回滚 drill

**现状**：从上一稳定版本升级到当前版本的真实 drill 在三 backend 上尚未完成
（审计「最小 GA 前置顺序」第 7 条）。

**方案**：

```text
对每个 backend ∈ {SQLite, MySQL, PostgreSQL}:
  1. 用上一稳定版本镜像启动，创建代表性数据集
     （线性 + DAG pipeline、加密 spec、connection、DLQ 记录、audit、run history、checkpoint）
  2. 停机 -> 备份 -> 升级到当前版本 -> 启动
  3. 断言：spec 可读、checkpoint 续跑、DLQ 可查可 replay、desired state 保持、secret 可解密
  4. 回滚到上一稳定版本，断言数据未被单向破坏
  5. 记录耗时（升级窗口 / 回滚窗口），供 RPO/RTO 声明引用
```

**复用**：优先在 IT-1 的 e2e 框架内表达，使其可重复执行；无法自动化的步骤显式记录为手工。

**注意**：步骤 4 的回滚断言是关键 —— 若迁移不可逆（如 IT-2 的 desired/observed 拆列），
必须在此暴露并写入发布说明。

### D. 显式判定

**产出**：`docs/ga-assessment-<date>.md`，结构固定：

```text
## 判定结论
  项目级              : 可 / 不可移除 beta —— 理由
  standalone          : 可 / 不可 —— 理由 + RTO/RPO 声明
  distributed         : 可 / 不可 —— 理由（默认保持 beta，除非 PR-D1 有当前版本证据）
  各 connector/path   : 逐条 maturity 与边界

## 五条项目级门槛核验
  | 门槛 | 结果 | 证据 | 缺口 |

## 残余边界（进入发布说明）
  at-least-once / 可能重复 / RPO / RTO / 单点 / 非原子 fanout / 未认证 connector

## 若不可移除：剩余阻断项
  | 阻断项 | 影响的声明形态 | 建议归属迭代 |
```

模板必须能表达否定结论（交付约束 5）。

## 架构约束

- 本迭代**不做功能实现**。代码改动限于 maturity 元数据、文档、发布配置与 drill 脚本。
- 不新增运行时依赖。
- 发现的缺口写入 ROADMAP，不就地修复。

## 风险与回滚

| 风险 | 影响 | 缓解 | 回滚路径 |
| --- | --- | --- | --- |
| 复核发现大量声明需降级 | 对外能力表述显著收缩 | 这是正确结果；降级是发现问题的证据而非失败 | 不回滚；补足证据后单独判定升级 |
| 升级 drill 暴露不可逆迁移 | GA 被阻断 | 提前在 IT-2/IT-3 的迁移设计中要求可逆性 | 提供降级脚本或声明为单向升级并写入发布说明 |
| 判定退化为走过场 | GA 声明失去可信度 | 判定文档强制包含否定结论的表达位；每条门槛必须有证据链接 | 重新执行核验 |
| 评估中就地修复缺口 | 评估失去独立性 | 交付约束 3；发现即记录，不动手 | 剥离修复 commit 到对应迭代 |

## 测试策略

| 层级 | 范围 | 命令 |
| --- | --- | --- |
| 1 静态与单测 | maturity 元数据一致性 | `go test ./internal/etl/server -run Certification` |
| 2 包级 | 不适用 | — |
| 3 后端矩阵 | 三 backend migration + backup/restore conformance | 既有 conformance 套件 |
| 4 容器 e2e | 两条主推荐链路全矩阵；三 backend 升级与回滚 drill | IT-1 框架 + 新增 upgrade drill |
| 5 外部环境认证 | 不适用（MaxCompute 属 PT-A） | — |
