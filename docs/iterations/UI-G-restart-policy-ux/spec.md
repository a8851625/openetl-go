# UI-G：restart_policy 的 UI 配置与 spec 往返保持

> 状态：`delivered`（2026-09-27：UI-G.1–UI-G.5 全部完成，最终非 skip 真实容器回归 281/281） | 依赖：无前置迭代 | 归属 roadmap 条目：[UI-G](../../ROADMAP.zh.md)

## 问题陈述

后端 `pipeline.RestartPolicy` 与 `server.go` 的 `reconcilerLoop`（指数退避、durable checkpoint 恢复）已能零丢失自动恢复 fail-stop pipeline，但该能力从产品面不可达：

1. 向导 `buildSpec`（web/src/pages/pipelines/first-task-wizard.tsx）从不生成 `restart_policy`；
2. `syncFromYaml` 解析 YAML 后不保存该字段，任何「YAML 导入 → 表单编辑 → 提交」都会静默丢弃 `restart_policy`，违反已交付的 P4 UI/YAML/API 往返保持验收；
3. DAG Designer 无该配置，虽然 orchestrator DAG spec 支持；
4. `docs/etl-config-schema.md` 未文档化该顶层字段。

线上影响：2026-09-26 复盘确认 v0.2.12-beta.20 生产同步在进程崩溃后依赖人工重启；配置了 `restart_policy` 的 spec 经 UI 再保存即丢失该保护。

## 可观察结果

1. 含 `restart_policy` 的 YAML 经向导导入、表单编辑、提交后字段逐值保留；
2. 用户能通过向导 Safety 步显式配置 restart policy（默认未配置等价 `never`，不改变现有行为）；
3. DAG Designer 与线性向导的 spec 口径一致；
4. Pipeline 详情如实显示已配置 policy 或「未配置（失败后不自动重启）」。

## 验收标准

| # | 验收标准 | 判定方式 |
| --- | --- | --- |
| 1 | 向导 YAML 导入含 `restart_policy` 的 spec → 表单编辑（改任一字段）→ 提交的 spec 字段逐值相等 | Playwright 往返断言 + API spec 比对 |
| 2 | 向导 Safety 步提供 mode/max_retries/initial_delay_ms/max_delay_ms/backoff_multiplier 配置，默认未配置等价 `never` | 表单值/确认页摘要/提交 spec 一致性断言 |
| 3 | DAG spec 含 `restart_policy` 的 validate/create 通过且 UI 配置值与 YAML 一致 | DAG e2e |
| 4 | 详情页显示 policy 两态（已配置/未配置），EN/ZH | 浏览器断言 |
| 5 | `docs/etl-config-schema.md` 补顶层 `restart_policy` 字段表与语义边界 | 文档评审 |
| 6 | 不改变 `pipeline.RestartPolicy` 结构与 reconciler 语义；at-least-once/checkpoint 契约不变 | 代码审查 + 既有回归 |

## 非目标

- 不改 reconciler 退避/max_retries 运行时语义、restart 模式集合或 checkpoint 契约；
- 不做通用未知字段透传框架（只修 `restart_policy` 这一个已知顶层字段）；
- 不新增 restart-attempt telemetry API 或 UI 侧重启状态猜测。

## 依赖与前置

| 类型 | 内容 | 状态 |
| --- | --- | --- |
| 迭代依赖 | 无 | — |
| 外部输入 | 无 | — |
| 待决策 | 无 | — |

## 完成定义（DoD）

迭代置 `delivered` 需同时满足：

1. `tasks.md` 中全部 task 状态为 `done`，且每项有实际执行的证据记录。
2. 上方验收标准全部 `passed`；`skipped` / `blocked` 项必须转为有界后续并在 ROADMAP 留痕。
3. 归属的 ROADMAP 条目验收矩阵已回填，状态已更新。
4. 相关组件文档在同一交付内更新。
5. `git diff --check` 通过；无 secret、生成产物或调试开关进入错误位面。
