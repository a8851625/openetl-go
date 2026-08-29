# PT-A 技术方案

> 本文是实现输入，不是验收标准。验收以 [spec.md](./spec.md) 为准。

## 方案总览

本轨**不新增实现**。writer、validator、preflight、错误分类、retry/backoff、metrics 均已存在。
技术工作集中在三件事：

1. 把 `hack/e2e-maxcompute.sh` 的环境门控路径实际跑通并固化为可重复流程；
2. 在受控测试资源上执行故障注入（权限、schema/partition 不匹配、暂时不可用）；
3. 产出与其他 connector 同构的证据记录。

## 分项设计

### A. 环境准备与凭据注入

**现有入口**：`hack/e2e-maxcompute.sh`（环境门控，缺变量时 skip）、
`docs/components/sink-maxcompute.md`。

**凭据注入**：全部通过环境变量，不落盘、不入仓库。CI 中若执行则使用仓库 secrets；
本地执行使用 shell 环境。证据文件中的 endpoint / project / table 需脱敏。

**测试资源要求**：

| 资源 | 用途 |
| --- | --- |
| 可写分区表 | 主链路与动态/静态分区验证 |
| 无写权限的表或账号 | 权限失败分类验证 |
| schema 不匹配的表 | 远端 preflight 验证 |
| 独立测试 project（推荐） | 避免在共享生产项目执行故障注入 |

### B. 认证链路

```text
Kafka ODS JSON
  -> project / type_convert
  -> MaxCompute 分区表（动态分区 + 静态分区两种配置各跑一次）
```

**故障注入序列**：

| 场景 | 期望行为 |
| --- | --- |
| 权限不足 | 错误被分类为权限类，不进入无限重试，信息可操作 |
| 远端 schema / partition 不匹配 | preflight 阶段拒绝，不在写入中途失败 |
| sink 暂时不可用 | 进入 DLQ；修复后 replay 写回成功 |
| 应用 restart | 按 checkpoint 恢复 |
| checkpoint reset / replay | 记录 append 模式下的重复边界 |

**数据语义**：MaxCompute append 模式无法天然吸收重放，重复边界必须显式记录而不是靠实现兜底。
若目标表有业务主键与去重策略，需在 path contract 中声明；否则声明为「append，重放会产生重复」。

### C. 证据产出

与其他 connector 同构：若 PT-A 在 IT-1 之后执行，则复用 IT-1 的结构化证据格式
（`docs/evidence/<path_id>.json`）；若在 IT-1 之前执行，则按现行 markdown 证据格式记录，
并在 IT-1 完成后迁移。

**必须记录**：实际命令、SDK / 依赖版本、执行日期、每项 passed/failed/skipped/blocker、
脱敏后的资源标识。

## 架构约束

- 不新增 writer 实现；发现的实现缺陷记为独立条目，不在本轨顺带重构。
- 不做 lookup / source 方向。
- 凭据不进入仓库、日志与证据文件。

## 风险与回滚

| 风险 | 影响 | 缓解 | 回滚路径 |
| --- | --- | --- | --- |
| 凭据长期不可得 | 本轨持续 `blocked_external` | 不阻塞主线；主线迭代独立推进 | 无需回滚 |
| 只有共享生产项目可用 | 故障注入有副作用风险 | 要求独立测试 project；不可得则跳过故障注入项并记为 `blocked`，不记 pass | 缩减认证范围并明确剩余边界 |
| 认证暴露实现缺陷 | 认证无法完成 | 缺陷记为独立 roadmap 条目并排期 | maturity 保持 `experimental` |
| quota 限制导致大批量写入失败 | 吞吐类断言无法执行 | 认证范围以正确性为主，吞吐单独声明 | 记录 quota 边界 |

## 测试策略

| 层级 | 范围 | 命令 |
| --- | --- | --- |
| 1 静态与单测 | 既有 sink 单测回归 | `go test ./internal/etl/sink -run MaxCompute` |
| 2 包级 | 既有 preflight / 错误分类测试 | 同上 |
| 3 后端矩阵 | 不适用（单一远端服务） | — |
| 4 容器 e2e | Kafka 侧本地容器 + 远端 MaxCompute | `hack/e2e-maxcompute.sh` |
| 5 外部环境认证 | **本轨的主体** | 同上，带完整环境变量 |
