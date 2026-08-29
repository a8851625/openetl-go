# PT-A：MaxCompute 真实环境认证（并行轨）

> 状态：`blocked_external` | 依赖：外部凭据 | 归属 roadmap 条目：P0

## 为什么是并行轨而不是主线迭代

P0 是 ROADMAP 当前的**最高优先级**，本文档不改变这一点。它被组织为并行轨的原因是它
`blocked_external` —— 缺的不是实现，是外部凭据。按 [AGENTS.md 步骤 2](../../../AGENTS.md)：
「`blocked_external` 项在其凭据/服务/授权输入可得前不具备领取资格」。

因此：**凭据到位即可插入任意时点执行，不阻塞 IT-1..IT-4；同时 IT-1..IT-4 的推进
也不构成对 P0 优先级的降级**。这与 ROADMAP「当外部凭据继续不可得且需要切换主任务时」
的既有约定一致。

## 问题陈述

MaxCompute / ODPS sink 的实现已经存在：SDK batch writer、partition/schema validator、
远端 preflight、错误分类、retry/backoff、metrics 和环境门控 e2e 脚本
（`hack/e2e-maxcompute.sh`、`docs/components/sink-maxcompute.md`）。

当前缺口**不是继续实现 writer**，而是真实 MaxCompute 环境中的认证证据。在该证据完成前，
`maxcompute` / `odps` 的 maturity 必须保持 `experimental`。

## 可观察结果

1. Kafka ODS JSON → `project` / `type_convert` → MaxCompute 分区表的完整链路在真实环境跑通。
2. 权限失败、schema/partition 不匹配等错误被正确分类并给出可操作信息。
3. sink 暂时失败进入 DLQ、修复后可 replay 写回。
4. 应用 restart 与 checkpoint reset/replay 的重复边界被明确记录。

## 验收标准

沿用 ROADMAP P0 的原验收标准，不改写：

| # | 验收标准 | 判定方式 |
| --- | --- | --- |
| 1 | 实跑主链路 | Kafka ODS JSON → `project` / `type_convert` → MaxCompute 分区表 |
| 2 | 分区与权限 | 正常写入、动态/静态分区、权限失败分类、远端 schema/partition preflight |
| 3 | DLQ 与 replay | sink 暂时失败进入 DLQ，修复后 replay 写回 |
| 4 | 恢复与重复边界 | 应用 restart、checkpoint reset/replay，并记录 append 模式可能重复的边界 |
| 5 | 文档与元数据 | 组件文档、connector readiness、certification evidence 更新 |
| 6 | maturity 门槛 | 上述证据完成前，`maxcompute` / `odps` 保持 `experimental` |

## 非目标

- 不做 ODPS/MaxCompute 的 lookup / source 方向（属 ROADMAP「有界后续」，须在 sink 认证后再评估）。
- 不因本轨阻塞而暂停 IT-1..IT-4 的推进。
- 不在无真实环境的情况下用单测或 mock 声称认证通过。

## 交付约束

1. **凭据不得进入仓库**。所有连接信息通过环境变量注入；证据文件中的 endpoint / project /
   table 需脱敏或使用专用测试资源。
2. **失败注入需要受控权限与测试表**，不得在共享生产项目上执行故障注入。
3. **不得用其他 sink 的证据外推**。MaxCompute 的分区语义与 SDK 行为与关系型/OLAP sink 不同。
4. **认证完成前 maturity 不变**。即使部分验收通过，也不得提前升级 maturity。

## 依赖与前置

| 类型 | 内容 | 状态 |
| --- | --- | --- |
| 外部输入 | `MAXCOMPUTE_ENDPOINT` | 缺失 |
| 外部输入 | `MAXCOMPUTE_PROJECT` | 缺失 |
| 外部输入 | `MAXCOMPUTE_TABLE` | 缺失 |
| 外部输入 | `MAXCOMPUTE_ACCESS_KEY_ID` | 缺失 |
| 外部输入 | `MAXCOMPUTE_ACCESS_KEY_SECRET` | 缺失 |
| 外部输入 | 可选：tunnel endpoint、quota | 缺失 |
| 外部输入 | 用于失败注入的受控权限 / 测试表 | 缺失 |

**解除阻塞条件**：上述必填项全部可得，且失败注入有受控资源。

## 完成定义（DoD）

1. 六项验收标准全部 `passed`。
2. ROADMAP P0 由 `blocked_external` 置 `delivered`。
3. `docs/components/sink-maxcompute.md`、connector readiness、certification evidence 更新。
4. `maxcompute` / `odps` maturity 按实际证据升级；证据不完整的部分保持 `experimental`
   并在文档说明边界。
