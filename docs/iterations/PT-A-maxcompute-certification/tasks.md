# PT-A 任务分解

> 状态取值：`todo` / `active` / `done` / `blocked`。
>
> **当前整轨为 `blocked_external`**：在 [spec.md 依赖与前置](./spec.md#依赖与前置) 的
> 全部必填凭据可得前，任何 task 都不具备领取资格。不得因「难以推进」而把主线迭代
> 标记为阻塞，也不得因主线推进而降级 P0 的优先级。

## Round 分组

| Round | 包含 task | 目标 |
| --- | --- | --- |
| 1/2 | PA.1、PA.2 | 环境就绪 + 主链路与分区语义跑通 |
| 2/2 | PA.3、PA.4 | 故障注入与恢复边界 + 证据和 maturity 收口 |

本轨预算 2 rounds 而非 5：实现已存在，工作量集中在执行与记录。

## 任务表

| ID | 任务 | 依赖 | 状态 | 证据落点 |
| --- | --- | --- | --- | --- |
| PA.1 | 环境与测试资源就绪 | 外部凭据 | `blocked` | 脱敏后的资源清单 |
| PA.2 | 主链路 + 动态/静态分区 | PA.1 | `blocked` | e2e 运行记录 |
| PA.3 | 故障注入、DLQ replay、restart/reset | PA.2 | `blocked` | 故障场景记录 |
| PA.4 | 证据产出与 maturity 判定 | PA.3 | `blocked` | 组件文档、certification evidence |

## 任务明细

### PA.1 环境与测试资源就绪

**验收**：

1. 五项必填环境变量可用，`hack/e2e-maxcompute.sh` 不再因门控而 skip。
2. 测试资源清单确认：可写分区表、无写权限表/账号、schema 不匹配表、
   （推荐）独立测试 project。
3. 凭据注入路径确认不落盘、不入仓库、不进日志。
4. 若只有共享生产项目可用 → 故障注入项记为 `blocked` 并缩减认证范围，**不得记 pass**。

**证据落点**：脱敏后的资源清单；门控通过的首次运行记录。

### PA.2 主链路 + 分区语义

**验收**（spec 验收 1、2 的正向部分）：

1. Kafka ODS JSON → `project` / `type_convert` → MaxCompute 分区表跑通。
2. 动态分区与静态分区两种配置各跑一次。
3. 远端 schema / partition preflight 在配置不匹配时于**写入前**拒绝。

**证据落点**：e2e 运行记录（实际命令、SDK 版本、执行日期）。

### PA.3 故障注入、DLQ replay、恢复边界

**验收**（spec 验收 2 的失败部分、3、4）：

1. 权限不足被分类为权限类错误，不进入无限重试，信息可操作。
2. sink 暂时失败进入 DLQ；修复后 replay 写回成功。
3. 应用 restart 后按 checkpoint 恢复。
4. checkpoint reset / replay 执行一次，**明确记录 append 模式可能重复的边界**；
   若目标表有业务主键与去重策略则在 path contract 声明，否则声明为「重放会产生重复」。

**证据落点**：各故障场景的运行记录与错误输出。

### PA.4 证据产出与 maturity 判定

**验收**（spec 验收 5、6）：

1. `docs/components/sink-maxcompute.md`、connector readiness、certification evidence 更新。
2. 证据记录含实际命令、SDK/依赖版本、执行日期、每项 passed/failed/skipped/blocker、
   脱敏资源标识；若 IT-1 已交付则采用其结构化格式。
3. maturity 按**实际证据**升级；任一验收未闭合的部分保持 `experimental` 并说明边界。
4. ROADMAP P0 由 `blocked_external` 置 `delivered`；若仅部分闭合则保持 `active` 并列出残留。
5. `docs/iterations/README.md` 状态看板更新。

**证据落点**：组件文档 diff；certification evidence；ROADMAP diff。

## 领取记录模板

```text
Round: <n>/2
Roadmap item: P0 (PT-A/PA.<n>)
Profile/path: connector path — kafka -> project/type_convert -> maxcompute partitioned table
Objective: <one observable outcome>
Scope: <files/components allowed —— 本轨不新增 writer 实现>
Non-goals: ODPS lookup/source 方向；用 mock 或单测替代真实环境认证
Acceptance: <numbered checks，直接引用本文件对应 task 的验收>
Evidence: <commands, SDK versions, run date, 脱敏资源标识>
Result: <delivered|active|blocked_external>
Residual/follow-up: <bounded next item or none>
```
