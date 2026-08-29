# IT-2 任务分解

> 状态取值：`todo` / `active` / `done` / `blocked`。一次只允许一个 `active`。
>
> **顺序约束**：T2.3（共享契约层）必须先于 T2.4、T2.5、T2.6 完成。控制面线（T2.1、T2.2）
> 与数据面线无代码依赖，可先行。

## Round 分组

| Round | 包含 task | 目标 | 可独立发版 |
| --- | --- | --- | --- |
| 1/5 | T2.1 | 重启后不再静默丢任务 | 是 |
| 2/5 | T2.2 | 停止的任务不会自行运行；reset 有 fencing | 是 |
| 3/5 | T2.3、T2.4 | 源序契约层落地；ClickHouse 乱序 replay 不再产生错值 | 是 |
| 4/5 | T2.5、T2.6 | 完整 Key 契约 + DLQ 身份上下文与受控 replay | 是 |
| 5/5 | T2.7、T2.8 | metadata-PK sink 跨路径认证 + 迭代收口 | 是 |

## 任务表

| ID | 任务 | 依赖 | 状态 | 证据落点 |
| --- | --- | --- | --- | --- |
| T2.1 | RA-2：`restore_failed` 持久化与可见性 | IT-1 | `todo` | `restore_visibility_test.go`、health 单测 |
| T2.2 | RA-3：desired/observed 拆分 + reset fencing | T2.1 | `todo` | `lifecycle_desired_state_test.go`、迁移测试 |
| T2.3 | 共享 record identity & ordering 契约层 | IT-1 | `todo` | `core` 契约层单测 + conformance fixture |
| T2.4 | RA-1：ClickHouse 版本列消费源序 | T2.3 | `todo` | 乱序 replay e2e、`sink-clickhouse.md` |
| T2.5 | GAP-7.1：完整 Key 产出与组合预检 | T2.3 | `todo` | kafka key conformance、preflight 单测 |
| T2.6 | GAP-7.2：DLQ 身份上下文与受控 replay 门禁 | T2.5 | `todo` | DLQ 迁移/conformance、replay crash 测试 |
| T2.7 | GAP-7.3：metadata-PK sink 一致性与跨路径认证 | T2.4、T2.6 | `todo` | shared conformance、跨路径 e2e |
| T2.8 | 迭代收口：文档、证据、ROADMAP 回填 | T2.1..T2.7 | `todo` | 各文档 diff、证据 JSON、README 看板 |

## 任务明细

### T2.1 RA-2：`restore_failed` 持久化与可见性

**验收**（spec 验收 1-3）：

1. 六类失败（`dag_yaml_parse` / `linear_yaml_parse` / `dag_connection_resolve` /
   `linear_connection_resolve` / `spec_validate` / `runner_build`）各有注入测试；重启后
   pipeline 仍在 API 与 health 中，状态 `restore_failed`，错误含阶段、错误码与 remediation。
2. production strict 模式启动失败并打印完整失败清单；非 strict 启动成功但 health `degraded`。
3. 修复底层问题后重启，pipeline 恢复为原 desired state，**且从原 checkpoint 续跑**
   （不得被当作首次启动）。
4. `hack/e2e-control-plane-persistence.sh` 回归通过。

**特别注意**：验收 3 的后半句是本任务最关键的语义约束。若 `restore_failed` 的实现顺带清空或
绕过了 checkpoint，本任务把一个可见性缺陷升级成了数据缺陷 —— 必须有显式测试覆盖。

**证据落点**：`internal/etl/server/restore_visibility_test.go`、health 单测、
`hack/e2e-control-plane-persistence.sh` 扩展用例、`docs/runtime-modes.md`。

### T2.2 RA-3：desired/observed 拆分 + reset fencing

**验收**（spec 验收 4-7）：

1. stop 后重启进程，pipeline 保持 stopped 且**无任何 source 读取与 sink 写入**；pause 同理。
2. desired state 持久化失败时 API 返回非 2xx，内存状态回到最后成功值。
3. 对运行中 pipeline 调用 reset 返回明确冲突错误；旧 generation 的 in-flight checkpoint
   写入被拒绝且该拒绝可观测（日志 + 指标）。
4. 每类 source（Kafka / MySQL CDC / MySQL snapshot+CDC / PostgreSQL CDC）的 reset 语义有
   测试与文档条目，实际行为与 `openapi.yaml` 描述一致。
5. 旧单列 `status` 拆分为 desired/observed 的迁移映射有固化测试，三 backend 均通过。
6. 容器 e2e：stop → 重启容器 → 确认无写入 → start → 从原 checkpoint 续跑。

**证据落点**：`internal/etl/server/lifecycle_desired_state_test.go`、fencing 单测、
迁移测试、`hack/e2e-lifecycle-desired-state.sh`、`docs/etl-api.md`、`docs/openapi.yaml`。

### T2.3 共享 record identity & ordering 契约层

**这是数据面的地基，必须先于 T2.4/T2.5/T2.6。**

**验收**：

1. `SourceOrder` 覆盖 `mysql_cdc`、`mysql_snapshot_cdc`、`postgres_cdc`、`kafka`、
   `mysql_batch`；对 `file` / `http` / `redis` 返回 `ok = false`。
2. `RecordIdentity` 返回完整性判定与不完整原因；UPDATE 的 Key 表达 before-image。
3. `ok = false` / `complete = false` 是一等返回值，调用方无法用 `_` 静默忽略
   （用返回结构体而非裸值强制）。
4. 源序映射跨进程重启单调，**不依赖 wall clock**；位宽分配有边界测试
   （binlog file 序号溢出、Kafka partition 上限）。
5. `-race` 下并发派生无版本倒挂。
6. conformance fixture 就位，供 T2.4/T2.5/T2.7 复用。
7. 契约层不反向依赖 `sink` / `source` 具体实现（`go list -deps` 断言）。

**证据落点**：契约层单测、conformance fixture、`go list -deps` 输出。

### T2.4 RA-1：ClickHouse 版本列消费源序

**验收**（spec 验收 8-10）：

1. 旧 UPDATE 晚于新 UPDATE 到达（**DLQ replay 与 checkpoint reset 两条路径**）时，
   `FINAL` 查询保持新值；旧 INSERT 晚于 DELETE 不重新生成已删除行。
2. 复合主键、主键变更 UPDATE、多表 fan-out 下版本派生一致。
3. 无源序能力的组合在 validate/preflight 被拒绝，错误指明缺失的 metadata 字段。
4. 时钟回拨与多写入进程并发下无版本倒挂。
5. 容器 e2e：MySQL snapshot+CDC → ClickHouse 与 Kafka → ClickHouse 各跑一次乱序 replay 矩阵，
   记录命令、镜像版本、执行日期与 passed/failed。
6. 新旧版本列混存的迁移边界已分析并文档化。

**证据落点**：`internal/etl/sink/clickhouse_version_test.go`、preflight 单测、
`internal/etl/e2e/` 乱序 replay 用例、`docs/components/sink-clickhouse.md`、
`docs/etl-idempotency.md`、`docs/reliability-certification.md`、`docs/path-contract.md`。

### T2.5 GAP-7.1：完整 Key 产出与组合预检

**验收**（沿用 ROADMAP GAP-7.1 原验收 1-4）：

1. 单列与复合 `pkNames` 的 INSERT/UPDATE/DELETE 均产出完整 JSON Key；复合键缺列时
   不再因「至少一列存在」生成部分 Key。
2. 主键变更 UPDATE 使用完整 Before Key；仅已登记的缺列语义允许以 `Data` 补齐，
   未登记 / `old` 缺失 / 冲突输入进入身份错误或 DLQ。
3. identity-aware sink 与无身份 source 的组合在 validate/preflight 给出具体 field issue；
   正常 Record 不得抵达 sink 写入路径。
4. 失败事件的 DLQ 持久化与 checkpoint 边界符合既有 at-least-once 约定；DLQ 写入失败时
   不得确认或静默跳过 source 位置。

**证据落点**：source/key conformance 单测、server validate/preflight 单测、
Kafka 解析错误与 checkpoint/DLQ 单测、至少一条 `canal_json` → metadata-PK sink focused e2e。

### T2.6 GAP-7.2：DLQ 身份上下文与受控 replay 门禁

**验收**（沿用 ROADMAP GAP-7.2 原验收 1-4）：

1. 新 DLQ 条目持久化完整身份上下文（原始 payload、声明 `pkNames`、缺键原因、
   **冻结的** format-contract ID、原始 `old` 状态分类、目标表/库、replay provenance）；
   重启、备份恢复与 API 查询后不丢失。
2. 可重建的历史记录在 replay 前生成完整 Key 并走普通 sink 路径；键值真正缺失、契约未知或
   `old` 状态不支持推断的记录保持 repair/quarantine。
3. 静态 PK 兼容只接受带 legacy provenance 的记录，且原始 `pkNames` 与目标静态键集合精确一致、
   全部值存在；任何不匹配、部分键或主键变更 UPDATE 均被拒绝并留在 DLQ。
4. replay → sink acknowledgement → checkpoint → DLQ 删除的顺序有 crash/restart 测试；
   重放可重复但不会造成静默错行定位。

**上线顺序**：先部署识别能力（只观测不拒绝）统计存量空 Key/部分 Key，确认可控后再切 fail-closed。

**证据落点**：DLQ storage migration/conformance、replay API/worker 单测、crash/restart 故障注入、
legacy 空 Key / 复合键 / 主键变更 / unknown contract 的 replay matrix e2e。

### T2.7 GAP-7.3：metadata-PK sink 一致性与跨路径认证

**验收**（沿用 ROADMAP GAP-7.3 原验收 1-4）：

1. 共享 conformance 覆盖单键/复合键、INSERT/UPDATE/DELETE、主键变更、跨表 fan-out、
   正常空/部分 Key 拒绝与合格 legacy replay；**每个**声明支持 metadata PK 的 sink 均通过。
2. preflight 对缺少 `Table` / `Database`、身份能力或目标 PK 配置的组合给出可操作错误；
   自动建表优先消费 `Metadata.ColumnTypes`，不以样本值推断替代可用的声明类型。
3. Kafka → ClickHouse 与 Kafka → PostgreSQL 多表路径完成容器 e2e，含 checkpoint reset/replay、
   sink outage → DLQ → replay、复合键与 key-changing UPDATE；ES 模板 fan-out 单独记录，
   **不复用关系型证据**。
4. 每条认证路径记录实际命令、镜像/依赖版本、执行日期与 passed/failed/skipped/blocker；
   缺容器、DSN 或服务只能保留 `queued` / `active` / `blocked_external`，不得标 `delivered`。

**证据落点**：shared conformance fixture、per-target metrics 与 `error_class` 断言、
跨路径 e2e 证据 JSON、各 sink 组件文档。

### T2.8 迭代收口

**验收**：

1. `spec.md` 的 16 项验收标准逐条核对为 `passed`。
2. ROADMAP 中 RA-1、RA-2、RA-3、GAP-7.1/.2/.3 置 `delivered`，GAP-7 父条目标注收口。
3. `reliability-certification.md`、`path-contract.md`、`etl-idempotency.md`、`etl-api.md`、
   `openapi.yaml`、`components/sink-clickhouse.md`、`runtime-modes.md` 已更新。
4. 受影响路径的证据 JSON 由 IT-1 框架重新产出并绑定当前 commit。
5. `docs/iterations/README.md` 状态看板更新。

**证据落点**：各文档 diff；证据 JSON；README 看板。

## 领取记录模板

```text
Round: <n>/5
Roadmap item: <RA-1 | RA-2 | RA-3 | GAP-7.1 | ...> (IT-2/T2.<n>)
Profile/path: <standalone | connector path>
Objective: <one observable outcome>
Scope: <files/components allowed>
Non-goals: <explicit exclusions>
Acceptance: <numbered checks，直接引用本文件对应 task 的验收>
Evidence: <commands, run URL, e2e, docs>
Result: <delivered|active|blocked_external>
Residual/follow-up: <bounded next item or none>
```
