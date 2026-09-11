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
| T2.1 | RA-2：`restore_failed` 持久化与可见性 | IT-1 | `done` | `restore_visibility_test.go`、health 单测 |
| T2.2 | RA-3：desired/observed 拆分 + reset fencing | T2.1 | `done` | `lifecycle_desired_state_test.go`、迁移测试、容器 e2e |
| T2.3 | 共享 record identity & ordering 契约层 | IT-1 | `done` | `core` 契约层单测 + conformance fixture |
| T2.4 | RA-1：ClickHouse 版本列消费源序 | T2.3 | `done` | 乱序 replay e2e、`sink-clickhouse.md` |
| T2.5 | GAP-7.1：完整 Key 产出与组合预检 | T2.3 | `done` | kafka key conformance、preflight 单测、focused e2e |
| T2.6 | GAP-7.2：DLQ 身份上下文与受控 replay 门禁 | T2.5 | `done` | DLQ 迁移/conformance、replay crash 测试 |
| T2.7 | GAP-7.3：metadata-PK sink 一致性与跨路径认证 | T2.4、T2.6 | `done` | shared conformance、跨路径 e2e |
| T2.8 | 迭代收口：文档、证据、ROADMAP 回填 | T2.1..T2.7 | `done` | 各文档 diff、证据 JSON、README 看板 |

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

### Round 8/不限 —— 迭代收口（T2.8），2026-09-05 领取

```text
Round: 8/不限
Roadmap item: IT-2 closeout (T2.8)
Profile/path: IT-2 全部交付面（控制面 + 数据面）
Objective: 16 项验收逐条核对；ROADMAP 父子条目置 delivered；证据 JSON 重绑当前 commit；看板收口。
Scope: tasks/spec/README 状态、ROADMAP 条目、reliability-certification 补录三条认证路径、证据 JSON。
Non-goals: 新能力、新认证路径、文档重写；发现的缺陷只登记不扩大范围。
Acceptance: T2.8 验收 1-5。
Evidence: 本矩阵；docs/evidence/*.json；各文档 diff。
Result: delivered
Residual/follow-up: 无（IT-3 接续）。
```

T2.8 验收矩阵（2026-09-05）：

| 验收 | 证据 | 结果 | 残留 |
| --- | --- | --- | --- |
| 1. spec 16 项验收逐条 passed | 下万 16 项核对矩阵（T2.1–T2.7 各验收矩阵 + 本轮 e2e 重跑） | passed | 无 |
| 2. ROADMAP RA-1/RA-2/RA-3/GAP-7.1/.2/.3 置 delivered；GAP-7 父条目收口 | ROADMAP 状态行（全部 delivered 2026-09-05） | passed | 无 |
| 3. 七份文档已更新 | git diff：reliability-certification(+91)、path-contract、etl-idempotency、etl-api、openapi、sink-clickhouse、runtime-modes（合计 581 insertions） | passed | 无 |
| 4. 证据 JSON 重绑当前 commit | `docs/evidence/mysql_cdc__mysql_upsert.json`、`mysql_snap_cdc__ch_rmt.json` 均为 commit `387daf07`、result passed（IT-1 框架 `go test -tags=e2e ./internal/etl/e2e/` 重跑） | passed | 无 |
| 5. README 看板更新 | 迭代总览表 IT-2 → complete | passed | 无 |

IT-2 spec 16 项验收核对（全部 passed，证据案引）：

| # | 验收 | 证据案引 |
| --- | --- | --- |
| 1 | 六类 restore 失败可见 | T2.1 矩阵行 1（`TestRestoreFailuresPersistAndRemainVisible`） |
| 2 | strict fail-closed | T2.1 矩阵行 2（`TestRestoreStrictProfileDefaultsAndOverrides`） |
| 3 | 修复后可恢复 | T2.1 矩阵行 3（`TestRestoreFailureRepairPreservesCheckpointAndEncryptedSpec`） |
| 4 | desired state 持久化 | T2.2 矩阵行 1（`TestDesiredStoppedAndPausedSurviveRestoreWithoutIO`；本轮 `e2e-lifecycle-desired-state.sh` 重跑 PASS） |
| 5 | 生命周期错误可见 | T2.2 矩阵行 2（`TestDesiredStatePersistenceFailureDoesNotChangeRuntime`） |
| 6 | reset fencing | T2.2 矩阵行 3（`TestRunningPipelineRejectsCheckpointResetAndSet`、fence observable） |
| 7 | source reset 语义 | T2.2 矩阵行 4（`TestCheckpointResetResponseDocumentsSourceBoundary`；OpenAPI parse） |
| 8 | 乱序 replay 不产生错值 | T2.4 矩阵行 1；本轮 `e2e-clickhouse-replay-ordering.sh` 重跑 PASS（DLQ replay + reset 两路径） |
| 9 | 源序派生一致 | T2.4 矩阵行 4（`-race` 无倒挂；不依赖 wall clock） |
| 10 | 无源序组合被拒 | T2.4 矩阵行 3（`TestClickHousePreflight*` fail-closed） |
| 11 | 完整 Key 契约 | T2.5 矩阵行 1（`TestTryCanalJSONCompleteCompositeKeyMatrix` 等） |
| 12 | 主键变更 UPDATE | T2.5 矩阵行 1-2（before-image/登记补齐/冲突拒绝） |
| 13 | DLQ 身份上下文完整 | T2.6 矩阵行 1（冻结上下文跨重启/备份/API 保真） |
| 14 | 受控 replay 门禁 | T2.6 矩阵行 2-3（重建/quarantine/legacy exact-match；本轮 `e2e-kafka-canal-identity.sh` 重跑 PASS） |
| 15 | metadata-PK sink 一致 | T2.7 矩阵行 1-2（四 sink 共享矩阵 + data error） |
| 16 | 跨路径容器认证 | T2.7 矩阵行 4-6（CH/PG 多表矩阵 + ES 独立记录，2026-09-05 重跑 PASS） |

## 领取记录模板

### Round 1/5 —— RA-2（T2.1），2026-09-04 领取

```text
Round: 1/5
Roadmap item: RA-2 (IT-2/T2.1)
Profile/path: standalone control plane restore
Objective: DB 中无法构建 runner 的 pipeline 在重启后仍以 restore_failed 出现在 API/health；production strict 可 fail-closed；修复后保留 checkpoint 并恢复。
Scope: PipelineRow/SQL backend additive migration、PipelineSpecStore restore-state adapter、Server.RestoreFromDB/list/health、runtime profile gate、focused tests、runtime runbook 与 roadmap evidence。
Non-goals: T2.2 desired/observed state 拆分、start/stop/pause/reset fencing；自动修复 YAML/connection；改变 checkpoint 或数据路径；ClickHouse/Kafka 候选能力。
Dependencies: P0 MaxCompute 仍 blocked_external；IT-1 complete；用户已明确要求按规划开始下一轮迭代。
Acceptance: T2.1 验收 1-4；六类失败可见，production strict fail-closed，修复后从原 checkpoint 恢复，control-plane persistence e2e 回归。
Data semantics/rollback: restore failure只更新控制面状态/结构化诊断，不删除或改写 checkpoint/DLQ/spec version；旧 schema 通过 additive migration；回滚代码后新增 nullable/默认空列可安全保留。
Evidence: internal/etl/server/restore_visibility_test.go、storage migration/conformance tests、hack/e2e-control-plane-persistence.sh、docs/runtime-modes.md。
Result: delivered
Residual/follow-up: T2.2 desired/observed + generation fencing；本轮不扩展范围。
```

T2.1 验收矩阵（2026-09-05）：

| 验收 | 证据 | 结果 | 残留 |
| --- | --- | --- | --- |
| 六类失败持久化并在 API/health 可见 | `go test ./internal/etl/server -run 'TestRestoreFailuresPersistAndRemainVisible' -count=1 -v`；`internal/etl/server/restore_visibility_test.go` | passed | 无 |
| production strict fail-closed；non-strict degraded | `TestRestoreFailuresPersistAndRemainVisible`、`TestRestoreStrictProfileDefaultsAndOverrides`；启动调用链 `internal/logic/app/app.go` 将 restore error 作为 fatal | passed | 无 |
| 修复后恢复原状态且 checkpoint/spec/version 不漂移 | `go test ./internal/etl/server -run 'TestRestoreFailureRepairPreservesCheckpointAndEncryptedSpec' -count=1 -v`；三 backend `PipelineRestoreState` conformance | passed | 无 |
| 控制面持久化回归 | `./hack/e2e-control-plane-persistence.sh` | passed | 无 |
| SQLite/MySQL/PostgreSQL migration 与 storage contract | `go test ./internal/etl/storage -run 'TestSQLiteConformance/PipelineRestoreState' -count=1 -v`；`CONTAINER_CLI=docker ./hack/e2e-storage-mysql.sh`；`CONTAINER_CLI=docker ./hack/e2e-storage-postgres.sh` | passed | 无 |
| 并发与文档结构 | `go test -race ./internal/etl/server ./internal/etl/storage/... -count=1`；OpenAPI YAML parse；`git diff --check` | passed | 无 |

### Round 2/5 —— RA-3（T2.2），2026-09-05 领取

```text
Round: 2/5
Roadmap item: RA-3 (IT-2/T2.2)
Profile/path: standalone lifecycle + Kafka/MySQL CDC/MySQL snapshot+CDC/PostgreSQL CDC checkpoint reset
Objective: stopped/paused 意图跨重启保持；生命周期写失败不改变 runtime；reset/set 只在静止态执行，并以 generation 拒绝旧 runner checkpoint。
Scope: pipeline desired/observed/generation 与 checkpoint generation additive migration；storage lifecycle CAS；server StartAll/start/stop/pause/resume/reset；scheduler/reconciler/hot-reload start gate；指标、四类 source reset 契约、focused/e2e/docs。
Non-goals: distributed task fencing重做、在线 drain/原子 spec rollout、源事件 ordering、跨 sink exactly-once、自动重置 Kafka consumer group 或复制槽。
Dependencies: T2.1 delivered；三 backend storage 可用；P0 外部阻塞不影响本项。
Acceptance: T2.2 验收 1-6；旧 status 映射固定，三 backend conformance，stop/pause 重启零读写，storage failure rollback，运行态 reset 409，旧 generation 写被拒且日志/指标可见，source-specific 文档/API 一致，容器 e2e。
Data semantics/rollback: 先持久化 desired 再改变 runtime；stop/pause 不清 checkpoint；reset 在同一 storage 事务提升 generation 后删除/替换 checkpoint；stale write 只能被拒绝，不能覆盖新边界；旧 status 列保留一个兼容周期。
Evidence: internal/etl/server/lifecycle_desired_state_test.go、storage migration/conformance 与 fencing tests、hack/e2e-lifecycle-desired-state.sh、docs/etl-api*.md、docs/openapi.yaml。
Result: delivered
Residual/follow-up: T2.3 record identity & ordering contract；本项不扩展到 RA-1。
```

T2.2 有界增量：

| 增量 | 可观察结果 | 允许触及 | 验收/故障场景 | 回滚 |
| --- | --- | --- | --- | --- |
| T2.2a storage truth + fence | 三 backend 能保存/迁移 desired、observed、generation；stale checkpoint CAS 返回稳定错误 | `storage`、`core.Checkpoint`、迁移/contract tests | 旧状态映射；reset 事务；reset 后旧 generation save；MySQL/PG/SQLite | 保留旧 `status`，新增列 additive |
| T2.2b runtime lifecycle | StartAll/scheduler/reconciler/API 均只按 desired 启动；持久化失败时 runtime 不变 | `server`、`orchestrator.Scheduler`、lifecycle tests | stop/pause 重启零读写；start/stop/pause/resume fault；并发 start/reset | 旧 status 继续镜像 observed |
| T2.2c reset contract + observability | running reset/set 409；四 source 返回准确边界；stale write 有 log/metric | API、pipeline metrics、docs/OpenAPI | Kafka/group、MySQL current position、snapshot 重拉、PG slot；fenced counter | 不自动操作 broker/slot |
| T2.2d container closeout | 容器 stop→restart 零写入→start 从原 checkpoint 续跑 | 新 e2e、现有 crash/storage gates、证据文档 | crash/restart、checkpoint 保留、重复边界、三 backend | 脚本隔离资源并清理 |

T2.2 验收矩阵（2026-09-05）：

| 验收 | 证据 | 结果 | 残留 |
| --- | --- | --- | --- |
| stop/pause 跨进程恢复保持静止且零 source/sink I/O | `go test ./internal/etl/server -run TestDesiredStoppedAndPausedSurviveRestoreWithoutIO -count=1 -v`；`internal/etl/server/lifecycle_desired_state_test.go` | passed | 无 |
| desired state 持久化失败不改变 runtime | `go test ./internal/etl/server -run TestDesiredStatePersistenceFailureDoesNotChangeRuntime -count=1 -v`（start/stop/pause） | passed | 无 |
| 运行态 reset/set 409；旧 generation checkpoint 拒绝、日志与指标可见 | `TestRunningPipelineRejectsCheckpointResetAndSet`；`TestRunnerCheckpointFenceIsObservableAndNotRetried`；storage `PipelineLifecycleFence` conformance | passed | 无 |
| 四类 source reset 契约与 API/OpenAPI 一致 | `TestCheckpointResetResponseDocumentsSourceBoundary`；`docs/etl-api.md`、`docs/etl-api.zh.md`、`docs/openapi.yaml`（Ruby YAML parse passed） | passed | 无 |
| 旧 status 映射与 SQLite/MySQL/PostgreSQL migration | `go test ./internal/etl/storage/... -count=1`；`CONTAINER_CLI=docker ./hack/e2e-storage-mysql.sh`；`CONTAINER_CLI=docker ./hack/e2e-storage-postgres.sh` | passed | 无 |
| 容器 stop→restart 零写入→显式 start 从原 checkpoint 续跑 | `CONTAINER_CLI=docker ./hack/e2e-lifecycle-desired-state.sh` | passed | 无 |
| crash/restart、控制面、全量与 race 回归 | `./hack/e2e-cdc-crash-recovery.sh`；`./hack/e2e-control-plane-persistence.sh`；`go test ./... -count=1`；`go test -race ./internal/etl/server ./internal/etl/orchestrator ./internal/etl/pipeline ./internal/etl/telemetry ./internal/etl/storage/... -count=1`；`git diff --check` | passed | 无 |

### Round 3/5 —— 共享契约层（T2.3），2026-09-05 领取

```text
Round: 3/5
Roadmap item: GAP-7 shared contract foundation (IT-2/T2.3)
Profile/path: core record metadata contract for MySQL CDC/snapshot+CDC, PostgreSQL CDC, Kafka and MySQL batch
Objective: 用不依赖 wall clock 的显式结果类型统一派生源事件顺序与 Record 身份完整性，为 RA-1、GAP-7.1/.2/.3 提供唯一实现地基。
Scope: internal/etl/core 或其相邻无反向依赖包、source-order/identity fixtures、边界/并发/依赖测试与契约文档。
Non-goals: ClickHouse 写入接入、Kafka 解析行为切换、DLQ schema/replay 门禁、sink preflight、跨 sink exactly-once。
Dependencies: IT-1 complete；T2.1/T2.2 delivered；T2.4/T2.5/T2.6 必须等待本项 delivered。
Acceptance: T2.3 验收 1-7；五类有序 source、三类明确无序 source、身份完整原因、UPDATE before-image、跨进程确定性、位宽边界、race、共享 fixture、无 sink/source 反向依赖。
Data semantics/rollback: 顺序值只编码持久化 source position，不读取 wall clock/进程计数；无法证明时返回显式 unavailable；身份不完整只分类不在本项改变投递；新增 additive API 可在未接入消费方时安全回滚。
Evidence: core contract tests、conformance fixtures、go test -race、go list -deps 断言、契约文档。
Result: delivered
Residual/follow-up: T2.4 ClickHouse source-order consumption；T2.5 normal-flow complete-key enforcement。
```

T2.3 有界增量：

| 增量 | 可观察结果 | 允许触及 | 验收/故障场景 | 回滚 |
| --- | --- | --- | --- | --- |
| T2.3a deterministic source order | 五类 source 返回结构化可用结果，file/http/redis 明确 unavailable；解析与位宽溢出不静默截断 | `core` 相邻契约包 + tests | binlog file/pos、snapshot phase、PG LSN、Kafka partition/offset、batch cursor；重启确定性与边界 | additive helper，消费方尚未切换 |
| T2.3b record identity result | 完整 Key 返回声明列和值；空/部分/冲突/UPDATE 旧键缺失返回稳定原因 | 同一契约包 + fixtures | 单键、复合键、空值、before-image、声明缺失 | 本项仅观测分类，不改变 pipeline 行为 |
| T2.3c conformance and dependency gate | RA-1/GAP-7 可复用 fixture；并发无倒挂；契约包不依赖具体 source/sink | conformance testdata、race/dependency tests、docs | `-race`、边界矩阵、`go list -deps` | 删除 additive fixture/helper 即可 |

T2.3 验收矩阵（2026-09-05）：

| 验收 | 证据 | 结果 | 残留 |
| --- | --- | --- | --- |
| 五类 source order 与 file/http/redis unavailable | `TestSourceOrderConformanceFixtures`；MySQL CDC/snapshot+CDC、PostgreSQL CDC、Kafka canal、MySQL batch producer tests | passed | PG initial snapshot 无 durable row cursor，按契约显式 unavailable |
| identity 完整性、稳定原因与 UPDATE before-image | `TestRecordIdentityConformanceFixtures`、`TestRecordIdentityReasonMatrix`、`TestRecordIdentityPreservesLargeNumbersAndBeforeImage` | passed | 本项只分类，不切换正常流 fail-closed |
| unavailable/incomplete 为结构体一等结果 | `core.SourceOrderResult`、`core.RecordIdentityResult` | passed | 消费方分别由 T2.4/T2.5 接入 |
| 无 wall clock、重启确定性与位宽边界 | `TestSourceOrderIgnoresWallClockAndInstanceName`、`TestSourceOrderSnapshotAlwaysPrecedesCDC`、`TestSourceOrderBoundariesAreExplicit` | passed | 文本 cursor 不伪造 UInt64 |
| 并发无倒挂与 race | `go test -race ./internal/etl/core/... ./internal/etl/source/... -count=1` | passed | 无 |
| 可复用 conformance fixture | `internal/etl/core/contracttest/fixtures.go` | passed | T2.4/T2.5/T2.7 消费 |
| 无 connector 反向依赖 | `go list -deps ./internal/etl/core/...`（81 packages；无 `internal/etl/source` / `sink`）；`TestCoreContractHasNoConnectorReverseDependencies` | passed | 无 |
| 全仓回归与文档 | `go test ./... -count=1`；`git diff --check`；`docs/record-contract.md`、`docs/path-contract.md` | passed | 无 |

### Round 4/不限 —— ClickHouse 消费源序（T2.4），2026-09-05 领取

> 用户已明确要求闭合全部规划迭代，本次执行不受默认五轮自动停止限制；仍严格保持一次一个 active 主项。

```text
Round: 4/不限
Roadmap item: RA-1 (IT-2/T2.4)
Profile/path: MySQL snapshot+CDC -> ClickHouse and Kafka -> ClickHouse
Objective: ReplacingMergeTree 版本与删除墓碑消费真实 source order，使旧 UPDATE/INSERT 的晚到 replay 不能覆盖新值或复活已删除行。
Scope: ClickHouse native/HTTP 写入与 auto-create/version schema、validate/preflight source-order gate、focused/e2e tests、ClickHouse/idempotency/path/reliability 文档。
Non-goals: T2.5 正常流完整 Key fail-closed、T2.6 DLQ schema/replay provenance、吞吐基准、跨 sink exactly-once、CH-C1..CH-C8 候选项。
Dependencies: T2.3 delivered；IT-1 e2e harness available；ClickHouse 24.3 local image required for container evidence。
Acceptance: T2.4 验收 1-6；DLQ replay/checkpoint reset 乱序矩阵、DELETE 不复活、复合键/改键/多表、无源序 preflight、并发/时钟、新旧版本迁移边界、两条容器路径。
Data semantics/rollback: sink ack 与 checkpoint 顺序不变；版本仅来自 source position；无法证明时 fail-closed；旧 Int64/时钟版本与新 UInt64/source 版本不得在同一 RMT 表无迁移地混用；回滚不得删除 tombstone 或 DLQ。
Evidence: internal/etl/sink/clickhouse_version_test.go、server preflight tests、hack/e2e-clickhouse-replay-ordering.sh、docs/components/sink-clickhouse.md、docs/etl-idempotency.md、docs/reliability-certification.md、docs/path-contract.md。
Result: delivered
Residual/follow-up: T2.5 GAP-7.1 complete-key production and preflight。
```

T2.4 有界增量：

| 增量 | 可观察结果 | 允许触及 | 验收/故障场景 | 回滚 |
| --- | --- | --- | --- | --- |
| T2.4a version write contract | native/HTTP 每行写入 `SourceOrder.Version`，缺失/溢出不回退时钟；auto-create 使用 UInt64 | ClickHouse sink + focused tests | replay 旧/新 UPDATE、并发、多表/复合键 | 保留显式 legacy 表迁移错误，不自动改历史表 |
| T2.4b delete + preflight safety | source-ordered tombstone 阻止旧 INSERT 复活；无可用 UInt64 source path 在 validate/preflight 阻断 | ClickHouse delete path、server validation/preflight | DELETE→旧 INSERT、PK change、file/http/text cursor | 禁止回滚时删除墓碑；可整表重建回旧策略 |
| T2.4c container closeout | snapshot+CDC 与 Kafka 两条路径完成 DLQ replay/checkpoint reset 乱序矩阵 | IT-1 harness/hack script、docs/evidence | outage、replay、reset、`FINAL` 逐字段对账 | 脚本资源隔离并清理 |

T2.4 验收矩阵（2026-09-05）：

| 验收 | 证据 | 结果 | 残留 |
| --- | --- | --- | --- |
| 晚到旧 UPDATE/INSERT 不覆盖新值或复活 DELETE（DLQ replay + checkpoint reset） | `CONTAINER_CLI=docker E2E_SKIP_BUILD=1 ./hack/e2e-clickhouse-replay-ordering.sh`；镜像 `openetl-go-etl:dev@sha256:8ded68e84e7434b591cc83d4fbb80f8171bdeb190f61e1182e574a489f4fa7c9` | passed | 默认仍为 checkpointed at-least-once；ClickHouse 以源序吸收重复 |
| 复合主键、主键变更 UPDATE、多表 fan-out | `TestClickHouseSourceOrder*` / `TestClickHouseRecord*` focused tests；`CONTAINER_CLI=docker E2E_SKIP_BUILD=1 ./hack/e2e-kafka-multitable-clickhouse.sh` | passed | 无 |
| 无源序能力/不兼容目标 fail-closed | `go test ./internal/etl/server -run 'TestClickHousePreflight' -count=1`；`internal/etl/sink/clickhouse_version_test.go` | passed | `window` 聚合必须显式使用 INSERT-only `append` |
| 不依赖时钟且并发无倒挂 | `go test -race ./internal/etl/core/... ./internal/etl/source ./internal/etl/sink -count=1` | passed | 顺序域由 source position 决定，不提供跨 lineage/partition 全序 |
| MySQL snapshot+CDC 与 Kafka 两条真实容器路径 | replay-ordering（native + HTTP）；`e2e-clickhouse.sh`、`e2e-clickhouse-autocreate.sh`、`e2e-snapshot-cdc-crash.sh`、`e2e-wide-table.sh` 回归 | passed | MySQL 坐标回退须重建/切换目标；业务键须稳定路由到同一 Kafka partition |
| 旧表迁移边界、全仓与静态检查 | `docs/components/sink-clickhouse.md`、`docs/etl-idempotency.md`、`docs/path-contract.md`、`docs/reliability-certification.md`；`go test ./... -count=1`；`go vet ./internal/etl/core ./internal/etl/source ./internal/etl/sink ./internal/etl/server`；`git diff --check` | passed | 旧 `Int64`/单参数 RMT 不原地混用，需新建双参数 RMT 后切换 |

### Round 5/不限 —— 完整 Key 产出与组合预检（T2.5），2026-09-05 领取

```text
Round: 5/不限
Roadmap item: GAP-7.1 (IT-2/T2.5)
Profile/path: Kafka canal_json -> identity-aware metadata-PK sink
Objective: 正常 INSERT/UPDATE/DELETE 只有在声明主键全部可证明时才产生 Record；部分/冲突/未知身份在 sink 前 fail-closed，并保持 DLQ/checkpoint 的 at-least-once 边界。
Scope: Kafka canal_json 解析与格式契约、core identity helper、pipeline/server validate/preflight、focused e2e、配置与身份契约文档。
Non-goals: T2.6 历史 DLQ schema/provenance/replay 修复；T2.7 全部 metadata-PK sink 认证；append-only source 虚构身份；exactly-once。
Dependencies: T2.3 delivered；T2.4 delivered；现有 DLQ/checkpoint writer 与 metadata-PK descriptors。
Acceptance: T2.5 验收 1-4；完整单/复合 Key、完整 Before Key 与登记补齐规则、组合 preflight、身份失败 DLQ/checkpoint 故障边界、至少一条 canal_json focused e2e。
Data semantics/rollback: sink ack/checkpoint 顺序不变；身份失败必须先持久化 DLQ，失败则返回 source error 且不确认位置；普通流不得借 legacy replay fallback；新增格式契约字段保持 additive。
Evidence: source/key conformance、Kafka parser + pipeline DLQ tests、server validate/preflight tests、canal_json -> metadata-PK sink e2e、docs/record-contract.md。
Result: delivered
Residual/follow-up: T2.6 冻结 DLQ 身份上下文与受控 replay 门禁。
```

T2.5 有界增量：

| 增量 | 可观察结果 | 允许触及 | 验收/故障场景 | 回滚 |
| --- | --- | --- | --- | --- |
| T2.5a format-aware complete key | canal_json 单/复合键 DML 只产出完整 Key；UPDATE 旧键按登记语义补齐或给出稳定身份错误 | Kafka parser、core identity/fixtures | 缺列、`old` 缺失/null/冲突、主键变更、大整数 | additive format contract；不得恢复部分 Key |
| T2.5b fail-closed delivery boundary | 身份错误在 sink 前进入 DLQ；DLQ save 失败不确认 Kafka offset/checkpoint | pipeline error/DLQ 接入与 focused tests | DLQ 成功、DLQ 故障、restart/replay、sink 零调用 | 保留原始 payload；回滚不能静默确认拒绝事件 |
| T2.5c composition preflight + e2e | 无身份 source + identity-aware sink 在 validate/preflight 报具体 field issue；合格 canal_json 容器路径通过 | server validation/preflight、descriptor、focused e2e/docs | file/http/普通 Kafka 格式拒绝；完整复合键 DML 通过 | append-only 模式不受影响 |

T2.5 验收矩阵（2026-09-05，镜像 `openetl-go-etl:dev@sha256:796a0db5e4335e8de5895661b8122894d030aeee516e6b9b64ba64b98c6c5ac8`）：

| Criterion | Evidence | Result | Residual or blocker |
| --- | --- | --- | --- |
| 单键/复合键 INSERT、UPDATE、DELETE 与完整 Before Key | `TestTryCanalJSONInsert`、`TestTryCanalJSONUpdateBefore`、`TestTryCanalJSONCompleteCompositeKeyMatrix`；`TestRecordIdentity*` | passed | 仅冻结的 `kafka.canal_json/v1` 允许按 Canal 省略语义补齐未变化键列 |
| 缺失/部分/冲突/未知身份 fail-closed | source/key conformance；`TestMetadataIdentityGateRoutesIncompleteRecordToDLQBeforeSinkAndCheckpoints`、`TestSourceRecordRejectionUsesSameDLQCheckpointBoundary` | passed | 历史 DLQ 的修复与 quarantine 属 T2.6 |
| DLQ 持久化先于 checkpoint；DLQ 故障阻断后续位置 | `TestMetadataIdentityGateDLQFailureBlocksCheckpoint`、`TestMetadataIdentityGateWritesOnlyCompleteSurvivors` | passed | 默认仍为 checkpointed at-least-once；不宣称 exactly-once |
| 无身份 source 组合预检 | `TestMetadataIdentityPreflightRejectsIncapableSourceWithFieldIssue`、`TestMetadataIdentityPreflightAcceptsCanalCapability`、`TestSinkDerivesPKFromMetadataIncludesAdvertisedSinks` | passed | append-only 非身份写入模式不受影响 |
| Canal → ClickHouse focused e2e | `CONTAINER_CLI=docker E2E_SKIP_BUILD=1 ./hack/e2e-kafka-canal-identity.sh` | passed | 覆盖复合键、key-changing UPDATE、两类 data DLQ、后续位置推进与 DELETE |
| 受影响路径回归 | `e2e-kafka-multitable-clickhouse.sh`、`e2e-bug6-column-types.sh`、`e2e-debezium-mysql.sh`、`e2e-kafka-postgres-fanout.sh` | passed | Debezium 启动时 compose 报已有 Redpanda 名称，复用健康实例后完整运行至 PASS |
| race、全仓与静态门禁 | `go test -race ./internal/etl/core/... ./internal/etl/source ./internal/etl/pipeline ./internal/etl/server ./internal/etl/sink -count=1`；`go test ./... -count=1`；`go vet ./internal/etl/core ./internal/etl/source ./internal/etl/pipeline ./internal/etl/server ./internal/etl/sink`；`git diff --check` | passed | 修复并覆盖 ClickHouse 完整 `engine_full` 参数解析回归 |

### Round 6/不限 —— DLQ 身份上下文与受控 replay 门禁（T2.6），2026-09-05 领取

```text
Round: 6/不限
Roadmap item: GAP-7.2 (IT-2/T2.6)
Profile/path: identity-invalid normal flow -> durable DLQ -> controlled replay/quarantine
Objective: DLQ 冻结足以重建或拒绝历史身份的上下文；replay 不依据当前配置猜键，并保持 sink ack/checkpoint/DLQ 删除的可恢复顺序。
Scope: core/DLQ entry、SQLite/MySQL/PostgreSQL storage migration/conformance、replay API/worker、format-contract registry、crash tests、focused e2e 与契约文档。
Non-goals: T2.7 全 metadata-PK sink conformance/跨路径认证；批量自动修复历史 DLQ；普通流绕过身份门禁；exactly-once。
Dependencies: T2.3/T2.5 delivered；现有 DLQ/replay API、storage backup/restore 与 checkpoint generation fencing。
Acceptance: T2.6 验收 1-4；完整冻结上下文跨重启/备份/API 保真、可证明重建与 quarantine、严格 legacy gate、replay crash/restart 顺序。
Data semantics/rollback: 原始 payload 与 DLQ 行不可丢；只有 sink acknowledgement 成功后才可推进 replay 状态/删除；任何不确定身份保持 repair/quarantine；迁移 additive 且旧行可读。
Evidence: 三后端 migration/conformance、replay 门禁/故障注入单测、legacy/unknown contract matrix e2e、API/backup 文档。
Result: delivered
Residual/follow-up: T2.7 metadata-PK sink 一致性与跨路径认证。
```

T2.6 有界增量：

| 增量 | 可观察结果 | 允许触及 | 验收/故障场景 | 回滚 |
| --- | --- | --- | --- | --- |
| T2.6a frozen identity context | 新 DLQ 行冻结 raw payload、PK 声明/原因、format contract、old 状态、目标与 provenance，三后端/API/backup 保真 | core、DLQ/storage/schema/API/docs | 新旧迁移、重启、backup/restore、未知字段 | additive columns/JSON；旧行标为 unknown，不猜测 |
| T2.6b reconstruction + quarantine gate | 仅可证明记录重建完整 Key；unknown/缺值/主键变更或 legacy 键集合不符保持 repair/quarantine | replay handler、format registry、audit/error | empty/partial/composite/key-change/unknown contract matrix | 禁用重建仍保留 DLQ，不删除原文 |
| T2.6c acknowledgement/crash order | replay 在 sink ack 后才更新 checkpoint/删除 DLQ；各故障点重启可重复且不静默错定位 | replay worker、checkpoint/DLQ state、focused e2e | ack 前后 crash、checkpoint/DLQ delete 失败、restart | at-least-once 重放；失败行继续可见 |

T2.6 验收矩阵（2026-09-05）：

| 验收 | 证据 | 结果 | 残留 |
| --- | --- | --- | --- |
| 新 DLQ 冻结上下文跨 API/重启/备份恢复保真 | `TestNewDLQIdentityContextFreezesSourcePayloadAndIdentity`、`TestNewDLQIdentityContextPreservesNonUTF8SourceBytes`、`TestDLQReplayReconstructsFrozenCompositeIdentityAndPreservesAPIContext`、`internal/etl/storage/backup/backup_test.go` | passed | raw payload 属生产数据，API 必须受认证/访问控制保护 |
| 可证明身份重建；缺值/unknown/old 不可推断保持 repair/quarantine | `core.ReconstructDLQRecordIdentity` tests、`TestDLQReplayUnknownContractIsQuarantinedWithStructuredConflict`、`TestDLQReplayRevalidatesIdentityAfterTransforms` | passed | 不提供批量自动修复 |
| legacy 静态 PK 仅接受原始声明与目标集合精确一致；部分/错键/改键拒绝 | `TestDLQLegacyIdentityRepairAPIRequiresExactFrozenDeclaration`、`TestDLQLegacyIdentityRepairRejectsMismatchPartialAndKeyChange`、`TestDLQReplayRechecksStaticSafetySetAfterLegacyRepair` | passed | 无原始声明的旧行继续 `repair_required` |
| sink ack → replay checkpoint → 删除的故障/重启顺序 | `TestDLQReplayRestartAfterDeleteFailureDoesNotWriteSinkTwice`、`TestDLQReplayCheckpointFailureRetainsRowAndAllowsAtLeastOnceRetry`、既有 DAG replay tests | passed | ack checkpoint 前仍可能重复，默认语义保持 at-least-once |
| SQLite/MySQL/PostgreSQL migration 与 CRUD/conformance | `go test ./internal/etl/storage/... -count=1`；`CONTAINER_CLI=docker ./hack/e2e-storage-mysql.sh`；`CONTAINER_CLI=docker ./hack/e2e-storage-postgres.sh` | passed | 无 |
| legacy/复合键/改键/unknown focused 容器矩阵 | `CONTAINER_CLI=docker E2E_SKIP_BUILD=1 ./hack/e2e-kafka-canal-identity.sh`；镜像 `openetl-go-etl:dev@sha256:0076a91ddef463046bb97e604cecd25b335c97d99c66b62dfd4eaaee6519d928` | passed | 单稳定 Kafka partition；跨 sink 一致性属 T2.7 |
| race、全仓、静态与文档门禁 | 受影响包 `go test -race`；`go test ./... -count=1`；受影响包 `go vet`；OpenAPI Ruby YAML parse；`git diff --check` | passed | 无 |

### Round 7/不限 —— metadata-PK sink 一致性与跨路径认证（T2.7），2026-09-05 领取

```text
Round: 7/不限
Roadmap item: GAP-7.3 (IT-2/T2.7)
Profile/path: Kafka/CDC identity contract -> ClickHouse/MySQL/PostgreSQL/Doris metadata-PK sinks
Objective: 所有公开声明 metadata PK 的 sink 对完整/不完整/legacy 身份给出同一 fail-closed 语义，并以真实 Kafka→ClickHouse/PostgreSQL 多表故障矩阵完成认证。
Scope: shared sink conformance fixture、ClickHouse/MySQL/PostgreSQL/Doris metadata-PK 接入点、preflight/schema、per-target error/metrics、既有 Kafka 多表 e2e 与组件/证据文档。
Non-goals: Kafka/S3/file 等非行主键 sink；ES 关系型证据复用；新 connector；exactly-once；MaxCompute 外部认证。
Dependencies: T2.4/T2.6 delivered；T2.3 shared fixtures；现有 Kafka→ClickHouse/PostgreSQL 与 ES template e2e。
Acceptance: T2.7 验收 1-4；四个声明 sink 的共享身份矩阵、可操作 preflight、两条多表故障 e2e 与独立 ES 记录、逐路径版本/日期/结果证据。
Data semantics/rollback: 普通空/部分 Key 一律在 sink 前或 sink 内 fail-closed；legacy_verified 不绕过完整值校验；sink ack/source checkpoint 顺序不变；回滚保留 DLQ 与既有静态 PK 模式。
Evidence: shared conformance、server preflight/metrics tests、Kafka→ClickHouse/PostgreSQL 容器矩阵、ES template 记录、race/full/vet/docs。
Result: delivered
Residual/follow-up: T2.8 迭代收口。
```

T2.7 有界增量：

| 增量 | 可观察结果 | 允许触及 | 验收/故障场景 | 回滚 |
| --- | --- | --- | --- | --- |
| T2.7a shared sink identity conformance | 四个声明 sink 统一接受完整单/复合键与合法改键，拒绝空/部分/冲突和不合格 legacy replay | shared fixture、四 sink 既有 metadata-PK helper/tests | INSERT/UPDATE/DELETE、before key、fan-out、legacy provenance、error class | 保留各 sink 静态 PK 模式，不放松 runner gate |
| T2.7b preflight + observability | 缺 Table/Database/身份能力/目标 PK 均返回可操作 field issue；per-target 指标与 data error 可区分 | server schema/preflight、pipeline/sink metrics/tests | auto-create ColumnTypes 优先、目标路由、拒绝不触发 sink ack | additive diagnostics，可回退展示不回退门禁 |
| T2.7c certified paths | Kafka→ClickHouse 与 Kafka→PostgreSQL 多表路径覆盖 reset/replay、outage、DLQ replay、复合/改键；ES template 单列证据 | 既有 e2e/testdata 与证据文档 | 依赖版本、业务键对账、重复吸收、失败/skip 显式化 | 脚本资源隔离；不提升未测 connector maturity |

T2.7 验收矩阵（2026-09-05，镜像 `openetl-go-etl:dev@sha256:8157cf4e50833cf04a962e0be7165e1f58bd4cefa747a5aeb5ae661fad628840`，`CONTAINER_CLI=podman`）：

| 验收 | 证据 | 结果 | 残留 |
| --- | --- | --- | --- |
| 四个声明 sink 共享身份矩阵（单/复合键、DML、改键、fan-out、空/部分 Key 拒绝、合格 legacy replay） | `TestMetadataPKDeclaredSinksShareFailClosedConformance`、`TestMetadataPKKeyChangingUpdateExpansionUsesValidatedOldKey`、`TestClickHouseMetadataPKKeyChangeProducesSameVersionTombstone`、`TestMySQLMetadataPKKeyChangeDeletesOldKeyInSameTransaction`、`TestPostgresPKFromMetadataConfig`、`TestDorisSchemaInputsCarryMetadataColumnTypes` | passed | Kafka/S3/file 等非行主键 sink 不在声明集内 |
| 身份拒绝为 data error 且不触发 sink ack | `TestMetadataPKIdentityRejectionIsDataErrorAndNotSinkAck` | passed | 无 |
| preflight 可操作错误 + auto-create 优先 ColumnTypes | `TestMetadataPKAutoCreatePrefersDeclaredColumnTypes`、`internal/etl/server/metadata_identity_preflight_test.go`、`TestClickHousePreflight*` | passed | 无 |
| Kafka→ClickHouse 多表容器矩阵（复合键/改键 tombstone/schema drift/outage→DLQ→replay/checkpoint+group reset replay 吸收） | `CONTAINER_CLI=podman E2E_SKIP_BUILD=1 ./hack/e2e-kafka-multitable-clickhouse.sh` | passed | 业务键须稳定路由到同一 partition |
| Kafka→PostgreSQL 多表容器矩阵（复合键/改键旧键删除/衍生 PK 约束/outage→DLQ→replay/reset replay 吸收） | `CONTAINER_CLI=podman E2E_SKIP_BUILD=1 ./hack/e2e-kafka-postgres-fanout.sh` | passed | 修复脚本缺陷：PG stop/start 后 IP 变化，app 容器 `--add-host` 需随重建刷新 |
| ES 模板 fan-out 独立记录（不复用关系型证据） | `TestElasticsearchResolveIndexTemplate`；`CONTAINER_CLI=podman E2E_SKIP_BUILD=1 ./hack/e2e-elasticsearch.sh`（mapping-conflict → DLQ → 修复 → replay → 文档落库） | passed | ES item 级非原子边界维持既有声明 |
| race、全仓与静态门禁 | `go test ./internal/etl/sink ./internal/etl/server ./internal/etl/source -count=1`；`go test ./... -count=1`；`go vet`；`git diff --check` | passed | 无 |

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
