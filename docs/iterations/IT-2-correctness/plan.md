# IT-2 技术方案

> 本文是实现输入，不是验收标准。验收以 [spec.md](./spec.md) 为准。

## 方案总览

四组缺口按「真值归属」分成两条互不阻塞的技术线：

- **控制面线**（RA-2 → RA-3）：让持久化状态成为运行时的输入而不只是输出。当前
  `RestoreFromDB` / `StartAll` 是纯写方向 —— 状态只被写出，从不被读回作为决策依据。
- **数据面线**（共享契约层 → RA-1 → GAP-7.1/.2/.3）：先建立一个 **record identity &
  ordering 契约层**，再让 ClickHouse 版本列、各 identity-aware sink、DLQ/replay 共同消费它。

两条线的关键约束是**顺序**：数据面必须先交付共享契约层，再改各消费方。否则 RA-1 会在
`clickhouse.go` 里长出一套源序推导，GAP-7 又在 `kafka.go` 与 `metadata_pk.go` 里长出另一套，
第二次交付会推翻第一次的验收。

## 分项设计

### A. RA-2：restore 失败可见

**现状**：`internal/etl/server/server.go:474-572`。11 处 `continue` + `Warningf`，
函数末尾 `return nil`。

**目标**：跳过原因成为持久化的、可查询的 pipeline 状态。

**关键接口与文件**：

| 文件 | 改动性质 |
| --- | --- |
| `internal/etl/server/server.go` | `RestoreFromDB` 收集失败明细，写入 `restore_failed` 状态 + 结构化错误 |
| `internal/etl/storage/` | pipeline row 增加 `restore_error`（阶段、错误码、remediation）字段与迁移 |
| `internal/etl/server/` health / 列表 handler | 暴露 `restore_failed` 与错误详情 |
| `internal/etl/server/runtime_profile.go` | production profile 的 strict 开关 |
| `web/src/` | 列表与 health 面板展示 `restore_failed` 与修复提示 |

**错误分类**（六类，与验收 1 对应）：`dag_yaml_parse`、`linear_yaml_parse`、
`dag_connection_resolve`、`linear_connection_resolve`、`spec_validate`、`runner_build`。
每类携带可操作 remediation 文本。

**数据语义**：不触碰 checkpoint 与 DLQ。`restore_failed` 是控制面状态，不改变该 pipeline
的 source position —— 修复后重启必须从原 checkpoint 继续，不得被当作首次启动。**这是本项
最关键的语义约束**：一个 restore 失败若导致 checkpoint 被视为空，就把可见性缺陷升级成了
数据缺陷。

**设计权衡**：

| 备选 | 结论 | 理由 |
| --- | --- | --- |
| 启动直接失败（无论 profile） | 否决 | 一条坏 spec 会阻断全部 pipeline，运维体验倒退 |
| 仅记录日志 + metrics | 否决 | 不满足「API 与 health 可见」；日志不是控制面真值 |
| **持久化 `restore_failed` + production strict 可选 fail-closed** | 采纳 | development 保持宽松，production 可 fail-closed，两种需求都覆盖 |

### B. RA-3：desired state 与 fencing

**现状**：`server.go:734-795`。`StartAll` 只写状态不读；`row.Status` 仅在 `:488`
`SaveCurrentWithID` 补写生成 ID 时透传。

**目标**：区分 **desired state**（用户意图：`running` / `stopped` / `paused`）与
**observed state**（实际运行结果：`running` / `completed` / `failed` / `scheduled` /
`restore_failed`）。

**关键接口与文件**：

| 文件 | 改动性质 |
| --- | --- |
| `internal/etl/storage/` | pipeline row 分列 `desired_state` 与 `observed_state` + 迁移（旧单列按语义拆分） |
| `internal/etl/server/server.go` | `StartAll` 以 `desired_state == running` 为启动门禁；start/stop/pause/resume 写 desired 并对失败返回非 2xx |
| `internal/etl/server/server.go` | checkpoint reset/set 要求 stopped/paused，或走 generation fencing |
| `internal/etl/checkpoint/` | checkpoint 记录 generation；旧 generation 的写入被拒绝 |
| `docs/openapi.yaml`、`docs/etl-api.md` | reset 语义按 source 类型分别声明 |

**迁移语义**（必须写明，否则升级会改变用户可见行为）：旧单列 `status` 拆分时，
`running` / `scheduled` → desired `running`；`stopped` / `paused` → desired 同值；
`failed` / `completed` → desired `running`（它们是 observed 结果，不表达停止意图）。
该映射需在迁移测试中固化。

**generation fencing 机制**：pipeline 每次进入运行态分配一个单调递增 generation；
checkpoint 写入携带该 generation；store 侧以 CAS 语义拒绝低于当前 generation 的写入。
这样 reset 后旧 runner 的 in-flight checkpoint 无法覆盖 reset 结果。

**source-specific reset 语义**（验收 7）：

| Source | reset 实际语义 | 必须在 API/文档声明 |
| --- | --- | --- |
| Kafka | 内部 checkpoint 清除，但实际起点仍受 broker consumer-group offset 约束 | 需同时重置 consumer group 才等价「从头」 |
| MySQL CDC | 无 checkpoint 时从当前 master position 开始 | reset ≠ 从最早 binlog，中间变更会丢 |
| MySQL snapshot+CDC | 回到 snapshot 阶段起点 | 全量重拉的代价与重复边界 |
| PostgreSQL CDC | 受 replication slot 已确认位置约束 | 早于 slot 确认位置的 WAL 已不可得 |

**参考实现**：Debezium 的 signal table（在线触发 incremental snapshot / re-snapshot）可作为
source-specific reset 的成熟设计参考，也与 BUG-2 的 `resnapshot` 策略共用形态。

### C. 共享契约层：record identity & ordering

**这是数据面的地基，必须先于 RA-1 与 GAP-7 的消费方交付。**

**目标**：在 `core` 或相邻包提供两组能力，供所有 sink / DLQ / replay 共同消费：

```text
1. SourceOrder(record) -> (ordinal int64 | multi-column version, ok bool)
     mysql_cdc / mysql_snapshot_cdc  : binlog file seq << N | pos
     postgres_cdc                    : LSN
     kafka                           : (partition, offset) -> 单调映射
     mysql_batch / snapshot 相位      : 已确认游标
     file / http / redis             : ok = false（无全序能力）

2. RecordIdentity(record) -> (key map[string]any, complete bool, reason string)
     完整性 = 覆盖全部声明 pkNames 且各分量非空
     UPDATE 的 Key 表达 before-image 旧行身份，Data 保持 after-image
```

**关键设计点**：

- `ok = false` 与 `complete = false` 是**一等返回值**，不是错误也不是零值。调用方必须显式
  处理，禁止 `_` 忽略。用类型设计强制（返回结构体而非裸值）。
- 源序映射必须**跨进程重启单调**，且不依赖 wall clock。binlog 的 `(file序号, pos)`、
  PG 的 LSN、Kafka 的 `(partition, offset)` 本身就单调，映射函数只做位宽压缩，不引入时间。
- 位宽分配需文档化并有边界测试（binlog file 序号溢出、Kafka partition 数上限）。

**设计权衡**：

| 备选 | 结论 | 理由 |
| --- | --- | --- |
| 各 sink 自行从 Metadata 推导 | 否决 | 正是当前的问题形态，会导致语义分叉 |
| 用 wall clock + 计数器（现状） | 否决 | 表达写入时刻而非事件时刻，RA-1 的根因 |
| 要求所有 source 提供全序 | 否决 | append-only source 无法满足，会导致虚构版本 |
| **共享契约层 + 显式 not-ok 语义 + preflight 阻断** | 采纳 | 有能力的路径用真源序，无能力的路径被拒绝而非降级 |

### D. RA-1：ClickHouse 版本列消费源序

**现状**：`internal/etl/sink/clickhouse.go:1210` `nextVersion()`；调用于 `:879`、`:911`。

**目标**：版本列取自 C 的 `SourceOrder`；`ok = false` 时不写入，由 preflight 提前拒绝该组合。

**关键文件**：`internal/etl/sink/clickhouse.go`（版本写入路径）、`metadata_pk.go`（与身份契约对齐）、
`internal/etl/server/schema.go` + preflight（组合校验）、`docs/components/sink-clickhouse.md`。

**数据语义**：

- checkpoint 边界、sink acknowledgement 顺序**不变**，本项只改版本列取值。
- DELETE 走物理 mutation 时，旧 INSERT 的重放仍可能重生成行 —— 该边界由源序版本 +
  `is_deleted` 标记或 mutation 顺序约束处理，需在 `etl-idempotency.md` 明确记录残余。
- 无法建立全序时的 duplicate / reconciliation 边界写入文档，而不是靠实现兜底。

### E. GAP-7.1/.2/.3：身份契约与受控 replay

沿用 ROADMAP GAP-7 的统一数据契约五条，此处只补技术落点：

| 切片 | 主要文件 | 要点 |
| --- | --- | --- |
| GAP-7.1 正常 DML 完整 Key | `internal/etl/source/kafka.go`（`tryCanalJSON`）、共享 identity helper、source/sink descriptor 与 spec validation | 消除「至少一列存在即非空」导致的部分复合 Key；identity-aware sink 与无身份 source 的组合在 preflight 拒绝 |
| GAP-7.2 DLQ 身份上下文与门禁 | DLQ entry/storage + 迁移、replay API/worker、format-contract registry | DLQ 入库时**冻结** format-contract ID，replay 不得用当前配置猜测历史事件身份 |
| GAP-7.3 sink 一致性与跨路径认证 | 各 metadata-PK sink 接入点、shared conformance fixture、per-target metrics 与 `error_class` | 共享 conformance 逐 sink 通过；auto-create 优先消费 `Metadata.ColumnTypes` |

**上线顺序（交付约束 2 的实现）**：

```text
1. 部署识别能力（只观测、不拒绝）：统计现存空 Key / 部分 Key 事件与 DLQ 存量
2. 确认存量可控后，切换正常流为 fail-closed
3. 受控修复历史 DLQ（可重建者补全 Key 后 replay；不可证明者 quarantine）
4. 静态 PK 兼容保持可撤销，但关闭它不得使正常流重新接受无身份 DML
```

## 架构约束

- 不引入通用 keyed state、任意 timer、Flink savepoint 或 SQL planner。
- 不新增外部基础设施依赖。
- 共享契约层放在 `core` 或其相邻包，**不得反向依赖** `sink` / `source` 具体实现。
- 不改变 `Source -> Transform -> Sink` 契约形态。
- distributed 相关的 fencing 不在本迭代（属 PR-D1）。

## 风险与回滚

| 风险 | 影响 | 缓解 | 回滚路径 |
| --- | --- | --- | --- |
| 版本列语义变更导致既有 ClickHouse 表新旧版本混存 | 迁移期 `FINAL` 结果不确定 | 新旧版本可比较性分析；必要时提供一次性重写工具或按表切换开关 | 按表回退到旧版本列取值 |
| desired/observed 拆列迁移误判用户意图 | 升级后 pipeline 意外启动或意外不启动 | 映射规则固化为迁移测试；升级前后状态快照比对 | 迁移可逆；保留旧列一个版本周期 |
| fail-closed 切换后大量正常流进 DLQ | 生产链路中断 | 强制两阶段上线（先观测后拒绝）；提供存量识别报告 | 配置开关退回观测模式 |
| 共享契约层设计不足，后续 sink 无法复用 | 二次重构 | 先用 ClickHouse + PostgreSQL 两个形态各异的 sink 验证抽象 | 契约层可加字段，不破坏既有调用 |
| RA-2 的 `restore_failed` 被误实现为清空 checkpoint | 可见性缺陷升级为数据缺陷 | 显式测试：restore 失败 → 修复 → 重启 → 从原 checkpoint 续跑 | 立即回退该 commit |

## 测试策略

| 层级 | 范围 | 命令 |
| --- | --- | --- |
| 1 静态与单测 | 契约层、错误分类、迁移映射 | `go test ./internal/etl/core/... ./internal/etl/server/...` |
| 2 包级 / `-race` | 生命周期、fencing、并发版本派生 | `go test -race ./internal/etl/server/... ./internal/etl/pipeline/... ./internal/etl/sink/...` |
| 3 后端矩阵 | desired/observed 迁移在 SQLite / MySQL / PostgreSQL | 三 backend conformance |
| 4 容器 e2e | 乱序 replay 矩阵、跨路径 metadata-PK 认证、stop→重启→无写入 | IT-1 框架：`go test -tags=e2e ./internal/etl/e2e/ -run 'Ordering|Identity|Lifecycle' -e2e.strict` |
| 5 外部环境认证 | 不适用 | — |

**必须包含的故障场景**（AGENTS.md 步骤 6 的数据路径要求）：sink ack 后 checkpoint 提交前崩溃、
应用重启与 checkpoint reset/replay、source/sink/Redis 中断与 DLQ replay、重复吸收边界、
schema drift 与 delete/update 语义、按业务键/版本对账。
