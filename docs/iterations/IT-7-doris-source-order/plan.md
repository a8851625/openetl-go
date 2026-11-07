# IT-7 技术方案

> 本文是实现输入，不是验收标准。验收以 [spec.md](./spec.md) 为准。
> **2026-10-30 修订**：T7.1 实测（[findings.md](./findings.md)）否决了两条原设计假设，本方案已据此收敛。

## 方案总览

复用 ClickHouse 已验证的"源序契约"层，但**只移植写入侧**，删除侧保持 Doris 现状：

| 层 | ClickHouse 现成实现 | Doris 对应物（实测确认后） |
| --- | --- | --- |
| 排序值来源 | `core.SourceOrder(rec).Version` | 同一套（不改 source） |
| 版本列 | `_version` UInt64 + `ReplacingMergeTree(_version,_is_deleted)` | sequence column（`function_column.sequence_col`），类型 **LARGEINT** |
| 写入序保护 | 引擎按 `_version` 合并 | Doris Unique Key 按 sequence 合并 —— **实测成立** |
| 墓碑 | `_is_deleted` UInt8 第二引擎参数 | **不可写入**（实测 4 种写法均无效）→ 保留原生 `DELETE`，其隐式 delete-sign 继承被删行的序列 |
| 主键变更 | `keyChangeTombstone()` 生成旧键墓碑 | 现有 `expandMetadataPKKeyChanges` 写新键 + `DELETE` 旧键（已存在） |
| 存量表保护 | `validateSourceOrderTable` fail-closed | 同模式；属性无法后设 → 只能新表 |
| 能力门禁 | `checkClickHouseSourceOrderCapability` | 泛化后共用 |

被否决的路线（含实测依据）：

- **用 `__DORIS_DELETE_SIGN` 在载荷里表达墓碑**：2.1.11 上 `columns` / `hidden_columns` / `partial_columns` / CSV 四种写法全部 `Status=Success` 但**不删除**，隐藏列甚至不可 `SELECT`/`DESC`。→ 删除仍走 MySQL `DELETE`。
- **`merge_type: DELETE` 头**：实测对已存在行无任何效果。
- **`DECIMAL(20,0)` 作序列列**：建表直接报 `Sequence type only support integer types and date types`。
- **`BIGINT` 作序列列**：写 `2^63` 静默回绕成 `-2^63`（会让"更新的记录"看起来更旧）。
- **存量表 `ALTER ... SET ("function_column.sequence_col")`**：报 `Unknown table property`；`ADD COLUMN ... NOT NULL DEFAULT 0` 是语法错误。
- **改变默认值为 `source_order`**：存量表没有序列列，会让所有既有 Doris 管道启动失败。
- **靠 mow=false + sequence**：写入序比较生效，但 `DELETE` 后旧重放会复活（实测）。

## 分项设计

### A. 语义验证（T7.1，已完成）

结论见 [findings.md](./findings.md)。要点：写侧乱序保护成立；删除保护成立但只覆盖"严格更旧"，且有两个已证明的下界（删除从未存在的行无墓碑、等序后写者胜）。实现不得超出该结论宣称能力。

### B. 配置面与表契约（T7.2）

**现状**：doris 的 `configSchema` 有 22 个字段（`internal/etl/server/schema.go:394`），无序列相关项；`NewDorisSink`（`doris.go:105`）解析配置；auto-create DDL 固定输出 `light_schema_change`（`doris.go:1279`）。

**目标**：新增 3 个字段，默认值确保零行为变化。

| 字段 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- |
| `version_mode` | enum `off`\|`source_order` | `off` | 有序写入开关 |
| `sequence_column` | string | `_version` | 承载 `core.SourceOrder().Version` 的列名 |
| `sequence_type` | enum `largeint`\|`bigint` | `largeint` | 序列列类型；`bigint` 仅 opt-in，越界必须 fail-closed |

构造期校验：

- `version_mode=source_order` + `write_mode=insert` → 错误（INSERT 回退无法保证与 `DELETE` 的相对顺序，且绕过 Stream Load 的 sequence 语义）。
- `version_mode=source_order` + `batch_mode=insert` → 错误（Unique Key 是前提）。
- `version_mode=source_order` + `sequence_column` 为空 / 与 key 列重名 → 错误。
- `version_mode=off` 时不校验其它两项，保持向后兼容。

**关键接口与文件**：

| 文件 | 改动性质 |
| --- | --- |
| `internal/etl/server/schema.go` | 新增 3 字段（doris 块），描述写明 mow + LARGEINT 前提与默认关闭原因 |
| `internal/etl/sink/doris.go` | `DorisSink` 字段 + 解析 + `validateConfig` |
| `internal/etl/server/server.go` | doris capabilities 增补 `source_order_replay`（实现完成并通过 e2e 后才加，避免声明超前） |

**数据语义**：本切片只加配置与校验，不改写入；`version_mode=off` 路径字节级不变。

### C. 写入路径：序列注入（T7.3，**已按实测收缩**）

**现状**：
- `Write`（`doris.go:318`）→ metadata PK 校验 → `expandMetadataPKKeyChanges` → `CompactRecordsByPK` → auto-create → 拆 `deletes`/`writes` → **先写后删**（`doris.go:414-437`）。
- 载荷构造：`buildJSONBody`（`doris.go:659`）、`buildCSVBody`/`csvColumnOrder`（`doris.go:673/687`）。
- 删除：`batchDeleteRecords`（`doris.go:824`）走 MySQL `DELETE`。

**目标（最小切片）**：`version_mode=source_order` 时，在写路径上为每条非 DELETE 记录注入序列列；**删除路径完全不动**。

```
prepareDorisSourceOrder(records) ([]core.Record, error):
  for rec where rec.Operation != OpDelete:
    order := core.SourceOrder(rec)
    if !order.VersionAvailable: return sourceOrderError(rec, order)   // 复用 CH 的字段级提示
    if sequenceType == bigint && order.Version > math.MaxInt64:
        return error("sequence value overflows BIGINT ...; use sequence_type: largeint")
    out += clone(rec) with Data[sequence_column] = order.Version
  delete records pass through untouched
```

要点：

- 序列列名固定取自配置 `sequence_column`；不涉及 `__DORIS_DELETE_SIGN`。
- 删除仍然产生 Doris 隐式 delete-sign（继承被删行序列）——这是删除侧保护的全部来源，**不需要**代码改动。
- `writeViaInsert` 在 source_order 下被 B 段拒绝，故写入统一走 Stream Load。
- CSV 路径：`csvColumnOrder` 需把序列列纳入列序（序列列是普通列，CSV 可正常携带）。
- `doris.go:421` 的混合批拒绝**保持不变**（删除与写入仍不同路径，原子性问题未消除）。

**数据语义**：

| 维度 | source_order 下的行为 |
| --- | --- |
| source position | `core.SourceOrder`（binlog file/pos、LSN、partition+offset、numeric cursor）；无可用序即失败进 DLQ |
| checkpoint 边界 | 不变：sink ack 后由 runner 提交；崩溃后重放由 sequence 收敛 |
| sink acknowledgement | Stream Load `Status=Success` 即确认；label 仍由载荷 hash 生成（`doris.go:578`），同载荷重试天然去重 |
| 重复策略 | 更旧写 = 被 sequence 忽略；同序写 = 后到者胜（实测）；更旧重放不会复活"严格更旧"的已删行 |
| 删除 | 仍走 MySQL `DELETE`；保护边界 = 被删行最后版本，实测见 findings 事实 2 与残窗 1/2 |
| DLQ | 语义不变：写失败按现有 `classifyDorisStreamLoadError` 分类进 DLQ；replay 保留原始 source position，因此仍是有序的 |
| restart/reset | checkpoint reset 全量重放会重写所有行，但终态与重放前一致（本迭代核心收益） |

### D. 自动建表与存量表保护（T7.2 收尾）

**现状**：`buildCreateTableDDLWithPK`（`doris.go:1197`）输出 `UNIQUE KEY` + `light_schema_change`；`validateUniqueKeyModel`（`doris.go:1040`）靠 `SHOW CREATE TABLE` 解析确认 UNIQUE KEY。

**目标**：

1. auto-create 在 source_order 下追加：序列列 `LARGEINT NOT NULL`（或 `sequence_type: bigint` 时 `BIGINT NOT NULL`）+ `PROPERTIES("enable_unique_key_merge_on_write"="true", "function_column.sequence_col"="<col>")`。
2. `validateUniqueKeyModel` 扩展为 `validateSourceOrderTable`，从 `SHOW CREATE TABLE` 文本确认四条：
   - Unique Key 模型；
   - `"enable_unique_key_merge_on_write" = "true"`（实测 mow=false 时删除保护失效）；
   - `"function_column.sequence_col" = "<sequence_column>"`（缺属性或指向别的列都拒绝）；
   - 序列列存在且类型为 `largeint`（或 `bigint` 且用户显式选了 `sequence_type: bigint`）。
3. 任一条不满足 → 返回带迁移步骤的错误：新建序列表 → 回填当前状态与单调序列 → 对账校验 → 原子切换 pipeline → 保留旧表以便回滚。**不得**尝试 `ALTER ... SET`（实测 `Unknown table property`）。

**实现落点（2026-10-30 完成）**：

- `ValidateSchema` 只发一次 `SHOW CREATE TABLE`，同时服务 `validateUniqueKeyStatement`（UNIQUE KEY 与 pk_columns 一致）与 `validateSourceOrderStatement`（source_order 契约），避免启动期重复查询。
- 新增 `DorisSink.ensureSourceOrderTables`，在 `Write` 的 `ensureTablesAndColumns` 之后、写入之前按本批路由表惰性校验，结果缓存在 `sync.Map`（每进程每表一次）。原因是 `ValidateSchema` 在 `table_template` 模式下按设计跳过单表预检（`s.table` 为空即提前返回），若不补这一层，多表扇出会静默退化为 last-write-wins。
- auto-create 的序列列类型**强制**取配置值，不从样本值推断：首批小值会把类型推断成 BIGINT，之后的大序值会被 Doris 静默回绕。

**设计权衡**：不做"加列 + 设属性"的自动迁移。实测属性无法后设；即使能，历史行的序列值也无法从现有数据推导（会与新的 source 序域不可比）。

### E. 能力门禁与 preflight（T7.4）

**现状**：`preflight.go:1056-1124` 只对 ClickHouse 校验 `version_mode`；"哪些源提供持久数值序"的知识内联在名字带 clickhouse 的函数里。

**目标**：

1. `checkClickHouseSourceOrderCapability` → `checkSourceOrderCapability(spec, sinkLabel string, result *PreflightResult)`，错误文案里的 sink 名参数化；ClickHouse 调用点行为与文案等价（现有 `clickhouse_order_preflight_test.go` 不改即通过）。
2. Doris 在 `preflight.go:448` 的 case 中，仅当 `version_mode=source_order` 时调用同一 helper。
3. 静态校验新增：`sequence_column` 非空且不与 key 列重名、`sequence_type` 合法、`write_mode/batch_mode` 组合合法。
4. `/api/v2/specs/validate` 复用同一路径，保证 UI 与 CLI 口径一致。

**数据语义**：纯诊断，不改写入。

### F. e2e 与证据（T7.5）

新增 `hack/e2e-doris-source-order.sh`（T7.1 spike 后收敛为写入侧排序验证，因为 delete-sign 无法随 Stream Load 携带），结构对齐 `hack/e2e-clickhouse-replay-ordering.sh`；探针 `hack/it7-doris-semantics-probe.sh` 已验证下列终态可判定（findings 事实 1/2/5/6）。

| 场景 | 断言 |
| --- | --- |
| 1 旧 UPDATE 后到 | 先提交 v2(seq2)，再 replay v1(seq1) → 查询为 v2 |
| 2 删除保护 | 行 seq=100 被 DELETE 后 replay seq=50 → 空；再写 seq=101 → 复活为新值 |
| 3 DLQ 延迟 replay | 故障注入使 v1 进 DLQ → 提交 v2 → 修好 → replay v1 → 终态 v2，DLQ 行被清理 |
| 4 主键变更 | key 1→2 → key 1 无行、key 2 为新值 |
| 5 checkpoint reset | reset 后全量重放 → 终态与重放前逐行一致（对账查询） |
| 6 关闭模式回归 | `version_mode=off` 的同一管道行为与现基线一致 |
| 7 uint64 全域 | Kafka 大 offset / binlog pos 级序列值精确落盘，不回绕 |

脚本固定 `DORIS_VERSION=2.1.11`；`source_order_replay` case 追加到 `internal/etl/server/evidence/connector-evidence.json` 的 doris 记录。

## 架构约束

- 不引入新的 source、新的外部服务、新的执行模型；序列值必须来自 `core.SourceOrder`。
- 不改 `version_mode=off` 的任何行为、默认值、错误文案。
- 不改 Stream Load label 生成规则（它现在就是重试去重的一部分）。
- 不试图写入 `__DORIS_DELETE_SIGN`（实测无效）；删除路径保持 MySQL `DELETE`。
- 不在 sink 内做跨表/跨批事务；不承诺跨 sink 原子性。

## 风险与回滚

| 风险 | 影响 | 缓解 | 回滚路径 |
| --- | --- | --- | --- |
| ~~R1 delete-sign 语义不符~~ | — | **已实测：不支持载荷写入**，方案改为保留 `DELETE`，风险消除 | — |
| ~~R2 BIGINT 无法容纳 uint64~~ | — | **已实测：BIGINT 静默回绕**；默认 `LARGEINT`，`bigint` opt-in + 越界 fail-closed | 用户改 `sequence_type` 或保持 `off` |
| ~~R3 部分列载荷被拒~~ | — | 不再需要部分列载荷（墓碑不由 sink 写） | — |
| R4 自动建表属性写错列名 | 序列不生效且不报错（最危险） | `validateSourceOrderTable` 解析 `SHOW CREATE TABLE` 校验属性指向；e2e 用"旧写不覆盖"断言证明生效 | 修复属性或回落 `off` |
| R5 存量管道误开 source_order 而表不满足契约 | 启动失败 | fail-closed + 迁移文案；默认值仍是 `off` | 改回 `off` 即恢复 |
| R6 用户用 mow=false 的表 | 删除保护静默失效（旧重放复活已删行） | `validateSourceOrderTable` 强制 mow=true 并 fail-closed | 重建为 mow=true 或保持 `off` |
| R7 用户期望"删除后永不复活" | 两个下界造成预期落差 | 组件文档与 spec 验收 12 强制写明残窗；e2e 场景 2 用严格更旧值断言 | 文档已声明，无需回滚 |
| R8 与 `allow_mixed_cdc_non_atomic` 语义冲突 | 老配置在新模式下行为变化 | 该校验在两种模式下**行为一致**（本迭代不改） | 无 |

## 测试策略

| 层级 | 范围 | 命令 |
| --- | --- | --- |
| 1 静态与单测 | 配置解析/校验、`prepareDorisSourceOrder`、bigint 越界、CSV 列序、DDL 生成、`validateSourceOrderTable`（`SHOW CREATE TABLE` fixture） | `go test ./internal/etl/sink -run Doris -count=1`；`gofmt -l`；`go vet ./internal/etl/sink ./internal/etl/server` |
| 2 包级 / race | sink + server | `go test ./internal/etl/sink ./internal/etl/server -count=1`；`go test -race ./internal/etl/sink ./internal/etl/server -count=1` |
| 3 后端矩阵 | 与 storage backend 无关（sink 直连 Doris） | 跳过（说明理由） |
| 4 容器 e2e | 新增排序 e2e + 既有 doris 脚本不回归 | `CONTAINER_CLI=podman ./hack/e2e-doris-source-order.sh`；`./hack/e2e-doris.sh`；`E2E_SKIP_BUILD=1 ./hack/e2e-doris-table-template.sh` |
| 5 外部认证 | Doris 真实 FE/BE 容器即满足（无需外部凭据）；证据回写 manifest | `grep source_order_replay internal/etl/server/evidence/connector-evidence.json`；`./hack/check-connector-evidence.sh` |
