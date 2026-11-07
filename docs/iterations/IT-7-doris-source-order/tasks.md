# IT-7 任务分解

> 状态取值：`todo` / `active` / `done` / `blocked`。一次只允许一个 `active`。
> 默认执行预算 5 轮（见 [AGENTS.md](../../../AGENTS.md) 的 round limit）；本迭代按 5 轮切分。

## Round 分组

| Round | 包含 task | 目标 |
| --- | --- | --- |
| 1/5 | T7.1 | 在真实 Doris 2.1.11 上证明 sequence/delete-sign 语义，冻结实现分支（**已完成**：主路径成立、范围收缩） |
| 2/5 | T7.2 | 配置面 + 构造期校验 + 自动建表序列契约 + 存量表 fail-closed（**已完成**） |
| 3/5 | T7.3 | 写入路径：序列注入（**不含墓碑**，删除路径不动；`off` 路径零改动）（**已完成**） |
| 4/5 | T7.4 | preflight/validate 能力门禁（泛化 CH helper）+ 单测收口（**已完成**） |
| 5/5 | T7.5 | 排序 e2e + 认证 case 回写 + 文档/CHANGELOG/ROADMAP 收口 —— 完成 |

## 任务表

| ID | 任务 | 依赖 | 状态 | 证据落点 |
| --- | --- | --- | --- | --- |
| T7.1 | Doris 序列/墓碑语义实测与设计冻结 | — | `done` | `docs/iterations/IT-7-doris-source-order/findings.md` |
| T7.2 | 配置面与表契约（schema/DDL/校验） | T7.1 | `done` | `internal/etl/sink/doris_test.go`、`internal/etl/server/schema.go` |
| T7.3 | 写入路径序列注入（不含墓碑） | T7.1, T7.2 | `done` | `internal/etl/sink/doris_source_order_write_test.go` |
| T7.4 | preflight/validate 能力门禁 | T7.2 | `done` | `internal/etl/server/doris_source_order_preflight_test.go` |
| T7.5 | 排序 e2e、认证回写与文档收口 | T7.2, T7.3, T7.4 | `done` | `hack/e2e-doris-source-order.sh`（实跑 PASS）、`connector-evidence.json`（doris 记录新增 `source_order_replay`） |

## 任务明细

### T7.1 Doris 序列/墓碑语义实测与设计冻结

**状态**：`done`（2026-10-30）

**实测结果**（详证见 [findings.md](./findings.md)，原始日志 `data-it7/t7.1-spike*.log`）：

1. ✅ sequence column 阻止旧写覆盖新写（`INSERT` 与 Stream Load 两条路均验证）。
2. ⚠️ 删除保护成立但只覆盖「严格更旧」；存在两个已证明下界（删除从未存在的行无墓碑、等序后写者胜）。
3. ❌ `__DORIS_DELETE_SIGN` 无法通过 Stream Load 写入（`columns` / `hidden_columns` / `partial_columns` / CSV 四种写法均无效，隐藏列甚至不可 SELECT/DESC）。
4. ❌ 存量表无法后设 `function_column.sequence_col`（`Unknown table property`），`ADD COLUMN ... DEFAULT` 也不被语法接受。
5. 额外发现：`DECIMAL` 不能做序列列；`BIGINT` 写 `2^63` 静默回绕成负数；`LARGEINT` 可精确容纳 `uint64`；`mow=false` 时删除保护失效。

**设计冻结**：删除继续走 MySQL `DELETE`；**不**实现墓碑载荷；序列类型默认 `LARGEINT`；强制 `mow=true`；存量表 fail-closed + 新表迁移；两条残窗文档化。

**探针脚本**：`hack/it7-doris-semantics-probe.sh`（`CONTAINER_CLI=podman sh hack/it7-doris-semantics-probe.sh`，一条命令重建 FE/BE 并复现全部结论）。

### T7.2 配置面与表契约（schema/DDL/校验）

**验收**：

1. `configSchema` doris 块新增 `version_mode`(默认 `off`)、`sequence_column`(默认 `_version`)、`sequence_type`(默认 `largeint`，可选 `bigint`)，描述写明 merge-on-write 前提与默认关闭原因。
2. `NewDorisSink` 解析三字段并做构造期组合校验：`source_order` + `write_mode=insert` 报错；`source_order` + `batch_mode=insert` 报错；`source_order` + `sequence_column` 空或与 key 列重名报错。
3. `buildCreateTableDDLWithPK` 在 `source_order` 下输出序列列（默认 `LARGEINT NOT NULL`）、`enable_unique_key_merge_on_write=true`、`function_column.sequence_col`；`off` 下 DDL 与当前逐字一致（快照断言）。
4. 新增 `validateSourceOrderTable`：解析 `SHOW CREATE TABLE`，缺属性 / 属性指向别的列 / mow!=true / 序列列类型不符时 fail-closed，错误含「新建表 + 回填 + 切换」迁移步骤，**不得**建议 `ALTER ... SET`。
5. `go test ./internal/etl/sink -run Doris -count=1` 全绿；`off` 路径无回归。

**证据落点**：

- `internal/etl/sink/doris_test.go`（DDL 快照、组合校验、`SHOW CREATE TABLE` fixture）
- `internal/etl/server/schema.go`
- `docs/etl-config-schema.md`（三字段表 + `source_order` 专节已在 T7.2 落地）

**状态**：`done`（2026-10-30）

**实现要点**：

1. `DorisSink` 新增 `versionMode` / `sequenceColumn` / `sequenceType`，默认 `off` / `_version` / `largeint`；`NewDorisSink` 末尾调用 `validateSourceOrderConfig`（仅 `source_order` 生效，`off` 路径完全不变）。
2. 构造期拒绝：未知 `version_mode` / 未知 `sequence_type` / 显式空 `sequence_column` / 与 `pk_columns` 重名 / 使用保留名 `__DORIS_DELETE_SIGN` / `write_mode: insert` / `batch_mode != upsert`（后两者会绕过 UNIQUE KEY + merge-on-write 校验）。
3. `buildCreateTableDDLWithPK`：`source_order` 下注入序列列（类型强制为配置值，**不**从样本值推断，避免首批小数据推断成 BIGINT 后被大值回绕）并追加 `enable_unique_key_merge_on_write` + `function_column.sequence_col`；`off` 路径逐字不变（测试做了全字符串比对）。
4. `validateSourceOrderStatement`（纯函数）：校验 UNIQUE KEY 模型、序列列不在 key 内、`enable_unique_key_merge_on_write=true`、`function_column.sequence_col` 指向配置列、序列列类型可容纳配置的序域；每类失败都附带「新建表 + 回填 + 对账 + 切换」迁移路径，并明确写出 `ALTER TABLE ... SET` 不可用。
5. `ValidateSchema` 改为一次 `SHOW CREATE TABLE` 同时服务 UNIQUE KEY 与 source_order 两个契约；新增 `ensureSourceOrderTables` 在 `Write` 前按路由表惰性校验（`sync.Map` 每进程每表一次），覆盖 `table_template` 扇出场景（该场景 `ValidateSchema` 按设计跳过单表预检），避免静默退化为 last-write-wins。
6. `internal/etl/server/schema.go` doris 块新增三字段，`schema_test.go` 的 `assertFields` 同步；`docs/etl-config-schema.md` 增字段表 + `source_order` 专节（含四条已实测边界）。

**证据**：

- `go test ./internal/etl/sink/ -run Doris -count=1` → ok（含新增 `TestDorisSourceOrderConfigValidation`（9 子测）、`TestDorisCreateTableDDLSourceOrder`、`TestDorisValidateSourceOrderStatement`（9 子测）、`TestDorisValidateSchemaEnforcesSourceOrderContract`（2 子测）、`TestDorisWriteEnforcesSourceOrderTableContract`、`TestDorisEnsureSourceOrderTablesCachesSuccess`、`TestDorisParseTablePropertiesAndColumnType`）
- `go test ./internal/etl/sink/... -count=1` → 全绿；`go test -race ./internal/etl/sink/ ./internal/etl/server/ -count=1` → 全绿
- `go test ./internal/etl/... -count=1` → 24 包 ok / 0 FAIL
- `go vet ./internal/etl/sink/` → clean；`git diff --check` → clean

### T7.3 写入路径序列注入（不含墓碑）

**验收**：

1. `prepareDorisSourceOrder`：为非 DELETE 记录注入 `sequence_column`；`core.SourceOrder` 不可用时报错，文案复用 ClickHouse 的字段级提示（binlog_file/pos、lsn、partition+offset、numeric cursor）。
2. `sequence_type=bigint` 且序列值 > `MaxInt64` 时**写入前**报错（不得静默回绕）；默认 `largeint` 无此限制。
3. DELETE 记录**原样透传**，`batchDeleteRecords` 不改；删除侧的序保护来自 Doris 隐式 delete-sign（实测继承被删行序列）。
4. `doris.go:421` 的混合批拒绝行为不变（两种模式一致）。
5. `writeViaInsert` 不参与有序模式（构造期已拒绝）；`version_mode=off` 的写入路径字节级不变。
6. 单测覆盖：序列注入、bigint 越界、DELETE 透传、`SourceOrder` 缺失、CSV 列序含序列列。

**证据落点**：

- `internal/etl/sink/doris_test.go`（新增 `TestDorisSourceOrder*` 系列）
- `go test ./internal/etl/sink -count=1`、`go test -race ./internal/etl/sink -count=1`

### T7.4 preflight/validate 能力门禁

**验收**：

1. `checkClickHouseSourceOrderCapability` 泛化为 `checkSourceOrderCapability(spec, sinkLabel, result)`；ClickHouse 行为与文案等价，`clickhouse_order_preflight_test.go` 不修改即通过。
2. doris 在 `version_mode=source_order` 时调用同一 helper：`file`/`http`/`redis` 源 → error；`mysql_cdc`/`mysql_snapshot_cdc`/`kafka`/`mysql_batch`(numeric) → pass。
3. 静态校验：`sequence_column` 为空、与 key 列重名、`sequence_type` 非法 → field error。
4. `/api/v2/specs/validate` 与 preflight 结论一致（同一 helper）。
5. `go test ./internal/etl/server -count=1` 全绿。

**证据落点**：

- `internal/etl/server/pipelines_preflight_test.go`（新增 Doris source_order 用例）
- `internal/etl/server/clickhouse_order_preflight_test.go`（不改动，作为等价性证据）

### T7.5 排序 e2e、认证回写与文档收口

**验收**（已全部达成，2026-11-01）：

1. 排序 e2e 落地为 `hack/e2e-doris-source-order.sh`（不是最初设想的
   （最初设想的 `e2e-doris-replay-ordering.sh`）：spike 已证明 delete-sign 无法随 Stream Load 携带，
   故 e2e 用"真实 mysql_cdc → Doris"验证写入侧排序，Stale 写入直接打 BE:8040 复现
   迟到旧序），固定 `DORIS_VERSION=2.1.11`，`CONTAINER_CLI=podman` 实跑退出码 0，三条断言全 PASS：
   行落库 `_version=9223372049739678695`（注入的真实 binlog 位点，非列默认 0）、更新后序列递增、
   `_version=1` 的迟到写入被拒（行保持 `v2`）。
2. `hack/e2e-doris.sh` 与 `hack/e2e-data-integrity.sh` 未回归（同一镜像实跑通过）。
3. `connector-evidence.json` 的 doris 记录追加 `source_order_replay` case 与脚本条目，16 条记录整体
   重新认证（2026-11-03），`./hack/check-connector-evidence.sh -strict -commit <HEAD> -image <digest>` 通过。
4. `docs/components/sink-doris.md` 新增 "Source-order writes (IT-7)" 小节：写模式、序列契约
   （`LARGEINT` + mow=true）、存量表迁移步骤、preflight fail-closed 条件，并写明四条残余窗口
   （删除保护仅覆盖严格更旧、删除从未存在的行不留墓碑、等序后写者胜、主键变更旧键在同序重放下可能复活）。
5. `docs/etl-config-schema.md` 已含 `version_mode`/`sequence_column`/`sequence_type`；`CHANGELOG(.zh).md`
   与 `docs/ROADMAP.zh.md` 已回填证据并把 IT-7 置为 `delivered`。
6. `git diff --check` 通过；无 secret/生成产物进入错误位面。

**证据落点**：

- `hack/e2e-doris-source-order.sh` 输出摘要（含终态 `_version` 查询结果）
- `internal/etl/server/evidence/connector-evidence.json`
- `docs/components/sink-doris.md`、`docs/etl-config-schema.md`、`CHANGELOG(.zh).md`、`docs/ROADMAP.zh.md`

## 领取记录模板

```text
Round: <n>/5
Roadmap item: IT-7 (IT-7/T7.<n>)
Profile/path: standalone / connector path (mysql_cdc|mysql_snapshot_cdc|kafka -> doris)
Objective: <one observable outcome>
Scope: <files/components allowed>
Non-goals: 不改变 version_mode=off；不做跨 sink 原子性；不新增 connector；不改 source
Acceptance: <numbered checks>
Evidence: <commands, e2e, docs>
Result: <delivered|active|blocked_external>
Residual/follow-up: <bounded next item or none>
```
