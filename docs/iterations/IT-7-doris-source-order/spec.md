# IT-7：Doris sink 源序写入（sequence column + delete sign）

> 状态：`queued` | 依赖：无（Doris sink 已 `production`；`core.SourceOrder` 契约已在 ClickHouse 侧落地） | 归属 roadmap 条目：IT-7

## 问题陈述

Doris sink 的 replay 安全性只覆盖"同值重复写幂等"，不覆盖"乱序写"。可追溯事实：

1. **无源序版本概念**。`internal/etl/sink/doris.go` 中不存在 `sequence` / `source_order` / `delete_sign` / `SourceOrder` 的任何引用；全仓 `core.SourceOrder`（`internal/etl/core/record_contract.go:92`）的消费方只有 `internal/etl/sink/clickhouse.go`、`internal/etl/server/preflight.go`、contracttest fixtures。
2. **删除没有可比较的版本**。`DorisSink.batchDeleteRecords`（`doris.go:824`）走 MySQL 协议 `DELETE FROM ... WHERE pk IN (...)`，表上没有任何序列表述可比较；因此在**当前无 sequence 列的现状下**，行被删除后任何更旧但更晚到达的 INSERT/UPDATE（延迟 DLQ replay、checkpoint reset 重放、并发 replay）都会把该行**写回来**，产生"已删除数据复活"。（T7.1 实测：Doris 的 `DELETE` 在带 sequence 列的 merge-on-write 表上会产生继承被删行序列的 delete-sign 行，从而挡住"严格更旧"的重放 —— 这正是本迭代要打开的能力。）
3. **UPDATE 无版本比较**。Stream Load（`doris.go:550`）与 INSERT 回退（`doris.go:711`）只按 Unique Key 覆盖，后写者胜。因此更旧的记录后到会**覆盖更新的状态**（lost update）。
4. **混合写/删批默认被拒绝**。`doris.go:421` 因 Stream Load 与 MySQL DELETE 不原子而报错，要求显式 `allow_mixed_cdc_non_atomic`（`doris.go:230`）。T7.1 实测已确认 delete-sign 无法通过 Stream Load 写入，因此本迭代**不**试图解除该限制，只在组件文档里如实说明其存在原因。
5. **能力元数据已对外声明** `unique_key_replay_safe`（`internal/etl/server/server.go:4534`）且 maturity 为 `production`，但上述乱序风险在 `docs/components/sink-doris.md` 的 "Checkpoint, DLQ, Idempotency" 段落里只以"非原子、旧行可能残留"表述，**没有说明"旧记录可以覆盖新状态"**。声明与证据之间存在可被用户误解的缺口。

后果（可观察）：把 MySQL CDC → Doris 跑在生产上，只要出现一次延迟 replay（DLQ 修复后 replay、checkpoint reset、crash 重放、并发 replay），Doris 侧就可能长期保留**旧值**或**已删除行**，而 ClickHouse 路径在同场景下是收敛的。同一份 spec 语义在两个 OLAP sink 上不一致。

对比参照：`internal/etl/sink/clickhouse.go:700` 的 `prepareSourceOrderRecords` 用 `core.SourceOrder` 注入 `_version`（UInt64）与 `_is_deleted`（UInt8），配双参数 `ReplacingMergeTree(_version,_is_deleted)`；`clickhouse.go:1505/1532` 对不兼容的存量表 fail-closed；`hack/e2e-clickhouse-replay-ordering.sh` 有"旧 INSERT 不得复活已删除行""旧 UPDATE 不得覆盖新 UPDATE"的实测。Doris 侧缺的就是这一层。

## 可观察结果

> 本节已经过 T7.1 实测校正（见 [findings.md](./findings.md)）：原计划的"墓碑走 Stream Load"与"混合批放行"两条被实测否决，已移除；新增的两条残窗已文档化。

1. 运维能在一个 Doris Unique Key 表上开启源序写入（`version_mode: source_order`），之后**更旧的记录后到会被忽略**，不再覆盖更新的状态。
2. 已删除的行**不会被严格更旧的重放复活**；删除仍由 Doris 原生 `DELETE` 表达（delete-sign 行由 Doris 隐式产生，其序列继承被删行当时的版本）。
3. 主键变更的 UPDATE 不会在旧键上留下陈旧行（旧键被 `DELETE` 墓碑化），也不会丢失新键。
4. 关闭该模式（`version_mode: off`，默认）时，行为与当前完全一致，存量管道零迁移。
5. 源不提供持久数值序（file/http/redis、文本游标、窗口聚合）时，preflight 与 validate **明确 fail-closed**，而不是运行时静默降级。
6. 存量表缺 sequence 属性、或未开 merge-on-write 时 fail-closed，并给出可执行的迁移说明（**新建表 + 回填 + 切换**；实测证明属性无法后设）。
7. 序列列类型默认 `largeint`，能精确容纳 `uint64` 全域（MySQL binlog pos、Kafka offset）；`bigint` 仅作为显式 opt-in 并在越界时 fail-closed（实测：BIGINT 会静默回绕成负数）。

## 验收标准

| # | 验收标准 | 判定方式 |
| --- | --- | --- |
| 1 | 旧 UPDATE 后到不覆盖新 UPDATE | `hack/e2e-doris-source-order.sh` 断言 3：真实 mysql_cdc 写入 v2 后，直接向 BE:8040 灌入 `_version=1` 的旧行，Doris `SELECT` 仍为 v2 |
| 2 | 严格更旧的重放不复活已 DELETE 的行 | 同脚本场景 2：行 seq=100 被 DELETE 后重放 seq=50，该 key 查询为空；再写 seq=101 能正常复活 |
| 3 | 旧 DLQ replay 不回归已提交的新状态 | 同脚本场景 3：故障注入把旧事件打进 DLQ → 提交新事件 → 修好后 replay，终态为新事件 |
| 4 | 主键变更 UPDATE 无陈旧旧键行 | 同脚本场景 4：key 1→2 后 key 1 不存在、key 2 为新值 |
| 5 | `version_mode=off` 行为不回归（含混合批仍按原规则拒绝） | 现有 `hack/e2e-doris.sh` 全绿 + `doris_test.go` 全绿；`off` 下 `allow_mixed_cdc_non_atomic` 校验与今日逐字一致；默认值确认为 `off` |
| 6 | 无持久序源 fail-closed | 单测 + `/api/v2/specs/validate`：`file`/`http`/`redis` source + `version_mode=source_order` 返回 error 并指名字段 |
| 7 | 有持久序源放行 | `mysql_cdc`/`mysql_snapshot_cdc`/`kafka`/`mysql_batch`(numeric cursor) 通过 preflight |
| 8 | 存量表/错误表契约 fail-closed | 单测：缺 `function_column.sequence_col`、`enable_unique_key_merge_on_write=false`、序列列类型不可容纳 `uint64` 时 `ValidateSchema`/`Open` 报错并含迁移文案（**不得**尝试 `ALTER ... SET`，实测不支持） |
| 9 | 自动建表带序列契约 | 单测 + e2e：auto_create 生成 `LARGEINT` 序列列 + `enable_unique_key_merge_on_write=true` + `function_column.sequence_col` |
| 10 | BIGINT opt-in 的越界拦截 | 单测：`sequence_type: bigint` 且序列值 > `MaxInt64` 时写入前报错进 DLQ，绝不静默回绕 |
| 11 | descriptor/schema/文档一致 | `configSchema` 的 doris 块、descriptor、`docs/components/sink-doris.md` 三处字段一致 |
| 12 | 残窗如实文档化 | `docs/components/sink-doris.md` 明确写出：删除保护仅覆盖"严格更旧"、删除从未存在的行无墓碑、等序后写者胜、主键变更旧键在同序重放下可能复活 |
| 13 | at-least-once 与 DLQ 契约不变 | 写失败仍进 DLQ（`error_class` 分类不变）；checkpoint 语义不变；`hack/e2e-doris.sh` 的 outage→DLQ→replay 仍通过 |

## 非目标

- 不做跨 sink exactly-once，不引入 source/checkpoint/sink 事务。
- 不改变 `version_mode: off` 的默认值与语义（存量管道零迁移）。
- **不实现 delete-sign 载荷写入**：实测 Doris 2.1.11 不识别 Stream Load 载荷里的 `__DORIS_DELETE_SIGN`（含 `columns` / `hidden_columns` / `partial_columns` / CSV 四种写法），因此删除继续走现有 MySQL `DELETE`；也因此**不承诺**解除 `allow_mixed_cdc_non_atomic` 限制。
- 不实现 Doris 的 partial column update、`INSERT INTO ... SELECT`、routine load、Flink connector 等能力。
- 不覆盖 Doris 1.x；不承诺非 Unique Key 表的乱序安全（保持现有 fail-closed）。
- 不修复 Doris 删除语义的两个下界（删除从未存在的行无墓碑、等序后写者胜）——只文档化。
- 不引入通用 keyed state / timer / 窗口语义；`window` 等派生聚合仍属 INSERT-only，不得伪装成有序。
- 不为 `insert` 回退路径发明墓碑语义（见 plan：该组合直接 fail-closed）。
- 不新增 connector，不改 source 侧，不新增外部服务。

## 交付约束

- **默认关闭**：`version_mode` 缺省必须是 `off`。Doris 已是 `production` 且存量表没有序列列，改变默认值等于静默改变数据语义。
- **序列值只来自 `core.SourceOrder`**：不得回退到墙钟时间、进程内计数器或批次序号（与 ClickHouse 的既有规则一致）。
- **序列列默认 `LARGEINT`**：实测 `DECIMAL` 被拒绝（`Sequence type only support integer types and date types`）、`BIGINT` 写 `2^63` 会静默回绕成 `-2^63`。
- **强制 `enable_unique_key_merge_on_write=true`**：实测 mow=false 时 `DELETE` 之后更旧的重放会复活该行。
- **delete sign 列名由 Doris 独占**：不在配置面暴露，也不尝试写入（实测不可写）。
- **先证明再实现**：T7.1 已完成（见 findings.md），实现必须与该档结论一致；后续若发现新事实，先补 findings 再改代码。
- 不得以降低 `version_mode=off` 的安全性为代价换取实现简单（例如删除现有 `allow_mixed_cdc_non_atomic` 保护）。
- 任何 maturity 表述不得超前于实测证据：残窗必须在组件文档里与收益同时出现，不得只宣传"乱序安全"。

## 依赖与前置

| 类型 | 内容 | 状态 |
| --- | --- | --- |
| 迭代依赖 | 无（可独立领取） | — |
| 代码前置 | `core.SourceOrder`（`internal/etl/core/record_contract.go:92`）、Doris `validateUniqueKeyModel`（`doris.go:1040`）、Stream Load 载荷构造（`doris.go:550-710`） | 已存在 |
| 参照实现 | ClickHouse `prepareSourceOrderRecords` / `validateSourceOrderTable` / `hack/e2e-clickhouse-replay-ordering.sh` | 已存在 |
| 外部输入 | 真实 Doris FE/BE 容器（`hack/e2e-doris.sh` 已 pin `DORIS_VERSION=2.1.11`），无需外部凭据 | 已满足（T7.1 已实跑） |
| 待决策 | ~~Doris 2.1.11 是否支持 delete-sign 载荷 / 部分列~~ → **已裁定：不支持，删除保留 MySQL `DELETE`；序列类型定为 LARGEINT** | 已裁定（[findings.md](./findings.md)） |

## 完成定义（DoD）

迭代置 `delivered` 需同时满足：

1. `tasks.md` 中全部 task 状态为 `done`，每项有实际执行证据（命令 + 输出摘要 + 镜像/版本）。
2. 上方 13 项验收标准全部 `passed`；任何 `skipped`/`blocked` 必须转为有界后续并在 ROADMAP 留痕。
3. `hack/e2e-doris.sh`（既有全量）与新增 `hack/e2e-doris-source-order.sh` 在同一镜像上通过；`hack/e2e-doris-table-template.sh` 不回归。
4. `internal/etl/server/evidence/connector-evidence.json` 的 doris 记录新增 `source_order_replay` case 并刷新 `finished_at`/`expires_at`（或明确记录 skip 原因）。
5. `docs/components/sink-doris.md`、`docs/etl-config-schema.md`、`CHANGELOG(.zh).md`、`docs/ROADMAP.zh.md` 同步更新；`git diff --check` 通过。
