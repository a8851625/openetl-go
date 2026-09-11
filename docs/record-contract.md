# Record Identity & Source Ordering Contract

> 状态：IT-2/T2.3 共享地基；T2.4 已由 ClickHouse `source_order` 消费；T2.5 已把
> 完整身份门禁接入普通 metadata-PK 写入路径。本文定义 source、sink、DLQ/replay
> 共用的唯一记录契约；历史 DLQ 受控 replay 由 T2.6 启用。

## 1. Metadata 真值

`core.Metadata` 中以下字段具有跨组件语义：

| 字段 | 语义 |
| --- | --- |
| `source` | connector 实例名，可由用户重命名，不用于判断 connector 类型 |
| `source_type` | 规范 connector 类型；顺序契约只读取该字段（旧记录仅对内置原名或无歧义位置做保守兼容） |
| `source_phase` | `snapshot` / `cdc` / `batch` 等位置相位 |
| `cursor` + `cursor_kind` | 每条 batch/snapshot 记录的精确游标；`numeric` 可尝试生成 UInt64，`ordered` 保留原文本且不得哈希伪装为数值顺序 |
| `snapshot_handoff_file` + `snapshot_handoff_pos` | `mysql_snapshot_cdc` 开始一致性快照前捕获的 binlog 边界；快照分页 cursor 不承担事件版本职责 |
| `primary_key_columns` | source 声明的完整、有序主键列集合；仅靠 `key` 无法识别“部分复合键” |
| `key` | JSON object 形式的行身份值 |
| `format_contract_id` | 产生身份时冻结的格式语义；不得在 replay 时按当前配置猜测。首个稳定值为 `kafka.canal_json/v1` |
| `before_image_state` | 上游 `old`/before 的原始状态：absent/null/empty array/present empty/partial/full/invalid |

这些字段是 additive metadata，不改变 checkpoint 边界。sink acknowledgement 成功、checkpoint
尚未提交时崩溃仍可能重放，整体继续是 checkpointed at-least-once。

## 2. SourceOrder 结果

`core.SourceOrder(record)` 返回 `SourceOrderResult`，调用方必须分别处理：

- `available`：source-owned 的结构化位置是否存在且有效；
- `version_available`：该位置是否能无损、保序地压成 UInt64；
- `scope`：可比较域。Kafka 明确是同一 topic partition，不能解释为跨 partition 时间全序；
- `reason`：缺失、非法、溢出、不支持或仅文本游标等稳定原因。

映射不读取 `Timestamp`、wall clock、随机数或进程内计数器：

| Source / phase | UInt64 映射 | 边界与 scope |
| --- | --- | --- |
| `mysql_cdc` | `1<<63 | file_sequence<<32 | binlog_pos` | file 尾部十进制序号最多 31 bit，pos 完整 32 bit；按库/表使用 |
| `mysql_snapshot_cdc` snapshot | `1<<63 \| handoff_file_sequence<<32 \| handoff_pos` | 与 CDC 共用同一 MySQL binlog 顺序域；数字或文本 cursor 都只负责分页 |
| `mysql_snapshot_cdc` cdc | 与 `mysql_cdc` 相同 | 同一 binlog lineage 内，handoff 前 CDC < snapshot < handoff 后 CDC |
| `postgres_cdc` cdc | PostgreSQL `X/Y` LSN 的完整 64 bit | 按库/表使用；当前无 keyset cursor 的 PG initial snapshot 明确 unavailable |
| `kafka` | `partition<<48 | offset` | partition `0..32767`，offset `0..2^48-1`；只在同一 topic partition 内可比 |
| `mysql_batch` | 非负数值 cursor 原值 | 文本/排序规则 cursor 不生成伪数值版本 |
| `file` / `http` / `rest` / `redis` | 无 | `source_order_unsupported` |

超过位宽、负 offset、非法 LSN、无法解析的 binlog 文件名都返回显式 unavailable；禁止截断、
wrap 或回退到写入时间。

顺序只在 `scope` 与同一 source lineage 内可比较：

- MySQL `RESET MASTER`、PITR、换主或恢复到 binlog 文件序号/位置更小的实例会创建新 lineage。
  旧、新坐标不能直接比较；ClickHouse 目标必须重建或切到新的表/版本域后再恢复。仅执行
  checkpoint reset 或 `cdc_on_binlog_purged: resnapshot` 不能让两个 lineage 自动可比。
- Kafka 版本只在一个 topic partition 内有序。同一业务 key 必须使用稳定消息 key、固定
  partition 路由，并在认证期间保持 partition 数不变；跨 partition 移动的同 key 事件不能依赖
  `_version` 判新旧。
- `window` 等合成多条输入的 transform 没有单一 connector-owned 位置。ClickHouse preflight
  拒绝其 `source_order` 组合；只产生 INSERT 的派生落点可显式选择 `version_mode: append`。

## 3. RecordIdentity 结果

`core.RecordIdentity(record)` 返回 `RecordIdentityResult`。完整身份要求：

1. `primary_key_columns` 已声明且无空列名/重复列名；
2. `key` 是非空 JSON object，精确覆盖全部声明列，不含未声明列；
3. 每个值非 `null`、非空字符串，且不是 object/array；
4. INSERT/DELETE 的 `Data` 完整包含同一组键值；UPDATE 的 `Data` 完整包含 after key；
5. UPDATE 的 Key 表达 before identity。默认要求每个值存在于 `Before` 并相等；只有冻结为
   `kafka.canal_json/v1` 且原始 `old` 是显式 present-empty/partial/full 时，才按 Canal
   “old 仅列出变化字段”的登记语义从 `Data` 补齐未变化键列。absent/null/empty-array/invalid
   old 一律拒绝。

稳定不完整原因包括：

| 类别 | reason |
| --- | --- |
| 主键声明 | `primary_key_columns_missing`、`primary_key_columns_invalid` |
| Key 载体 | `key_missing`、`key_invalid_json`、`key_not_object`、`key_empty` |
| Key 分量 | `key_component_missing`、`key_component_empty`、`key_columns_conflict` |
| Data 键分量 | `data_key_component_missing`、`data_key_component_empty`、`data_key_component_conflict` |
| UPDATE before-image | `update_before_image_missing`、`update_before_key_missing`、`update_before_key_empty`、`update_before_key_conflict`、`update_before_state_invalid` |

配置 `pk_columns_from_metadata: true` 后，validate/preflight 先检查 source/format 是否有身份能力，
Runner 再在 transform 之后、sink 之前执行逐记录完整性校验。不完整记录不会调用 sink：DLQ 保存
成功后其 source position 才能进入 checkpoint；DLQ 保存失败会 fence 当前执行代的所有后续
checkpoint，使该范围在重启后重放。Kafka `on_parse_error: dlq` 也使用同一条 positioned-record
路径，而不是把解析错误混入连接错误重试通道。整体仍是 checkpointed at-least-once。

OpenETL/Debezium envelope 的 JSON-object Kafka key会恢复主键列；OpenETL Kafka sink 还会在
envelope source metadata 中显式携带 `primary_key_columns`。UPDATE 必须携带可验证的完整 before
identity；旧的无声明/无 before envelope 在 metadata-PK 模式下会进入 DLQ，而不会静态猜键。

## 4. metadata-PK sink 共同语义

ClickHouse、MySQL、PostgreSQL 与 Doris 是当前公开声明
`pk_columns_from_metadata` 的四个 sink。它们在任何 DDL、schema drift 或行写入前复用同一
批次验证：目标库/表必须可解析，声明列、JSON Key、Data 及 UPDATE before identity 必须完整
一致，同一目标表的键集合不得漂移。普通空键、部分键、冲突键和未知 replay provenance 都以
稳定的 `error_class=data` 拒绝；拒绝计入目标表 error 指标，但不计成功 row/batch ack。静态
`pk_columns` 不参与普通流猜测，只能作为 `legacy_verified` 单条 replay 的精确安全集合。

主键变更 UPDATE 使用共享验证返回的旧键，不由 sink 再猜测：ClickHouse 写同一源序版本的新行
与旧键 tombstone；MySQL/PostgreSQL 在一个目标事务内先 upsert 新键、再删除旧键；Doris 因
Stream Load 与 MySQL DELETE 无跨协议事务，只能先写新键再删旧键，崩溃窗口可能暂留旧行但不会
丢新行，retry/replay 会收敛。自动建表优先使用 `Metadata.ColumnTypes`，只有声明类型不存在时
才退到样本推断。

## 5. Conformance 与依赖门禁

- 共享 fixture：`internal/etl/core/contracttest/fixtures.go`
- 契约与边界测试：`internal/etl/core/record_contract_test.go`
- producer metadata 测试：`internal/etl/source/*_test.go`
- 依赖门禁：`go list -deps ./internal/etl/core/...` 不得出现 `internal/etl/source` 或
  `internal/etl/sink`；共享层不反向依赖 connector。

推荐验证：

```bash
go test ./internal/etl/core/... ./internal/etl/source/... -count=1
go test -race ./internal/etl/core/... ./internal/etl/source/... -count=1
go list -deps ./internal/etl/core/...
```
