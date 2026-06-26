# ETL 幂等性规约

运行时提供 at-least-once 投递语义。Checkpoint 仅在批次成功写入 Sink 之后提交，因此崩溃时可能重放最后一个未提交的批次。生产管道必须选择能够容忍重复或重放 CDC 事件的 Sink 模式。

## 通用规则

- 每个 CDC/snapshot+CDC 管道应在 `record.data` 中保留稳定的主键。
- 批量和快照任务应写入幂等目标模式，或写入新的分区/对象前缀。
- 非幂等 Sink 仅适用于追加型审计/事件流，其中重复是预期的，由下游消费者去重。
- DLQ 重放会重新应用 Transform 并写入同一个 Sink；Sink 模式也必须容忍重放。

## Sink 规约

| Sink | 推荐模式 | 重复行为 | 说明 |
| --- | --- | --- | --- |
| MySQL/TiDB | `batch_mode: upsert` + `pk_columns` | 重放行覆盖相同主键 | CDC 和崩溃恢复（使用可变表时）必须使用此模式。普通 insert 仅对追加型唯一事件安全。 |
| ClickHouse | 兼容 ReplacingMergeTree 的表（含 `_version`），或 ETL `auto_create: true` | 后续版本通过 `FINAL` 去重；合并前可能存在原始重复行 | 需要精确当前状态的查询应使用 `FINAL` 或下游物化。删除依赖表设计/墓碑策略。 |
| Kafka Sink | 生产者写入为 at-least-once | 可能出现重复消息 | 使用确定性消息 key 和消费者端幂等。Kafka 精确一次事务尚未实现。 |
| Elasticsearch | 基于主键的稳定文档 `_id` | 重放文档替换相同 ID | 局部 bulk 条目错误会暴露失败记录索引，runner 只把失败记录写入 DLQ，不会重写已被接受的记录。 |
| S3/OSS/File Sink | 每批次新建对象/文件 | 重放批次可能创建额外对象 | 回填任务使用清单文件或确定性输出前缀。对象清单支持尚未实现。 |
| 本地 File Sink | 每次 flush 新建文件 | 重放批次可能创建额外文件 | 适用于提取/调试流程；如需精确一次输出，消费者必须去重。 |

## Source 指导

| Source | 推荐 Sink 规约 |
| --- | --- |
| `mysql_cdc` | 以源主键为 Key 的 Upsert/Merge 目标。 |
| `mysql_snapshot_cdc` | Upsert/Merge 目标，因为快照行可能重放，且 CDC 可能围绕捕获的 binlog 位置重叠。 |
| `mysql_batch` | 可变目标表使用 Upsert；提取场景可使用追加型 file/S3。 |
| `file` | 取决于文件语义；如果源文件会被重新处理，下游使用确定性 Key。 |
| `http` | 游标/分页 checkpoint 减少重放，但崩溃后 Sink 仍需容忍重复页面。 |
| `kafka` | 使用确定性 Sink Key 或消费者去重。 |

## 运行时保证

- Checkpoint 在 Sink 写入成功后推进。
- 被过滤的记录可推进 checkpoint，因为它们是故意跳过的。
- 失败记录写入 DLQ，尽可能附带 `error_class` 分类。
- Transient 和 unknown 错误会重试；config/auth/schema/data/programming 错误快速失败进入 DLQ 或使操作失败。

## 尚未保证

- 跨 Sink 原子扇出尚未实现。
- Kafka 事务性精确一次尚未实现。
- S3/File 确定性对象清单尚未实现。
