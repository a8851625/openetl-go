# IT-6 技术方案：mysql_snapshot_cdc 复合主键快照

## 现状与代码落点

| 关注点 | 现状 | 位置 |
| --- | --- | --- |
| PK 解析 | `resolveTablePKs`/`resolveOnePK` 只探测单列 PK（优先级：显式 override > 全局 pkCol > 单列自检） | internal/etl/source/mysql_snapshot_cdc.go:290-336 |
| 快照跳过 | `pkKindNone` 表跳过并 WARN | 同文件 :849-862 |
| 分页 SQL | 单列游标 `WHERE pk > ? ORDER BY pk LIMIT n`（numeric）/字符串游标（ordered）；`shard_*` MOD 只挂单列 numeric | 同文件 snapshotTable :940-1050 |
| checkpoint position | `last_ids map[string]int64` + `last_str`（字符串游标） | decodeSnapshotCDCCheckpointPosition |
| 快照记录元数据 | `PrimaryKeyColumns: []string{rpk.column}` 写死单列 | 同文件 :1045 |
| CDC 复合键 | canal `pkColumnNames` 多列 + `metadataKeyJSONMulti` 已实现 | mysql_cdc.go:736、mysql_snapshot_cdc.go:1657-1674 |
| 下游消费 | envelope `primary_key_columns` 数组；CH sink `pk_columns_from_metadata`（`map[string][]string`，多列 ORDER BY 建表 + 复合键 tombstone）已实现 | kafka sink/source、metadata_pk.go、clickhouse.go:918 |

## 设计决策

### D1：复合 PK 解析（T6.1）

`resolveOnePK` 在单列自检失败后新增第 3.5 步：查 `information_schema.KEY_COLUMN_USAGE`（`CONSTRAINT_NAME='PRIMARY'`，`ORDINAL_POSITION` 排序）取复合列集。新增 `pkKindComposite`，`resolvedPK` 增 `columns []string`（复用现有 `column` 字段时留空或存首列，供日志兼容）。

列类型约束：`classifyPKColumns` 逐列检查 `is_nullable='NO'` 且类型可排序（数值族/字符串族/日期族），任一不满足 → 归 `pkKindNone`（不猜序、不用非确定性序）。理由：快照游标的首要性质是**稳定全序**；含 NULL 或不可比较类型的复合键无法保证。

拒绝的替代方案：
- 按全表 `ORDER BY col1, col2 OFFSET n` 分页——OFFSET 深翻页 O(n²)，且与既有 checkpoint 恢复模型不兼容。
- 为复合键造合成单列（如拼接字符串）——破坏与 CDC `primary_key_columns` 的一致性，下游 ORDER BY 无法对应。

### D2：行值比较分页（T6.1）

MySQL 8 行值比较走联合主键索引（row constructor optimization）：

```sql
SELECT * FROM `t`
WHERE (`a`, `b`) > (?, ?)
ORDER BY `a`, `b`
LIMIT ?
```

- 参数顺序严格等于 ORDER BY 列序（联合索引序）。
- 复合键不支持 `shard_*` MOD 分片（单列数值才可取模）；如配置了 shard 且表归复合 kind，降级为不分片并记 INFO（不阻断——分片是性能选项不是正确性选项）。
- 游标推进：每批末行的复合键值元组存入 checkpoint（D3）。

### D3：checkpoint 复合游标（T6.2）

position 新增可选字段：

```json
{
  "phase": "snapshot",
  "last_ids": {"orders": 123},            // 既有：单列 numeric
  "last_str": {...},                       // 既有：单列 ordered（若在用）
  "last_composite_cursors": {              // 新增：表 -> 列值数组（有序）
    "category_description": [101, 1]
  }
}
```

- `omitempty`：旧 checkpoint 反序列化后为空 map，既有管道行为零变化。
- 恢复路径：`tableLastIDs`/`tableLastStr` 旁新增 `tableComposite map[string][]any`；快照循环按表的 resolved kind 选择游标容器。
- 数值/字符串混合元组的 JSON 序列化按列声明类型还原（`column_types` 已在 information_schema 查询中拿到）。

### D4：快照记录复合键元数据（T6.3）

`mysql_snapshot_cdc.go:1045` 改为：复合 kind 写 `PrimaryKeyColumns: rpk.columns`；`Metadata.Key` 用现成 `metadataKeyJSONMulti(rpk.columns, row)`（CDC 路径同源函数，保证快照/CDC 键格式一致——这是下游 RMT 合并正确性的前提）。

### D5：可见性（T6.4）

- WARN 文案拆分：「composite PK now snapshotted」（不应再出现）/「no primary key; snapshot skipped」（仅真无 PK 表）。
- preflight（`checkSchemaCompatibility` 或 snapshot 特定检查）输出 `snapshot_coverage`：`tables_total / tables_snapshotted / tables_skipped_no_pk` 及跳过表名清单。UI 可后续消费，本迭代只保证 API 层输出。

## 数据语义与回滚

- **版本语义零变化**：快照行 `_version` 仍来自 `mysqlSnapshotHandoffOrder`（binlog handoff 打包），与游标形态无关；CH RMT 合并、tombstone、DLQ、replay 全部不动。
- **回滚**：代码回滚后旧 checkpoint 中 `last_composite_cursors` 被忽略（字段可忽略），管道回到跳过语义；已写入 CH 的存量行不受影响（version=handoff 的正常行）。无 storage 迁移、无不可逆操作。

## 允许触碰的文件

- internal/etl/source/mysql_snapshot_cdc.go（解析/分页/checkpoint 恢复/元数据）
- internal/etl/source/mysql_snapshot_cdc_*_test.go（新增单测）
- internal/etl/e2e/（复合键 e2e 扩展）
- internal/etl/server/preflight.go（snapshot_coverage 输出，最小改动）
- docs/etl-config-schema.md、docs/etl-idempotency.md（行为澄清）

不触碰：mysql_cdc.go（CDC 路径）、kafka envelope、ClickHouse sink、storage 层、checkpoint envelope 结构（sink_commit）。

## 测试计划

1. 单测：PK 解析矩阵（两列 int / int+varchar / 含 nullable 列 / 无 PK / 显式 override 优先）、行值比较 SQL 构造与参数序、复合游标序列化-恢复-旧格式兼容、shard 降级逻辑。
2. 集成（MySQL 8 容器）：复合 PK 表分页穷举（构造 1000 行跨页边界值、含重复首列值——验证第二列参与排序）、快照中断续跑。
3. e2e：复合 PK 表（`category_id int + language_id int`）mysql_snapshot_cdc → ClickHouse：存量完整、ORDER BY (category_id, language_id) 建表、快照后 UPDATE 覆盖、DELETE tombstone、重启恢复；`-e2e.strict` 全量回归。
4. 回归：14 认证脚本中涉及 snapshot_cdc 的 3 条（e2e-snapshot-cdc.sh、e2e-snapshot-cdc-clickhouse.sh、e2e.sh）+ 三 backend upgrade drill。
