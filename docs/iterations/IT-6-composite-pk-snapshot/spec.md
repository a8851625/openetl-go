# IT-6：mysql_snapshot_cdc 复合主键快照（源连接器正确性）

> 状态：`delivered`（2026-09-26，3 轮交付；验收矩阵见 tasks.md）| 依赖：无（现有 mysql_snapshot_cdc/ClickHouse sink 均为 delivered 基线）| 归属 roadmap 条目：IT-6

## 问题陈述

v0.2.12-beta.20 线上实战（192.168.31.35，cct 库全量同步，`tables: ["*"]`）发现：OpenCart 风格复合主键表在快照阶段被整体跳过，**存量数据完全缺失**，且缺失对用户不可见（仅运行日志 WARN）。

具体缺口（代码核对结论）：

1. **快照游标只支持单列**：`resolveTablePKs`/`resolveOnePK`/`classifyPK`（internal/etl/source/mysql_snapshot_cdc.go:290-336）只探测单列 PRIMARY KEY（numeric/ordered 两种 kind）；复合主键表归 `pkKindNone`，`tables:["*"]` 下跳过快照（:856 WARN）。
2. **CDC 与下游早已支持复合键，唯独快照不支持**：canal `pkColumnNames`（mysql_cdc.go:739）返回多列；envelope `primary_key_columns` 为数组（kafka.go:440）；ClickHouse sink `pk_columns_from_metadata` 走 `validateMetadataPKBatch` 的 `map[string][]string`（metadata_pk.go:27），多列 ORDER BY 建表与复合键 tombstone 已实现。缺口是**孤立的、只在快照路径**。
3. **存量缺失不可见**：preflight/specs validate 不输出快照覆盖/跳过清单，用户只能翻运行日志。

线上影响实例：`*_description`（`(entity_id, language_id)`）、`*_to_store`、`*_to_layout`、关联表共 29 张被跳过；CH 侧 77 张表与跳过清单交集为 0（这些表连表都没建）。

## 可观察结果

1. 复合主键表（如 `(category_id, language_id)`）在 `tables: ["*"]` 全量同步中，存量行完整进入 ClickHouse（复合 ORDER BY 表自动建表），不再出现"no single-column primary key"跳过 WARN。
2. 快照过程中崩溃/重启，复合游标从 checkpoint 恢复，续跑无缺行、无死循环。
3. 复合 PK 表快照行与后续 CDC 增量的版本合并正确：快照行 `_version` = binlog handoff 打包值，后续 UPDATE/DELETE 正常覆盖/删除（ReplacingMergeTree 语义不变）。
4. 真正无主键的表仍跳过并 WARN（文案区分「无主键」），preflight/validate 输出快照覆盖/跳过清单，缺失在启动前可见。

## 验收标准

| # | 验收标准 | 判定方式 |
| --- | --- | --- |
| 1 | 复合 PK 解析：`information_schema.KEY_COLUMN_USAGE` 有序复合列集探测成功；全部列非 NULL 且可排序（数值/字符串/日期）才归复合 kind，否则仍归 `pkKindNone` 跳过 | 单测（解析矩阵：两列 int、int+varchar、含 NULL 列、无 PK）+ 真实 MySQL 8 |
| 2 | 复合游标分页：行值比较 `WHERE (\`a\`,\`b\`) > (?,?) ORDER BY \`a\`,\`b\` LIMIT n` 走联合主键索引；分页正确无缺行无重复 | 单测（SQL 构造/参数）+ 集成测试（复合 PK 表分页穷举） |
| 3 | checkpoint 兼容：新增复合游标字段可选；旧 checkpoint（单列 `last_ids`）反序列化后字段为空、不影响既有管道；断点续跑恢复正确 | 单测（序列化/恢复/旧格式）+ e2e 快照中断重启 |
| 4 | 复合键元数据：快照记录 `PrimaryKeyColumns` 写全部复合列、`Metadata.Key` 用 `metadataKeyJSONMulti`；下游 envelope/CH sink 零改动即正确消费 | 单测 + e2e（CH 建表 ORDER BY 复合列、key 元数据完整） |
| 5 | 快照→CDC 版本合并：复合 PK 表快照行 version=handoff，CDC UPDATE 覆盖、DELETE tombstone 生效 | e2e（扩展 mysql_snap_cdc__ch_rmt 或新增复合键路径） |
| 6 | 可见性：无 PK 跳过 WARN 文案区分；preflight/specs validate 输出快照覆盖/跳过表清单 | API 断言 + e2e |
| 7 | 回归：既有 6 个路径证据 `-e2e.strict` 全绿；14 认证脚本不回归；三 backend upgrade drill PASS（本迭代无新 storage 迁移，确认无影响） | 全量回归记录 |

## 非目标

- 无主键表的快照（仍跳过，语义不变——无稳定游标可分页）。
- 复合键的 `shard_*` MOD 分片（后续可用表达式 hash 扩展，本迭代不做）。
- MySQL 8 以外的行值比较语法兼容（项目 MySQL 基线即 8.0）。
- 不新增 connector；不改 ClickHouse sink 合并语义、at-least-once 契约、CDC 路径任何行为。
- 不做线上 29 张表的手工补数（运维操作，用 CH `mysql()` 表函数另行处理，不属迭代交付）。

## 交付约束

- 快照分页在快照事务（REPEATABLE READ 只读）内执行，不引入锁升级。
- 行值比较的参数顺序必须与 ORDER BY 列序一致（联合索引序），错序即功能错误。
- checkpoint 新字段必须向后兼容：旧 checkpoint 恢复不报错、不改变既有单列表行为。
- 复合键 tombstone/合并语义完全复用现有 CDC 路径实现，不新写合并逻辑。
- 现有全部 e2e（6 路径证据 + 14 认证脚本 + UI e2e）不得回归。

## 依赖与前置

| 类型 | 内容 | 状态 |
| --- | --- | --- |
| 迭代依赖 | mysql_snapshot_cdc 现有快照框架（事务、handoff、游标恢复） | delivered |
| 迭代依赖 | ClickHouse sink `pk_columns_from_metadata` 复合键支持 | delivered |
| 外部输入 | 无（MySQL 8 容器镜像已有） | — |

## 完成定义（DoD）

1. `tasks.md` 全部 task `done` 且有实际执行证据记录。
2. 上方验收标准全部 `passed`；skip/block 转有界后续并在 ROADMAP 留痕。
3. ROADMAP IT-6 状态更新；docs/iterations/README.md 看板更新。
4. docs/etl-config-schema.md（快照 PK 解析行为）、etl-idempotency.md（若版本合并语义有澄清）同一交付内更新。
5. `git diff --check` 通过；无 secret/生成产物进入错误位面。
