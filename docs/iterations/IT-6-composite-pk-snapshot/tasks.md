# IT-6 任务分解：mysql_snapshot_cdc 复合主键快照

> 状态：`delivered`（2026-09-26：T6.1–T6.5 全部交付，3 轮完成）| 窗口：2–3 轮 | spec：[spec.md](./spec.md) | 方案：[plan.md](./plan.md)

## 任务表

| ID | 任务 | 状态 | 证据位置 |
| --- | --- | --- | --- |
| T6.1 | 复合 PK 解析（KEY_COLUMN_USAGE 探测 + pkKindComposite + 可排序约束）与行值比较分页 | done（2026-09-26 Round 1） | internal/etl/source/mysql_snapshot_cdc.go + mysql_snapshot_cdc_composite_test.go + e2e source_composite_pk_paging_test.go |
| T6.2 | checkpoint 复合游标持久化（last_composite_cursors 可选字段 + 旧格式兼容 + 断点续跑） | done（2026-09-26 Round 1） | 同上（恢复/兼容/防回退单测 + e2e resume 断言） |
| T6.3 | 快照记录复合键元数据（PrimaryKeyColumns 全列 + metadataKeyJSONMulti） | done（随 Round 1 实现；e2e 元数据断言已覆盖；CH 建表验证归 T6.4 e2e） | mysql_snapshot_cdc.go snapshotMetadataKey/snapshotPrimaryKeyColumns |
| T6.4 | e2e（复合 PK 表快照→CH 建表/合并/重启恢复 + -e2e.strict 回归）与可见性（preflight snapshot_coverage） | done（2026-09-26 Round 2） | internal/etl/e2e/path_composite_pk_snapcdc_clickhouse_test.go + preflight.go + docs/evidence/mysql_snap_cdc__ch_rmt_composite_pk.json |
| T6.5 | 回归收口（三 backend drill 确认无影响 + 认证脚本回归 + 文档 + ROADMAP/看板更新） | done（2026-09-26 Round 3） | drill 三脚本 PASS + e2e-snapshot-cdc{,-clickhouse}.sh + e2e.sh PASS + docs/CHANGELOG/ROADMAP/看板更新 |

## 轮次规划

| 轮次 | 内容 |
| --- | --- |
| Round 1 | T6.1 + T6.2（含 T6.3 元数据 seam 随实现交付）✅ 2026-09-26 |
| Round 2 | T6.4（CH e2e 复合路径 5 case 全绿 + preflight snapshot_coverage 可见性）✅ 2026-09-26 |
| Round 3 | T6.5（收口；若 Round 2 顺利可并入） |

## Claim 记录

```text
Round: 1/5
Roadmap item: IT-6 / T6.1 + T6.2（含 T6.3 元数据 seam 随实现交付）
Profile/path: standalone source connector（mysql_snapshot_cdc 快照路径）
Objective: 复合主键表在快照阶段被正确分页读取（行值比较游标），checkpoint 复合游标可持久化/恢复且旧格式兼容
Scope: internal/etl/source/mysql_snapshot_cdc.go、checkpoint_validation.go、mysql_snapshot_cdc_pk_test.go、新 mysql_snapshot_cdc_composite_test.go、新 e2e source_composite_pk_paging_test.go
Non-goals: 无 PK 表快照（仍跳过、fail-closed 语义不变）、shard_* MOD 复合键分片（降级不分片+INFO）、CDC 路径、CH sink、storage 迁移
Acceptance: 1) KEY_COLUMN_USAGE 有序复合列集探测 + 可排序约束（NOT NULL + 数值/字符串/日期族）；2) 行值比较 SQL 构造与参数序 = ORDER BY 列序；3) last_composite_cursors 可选字段、旧 checkpoint 反序列化兼容且 omitempty 不新增字段；4) 断点续跑从复合游标恢复无缺行无前缀重放
Evidence: go test ./internal/etl/source/ -count=1 全绿（新增 18 个复合用例）；go test -race ./internal/etl/source/；go build ./...；go test ./... -count=1；e2e TestSourceCompositePKSnapshotPaging（真实 MySQL 8 容器：60/60 穷举无缺无重、跨重复首列组分页、复合元数据契约、21 行 ack 后重启仅 39 行且零前缀重放、no_pk 保持跳过+fail-closed）；-e2e.strict 下 TestPathMySQLSnapshotCDCToClickHouse 回归 PASS
Result: T6.1+T6.2 delivered（T6.3 实现随交付，CH 建表验证归 T6.4）；迭代保持 active
Residual/follow-up: T6.4（CH e2e 复合路径 + preflight snapshot_coverage）→ T6.5 收口
```

## Round 2 Claim 记录

```text
Round: 2/5
Roadmap item: IT-6 / T6.4（e2e + 可见性）
Profile/path: standalone source connector → ClickHouse sink 全路径
Objective: 复合 PK 表快照→CH 端到端认证（复合 ORDER BY 建表/存量完整/版本合并/tombstone/重启恢复/重放吸收）+ preflight snapshot_coverage 启动前可见
Scope: internal/etl/source/snapshot_coverage.go（新 ProbeSnapshotCoverage）、mysql_snapshot_cdc.go（resolveOnePK 提为包级 resolveSnapshotTableKey 复用，零分类漂移）、internal/etl/server/preflight.go（snapshot_coverage field issue + 跳过表 WARN）、新 e2e path_composite_pk_snapcdc_clickhouse_test.go、新 preflight 单测 snapshot_coverage_preflight_test.go
Non-goals: UI 消费（后续）、DAG/调度、sink 行为改动、no-PK 表语义
Acceptance: 1) preflight/validate 输出 snapshot_coverage（表→游标类型清单，含跳过表 WARN）；2) 复合表快照→CH auto_create ORDER BY(entity_id,language_id)、6/6 存量 FINAL；3) UPDATE 版本合并 + DELETE 复合键 tombstone；4) SIGKILL 重启从 checkpoint 恢复；5) checkpoint reset 重放被 RMT 吸收；6) -e2e.strict 全量回归无破坏
Evidence: e2e TestPathCompositePKSnapshotCDCToClickHouse 5/5 case PASS（docs/evidence/mysql_snap_cdc__ch_rmt_composite_pk.json，自动绑定 commit）；preflight 单测 3 个（composite 计划/no-PK WARN/numeric 基线）；go test ./... 全绿；go test -race source+server；-e2e.strict 下全套 8/8 PASS（含既有 6 路径）
Result: T6.4 delivered
Residual/follow-up: T6.5（三 backend drill + 认证脚本回归 + docs 更新 + ROADMAP/看板收口）
```

### Round 2 验收矩阵

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| preflight snapshot_coverage 可见性 | `TestRunPreflightSnapshotCoverageReportsCompositeCursor/WarnsOnNoPKTable/SingleColumnNumeric`（sqlmock）；e2e case preflight_snapshot_coverage 断言 validate 响应含 composite 计划、无跳过 WARN | passed | — |
| 复合表快照→CH 建表/存量完整 | e2e case snapshot_composite：6/6 FINAL、system.tables sorting_key 断言 ORDER BY (entity_id, language_id)、ReplacingMergeTree | passed | — |
| 快照→CDC 版本合并 + tombstone | e2e case cdc_update_delete：UPDATE 后 FINAL=1 新值、DELETE 后 FINAL=0、总数 6→5 | passed | — |
| 重启恢复 | e2e case restart_recovery：SIGKILL + 停机写入 entity 4 → 恢复后 FINAL=7 | passed | — |
| 重放吸收 | e2e case checkpoint_reset：reset 后 FINAL=7 不变、UPDATE 值保持、tombstone 保持 | passed | — |
| 全量回归 | `-e2e.strict` 8/8 PASS（ch_dedup_crash/native+http、ch_dedup_multitable、kafka_envelope、mysql_cdc__mysql_upsert、mysql_snap_cdc__ch_rmt、schema_contract、新增两测试）；`go test ./...` 全绿；`-race` source+server | passed | 14 认证 shell 脚本回归归 T6.5 |

## Round 3 Claim 记录

```text
Round: 3/5
Roadmap item: IT-6 / T6.5（回归收口）
Profile/path: 全仓收口（storage 三 backend + 认证脚本 + 文档）
Objective: 确认复合 PK 改动对 storage 三 backend 与既有认证脚本无影响；文档/CHANGELOG/ROADMAP/看板收口
Scope: docs/etl-config-schema.md、CHANGELOG.md、CHANGELOG.zh.md、docs/ROADMAP.zh.md、docs/iterations/README.md、tasks.md（无代码改动）
Non-goals: 新功能；复合键 hash 分片（非目标）；线上 29 表手工补数（运维操作）
Acceptance: 1) 三 backend upgrade drill PASS；2) snapshot_cdc 相关 3 认证脚本回归 PASS；3) 文档更新（快照 PK 解析行为 + snapshot_coverage 可见性）；4) ROADMAP IT-6 → delivered + 看板更新
Evidence: hack/e2e-storage-upgrade-{sqlite,mysql,postgres}.sh 全 PASS（postgres 临时端口 15439 避开 dev 常驻容器）；CONTAINER_CLI=podman E2E_SKIP_BUILD=1 ./hack/e2e-snapshot-cdc.sh、e2e-snapshot-cdc-clickhouse.sh、e2e.sh 全 PASS；docs/etl-config-schema.md（复合 PK 解析序/行值游标/元数据/可见性章节）；CHANGELOG.md/.zh.md Unreleased Added 条目
Result: T6.5 delivered；IT-6 迭代整体 delivered（3/5 轮）
Residual/follow-up: none（非目标内的复合键 hash 分片归后续候选池，不属本迭代残留）
```

### Round 3 验收矩阵

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| 三 backend upgrade drill 无影响 | e2e-storage-upgrade-sqlite.sh PASS；e2e-storage-upgrade-mysql.sh PASS；e2e-storage-upgrade-postgres.sh PASS（端口 15439） | passed | — |
| snapshot_cdc 认证脚本回归 | e2e-snapshot-cdc.sh PASS（快照 5+CDC 2=7 行、checkpoint 持久化）；e2e-snapshot-cdc-clickhouse.sh PASS（快照+CDC+DLQ replay）；e2e.sh PASS（file→file/MySQL→file/MySQL→MySQL + storage checkpoint 校验） | passed | 其余 11 认证脚本与 storage 语义无关（本次无 storage/schema 变更） |
| 文档更新 | etl-config-schema.md：pk_column 解析序、复合键约束与行值游标、元数据契约、shard 降级、snapshot_coverage 可见性；CHANGELOG.md/.zh.md Unreleased | passed | — |
| ROADMAP/看板收口 | ROADMAP.zh.md IT-6 → delivered（含证据摘要）；docs/iterations/README.md 看板行 + 拓扑图更新 | passed | — |

### Round 1 验收矩阵

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| 复合 PK 解析：有序复合列集 + NOT NULL/可排序约束 | `TestResolveTablePKs_CompositePKResolvedForExplicitTable`、`...RejectedWhenNotOrderable`、`...NonOrderableTypeFallsBackToNone`、`TestCompositeCursorOrderable`；真实 MySQL 8 e2e：(int,int)/(int,varchar) 解析为 composite、no_pk 为 none | passed | — |
| 行值比较分页：SQL 构造/参数序 = ORDER BY 列序；穷举无缺无重 | `TestCompositeCursorQuery`（首页/翻页/int+varchar/三列）、`TestCompositeCursorQueryParameterOrderMatchesOrderBy`、`TestSnapshotTableCompositePaging`（sqlmock 三页穷举）；e2e 真实 MySQL 60/60 行（重复首列组跨界）、25/25 varchar 复合 | passed | — |
| checkpoint 兼容：可选字段、旧格式反序列化、omitempty 不新增字段 | `TestSnapshotCheckpointLegacyWithoutCompositeFieldRoundTrips`、`TestMarshalCompositeCursorsOmitEmpty`、`TestValidateCheckpointCompositeCursors`（fail-closed 校验）；既有全部 checkpoint 测试保持绿 | passed | — |
| 断点续跑：恢复无缺行、无前缀重放、游标只前进 | `TestSnapshotCheckpointCompositeRestoreIntoReader`、`TestSnapshotCheckpointPersistsCompositeCursorAfterDurableAck`（ack 前不突变 durable 状态）、`...OnlyAdvancesForward`；e2e 21 ack → 39 行、全部 entity_id≥4 | passed | — |
| shard 降级 | `TestCompositeCursorDegradesSharding`（复合 SQL 无 MOD）；运行时 INFO 日志说明降级 | passed | 复合键 hash 分片为非目标 |
| NULL/畸形防御 | `TestSnapshotTableCompositeCursorRejectsNullKeyColumn`（fail loudly）；Open 恢复路径对畸形/null 元素只丢弃该表游标（重放该表，at-least-once 正确）并 WARN | passed | — |
| 元数据契约（T6.3 seam） | `TestSnapshotMetadataKeyComposite`（multi-column JSON key 与 CDC 同源同格式）、`snapshotPrimaryKeyColumns` 全列；e2e 断言 PrimaryKeyColumns=[entity_id store_id]、Key 为复合 JSON 对象 | passed | CH 复合 ORDER BY 建表 e2e 归 T6.4 |
| 既有行为回归 | `go test ./... -count=1` 全绿；`go test -race ./internal/etl/source/`；`-e2e.strict` 下 `TestPathMySQLSnapshotCDCToClickHouse` PASS（单列 PK 快照/CDC/重启/重放路径无回归） | passed | 全量 6 路径与 14 认证脚本回归归 T6.5 |
