# IT-8 数据完整性闭环（对账 / 对象提交清单 / 单表重同步 / 无主键表快照）

状态：`delivered`（2026-11-01）

依赖：无（在 IT-6 复合主键快照、IT-7 Doris 源序之上并行推进）

归属 roadmap：IT-8

## 问题陈述

2026-10-31 的「还有哪些实质功能没完成」盘点把 roadmap 之外的实质功能缺口收敛为四条，全部有代码级证据：

1. **维表只能全量刷新，没有增量/CDC 维度更新**。`internal/etl/transform/enricher.go` 的 `lookup` `mode: cache` 是「每 `refresh_interval_sec` 整表重载」；百万行维表每 300s 拉一次，且无法做到维表变更秒级生效。
2. **没有任何数据对账能力**。全仓 `reconcile` 只命中运行时自动重启的 `reconcilerLoop` 与 checkpoint reconcile；不存在 source↔sink 的行数/内容比较。同步是否正确只能靠 e2e 脚本人工比对，`docs/ROADMAP.zh.md` 把「记录对账」列为一致性门槛却无实现。
3. **S3 / File 输出没有提交清单**。`grep -iE "_SUCCESS|manifest|commit_marker" internal/etl/sink/` 为空；`FileSink.Write` 只做「数据对象写入」（`internal/etl/sink/s3.go`，文件路径 tmp+rename、S3 直接 `PutObject`）。数据对象与 checkpoint 保存之间崩溃会留下无法分类的对象，读取侧无法区分「已提交」与「半途失败」，下游 Hive/Spark 会把它读成数据。
4. **只能整条 pipeline 重同步，不能单表**。`resync` 只命中 binlog purge 的 resnapshot 策略；`POST /api/v2/pipelines/{id}/checkpoint/reset` 是 pipeline 级（`internal/etl/server/checkpoint_reset.go` 全文无 table 维度）。多表 fanout 中一张表漂移必须全量重放所有表。
5. **无主键表无法快照同步**。`internal/etl/source/mysql_snapshot_cdc.go` 在 `runSnapshot` 里对 `pkKindNone` 表只打一条 WARN 就 `continue`，历史行永远不进目标库，只能靠后续 CDC 变更。

五条都属于「功能缺失」而非「验证缺失」，且都可在本地容器环境内实现与验证。

## 可观察结果

完成后，操作者可以：

- 对一条 pipeline 发起对账，拿到逐表 `match` / `count_mismatch` / `content_mismatch` / `schema_mismatch` / `missing_target_table` / `error` 判定，以及（数值主键可用时）具体漂移的 key 分桶；
- 打开 `manifest: true` 后，在对象存储/本地目录里看到每个批次的数据对象 + `.manifest.json` 提交标记，并能区分「已提交对象」与崩溃残留；
- 只对一张表重跑 snapshot，其他表保持游标不动；
- 让无主键表也进入历史快照，且重放/重启幂等；
- 让维表按单调列（如 `updated_at`）增量刷新，而不是每 N 秒整表重载。

同时，系统仍然**不能**：

- 把对账结果当作 exactly-once 证明（每次响应都带 `limits`）；
- 对 Kafka / 文件 / HTTP / 对象存储 / ES 端做 SQL 对账（显式拒绝并给原因）；
- 对声明了 `transforms` 的 pipeline 给出逐列对账结论（显式拒绝）；
- 让 manifest 变成湖仓事务（不改写 Hive/Delta/Iceberg 元数据，不做小文件合并）；
- 让无主键表的列值完全相同的多行保持多行（内容哈希会折叠）。

## 验收标准

| # | 标准 | 判定方式 |
| --- | --- | --- |
| 1 | `POST /api/v2/pipelines/{id}/reconcile` 对 SQL 可查询的源/目标对给出逐表判定，且响应固定携带 `limits` | 单测 `TestReconcileVerdictsAndBucketLocalisation` + 容器 e2e `hack/e2e-data-integrity.sh` 场景 1 |
| 2 | 目标端被删行时判定为 `count_mismatch`、`row_delta` 正确，并对单一数值主键给出 `bucket_mismatches` | 同上（e2e 断言 `row_delta":1` 与 `bucket_mismatches`） |
| 3 | 目标缺列判定 `schema_mismatch`；目标表缺失判定 `missing_target_table`；Kafka/文件端与 `transforms` pipeline 显式拒绝 | 单测 `TestReconcileRejectsUnsupportedShapes` + `TestReconcileVerdictsAndBucketLocalisation/missing_target_columns_*` |
| 4 | 对账 SQL 在 mysql / doris / postgres / clickhouse 四种方言下按各自语法生成（标识符转义、NULL 标记、分隔符、摘要函数、分桶表达式） | 单测 `TestReconcileSQLGeneration` / `TestReconcileQuoteIdentEscapes` |
| 5 | `s3` / `file_sink` 的 `manifest: true` 先写数据对象、再写 `.manifest.json`（含 key/sha256/bytes/rows/batch_seq），manifest 名由内容派生（同批次重放只重写同一个 manifest） | 单测 `TestFileSinkManifestCommitsBatch` / `TestFileSinkManifestIsIdempotentPerBatch`；容器 e2e 场景 2（file）与 2b（MinIO） |
| 6 | 未被任何 manifest 引用的对象不被视为已提交（崩溃窗口语义），`manifest: false` 时行为与旧版完全一致且不产生 native commit metadata | 单测 `TestFileSinkManifestMarksCrashWindowObjectUncommitted` / `TestFileSinkWithoutManifestKeepsLegacyLayout`；e2e 孤儿对象断言 |
| 7 | manifest key 进入 checkpoint（`sink_commit.native.manifest_key`），持久化边界与可读边界是同一个对象 | 单测 `TestFileSinkManifestCommitsBatch`（经 `checkpoint.BuildSinkCommitMetadata`）+ e2e 断言 checkpoint 含 `manifest_key` |
| 8 | `POST /api/v2/pipelines/{id}/resync` 只清指定表的快照游标、保留 binlog 位点、phase 回到 `snapshot`，并保留 envelope 的 state/sink_commit | 单测 `TestResyncSnapshotTablesClearsOnlyRequestedCursors` / `TestResyncSnapshotTablesPreservesEnvelope` |
| 9 | resync 对运行中 pipeline 返回 409、对未配置表名返回 400、对非 `mysql_snapshot_cdc` 源返回 400，响应带 `semantics` | 单测 `TestResyncHTTPSurface` / `TestResyncConfiguredTables`；e2e 断言 409 与 400 |
| 10 | resync 后目标库漂移被修复，未指定表不重复投递；对账回到 `match`/`row_delta=0` | 容器 e2e 场景 3（真实 MySQL 快照+CDC） |
| 11 | `mysql_snapshot_cdc` 的 `no_pk_snapshot: hash` 让无主键表进入快照，写出 64 位 sha256 的 `_row_hash` 并作为 `Metadata.PrimaryKeyColumns`/`Key` 传递；无游标表在 checkpoint 中只推进 handoff 而不报错 | 单测 `TestSnapshotTableRowHashEmitsSyntheticKeys` / `TestSnapshotRowHashIsContentStable` / `TestCheckpointFromRecordsHandlesRowHashTables`；e2e 场景 4 |
| 12 | `no_pk_snapshot: hash` 而 sink 未开 `pk_columns_from_metadata` 时 spec 校验失败；preflight 的 `snapshot_coverage` 新增 `row_hash` 类别并对被跳过的表给出建议 | 单测 `TestResolveTablePKsHashModeAcceptsKeylessTable` / `TestResolveTablePKsSkipModeStillFailsClosed` + `go test ./internal/etl/pipeline ./internal/etl/server` |
| 13 | 对账对 `_row_hash` 目标表解释重复行折叠，不把它误报成数据丢失 | 单测（reconcile row-hash 分支）+ e2e 断言 detail 含折叠说明 |
| 14 | `lookup` 的 `incremental_column` 让维表只拉取单调列更新的行并合并进缓存；软删除列生效；增量失败回退全量重载（不留半更新缓存）；查询未投影标记列时明确失败 | 单测 `TestLookupIncrementalRefreshMergesOnlyNewRows` / `TestLookupIncrementalSoftDeleteRemovesRow` / `TestLookupIncrementalFailureFallsBackToFullReload` / `TestLookupIncrementalRequiresMarkerColumn`；e2e 场景 5（真实 MySQL `TIMESTAMP` 高水位） |
| 15 | `incremental_column` 与 `incremental_query` 必须成对出现、查询含且仅含一个占位符、仅用于 `mode=cache`；`full_refresh_interval_sec` 不得为负 | 单测 `TestValidateLookupIncrementalConfig` + `TestLookupIncrementalConfigValidation` |
| 16 | 全量 `go test ./...` 与关键包 `-race` 全绿；`go vet`、`gofmt`、`git diff --check` 干净 | 交付记录 |

## 非目标

- 不做跨系统事务 / exactly-once：对账是时间点只读比较，manifest 是提交清单而非湖仓事务。
- 不做 Kafka EOS、跨 sink 原子 fanout、savepoint（仍属明确不做）。
- 不新增 connector，不改 source 侧语义（除 `mysql_snapshot_cdc` 新增可选的 `no_pk_snapshot`）。
- 不把对账结果持久化成新表：报告经 HTTP 返回，审计走 `pipeline.reconcile` audit 记录。
- 不改 UI（前端呈现作为后续增量，见 tasks 的 residual）。
- 不做 ClickHouse 原生协议之外的新查询栈：复用既有 `clickhouse-go` 与 HTTP 查询路径。

## 交付约束

- 所有新行为**默认关闭**或保持向后兼容：`manifest` 默认 `false`、`no_pk_snapshot` 默认 `skip`；`reconcile`/`resync` 是新增端点，不影响既有路径。
- 对账只发只读 `SELECT`，SQL 由方言生成器拼装，标识符一律转义，不拼接用户输入（`contains` 类过滤与本迭代无关）。
- 失败要 fail-closed 且可解释：不支持的组合返回 400 并说明原因，不返回「看似成功」的空结论。
- 每条能力都要有单测 + 真实容器 e2e 证据，并在文档（config schema / API / component / runbook）同步。

## 依赖与前置

| 依赖 | 说明 |
| --- | --- |
| `database/sql` + mysql/pgx/clickhouse 驱动 | 已在 `internal/etl/server` 注册，可直接复用 |
| `internal/etl/server/connection_context.go` 的 DSN 构造与列内省 | 复用 `mysqlColumns` / `postgresColumns` / `clickHouseColumns` / `clickHouseHTTPQuery` |
| `core.SinkCommitMetadataProvider` + `checkpoint.BuildSinkCommitMetadata` | manifest key 进入 checkpoint 的既有通道 |
| `storage.PipelineLifecycleStore.SetPipelineCheckpoint` | resync 的原子写 + generation 递增 + 静止校验 |
| `mysql_snapshot_cdc` 的 per-table 游标（`last_ids`/`last_strs`/`last_composite_cursors`） | resync 与 hash 快照的落点 |
| podman + mysql/minio 镜像 | e2e 证据 |

## 完成定义（DoD）

1. 四条能力均有实现、单测与容器 e2e 证据（`hack/e2e-data-integrity.sh` 全绿）。
2. 文档同步：`docs/etl-api(.zh).md`、`docs/etl-config-schema(.zh).md`、`docs/openapi.yaml`、`docs/components/{source-mysql_snapshot_cdc,sink-s3,sink-file_sink}.md`、`docs/ops-runbook.md`、`CHANGELOG(.zh).md`。
3. 全量 `go test ./...` 与关键包 `-race` 全绿，`go vet`/`gofmt`/`git diff --check` 干净。
4. 已知边界（重复行折叠、不可断点续传、对账非 exactly-once、manifest 非湖仓事务）写进文档，不以「已实现」掩盖。
5. roadmap / iterations board 状态更新为 delivered，并记录 residual（UI 呈现）。
