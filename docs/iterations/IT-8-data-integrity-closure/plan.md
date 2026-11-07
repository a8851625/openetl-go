# IT-8 技术方案

状态：`delivered`（2026-11-01）；对应 [spec](./spec.md) 与 [tasks](./tasks.md)。

## 方案总览

四条能力互相独立，共用两条既有基础设施：`internal/etl/server` 里已经存在的连接构造/内省助手，以及 `core.SinkCommitMetadataProvider` → checkpoint 的提交元数据通道。

| 能力 | 落点 | 关键设计决策 |
| --- | --- | --- |
| 数据对账 | 新文件 `internal/etl/server/reconcile.go` | 只读 SQL 比较；方言生成器拼装；行数→整表摘要→分桶定位三级递进；不支持组合 fail-closed |
| 对象提交清单 | 新文件 `internal/etl/sink/manifest.go` + `s3.go` 接线 | 数据对象先行、manifest 后行；manifest 名由内容派生；manifest key 进 checkpoint |
| 单表重同步 | 新文件 `internal/etl/server/resync.go` | 重写 checkpoint position（清指定表游标、保留 binlog 位点、phase 回 snapshot），复用 lifecycle store 的静止校验与 generation |
| 无主键表快照 | `internal/etl/source/mysql_snapshot_cdc.go` | 新增 `pkKindRowHash`；合成 `_row_hash` 内容键经 metadata 传递；sink 必须 `pk_columns_from_metadata` |
| 维表增量刷新 | `internal/etl/transform/enricher.go`（lookup） | 高水位增量合并 + 软删除列 + 兜底全量刷新；失败回退全量；不持久化高水位 |

## 分项设计

### A. 数据对账（`internal/etl/server/reconcile.go`）

**现状**：无任何 source↔sink 比较能力。`reconcilerLoop` 是运行时自动重启，与数据无关。

**目标**：一次定点、只读、可解释的比较，能回答「目标库现在到底等不等于源库」，并在不等时给出可操作的定位信息。

**关键接口与文件**

| 接口 | 说明 |
| --- | --- |
| `Server.RunPipelineReconcile(ctx, id, spec, req) (*reconcileReport, error)` | 入口；校验源/目标方言、transform、表清单，逐个表比较 |
| `reconcileSide` + `reconcileColumns` | 一端（source/target）的 SQL 句柄与列内省；ClickHouse 走 native 或 HTTP 两条路径 |
| `reconcileCountSQL` / `reconcileDigestSQL` / `reconcileBucketSQL` | 方言化 SQL 生成（纯函数，便于断言） |
| `reconcileCompareBuckets` | 分桶摘要比对，定位漂移的 key 段 |
| `handlePipelineReconcile` | `POST` 执行 / `GET` 返回能力矩阵 |

**数据语义**

- 比较列 = 两端共有列（大小写不敏感匹配，各自用本端拼写引用）；源有目标没有的列 → 直接 `schema_mismatch`。
- 摘要 = `SUM(<per-row hash>(c1 ⋯ cn))`，NULL 用字面量 `<NULL>` 标记（与空串可区分）。
- 分桶 = `MOD(<numeric key>, buckets)`，两端不需要相同哈希函数即可对齐桶号；无单一数值主键时只给全表结论。
- 判定优先级：`schema_mismatch` > `count_mismatch` > `content_mismatch` > `match`；任何一步查询失败 → `error` + 原因，不返回部分结论冒充成功。

**设计权衡**

- 选择 `SUM(hash)` 而非排序后逐行比对：无需 ORDER BY、可下推到数据库、代价 O(rows) 单次扫描；代价是无法定位到具体行，因此用分桶补齐定位。
- 选择在 `internal/etl/server` 内实现而非新包：DSN 构造、列内省、CH HTTP 查询都已在此包，抽包会重复实现或引入循环依赖。
- 拒绝 `transforms` pipeline：源/目标列名与语义不再一一对应，任何结论都可能误导。宁可拒绝，也不给"看起来对"的答案。

### B. 对象提交清单（`internal/etl/sink/manifest.go`）

**现状**：`FileSink.Write` 只写数据对象；内容寻址只能吸收「同批次边界」的重放，崩溃窗口内的对象无法分类。

**目标**：给对象输出一个显式提交边界，读取侧可解析，且不引入新的状态存储。

**关键接口与文件**

| 接口 | 说明 |
| --- | --- |
| `FileSinkConfig.Manifest` / `ManifestPrefix` | `manifest` / `manifest_prefix` 配置（默认 `false` / `_manifests/`） |
| `(*FileSink).commitManifest` | 写 `.manifest.json` 并记录 `lastCommit` |
| `(*FileSink).SinkCommitMetadata` | 实现 `core.SinkCommitMetadataProvider`，把 manifest key 交给 checkpoint |
| `ReadLocalManifests` / `CommittedObjectKeys` | 读取侧解析已提交视图（file_sink 可直接用，S3 按同布局列举） |

**数据语义**

- 顺序：数据对象 → manifest。manifest 出现才代表提交，中间崩溃只留下无引用对象。
- manifest 名 = `sha256(排序后的 "key|sha256|bytes|rows|format")`，同批次重放重写同一个 manifest。
- 布局：`<prefix><manifest_prefix><date>/<hash>.manifest.json`（`.manifest.json` 后缀避免与 `*.json` 数据对象混淆）。
- `manifest: false` 时零变化：不建目录、不写文件、`SinkCommitMetadata` 返回 `nil`（不阻断 checkpoint）。

**设计权衡**

- 为什么默认关闭：开启会多出对象，可能影响既有下游 glob 行为；本仓一贯做法是新语义默认关闭 + preflight 建议（参见 Doris `version_mode`）。
- 为什么不用 checkpoint 里的 position 做边界：对象可见性与 metadata 库不是同一个事务域，读取侧（Hive/Spark）不该依赖 control plane。
- 已知不解决：content-addressed key 含写入日期，跨日重放会产生第二个已提交对象；manifest 让重复可见而非静默翻倍。湖仓事务（元数据改写、小文件合并）仍不做。

### C. 单表重同步（`internal/etl/server/resync.go`）

**现状**：只有 pipeline 级 `checkpoint/reset`，单表漂移需全量重放。

**目标**：把「重跑某张表的 snapshot」表达成一次 checkpoint 重写。

**关键接口与文件**

| 接口 | 说明 |
| --- | --- |
| `resyncSnapshotTables(position, tables)` | 纯函数：解 envelope、清指定表游标、phase→snapshot、复用 binlog 位点、重建 envelope |
| `resyncConfiguredTables(spec, req)` | 表名校验（必须在 `tables`/`table` 内，去重） |
| `handlePipelineResync` | HTTP 层：方法/源类型/表名校验 → 读写 checkpoint → audit → 返回 semantics |

**数据语义**

- 保留 `file`/`pos` 作为新 handoff：重读行的源序不会晚于实时 CDC 行，版本化 sink 不会被快照数据回退。
- 其他表游标不动 → 它们已越过末位键，重入 snapshot 时读到 0 行。
- `last_id`（单表 legacy 游标）一并清零，否则 `Open()` 会把它重新灌回 `last_ids`。
- envelope 的 `state`/`sink_commit` 原样保留，只替换 `source` 段。

**设计权衡**

- 为什么要求静止：运行中的 runner 会在下一次 checkpoint 保存时覆盖重写结果，语义不确定，因此复用 `ErrPipelineNotQuiescent`（409）。
- 为什么不让用户传 binlog 位点：多一个能写坏位置的入口，风险大于灵活性；需要点位控制时用 `checkpoint/set`。
- 无游标表（`no_pk_snapshot: hash`）在任何 snapshot 重入时都会被整表重读——因为设计上不持久化游标。这是可接受的（写入幂等），但必须写进文档与响应的 semantics。

### D. 无主键表快照（`mysql_snapshot_cdc`）

**现状**：`pkKindNone` 表在 `runSnapshot` 中 WARN 后跳过，历史行永不入目标库。

**目标**：让这类表也能进历史快照，且重放/重启幂等。

**关键接口与文件**

| 接口 | 说明 |
| --- | --- |
| `noPKSnapshotMode`（`skip`/`hash`） | 新配置 `no_pk_snapshot`，默认 `skip` 保持旧行为 |
| `pkKindRowHash` + `snapshotRowHashColumn`（`_row_hash`） | 新键类型与合成列名 |
| `(*snapshotCDCReader).snapshotTableRowHash` | 一致性快照事务内流式读取整表，逐行算 hash |
| `snapshotRowHash(cols, data)` | 列名排序 + NULL 标记 + sha256，稳定且与 information_schema 顺序无关 |
| `resolveTablePKs` / `ProbeSnapshotCoverage` | hash 模式下 keyless 表映射为 row_hash；preflight 计划新增该类别 |
| `checkpointFromRecords` | row_hash 记录只推进 handoff、不写游标（回归测试钉住） |
| `pipeline.Validate` | `no_pk_snapshot=hash` 且 sink 未开 `pk_columns_from_metadata` → 校验失败 |

**数据语义**

- 合成键经 `Metadata.PrimaryKeyColumns=["_row_hash"]` + `Metadata.Key` 传递，sink 以 metadata PK upsert。
- 不可断点续传：中断后整表重读，靠 _row_hash 幂等吸收。
- 列值完全相同的多行折叠为一行（内容不可区分）。
- 读取期间持有快照事务 undo log。

**设计权衡**

- 为什么用内容哈希而不是 ROWID/OFFSET 伪造键：ROWID 不稳定（表重建、页面重排），OFFSET 在并发写入下会错位；内容哈希是唯一在重放时幂等的选择。
- 为什么默认 `skip`：把「无主键表突然开始同步」变成隐式行为变化，风险高于收益；preflight 会建议开启并说明代价。
- 为什么强制 `pk_columns_from_metadata`：静态 `pk_columns` 的 sink 无法使用合成键，重快照必然重复，属于「看起来成功但会翻倍」的陷阱。

### E. 维表增量刷新（`lookup`）

**现状**：`mode: cache` 在每次 `refresh_interval_sec` 到点时整表重载；维表越大，刷新越慢越贵，且变更生效延迟等于刷新间隔。

**目标**：把刷新从「整表重载」改成「高水位增量合并」，在不引入 Flink 级状态语义的前提下把维表变更的生效延迟降到轮询间隔。

**关键接口与文件**

| 接口 | 说明 |
| --- | --- |
| `LookupTransform.incrementalColumn/incrementalQuery/incrementalDelete/fullRefreshIv/watermark` | 增量刷新配置与进程内高水位 |
| `(*LookupTransform).mergeIncrementalLocked` | 拉取 `> watermark` 的行、按 `dim_key` 合并、处理软删除、推进高水位 |
| `lookupHighWater` / `lookupMonotonicGreater` / `lookupTruthy` | 高水位推导与比较（time/int/float/string 驱动类型安全） |
| `validateLookupIncrementalConfig`（pipeline） | 配置成对性、占位符、mode 限制的 spec 校验 |

**数据语义**

- 首次刷新 = 全量 `query`，并从结果推导高水位（因此 `query` 必须投影 `incremental_column`）。
- 后续刷新 = `incremental_query`（单参数=高水位），按 `dim_key` upsert；`incremental_delete_column` 为真时移除该 key。
- 任一步失败 → 回退整表重载，绝不留下半更新缓存。
- 高水位是进程内的：重启后第一次刷新是全量（正确性优先），持久化缓存仍会恢复。
- 兜底：`full_refresh_interval_sec` 到点时强制全量，覆盖硬 DELETE 与标记列未变更的行。

**设计权衡**

- 为什么不做 binlog 驱动的维表 CDC：那需要长连接 binlog/复制槽与表级订阅，属于「任意 keyed state」边界；高水位轮询用一条普通 SQL 就能把 90% 的成本消掉，且对源库零侵入。
- 为什么不持久化高水位：持久化就要改 state 格式并处理「缓存被裁剪但高水位还在」的一致性；重启一次全量是可接受的代价，且不引入新的状态正确性风险。
- 为什么要求查询投影标记列：否则高水位无法推进，会静默退化成「每次都拉全表」或「永远拉不到新行」，两种都必须失败而不是猜测。

## 架构约束

- 对账只在 `internal/etl/server` 内做，不外扩为独立包/CLI（复用既有连接助手，避免重复实现）。
- resync 与 reconcile 都是**新的独立 action**，不改变既有 action 语义；action 白名单显式登记。
- 无新增数据库表；报告走 HTTP + audit，manifest 走对象存储，resync 走既有 checkpoint 表。
- 所有新配置默认关闭/保持兼容，`off`/`skip` 路径逐字不变（有断言）。

## 风险与回滚

| 风险 | 影响 | 缓解 / 回滚 |
| --- | --- | --- |
| 对账 SQL 在真实方言上不合法 | 功能不可用（不会损坏数据） | 纯函数单测钉住四种方言；容器 e2e 覆盖 mysql（源+目标）；只读 SELECT，失败只报错 |
| 对账被当成正确性证明 | 误判风险 | 响应固定携带 `limits`；文档与 runbook 明确边界 |
| manifest 对象被下游当数据读 | 数据污染 | `.manifest.json` 后缀 + 独立前缀；文档写明读取侧规则 |
| 跨日重放产生第二个已提交对象 | 重复数据（可见） | manifest 使重复可见；文档记录；不做湖仓事务 |
| resync 误用（sink 非幂等） | 重复行 | 响应 `semantics.sink_requirement` + 409 静止校验 + runbook 前置条件 |
| hash 快照导致行数少于源 | 看似丢数据 | reconcile 对 `_row_hash` 目标表显式解释折叠；文档写明 |
| hash 快照大表长事务 | undo log 膨胀 | 文档写明；默认 `skip`；可用 `limit`… （不适用，说明见 spec 非目标） |
| 回滚 | —— | 全部为新端点/新开关：`manifest: false`、`no_pk_snapshot: skip`、不调用新端点即等于旧行为 |

## 测试策略

| 层级 | 手段 | 覆盖 |
| --- | --- | --- |
| 1 单元（纯函数） | `TestReconcileSQLGeneration` / `TestReconcileQuoteIdentEscapes` / `TestReconcileSourceTablesAndTargetMapping` | 方言 SQL、标识符转义、表映射 |
| 2 单元（引擎） | `TestReconcileVerdictsAndBucketLocalisation`（fake side）+ `TestFileSinkManifest*` + `TestResyncSnapshotTables*` + `TestSnapshotRowHash*` | 判定逻辑、manifest 语义、checkpoint 重写、hash 稳定性 |
| 3 HTTP / 集成 | `TestReconcileHTTPHandler` / `TestResyncHTTPSurface` / `TestResyncRewritesPersistedCheckpoint` / `TestCheckpointFromRecordsHandlesRowHashTables` | 端点契约、持久化效果、checkpoint 生成 |
| 4 真实容器 e2e | `hack/e2e-data-integrity.sh`（MySQL 真实快照+CDC、MinIO 真实对象） | 四条能力的端到端行为与真实 SQL 合法性 |
| 5 回归 / 静态 | `go test ./...`、关键包 `-race`、`go vet`、`gofmt`、`git diff --check`、markdown 表格一致性 | 零回归 |
