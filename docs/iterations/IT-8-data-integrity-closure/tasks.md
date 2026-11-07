# IT-8 任务切分

状态：`delivered`（2026-11-01）。所有任务均已完成并附证据落点。

## Round 分组

| Round | 任务 | 主题 |
| --- | --- | --- |
| 1/4 | T8.1 | 数据对账（SQL 生成 + 引擎 + 端点 + 单测）**已完成** |
| 2/4 | T8.2 | 对象提交清单（file + S3 + checkpoint 接线 + 单测）**已完成** |
| 3/4 | T8.3 | 单表重同步（checkpoint 重写 + 端点 + 单测）**已完成** |
| 4/4 | T8.4 | 无主键表快照（hash 模式 + preflight + 校验 + e2e 收口 + 文档）**已完成** |
| 追加 | T8.6 | 维表增量刷新（lookup 高水位合并 + 软删除 + 兜底全量 + 单测 + e2e）**已完成** |

## 任务表

| ID | 任务 | 依赖 | 状态 | 证据落点 |
| --- | --- | --- | --- | --- |
| T8.1 | 数据对账：`reconcile.go` + `reconcile_test.go` + `POST/GET /pipelines/{id}/reconcile` | 无 | `done` | `internal/etl/server/reconcile.go`、`reconcile_test.go`、`server.go` action 白名单 |
| T8.2 | 对象提交清单：`manifest.go` + `s3.go` 接线 + schema 字段 + preflight 建议 | 无 | `done` | `internal/etl/sink/manifest.go`、`manifest_test.go`、`internal/etl/server/schema.go`、`preflight.go` |
| T8.3 | 单表重同步：`resync.go` + `resync_test.go` + `POST /pipelines/{id}/resync` | 无 | `done` | `internal/etl/server/resync.go`、`resync_test.go` |
| T8.4 | 无主键表快照：`pkKindRowHash` + `snapshotTableRowHash` + preflight/校验 + 回归测试 | T8.2 无依赖，与 T8.1–T8.3 并行 | `done` | `internal/etl/source/mysql_snapshot_cdc.go`、`mysql_snapshot_cdc_nopk_test.go`、`snapshot_coverage.go`、`pipeline/spec.go` |
| T8.6 | 维表增量刷新：`incremental_column`/`incremental_query`/`incremental_delete_column`/`full_refresh_interval_sec` + 指标 + spec 校验 | 无 | `done` | `internal/etl/transform/enricher.go`、`lookup_incremental_test.go`、`internal/etl/pipeline/spec.go`、`spec_validation_incremental_test.go` |
| T8.5 | 容器 e2e + 文档收口（api/config/component/runbook/openapi/CHANGELOG/roadmap） | T8.1–T8.4 | `done` | `hack/e2e-data-integrity.sh`、`testdata/pipes-data-integrity/*`、`docs/*` |

## 任务明细

### T8.1 数据对账

**验收**：见 spec 验收 1–4、13。

**实现要点**

- `reconcileSide` 抽象两端 SQL 句柄；mysql/doris 用 `go-sql-driver`，postgres 用 `pgx`，clickhouse 走 native（`clickhouse-go`）或 HTTP（`clickHouseHTTPQuery`），全部复用既有 DSN 构造。
- 判定优先级 `schema_mismatch > count_mismatch > content_mismatch > match`；任一步查询失败即 `error` + 原因。
- 分桶 `MOD(key, buckets)` 两端对齐；无单一数值主键时只给全表结论（写进 `limits`）。
- 目标端出现 `_row_hash` 列时，行数差被解释为重复行折叠而不是数据丢失。

**证据落点**：`internal/etl/server/reconcile_test.go`（SQL 生成 4 方言、转义、表映射、判定矩阵、分桶定位、HTTP 端点、拒绝矩阵）。

### T8.2 对象提交清单

**验收**：见 spec 验收 5–7。

**实现要点**

- 顺序固定：数据对象 → `.manifest.json`；manifest 名由内容派生，重放同批次只重写同一个提交标记。
- `SinkCommitMetadata` 让 manifest key 进入 `sink_commit.native.manifest_key`；`manifest: false` 时返回 `nil` 不阻断 checkpoint。
- `ReadLocalManifests` / `CommittedObjectKeys` 提供读取侧解析；preflight 对每个 `s3`/`file_sink` pipeline 建议开启。

**证据落点**：`internal/etl/sink/manifest_test.go`（提交、幂等、legacy 零变化、崩溃窗口孤儿、前缀覆盖、checkpoint 元数据、配置解析）。

### T8.3 单表重同步

**验收**：见 spec 验收 8–10。

**实现要点**

- 纯函数 `resyncSnapshotTables` 解 envelope → 清指定表游标（含 legacy `last_id`）→ `phase=snapshot` → 复用 `file`/`pos` → 重建 envelope（保留 state/sink_commit）。
- HTTP 层校验：方法（POST）、源类型（`mysql_snapshot_cdc`）、表名归属、checkpoint 存在、位点可用；写入走 `setManagedCheckpoint`（generation 递增 + 静止校验 → 409）。
- 响应携带 `semantics`（effect / sink_requirement / cdc_anchor / checkpoint / scope）。

**证据落点**：`internal/etl/server/resync_test.go`（游标清理、空 map 清理、envelope 保留、非法位置、表名校验、HTTP 409/400/405、持久化改写）。

### T8.4 无主键表快照

**验收**：见 spec 验收 11–12。

**实现要点**

- `pkKindRowHash` + `snapshotRowHashColumn="_row_hash"`；`no_pk_snapshot: skip|hash`（默认 `skip`）。
- `snapshotTableRowHash` 在一致性快照事务内流式 `SELECT *`，逐行算 hash 并作为 metadata PK 下发。
- `checkpointFromRecords` 对 row_hash 记录只校验合成键存在并推进 handoff（此处的遗漏曾导致 e2e 暴露的 checkpoint blocked 故障，已补回归测试）。
- `ProbeSnapshotCoverage` 新增 `row_hash` 类别与 `tables_snapshotted_by_row_hash` 计数；preflight 对被跳过表给出 `no_pk_snapshot: hash` 建议。
- `pipeline.Validate` 强制 `sink.config.pk_columns_from_metadata: true`。

**证据落点**：`internal/etl/source/mysql_snapshot_cdc_nopk_test.go`、`internal/etl/source/snapshot_coverage.go`、`internal/etl/pipeline/spec.go`。

### T8.6 维表增量刷新

**验收**：见 spec 验收 14–15。

**实现要点**

- `loadCache` 在「已加载 + 有高水位 + 未到兜底间隔」时走 `mergeIncrementalLocked`，否则全量；增量失败回退全量并计入 `incremental_skips`。
- 高水位从驱动原始类型比较（`time.Time`/int64/float64/string），未知类型不推进，避免水位倒退。
- 软删除列支持 `bool`/数值/`"1"|"true"|"yes"` 等形态。
- `persistCacheToStateLocked` 复用既有持久化，重启后先恢复缓存再做一次全量刷新。

**证据落点**：`internal/etl/transform/lookup_incremental_test.go`（配置校验、只合并新行、软删除、失败回退、标记列缺失、比较函数）、`internal/etl/pipeline/spec_validation_incremental_test.go`、e2e 场景 5（真实 MySQL：运行中改维表行 → 后续记录取到新值，且 `incremental_merge` 计到 6 次而非全量重载）。

### T8.5 e2e 与文档收口

**验收**：见 spec 验收 14 + DoD。

**实现要点**

- `hack/e2e-data-integrity.sh`（podman）：真实 MySQL 上跑 snapshot+CDC，验证 hash 快照、对账（match / 漂移 / 分桶）、resync（409/400/修复/幂等）、file manifest（提交/孤儿/checkpoint token）、MinIO manifest（对象存在/内容/孤儿/checkpoint token）。
- 文档同步：`docs/etl-api(.zh).md`、`docs/etl-config-schema(.zh).md`、`docs/openapi.yaml`、`docs/components/*`、`docs/ops-runbook.md`、`CHANGELOG(.zh).md`、`docs/ROADMAP.zh.md`、`docs/iterations/README.md`。

**证据落点**：`hack/e2e-data-integrity.sh` 全绿输出、`go test ./...`、关键包 `-race`。

## Residual / follow-up

- **UI 呈现**：对账报告与 resync 目前只有 API/curl 入口，尚未进入前端页面（可作为 UI 后续增量：管道详情页加「对账」按钮与结果面板、DLQ/问题中心联动）。
- **对账的跨端全量比对**：当前用摘要 + 分桶定位，不返回具体差异行；如需行级 diff 需要额外的 key 采样接口。
- **manifest 的湖仓事务**：仍不做元数据改写与小文件合并（与「明确暂缓或不做」一致）。
- **无主键表快照的断点续传**：明确不做（内容哈希幂等替代），大表重读成本已写入文档。
