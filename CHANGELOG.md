# OpenETL-Go Release Notes

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 格式。

## [Unreleased]

## [v0.2.11-beta.1] — 2026-07-21 — Task-oriented Web UI redesign (P4 landing)

### Highlights

- Primary navigation is now task-grouped: **Overview / Run / Resources / System**, with **New pipeline** as the primary action.
- **Designer/DAG is demoted, not removed**: day-to-day create uses the Source → Transform → Sink wizard; multi-source/router/fanout still use the same pipeline/DAG canvas for advanced edit.
- Shared pipeline health view-model: `healthy` / `degraded` / `failed` / `paused` / `scheduled` / `completed` derived from runtime state, lag, checkpoint age, DLQ, and last error. Overview is issue-first and no longer treats running/total as health.
- Shareable hash routes: `#/overview`, `#/pipelines`, `#/pipelines/new`, `#/pipelines/:id/:tab`, `#/issues`, `#/dlq`, `#/connections`, `#/connectors`, `#/designer`, and more.
- New Issues center, Connector catalog (separated from Connection instances), and pipeline detail tabs (Overview / Runs / Issues / Checkpoints / Spec).
- DLQ aggregates by error class / DAG node; bulk destructive actions stay hidden on empty backlog; replay reports remaining backlog.
- Visual tokens (cool canvas + teal accent), bilingual IA copy, and progressive disclosure of Workers only in distributed mode.
- Design baseline: `docs/UI-REDESIGN.zh.md`, `docs/UI-REDESIGN-PROTOTYPE.html`.

### Validation

- `npm --prefix web run typecheck`
- `npm --prefix web run build`
- `./hack/e2e-ui.sh` → **107 passed, 0 failed**

### Boundary

- Default delivery remains **at-least-once**; the UI does not introduce a separate execution model or UI-only spec.
- Remaining P4 work (step-wizard reorganization, some field-level remediation, fuller a11y matrix) stays tracked in the roadmap.

## [v0.2.10-beta.1] — 2026-07-14 — Reliability certification and real WASM plugin path

### P1: Reliability certification closure

- Unified linear pipeline and DAG checkpoint envelopes around source position, StateStore snapshot versions, and sink acknowledgement metadata while retaining at-least-once delivery rather than cross-system transactions.
- Made checkpoint advancement fail closed: DLQ persistence, state snapshot collection, sink acknowledgement metadata collection, or checkpoint storage failures no longer silently advance the source position.
- Fixed Kafka offset `0` being skipped by zero-value checkpoint checks.
- Fixed the final sink-acknowledged batch remaining uncheckpointed after interval throttling when traffic becomes idle; pending boundaries are now flushed by a timer and on Stop/EOF.
- Made `allow_unsafe` an executable spec field. Kafka/CDC to file/S3 remains blocked by default and requires an explicit opt-in limited to paths whose replay-duplicate boundary has been tested and documented.
- Added the [reliability certification matrix](docs/reliability-certification.md) and expanded Kafka/wide-table coverage for process crashes, broker restarts, consumer group rebalances, offset replay, state restoration, and sink acknowledgement envelopes.

### P2: Real WASM plugin end-to-end path

- Added a real TypeScript transform fixture and `hack/e2e-wasm-plugin.sh` covering real WASM compilation, ABI v1 manifest installation, 0/1/N outputs, secret configuration, DLQ routing, replay after upgrade, and restart reload.
- Added a compiler image with architecture checksum validation and pinned esbuild 0.25.6, Extism JS PDK 1.6.0, and Binaryen 130, including build-time checks for `wasm-merge` and `wasm-opt`.
- Updated the Extism JS SDK bridge to current `Host`, `Config`, and `Var` globals, with WASI, per-call configuration updates, state bridging, and concurrency-safe install/unload/exec behavior.
- Fixed the server-side transform-only compilation path and public docs: TypeScript is bundled to CommonJS by esbuild before the current `extism-js input.js -i interface.d.ts -o output.wasm` CLI is invoked; source and sink plugins remain offline-compile/install flows.
- Added static certification gates for real WASM fixtures, manifests, and compiler inputs. Third-party plugins without independent fault/replay evidence remain beta/dev-only.

### Release Boundary

- Default delivery semantics remain **at-least-once**; this release does not provide cross-system exactly-once transactions. Production pipelines should use stable business keys, versions, upserts, ReplacingMergeTree-style sinks, or explicit deduplication to absorb replay.
- MaxCompute/ODPS remains experimental and externally blocked until SDK-backed writes, real permission/table checks, and DLQ/retry end-to-end evidence are available.
- Uncertified third-party plugins and Feishu plugin samples remain beta/dev-only until they have real-environment fault injection and replay evidence.

### Validation

- `go test ./... -count=1`
- `go test -tags=extism ./internal/etl/plugin/pluginsystem ./internal/etl/server -count=1`
- `npm --prefix web/plugin-sdk run build`
- `npm --prefix web run build`
- `./hack/e2e-kafka.sh`
- `E2E_SKIP_BUILD=1 ./hack/e2e-wide-table.sh`
- `E2E_SKIP_BUILD=1 ./hack/e2e-lookup-state.sh`
- `./hack/e2e-wasm-plugin.sh`

## [v0.2.9] — 2026-07-13 — Multi-table mapping sync, CDC wide-table path, UI scenario entry, connection scope

### Highlights
- **Multi-table A→B sync with table name mapping**:
  - Pipeline-level `table_mapping` supports `template` / `rules` / `regex` with `{source_table}` and `{source_db}` tokens.
  - Mapping preserves `_source_table` / `_source_database` before rewrite.
  - `mysql_cdc` / `mysql_snapshot_cdc` now populate `Metadata.Database` for qualified mapping and CDC policy filters.
  - Snapshot checkpoint cursors remain keyed by original source table after mapping.
  - New e2e: `hack/e2e-multi-table-map.sh` + `testdata/pipes-multi-table-map/`.
- **Multi-table binlog → wide table**:
  - Production-candidate path: `mysql_cdc` + `cdc_policy` + `lookup` + rename/type_convert → ClickHouse wide table.
  - New e2e: `hack/e2e-mysql-cdc-wide.sh` + `testdata/pipes-mysql-cdc-wide/`.
- **UI productization for the two core scenarios**:
  - Wizard adds recommended templates: multi-table DB sync + mapping, and CDC wide table (lookup).
  - Wizard exposes editable `table_mapping` and generates ordinary pipeline YAML.
  - Connection Catalog / Wizard / DAG forms use connection vs task-parameter field scope with clearer labels.
  - Designer toolbar labels, empty-state copy, and empty pipeline/connection/DLQ/audit/WASM hints improved.
  - Fixed WASM Plugins and Workers i18n bare keys (EN/ZH).
- **Extension and ops packaging**:
  - Official Feishu sheet source plugin sample under `web/plugin-sdk/examples/feishu-sheet-source/` (beta/dev-only).
  - Lightweight runtime modes doc + smoke: `docs/runtime-modes.md`, `hack/e2e-runtime-smoke.sh`.
  - Descriptor/schema field `scope` annotation and certification kit sample checks extended.
- **Warehouse ETL residual evidence** (carried from mainline work):
  - Relational write modes, generated-column skip, Debezium metadata PK, DAG load/DLQ replay, and related e2e coverage remain part of the release surface.

### Release Boundary
- Default delivery semantics remain **at-least-once**. Use upsert, stable business keys, version columns, ReplacingMergeTree, or explicit deduplication to absorb replay.
- MaxCompute/ODPS remains experimental without real-environment write/DLQ/replay evidence.
- Built-in `feishu_sheet` and the Feishu WASM plugin sample remain beta/dev-only until real Feishu fault-injection evidence exists.
- Complex multi-fact real-time merge / Flink-style wide-table semantics are still out of scope; the certified wide-table path is fact stream + dimension lookup (+ optional tumbling aggregate).

### Validation
- `go test ./internal/etl/server ./internal/etl/pipeline ./internal/etl/source ./internal/cmd -count=1`
- `npm --prefix web run build`
- `./hack/pack.sh` (or `SKIP_UI=1 ./hack/pack.sh` after UI build)
- `bash hack/e2e-runtime-smoke.sh`
- `E2E_SKIP_BUILD=1 bash hack/e2e-multi-table-map.sh`
- `E2E_SKIP_BUILD=1 bash hack/e2e-mysql-cdc-wide.sh`
- Playwright UI spot-check: Wizard templates, table_mapping panel, WASM/Workers ZH i18n

## [v0.2.8] — 2026-07-06 — Lookup query-mode certification, Plugin ABI v1 production boundary, Doris/UI release closure

### Highlights
- **Lookup query-mode and state certification**:
  - Closed the first lookup asynchronous I/O loop with query-mode validation, Redis-only cache gate, preflight/schema/spec checks, and `hack/e2e-lookup-query.sh`.
  - Added lookup query fixtures covering successful lookup, miss, timeout, and lock-wait/replay behavior.
  - Added runner DLQ context regression coverage so DLQ write failures do not silently advance checkpoints.
- **Connector certification kit expansion**:
  - Added/extended certification checks for descriptor/schema/readiness/e2e evidence and component docs.
  - Added production-candidate evidence for MySQL, ClickHouse, Kafka, S3/File and ongoing Doris certification.
  - Updated certification docs with plugin ABI rules and production plugin gates.
- **Plugin ABI v1 production boundary**:
  - Centralized plugin name/kind/manifest validation in `internal/etl/plugin/pluginsystem`.
  - `/api/v2/plugins/install` now accepts an optional Plugin ABI v1 `manifest` field and validates explicit manifests before writing/loading WASM.
  - Plugin metadata persisted in storage now includes ABI, minimum runtime version, manifest JSON, and `manifest_validated`.
  - `/api/v2/plugins` and `/api/v2/plugins/schema` expose the current `plugin_abi` contract.
  - TypeScript SDK exports ABI constants, manifest types, and `definePluginManifest`; the VIP example now declares a manifest.
  - Added `docs/plugin-abi-v1.md` with the manifest shape, compatibility matrix, deprecation policy, and certification boundary.
- **Doris production-candidate certification hardening**:
  - Expanded `hack/e2e-doris.sh` to use an independent MySQL source port and cover MySQL CDC -> Doris plus MySQL snapshot+CDC -> Doris.
  - Added restart/replay evidence: app restart continuation, checkpoint reset replay absorption, schema drift add-column, and Doris BE outage -> DLQ -> recovery replay.
- **Phase 1 verification and UI productization closure**:
  - Fixed PostgreSQL CDC e2e MySQL client host usage.
  - Completed Wizard transform-chain productization for add/remove, type switch, reorder, per-stage dry-run, and stage-positioned partial errors.
  - UI e2e now covers the transform-chain controls and remains at 99 passing checks.
- **Operational polish**:
  - Added distributed worker label HTTP e2e coverage.
  - Added logging regression coverage.
  - Refreshed packed UI assets and release version metadata.

### Release Boundary
- Plugin ABI v1 infrastructure is production-ready as an extension boundary. Individual third-party plugins are not production-certified unless they provide their own manifest, docs, tests, and runtime evidence.
- Feishu/Lark spreadsheet plugin integration is recorded as the next official plugin-sample item in the roadmap; the existing built-in `feishu_sheet` source remains beta until more real-environment evidence is available.
- Default delivery semantics remain at-least-once; production guidance continues to rely on upserts, stable business keys, version columns, and sink-specific replay absorption.

### Validation
- `go test ./internal/etl/plugin/pluginsystem ./internal/etl/server ./internal/etl/storage/... -count=1`
- `go test ./internal/etl/... ./internal/cmd -count=1`
- `go test ./... -count=1`
- `npm --prefix web/plugin-sdk run build`
- `npm --prefix web run build`
- `SKIP_UI=1 ./hack/pack.sh`
- `CONTAINER_CLI=podman ./hack/e2e-ui.sh` — 99 passed, 0 failed
- `git diff --check`

## [v0.2.7] — 2026-07-03 — Debezium CDC preflight fix, enricher async I/O enhancement, Phase 1 数仓 ETL 场景闭环

### Highlights
- **Debezium CDC preflight fix**: Added `hasDebeziumCDCTransform()` helper; `checkRelationalSinkConfig` and `checkDorisSinkConfig` now skip `table` and `pk_columns` static requirements when the pipeline carries a `debezium_cdc` transform with `auto_create: true` / `pk_columns_from_metadata: true`. Suppressed `pk_columns` recommendation for CDC pipelines.
- **enricher async I/O enhancement** (Phase 1 "异步 I/O 维表查询增强"): Rewrote `EnricherTransform` with:
  - `concurrency` / `max_in_flight` controls for parallel in-flight enrichment calls within a batch via `BatchTransform`.
  - `max_retries` / `retry_base_ms` with exponential backoff for transient errors (HTTP 429/5xx, network timeouts).
  - HTTP 429 `Retry-After` header honored during retry.
  - Explicit failure classification: 429/5xx → `transient`, 401/403 → `auth`, other 4xx → `data`.
  - Full `TransformMetricsProvider` with 10 counters (`processed`, `hits`, `misses`, `cache_hits`, `cache_misses`, `timeouts`, `retries`, `errors`, `succeeded`, `in_flight`).
  - SQL mode now benefits from `timeout_seconds` context deadline (previously only HTTP).
  - `hub e2e-enricher.sh` with 4 scenarios: happy path, 429+Retry-After retry, timeout→DLQ, batch partial failure→DLQ.
- **Phase 1 数仓 ETL 场景闭环** delivered in full:
  - pre_write action (MySQL/PostgreSQL: delete/truncate/truncate_partition with parameterized condition).
  - map_fields transform (declarative enum/status code mapping).
  - Post-Commit Trigger via `schedule.type: dependency` for CDC→recalculation patterns.
  - increment batch_mode for accumulator columns (MySQL/PostgreSQL).
  - extract transform (regex `pattern`+`group` and `template` join).
  - feishu_sheet source connector (OAuth2 client_credentials + sheet polling).
  - HTTP source OAuth2 client_credentials auth enhancement.
  - Connection config responsibility consolidation (behavior fields deprecation warning).
  - Sink metadata-driven column set: generated column skipping and `pk_columns_from_metadata` for Debezium key PK derivation.

### Validation
- `go test -count=1 -run TestRunPreflight ./internal/etl/server/`
- `go test -count=1 -run TestEnricher ./internal/etl/transform/`
- `go test ./internal/etl/transform/ ./internal/etl/server/ ./internal/cmd -count=1`
- `go vet ./internal/etl/... ./internal/cmd`
- `E2E_SKIP_BUILD=1 ./hack/e2e-enricher.sh` — 4 scenarios passed
- `go build -buildvcs=false ./...`

## [v0.2.6-beta-2] — 2026-07-01 — Wire runtime Scheduler into Server

### Highlights
- Wired `orchestrator.Scheduler` (cron/periodic/dependency schedule engine) into `Server.StartAll` so deferred-schedule pipelines are no longer started immediately but registered with the scheduler.
- Added `s.scheduler` field to the Server struct, initialized in `NewServer`, with `StartAll` registering each deferred pipeline and calling `go s.scheduler.Run(ctx)`.
- All runtime API paths (create, update, import, schedule PUT/DELETE, pipeline delete) now register or unregister the schedule entry on the fly without requiring a restart.
- Added a `schedulerScheduleFor` helper that resolves pipeline display-name references to stable IDs for dependency schedules.
- Refactored `Scheduler` to accept `pipeline.RunnerInterface` instead of `*DAGExecutor`, so linear runners, parallel runners, and DAG runners are all schedulable.
- Added integration tests covering cron schedules not starting immediately on boot, and periodic schedules actually triggering the runner.

### Validation
- `go test ./internal/etl/... ./internal/cmd -count=1`

## [v0.2.5-beta.1] — 2026-06-29 — AI context pack and reviewed DAG generation

### Highlights
- Added an AI context pack generated from connector descriptors, plugin schema, maturity metadata, component docs, product boundaries, DAG rules, examples, and common error patterns.
- Added `GET /api/v2/ai/context` and updated `POST /api/v2/ai/generate` to use the context pack instead of a hard-coded prompt; generated drafts now return `context_pack_version`, `validation`, and `review` metadata.
- Added AI review flags for missing required fields, secret confirmation, experimental/dev-only maturity, CDC-to-append replay risk, MaxCompute/ODPS writer-disabled paths, DDL apply, script transforms, and disabled DLQ.
- Updated the DAG editor AI drawer to show validation status, missing fields, risk flags, required confirmations, and current-vs-generated YAML before the user applies the draft to the canvas.
- Improved the first-task wizard transform chain with add/remove/reorder controls, transform type switching, and per-stage dry-run while preserving the ordinary `transforms` array spec.
- Added first-batch component docs under `docs/components/` for core production-candidate sources, sinks, and transforms, with purpose, fields, record shape, checkpoint/DLQ/idempotency boundaries, examples, and evidence.
- Refreshed API/OpenAPI/Quickstart docs and UI assets for AI-assisted generation boundaries.

### Validation
- `npm --prefix web run build`
- `go test ./internal/etl/server ./internal/etl/transform -count=1`
- `podman run --rm -v "$PWD:/workspace" -v openetl-go_go-cache:/go -v openetl-go_go-build-cache:/root/.cache/go-build -w /workspace etl-go-dev:latest sh -c 'go test ./internal/etl/server ./internal/etl/transform -count=1'`
- `./hack/e2e-ui.sh` — 92 passed, 0 failed
- `./hack/pack.sh`

## [v0.2.4-beta.1] — 2026-06-29 — Connection context and schema introspection

### Highlights
- Added `GET /api/v2/connections/{name}/context`, returning a saved connection, connector descriptor, recommended schedule/batch/checkpoint settings, and best-effort source introspection.
- Added source introspection adapters for file/HTTP/demo samples, MySQL/PostgreSQL database/table/column/primary-key metadata, and Kafka topic/partition metadata.
- Updated the first-task wizard to select saved source/sink connections, render health/schema/sample/topic/table context, and generate ordinary specs with `connection` references plus recommended batch/checkpoint values.
- Updated the DAG editor node properties to show saved connection context while preserving the existing `connection` field in DAG specs.
- Refreshed API docs, OpenAPI metadata, embedded UI assets, and UI e2e coverage for saved connection context.

### Validation
- `go test ./internal/etl/server -count=1`
- `npm run build` in `web/`
- `./hack/pack.sh`
- `./hack/e2e-ui.sh` — 92 passed, 0 failed

## [v0.2.3-beta-1] — 2026-06-27 — First-task UI and runtime flags

### Highlights
- Added a first-task wizard in the React UI for database sync, Kafka detail/aggregation, Debezium CDC sync, Kafka protocol parsing, and file/HTTP landing tasks. The wizard emits ordinary pipeline specs and keeps YAML as the auditable source of truth.
- Added schema-driven source/sink/transform configuration forms, generated YAML editing, YAML-to-form sync, transform dry-run, validate + preflight, and create-and-start flow in the wizard.
- Extended the DAG editor with YAML-to-canvas/form roundtrip, validate + preflight actions, and structured rendering for errors, warnings, preflight issues, field issues, remediation, and DDL preview.
- Added runtime CLI flags for config path, local data/log/plugin/schema/spec directories, HTTP and ETL API bind addresses, storage, TLS, API token, audit, logger format, and standalone/master/worker role settings. Runtime precedence is now CLI flags > environment variables > config file > built-in defaults.
- Added shared Podman/Docker detection for hack scripts via `hack/container-cli.sh`, and updated e2e scripts and docs around the new container runtime selection.

### Validation
- `go test ./internal/cmd ./internal/etl/server ./internal/etl/sink`
- `go run . --help`
- Invalid `--role` startup check
- `E2E_SKIP_BUILD=1 ./hack/e2e-ui.sh` — 88 passed, 0 failed

## [v0.2.3-beta] — Doris validation and schedule constraints

### Highlights
- Promoted the Doris sink contract with safer defaults and real FE/BE validation: `ddl_policy` now defaults to `reject`, schema validation checks table existence, field compatibility, and Unique Key / `pk_columns` alignment, and `ddl_policy=apply` is limited to safe add-column changes.
- Hardened Doris writes and DDL for Doris 2.1: Stream Load labels are deterministic, JSON/CSV headers are explicit, errors are classified for retry/DLQ behavior, auto-create requires a stable key, and generated Unique Key DDL uses Doris-compatible column ordering and type inference.
- Added `hack/e2e-doris.sh` and included it in `hack/e2e-all.sh`; the script runs with Podman or Docker and validates MySQL batch -> Doris Stream Load JSON, Stream Load CSV, MySQL insert fallback, auto-create Unique Key, decimal inference, and zero failed records against official Doris FE/BE 2.1.11 images.
- Added source-bound scheduling metadata: source descriptors now expose `supported_schedules` and `default_schedule`, specs apply default schedules, and validation rejects unsupported `schedule.type` values with required-field checks for `cron`, `periodic`, and `dependency`.
- Updated the DAG editor to load connector descriptors, filter schedule types by the selected source set, support dependency schedules, and reset unsupported schedule selections when sources change.

### Validation
- `CONTAINER_CLI="${CONTAINER_CLI:-$(command -v podman || command -v docker)}"; "$CONTAINER_CLI" run --rm -v "$PWD:/workspace" -v openetl-go_go-cache:/go -v openetl-go_go-build-cache:/root/.cache/go-build -w /workspace localhost/etl-go-dev:latest sh -c 'go test ./internal/etl/...'`
- `npm run build` in `web/`
- `E2E_SKIP_BUILD=1 ./hack/e2e-doris.sh`

## [v0.2.1] — Pipeline orchestration cleanup and connection reuse

### Highlights
- Removed the standalone wide-table preview API and dedicated frontend page. Wide-table detail and aggregate use cases are now documented as ordinary pipeline/DAG orchestration patterns built from source, transform, state, and sink capabilities.
- Added saved connection references to linear pipeline specs and DAG nodes through `connection` / `connection_ref`, allowing shared connector credentials and base configs to live in the connection catalog while per-pipeline fields stay inline.
- Reworked the English and Chinese READMEs into a clearer product entrypoint covering quick start, minimal specs, connection reuse, orchestration-based wide-table aggregation, connector surfaces, runtime model, and documentation links.

### Validation
- `go test ./internal/etl/server ./internal/etl/pipeline ./internal/etl/orchestrator`
- `npm run build` in `web/`

## [v0.2.0] — Pipeline orchestration and reliability release

### Highlights
- Fixed React production bundle blank-page regressions caused by undefined runtime variables in routed pages, and refreshed the packed `resource/public` assets used by the Go server.
- Added a pipeline orchestration path around Kafka facts, lookup dimensions, tumbling aggregation, and ClickHouse output, including UI entries for orchestration preview, Connections, and Schedules.
- Added stable DLQ IDs for replay/delete flows, improved stateful transform metrics, and introduced state/checkpoint envelopes for state-backed deduplicate, lookup, join, and window paths.
- Added connector/roadmap maturity guidance so source, sink, transform, storage, and plugin capabilities are presented with explicit maturity instead of over-claiming production readiness.

### Pipeline Validation
- Added `hack/e2e-wide-table.sh` for Docker-based Redpanda + MySQL + ClickHouse validation.
- Covered Kafka -> lookup -> ClickHouse detail pipelines, Kafka -> deduplicate -> lookup -> tumbling aggregate -> ClickHouse pipelines, duplicate Kafka message absorption, schema drift DLQ, lookup miss DLQ and replay, lookup refresh failure DLQ, and ClickHouse outage DLQ/replay.

### Release Boundary
- This is a 0.2.0 release. Kafka orchestration-based aggregation, ClickHouse sink usage, lookup stream-table joins, tumbling aggregation, and SQLite-backed state are available as validated building blocks, not a blanket production-ready guarantee.
- Default delivery semantics remain at-least-once. Exactly-once, Kafka rebalance/crash guarantees, DAG/stateful replay, stream-stream production joins, complex windows, and full connector certification remain roadmap items.

### Verification
- `./hack/e2e-wide-table.sh`
- `./hack/e2e-ui.sh` — 73 passed, 0 failed
- Docker: `go test -timeout 120s ./internal/etl/...`

## [v0.1.0-beta2] — Phase 5 reliability and usability release

### Highlights
- Closed the beta2 P0/P1 reliability bar: standalone runner creation, file-source resume, zero-survivor checkpoint safety, Postgres CDC pgoutput parsing, worker slot accounting, sink error metrics, and preflight rejection for hard pipeline misconfigurations.
- Reworked the public quickstart surface around OpenETL-Go: canonical MySQL CDC -> ClickHouse examples, aligned Docker compose settings, richer `/api/v2/plugins/schema` metadata, and updated README/quickstart/deployment docs.
- Improved the lightweight release shape by excluding test fixtures from runtime images and publishing `-tags=nolua` as the Lua-free build option while keeping default Lua compatibility.

### Verification
- Added/updated focused tests for server preflight behavior, plugin schema coverage, runner checkpoint safety, Postgres CDC non-row messages, and worker slot limits.
- Verified affected packages with `go test -race -count=1 -timeout=120s ./internal/etl/server ./internal/etl/pipeline ./internal/etl/source ./internal/etl/worker`.

## [v0.1.0-beta] — 首个公开测试版

### 亮点
- **单二进制 ETL/CDC 引擎**,纯 Go 默认构建,零外部运行时依赖
- 8 种 Source + 9 种 Sink + 19 种 Transform,覆盖主流数据同步/清洗/轻度加工场景
- MySQL CDC (binlog) + PostgreSQL CDC (逻辑复制) + 快照增量衔接
- JDBC Sink (支持任意 JDBC 数据库,含 Oracle/SQL Server/DB2 等)
- 22 个 E2E 脚本验证(CDC 崩溃恢复 / DLQ / 分布式分片 / ClickHouse 自动建表 …)
- 单机 SQLite(零依赖) / 可扩展 MySQL·PG + master-worker 真分布式

### 连接器 (Sources)
- `mysql_cdc` — MySQL binlog CDC (行级增删改,含 GTID/position checkpoint)
- `mysql_snapshot_cdc` — MySQL 快照(全量) + 增量 CDC 无缝衔接
- `postgres_cdc` — PostgreSQL 逻辑复制 (pgoutput)
- `mysql_batch` — MySQL 全量批量读取
- `kafka` — Kafka 消费者组 (at-least-once,offset 断点)
- `redis` — Redis SCAN 全量
- `http` — HTTP API 分页读取(断点续传,429/5xx 指数退避)
- `file` — JSON Lines / CSV 文件(byte-offset checkpoint)

### 连接器 (Sinks)
- `clickhouse` — 原生协议 + HTTP 协议,自动建表(DDL 翻译),ReplacingMergeTree 裁剪
- `mysql` — 批量 INSERT / upsert(INSERT … ON DUPLICATE KEY UPDATE),幂等,自动建表
- `postgres` — 批量 INSERT / upsert(INSERT … ON CONFLICT),自动建表
- `doris` — Stream Load + MySQL DELETE,auto-create,DDL 翻译
- `kafka` — 同步生产者(支持幂等),auto-create topic
- `elasticsearch` — Bulk API,动态索引,多 host 轮询,429 Retry-After
- `redis` — HASH/STRING/LIST 三种模式
- `s3` — MinIO/S3 对象存储(分片上传,断点重试)
- `jdbc` — 任意 JDBC 数据库 (MySQL/PostgreSQL/Oracle/SQL Server/DB2/…)

### 转换 (Transforms)
- **清洗**: `filter`(表达式)、`deduplicate`、`validate`、`type_convert`
- **加工**: `rename`/`drop_field`/`add_field`、`enricher`、`lookup`、`join`、`window`
- **路由**: `router`(条件分流)、`fanout`(一对多) `tap`(旁路) `rate_limiter`
- **脚本**: `lua`(默认,gopher-lua)、`javascript`/`ts`(QuickJS,CGO)、WASM 插件(extism,wazero)

### 执行模式
- 线性 Pipeline — 串行 Source→Transform→Sink
- DAG — 多源多汇有向无环图,条件边路由
- ParallelRunner — 单源表分片并行写入
- master-worker 分布式 — MySQL/PG 共享存储,分片跨 worker 不重叠分发,worker 崩溃重分配

### 可靠性
- at-least-once + 幂等 sink (upsert / 版本列)
- DLQ 死信队列 (SQLite/MySQL/PG,`/api/v2/dlq/*` 查看重放删除)
- 三态断路器 (closed→open→half-open),基于 sink 独立隔离
- 指数退避重试 (`retry.Do` + 可重试错误分类)
- `-race` 默认跑测试;零静默数据丢失 (SPEC §4.2/§6.1)

### 运维
- REST API `/api/v2/*` (CRUD pipeline,上传下载 YAML,启停,查看状态/DLQ/preflight)
- Prometheus `/metrics` (每 sink 指标:rows/batches/errors/latency,断路器状态,lineage)
- JSON 结构化日志 (`LOGGER_FORMAT=json`)
- SQLite / MySQL / PostgreSQL 存储后端 (pipeline 定义/checkpoint/DLQ/audit)
- Web 管理界面 (Svelte,GoFrame resource-pack)
- `make test` / `make test-quick` / `make test-integration`

### 平台
- Linux (amd64, arm64)
- macOS (amd64, arm64 / Apple Silicon)
- Windows (amd64)

### 构建标签
| 标签 | 效果 | 默认? |
|------|------|------|
| *(无)* | 纯 Go 核心 + 全部 Sink/Source + Lua(gopher-lua,纯 Go) | ✅ |
| `-tags=extism` | + WASM 插件运行时(wazero,纯 Go) | — |
| `-tags=nolua` | 剥离 Lua 运行时,进一步瘦身 | — |
| `CGO_ENABLED=1` | + JavaScript/TypeScript transform(QuickJS,CGO) | — |

### 文档
- `docs/quickstart.md` — 5 分钟入门
- `docs/etl-api.md` — REST API
- `docs/etl-config-schema.md` — 配置字段
- `docs/etl-idempotency.md` — 幂等与 exactly-once 语义
- `docs/parallelism-and-batching.md` — 并行与批处理
- `SPEC.md` — 架构与生产就绪标准 (Phase 0-5 全部完成)
