# AGENTS.md

## Commands
- `make build` is the safest build path: it installs the GoFrame CLI if missing, then runs `gf build -ew` using `hack/config.yaml`.
- Do not assume plain `go build ./...` works from a fresh checkout: `main.go` imports generated `openetl-go/internal/packed`, and `hack/config.yaml` packs `resource/` into `internal/packed/packed.go` during GoFrame builds.
- Go module is `github.com/a8851625/openetl-go`, with `go 1.24.0` and `toolchain go1.24.13` in `go.mod`.
- Local Go may be unavailable; use `podman run --rm -v "$PWD:/workspace" -v openetl-go_go-cache:/go -v openetl-go_go-build-cache:/root/.cache/go-build -w /workspace etl-go-dev:latest sh -c "go test ./..."` for tests.
- `hack/e2e.sh` builds the app image and validates file->file, MySQL batch->file, and MySQL batch->MySQL via Podman.
- `hack/e2e-cdc-mysql.sh` validates MySQL CDC->MySQL.
- `hack/e2e-clickhouse.sh` validates MySQL CDC->ClickHouse; it requires `docker.io/clickhouse/clickhouse-server:24.3-alpine` locally.
- `hack/e2e-dlq.sh` validates DLQ list/replay against MySQL.
- `hack/e2e-snapshot-cdc.sh` validates integrated MySQL snapshot+CDC->MySQL.
- `hack/e2e-clickhouse-autocreate.sh` validates ClickHouse auto-create and schema drift add-columns.
- `hack/e2e-s3-minio.sh` validates S3 sink against MinIO.
- `hack/e2e-http-source.sh` validates HTTP source pagination/auth headers.
- `hack/e2e-auth-audit.sh` validates ETL API token auth and audit logging.
- `hack/e2e-crash-recovery.sh` validates committed checkpoints and container crash recovery.
- `hack/e2e-cdc-crash-recovery.sh` validates MySQL CDC checkpoint restart behavior.
- `hack/e2e-duplicate-spec.sh` validates duplicate pipeline spec detection.
- `hack/e2e-api-conflict.sh` validates duplicate runtime pipeline create conflicts.
- `hack/e2e-kafka.sh` validates Kafka source/sink via Redpanda (file->Kafka, Kafka->file).
- `hack/e2e-ui.sh` validates the built React UI served by the app container using Playwright CLI (74 tests in 13 sections: sidebar navigation, i18n EN/ZH toggle, dashboard, pipelines, designer, DLQ, plugins, audit, settings, reload specs, backend APIs, Chinese navigation).
- `hack/e2e-elasticsearch.sh` validates Elasticsearch/OpenSearch bulk indexing (mysql_batch->ES).
- `hack/e2e-snapshot-cdc-crash.sh` validates snapshot+CDC crash recovery during both snapshot and CDC phases.
- `make image TAG=<tag>` builds a Docker image through `gf docker`; without `TAG`, it uses the short git SHA and appends `.dirty` when the worktree is dirty. `make image.push` pushes the image.
- `make release` packages already-built files under `bin/$VERSION/*`; `hack/release.sh` defaults to `v3.0.0`, so pass/check the version when cutting releases.
- Frontend source lives in `web/`; run `npm install` and `npm run build` there to regenerate `resource/public`.

## Runtime Config
- Main config lives at `manifest/config/config.yaml`; Docker Compose instead mounts `./config.yaml` to `/app/manifest/config/config.yaml`.
- ETL mode is controlled by `etl.enabled`; when true, the app loads pipeline specs from `etl.specsDir` / `./pipes` and serves ETL API on `etl.address` / `:8001`.
- Pipeline specs are YAML files under `pipes/` or `testdata/pipes-*`; they are loaded into SQLite on startup. Hot-reload via `fsnotify` still watches the `specsDir` for new YAML files.
- Sources/sinks/transforms register by blank imports in `internal/logic/app/app.go`; extism WASM plugins are loaded from the `plugins` table in SQLite at startup.
- The app needs MySQL binlog ROW/FULL and a replication-capable user; the configured source is `database.default.link` in GoFrame format `mysql:user:password@tcp(host:port)/db`.
- `canal.dump` is configured false, but code also forces `cfg.Dump.ExecutionPath = ""`; this service is incremental-only unless that code changes.
- ClickHouse target tables must already exist and include the extra `_version` column because row conversion appends `time.Now().UnixNano()`.
- New ETL ClickHouse sink reads `system.columns`, writes columns in table order, and converts common String values to Int/Decimal/DateTime. Legacy sync code still uses `ReplacingMergeTree(_version)` semantics.
- ClickHouse ETL sink supports `auto_create: true` and `schema_drift: ignore|fail|add_columns`.
- ETL API auth is enabled by `ETL_API_TOKEN`; clients may use `X-API-Token` or `Authorization: Bearer <token>`. Audit logs default to `data/audit.log` or `ETL_AUDIT_LOG`.
- Runtime operations API includes spec validation (`POST /api/v2/specs/validate`), connection test (`POST /api/v2/connections/test`), transform dry-run (`POST /api/v2/transforms/dry-run`), spec reload (`POST /api/v2/specs/reload`), audit listing (`GET /api/v2/audit?limit=N`), and plugin schema (`GET /api/v2/plugins/schema`); use these when validating UI operations.
- ETL API server uses a long-lived server context (`s.ctx`) for pipeline lifecycle; HTTP request context is NOT passed to `runner.Start()` because it gets cancelled when the HTTP response returns.
- At-least-once delivery and sink idempotency expectations are documented in `docs/etl-idempotency.md`; keep it updated when changing source/sink semantics.
- YAML spec shape and connector config fields are documented in `docs/etl-config-schema.md`; keep it updated when changing plugin config.
- Plugin config schema is available at `GET /api/v2/plugins/schema` with typed fields, required markers, defaults, secret flags, and examples for every connector.
- Pipeline metrics include: `source_read_latency_ms`, `sink_write_latency_ms`, `last_batch_size`, `avg_batch_size`, `batch_count`, `checkpoint_age_seconds`, `dlq_file_count`, `dlq_replay_count`, `dlq_delete_count`, `cdc_lag_ms`.
- Spec validation (`POST /api/v2/specs/validate`) returns idempotency warnings for dangerous source/sink combinations (CDC+file, batch+insert, etc.).
- `TZ` is honored at startup; `TZ=CST-8` is special-cased to a fixed UTC+8 zone.
- Storage backend defaults to SQLite (`./data/etl.db`); configured via `etl.storage.type: sqlite|mysql|postgresql` in `config.yaml`. All checkpoints, DLQ, audit logs, pipeline specs, run history, workers, and plugins are persisted to the SQL database. File-based migration happens automatically on first startup.

## Architecture Notes
- Startup path is `main.go` -> `internal/cmd/cmd.go`; `cmd.Main` initializes monitor routes, binds static SPA files from `resource/public`, starts Canal sync in a goroutine, then runs the GoFrame HTTP server.
- GoFrame server (`:8000`) proxies `/api/v2/*` and `/metrics` to the ETL API server (`:8001`) via `httputil.ReverseProxy` so the production UI works without Vite dev proxy.
- MySQL sink and PostgreSQL sink use transaction-bounded batch writes (`BEGIN`/`COMMIT`) for atomicity; DLQ writer and audit logger use persistent file handles.
- Pipeline goroutines have panic recovery; readLoop has exponential backoff on persistent errors (1s→30s cap).
- HTTP server has body size limit (10MB), panic recovery middleware, constant-time token comparison, ReadTimeout/WriteTimeout/IdleTimeout.
- Dockerfile runs as non-root user `etl` (uid 1001), has HEALTHCHECK, and `.dockerignore` excludes build artifacts.
- Alert manager is fully async with a 256-item buffered channel; overflows are dropped rather than blocking pipeline writes.
- ETL architecture lives under `internal/etl`: `core` interfaces, `pipeline.Runner`, file checkpoint store, DLQ writer, plugin `registry`, sources, sinks, transforms, and API v2 server.
- **v4.0 Architecture**: Storage layer (`internal/etl/storage/`) provides SQLite-backed persistence for pipelines, checkpoints, DLQ, audit, run history, workers, tasks, and plugins. Factory in `storage/factory/` selects backend by config.
- **DAG Orchestrator** (`internal/etl/orchestrator/`): `PipelineSpec` with DAG (nodes/edges/conditions), `DAGExecutor` for parallel multi-source/multi-sink execution, `Scheduler` for cron/periodic/streaming/dependency triggers, `ConvertLinearSpec` for backward-compatible YAML loading.
- **Plugin System** (`internal/etl/plugin/pluginsystem/`): extism WASM runtime with install/unload/exec. TypeScript SDK in `web/plugin-sdk/`.
- **Master-Worker** (`internal/etl/master/` and `internal/etl/worker/`): Worker registration, heartbeat health checks, slot management. Standalone mode runs both in one process; cluster mode supports HTTP+WebSocket distributed execution.
- Worker management API: `GET/POST /api/v2/workers`, `POST /api/v2/workers/{id}/heartbeat`, `DELETE /api/v2/workers/{id}/deregister`.
- Plugin install/uninstall API: `POST /api/v2/plugins/install` (multipart form: wasm, name, kind, version), `DELETE /api/v2/plugins/{name}`, `GET /api/v2/plugins/{name}`.
- Frontend source lives in `web/`; run `npm install` and `npm run build` there to regenerate `resource/public`. New pages: DAG Editor (`web/src/DagEditorPage.tsx`), Workers (`web/src/WorkersPage.tsx`), WASM Plugins (`web/src/MyPluginsPage.tsx`), Schedules (`web/src/SchedulesPage.tsx`); TypeScript Plugin SDK in `web/plugin-sdk/`.
- Implemented ETL sources: `mysql_cdc`, `mysql_batch` (supports custom `query` for JOIN), `mysql_snapshot_cdc`, `postgres_cdc` (pgoutput parsing), `kafka`, `file`, `http`.
- Implemented ETL sinks: `clickhouse`, `mysql`, `postgres`/`postgresql` (insert/upsert via pgx), `elasticsearch`/`es`, `kafka`, `s3`/`file_sink`; S3 uses MinIO-compatible API when endpoint/bucket are configured and local file output otherwise. File/S3 sinks support real Parquet output via `parquet-go`.
- Implemented transforms: `identity`, `rename`, `drop_field`, `add_field`, `type_convert`, `filter`, `lua`.
- `internal/logic/logic.go` blank-imports logic packages so their `init()` functions register service implementations. Keep registrations in sync when adding services.
- Sync flow is `go-mysql` Canal -> `NewMultiHandler` -> each `service.SyncTarget`; target failures are logged and sent to monitoring but do not stop other targets.
- Legacy sync flow only implements ClickHouse targets; use the new ETL pipeline framework for MySQL/ES/Kafka/S3 targets.
- Canal table filtering is the union of all target `tables`, built as `<sync.database>.<table>` regexes.
- Monitor HTTP routes are registered under `/api/monitor`; static file fallback is registered later so API routes win.

## Generated And Vendored Assets
- `internal/service/*.go` and `internal/model/entity/config.go` carry GoFrame generated-file headers. Prefer updating the source shape and regenerating with `make service` / `make dao` when appropriate.
- `resource/public` is built frontend output embedded into the Go binary. New ETL UI source is in `web/` and uses Vite/React/Tailwind with sidebar navigation, 6 pages (Dashboard/Pipelines/Designer/DLQ/Plugins/Audit), i18n (EN/ZH) via `web/src/i18n.ts`, auto-refresh, and toast notifications.
- Kubernetes manifests under `manifest/deploy/kustomize` still use `template-single` placeholder names/images; verify and patch overlays before deploying.
