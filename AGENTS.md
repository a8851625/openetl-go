# AGENTS.md

## Product Positioning
- OpenETL-Go is a lightweight, self-hosted, open-source CDC/ETL runtime for data synchronization, cleansing, and aggregation. Keep new product and engineering work aligned with that purpose.
- The primary product surface is `Source -> Transform -> Sink` pipelines, with YAML/API/UI as equivalent ways to operate the same spec. DAG, schedules, parallel shards, and master-worker mode should extend this model rather than become a separate product line.
- Optimize for the common operational data paths: databases, Kafka, files, HTTP/object storage, OLAP/search sinks, checkpointed at-least-once delivery, DLQ visibility/replay, idempotent sink modes, schema/preflight safety, and small-team self-hosting.
- Do not position the project as a full replacement for Flink/Spark stream processing, Airflow/Dagster workflow orchestration, Airbyte-style SaaS ELT catalogs, or Debezium/Kafka Connect CDC infrastructure. It can complement those systems or replace lighter hand-written sync jobs.
- Avoid roadmap or implementation choices that turn the project into a partial stream-compute platform: generic keyed state APIs, arbitrary processing-time timers, full SQL planners, Flink-compatible savepoints, cross-sink exactly-once transactions, and complex sliding/session window semantics are out of the near-term core.
- Prefer improving reliability, first-run usability, connector certification, plugin contracts, and lightweight operations over adding many unverified connectors.
- Public claims must match tested maturity. Default semantics are at-least-once; production guidance should rely on business keys, versions, upserts, ReplacingMergeTree-style sinks, or explicit deduplication to absorb replay.

## Roadmap Execution Discipline
- Treat `docs/ROADMAP.zh.md` as the execution backlog, not as an open brainstorming document. For implementation work, pick an existing roadmap item and drive it to its stated acceptance criteria before expanding scope.
- Do not broaden the roadmap while implementing a task. New ideas, adjacent connector work, extra UI flows, or larger architectural changes should be captured only when the user explicitly asks for roadmap planning, or when the current task is blocked by a concrete missing prerequisite.
- Keep work-in-progress narrow: one primary roadmap item at a time. Finish it, verify it, or mark the precise blocker before starting another roadmap item.
- Roadmap changes must preserve priority. Adding a new item must not silently displace the current top task, current phase goals, or existing acceptance criteria. If priority should change, call that out explicitly and get user direction.
- When a request touches an area outside the current roadmap, default to the smallest change that satisfies the request. Do not convert it into a new product line, connector family, or broad refactor unless the user explicitly asks.
- For each completed roadmap item, update evidence rather than only adding new plans: tests run, e2e coverage, docs updated, maturity metadata changed, or known residual gaps.
- If a task reveals more work than expected, split follow-ups into a bounded backlog section and continue finishing the original deliverable. Avoid repeatedly rewriting acceptance criteria midstream.

## Roadmap-to-Delivery Workflow

All implementation work follows one closed loop:

```text
sync current state -> claim one roadmap item -> split bounded increments
    -> implement a vertical slice -> test at acceptance scope
    -> reconcile evidence and update roadmap -> claim the next item
```

The loop is a delivery rule, not a suggestion. Do not start coding from a stale summary, an untracked idea, or a test failure without first mapping it to a roadmap item or an explicitly authorized bounded follow-up.

### 1. Sync before claiming

- Read the current `docs/ROADMAP.zh.md`, the relevant component/reliability documents, and the working tree status before selecting work. Treat the current files and commands as authoritative; previous conversation summaries are navigation hints only.
- Identify the current highest-priority item, its status, dependencies, acceptance criteria, and required evidence. Confirm whether an `active` item already exists.
- Inspect existing user changes with `git status`/`git diff` and preserve them. Do not reset, checkout, clean, or overwrite unrelated work.
- Check whether the task is for the standalone production profile, a connector/path certification, the distributed profile, UI/onboarding, or another explicitly named roadmap scope. Do not use evidence from one profile to claim another.

### 2. Claim exactly one item

- Claim the highest-priority eligible `queued` item, or continue the already `active` item. Never claim two primary items at once.
- A `blocked_external` item is not eligible until its documented credential/service/authorization input is available. Record the missing input and the unblock condition; do not silently replace the current top priority with a lower item.
- Before implementation, record a short claim in the task/PR or work log: roadmap ID, objective, why it is eligible now, scope, non-goals, dependencies, and intended evidence.
- Set the roadmap status to `active` only for the one item actually being worked. Do not invent `in_progress`; the allowed states are `active`, `blocked_external`, `queued`, `delivered`, and `deferred`.
- Changing priority, moving an item ahead of the current top task, or turning a follow-up into a new roadmap item requires an explicit user/product decision and a note in the roadmap.

### 3. Split the requirement before coding

For the claimed item, create bounded increments (for example `PR-0.1`, `PR-0.2`, `PR-0.3`). Each increment must state:

- one observable outcome and the smallest vertical slice that can deliver it;
- explicit non-goals and the interfaces/files it is allowed to touch;
- acceptance checks, test data, failure/recovery scenarios, and the evidence artifact to update;
- data semantics and rollback behavior, especially for checkpoint, DLQ, replay, idempotency, schema drift, and secret handling;
- dependencies and a precise external blocker, if any.

Do not split only by file or layer. A valid increment closes a user-visible or operationally verifiable behavior from configuration/API/UI through runtime and persistence where applicable. If the split reveals adjacent work, put it in a bounded follow-up section and keep the current acceptance criteria unchanged.

### 4. Preflight the implementation

- Locate the existing implementation, registration path, config/schema descriptor, docs, and nearest tests before editing. Prefer extending the established `Source -> Transform -> Sink` contract over adding a parallel execution model.
- For a data path, write down the source position, checkpoint boundary, sink acknowledgement, duplicate strategy, DLQ behavior, and restart/reset expectation before changing code.
- For a security or storage change, identify plaintext/secret paths, migration compatibility, backup/restore impact, and failure-atomicity requirements before coding.
- For UI work, use the real descriptor/introspection/preflight/API spec; do not create a static UI-only execution semantic.

### 5. Implement the smallest vertical slice

- Make the smallest change that satisfies the claimed increment, including necessary tests and documentation. Avoid opportunistic refactors, new connectors, broad abstractions, or unverified maturity upgrades.
- Preserve the documented at-least-once contract. A retry, replay, checkpoint, or sink change must not silently turn a possible duplicate into a possible loss.
- Keep production claims conservative while evidence is incomplete: use `beta`, `experimental`, `production_with_review`, or an explicit warning instead of changing metadata to `production` early.
- Treat persistence and lifecycle errors as user-visible outcomes. Do not ignore save, delete, checkpoint, migration, or task-state errors merely to keep a happy-path test green.
- Update the relevant component/runbook/evidence document in the same delivery slice when behavior or operational requirements change.

### 6. Test and verify at the acceptance scope

Run the narrowest useful checks first, then broaden them in proportion to risk:

1. formatting, static checks, and targeted unit tests;
2. package integration/conformance tests and `-race` where concurrency or lifecycle state is involved;
3. backend-specific tests for every storage/connector backend still advertised by the item;
4. the relevant `hack/e2e-*.sh`, UI, crash/restart, outage, checkpoint-reset, DLQ/replay, and duplicate-absorption scenarios;
5. real external-environment certification when the roadmap item requires credentials or a remote service.

For every result, record `passed`, `failed`, `skipped`, or `blocked` with the reason. A missing DSN, image, service, or credential is a skip/blocker, not a pass. A narrow unit test cannot certify a cross-process, multi-backend, or production connector requirement.

Data-path changes must test, where applicable:

- normal write and source/sink schema or preflight validation;
- crash after sink acknowledgement and before checkpoint commit;
- application restart and checkpoint reset/replay;
- source/sink/Redis outage, retry/backoff, DLQ persistence and replay;
- duplicate absorption or the explicitly documented duplicate boundary;
- schema drift, delete/update semantics, and reconciliation by business key/version.

Use the repository's container selection rules and commands. If local Go is unavailable, use the documented `CONTAINER_CLI` invocation; do not substitute an untracked local toolchain result for the required container or e2e evidence.

### 7. Reconcile the result against the roadmap

Before declaring completion, create an acceptance matrix:

| Criterion | Evidence (command/file/run) | Result | Residual or blocker |
| --- | --- | --- | --- |
| stated acceptance criterion | exact test/e2e/doc/release evidence | pass/fail/skip/block | concrete next action |

Then inspect the final diff and run `git diff --check`. Confirm that:

- no secret, token, password, generated artifact, or debug/demo control was introduced into the wrong surface;
- API response, in-memory lifecycle, storage rows, checkpoint/DLQ state, docs, metrics, and maturity metadata agree;
- the test scope matches the claim scope (standalone vs distributed, connector path, storage backend, and release version);
- known residual gaps are written down rather than hidden by a broad “passed” statement.

### 8. Close, hand off, and loop

- Mark an item `delivered` only when every mandatory acceptance criterion has passing evidence and the required docs/changelog/maturity metadata are updated. “Code merged” or “unit tests pass” alone is not completion.
- Keep the item `active` when implementation or required evidence remains. Mark it `blocked_external` only when the missing condition is genuinely external and the exact input, owner, and unblock test are recorded. Do not mark work blocked merely because it is difficult or incomplete.
- If the original scope is complete but follow-up work remains, add a bounded follow-up with its own acceptance criteria; do not expand the completed item retroactively.
- After `delivered`, return to the top of `docs/ROADMAP.zh.md`, select the next eligible item by preserved priority, and repeat from step 1. Do not automatically start the next item in the same change if the user has not authorized a priority switch.
- If no eligible item remains, leave the roadmap evidence current and provide a handoff listing completed work, skipped/blocked checks, residual risks, and the next recommended claim.

### Default autonomous round limit

- Unless the user specifies otherwise, one roadmap execution request has a default limit of **5 rounds**.
- One round means one complete bounded claim-to-close cycle: claim one roadmap increment, implement it, run its acceptance checks, and close or hand it off. Tool calls, individual test retries, and planning substeps do not count as extra rounds.
- Record the counter as `Round: n/5` in the work log or handoff. Do not claim a sixth increment automatically after `Round: 5/5`.
- At the limit, finish only the safe closeout of the current round, update roadmap evidence, and stop with a handoff listing the next eligible item. Continue, reset, or extend the counter only after an explicit user prompt; do not infer permission from silence.
- If the user specifies a different limit (including `0` for planning-only), that limit takes precedence for the request. A later explicit “continue” starts a new five-round window unless the user supplies another number.
- If a round becomes `blocked_external`, it still consumes the round and must end with the exact missing input, owner, unblock test, and next claim; do not spin on the same blocker within the remaining rounds.

### Minimal claim/close record

Use this compact record in a PR, task note, or final handoff:

```text
Round: <n>/<limit>
Roadmap item: <P0 / PR-0.1 / ...>
Profile/path: <standalone|distributed|connector path>
Objective: <one observable outcome>
Scope: <files/components allowed>
Non-goals: <explicit exclusions>
Acceptance: <numbered checks>
Evidence: <commands, e2e, docs, release record>
Result: <delivered|active|blocked_external>
Residual/follow-up: <bounded next item or none>
```

- Local Go may be unavailable; use `CONTAINER_CLI="${CONTAINER_CLI:-$(command -v docker || command -v podman)}"; "$CONTAINER_CLI" run --rm -v "$PWD:/workspace" -v openetl-go_go-cache:/go -v openetl-go_go-build-cache:/root/.cache/go-build -w /workspace etl-go-dev:latest sh -c "go test ./..."` for tests.

## Container Runtime Selection
- `hack/container-cli.sh` is the single source of truth for which container CLI hack scripts, e2e tests, `make image`, and `./hack/pack.sh` use.
- Default detection order is **docker first, then podman**. Pick the CLI that exists in your current environment: most Linux/CI/macOS-Docker-Desktop setups have `docker`; rootless or daemonless setups (some Linux workstations, CI runners without a Docker daemon) typically have `podman`.
- Force one explicitly via `export CONTAINER_CLI=docker` or `export CONTAINER_CLI=podman`. This is required when both are installed but you need the non-default one (e.g. Docker Desktop installed alongside podman).
- Compose subcommand resolution: native `docker compose` / `podman compose` is preferred; `docker-compose` / `podman-compose` fallback is auto-selected when the native plugin is missing.
- All `hack/e2e-*.sh` scripts, `make image`, `./hack/pack.sh`, and the compose files honor `CONTAINER_CLI`. Document examples default to `docker`; swap to `podman` only when the local environment requires it.

## Commands
- `make build` is the safest build path: it installs the GoFrame CLI if missing, then runs `gf build -ew` using `hack/config.yaml`.
- `main.go` imports generated `github.com/a8851625/openetl-go/internal/packed`, and `hack/config.yaml` packs `resource/` into `internal/packed/packed.go` during GoFrame builds. Plain `go build ./...` works if `internal/packed/packed.go` exists (it is committed as a 15-byte stub).
- Go module is `github.com/a8851625/openetl-go`, with `go 1.24.0` and `toolchain go1.24.13` in `go.mod`.
- Local Go may be unavailable; use `CONTAINER_CLI="${CONTAINER_CLI:-$(command -v podman || command -v docker)}"; "$CONTAINER_CLI" run --rm -v "$PWD:/workspace" -v openetl-go_go-cache:/go -v openetl-go_go-build-cache:/root/.cache/go-build -w /workspace etl-go-dev:latest sh -c "go test ./..."` for tests.
- `make test` runs unit tests with `-race`; `make test-quick` runs only `./internal/etl/...`; `make test-integration` requires MySQL + ClickHouse containers.
- `hack/e2e.sh` builds the app image and validates file→file, MySQL batch→file, and MySQL batch→MySQL via the detected container runtime (`docker` or `podman`; override with `CONTAINER_CLI`).
- `hack/e2e-mysql-postgres.sh` validates MySQL batch custom-query/JOIN→PostgreSQL upsert, schema preflight rejection, and checkpoint reset replay absorption.
- `hack/e2e-cdc-mysql.sh` validates MySQL CDC→MySQL.
- `hack/e2e-cdc-postgres.sh` validates MySQL CDC→PostgreSQL insert/update/delete and checkpoint stop/restart recovery.
- `hack/e2e-clickhouse.sh` validates MySQL CDC→ClickHouse; it requires `docker.io/clickhouse/clickhouse-server:24.3-alpine` locally.
- `hack/e2e-dlq.sh` validates DLQ list/replay against MySQL.
- `hack/e2e-snapshot-cdc.sh` validates integrated MySQL snapshot+CDC→MySQL.
- `hack/e2e-clickhouse-autocreate.sh` validates ClickHouse auto-create and schema drift add-columns.
- `hack/e2e-snapshot-cdc-clickhouse.sh` validates MySQL snapshot+CDC→ClickHouse, including schema drift, checkpoint restart recovery, checkpoint reset replay absorption, and ClickHouse outage DLQ/replay.
- `hack/e2e-s3-minio.sh` validates S3 sink against MinIO, including deterministic content-addressed object replay after checkpoint reset.
- `hack/e2e-http-source.sh` validates HTTP source pagination/auth headers.
- `hack/e2e-auth-audit.sh` validates ETL API token auth and audit logging.
- `hack/e2e-crash-recovery.sh` validates committed checkpoints and container crash recovery.
- `hack/e2e-cdc-crash-recovery.sh` validates MySQL CDC checkpoint restart behavior.
- `hack/e2e-duplicate-spec.sh` validates duplicate pipeline spec detection.
- `hack/e2e-api-conflict.sh` validates duplicate runtime pipeline create conflicts.
- `hack/e2e-kafka.sh` validates Kafka source/sink via Redpanda (file→Kafka, Kafka→file).
- `hack/e2e-lookup-state.sh` validates Kafka→lookup→ClickHouse lookup StateStore crash recovery when the dimension query becomes unavailable after restart.
- `hack/e2e-wide-table.sh` validates Kafka JSON/Debezium→lookup/deduplicate/window→ClickHouse, including duplicate absorption, lookup DLQ/replay, ClickHouse outage DLQ/replay, and SIGKILL state recovery for deduplicate/window.
- `hack/e2e-kafka-raw-ods.sh` validates Kafka raw protocol messages -> Lua `flat_map` parser -> MySQL `lookup` -> `project`/`type_convert` -> Kafka ODS, including parser DLQ, lookup-miss DLQ, and offset replay append-duplicate boundary.
- `hack/e2e-debezium-mysql.sh` validates Debezium Kafka CDC→MySQL ODS upsert with source include/exclude, snapshot/delete skips, DDL drop/reject policy, Kafka offset replay, Redpanda broker restart recovery, consumer group rebalance recovery, MySQL lock-wait retry, and DLQ replay after a MySQL value-range write failure.
- `hack/e2e-ui.sh` validates the built React UI served by the app container using Playwright CLI.
- `hack/e2e-elasticsearch.sh` validates Elasticsearch/OpenSearch bulk indexing and mapping-conflict item DLQ/replay (mysql_batch→ES).
- `hack/e2e-snapshot-cdc-crash.sh` validates snapshot+CDC crash recovery during both snapshot and CDC phases.
- `hack/e2e-storage-mysql.sh` / `hack/e2e-storage-postgres.sh` validate MySQL/PostgreSQL storage backends.
- `hack/e2e-distributed.sh` validates master-worker distributed dispatch with 2 workers.
- `make image TAG=<tag>` builds a container image through the detected runtime (`docker` or `podman`; override with `CONTAINER_CLI`); without `TAG`, it uses the short git SHA and appends `.dirty` when the worktree is dirty. `make image.push` pushes the image.
- Frontend source lives in `web/`; run `npm install` and `npm run build` there to regenerate `resource/public`.

## Runtime Config
- Main config lives at `manifest/config/config.yaml`; container Compose deployments instead mount `./config.yaml` to `/app/manifest/config/config.yaml`.
- Runtime config priority is CLI flags > environment variables > config file > built-in defaults. `./openetl-go --help` documents `--config`, directory flags, HTTP/ETL API bind flags, storage, TLS/auth/audit, and master/worker role flags.
- Pipeline specs are YAML files under `pipes/` or `testdata/pipes-*`; they are loaded into the storage backend on startup. Hot-reload via `fsnotify` watches the `specsDir` for new YAML files.
- Sources/sinks/transforms register by blank imports in `internal/logic/app/app.go`; extism WASM plugins are loaded from the `plugins` table in the storage backend at startup.
- ETL API auth is enabled by `ETL_API_TOKEN`, `etl.apiToken`, or `--api-token`; clients may use `X-API-Token` or `Authorization: Bearer <token>`. Audit logs are persisted in the configured SQL storage backend and can be disabled with `ETL_AUDIT_ENABLED=false`, `etl.audit.enabled: false`, or `--audit-enabled=false`.
- Runtime operations API includes spec validation (`POST /api/v2/specs/validate`), connection test (`POST /api/v2/connections/test`), transform dry-run (`POST /api/v2/transforms/dry-run`), spec reload (`POST /api/v2/specs/reload`), audit listing (`GET /api/v2/audit?limit=N`), and plugin schema (`GET /api/v2/plugins/schema`).
- ETL API server uses a long-lived server context (`s.ctx`) for pipeline lifecycle; HTTP request context is NOT passed to `runner.Start()` because it gets cancelled when the HTTP response returns.
- At-least-once delivery and sink idempotency expectations are documented in `docs/etl-idempotency.md`.
- YAML spec shape and connector config fields are documented in `docs/etl-config-schema.md`.
- Plugin config schema is available at `GET /api/v2/plugins/schema` with typed fields, required markers, defaults, secret flags, and examples for every connector.
- Preflight opens sinks with a short timeout for real reachability checks; generic sink outages are returned as warnings while writer-disabled experimental sinks such as MaxCompute remain blocking errors. Preflight responses include structured `field_issues` and best-effort target `ddl_preview` when source schema metadata is available.
- Pipeline metrics include: `source_read_latency_ms`, `sink_write_latency_ms`, `last_batch_size`, `avg_batch_size`, `batch_count`, `checkpoint_age_seconds`, `dlq_file_count`, `dlq_replay_count`, `dlq_delete_count`, `cdc_lag_ms`.
- Spec validation (`POST /api/v2/specs/validate`) returns idempotency warnings for dangerous source/sink combinations (CDC+file, batch+insert, etc.).
- `TZ` is honored at startup; `TZ=CST-8` is special-cased to a fixed UTC+8 zone.
- Storage backend defaults to SQLite (`./data/etl.db`); configured via `etl.storage.type: sqlite|mysql|postgresql` in `config.yaml`. All checkpoints, DLQ, audit logs, pipeline specs, run history, workers, and plugins are persisted to the SQL database.

## Architecture Notes
- Startup path is `main.go` → `internal/cmd/cmd.go`; `cmd.Main` sets up structured logging, starts the ETL server asynchronously, then runs the GoFrame HTTP server.
- GoFrame server (`:8000`) proxies `/api/v2/*` and `/metrics` to the ETL API server (`:8001`) via `httputil.ReverseProxy` so the production UI works without Vite dev proxy.
- MySQL sink and PostgreSQL sink use transaction-bounded batch writes (`BEGIN`/`COMMIT`) for atomicity; DLQ and audit records are persisted through the configured storage backend.
- Pipeline goroutines have panic recovery; readLoop has exponential backoff on persistent errors (1s→30s cap).
- HTTP server has body size limit (10MB), panic recovery middleware, constant-time token comparison, ReadTimeout/WriteTimeout/IdleTimeout.
- Dockerfile runs as non-root user `etl` (uid 1001), has HEALTHCHECK, and `.dockerignore` excludes build artifacts.
- Alert manager is fully async with a 256-item buffered channel; overflows are dropped rather than blocking pipeline writes.
- **ETL architecture** lives under `internal/etl`: `core` interfaces, `pipeline.Runner` + `ParallelRunner`, checkpoint store, DLQ writer, plugin `registry`, sources, sinks, transforms, and API v2 server.
- **Storage layer** (`internal/etl/storage/`): SQLite/MySQL/PostgreSQL-backed persistence for pipelines, checkpoints, DLQ, audit, run history, workers, tasks, and plugins. Factory in `storage/factory/` selects backend by config.
- **DAG Orchestrator** (`internal/etl/orchestrator/`): `PipelineSpec` with DAG (nodes/edges/conditions), `DAGExecutor` for parallel multi-source/multi-sink execution, `Scheduler` for cron/periodic/streaming/dependency triggers, `ConvertLinearSpec` for backward-compatible YAML loading. `Server.StartAll` wires scheduled specs into this scheduler: deferred `cron`/`periodic`/`dependency` pipelines are marked `scheduled` and are not started immediately; nil/`once`/`streaming` schedules still start immediately. The scheduler drives any `pipeline.RunnerInterface`, so both linear and DAG specs are covered.
- **Plugin System** (`internal/etl/plugin/pluginsystem/`): extism WASM runtime with install/unload/exec. TypeScript SDK in `web/plugin-sdk/`.
- **Master-Worker** (`internal/etl/master/` and `internal/etl/worker/`): Worker registration, heartbeat health checks, slot management. Standalone mode runs both in one process; cluster mode supports HTTP+WebSocket distributed execution with shard dispatch and crash reassignment.
- Worker management API: `GET/POST /api/v2/workers`, `POST /api/v2/workers/{id}/heartbeat`, `DELETE /api/v2/workers/{id}/deregister`.
- Plugin install/uninstall API: `POST /api/v2/plugins/install` (multipart form: wasm, name, kind, version), `DELETE /api/v2/plugins/{name}`, `GET /api/v2/plugins/{name}`.
- Implemented ETL sources: `mysql_cdc`, `mysql_batch` (supports custom `query` for JOIN), `mysql_snapshot_cdc`, `postgres_cdc` (pgoutput parsing), `kafka`, `file`, `http`, `redis`.
- Implemented ETL sinks: `clickhouse` (native + HTTP, auto-create, DDL translation), `mysql` (batch INSERT/upsert, auto-create), `postgres`/`postgresql` (insert/upsert via pgx, auto-create), `doris` (Stream Load + MySQL DELETE, auto-create), `elasticsearch`/`es` (bulk API, round-robin, 429 Retry-After), `kafka` (sync producer, idempotent), `redis` (HASH/STRING/LIST), `s3`/`file_sink` (MinIO-compatible, multipart, Parquet support), `jdbc` (any JDBC database).
- Experimental sink contract: `maxcompute`/`odps` is registered for descriptor/config/schema/partition validation and preflight blocks writer-disabled pipelines; the SDK-backed batch writer, remote permission/table checks, DLQ/retry e2e, and production maturity are not implemented yet.
- Implemented transforms: `filter` (expression engine), `deduplicate`, `validate`, `type_convert`, `project`/`select_fields`, `flat_map`/`udtf` (Lua-backed first ABI), `debezium_cdc`, `cdc_policy`/`ddl_guard`, `rename`, `drop_field`, `add_field`, `enricher`, `lookup`, `join`, `window`, `router`, `fanout`, `tap`, `rate_limiter`, `lua` (gopher-lua), `javascript`/`typescript` (QuickJS, CGO), WASM plugins (extism).
- `internal/logic/logic.go` blank-imports logic packages so their `init()` functions register service implementations.

## Build Tags
| Tag | Effect | Default? |
|------|------|------|
| *(none)* | Pure Go core + all sinks/sources + Lua (gopher-lua, pure Go) | ✅ |
| `-tags=extism` | + WASM plugin runtime (wazero, pure Go) | — |
| `-tags=nolua` | Strip Lua runtime for smaller binary | — |
| `CGO_ENABLED=1` | + JavaScript/TypeScript transform (QuickJS, CGO) | — |

## Generated And Vendored Assets
- `internal/service/*.go` and `internal/model/entity/*.go` carry GoFrame generated-file headers. Prefer updating the source shape and regenerating with `make service` / `make dao` when appropriate.
- `resource/public` is built frontend output embedded into the Go binary. New ETL UI source is in `web/` and uses Vite/React/Tailwind with sidebar navigation, pages (Dashboard/Pipelines/Designer/DLQ/Plugins/Audit/DAG Editor/Workers/Schedules), i18n (EN/ZH) via `web/src/i18n.ts`, auto-refresh, and toast notifications.
- Production deployment is via Compose, not Kubernetes manifests: `docker-compose.yml` (standalone: app + MySQL metadata + Redis state, with API token / spec encryption / TLS / audit / DLQ TTL / alerting / resource limits / JSON logs aligned to `docs/quickstart.zh.md` production checklist) and `docker-compose.distributed.yml` (master + scalable workers sharing MySQL + Redis). `docker-compose.quickstart.yml` is the MySQL-CDC→ClickHouse demo; `docker-compose.dev.yml` is the local dev harness with all dependency services. Production config example at `manifest/examples/config.production.yaml`; runtime values are injected via env (`ETL_API_TOKEN`, `ETL_STORAGE_TYPE`/`ETL_STORAGE_DSN`, `ETL_STATE_REDIS_*`, `ETL_TLS_*`, `LOGGER_FORMAT=json`, etc.).
