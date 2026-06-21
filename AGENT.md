# AGENT.md

## Mission
- Build this repository into a mature single-node ETL system, not just a CDC demo.
- Work down `TODO.md` in priority order: finish P0 before widening connector scope.
- A feature is not complete until it has code, unit tests where useful, and a Podman/Podman Compose E2E path.

## Development Rules
- Use containers for Go commands if local Go is missing: `podman run --rm -v "$PWD:/workspace" -v openetl-go_go-cache:/go -v openetl-go_go-build-cache:/root/.cache/go-build -w /workspace etl-go-dev:latest sh -c "go test ./..."`.
- Use Podman and Podman Compose for all services in E2E tests; do not require host-installed MySQL, ClickHouse, Kafka, MinIO, Elasticsearch, or PostgreSQL.
- Keep `Dockerfile` production-runnable and `Dockerfile.dev` suitable for local test/build loops.
- Keep `manifest/config/config.yaml` and `pipes/*.yaml` aligned with actual container hostnames and ports.
- Do not mark TODO items complete without running the relevant E2E script.
- After each TODO batch is completed, immediately re-review the system against mature ETL criteria and add newly discovered gaps to `TODO.md` before starting another batch.
- Treat `TODO.md` as a living backlog, not a one-time plan. Do not stop just because the current visible TODO batch is complete; create the next batch from the residual gaps and continue unless explicitly told to pause.
- Every iteration must end with updated TODO checkboxes, updated agent guidance if rules changed, `go test ./...`, relevant Podman E2E scripts, and a concise residual-risk summary.
- If an item is externally blocked, keep it unchecked and record the blocker in the final summary or next to the TODO. Do not mark blocked work complete.
- Frontend source lives in `web/` and builds with Vite/React/Tailwind into `resource/public`; do not edit `resource/public` directly unless regenerating build output.
- Use shadcn/ui design conventions for UI work: tokenized colors, rounded cards, accessible buttons/inputs, clear empty/error states, and responsive layouts.
- After frontend changes, run `npm install` if dependencies changed and `npm run build` from `web/`, then verify `resource/public/index.html` and assets are updated.

## Iteration Loop
- Select the highest-priority unchecked TODO that is not externally blocked.
- Implement the smallest correct production-oriented change.
- Add or update unit tests where useful.
- Add or update a Podman/Podman Compose E2E script.
- Run `go test ./...` and the relevant E2E script.
- Mark the TODO complete only after verification passes.
- Re-review mature ETL gaps and append the next missing capabilities to `TODO.md`.
- Continue the loop.

## Architecture Targets
- Core abstractions live under `internal/etl/core`: `Source`, `RecordReader`, `Transform`, `Sink`, and `CheckpointStore`.
- Runtime orchestration lives under `internal/etl/pipeline`; reliability features belong there or in `checkpoint`, `retry`, `dlq`, and `alert` packages.
- Plugins register through `internal/etl/registry`; add new sources/sinks/transforms by registering builders in package `init()` and blank-importing packages from `internal/logic/app/app.go`.
- ETL API v2 lives under `internal/etl/server`; operational features such as DLQ replay, checkpoint reset, reload, and connection tests should be exposed there.
- Pipeline specs are YAML files under `pipes/` or `testdata/pipes-*`; use E2E-specific spec directories to isolate tests.

## Reliability Requirements
- Default runtime semantics are at-least-once with sink idempotency where possible.
- Checkpoint must only advance after sink writes succeed.
- Filtered records are normal drops and must not go to DLQ.
- Permanent data errors should go to DLQ; transient connection errors should retry with backoff.
- DLQ must support list, replay, and delete before production trial.

## Required E2E Coverage
- `hack/e2e.sh`: file->file, MySQL batch->file, MySQL batch->MySQL.
- `hack/e2e-cdc-mysql.sh`: MySQL CDC->MySQL.
- `hack/e2e-clickhouse.sh`: MySQL CDC->ClickHouse with `docker.io/clickhouse/clickhouse-server:24.3-alpine`.
- Add new `hack/e2e-*.sh` scripts for MinIO/S3, HTTP source, Kafka, Elasticsearch, PostgreSQL CDC, crash recovery, and DLQ replay.
- Current added E2E scripts: `hack/e2e-dlq.sh`, `hack/e2e-snapshot-cdc.sh`, `hack/e2e-clickhouse-autocreate.sh`, `hack/e2e-s3-minio.sh`, `hack/e2e-http-source.sh`, `hack/e2e-auth-audit.sh`, `hack/e2e-crash-recovery.sh`, `hack/e2e-cdc-crash-recovery.sh`, `hack/e2e-duplicate-spec.sh`, `hack/e2e-api-conflict.sh`, and `hack/e2e-kafka.sh`.

## Frontend Targets
- The UI must cover visual pipeline configuration, task operations, observability, checkpoints, DLQ operations, plugin discovery, and API token handling.
- Prefer progressive enhancement: keep YAML editable even when visual forms/canvas are incomplete.
- Any UI feature that mutates runtime state must call ETL API v2 and show success/error feedback.
