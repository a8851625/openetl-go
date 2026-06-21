# openetl-go · Production-Ready SPEC

> **Status**: Draft v1 · 2026-06-21
> **Scope**: Defines the contract for openetl-go to be considered production-ready, given three balanced goals — **数据同步可靠 · 易用 · 轻量**.
> This document is the source of truth for architecture, commands, structure, style, testing, and hard boundaries. Code must conform to it; deviations require updating this file.

---

## 1. Objective

### 1.1 What this is

openetl-go is a **single-binary, plugin-based ETL/CDC engine** built around three abstractions:

```
Source ──► Transform ──► Sink
```

It captures changes from databases (MySQL binlog, PostgreSQL logical replication), streams, files, and HTTP; applies a chain of transforms; and writes to analytical stores (ClickHouse, MySQL, PostgreSQL, Doris, Elasticsearch, Kafka, Redis, S3). The pipeline framework (`internal/etl/`) is the primary product surface. The legacy `internal/logic/sync/` Canal path is retained only for backward compatibility and is not the focus.

### 1.2 Two operating modes

The binary runs in **two modes**, selected by `etl.storage.type` in config:

| Mode | Storage | Distributed (master-worker) | Intended use |
|------|---------|------------------------------|--------------|
| **Demo / single-node** | `sqlite` (default, zero-dependency) | **Degraded** — shards run inline in one process; no inter-node dispatch | Local evaluation, single-host sync, CI |
| **Scalable** | `mysql` or `postgresql` | **Enabled** — multiple binaries share metadata, master dispatches shards to workers | Production horizontal scale |

This dual-mode is the central architectural decision. SQLite is the zero-friction entry point; MySQL/PG is the scale-out path. **The production-ready bar applies to both modes**, with distributed guarantees only claimed in scalable mode.

### 1.3 Target users

- **Primary**: open-source community — operators and engineers syncing OLTP → OLAP (MySQL → ClickHouse is the canonical case).
- **Expectations**: 5-minute quickstart from README, runnable examples, clear failure messages, no hidden state. Docs and ergonomics are first-class.

### 1.4 The three balanced goals

When goals conflict, resolve in this order, but all three must reach a minimum bar:

1. **数据同步可靠 (Reliability)** — at-least-once delivery with idempotent sinks; zero silent data loss (hard constraint, §6).
2. **易用 (Usability)** — auto-create target tables, schema validation, clear startup checks, friendly errors.
3. **轻量 (Lightweight)** — single static binary, pure Go, low resource footprint, minimal external dependencies.

### 1.5 Production-ready definition

openetl-go is production-ready when, for **both** operating modes:

- **No data is silently lost** on any write path (retry → DLQ, never bare `continue`).
- **A crash or SIGTERM** never loses committed data and never re-delivers beyond idempotency tolerance.
- **Schema changes** are either auto-applied or clearly rejected — never silently dropped.
- **Health and metrics** are observable via standard interfaces (HTTP `/api/v2/health` returning 503 when unhealthy; Prometheus `/metrics` with correct types).
- **Scalable mode** (MySQL/PG) genuinely distributes shards across nodes with heartbeat-based reassignment.
- **A new user** can go from `git clone` to a working MySQL→ClickHouse sync in under 5 minutes using SQLite mode and the quickstart.

---

## 2. Commands

### 2.1 Build & run

| Command | Purpose |
|---------|---------|
| `make build` | Build via GoFrame (`gf build -ew`) — single static binary |
| `go build -o bin/openetl-go .` | Plain Go build (no GoFrame CLI needed) |
| `./openetl-go` | Run with `manifest/config/config.yaml` |

### 2.2 Testing (the test pyramid)

| Command | Purpose |
|---------|---------|
| `make test` | Unit tests with `-race` across `internal/etl/...`, `internal/logic/...`, `internal/controller/...` |
| `make test-quick` | Same without `-race` (fast dev loop) |
| `make test-pkg PKG=pipeline` | One package, verbose |
| `make test-integration` | Integration tests with podman-compose services (MySQL + ClickHouse) |

Integration tests use the **`integration` build tag** and require live databases:

```bash
CLICKHOUSE_HOST=... MYSQL_HOST=... go test -tags=integration ./internal/etl/sink/...
```

### 2.3 Dev environment

- **podman** is the supported dev/container runtime (`podman compose -f docker-compose.dev.yml`).
- `go-dev` container (golang:1.24-alpine) mounts the workspace and is where builds/tests run.
- Required dev services: `mysql-source` (binlog enabled), `clickhouse`, plus `minio`/`redpanda` as needed.

### 2.4 API surface

- **ETL API** (`:8001`, proxied through `:8000/api/v2/*`): pipeline CRUD, start/stop/pause/resume, checkpoint, DLQ replay, spec validation, connection test, plugin management, AI generation.
- **Observability**: `/api/v2/health` (503 when unhealthy), `/metrics` (Prometheus), `/api/v2/metrics` (JSON).
- **Legacy monitor API** (`:8000/api/monitor/*`): retained but not the primary surface.

---

## 3. Project Structure

### 3.1 Layout

```
openetl-go/
├── main.go, internal/cmd/           # entry + CLI
├── internal/etl/                    # PRIMARY product surface
│   ├── core/                        # Source/Sink/Transform/Record interfaces
│   ├── pipeline/                    # Runner, ParallelRunner, circuit breaker, metrics
│   ├── orchestrator/                # DAG executor (node-based pipelines)
│   ├── source/                      # 8 source plugins
│   ├── sink/                        # 9 sink plugins
│   ├── sink/typing/                 # unified column-type inference (cross-sink)
│   ├── sink/ddl/                    # DDL dialect translation
│   ├── transform/                   # 15+ transform plugins
│   ├── registry/                    # plugin builder registry
│   ├── storage/{sqlite,mysql,postgres}/  # metadata persistence backends
│   ├── server/                      # HTTP API + reconciliation + hot-reload
│   ├── master/, worker/             # distributed dispatch (scalable mode)
│   ├── alert/, dlq/, retry/, checkpoint/, telemetry/, plugin/
├── internal/logic/{app,sync,monitor}/  # app bootstrap + legacy Canal + monitor
├── internal/controller/monitor/     # legacy monitor HTTP API
├── internal/service/, model/entity/ # GoFrame-generated interfaces & entities
├── pipes/                           # example YAML pipeline specs
├── manifest/config/config.yaml      # default config
├── hack/                            # E2E scripts, release tooling
├── Dockerfile, docker-compose*.yml  # deployment
└── SPEC.md                          # this file
```

### 3.2 The plugin contract (the heart of the system)

Every source/sink/transform implements a small core interface (`internal/etl/core/core.go`) and registers via `registry.Register*` in `init()`. Optional interfaces extend behavior:

- `core.SchemaDescriptor` — source exposes its output schema (enables validation + auto-create).
- `core.SchemaValidator` — sink validates source schema at startup.
- `core.SinkMetricsProvider` — sink exposes per-sink write metrics.
- `core.RecordCheckpointer` — reader produces per-record checkpoints (at-least-once).

**Rule**: capability is declared by implementing an interface, not by string metadata. The `server.go` `pluginMetadata()` table is advisory only and must not diverge from actual interface implementation.

### 3.3 Dual-mode storage boundary

`storage/factory.NewStore` selects the backend from config. All state (pipeline specs, checkpoints, DLQ, audit, worker registry, run history) goes through the `storage.Storage` interface — **never** direct file I/O from production paths. The legacy `checkpoint/` and `dlq/` file-based writers exist only for one-time migration and must not be called from active code.

### 3.4 Distributed dispatch (scalable mode only)

`master.TaskDispatcher` + `worker` implement shard dispatch when storage is MySQL/PG:

- Master extracts shards from a `ParallelRunner`, records them in the shared `tasks` table.
- Workers long-poll `POST /api/v2/workers/{id}/poll` for assignment.
- Heartbeat timeout (default 60s) triggers reassignment.
- **In SQLite mode, dispatch is short-circuited**: shards run inline. No false claims of distribution.

---

## 4. Code Style

### 4.1 Go conventions

- **Go 1.24**, modules, `gofmt` + `goimports`. Match the surrounding file's idiom and comment density.
- **Errors**: wrap with `%w` for propagation; classify via `core/errors.go` (transient/data/schema/auth/config). Surfaces that handle errors must decide retryable vs fatal.
- **Context**: all I/O functions take `ctx context.Context` as the first param and respect cancellation. No `context.Background()` inside hot paths except where a deliberate timeout is forked.
- **Concurrency**: shared mutable state behind a mutex or atomics. The pipeline runner uses a bounded channel for backpressure — sinks must never block indefinitely without a context check.

### 4.2 The zero-loss rule (hard constraint, §6)

**No write path may silently drop a record.** Concretely:

- On sink `Write` failure: retry with backoff (`retry.Do`); on exhaustion, route each record to DLQ; if DLQ write itself fails, escalate (alert + halt the pipeline), never `continue`.
- The legacy `logic/sync/handler.go` `continue`-on-error pattern is a known violation to be phased out; new code must not replicate it.
- Idempotency is the complement: sinks must tolerate re-delivery (upsert / version columns / dedup keys) so at-least-once is observationally exactly-once.

### 4.3 Error messages for humans

Open-source users see these errors. Rules:

- State **what** failed, **where** (which plugin/table/pipeline), and **why** (the underlying cause).
- When a startup check can fail, offer the remediation: e.g. `binlog_format must be ROW; run SET GLOBAL binlog_format='ROW'`.
- Never expose raw stack dumps as the primary message; wrap with context.

### 4.4 Interface, not metadata

Prefer typed optional interfaces (`SchemaDescriptor`, `SinkMetricsProvider`) over `map[string]any` capability flags. The only legitimate use of untyped config maps is plugin construction (`NewXSource(config map[string]any)`), where unknown keys are ignored with a debug log, not an error, to keep specs forward-compatible.

---

## 5. Testing Strategy

### 5.1 Pyramid

| Layer | Scope | Tooling | Required for PR |
|-------|-------|---------|-----------------|
| **Unit** | Pure functions, interfaces, type mappers, DDL translation, DAG routing, retry/backoff, circuit breaker | `go test -race` in-package `_test.go` | ✅ All |
| **Integration** | Sink writes against live DBs (type inference, idempotency, auto-create), source checkpoint resume | `_test.go` with `//go:build integration`, podman services | ✅ For changed plugin |
| **E2E** | Full pipeline MySQL CDC → ClickHouse, crash recovery, DLQ replay | `hack/e2e-*.sh` over podman-compose | ✅ For pipeline/core changes |

### 5.2 Test matrix for dual-mode

Every feature touching storage or dispatch must be verified in **both** modes:

| Feature | SQLite (demo) | MySQL/PG (scalable) |
|---------|---------------|---------------------|
| Pipeline CRUD + run | ✅ required | ✅ required |
| Checkpoint save/resume | ✅ required | ✅ required |
| DLQ write/replay/delete | ✅ required | ✅ required |
| Shard dispatch | runs inline (degraded) | ✅ distributed across workers |
| Concurrent pipelines | ✅ single process | ✅ cross-process |

### 5.3 Reliability invariants (always tested)

- **At-least-once**: crash a pipeline mid-batch → on restart, the last batch is re-read (unit + E2E crash-recovery).
- **Idempotency**: replay the same batch to an upsert sink → no duplicates (integration per sink).
- **Graceful shutdown**: SIGTERM mid-write → committed data survives, in-flight batch flushed or safely re-delivered.
- **Version monotonicity**: ClickHouse `_version` strictly increases even under concurrency (unit, race-detector).
- **Zero-loss**: a forced sink failure → record appears in DLQ, never vanishes (unit + integration).

### 5.4 Race and build hygiene

- `-race` is the default for all test commands.
- `go vet ./...` must be clean for any modified package.
- No test may depend on wall-clock ordering where logical ordering suffices; the workflow runtime forbids `time.Now()`/`math.Random()` in scripts — application tests should prefer deterministic seeds.

---

## 6. Boundaries (hard constraints)

These are **non-negotiable**. A change that violates one must be rejected or must first update this SPEC with explicit user sign-off.

### 6.1 Must always do

- **Single static binary, pure Go.** No CGO in the default build; no runtime dependency on JVM/Python/Node for core function (the WASM/Lua/QuickJS plugin runtimes are opt-in sandboxes, not core). New external dependencies require review for binary-size and supply-chain impact.
- **Zero data loss.** Every record either reaches the sink (after retry) or the DLQ. Bare error-swallowing (`continue`, `_, _ = ...`) on data paths is prohibited in new code.
- **Backward-compatible YAML specs.** Existing `pipes/*.yaml` and user specs must keep working across releases. New fields default sensibly; removed fields require a deprecation cycle. `ValidateSpec` rejects unknown plugins but must tolerate unknown config keys.
- **Dual-mode parity of the core.** Anything that works in SQLite mode must also work in MySQL/PG mode (dispatch is the only intentional divergence). Do not implement a feature that silently breaks in one mode.

### 6.2 Ask first about

- New top-level dependencies (any `go get` of a non-trivial library).
- Changes to the `core.Source/Sink/Transform` interfaces — these are the public plugin ABI.
- Auto-apply of DDL by default (`ddl_policy: apply`) — destructive on real schemas; prefer opt-in.
- Anything that adds a required external service (etcd, zookeeper, a message broker beyond what's already integrated).

### 6.3 Never do

- **Do not introduce an external orchestrator dependency** (etcd/ZK/Kubernetes operator) for core function. Distribution uses the shared SQL store + heartbeat, nothing else.
- **Do not silently drop data** to make a test pass or a pipeline "succeed".
- **Do not break the `integration`/unit split** — unit tests must run with no live services; integration tests must be tagged so `make test` stays hermetic.
- **Do not fork the two code paths** (`internal/etl/` vs `internal/logic/sync/`) further. The ETL pipeline framework is canonical; the legacy Canal path is frozen and scheduled for deprecation, not new features.
- **Do not claim distributed guarantees in SQLite mode.** Health/status output must reflect actual capability (degraded dispatch is reported, not hidden).

---

## 7. Production-Readiness Gap → Workstream

Mapping the SPEC's bars to the remaining work. Each item cites the section it satisfies. *(Status reflects the audit on 2026-06-21.)*

### Tier A — Required for "production-ready" claim (both modes)

| ID | Gap | SPEC ref | Status |
|----|-----|----------|--------|
| A1 | LogBuffer formatting bug (args dropped) | §4 | ✅ done |
| A2 | DAG condition operators Gt/Lt/Ge/Le/Regex unimplemented | §3.2 | ✅ done |
| A3 | MySQL/PG/JDBC sink auto-create used all-TEXT | §1.4 易用 | ✅ done (unified `typing`) |
| A4 | Redis source used blocking `KEYS` | §1.5 | ✅ done (SCAN) |
| A5 | Prometheus metrics wrong types (gauge for counters) | §1.5, §3.2 | ✅ done |
| A6 | `_version` non-monotonic under concurrency | §5.3 | ✅ done |
| A7 | DDL `apply` sent raw source DDL to target | §1.4 | ✅ done (DDL translator) |
| A8 | Schema mismatch failed silently at runtime | §1.4, §3.2 | ✅ done (SchemaValidator) |
| A9 | Per-sink metrics only on ClickHouse | §1.5 | ✅ done (SinkMetricsProvider) |
| A10 | MySQL/PG storage backends unverified | §1.5, §3.3, §5.2 | ⬜ **open** — integration test matrix needed |
| A11 | Master-worker dispatch not verified end-to-end | §1.5, §3.4, §5.2 | ⬜ **open** — 2-worker distributed E2E |
| A12 | `make test`/CI scaffolding | §5, §2.2 | ✅ done (CI workflow is the one excluded deliverable) |

### Tier B — Strongly recommended for the open-source launch

| ID | Gap | SPEC ref | Status |
|----|-----|----------|--------|
| B1 | 5-minute quickstart (SQLite → ClickHouse) validated | §1.3, §1.5 | ⬜ open |
| B2 | Clear startup checks (binlog ROW, perms, reachability) | §4.3 | ⬜ open |
| B3 | Structured/JSON logging option | §1.5 | ⬜ open |
| B4 | postgres_cdc is a partial implementation (pgoutput only, no DDL/truncate) | §1.4 | ⬜ open |
| B5 | S3 multipart + retry; ES cluster + 429 retry | §1.4 | ⬜ open |

### Tier C — Out of scope for this SPEC's "production-ready" bar

- K8s operator / Helm charts (§6.3 — no orchestrator dependency).
- Exactly-once Kafka transactions (idempotent sinks cover this observationally).
- Avro/Protobuf schema registry (plugins may add this; not core).

---

## 8. Change Control

- This SPEC is versioned (`Status: Stable v1`). Material changes to §1 (objective/modes), §3 (plugin ABI), or §6 (boundaries) require a version bump and user sign-off.
- Implementation progress is tracked in `TODO.md`; this file defines the bar, `TODO.md` tracks reaching it.


---

## Errata (Phase 2+3 completions)

- **2026-06-21**: B1 5-min quickstart validated ✅
- **2026-06-21**: B2 preflight startup checks ✅
- **2026-06-21**: B3 JSON logging documented ✅
- **2026-06-21**: B4 postgres_cdc TRUNCATE fix ✅
- **2026-06-21**: B5 S3/ES hardening (verified existing) ✅
- **2026-06-21**: Phase 3 legacy freeze + docs pass ✅
