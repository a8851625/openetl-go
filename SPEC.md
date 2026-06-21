# openetl-go · Production-Ready SPEC

> **Status**: Stable v2 (Re-Audited) · 2026-06-21
> **Scope**: Defines the contract for openetl-go to be considered production-ready, given three balanced goals — **数据同步可靠 · 易用 · 轻量**.
> This document is the source of truth for architecture, commands, structure, style, testing, and hard boundaries. Code must conform to it; deviations require updating this file.

> **Re-Audit Changelog (v1 → v2, 2026-06-21)**: An independent six-subsystem re-audit
> verified every v1 "done" claim against actual code (not the TODO log). **Several were
> overstated**: A11 (distributed dispatch) is decorative — no binary runs a real worker;
> A3/R1.4 (unified typing) is wired into only 3 of 5 relational sinks (ClickHouse + Doris
> bypass it); A8/R2.3 (schema validation) is wired but inert — zero implementors; A9/R2.5
> (per-sink metrics) is ClickHouse-only; P1-6 (Lua memory/time budget) is absent; P1-6 (DLQ
> replay atomicity) is still lossy; P2-1 (extism injection) is open; B2 (sink-reachable
> preflight) and B3 (JSON logging) are not delivered. The re-audit also surfaced ~35 new
> gaps (6 P0, 15 P1). **§7 and §9 below are the corrected source of truth**; the v1 errata
> is retained in §8 for history. What is genuinely solid: the linear pipeline's at-least-once
> ordering, circuit breaker, retry, storage-backend conformance (SQLite/MySQL/PG), Kafka/HTTP/
> file/Redis CDC checkpointing, MySQL/PG multi-row batch writes, and ClickHouse `_version`
> monotonicity.

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

> ⚠️ **Re-Audit Reality (ST-2)**: The "Scalable" row is the **contract**, not the current state.
> Today both modes execute shards in-process via `ParallelRunner` (`parallel.go:138` loops
> `inst.Start(ctx)` for every shard). The `master/` + `worker/` dispatch stack is wired but
> its worker executor is a no-op (`server.go:87-95`), `worker.New(Config{MasterURL})` is
> **never instantiated by any binary**, and there is no `--role` flag. MySQL/PG storage
> currently buys **shared metadata + HA/observability**, not horizontal execution.
>
> **Decision (2026-06-21): A11-redo path (a)** — make distributed execution *real*. Add
> `--role=master|worker`, make `ParallelRunner` delegate shard execution to `task_assignments`
> + worker poll, and verify with a true two-binary E2E. Until that lands, advertise scalable
> mode as "shared-metadata single-process". See ROADMAP §Phase 4 / P4-A.

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

`master.TaskDispatcher` + `worker` **are specified** to implement shard dispatch when storage is MySQL/PG:

- Master extracts shards from a `ParallelRunner`, records them in the shared `tasks` table.
- Workers long-poll `POST /api/v2/workers/{id}/poll` for assignment.
- Heartbeat timeout (default 60s) triggers reassignment.
- **In SQLite mode, dispatch is short-circuited**: shards run inline. No false claims of distribution.

> ⚠️ **Re-Audit Reality (ST-2)**: The four bullets above are the **target design**. Today the
> dispatch path is wired (`server.go:2819` starts an in-process master + standalone worker)
> but the standalone worker's task executor is a no-op that assumes `ParallelRunner` already
> did the work (`server.go:87-95`), and `ParallelRunner.Start` runs every shard inline in the
> same process (`parallel.go:138-156`). The "workers long-poll" path is implemented but no
> binary enters it. Bringing this to life is **A11-redo** (§7): add `--role=master|worker`,
> make `ParallelRunner` delegate shard execution to `task_assignments` + worker poll, and
> verify with a real two-binary E2E (not a single-process simulation).

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

- **Single static binary, pure Go.** No CGO in the default build; no runtime dependency on JVM/Python/Node for core function (the WASM/Lua/QuickJS plugin runtimes are opt-in sandboxes, not core). New external dependencies require review for binary-size and supply-chain impact. **Opt-in runtimes must be gated behind build tags** (`//go:build cgo`/`extism`) so a deployment that does not use them does not link them — currently WASM is linked unconditionally (TF-4).
- **Zero data loss — on every path, including DAG and shutdown.** Every record either reaches the sink (after retry) or the DLQ; if the DLQ write itself fails, escalate (alert + halt), never advance the checkpoint, never `continue`, never `_ =`. This must hold in the DAG executor (today it does not — PC-1/TF-9) and during graceful Stop (today ~50% of stops flush with a cancelled context — PC-2/PC-6).
- **Script runtimes are resource-bounded.** Lua and QuickJS transforms must enforce a per-record memory cap **and** a CPU/instruction/time budget; a runaway script (`while true`) must error one record, never hang or OOM the process (today neither has a CPU/time budget — TF-1/TF-2).
- **Backward-compatible YAML specs.** Existing `pipes/*.yaml` and user specs must keep working across releases. New fields default sensibly; removed fields require a deprecation cycle. `ValidateSpec` rejects unknown plugins but must tolerate unknown config keys.
- **Dual-mode parity of the core.** Anything that works in SQLite mode must also work in MySQL/PG mode (dispatch is the only intentional divergence). Do not implement a feature that silently breaks in one mode.
- **Honest capability claims.** A feature is "done" only when (a) the code implements it, (b) a test against real code proves it, and (c) `server.go pluginMetadata()` / README / this SPEC agree with the implementation. Optional interfaces (`SchemaDescriptor`, `SchemaValidator`, `SinkMetricsProvider`) confer a capability only when at least one shipped plugin implements them — otherwise the wiring is dead code, not a feature (SK-3/SK-4).

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
- **Do not claim distributed guarantees in SQLite mode.** Health/status output must reflect actual capability (degraded dispatch is reported, not hidden). Equally, **do not claim distributed execution in MySQL/PG mode until a real second binary runs a worker** (ST-2).
- **Do not execute untrusted/network-fetched tooling at request time.** No `npx --yes <pkg>` auto-fetch on a server path; pin versions and prefer pre-installed binaries (TF-3). User-supplied names joined into filesystem paths must be validated (`^[A-Za-z0-9_.-]+$`, no `/`/`..`).
- **Do not mutate `Record.Data` (a shared map) in place** across a transform chain without a defensive copy, and do not alias join/window state to live record pointers (TF-11).

---

## 7. Production-Readiness Gap → Workstream

Mapping the SPEC's bars to the remaining work. Status reflects the **2026-06-21 independent re-audit** (verified against code, not the TODO log). Items marked ✅ were re-confirmed true; ⚠️ are partial; ❌ are v1 claims that were **overstated**; 🆕 are gaps the re-audit newly surfaced. Detail for every item is in **§9 Re-Audit Findings Register**.

### Tier A — Required for "production-ready" claim (both modes)

| ID | Gap | SPEC ref | Status |
|----|-----|----------|--------|
| A1 | LogBuffer formatting bug (args dropped) | §4 | ✅ done |
| A2 | DAG condition operators Gt/Lt/Ge/Le/Regex unimplemented | §3.2 | ✅ done |
| A3 | Sink auto-create used all-TEXT | §1.4 | ⚠️ **partial** — MySQL/PG/JDBC fixed; **ClickHouse + Doris bypass `typing`** (SK-1, SK-2) |
| A4 | Redis source used blocking `KEYS` | §1.5 | ✅ done (SCAN) |
| A5 | Prometheus metrics wrong types (gauge for counters) | §1.5, §3.2 | ✅ done |
| A6 | `_version` non-monotonic under concurrency | §5.3 | ✅ done |
| A7 | DDL `apply` sent raw source DDL to target | §1.4 | ⚠️ **partial** — translator wired into ClickHouse only; Doris/MySQL/PG still apply raw |
| A8 | Schema mismatch failed silently at runtime | §1.4, §3.2 | ❌ **false** — interfaces wired but **zero implementors** (SK-3); startup validation never runs |
| A9 | Per-sink metrics only on ClickHouse | §1.5 | ❌ **false** — still ClickHouse-only (SK-4) |
| A10 | MySQL/PG storage backends unverified | §1.5, §3.3, §5.2 | ✅ done — conformance suite passes on all 3 backends |
| A11 | Master-worker dispatch verified end-to-end | §1.5, §3.4, §5.2 | ❌ **refuted** — dispatch is decorative; no binary runs a worker; "2-worker" test is a single-process simulation (ST-2). **Reopened as A11-redo.** |
| A12 | `make test`/CI scaffolding | §5, §2.2 | ✅ done (CI workflow itself is the one excluded deliverable, per user) |

### Tier A.1 — 🆕 Re-audit P0 gaps (data loss / security)

| ID | Gap | SPEC ref | Status |
|----|-----|----------|--------|
| P4-1 | DAG executor swallows DLQ-write errors and lies about `RecordsDLQ` counter | §6.1 | 🆕 open (PC-1/TF-9) |
| P4-2 | Graceful `Stop()` loses the in-flight batch ~50% of the time (flush uses cancelled ctx) | §6.1, §5.3 | 🆕 open (PC-2/PC-6) |
| P4-3 | Lua transform has no memory/CPU/time budget → runaway script hangs/OOMs | §6.1 | 🆕 open (TF-1) |
| P4-4 | QuickJS transform has no CPU/time budget (memory only) → `while(true)` hangs | §6.1 | 🆕 open (TF-2) |
| P4-5 | `npx --yes extism-js` auto-fetch + unsanitized `name` → path traversal / supply-chain | §6.3 | 🆕 open (TF-3) |

### Tier A.2 — 🆕 Re-audit P1 gaps (correctness / safety nets)

| ID | Gap | SPEC ref | Status |
|----|-----|----------|--------|
| P4-6 | Data race on `Runner.lastRecordAt` (read without lock) → `-race` fails | §5.4 | 🆕 open (PC-3) |
| P4-7 | `retry` panics if `InitialInterval == 0` (`rand.Int63n(0)`) | §6.1 | 🆕 open (PC-4) |
| P4-8 | File DLQ writer never `fsync`s → crash loses DLQ records | §6.1 | 🆕 open (PC-5) |
| P4-9 | DAG executor swallows checkpoint-save errors → silent duplicates on restart | §6.1 | 🆕 open (PC-7) |
| P4-10 | DLQ replay deletes by 1-second timestamp window → lossy + duplicate (still) | §6.1 | 🆕 open (SV-1) |
| P4-11 | Preflight sink-reachability is a no-op (builds plugin, never `Open`/`Ping`) | §4.3 | 🆕 open (SV-2) |
| P4-12 | Deduplicator transform has no mutex → data race / fatal map crash | §5.4 | 🆕 open (TF-6) |
| P4-13 | Join inner-miss silently drops records via `ErrRecordFiltered` (no DLQ/metric) | §6.1 | 🆕 open (TF-7) |
| P4-14 | Window transform has no watermark → late/out-of-order records double-count; sliding/session advertised but unimplemented | §1.4 | 🆕 open (TF-8) |
| P4-15 | No per-record panic recovery around transform/route invocation → one bad record = outage | §6.1 | 🆕 open (TF-10) |
| P4-16 | Enricher swallows all errors + leaks `sync.Map` cache unboundedly | §6.1 | 🆕 open (TF-13) |
| P4-17 | `mysql_batch` sets `done=true` prematurely when `batch_size < source limit` w/ custom query → truncated reads | §1.4 | ✅ **verified false-positive (no change)** — the check compares against the *capped* local `limit` (`min(r.limit, n)`), which is exactly what the SQL `LIMIT %d` uses; `50 < 50` is false so `done` is never wrongly set. SRC-5 retracted. |
| P4-18 | `ListTasks` silently caps at 50 rows → dispatch loses track of older tasks at scale | §3.4 | 🆕 open (ST-1) |

### Tier B — Strongly recommended for the open-source launch

| ID | Gap | SPEC ref | Status |
|----|-----|----------|--------|
| B1 | 5-minute quickstart (SQLite → ClickHouse) validated | §1.3, §1.5 | ✅ done |
| B2 | Clear startup checks (binlog ROW, perms, reachability) | §4.3 | ⚠️ **partial** — binlog/grants/tables real; **sink-reachability no-op** (SV-2); preflight errors downgraded to warnings (SV-3) |
| B3 | Structured/JSON logging option | §1.5 | ❌ **false** — documentation only, not configured anywhere (SV-6) |
| B4 | postgres_cdc completeness (TRUNCATE/DDL) | §1.4 | ⚠️ **partial** — TRUNCATE no longer halts the loop; semantic replication (sink still populated) is a known limitation (SRC-2) |
| B5 | S3 multipart + retry; ES cluster + 429 retry | §1.4 | ✅ done — ES round-robin + Retry-After; S3 5xx retry; (S3 Parquet still loses all column types — SK-5) |

### Tier B.1 — 🆕 Re-audit P2 gaps (advertised-missing / polish)

| ID | Gap | SPEC ref | Status |
|----|-----|----------|--------|
| P4-19 | SchemaValidator/SchemaDescriptor are dead code — implement or remove (A8) | §3.2 | 🆕 open (SK-3) |
| P4-20 | `SinkMetricsProvider` on the 8 non-ClickHouse sinks (A9) | §1.5 | 🆕 open (SK-4) |
| P4-21 | S3 Parquet encodes every column as String (loses all types) | §1.4 | 🆕 open (SK-5) |
| P4-22 | WASM runtime linked unconditionally — gate behind `//go:build extism` | §6.1 | 🆕 open (TF-4) |
| P4-23 | Router overwrites `Metadata.Source` as a route tag (destroys provenance) | §3.2 | 🆕 open (TF-5) |
| P4-24 | Transform chain shares `Record.Data` map by reference (cross-batch contamination) | §6.3 | 🆕 open (TF-11) |
| P4-25 | Lua `os` only field-pruned (not nilled); unsafe type-assert can panic | §6.1 | 🆕 open (TF-12) |
| P4-26 | Filter type-mismatch silently drops records (no `strict_types`) | §6.1 | 🆕 open (TF-14) |
| P4-27 | `MarkOffline` deregisters workers instead of marking offline (destroys history) | §3.4 | 🆕 open (ST-3) |
| P4-28 | `SavePipelineVersion` read-then-write race on concurrent saves | §3.3 | 🆕 open (ST-4) |
| P4-29 | Dispatch never retries/reassigns `failed` tasks | §3.4 | 🆕 open (ST-5) |

### Tier C — Out of scope for this SPEC's "production-ready" bar

- K8s operator / Helm charts (§6.3 — no orchestrator dependency).
- Exactly-once Kafka transactions (idempotent sinks cover this observationally).
- Avro/Protobuf schema registry (plugins may add this; not core).
- P3 polish items catalogued in §9 (alert-queue overflow, MySQL `VALUES()` deprecation, SQLite `LIMIT` parameterization, redis checkpoint non-determinism, etc.).

---

## 8. Change Control

- This SPEC is versioned (`Status: Stable v2`). Material changes to §1 (objective/modes), §3 (plugin ABI), or §6 (boundaries) require a version bump and user sign-off.
- Implementation progress is tracked in `TODO.md` and `ROADMAP.md`; this file defines the bar, those track reaching it.
- **Honesty rule (new in v2):** a claim of "done" is retractable. When a re-audit refutes a prior claim, the SPEC must record it (as ❌ in §7) within the same release — never leave a refuted "✅" standing.

### 8.1 Historical errata (v1, Phase 2+3) — retained for provenance

These were recorded as complete in v1. The 2026-06-21 re-audit **confirmed** the un-annotated ones and **refuted/partial'd** the others (see §7 for the corrected status):

- **2026-06-21**: B1 5-min quickstart validated ✅ (re-confirmed)
- **2026-06-21**: B2 preflight startup checks ⚠️ (binlog/grants/tables real; sink-reachability no-op — SV-2)
- **2026-06-21**: B3 JSON logging ❌ (documentation only, not configured — SV-6)
- **2026-06-21**: B4 postgres_cdc TRUNCATE fix ⚠️ (no-crash yes; semantic replication still missing — SRC-2)
- **2026-06-21**: B5 S3/ES hardening ✅ (re-confirmed; S3 Parquet type-loss — SK-5)
- **2026-06-21**: Phase 3 legacy freeze + docs pass ✅ (re-confirmed)

---

## 9. Re-Audit Findings Register (2026-06-21)

Six independent auditors read the actual code across pipeline-core, sources, sinks/typing/DDL, storage/dispatch, server/observability, and transforms/plugins. Findings below are the evidence base for §7. Format: **ID (severity) — title — `file:line` — evidence — fix**. Severities: P0 data-loss/security · P1 correctness/safety-net · P2 advertised-missing · P3 polish.

### P0 — data loss / security

- **PC-1 / TF-9 (P0)** — DAG executor swallows DLQ-write errors and lies about the counter — `orchestrator/executor.go:562` & `:553-565`. `_ = e.dlqWriter.WriteDLQ(ctx, entry); atomic.AddInt64(&e.stats.RecordsDLQ, 1)` — a failed DLQ write is discarded yet the counter increments; records are gone and ops is told they're safe. The linear Runner gets this right (`pipeline.go:894-903`); the DAG path diverged. **Fix:** mirror the linear path — on DLQ-write error, log + alert + do NOT advance checkpoint + trip breaker; only increment on success.
- **PC-2 (P1→treat as P0 for shutdown)** — Graceful `Stop()` loses the in-flight batch ~50% of the time — `pipeline.go:440-458` cancels ctx; writeLoop select (`:690-722`) races between `!ok` (calls `forceFlushAndCheckpoint()` → `writeBatch(ctx, …)` with the *cancelled* ctx) and `ctx.Done` (uses a fresh `context.Background()`). The `!ok` branch's `retry.Do` returns `ctx.Err()` immediately → batch dropped + checkpoint not saved → duplicate of the *previous* batch on restart. **Fix:** `forceFlushAndCheckpoint` and the EOF flush must use a fresh `context.WithTimeout(context.Background(), 10s)` exactly like the `ctx.Done` branch; or make `Stop()` wait for `done` before cancelling.
- **TF-1 (P0)** — Lua transform has no memory/CPU/time budget — `transform/lua.go:97-137`. `PCall(0,0,nil)` ignores `ctx`; no `SetMState`/`SetDebugHook`. `while true do end` pins the goroutine forever; unbounded table growth OOMs. **Fix:** wrap `Apply` in a goroutine + `select{<-ctx.Done(); L.Close()}`, and/or install an instruction-count debug hook.
- **TF-2 (P0)** — QuickJS transform has memory cap only, no CPU/time budget — `transform/ts.go:69,93,126`. `while(true){}` hangs the transform goroutine (and `t.mu`). **Fix:** `runtime.SetInterruptHandler(func() bool { return ctx.Err() != nil })` + eval-in-goroutine with a `select` on `ctx.Done()`.
- **TF-3 (P0)** — `npx --yes extism-js` auto-fetch + unsanitized `name` — `server.go:2312,2321`, `plugin/pluginsystem/manager.go:69`. `outFile := filepath.Join(tmpDir, name+".wasm")` with `name` from an HTTP form, no validation → `../../etc/x` escapes the temp dir; `npx --yes` pulls from npm at request time. **Fix:** validate `^[A-Za-z0-9_.-]+$`, reject `/`/`..`; pin `@extism/js-pdk@x.y.z`; prefer a pre-installed binary.
- **ST-2 (P0 for the architecture claim)** — "Scalable distributed mode" does not distribute — `server.go:87-95` (standalone worker executor is a no-op), `parallel.go:138-156` (all shards run inline), `app.go:130-138` (no role selection); `worker.New(Config{MasterURL})` is never called in non-test code. The "2-worker splits 4 shards" E2E is a single-process store-mutation simulation. **Fix:** add `--role=master|worker`, make `ParallelRunner` delegate shard execution to `task_assignments` + worker poll, verify with a real two-binary E2E. **(A11-redo.)**

### P1 — correctness / safety nets

- **PC-3 (P1)** — Data race on `Runner.lastRecordAt` — `pipeline.go:650` (write under `r.mu`) vs `:567` (read without lock in alert checker). `time.Time` is a 3-word struct → torn read, `-race` fails. **Fix:** `RLock` the read or store as `atomic.Int64` (unix-nano).
- **PC-4 (P1)** — `retry` panics if `InitialInterval == 0` — `retry/retry.go:58` `rand.Int63n(int64(interval)/4)` → `Int63n(0)` panics; a single transient write error then kills the pipeline via the writeLoop recover. **Fix:** guard `if interval > 0`, clamp callers, validate in `DefaultConfig`.
- **PC-5 (P1)** — File DLQ writer never `fsync`s — `dlq/dlq.go:73-78` writes then returns (no `f.Sync()`); contrast `checkpoint/checkpoint.go:51-64` which does `tmp.Sync()` + dir sync. A crash within ~30s loses DLQ records — the very safety net for data loss. **Fix:** `f.Sync()` after write (optionally batched behind a config flag).
- **PC-6 (P1)** — DAG `routeAndWrite` ctx.Done flush uses the cancelled context — `orchestrator/executor.go:426-428`. Same class as PC-2, DAG path, no fresh-context escape. **Fix:** `flushAll` takes a fresh background timeout ctx.
- **PC-7 (P1)** — DAG executor swallows checkpoint-save errors — `orchestrator/executor.go:536` `_ = e.cpAdapter.Save(ctx, cp)`. On failure the pipeline keeps writing, no alert/breaker → duplicates on restart. Linear Runner logs+alerts+trips (`pipeline.go:925-946`). **Fix:** mirror the linear path.
- **SV-1 (P1)** — DLQ replay deletes by 1-second timestamp window → lossy + duplicate — `server.go:2665-2678` + `storage/dlq_compat.go:58-69`. Two DLQ entries written in the same second: replaying A deletes B (loss), or replays B twice (dup); loop also returns on first transform error *after* deleting earlier items. **Fix:** carry a stable ID on `DeadLetter` (auto-increment IDs + `DeleteByID` already exist); delete per-item only after its sink write succeeds.
- **SV-2 (P1)** — Preflight sink-reachability is a no-op — `server/preflight.go:177-191` only `registry.BuildSink` (struct validation), never `sink.Open`/`Ping`. A typo'd ClickHouse host passes preflight. **Fix:** call `sink.Open` with a short timeout (reuse `handleConnectionTest` at `server.go:802-808`).
- **TF-6 (P1)** — Deduplicator transform has no mutex — `transform/router.go:131-137` (`cache`/`cacheMap`/`pos` mutated in `Apply` with no lock). Concurrent Apply → fatal `concurrent map read and map write`. Sibling `join.go:42` correctly uses `sync.RWMutex`. **Fix:** add `mu sync.Mutex`.
- **TF-7 (P1)** — Join inner-miss silently drops records — `transform/join.go:118-123,156-174` returns `ErrRecordFiltered` (same sentinel as intentional filter drops); runner `continue`s with no DLQ/metric, so schema-drift losses look like user-intended filtering. **Fix:** `on_miss: drop|dlq|error` config, default `dlq` for inner joins.
- **TF-8 (P1)** — Window has no watermark; sliding/session advertised but unimplemented — `transform/window.go:181-202` (no lateness check; `window_type` config never read by the constructor, only tumbling works). Out-of-order CDC replays silently double-count aggregates. **Fix:** track `maxEventTimeSeen`, allow `allowed_lateness`, drop/DLQ older records; implement or remove sliding/session.
- **TF-10 (P1)** — No per-record panic recovery around transforms — `orchestrator/executor.go:301-304` (`routeAndWrite` goroutine has no `recover`; the outer `runDAG` defer is a different goroutine) and `pipeline.go:547-553` (writeLoop recover sets `LastError` but never `setStatus(StatusFailed)` → pipeline hangs "running"). One panicking transform/record = full outage. **Fix:** `defer recover()` per `Apply`/`route` → `handleFailed`; writeLoop recover calls `setStatus(StatusFailed)`.
- **TF-13 (P1)** — Enricher swallows all errors + leaks cache — `transform/enricher.go:160-166,179-182` returns `rec, nil` on error (no log); expired entries are checked but never evicted from `sync.Map` → unbounded growth. Flaky endpoint → silent data-quality degradation. **Fix:** return the error (→ DLQ/retry); periodic cache sweep.
- **SRC-5 (✅ verified false-positive, retracted)** — `mysql_batch` done-flag was claimed to terminate early. Re-check: `source/mysql_batch.go:303` compares `len(records) < limit` where `limit` is the *capped* local `min(r.limit, n)` — exactly the value passed to the SQL `LIMIT %d`. `50 < 50` is false, so `done` is never set on a full page; the check is the standard keyset "fewer rows than requested = exhausted" signal and is correct. No change made. (The re-audit agent's own example contained an arithmetic error.)
- **ST-1 (P1)** — `ListTasks` silently caps at 50 — `storage/{sqlite,mysql,postgres}/*.go` `ORDER BY assigned_at DESC LIMIT 50`. `AssignNextTask`, `ReassignStaleTasks`, and `pollTaskFromStore` all read through this; >50 task rows → older pending tasks become invisible/never dispatched. No retention janitor. **Fix:** `WHERE status IN ('pending','assigned','running')` before LIMIT, or a `ListPendingTasks` with no cap; add a finished-task janitor.

### P2 — advertised-missing / polish

- **SK-1 (P1→P2)** — ClickHouse auto-create ignores `typing` — `sink/clickhouse.go:1161-1183` (`inferClickHouseType`); name-blind, all `int`→`Int64`, no DECIMAL, no `id`/`_at` hints. The most-used sink bypasses the unified engine. **Fix:** `return typing.InferFromValue(typing.DialectClickHouse, colName, v)`; thread column names through.
- **SK-2 (P1→P2)** — Doris auto-create is name-only — `sink/doris.go:880-904` (`inferDorisType(colName)`); `EnsureSchemaGeneric` called with `nil` fieldValues. A column named `foo` with an int value → `STRING`. **Fix:** populate `fieldValues`; add a Doris dialect to `typing` (or fall back to value-driven).
- **SK-3 (P2)** — `SchemaValidator`/`SchemaDescriptor` are dead code — `core/core.go:108-118`, wired at `pipeline.go:382-396`, **zero implementors** in source/sink. Startup validation never runs. **Fix:** implement on the 5 relational sinks + MySQL/PG CDC sources, or remove the interfaces + wiring.
- **SK-4 (P2)** — `SinkMetricsProvider` only on ClickHouse — only `sink/clickhouse.go:216`; the other 8 sinks report zero to Prometheus. **Fix:** add the `recordMetrics` helper to MySQL/PG/Doris/JDBC/Kafka/ES/Redis/S3 `Write` paths.
- **SK-5 (P2)** — S3 Parquet encodes every column as String — `sink/s3.go:329-372` (`parquet.String()` + `fmt.Sprintf("%v", v)`). All type info lost. **Fix:** switch on the sample value (`parquet.Int(64)`, `Double`, `Timestamp`, `String`).
- **SV-3 (P2)** — Preflight errors downgraded to warnings — `server.go:716-724` appends `level:"error"` issues to `warnings` and still returns `valid:true`. **Fix:** surface `level=="error"` as `valid:false` in `errors`.
- **SV-4 (P2)** — AI-generation endpoint not mounted; generated YAML unvalidated — `server.go:3055-3158` handler exists but no route in the table (`:582-600`); LLM output returned raw. **Fix:** register the route; pipe output through `ValidateSpec` + `RunPreflight`.
- **SV-6 (P2)** — B3 JSON logging not implemented — all logging is `g.Log()` default text; no `format: json` wiring in code or `manifest/config/config.yaml`. **Fix:** set GoFrame glog JSON format + document, or retract the "done" claim.
- **TF-4 (P2)** — WASM linked unconditionally — `plugin/pluginsystem/manager.go:11` imports extism with no `//go:build extism` tag; every binary links wazero. Violates lightweight-core (QuickJS/Lua are correctly gated). **Fix:** split registry (core) from `extism` (build-tagged) package.
- **TF-5 (P2)** — Router overwrites `Metadata.Source` — `transform/router.go:78,85,87` reuses the provenance field as a route tag; downstream nodes lose origin. **Fix:** add `Metadata.Route`; teach DAG edge-matching to consult it.
- **TF-11 (P2)** — Transform chain shares `Record.Data` by reference — `core/core.go:146-155`; `join.go:148` aliases `&e.record`. In-place mutation → cross-batch contamination. **Fix:** deep-copy `Data` at chain entry, or enforce a no-mutation contract.
- **TF-12 (P2)** — Lua `os` field-pruned not nilled; unsafe type-assert — `transform/lua.go:74-86` leaves `os.date/time/setlocale` callable; bare `.(*lua.LTable)` panics if `os` is non-table. **Fix:** `L.SetGlobal("os", lua.LNil)`; comma-ok assert.
- **TF-14 (P2)** — Filter type-mismatch silently drops records — `transform/builtin.go:255-259,276-290`; schema drift → every previously-matching row filtered out with no log. **Fix:** `strict_types` config → error/DLQ on mismatch.
- **ST-3 (P2)** — `MarkOffline` deregisters instead of marking offline — `master/master.go:106-109` deletes the worker row on a transient heartbeat blip. **Fix:** `UPDATE workers SET status='offline'`; reserve `DeregisterWorker` for explicit shutdown.
- **ST-4 (P2)** — `SavePipelineVersion` read-then-write race — all 3 backends `SELECT MAX(version)` then `INSERT`; concurrent saves collide on `UNIQUE(pipeline, version)` (SQLite safe under `MaxOpenConns(1)`, MySQL/PG exposed). **Fix:** atomic `INSERT ... SELECT COALESCE(MAX(version),0)+1` (PG) / retry-on-duplicate (MySQL).
- **ST-5 (P2)** — Dispatch never retries `failed` tasks — `worker/poll.go:148-160` marks `failed`; `dispatch.go:115` only reassigns `assigned`/`running`. **Fix:** retry counter + re-queue, or surface terminal failures.
- **SRC-2 (P2)** — postgres_cdc TRUNCATE leaves the sink populated — `source/postgres_cdc.go:491-504` logs a warning but emits no delete; source/target diverge. **Fix:** emit a synthetic truncate/delete record; or document as a known limitation.

### P3 — polish (catalogued, not blocking)

PC-8 alert queue overflow drops critical events (`alert/alert.go:163-168`, uses `fmt.Printf` bypassing `g.Log`); PC-9 `contains` redundant prefix branch (`executor.go:696-700`); SK-6 dead `inferColumnType` in `jdbc.go:613-630`; SK-7 Doris stream-load label cross-instance collision (`doris.go:435`); ST-6 MySQL `VALUES(col)` UPSERT deprecated in 8.0.21+ (`mysql.go` 5 sites); ST-7 SQLite DLQ `LIMIT` via `fmt.Sprintf` (`sqlite.go:453-457`); SV-5 in-process rate limiter undocumented for multi-instance (`server.go:2859-2921`); SRC-1 `mysql_snapshot_cdc.Close()` double-close/nil race (`:667-673`); SRC-3 file `Read` ignores ctx (`file.go:194-203`); SRC-4 `shardTotal>0` vs `>1` inconsistency (`mysql_cdc.go:148`); SRC-6 redis checkpoint non-deterministic across restart (`redis.go:131-142`); TF-15 capability metadata absent from registry (`registry.go`).

### What the re-audit confirmed SOLID (no action)

Linear Runner at-least-once checkpoint ordering (`pipeline.go:786-847`); linear DLQ-write-failure escalation (`pipeline.go:894-903`); three-state circuit breaker (`circuit_breaker.go`); retry backoff+jitter+context+retryable-classification (`retry/retry.go`, `core/errors.go`); LogBuffer `*f` formatting (`logbuffer.go:66-80`); all 8 DAG condition operators (`executor.go:608-700`); panic recovery in readLoop/writeLoop; Kafka MarkOffset-after-commit (`kafka.go:392-400`); HTTP checkpoint restore + 429/5xx backoff (`http.go`); file CSV/JSON checkpoint (`file.go`); Redis SCAN streaming (`redis.go:153-157`); mysql_cdc no `time.Local` mutation + FNV server_id (`mysql_cdc.go`); mysql_snapshot_cdc two-phase FTWRL consistency + `db.Close`; postgres_cdc late-ACK LSN + reconnect-backoff + slot-not-dropped default; ClickHouse `_version` atomic monotonic counter (`clickhouse.go:974-977`); MySQL/PG/JDBC multi-row batch INSERT in one tx, chunked 500; all quoting/sanitization (`quote.go`); Kafka idempotent producer; ES round-robin + Retry-After; S3 5xx retry; DDL translator correctness + ClickHouse wiring (`ddl/translator.go`, `clickhouse.go:391-400`); `typing` engine correctness for the 3 sinks that use it; storage conformance across SQLite/MySQL/PG; MySQL zero-timestamp normalization + `plugin_state.key` VARCHAR(255); preflight MySQL binlog/grants/tables checks; reconciler uses current spec (`server.go:493-496`); health 503 (`telemetry.go:117-120`); secrets masking; uniform auth + constant-time compare; correct Prometheus metric types; AES-256-GCM spec encryption; hot-reload graceful degradation.
