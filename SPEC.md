# openetl-go · Production-Ready SPEC (v3)

> **Status**: Release candidate v3 · 2026-06-24 — Phase 5 beta2 closure.
> **Supersedes**: SPEC v2 (2026-06-21 re-audit), preserved verbatim at
> [`docs/SPEC-v2-reaudit-2026-06.md`](./docs/SPEC-v2-reaudit-2026-06.md). The v2 §9 findings
> register remains the evidence base for everything carried forward; this file points into it
> rather than duplicating it.
> **Scope**: the contract for openetl-go to be considered production-ready across **both**
> operating modes (single-node SQLite **and** scalable MySQL/PG), balanced on three goals —
> **数据同步可靠 · 易用 · 轻量**. Code must conform; deviations require updating this file.
>
> **v3 changelog (v2 → v3)**: An independent three-pillar audit (reliability / usability /
> lightweight, 2026-06-22) verified the Phase 4 P0–P2 closure **did land in code** — but found
> (a) **two new committed P0 regressions**, (b) one **v2 "done" claim that is inert** (P4-23 JSON
> logging), and (c) substantial 易用 / 轻量 gaps. Phase 5 is closed for `v0.1.0-beta2`
> (2026-06-24): the P0/P1 reliability fixes, README/quickstart/schema ergonomics, JSON logging,
> worker slot accounting, and lightweight Lua opt-out are implemented and covered by tests/E2E.
> What is genuinely solid: linear Runner at-least-once + DLQ escalation, three-state circuit
> breaker, retry/backoff, DAG per-record panic recovery, real distributed dispatch (A11-redo,
> verified real not simulated), unified typing engine (now including ClickHouse + Doris), storage
> conformance across SQLite/MySQL/PG, WASM correctly gated behind `//go:build extism`, and
> single-node SQLite booting with zero external dependencies.

---

## 0. Headline (read this first)

1. **P0 regressions are closed.** `Server.newRunner` now takes the inline
   `pipeline.NewPipeline(...)` path for standalone/single-shard execution, and
   `TestNewRunnerNotRecursive` guards against the prior stack-overflow regression (P5-1).
   JSON-lines file checkpoints no longer compound byte offsets across restarts, with
   `TestFileJSONCheckpointNoCompoundOnRestart` covering repeated resume (P5-2).

2. **P1 reliability is closed for beta2.** DAG reader-map access and `ParallelRunner.cancel`
   are race-safe; ES bulk 2xx/unparseable responses return an error; Kafka idempotent fallback
   warns loudly; Postgres CDC skips known non-row pgoutput messages by wire format; zero-survivor
   batches checkpoint only when every drop is intentional `ErrRecordFiltered`; worker slot
   accounting uses an atomic in-flight counter; sink error counters are wired (P5-3..P5-12).

3. **易用 beta2 bar is closed.** README and quickstart now describe OpenETL-Go, create/update
   APIs run preflight and reject hard errors, connector schemas expose real fields and aliases,
   canonical MySQL CDC → ClickHouse examples ship in `pipes/`, and compose quickstarts use
   consistent MySQL/ClickHouse settings (P5-13..P5-21).

4. **轻量 beta2 bar is closed with a compatibility compromise.** WASM remains opt-in
   (`-tags=extism`), QuickJS remains CGO-gated, and Lua is an opt-out runtime via `-tags=nolua`
   so existing default Lua users are not broken (P5-22). API-only/headless mode and per-sink
   build tags remain deferred product work (P5-23/P5-24).

---

## 1. Objective

### 1.1 What this is

openetl-go is a **single-binary, plugin-based ETL/CDC engine** built on three abstractions:

```
Source ──► Transform ──► Sink
```

It captures changes from databases (MySQL binlog, PostgreSQL logical replication), streams
(Kafka), files, HTTP, and Redis; applies a chain of transforms; and writes to analytical stores
(ClickHouse, MySQL, PostgreSQL, Doris, Elasticsearch, Kafka, Redis, S3, JDBC). The pipeline
framework (`internal/etl/`) is the primary product surface. The legacy `internal/logic/sync/`
Canal path is retained for backward compatibility only.

### 1.2 Two operating modes

The binary runs in **two modes**, selected by `etl.storage.type` (and, for execution role,
`etl.role`):

| Mode | Storage | Execution role | Distributed | Intended use |
|------|---------|----------------|-------------|--------------|
| **Single-node (demo/CI)** | `sqlite` (default, zero-dependency, pure Go) | `standalone` (default) | Shards run inline in one process | Local eval, single-host sync, CI |
| **Scalable** | `mysql` or `postgresql` | `master` + N × `worker` | **Enabled** — master dispatches shards to workers via shared `tasks` table + heartbeat | Production horizontal scale |

**Both modes must meet the production-ready bar.** Distributed guarantees (heartbeat-based shard
reassignment, cross-process execution) are claimed *only* in scalable mode. SQLite mode is the
zero-friction entry point and the轻量 reference; MySQL/PG mode is the scale-out path. The
dual-mode architecture is the central decision of the project (v2 §1.2; unchanged in v3).

> **A11-redo ✅ verified real (2026-06-22).** `worker.ExecuteShard`, `worker.New`,
> `pipeline.NewDistributedPipeline` + `ShardDispatcher`, and `etl.role` selection are all wired
> and exercised by integration tests (4 shards split 2/2 across real HTTP workers; crash
> reassignment). This is genuine distributed execution, not a single-process simulation — the v2
> ST-2 finding is closed. **P5-11 is now closed:** the worker poll loop gates new task goroutines
> with an atomic in-flight counter and `TestPollLoopRespectsSlotsLimit` pins slot behavior.

### 1.3 Target users

- **Primary**: open-source community — operators and engineers syncing OLTP → OLAP (MySQL →
  ClickHouse is the canonical case).
- **Expectations**: 5-minute quickstart from README, runnable examples that work unmodified,
  clear failure messages (what + where + why + remediation), no hidden state, no external
  services required to evaluate. Docs and ergonomics are first-class — the README and quickstart
  must describe the ETL framework, not the legacy Canal product (P5-13).

### 1.4 The three balanced goals

When goals conflict, resolve in this order — but all three must reach the minimum bar:

1. **数据同步可靠 (Reliability)** — at-least-once delivery with idempotent sinks; zero silent
   data loss on any path (hard constraint, §6.1). *Non-negotiable; gates everything.*
2. **易用 (Usability)** — auto-create target tables, schema validation, clear startup checks,
   friendly errors with context, a README that matches the product.
3. **轻量 (Lightweight)** — single static pure-Go binary, low resource footprint, **all opt-in
   script runtimes gated behind build tags**, minimal external dependencies, zero-dependency
   single-node mode.

> Per project direction (this audit), 易用 and 轻量 are elevated in priority relative to v2: a
> new user's 5-minute experience and a minimal binary are treated as production-blocking, not
> polish. Reliability remains #1.

### 1.5 Production-ready definition

openetl-go is production-ready when, for **both** modes:

- **No data is silently lost** on any write path (retry → DLQ → escalate; never bare `continue`,
  never a silent success on an unparseable response). *(P5-2, P5-5, P5-9, P5-10 closed current
  violations.)*
- **A crash or SIGTERM** never loses committed data and never re-delivers beyond idempotency
  tolerance; checkpoint advances only after sink commit.
- **Schema changes** are either auto-applied or clearly rejected — never silently dropped.
- **Health and metrics** are observable (`/api/v2/health` returns 503 when unhealthy; Prometheus
  `/metrics` with correct types; per-sink error metrics are non-zero when sinks fail). *(P5-12
  closed.)*
- **Scalable mode** genuinely distributes shards across nodes with heartbeat-based reassignment
  and respects per-worker slot limits. *(A11-redo done; P5-11 closed.)*
- **A new user** goes from `git clone` to a working MySQL→ClickHouse sync in < 5 minutes using
  SQLite mode and the README quickstart, with a quickstart example that runs unmodified.
  *(P5-13, P5-17, P5-19 closed.)*
- **The binary is lightweight by default**: the default build excludes every opt-in runtime
  (WASM/QuickJS) and supports `-tags=nolua` for Lua-free builds; every unused sink remains a
  candidate for later exclusion. *(P5-22 closed; P5-24 deferred.)*

---

## 2. Commands

### 2.1 Build & run

| Command | Purpose |
|---------|---------|
| `make build` | GoFrame build (`gf build -ew`) — packs `resource/` into `internal/packed`; safest path. |
| `go build -tags=extism -o bin/openetl-go .` | Plain Go build with WASM support (needs generated `internal/packed`; see AGENTS.md). |
| `go build ./...` | Compile-check all packages (default: no extism, no CGO runtimes). |
| `./openetl-go` | Run with `manifest/config/config.yaml`. |

**Build tags** (the轻量 contract, §6.1):

| Tag | Effect | Default? |
|-----|--------|----------|
| *(none)* | Pure-Go core + all sinks; **no** WASM, **no** QuickJS, **no** CGO | ✅ |
| `-tags=extism` | + WASM plugin runtime (wazero, pure Go) | — |
| `CGO_ENABLED=1` | + QuickJS transform runtime (CGO) | — |
| `-tags=nolua` *(P5-22)* | **Opt-out** Lua runtime (gopher-lua) — default build keeps Lua; `-tags=nolua` drops it for 轻量 builds | — |

### 2.2 Testing (the pyramid)

| Command | Purpose |
|---------|---------|
| `make test` | Unit tests with `-race` across `internal/etl/...`, `internal/logic/...`, `internal/controller/...` |
| `make test-quick` | Same without `-race` |
| `make test-pkg PKG=pipeline` | One package, verbose |
| `make test-integration` | Integration tests with podman-compose services (MySQL + ClickHouse + …) |

Integration tests use the **`integration` build tag** and require live databases. Go runs inside
the `etl-go-dev` podman container (host has no `go`).

### 2.3 Dev environment

- **podman** is the supported runtime (`podman compose -f docker-compose.dev.yml`).
- `etl-go-dev` container (golang:1.24) mounts the workspace; builds/tests run there.
- Services: `mysql-source` (binlog enabled), `clickhouse`, plus `minio`/`redpanda`/`postgres` as needed.

### 2.4 API surface

- **ETL API** (`:8001`, proxied via `:8000/api/v2/*`): pipeline CRUD, start/stop/pause/resume,
  checkpoint, DLQ replay, spec validation, connection test, transform dry-run, plugin management,
  AI generation, audit.
- **Observability**: `/api/v2/health` (503 when unhealthy), `/metrics` (Prometheus),
  `/api/v2/metrics` (JSON).
- **Workers**: `GET/POST /api/v2/workers`, `POST /api/v2/workers/{id}/poll` (long-poll for shard
  assignment), `/heartbeat`, `/deregister`.
- **Legacy monitor API** (`:8000/api/monitor/*`): retained, not primary.

---

## 3. Project Structure

### 3.1 Layout

```
openetl-go/
├── main.go, internal/cmd/              # entry + CLI
├── internal/etl/                       # PRIMARY product surface
│   ├── core/                           # Source/Sink/Transform/Record interfaces + errors
│   ├── pipeline/                       # Runner, ParallelRunner, DistributedPipeline, circuit breaker, metrics
│   ├── orchestrator/                   # DAG executor (node-based pipelines)
│   ├── source/                         # 8 source plugins
│   ├── sink/                           # 9 sink plugins
│   ├── sink/typing/                    # unified column-type inference (cross-sink, MySQL/PG/CH/Doris/JDBC)
│   ├── sink/ddl/                       # DDL dialect translation
│   ├── transform/                      # 15+ transform plugins (lua/ts gated)
│   ├── registry/                       # plugin builder registry
│   ├── storage/{sqlite,mysql,postgres}/# metadata persistence backends + factory
│   ├── server/                         # HTTP API + reconciliation + hot-reload + preflight
│   ├── master/, worker/                # distributed dispatch (scalable mode)
│   ├── alert/, dlq/, retry/, checkpoint/, telemetry/, plugin/pluginsystem/
├── internal/logic/{app,sync,monitor}/  # app bootstrap + legacy Canal + monitor
├── internal/controller/monitor/        # legacy monitor HTTP API
├── pipes/, pipes-quickstart/           # example YAML pipeline specs
├── manifest/config/config.yaml         # default config
├── hack/                               # E2E scripts, release tooling
├── docs/                               # incl. SPEC-v2-reaudit-2026-06.md (preserved)
├── Dockerfile, docker-compose*.yml     # deployment
└── SPEC.md                             # this file
```

### 3.2 The plugin contract (the heart of the system)

Every source/sink/transform implements a small core interface (`internal/etl/core/core.go`) and
registers via `registry.Register*` in `init()`. Optional interfaces extend behavior:

- `core.SchemaDescriptor` — source exposes its output schema (enables validation + auto-create).
- `core.SchemaValidator` — sink validates source schema at startup.
- `core.SinkMetricsProvider` — sink exposes per-sink write metrics.
- `core.RecordCheckpointer` — reader produces per-record checkpoints (at-least-once).

**Rule**: capability is declared by implementing an interface, not by string metadata. The
`server.go pluginMetadata()` table is advisory only and must not diverge from actual interface
implementation.

> **Honesty note (P4-19, verified):** `SchemaValidator`/`SchemaDescriptor` currently have **no
> built-in implementors**; the wiring (`pipeline.go:382-383`) is an intentional no-op, documented
> as such in `core.go`. They are extension points for SDK-built custom plugins, not a shipped
> feature. This is the honest state; do not re-advertise them as active.

### 3.3 Dual-mode storage boundary

`storage/factory.NewStore` selects the backend from config. All state (pipeline specs, versions,
checkpoints, DLQ, audit, worker registry, run history, tasks, plugins) goes through the
`storage.Storage` interface — **never** direct file I/O from production paths. The legacy
`checkpoint/` and `dlq/` file-based writers exist only for one-time migration. SQLite is pure-Go
(`modernc.org/sqlite`, no CGO), so single-node mode boots with zero external services.

### 3.4 Distributed dispatch (scalable mode only)

`master` + `worker` implement shard dispatch when storage is MySQL/PG and `etl.role ∈ {master,
worker}`:

- Master creates `task_assignments` (with shard index/total metadata) and waits via
  `ShardDispatcher.WaitShard`.
- Workers long-poll `POST /api/v2/workers/{id}/poll`, claim a task, and execute the shard
  in-process via `worker.ExecuteShard` → `pipeline.BuildShardRunner`.
- Heartbeat timeout (default 60s) triggers `ReassignStaleTasks`.
- **In SQLite / standalone mode, dispatch is short-circuited**: shards run inline via
  `ParallelRunner`. No false claims of distribution.
- **DAG pipelines do NOT shard-distribute** (they don't go through `ParallelRunner`); distributed
  dispatch is linear-spec only.

---

## 4. Code Style

### 4.1 Go conventions

- **Go 1.24**, modules, `gofmt` + `goimports`. Match surrounding idiom and comment density.
- **Errors**: wrap with `%w`; classify via `core/errors.go` (transient/data/schema/auth/config).
- **Context**: all I/O takes `ctx context.Context` first and respects cancellation. No
  `context.Background()` in hot paths except a deliberate flush timeout (see §4.2).
- **Concurrency**: shared mutable state behind a mutex or atomics. `-race` is the default.

### 4.2 The zero-loss rule (hard constraint)

**No write path may silently drop a record.** Concretely:

- On sink `Write` failure: retry with backoff (`retry.Do`); on exhaustion, route each record to
  DLQ; **if the DLQ write itself fails, escalate** (alert + trip circuit breaker + do not advance
  checkpoint), never `continue`, never `_ =`. *(P5-9: ✅ done — both the linear `Runner` and the
  DAG `DAGExecutor` trip the per-sink circuit breaker on a DLQ-write failure.)*
- A batch with zero surviving records (all filtered/errored) must not silently advance the
  checkpoint unless every dropped record was an intentional `ErrRecordFiltered`. *(P5-10: ✅
  done in the linear Runner, with zero-survivor tests.)*
- Idempotency is the complement: sinks tolerate re-delivery (upsert / version columns / dedup
  keys) so at-least-once is observationally exactly-once.
- **Graceful `Stop()` flush uses a fresh `context.WithTimeout(context.Background(), ~10s)`**, not
  the cancelled loop ctx — on both linear and DAG paths. *(P4-2, verified landed.)*

### 4.3 Error messages for humans (WHERE matters)

Open-source users see these errors. Rules:

- State **what** failed, **where** (which plugin / host:port / db / table / pipeline), and **why**
  (the underlying cause).
- When a startup check can fail, offer the **remediation**: e.g. `mysql_cdc source "src":
  binlog_format is STATEMENT, must be ROW — run SET GLOBAL binlog_format='ROW'`.
- Never expose raw stack dumps as the primary message; wrap with context.

> **P5-15 (✅ done):** sink/source connect/ping/create errors across all 9 sinks and the
> CDC/batch sources now carry WHERE context — `(host %s:%d, db %s)` for DB sinks,
> `(brokers %v, topic %s)` for kafka, `(endpoint %s, bucket %s, region %s)` for s3 — replicating
> the `doris.go` template. A user with two ClickHouse instances can now tell which failed.
> Per-write errors largely already carried the table object.

### 4.4 Interface, not metadata

Prefer typed optional interfaces over `map[string]any` capability flags. The only legitimate use
of untyped config maps is plugin construction; unknown keys are ignored with a debug log, not an
error, to keep specs forward-compatible.

---

## 5. Testing Strategy

### 5.1 Pyramid

| Layer | Scope | Tooling | Required for PR |
|-------|-------|---------|-----------------|
| **Unit** | Pure functions, interfaces, type mappers, DDL, DAG routing, retry, breaker | `go test -race` in-package | ✅ All |
| **Integration** | Sink writes vs live DBs (type inference, idempotency, auto-create), source resume | `_test.go` `//go:build integration`, podman | ✅ Changed plugin |
| **E2E** | Full pipeline MySQL CDC → ClickHouse, crash recovery, DLQ replay, distributed | `hack/e2e-*.sh` over podman-compose | ✅ Pipeline/core changes |

### 5.2 Test matrix for dual-mode

Every feature touching storage or dispatch is verified in **both** modes:

| Feature | SQLite (single-node) | MySQL/PG (scalable) |
|---------|----------------------|---------------------|
| Pipeline CRUD + run | ✅ required | ✅ required |
| Checkpoint save/resume | ✅ required | ✅ required |
| DLQ write/replay/delete | ✅ required | ✅ required |
| Shard dispatch | runs inline | ✅ distributed across workers |
| Concurrent pipelines | ✅ single process | ✅ cross-process |

### 5.3 Reliability invariants (always tested)

- **At-least-once**: crash mid-batch → on restart the last batch is re-read.
- **Idempotency**: replay the same batch to an upsert sink → no duplicates.
- **Graceful shutdown**: SIGTERM mid-write → committed data survives, in-flight batch flushed or
  safely re-delivered.
- **Version monotonicity**: ClickHouse `_version` strictly increases under concurrency.
- **Zero-loss**: a forced sink failure → record appears in DLQ, never vanishes.
- **Resume-correctness**: a file/HTTP source restarted mid-stream resumes at the exact next record
  — no skip, no replay-flood. *(P5-2, P5-8 add the missing tests.)*

### 5.4 Race & build hygiene

- `-race` is the default for all test commands.
- `go vet ./...` clean for any modified package.
- No test depends on wall-clock ordering where logical ordering suffices.

### 5.5 Coverage gaps exposed by this audit (NEW in v3)

The two P0 regressions (P5-1, P5-2) reached `main` because the test suite did not exercise the
specific paths. Close these gaps as part of Phase 5:

- **No test starts a standalone-role / single-shard pipeline through `Server.newRunner`** → the
  infinite recursion (P5-1) was invisible. Add a server-level integration test that boots a
  standalone pipeline and asserts it reaches `running` (not a stack-overflow fatal).
- **No test restarts a JSON-lines file source and asserts resume-correctness** → P5-2 was
  invisible. Add a write-N / checkpoint / restart / expect-next-N test.
- **`internal/etl/pipeline` and `orchestrator` `-race` tests pass, but do not exercise the
  concurrent `DAGExecutor.readers` path nor `ParallelRunner` Start/Stop concurrency** → P5-3,
  P5-4 are latent. Add concurrent Start/Stop and multi-source DAG tests under `-race`.
- **Per-sink error-metric tests absent** → P5-12 (dead `recordError`) was invisible. Add a test
  that forces a sink failure and asserts Prometheus `Errors` increments.

---

## 6. Boundaries (hard constraints)

Non-negotiable. A change that violates one must be rejected or must first update this SPEC with
explicit sign-off.

### 6.1 Must always do

- **Single static binary, pure Go (default build).** No CGO in the default build; no runtime
  dependency on JVM/Python/Node for core function. **All opt-in script runtimes — WASM, Lua,
  QuickJS — must be gated behind build tags** so a deployment that does not use them does not link
  them. Status: WASM ✅ (`//go:build extism`), QuickJS ✅ (`//go:build cgo`), **Lua ✅ opt-out**
  (`//go:build !nolua` — default keeps Lua; `-tags=nolua` drops gopher-lua from the binary, P5-22).
  New external dependencies require review for binary-size and supply-chain impact.
- **Zero data loss — on every path, including DAG, shutdown, and the DLQ-failure path.** Every
  record reaches the sink (after retry) or the DLQ; a DLQ-write failure escalates (alert +
  breaker + no-checkpoint-advance). A zero-survivor batch does not silently advance the
  checkpoint. *(P5-9, P5-10.)*
- **Script runtimes are resource-bounded.** Lua and QuickJS enforce a per-record memory cap **and**
  a CPU/instruction/time budget; a runaway script errors one record, never hangs or OOMs.
- **Backward-compatible YAML specs.** Existing `pipes/*.yaml` and user specs keep working. New
  fields default sensibly; removed fields require a deprecation cycle. `ValidateSpec` rejects
  unknown plugins but tolerates unknown config keys.
- **Dual-mode parity of the core.** Anything working in SQLite mode also works in MySQL/PG
  (dispatch is the only intentional divergence).
- **Honest capability claims.** A feature is "done" only when (a) code implements it, (b) a test
  against real code proves it, and (c) `pluginMetadata()` / README / this SPEC agree with the
  implementation. Optional interfaces confer a capability only when ≥1 shipped plugin implements
  them. A refuted claim is retractable within the same release (the v2→v3 P4-23 retraction is the
  precedent).

### 6.2 Ask first about

- New top-level dependencies (any `go get` of a non-trivial library).
- Changes to the `core.Source/Sink/Transform` interfaces (the public plugin ABI).
- Auto-apply of DDL by default (`ddl_policy: apply`) — destructive; prefer opt-in.
- Anything that adds a required external service (etcd, zookeeper, an extra broker).

### 6.3 Never do

- **No external orchestrator dependency** (etcd/ZK/K8s operator) for core function. Distribution
  uses the shared SQL store + heartbeat, nothing else.
- **No silent data drop** to make a test or pipeline "succeed".
- **No breaking the `integration`/unit split** — unit tests run with no live services.
- **No forking the two code paths** (`internal/etl/` vs `internal/logic/sync/`) further. The ETL
  framework is canonical; the Canal path is frozen, scheduled for deprecation.
- **No distributed claims in SQLite mode**, and no distributed *execution* claim in MySQL/PG until
  a real second binary runs a worker (A11-redo satisfied this).
- **No untrusted/network-fetched tooling at request time** — no `npx --yes <pkg>` auto-fetch on a
  server path; pin versions, prefer pre-installed binaries (P4-5, landed; **P5-26: default
  `extismPkg` still unpinned**). User names joined into paths are validated
  (`^[A-Za-z0-9_.-]+$`).
- **No in-place mutation of `Record.Data`** (a shared map) across a transform chain without a
  defensive copy.

---

## 7. Phase 5 — Gap → Workstream (the development plan)

Mapping the SPEC bars to the remaining work. Status reflects the **2026-06-22 independent
three-pillar audit** (verified against code, not the TODO log). IDs `P5-n` are this phase.
Carried-forward v2 items keep their `P4-n` ID. Evidence and fix sketches are summaries; the v2
§9 register (`docs/SPEC-v2-reaudit-2026-06.md`) holds the original detail for carried items.

### Tier A — P0 (gating; blocks ALL production use)

| ID | Gap | Fix | Acceptance | Size | Mode |
|----|-----|-----|------------|------|------|
| **P5-1** | `Server.newRunner` infinite-recurses on every non-distributed path → standalone (default) & single-shard pipelines stack-overflow at start. `server.go:78`. | ✅ Closed: non-distributed path now returns `pipeline.NewPipeline(...)`; guarded by `internal/etl/server/newrunner_test.go`. | `TestNewRunnerNotRecursive`; `./hack/e2e.sh`; `./hack/e2e-ui.sh`. | 0.25d | both |
| **P5-2** | JSON-lines file source `byteOffset` seeded with absolute resume offset (`file.go:181`), then `Snapshot` emits `base+offset` → offset doubles each restart → records skipped. | ✅ Closed: resume offset no longer compounds. | `TestFileJSONCheckpointNoCompoundOnRestart`; `go test ./internal/etl/source`. | 0.25d | both |

### Tier B — P1 reliability (correctness / safety nets)

| ID | Gap | Fix sketch | Size | Ref |
|----|-----|------------|------|-----|
| P5-3 | `DAGExecutor.readers` map read unlocked (`executor.go:578`) vs locked writes (`:357,:361`) → `-race` latent. | ✅ Closed: `checkpointForRecord` now reads `e.readers` under `RLock`. | 0.25d | done |
| P5-4 | `ParallelRunner.cancel` assigned after `Unlock` (`parallel.go:165`) → Start/Stop race. | ✅ Closed: `context.WithCancel` assignment happens under `pr.mu`. | 0.25d | done |
| P5-5 | ES sink returns `nil` on unparseable bulk response (`elasticsearch.go:308-322`) → unknown commit state, checkpoint advances. | ✅ Closed: 2xx/unparseable bulk responses now return an error. | 0.25d | done |
| P5-6 | Kafka producer silently falls back to non-idempotent mode (`kafka.go:199-208`) → duplicate risk on retry. | ✅ Closed: fallback is explicit and emits a warning with broker/topic context. | 0.25d | done |
| P5-7 | `postgres_cdc` unknown pgoutput message type `return`s, dropping the rest of the frame (`postgres_cdc.go:460-463`); LSN still ACKed → silent loss on future PG message types. | ✅ Closed for protocol v1 known non-row messages: `O`/`Y`/`M` are skipped by wire format; unknown types still hard-error the frame. | 0.5d | done |
| P5-8 | ~~HTTP source advances `committedPage` before sink~~ — **✅ retracted (false-positive, like P4-17)**: `committedPage` is in-memory only; the persisted checkpoint is gated on sink-write via `CheckpointForRecord`; restart resumes from the persisted page and re-fetches (at-least-once). The proposed "drain-gated" fix is already the existing behavior. | — | done (retracted) |
| P5-9 | DLQ write-failure path never trips the circuit breaker (`pipeline.go:913-922`). | ✅ **Linear + DAG**: linear `Runner` calls `circuitBreaker.RecordFailure(ctx, dlqErr)`; DAG `handleFailed` now takes `sinkID` and trips `e.breakers[sinkID]` on DLQ-write failure (Wave 4). | 0.25d | done |
| P5-10 | Zero-survivor batch saves checkpoint before any sink write (`pipeline.go:783-786`); combined with no-DLQ-configured silent drop (`:906-926`) → permanent loss. | ✅ Closed: checkpoint only advances when every dropped record is `ErrRecordFiltered`; batch-empty/error paths log+alert and replay. | 0.5d | done |
| P5-11 | Worker poll loop slot check uses `len(w.executors)` but `ExecuteShard` goroutine never registers (`worker/poll.go:98-101,133`) → unbounded concurrent shard fan-out. | ✅ Closed: atomic `inFlight` counter gates task claims; `TestPollLoopRespectsSlotsLimit` covers it. | 0.5d | done |
| P5-12 | `sinkCounters.recordError()` is dead code in all 9 sinks (`sink_metrics.go:30`) → Prometheus `Errors` permanently 0. | ✅ Closed: each sink `Write` defers `recordError()` on failure and exposes `SinkMetrics()`. | 0.5d | done |

### Tier C — 易用 (usability; production-blocking per §1.4)

| ID | Gap | Fix sketch | Size | Impact |
|----|-----|------------|------|--------|
| **P5-13** | **README.md advertises the legacy Canal product, not the ETL framework** (Canal mode, hardcoded creds, manual DDL) — a new user clones and reads about the wrong product. | ✅ Closed: README now leads with OpenETL-Go’s ETL/CDC model, `/api/v2/*`, quickstart, docs, and capability matrix. | 1d | H→done |
| **P5-14** | Pipeline create/update (`handlePipelines` POST/PUT, `server.go:1086-1128`) calls only `ApplyDefaults`+`ValidateSpec`, never `RunPreflight` → misconfigs (bad host) return `valid` and fail late/opaque at `/start`. | ✅ Closed: create/update call `RunPreflight`, reject `level:error`, and return warnings in the response. | 0.5d | H→done |
| **P5-15** ✅ | Sink/source error messages omit WHERE (host/port/db/table) — §4.3 violation (`clickhouse.go`, `mysql_cdc.go`, others). | ✅ Done (Wave 4): connect/ping/create errors across 9 sinks + CDC/batch sources now carry `(host:port, db)` / `(brokers, topic)` / `(endpoint, bucket, region)`, replicating the `doris.go` template. | 1d | H→done |
| **P5-16** | **P4-23 refuted**: JSON logging is inert — no code reads `LOGGER_FORMAT`/`stdoutFormat`/`fileFormat`; config comment is false. | ✅ Closed: `internal/logic/app/logging.go` installs a real glog JSON stdout handler when `LOGGER_FORMAT=json`. | 0.5d | H→done |
| P5-17 | Shipped quickstart example pipe broken — combined `host: quickstart-clickhouse:9000` (`pipes-quickstart/order-aggregation-demo.yaml:31`) → DNS fail at `clickhouse.Open`. | ✅ Closed: quickstart pipe uses distinct `host` / `port` and matching HTTP settings. | 0.1d | H→done |
| P5-18 | `GET /api/v2/plugins/schema` omits real keys (`auto_create`/`schema_drift`/`insert_chunk_size` for mysql/postgres — `schema.go:129-140,162-172`) → users conclude auto-create unsupported. | ✅ Closed: schema includes implemented fields/aliases across sources, sinks, and transforms; covered by `schema_test.go`. | 0.5d | M→done |
| P5-19 | Default `pipes/` has no `mysql_cdc → clickhouse` canonical example (SPEC §1.3); it lives only in `pipes-quickstart/`. | ✅ Closed: `pipes/mysql-cdc-to-clickhouse.yaml` ships as the canonical example. | 0.25d | M→done |
| P5-20 | `docker-compose.dev.yml` doesn't pass `CLICKHOUSE_HOST/PORT/PASSWORD` to the ETL container → monitor writes silently fail inside the container. | ✅ Closed: dev compose now injects ClickHouse monitor env vars into `openetl-go`. | 0.1d | M→done |
| P5-21 | Quickstart drift: `docker-compose.quickstart.yml` `MYSQL_DATABASE: demo` vs init SQL `dzh3136_go`; `docs/quickstart.md` stale hostnames + `file_sink` example uses `path` (should be `output_dir`). | ✅ Closed: quickstart compose/docs/specs are aligned on `dzh3136_go`, ClickHouse HTTP, and `output_dir`. | 0.5d | M→done |

### Tier D — 轻量 (lightweight; §6.1)

| ID | Gap | Fix sketch | Size | Impact |
|----|-----|------------|------|--------|
| P5-22 | Lua (`gopher-lua`) linked into the default binary (`lua.go` + `pipeline/hooks.go`). | ✅ **Done (Wave 4)**: opt-out `//go:build !nolua` — the default build keeps Lua (non-breaking), `-tags=nolua` drops gopher-lua via `lua_nolua.go` + `lua_hook_nolua.go` stubs; `type:lua` returns a clear error under nolua. Verified: `go list -deps -tags=nolua` is free of gopher-lua. | — | done |
| P5-23 | GoFrame HTTP server boots alongside the ETL API. | **Retracted/deferred**: the GoFrame server (`:8000`) serves the UI (`resource/public`) AND proxies `/api/v2/*` to `:8001` — "skipping" it removes the UI. The dual-listener is intentional (unified port). A headless API-only mode is a feature, not a fix. | — | retracted |
| P5-25 | `config.yaml` was legacy-Canal-heavy. | ✅ Default `manifest/config/config.yaml` is now a minimal single-node ETL template (server+etl+logger, SQLite, no Canal/sync/database). | 0.5d | done |
| P5-26 | Default `extismPkg` unpinned. | ✅ Pinned `@extism/js-pdk@1.1.0` (Wave 2); env overrides. | 0.1d | done |
| P5-24 *(optional/defer)* | All 9 sink connectors linked unconditionally (~77MB binary is sink-dominated). | Per-sink build tags with no-op stubs, or a `sinks_all` default. | 2–3d | M |

### Tier E — Carry-forward (verify open / finish)

| ID | Gap | Note | Size |
|----|-----|------|------|
| P4-3 | Lua transform per-record CPU/memory budget (v2 TF-1). | ✅ **Verified present** (`lua.go:29-38`, `timeout_ms` per-record budget). | done |
| P3 polish | v2 §9 P3 list (alert-queue overflow via `fmt.Printf`, MySQL `VALUES()` deprecation, SQLite `LIMIT` parameterization, redis checkpoint non-determinism, dead `inferColumnType`, Doris stream-load label collision, etc.). | Catalogued in v2 §9; not blocking. Tackle opportunistically. | — |

### Sequencing

```
Wave 0 — hotfix (unblock default mode + quickstart), ~0.5d:
   P5-1, P5-2, P5-17, P5-20            ← tiny, zero-risk, restores standalone + quickstart
Wave 1 — reliability P1, ~3–4d:
   P5-3..P5-12                          ← zero-loss + race-clean + real metrics
Wave 2 — 易用, ~3–4d:
   P5-13, P5-14, P5-15, P5-16, P5-18, P5-19, P5-21
Wave 3 — 轻量, ~2–3d:
   P5-22, P5-23, P5-25, P5-26 (P5-24 optional/defer)
Carry-forward: P4-3 folds into Wave 3 (P5-22).
```

**Sizing total (Waves 0–3): ~9–12 days** to dual-mode production-ready with the 易用/轻量 bar met.

### Phase 5 risk

| Risk | Mitigation |
|------|------------|
| P5-1 fix changes runner wiring widely | One-line constructor swap; the regression test pins it; all `hack/e2e-*.sh` re-run. |
| P5-22 (Lua build tag) breaks users who relied on Lua by default | **Mitigated (Wave 4):** the gate is an opt-OUT (`-tags=nolua`), so the default build keeps Lua byte-for-byte — no deprecation cycle needed (§6.1). |
| P5-13 README rewrite loses legacy-Canal users | Keep a clearly-labeled "Legacy Canal mode" section; link v2 docs. |
| More latent races surface under new `-race` tests (§5.5) | Budget Wave 1 conservatively; each race is a small targeted fix. |

---

## 8. Verification (how Phase 5 acceptance is proven)

```bash
# Wave 0 smoke: standalone pipelines boot again
podman exec etl-go-dev go build ./...
podman exec etl-go-dev go test -race -count=1 ./internal/etl/server/... ./internal/etl/source/...

# Waves 1–3: full reliability + race
podman exec etl-go-dev go test -race -count=1 ./internal/etl/...

# Lightweight: default build excludes all opt-in runtimes
podman exec etl-go-dev go list -deps ./... | grep -E 'gopher-lua|quickjs|extism|wazero'   # must be EMPTY for default
podman exec etl-go-dev go build -tags=extism -o /tmp/oa-extism .                          # still compiles
podman exec etl-go-dev go build -tags=nolua -o /tmp/oa-nolua .                            # 轻量: Lua opt-out (P5-22)

# Dual-mode E2E
./hack/e2e.sh                       # file→file, mysql_batch→mysql
./hack/e2e-clickhouse.sh            # mysql_cdc → clickhouse (canonical)
./hack/e2e-dlq.sh                   # DLQ list/replay/delete
./hack/e2e-crash-recovery.sh        # checkpoint + crash
./hack/e2e-distributed.sh           # scalable mode: 4 shards / 2 workers / crash reassign

# Usability: README quickstart runs unmodified end-to-end
./hack/e2e-quickstart.sh            # (new) clone → compose → first row < 5 min
```

**Definition of Done (per item)**: every acceptance checkbox met · `-race` green for changed
packages · relevant `hack/e2e-*.sh` green · `go vet` clean · SPEC and paired docs updated
if the bar moved.

---

## 9. Appendix A — Phase 4 verification (2026-06-22 independent)

Verified against code (not the TODO log) by the three-pillar audit:

| Phase 4 item | Verdict | Evidence |
|--------------|---------|----------|
| P4-1/P4-9 (DAG DLQ+ckpt errors) | ✅ landed | `orchestrator/executor.go:588-614` (DLQ, counter on success only), `:546-573` (ckpt, log+breaker+alert) |
| P4-2 (fresh ctx on Stop flush) | ✅ landed | `pipeline.go:698,713,734`; `executor.go:414` — all flush sites use fresh `WithTimeout(ctx.Background(),…)` |
| P4-8 (DLQ writer fsync) | ✅ landed | `dlq/dlq.go:84-86` |
| P4-10 (DLQ replay delete-by-ID) | ✅ landed | `server.go:2750-2763`, `dlq_compat.go:110-115` — per-item after sink success |
| P4-12 (dedup mutex) | ✅ landed | `transform/router.go:143,224-225` |
| P4-15 (per-record panic recovery) | ✅ landed | `executor.go:620-629`; `pipeline.go:549-556` (recover→StatusFailed) |
| P4-5 (WASM compile name validation) | ✅ landed | `server.go:2347-2373` (charset+`..`+len), both upload sites; **P5-26: pkg still unpinned by default** |
| P4-22 (CH+Doris unified typing) | ✅ landed | `clickhouse.go:1162-1167`, `doris.go:884-889` → `typing.InferFromValue` |
| P4-19 (SchemaValidator honesty) | ✅ landed | `core.go:113-142` documented as extension-only |
| P4-24 (AI generate mounted+validated) | ✅ landed | route `server.go:624`; `ValidateSpec`+`RunPreflight` at `:3296-3303` |
| P4-25 (WASM build tag) | ✅ landed | `types.go` (no tag), `manager.go`/`hostfunc.go`/`source_sink.go`/`transform.go` (`extism`), `nop.go` (`!extism`); both builds compile; wazero excluded from default |
| A11-redo (real distributed) | ✅ landed (real) | `worker/executor.go:39-71`, `worker/worker.go:64-81`, `master/dispatch.go:33-101`, `pipeline/parallel.go:210-259`, `pipeline/shard_builder.go:26-68`, `logic/app/app.go:206-272`; **P5-11: worker slot accounting broken** |
| **P4-23 (JSON logging)** | ❌ **inert / refuted** | No code reads `LOGGER_FORMAT`/`stdoutFormat`/`fileFormat`; config comment false → re-opened as **P5-16** |

## 10. Appendix B — prior re-audit preserved

The full v2 (2026-06-21) six-subsystem re-audit findings register (P0/P1/P2/P3, ~35 items, with
`file:line` evidence and fix sketches) is preserved verbatim at
[`docs/SPEC-v2-reaudit-2026-06.md`](./docs/SPEC-v2-reaudit-2026-06.md) §9. Items still open after
Phase 4 are carried into §7 here (P4-3, the P3 polish list). Items confirmed solid (linear
at-least-once, breaker, retry, Kafka/HTTP/file/Redis checkpointing, multi-row batch writes,
ClickHouse `_version` monotonicity, storage conformance) remain the reliability foundation and
are not re-litigated.
