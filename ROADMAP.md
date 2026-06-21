# openetl-go · Production-Ready Roadmap

> **Companion to**: [`SPEC.md`](./SPEC.md) (Draft v1, 2026-06-21)
> **Purpose**: Operationalize SPEC §7 workstream into sequenced, sized, acceptance-criteria-driven tasks.
> **Convention**: SPEC defines the bar; this file tracks reaching it; `TODO.md` holds the per-item done log.

---

## Phasing

```
Phase 1 — Close the production-blocker gaps (Tier A open items)
           A10 storage matrix  ──►  A11 distributed dispatch
           (unblocks the "scalable mode" production claim)

Phase 2 — Open-source launch readiness (Tier B)
           B1 quickstart · B2 startup checks · B3 logging
           B4 postgres_cdc · B5 S3/ES hardening

Phase 3 ✅ — Freeze legacy, docs pass

Phase 4 — Re-audit closure (2026-06-21)  ← CURRENT
           P4-A P0 data-loss/security (incl. A11-redo decision)
           P4-B P1 correctness/safety-nets
           P4-C P2 advertised-missing & honesty
           (closes gaps the v1 "all done" claims overstated; see SPEC §9)
```

> **Phase 4 is gating**: the v1 "production-ready" claim rested on several items (A8, A9, A11, B3, P1-6 budget, P1-6 DLQ-replay, P2-1) that the re-audit refuted or partial'd. They must be resolved (closed honestly or rescoped in SPEC) before the v4 release.

Phase 1 is gating: the project cannot claim production-ready for scalable mode until A10 + A11 pass. Phase 2 is gating for the OSS launch but not for internal/single-node production use.

---

## Phase 1 — Production-Blocker Closure

### A10 — MySQL/PG storage backend verification matrix

**Why**: SPEC §1.5, §3.3, §5.2. The `mysql` and `postgresql` storage backends exist (`internal/etl/storage/{mysql,postgres}/`) but have never been exercised against live databases. The scalable-mode claim (SPEC §1.2) depends entirely on these being correct.

**What**: Build a storage conformance suite that runs the full `storage.Storage` interface against real MySQL and PostgreSQL, mirroring what SQLite already covers.

**Acceptance criteria**:
- [ ] `internal/etl/storage/conformance_test.go` (build tag `integration`) — one parameterized suite run against SQLite, MySQL, and PostgreSQL via a `storageBackend` test var.
- [ ] Suite covers: pipeline spec CRUD + versioning, checkpoint save/load/reset, DLQ write/list/replay/delete + TTL purge, audit log append/read, run-history record/query, worker register/heartbeat/reap, task create/claim/complete.
- [ ] `hack/e2e-storage-mysql.sh` and `hack/e2e-storage-postgres.sh` pass in podman.
- [ ] Concurrency test: two writers to the same checkpoint key → last-writer-wins or versioned, no corruption.
- [ ] Migration parity: a pipeline defined under SQLite, exported, imported into MySQL → identical behavior.

**Sizing**: 2–3 days. Backend code likely surfaces bugs; budget time for fixes.

**Dependencies**: none (existing storage impls).

**Key files**:
- New: `internal/etl/storage/conformance_test.go`, `hack/e2e-storage-mysql.sh`, `hack/e2e-storage-postgres.sh`
- Likely edits: `internal/etl/storage/mysql/*.go`, `internal/etl/storage/postgres/*.go`
- Reuse: `internal/etl/storage/sqlite/sqlite_test.go` as the conformance reference

**Verification**: `make test-integration` green with `ETL_STORAGE_TYPE={mysql,postgresql}`.

---

### A11 — Master-worker distributed dispatch, verified end-to-end

**Why**: SPEC §1.5, §3.4, §5.2. `master/` and `worker/` exist (dispatch.go, poll.go, master.go, worker.go) but the "2-worker distributes shards of a 4-shard pipeline" scenario (TODO.md P2-3) was never run. This is the proof that scalable mode actually scales.

**What**: Wire dispatch into the server bootstrap when `storage.type != sqlite`, and prove distribution with a multi-instance E2E.

**Acceptance criteria**:
- [ ] `internal/logic/app/app.go` (or `etl/server`) starts a `master.Master` + standalone `worker` when storage is MySQL/PG; in SQLite mode, dispatch is short-circuited (shards inline) per SPEC §3.4.
- [ ] `POST /api/v2/workers/{id}/poll` returns the next pending shard; documented.
- [ ] Heartbeat timeout (configurable, default 60s) reassigns a dead worker's shards to a live one.
- [ ] `hack/e2e-distributed.sh`: spin up shared MySQL store + 2 `openetl-go` instances, run a 4-shard `mysql_batch` pipeline, assert (a) shards split across workers with no overlap, (b) total output row count matches source.
- [ ] Crash test: kill worker-1 mid-shard → its shards are reassigned to worker-2 within `heartbeat × 2`, no records lost.
- [ ] Health endpoint reports distributed topology: registered workers, assigned shards, lag.

**Sizing**: 3–4 days. Dispatch code is the riskiest untested surface; expect bug fixes.

**Dependencies**: **A10** (need a verified MySQL backend to share state).

**Key files**:
- Edits: `internal/etl/server/server.go` (wire Master into lifecycle), `internal/etl/master/*.go`, `internal/etl/worker/*.go`, `internal/logic/app/app.go`
- New: `hack/e2e-distributed.sh`
- Reuse: `internal/etl/pipeline/parallel.go` (shard extraction)

**Verification**: `hack/e2e-distributed.sh` green; `curl :8000/api/v2/health` shows topology.

**Defer note**: If dispatch surfaces deep issues, the fallback per SPEC §6.3 is to **document SQLite mode as the only production-supported mode for v1** and ship A11 as v1.1. This keeps the release unblocked — decision point at end of A11 spike.

---

## Phase 2 — Open-Source Launch Readiness

### B1 ✅ — 5-minute quickstart, validated

**Why**: SPEC §1.3, §1.5. An OSS tool that can't be tried in 5 minutes loses its audience.

**Acceptance**:
- [ ] `README.md` quickstart: clone → `podman compose -f docker-compose.quickstart.yml up` → first row in ClickHouse, **timed < 5 min on a fresh machine** (no pre-existing containers).
- [ ] A single example `pipes/quickstart.yaml` (sqlite storage, mysql_cdc → clickhouse, auto_create on) that works unmodified.
- [ ] Troubleshooting section covers the top 5 first-run failures (binlog off, wrong port, password, table missing, timezone).

**Sizing**: 1 day. **Dependencies**: none.

---

### B2 ✅ — Preflight startup checks

**Why**: SPEC §4.3. Currently a misconfigured source fails late and opaquely.

**Acceptance**:
- [ ] `internal/etl/server/preflight.go` runs before pipeline start: MySQL binlog format/row-image, `REPLICATION SLAVE/CLIENT` grants, source tables exist, sink reachable, server_id uniqueness hint.
- [ ] Each failure prints **what + where + remediation** (SPEC §4.3). Example: `mysql_cdc source "src": binlog_format is STATEMENT, must be ROW — run SET GLOBAL binlog_format='ROW'`.
- [ ] `POST /api/v2/specs/validate` invokes preflight in dry-run mode (no connections held open).
- [ ] `--preflight` or `OPENETL_PREFLIGHT=1` runs all checks and exits non-zero on failure.

**Sizing**: 1–2 days. **Dependencies**: none.

---

### B3 ✅ — Structured (JSON) logging option

**Why**: SPEC §1.5. Unstructured logs can't feed Loki/ELK cleanly.

**Acceptance**:
- [ ] `logger.format: json` in config → GoFrame emits JSON lines with level/ts/msg/fields.
- [ ] Pipeline log lines include `pipeline`, `source`, `sink` as structured fields where relevant.
- [ ] Secrets masked in logs (reuse existing mask logic).

**Sizing**: 0.5 day. **Dependencies**: none.

---

### B4 ✅ — postgres_cdc completeness

**Why**: SPEC §1.4. `postgres_cdc` is pgoutput-only and silently skips `TRUNCATE` and DDL.

**Acceptance** (pick scope at kickoff):
- [ ] Emit `OpDDL` records for `ALTER TABLE` via `pgoutput` where available, OR document the limitation in `server.go pluginMetadata()` and README.
- [ ] Handle `TRUNCATE` (either as a batch-delete or a documented no-op with a warning).
- [ ] Integration test: postgres_cdc → postgres sink captures INSERT/UPDATE/DELETE with correct types (extends existing `postgres_cdc_test.go`).
- [ ] Optionally add `wal2json` support if pgoutput gaps prove blocking.

**Sizing**: 2–3 days. **Dependencies**: a Postgres container in dev compose (add to `docker-compose.dev.yml`).

---

### B5 ✅ — S3 + Elasticsearch sink hardening

**Why**: SPEC §1.4. `s3` lacks multipart/retry; `elasticsearch` lacks cluster failover + 429 handling.

**Acceptance**:
- [ ] S3: use `manager.Uploader` multipart; retry on 5xx with backoff; e2e uploads a >100MB object to MinIO.
- [ ] ES: round-robin across configured `hosts`; retry on 429 honoring `Retry-After`; e2e against a 2-node setup (or simulated).
- [ ] Both covered by `hack/e2e-s3.sh` / `hack/e2e-elasticsearch.sh`.

**Sizing**: 2 days. **Dependencies**: MinIO + ES already in dev compose.

---

## Phase 3 ✅ — Freeze & Document

| Task | Detail | Sizing |
|------|--------|--------|
| Legacy path freeze | Mark `internal/logic/sync/` as deprecated in code comments + README; remove from default config examples; keep compiling for one release. | 0.5 day |
| SPEC ↔ reality audit | Walk SPEC §1.5 production-ready definition line by line against the shipped binary; update SPEC status to `Stable v1`. | 0.5 day |
| Release notes + migration guide | v3.x → v4 changelog; YAML spec migration notes; dual-mode deployment guide. | 1 day |

---

## Phase 4 — Re-Audit Closure (2026-06-21)

> **Trigger**: the 2026-06-21 independent six-subsystem re-audit (see `SPEC.md` §9) found that several v1 "done" claims were overstated and surfaced ~35 new gaps (6 P0, 15 P1, 13 P2/P3). Phase 1–3 are **not** reopened; Phase 4 closes the real residual work. Item IDs (`P4-n`) map 1:1 to `SPEC.md` §7.

### P4-A — P0 closure: data loss & security (gating)

**Why**: these are direct violations of the zero-loss (§6.1) and security boundaries, or of the central "scalable mode scales" architecture claim.

| ID | Work | Acceptance | Sizing | Ref |
|----|------|-----------|--------|-----|
| P4-1 | DAG executor: stop swallowing DLQ-write errors; mirror linear Runner's log+alert+no-checkpoint-advance+breaker; only increment `RecordsDLQ` on success | Unit test: force DLQ backend down → no `RecordsDLQ` increment, alert fired, checkpoint not advanced | 0.5 day | PC-1/TF-9 |
| P4-2 | Fix graceful `Stop()` flush: `forceFlushAndCheckpoint` + EOF flush + DAG `flushAll` use a fresh `context.WithTimeout(context.Background(), 10s)`, not the cancelled loop ctx | Race-free test: SIGTERM mid-batch → in-flight batch reaches sink, checkpoint saved, no duplicate on restart | 1 day | PC-2/PC-6 |
| P4-3 | Lua transform: per-record CPU budget via instruction-count debug hook + memory cap (`SetMState`); honor `ctx` via goroutine+select | Test: `while true do end` errors one record (→ DLQ) within the budget, does not hang | 1–1.5 days | TF-1 |
| P4-4 | QuickJS transform: `SetInterruptHandler` tied to `ctx`; eval-in-goroutine + select on `ctx.Done()` | Test: `while(true){}` aborts within timeout | 0.5–1 day | TF-2 |
| P4-5 | WASM compile path: validate `name` (`^[A-Za-z0-9_.-]+$`), reject `/`/`..`; pin `@extism/js-pdk@<ver>`; prefer pre-installed binary over `npx --yes` | Test: `../../etc/x` rejected; offline build still compiles (no npm fetch) | 0.5 day | TF-3 |
| **A11-redo (a)** | **Decision locked 2026-06-21: make distributed execution real.** Add `--role=master\|worker`; `ParallelRunner` delegates shard execution to `task_assignments` + worker poll (master assigns, worker long-polls `POST /api/v2/workers/{id}/poll`, executes the shard in-process, reports back); real two-binary E2E: 2 instances + shared MySQL split a 4-shard pipeline with no overlap; crash one worker → shards reassigned within `heartbeat×2`, no records lost. | True 2-binary E2E splitting 4 shards; `curl :8000/api/v2/health` shows topology | 4–5 days | ST-2 |

**Dependencies**: none. **Verification**: `make test -race` green; for P4-2 a new `TestStopFlushesInflightBatch`; for A11-redo the chosen path's E2E.

### P4-B — P1 closure: correctness & safety nets

**Why**: these are correctness regressions or missing safety nets that the reliability goal requires; several (PC-5, PC-7, TF-10, TF-6) can lose data or crash under realistic conditions.

| ID | Work | Sizing | Ref |
|----|------|--------|-----|
| P4-6 | `Runner.lastRecordAt`: `RLock` the alert-checker read, or store as `atomic.Int64` | 0.25 day | PC-3 |
| P4-7 | `retry`: guard `rand.Int63n(0)` panic; validate `InitialInterval > 0` in `DefaultConfig` | 0.25 day | PC-4 |
| P4-8 | File DLQ writer: `f.Sync()` after write (config-gated batching) | 0.25 day | PC-5 |
| P4-9 | DAG executor: check checkpoint-save error → log+alert+trip breaker (mirror linear path) | 0.5 day | PC-7 |
| P4-10 | DLQ replay: carry stable ID on `DeadLetter`; delete-by-ID per item only after its sink write succeeds; return partial count on error | 1 day | SV-1 |
| P4-11 | Preflight: real sink reachability (`sink.Open` short-timeout, reuse `handleConnectionTest`); honor `level:error` as `valid:false` in spec-validate | 0.5 day | SV-2/SV-3 |
| P4-12 | Deduplicator transform: add `sync.Mutex` around cache mutation | 0.25 day | TF-6 |
| P4-13 | Join: `on_miss: drop\|dlq\|error` config (default `dlq` for inner) + join-miss metric | 0.5 day | TF-7 |
| P4-14 | Window: watermark + `allowed_lateness`; implement sliding/session or remove them from the docstring | 1–1.5 days | TF-8 |
| P4-15 | Per-record panic recovery: `defer recover()` in `routeAndWrite` and around `Apply` → `handleFailed`; writeLoop recover calls `setStatus(StatusFailed)` | 0.5 day | TF-10 |
| P4-16 | Enricher: return error (→ DLQ/retry); periodic cache sweep | 0.5 day | TF-13 |
| P4-17 | `mysql_batch`: compare `done` against the *effective* page limit, not `r.limit` | 0.25 day | SRC-5 |
| P4-18 | `ListTasks`: filter `status IN ('pending','assigned','running')` before LIMIT, or `ListPendingTasks` no-cap; finished-task janitor | 0.5 day | ST-1 |

**Dependencies**: P4-15 benefits from P4-1/P4-9 (shared DAG error-handling pattern). **Verification**: `go test -race ./internal/etl/...` clean (kills PC-3, TF-6); new tests per row.

### P4-C — P2 closure: advertised-missing & honesty

**Why**: these are capabilities advertised (in SPEC/README/UI) but not delivered, plus the lightweight-core build-tag violation. Closing them makes the project's claims trustworthy.

| ID | Work | Sizing | Ref |
|----|------|--------|-----|
| P4-19 | `SchemaValidator`/`SchemaDescriptor`: implement on 5 relational sinks + MySQL/PG CDC sources, OR remove the dead interfaces + pipeline wiring | 1–2 days | SK-3 |
| P4-20 | `SinkMetricsProvider` on MySQL/PG/Doris/JDBC/Kafka/ES/Redis/S3 (copy ClickHouse's `recordMetrics`) | 1 day | SK-4 |
| P4-21 | S3 Parquet: typed columns from sample value (`Int`/`Double`/`Timestamp`/`String`) | 0.5 day | SK-5 |
| P4-22 | ClickHouse + Doris: route auto-create through `typing.InferFromValue` (thread column names) | 1 day | SK-1/SK-2 |
| P4-23 | JSON logging: actually configure GoFrame glog `format: json` + document, or retract the B3 "done" claim | 0.25 day | SV-6 |
| P4-24 | AI-generation endpoint: mount the route; pipe LLM YAML through `ValidateSpec`+`RunPreflight` | 0.5 day | SV-4 |
| P4-25 | WASM behind `//go:build extism` (split registry from extism package) | 0.5–1 day | TF-4 |
| P4-26 | Router: add `Metadata.Route`; DAG edge-matching consults it (stop hijacking `Source`) | 0.5 day | TF-5 |
| P4-27 | Transform chain: deep-copy `Record.Data` at chain entry, or enforce no-mutation contract | 0.5 day | TF-11 |
| P4-28 | Lua: nil `os` outright (or metatable stub) + comma-ok assert | 0.25 day | TF-12 |
| P4-29 | Filter: `strict_types` config → error/DLQ on type mismatch | 0.25 day | TF-14 |

**Dependencies**: P4-22 depends on nothing (typing engine already exists); P4-19 is the larger of the two schema items. **Verification**: per-row unit/integration tests; P4-25 verified by `go build` producing a smaller WASM-free binary.

### Phase 4 risk

| Risk | Mitigation |
|------|-----------|
| A11-redo path (a) surfaces deep dispatch bugs | SPEC §6.3 fallback still applies: ship path (b) (honest relabel + dead-code removal) for v4, defer true distribution to v1.1. **Decision point at kickoff.** |
| P4-3/P4-4 sandbox budgets slow down hot transforms | Budget defaults generous; benchmark before/after; make budgets configurable per-transform. |
| P4-19 schema validation rejects existing user specs | `schema_policy: warn` default; only `strict` fails. |

---

## Sizing Summary

| Phase | Items | Effort |
|-------|-------|--------|
| Phase 1 (gating) | A10, A11 | 5–7 days |
| Phase 2 | B1–B5 | 6.5–8.5 days |
| Phase 3 | freeze + docs | 2 days |
| **Phase 4 (re-audit closure)** | P4-1…P4-29 + A11-redo | **~16–23 days** |
| **Total to OSS-launch-ready** | | **~30–40 days** (Phases 1–4) |

---

## Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| A11 dispatch has latent bugs that can't be fixed in-schedule | Medium | High | SPEC §6.3 fallback: ship SQLite-only as production-supported for v1, defer distributed to v1.1. Decision at A11 end. |
| MySQL/PG storage backends have subtle SQL dialect bugs | Medium | Medium | A10 conformance suite catches them before A11 depends on them. |
| postgres_cdc gaps block real PG users | Medium | Medium | B4 offers a document-the-limitation escape hatch. |
| Scope creep from "one more sink feature" | High | Medium | SPEC §6.2: new top-level deps and ABI changes require sign-off; Tier C is explicitly out of scope. |

---

## Definition of Done (per task)

A task is done when **all** hold:
1. Every acceptance checkbox is checked.
2. `make test` (unit, `-race`) is green for the changed packages.
3. The relevant `make test-integration` / `hack/e2e-*.sh` is green in podman.
4. `go vet ./...` is clean for changed packages.
5. The SPEC section it satisfies is updated if the work changed the bar.
6. A one-line entry is appended to `TODO.md` Done Log.
