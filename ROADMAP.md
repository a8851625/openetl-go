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
```

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

## Sizing Summary

| Phase | Items | Effort |
|-------|-------|--------|
| Phase 1 (gating) | A10, A11 | 5–7 days |
| Phase 2 | B1–B5 | 6.5–8.5 days |
| Phase 3 | freeze + docs | 2 days |
| **Total to OSS-launch-ready** | | **~13–17 days** |

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
