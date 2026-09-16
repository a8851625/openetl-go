# Release Checklist (P5)

Use this checklist before cutting a release candidate or production tag.
It binds CI evidence, resource baseline, storage matrix, and residual risks.

## 1. Code gates (pipeline-enforced — not manual checks)

Since IT-1/T1.1 + T1.2 (roadmap RA-4) these gates run inside the reusable
workflow `.github/workflows/_gate.yml`; the aggregated `gate-passed` job is the
single verdict. `release.yml` and `release-beta-container.yml` both declare
`needs: gate`, so a red **or skipped** gate job blocks GitHub Release / GHCR
publishing. The table below documents what the pipeline runs — it must not be
checked by hand.

| Gate job | What the pipeline runs | Pass criteria |
| --- | --- | --- |
| lint | `go vet ./...` | exit 0 |
| connector-evidence | `hack/check-connector-evidence.sh` (+ `-strict` on main push) | exit 0 |
| unit-test | `go test -race -count=1 ./internal/etl/... ./internal/logic/...` | exit 0 |
| production-gate | telemetry/alert unit tests, release asset scan, production profile / runtime / SQLite backup smokes | exit 0 |
| storage-mysql | `hack/e2e-storage-mysql.sh` | exit 0 |
| storage-postgres | `hack/e2e-storage-postgres.sh` | exit 0 |
| integration-test | `go test -race -tags=integration ./internal/etl/...` (MySQL + ClickHouse services) | exit 0 |

### 1a. Aggregated verdict (skip is never pass)

`gate-passed` re-asserts `toJSON(needs)` with jq: **every** gate job must have
`result == "success"`. GitHub Actions does not fail a `needs` chain when an
upstream job is skipped, so skipped/cancelled jobs are counted as gate failures
here (IT-1 spec 交付约束 1). Callers: `test.yml` (push/PR), `release.yml` and
`release-beta-container.yml` (tag push; the beta workflow pins the gate to the
requested tag via the `ref` input, so the verdict binds to the tag commit).

### 1b. Exemption allowlist (empty by default)

`GATE_ALLOWLIST` in the `gate-passed` job is empty by default. An exemption may
be added only when a job cannot run for a concrete, time-bounded reason, and
must be recorded here with a re-review date:

| Job | Reason | Owner | Re-review date |
| --- | --- | --- | --- |
| (none) | — | — | — |

An allowlist entry without a row in this table is an undocumented silent skip
and must be reverted.

## 2. Storage / upgrade / backup matrix

| Backend | Upgrade | Logical backup | Restore drill | Evidence |
| --- | --- | --- | --- | --- |
| SQLite | migration lock + version | `hack/e2e-backup-restore-sqlite.sh` | file copy runbook | unit + script |
| MySQL | `hack/e2e-storage-mysql.sh` | `hack/e2e-backup-restore-mysql.sh` | mysqldump restore | skip if no env |
| PostgreSQL | `hack/e2e-storage-postgres.sh` | `hack/e2e-backup-restore-postgres.sh` | pg_dump restore | skip if no env |

Record each cell as `passed` / `skipped(<reason>)` / `failed` in the release notes.
Skipped external backends **do not** count as production certification for that backend.

## 3. Reliability / path contracts

- [ ] PR-2 path contracts green for declared production paths (`docs/path-contract.md`)
- [ ] RPO/RTO restated: **checkpointed at-least-once**; crash may replay last batch
- [ ] DLQ replay + sink outage matrix still referenced from release notes

## 4. Health & observability

- [ ] `/api/v2/health` reflects storage, redis_state (when configured), scheduler, workers, alert_queue, pipeline health
- [ ] Failed pipelines → overall `unhealthy` (HTTP 503); degraded lag/DLQ/checkpoint → `degraded` (HTTP 503)
- [ ] `/metrics` escapes pipeline labels (`"`, `\`, newline); exposes `etl_alert_dropped_total` and `etl_pipeline_health`
- [ ] UI `derivePipelineHealth` thresholds remain aligned (300s checkpoint / 60s CDC lag)

Threshold overrides (optional):

- `ETL_HEALTH_CHECKPOINT_STALE_SEC` (default 300)
- `ETL_HEALTH_CDC_LAG_MS` (default 60000)
- `ETL_HEALTH_WORKER_STALE_SEC` (default 30)

## 5. Production deployment profile

- [ ] `docker-compose.yml` requires `OPENETL_IMAGE`, `ETL_API_TOKEN`, `ETL_SPEC_ENCRYPTION_KEY`, TLS certs
- [ ] Image is a **pinned tag or digest**, never bare `latest`
- [ ] No `change-me` / empty token defaults in production compose or `deploy/production/*`
- [ ] Resource limits set (default 2 CPU / 2Gi; see `docs/resource-baseline.md`)
- [ ] JSON logs (`LOGGER_FORMAT=json`), audit on, DLQ TTL configured
- [ ] `deploy/production` package smoke: `scripts/smoke.sh` + `scripts/validate-pipes.sh`

## 6. Ops runbook drills (repeatable)

Follow [ops-runbook.md](./ops-runbook.md):

- [ ] Backup metadata (logical and/or physical)
- [ ] Restore into a clean directory / DB and reconcile object counts + sample checkpoint
- [ ] Forward upgrade from previous stable image/tag
- [ ] Rollback plan recorded (previous image + dump)
- [ ] Retention/janitor: `ApplyRetention` / DLQ TTL verified or scheduled

## 7. Frontend / packaging baseline

- [ ] `cd web && npm ci && npm run build` succeeds
- [ ] Bundle size regression noted if >20% over last release (record exemption)
- [ ] No new secrets in client logs (token remains memory-only)

## 8. Release notes must include

1. Supported storage/backend matrix with pass/skip/fail
2. Resource baseline snapshot (binary/image size, startup, idle memory)
3. RPO/RTO and at-least-once replay boundary
4. Known residuals (MaxCompute external, distributed PR-D1 beta, any skipped e2e)
5. Manual steps still required (TLS cert issuance, Kafka topic partitions, ODS grants)

## 9. Forbidden production defaults

| Pattern | Status |
| --- | --- |
| Empty `ETL_API_TOKEN` | Rejected by production profile / compose `:?` |
| `change-me` passwords | Rejected by production profile validation |
| `ghcr.io/...:latest` as compose default | Forbidden in production assets |
| Distributed compose with placeholders | **Beta only** until PR-D1; not standalone production evidence |

## 9b. GA assessment

Current explicit assessment: [ga-assessment-2026-09-16.md](./ga-assessment-2026-09-16.md)
(standalone may claim production ready; project-level and distributed remain beta
until the release-candidate recertification loop completes). Update this link on
each assessment.

## 10. Sign-off

| Role | Name | Date | Notes |
| --- | --- | --- | --- |
| Implementer | | | |
| Reviewer | | | |
| Release tag | | | |
