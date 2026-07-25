# Operations Runbook (Upgrade / Backup / Restore / Health)

Audience: a single maintainer without a dedicated SRE team.
Default semantics: **checkpointed at-least-once** (not cross-sink exactly-once).

Related: [runtime-modes.md](./runtime-modes.md), [release-checklist.md](./release-checklist.md),
[resource-baseline.md](./resource-baseline.md), [path-contract.md](./path-contract.md).

## 0. Daily health check

```bash
export ETL_API_TOKEN=...
curl -fsS -H "X-API-Token: $ETL_API_TOKEN" https://localhost:8000/api/v2/health | jq .
curl -fsS https://localhost:8000/metrics | head
```

Interpret `/api/v2/health`:

| Field | Meaning |
| --- | --- |
| `status=ok` | Storage up; no failed/degraded pipelines; redis (if configured) reachable |
| `status=degraded` | Process up; lag / checkpoint stale / DLQ / worker partial / alert drops |
| `status=unhealthy` | Storage down, all workers offline (master), or failed pipelines |
| HTTP 503 | Non-ok status (compose/k8s probes fail → investigate) |

Components: `storage`, `redis_state`, `scheduler`, `workers`, `alert_queue`,
`pipeline_<name>` (derived health).

Thresholds: `ETL_HEALTH_CHECKPOINT_STALE_SEC` (300), `ETL_HEALTH_CDC_LAG_MS` (60000),
`ETL_HEALTH_WORKER_STALE_SEC` (30).

## 1. Backup

### 1.1 Logical control-plane backup (all SQL backends)

Programmatic (Go / maintenance job):

```go
man, err := storage.BackupSQLStore(ctx, store, "./backup", []string{
    // known plaintext secrets that must never appear after encryption
})
// man.SecretScan.OK must be true before shipping off-box
```

Artifacts: `openetl-backup-<ts>/{manifest.json,*.jsonl}` covering pipelines,
versions, checkpoints, DLQ, audit, runs, workers, plugins, connections, settings.

Evidence scripts:

```bash
./hack/e2e-backup-restore-sqlite.sh
# with env:
./hack/e2e-backup-restore-mysql.sh
./hack/e2e-backup-restore-postgres.sh
```

### 1.2 Physical / vendor dump

SQLite (stop writer or use consistent copy):

```bash
cp ./data/etl.db ./backup/etl.db.$(date +%Y%m%d%H%M)
```

MySQL (compose example):

```bash
docker exec openetl-mysql \
  mysqldump -u root -p"$MYSQL_ROOT_PASSWORD" --single-transaction openetl \
  > backup-openetl-$(date +%Y%m%d%H%M).sql
```

PostgreSQL:

```bash
pg_dump --format=custom --file=backup-openetl.dump "$ETL_STORAGE_DSN"
```

Also snapshot:

- `pipes/` (or DB-only specs if YAML is import-only)
- `data/plugins/` WASM artifacts
- TLS certs / `.env` (offline secret store — never commit)

### 1.3 Retention / janitor

```go
rep, err := storage.ApplyRetention(ctx, db, time.Now().UTC(), storage.RetentionPolicy{
    AuditLogs:  30 * 24 * time.Hour,
    RunHistory: 14 * 24 * time.Hour,
    // dead letters also covered by ETL_DLQ_TTL when configured
})
```

DLQ TTL: `ETL_DLQ_TTL` (default production `168h`). Monitor `dlq_file_count` /
`records_dlq` and alert queue drops (`etl_alert_dropped_total`).

## 2. Restore

1. Stop OpenETL process/containers (`docker compose stop openetl-go`).
2. Restore metadata DB (copy file / `mysql < dump` / `pg_restore`).
3. Restore `data/` volume if checkpoints/DLQ files are file-backed.
4. Ensure `ETL_SPEC_ENCRYPTION_KEY` (+ previous keys if rotating) matches backup era.
5. Start process; confirm:

```bash
curl -fsS -H "X-API-Token: $ETL_API_TOKEN" .../api/v2/health
curl -fsS -H "X-API-Token: $ETL_API_TOKEN" .../api/v2/pipelines
# reconcile: pipeline count, sample checkpoint age, DLQ backlog
```

6. Start critical pipelines; watch lag / DLQ for one checkpoint interval.

**Success criteria:** object counts match backup manifest; critical pipeline
checkpoints resume without silent skip; secrets remain encrypted/masked in API.

## 3. Upgrade

1. Record current `OPENETL_IMAGE` / binary version and take a fresh backup.
2. Read CHANGELOG for migration notes.
3. Pin new image tag/digest in compose `.env` (`OPENETL_IMAGE=ghcr.io/...:vX.Y.Z`).
4. `docker compose pull && docker compose up -d` (or replace binary + restart).
5. Schema migrations run under backend locks (SQLite lease / MySQL `GET_LOCK` /
   PG advisory). Failed migration does **not** stamp schema version — fix and retry.
6. Smoke:

```bash
./hack/e2e-production-profile.sh   # config gate
deploy/production/scripts/smoke.sh
curl .../api/v2/health
```

7. Spot-check production paths (CDC lag, upsert sink, DLQ empty).

### Rollback

1. Stop app.
2. If schema advanced and is incompatible: restore DB dump from step 1.
3. Set `OPENETL_IMAGE` back to previous pin; `up -d`.
4. Verify health + pipeline resume.

RPO: last successful checkpoint (typically ≤ checkpoint interval, default 30s for many specs).
RTO: container restart + restore time (target < 15 minutes for single-node metadata restore).

## 4. Common incidents

### DLQ backlog

1. `GET /api/v2/dlq/{pipeline}?limit=100`
2. Fix sink/schema root cause
3. `POST /api/v2/dlq/{pipeline}/replay`
4. Confirm `records_dlq` / `dlq_file_count` decline

### Checkpoint stale / CDC lag

1. Check sink latency metrics and circuit breaker state
2. Check source connectivity / binlog / Kafka consumer group
3. Pause → fix → resume (checkpoint retained)
4. Only reset checkpoint when intentional full reprocess is approved

### Worker offline (master role)

1. `GET /api/v2/workers`
2. Restart worker; confirm heartbeat < 30s
3. Stale tasks reassigned by master loop

### Alert drops

`etl_alert_dropped_total` > 0 or health `alert_queue: degraded` means webhook
backpressure. Fix channel latency / reduce noise; events were not delivered.

## 5. Production profile reminders

- `ETL_PROFILE=production` fails closed without token, 32-byte encryption key, TLS, audit
- Compose requires pinned `OPENETL_IMAGE` and secrets via `:?` interpolation
- Never use `change-me` or `:latest` as production defaults
- Distributed compose remains **beta** until PR-D1

## 6. Evidence commands (copy into release notes)

```bash
go test -race -count=1 ./internal/etl/telemetry ./internal/etl/alert ./internal/etl/storage
./hack/check-release-assets.sh
./hack/e2e-production-profile.sh
./hack/e2e-backup-restore-sqlite.sh
./hack/e2e-production-gate.sh
```
