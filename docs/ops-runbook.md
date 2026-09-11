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

### 1.1 Portable control-plane backup (all SQL backends)

Stop every OpenETL process that writes this metadata database and its Redis state,
including workers and scheduled jobs. Keep them stopped until both artifacts below
are complete. The maintenance command does not start HTTP, pipelines, workers or
retention jobs, but cannot stop another process for you. Online consistent snapshots
across SQL, Redis and plugin files are not provided.

```bash
mkdir -p /backup/openetl
openetl-go --config /etc/openetl/config.yaml --backup-file /backup/openetl/control-plane.json
```

Configuration uses the normal CLI > environment > file priority. In particular,
`--storage postgresql` works with `ETL_STORAGE_DSN`; `--plugins-dir` / `ETL_PLUGINS_DIR`
select the destination artifact directory for restore. No encryption key is needed
for ciphertext passthrough, but the original key and any previous rotation keys are
required before the restored runtime starts.

The portable JSON v2 contains pipelines, all retained versions, checkpoints, DLQ,
audit, run history, workers, tasks, plugin metadata plus WASM bytes, connections and
settings. Stored credential envelopes remain unchanged. The command checks stored
connections/settings using the runtime descriptor policy and refuses export if
known secret fields still contain plaintext. Use section 1.1b to migrate them, then
scan the resulting artifact. Export also fails when an installed plugin's recorded
artifact is unreadable.

The command uses `backup.ExportFile` from `internal/etl/storage/backup`: all tables
are read in primary-key order in pages of 1,000 rows and streamed to a private
temporary file. This includes DLQ/version history whose pipeline was deleted and
retained completed/failed tasks. Counts must match both the initial and final SQL
inventory. A read, encoding, count or write failure aborts publication and preserves
the previous backup. Plugin WASM bytes are also streamed. JSON integer business
keys retain their precision, including values larger than `2^53`.

Library callers can use `ExportJSON` for an output stream; on error its output is
incomplete and must be discarded. `Export` still returns an in-memory `Snapshot`
for small callers. `ReadFile` and restore also load the full snapshot: allow memory
for the complete decoded data even though export has bounded memory. Library callers must pass a descriptor-configured
`SecretFieldStore` or `Options.SecretFieldResolver` to obtain the same secret
preflight; otherwise only legacy field-name matching is available.
`storage.BackupSQLStore` produces a separate
`manifest.json` + table JSONL diagnostic export; those files are **not** input to
`--restore-file`. A scanner result from one format does not certify the other.

The large-data drill is `CONTAINER_CLI=podman bash hack/e2e-backup-volume.sh
sqlite` (also `mysql` / `postgres`; use the installed container CLI). It creates
isolated data with 100,037 rows in **each** of DLQ/audit/run history and 2,003 retained
tasks, verifies every history field, and compares independent SQL hashes before and
after restore. It records process peak RSS for 10,003 versus 100,037 rows per table.
The [2026-09-08 evidence](./evidence/it3-backup-volume-20260908/manifest.json) reports
roughly 51–61 MiB export peak RSS and 710–724 MiB restore peak RSS for a 140–142 MiB
JSON fixture on the recorded macOS host. These are maintenance fixture measurements;
production capacity and streaming pipeline throughput are measured separately.

### 1.1a Coordinated Redis state backup

Redis transform state is separate from the portable control-plane JSON. While all
writers remain stopped, capture the same deployment's Redis instance:

```bash
# Use REDISCLI_AUTH for password authentication; add --tls if required.
redis-cli -h "$REDIS_HOST" -p "${REDIS_PORT:-6379}" --rdb /backup/openetl/state.rdb
```

Keep `control-plane.json`, `state.rdb`, the image/version pin, the Redis DB number
and key prefix, and the plugin/runtime configuration together. Redis RDB covers all
logical databases and retains absolute expiry times. Preserve the encryption keys
separately in the deployment's secret store. Existing `plugin_state` or
`etl_state_entries` SQL tables and file-backed state are outside this portable
format; use a physical/vendor database backup and the associated data volume when
using those legacy local state stores. Restore never deletes these unexported tables.

For restore, load `state.rdb` as `dump.rdb` into a **fresh, stopped Redis data volume**
and start that Redis instance before starting any OpenETL writer. Do not mix it with
an existing AOF (AOF takes precedence over RDB). Point OpenETL at the restored
instance with the original DB number and key prefix, then restore SQL as in section 2.

Lookup caches may be rebuilt from an authoritative dimension source if that recovery
choice is explicit. Deduplication and unfinished aggregate/window state cannot be
recreated merely by restarting at an already advanced checkpoint. Without matching
state, keep the pipelines stopped and plan source replay/reset plus sink duplicate
absorption. Do not label that recovery lossless or atomic across SQL and Redis.

Evidence scripts use their own temporary resources and never borrow ambient DSNs:

```bash
bash ./hack/e2e-backup-restore-sqlite.sh
bash ./hack/e2e-backup-restore-mysql.sh
bash ./hack/e2e-backup-restore-postgres.sh
bash ./hack/e2e-backup-restore-state.sh
```

The state drill closes all fixture writers, exports metadata plus Redis RDB, loads a
new Redis instance, then compares checkpoint generation/offset and the real
`RedisStore` values, key indexes and TTL for lookup/deduplicate/window state. It
certifies the offline RDB procedure, not an online distributed snapshot protocol.

### 1.1b Existing plaintext secrets

On startup, OpenETL performs a read-only scan of stored connection/settings secret
fields. Findings produce a warning and a degraded `secret_encryption` health
component. Fields use connector descriptors; undeclared fields use legacy
name matching, with one warning per kind/type/field. Logs and detection reports
contain identifiers/field names, never the credential values.

Stop all writers and keep a protected physical backup before migration. Check the
same config/backend that the runtime uses:

```bash
openetl-go --config /etc/openetl/config.yaml --check-secrets
# Set ETL_SPEC_ENCRYPTION_KEY, ETL_SPEC_ENCRYPTION_KEY_ID and any
# ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS through the deployment's secret store.
openetl-go --config /etc/openetl/config.yaml --remediate-secrets
openetl-go --config /etc/openetl/config.yaml --check-secrets
```

The check exits nonzero for remaining plaintext or a failed scan. Remediation
requires an encryption key and rewrites only rows with plaintext findings. Existing
envelopes stay unchanged, so retain their previous keys. Interrupted migration can
be rerun; a successful repeat does not re-encrypt already sealed fields. These
maintenance commands exit without starting HTTP, pipelines or retention jobs.

Scan actual exports with a private file of known plaintext test values, one value
per line. Use a plain SQL dump for this scan; a compressed/custom-format dump must
first be rendered as SQL using the matching vendor utility.

```bash
bash hack/check-plaintext-secrets.sh /backup/openetl/control-plane.json \
  --needles-file /secure/known-secret-values.txt
bash hack/check-plaintext-secrets.sh /backup/openetl/control-plane.sql \
  --needles-file /secure/known-secret-values.txt
```

The scanner exits 0 for clean, 1 for a known plaintext match, and 2 for invalid input
or a read failure. It handles JSON/SQL escaping and never prints matching values.
It is a guard for supplied values, not a detector of every possible secret in
arbitrary payloads. CI runs `hack/e2e-secret-artifacts.sh` for all three backends:
actual legacy SQL must fail, migrated portable/JSONL/SQL products must pass, and a
deliberately leaked value in an actual generated portable backup must fail.

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

1. Stop **all** OpenETL writers that share the metadata DB and Redis; save a rollback
   copy of the destination before replacing it.
2. Prepare matching Redis/file state as in section 1.1a, and the backup-era encryption
   key plus previous keys if rotating.
3. Use the portable maintenance command (or the matching physical/vendor restore
   procedure for a physical backup):

```bash
openetl-go --config /etc/openetl/config.yaml --restore-file /backup/openetl/control-plane.json
```

The command replaces covered SQL rows in one transaction and exits. IDs, version
numbers, lifecycle/generation, run timestamps/status/statistics, checkpoint positions
and DLQ identity fields are preserved. Timestamp instants retain the backend's native
precision (MySQL metadata uses milliseconds; PostgreSQL may display another timezone).
The command prints a per-table count reconciliation and returns nonzero on failure.
Review historical versions, checkpoint position and representative payloads as well
as counts before restarting.

Plugin files are fsynced into a fresh private `plugins/.restore-*` directory before
SQL commits their new absolute paths. The runtime loads those recorded paths; do not
move the generation directory afterward. Restore does not overwrite old WASM files.
A failed SQL restore rolls back its rows and leaves the previous files usable. An
error during COMMIT can have an unknown outcome: staged directories are deliberately
retained. Inspect the `plugins.wasm_path` database references before retry/cleanup;
remove only generations that no row references, while writers remain stopped.

Legacy v1/unversioned JSON is accepted. Because v1 had no bundled WASM bytes, first
supply each original `<name>.wasm` in `--plugins-dir` (or retain the recorded original
path). Missing bytes abort restore before SQL mutation. Unknown format versions,
invalid JSON and nontransactional storage implementations fail closed.

4. Start the runtime only after metadata, artifacts and Redis state agree. Check
   health, pipeline desired/observed state, retained DLQ, checkpoint and source lag:

```bash
curl -fsS -H "X-API-Token: $ETL_API_TOKEN" .../api/v2/health
curl -fsS -H "X-API-Token: $ETL_API_TOKEN" .../api/v2/pipelines
```

5. Observe critical pipelines for at least one checkpoint interval. Source retention
   must still cover the restored position; at-least-once replay requires the path's
   documented business-key/version/upsert or duplicate-absorption strategy.

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

RPO depends on the latest matching metadata/state backup and source retention; an older restored checkpoint can replay only data the source still retains.
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
