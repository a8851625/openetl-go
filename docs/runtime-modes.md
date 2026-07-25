# Runtime Modes And Production Runbook

Lightweight self-hosted deployment modes for OpenETL-Go.

## Runtime modes

| Mode | Role flag | When to use |
| --- | --- | --- |
| Standalone | `--role standalone` (default) | Single binary: API + UI + worker in one process |
| Master-only | `--role master` | Control plane: API, dispatch, no local shard execution |
| Worker-only | `--role worker` | Data plane: polls master, runs shards |
| API-only / headless | standalone or master with no UI clients | Automate via REST; UI still embedded but unused |

### Runtime profile gate

`ETL_PROFILE=development` is the compatibility default for local tests and
legacy plaintext metadata. `ETL_PROFILE=production` fails before restore or
HTTP startup unless API auth, a 32-byte spec-encryption key, valid TLS
certificate/key files, and SQL audit logging are configured. Placeholder
credentials such as `change-me` are rejected. Set `ETL_INSECURE_DEV=true`
only as an explicit, logged development bypass; it is not a production
readiness claim. The CLI equivalents are `--profile production` and
`--insecure-dev true`.

The standalone production Compose file uses required-variable interpolation
for all credentials, a required certificate directory, and a pinned image
tag/digest. The quickstart and distributed files remain demo/beta profiles and
must not be used as evidence for standalone production readiness.

The repository `.dockerignore` excludes local Go/race-test caches, build
outputs, and browser screenshots from the image context. Keep those artifacts
out of production build inputs; a large local cache must not turn a normal
image build into an opaque or effectively unbounded upload.

Priority: **CLI flags > environment variables > config.yaml > built-in defaults**.

### HTTP security boundary

The ETL API uses an explicit origin allow-list. Development keeps the legacy
`*` default; production rejects wildcard origins and defaults to same-host
access. Configure cross-origin consoles with a comma-separated
`ETL_CORS_ORIGINS` value (for example,
`https://etl-console.example,https://localhost:8443`). Requests carrying an
unlisted `Origin` are rejected with `403`, including non-preflight writes, so a
browser cannot use a simple request to bypass the preflight boundary. The
embedded UI reaches the API through the same-origin GoFrame proxy by default.

`X-Forwarded-For` and `X-Real-IP` are ignored unless the immediate TCP peer is
inside `ETL_TRUSTED_PROXY_CIDRS` (or `etl.trustedProxyCIDRs`). Leave it empty
when the API is directly exposed. The selected client IP is used for rate
limiting and audit records; an arbitrary header from an untrusted client cannot
change either identity.

API responses include `X-Content-Type-Options: nosniff`, `X-Frame-Options:
DENY`, `Referrer-Policy: no-referrer`, and a restrictive `Permissions-Policy`.
The production profile also emits HSTS. Authentication failures return a
`WWW-Authenticate: Bearer realm="openetl-etl-api"` challenge.

The web console keeps the API token in page memory only. It is never read from
or written to long-lived `localStorage`; a full reload intentionally requires
the operator to enter the token again. This is a shared-token console, not an
RBAC system: scope, rotation, and revocation remain deployment responsibilities
until a future authenticated control-plane milestone.

### TLS termination topology

The supported standalone production topology terminates TLS inside the same
OpenETL-Go process on both listeners:

- `https://<host>:8000` serves the embedded UI and proxies `/api/v2/*` plus
  `/metrics`;
- `https://<host>:8001` is the direct ETL API listener and should normally stay
  inside the deployment network;
- no clear-text HTTP listener remains on either port when
  `ETL_TLS_CERT`/`ETL_TLS_KEY` are configured.

Both listeners use the same certificate pair. The local UI-to-API reverse
proxy does not skip certificate verification: it adds the configured
certificate chain to its trust pool and validates the name supplied by
`ETL_TLS_SERVER_NAME` (or `etl.tls.serverName` / `--tls-server-name`). Set that
value to a SAN in the certificate, commonly `localhost` for a single-container
deployment. A partial certificate pair, an HTTP proxy target while TLS is
enabled, an unreadable certificate, or a name/chain verification failure is a
startup or `502` error rather than a silent downgrade.

The production Compose health check uses
`https://localhost:8000/api/v2/health`. Development without a certificate pair
continues to use HTTP on both ports. The verified smoke entry point is:

```sh
./hack/e2e-tls-topology.sh
```

## Pipeline spec encryption and restart recovery

Pipeline rows and version rows use the same storage adapter in standalone,
master, and worker roles. When `ETL_SPEC_ENCRYPTION_KEY` is configured, new
rows use AES-256-GCM with the envelope shape `enc:v1:<key-id>:<payload>`;
legacy `enc:<payload>` rows remain readable for upgrades. The API only exposes
masked spec/version YAML, while restore and worker execution decrypt through
the adapter before parsing.

```sh
export ETL_SPEC_ENCRYPTION_KEY="$(openssl rand -base64 32)"
export ETL_SPEC_ENCRYPTION_KEY_ID="primary"
```

During a key rotation, deploy the new key together with the old key in
`ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS` (JSON or comma-separated
`key-id=base64-key` entries). New writes use the new key; current and
historical rows written with the old key remain readable until the migration
has been verified. Remove the previous key only after all retained historical
versions have been re-encrypted and a restart/rollback check passes.

An encrypted row without a matching key, an authentication failure, damaged
ciphertext, or an unsupported envelope version is a startup error for the
control plane and worker. The error names the remediation environment
variable but never includes ciphertext or secret values. Plaintext rows are
accepted only for legacy/development compatibility; production fail-closed
profile checks are a separate `PR-0.3` gate.

## Connection catalog and settings secret envelope

The same `ETL_SPEC_ENCRYPTION_KEY` material also encrypts secret-bearing fields
in the connection catalog (`connections.config_json`) and secret settings such
as `llm_api_key`. Encryption is field-level: non-secret keys stay plaintext so
operators can still inspect host/topic/model metadata in SQL dumps, while
values whose keys match secret patterns (`password`, `token`, `api_key`,
`secret`, …) are stored as `enc:v1:<key-id>:<payload>`.

Control-plane behavior:

- `GET /api/v2/connections` and `GET /api/v2/settings` only return masked
  secret values (`******` for connections; historical `prefix****` for settings).
- A full-form resubmit that echoes the mask preserves the previously stored
  secret instead of overwriting it with the placeholder.
- Runtime reads (`GetConnection` / `GetSetting`) decrypt through
  `storage.SecretFieldStore` so connection tests, preflight, and LLM proxy use
  the real secret without exposing it in API responses.
- Legacy plaintext connection/settings secrets remain readable. Re-saving the
  object, or calling `SecretFieldStore.ReencryptSecrets` during a maintenance
  window, seals them with the current key.
- Wrong key, missing previous key, malformed ciphertext, and unsupported
  envelope versions fail closed. Error strings name the key configuration but
  must not include the secret or ciphertext payload.

Rotation procedure for connection/settings secrets is the same as pipeline
specs: deploy `ETL_SPEC_ENCRYPTION_KEY` + `ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS`,
re-encrypt retained secrets, restart, verify connection test / settings-backed
features, then remove the previous key only after the verification passes.
SQL dump scanners and direct queries must not observe the fixed test secrets
once encryption is enabled.

Evidence commands:

```sh
go test ./internal/etl/storage -count=1 -run 'Secret|ConfigContains|EncryptConfig|Conformance'
go test ./internal/etl/server -count=1 -run 'SecretEnvelope|ConnectionCatalogCRUD|StorageFailure'
```

Pipeline lifecycle writes use one transaction for the current row, the new
version row, and (when requested) checkpoint reset. Delete uses the same
boundary for the pipeline, retained versions, and checkpoint. The API prepares
the replacement runner first and only swaps the in-memory runner/scheduler
after that transaction commits; an injected storage failure returns non-2xx
and leaves the last successful runtime state available for restart.

### Schema migration lock and concurrent version allocation

Every SQL backend opens under `sqlstore.WithMigrationLock` so master/worker
processes cannot race DDL:

- SQLite: process-local mutex + `_migration_lock` lease row
- MySQL: `GET_LOCK('openetl_go_schema_migration', 30)` on a dedicated connection
- PostgreSQL: `pg_advisory_lock` with a stable 64-bit key

Versioned migrations (`_schema_version`) treat create/read/DDL/record as
explicit errors. A failed step does **not** insert the version row, so the next
startup retries instead of treating a half-applied schema as complete.

Pipeline version numbers are allocated under the unique `(pipeline, version)`
constraint. Concurrent `SavePipelineWithVersion` callers that observe the same
`MAX(version)` retry the whole current/version/checkpoint transaction rather
than invent a duplicate version.

Evidence commands:

```sh
go test ./internal/etl/storage/sqlstore -count=1 -run 'MigrationLock|ConcurrentPipelineVersion|ConcurrentSQLiteStoreOpen|MigrationFailure'
go test ./internal/etl/storage -count=1 -run 'TestSQLiteConformance|Atomic|Concurrent'
./hack/e2e-spec-encryption-recovery.sh
./hack/e2e-control-plane-persistence.sh
CONTAINER_CLI=podman ./hack/e2e-storage-mysql.sh
CONTAINER_CLI=podman ./hack/e2e-storage-postgres.sh
```

## Streaming / CDC scale-out (placement semantics)

Distributed dispatch is **linear-spec only** (DAG executors always run in the process that loads them). Placement rules:

| Spec shape | Standalone | Master + workers |
| --- | --- | --- |
| Streaming/CDC, `logical_shards=1` (default) | Runs **in this process** | Dispatched as **one continuous shard task** to a worker (pipeline-level placement). Still **one replica** — not multi-active HA. |
| Linear, `logical_shards > 1` | Inline `ParallelRunner` (bounded by `max_active_shards`) | One task per shard; workers claim continuous/batch shards |
| DAG | Local `DAGExecutor` | Local on master (not shard-distributed) |

### Decision tree for pure Kafka CDC (e.g. many independent topics)

1. **Default / small fleet**: `standalone` with all pipelines in one process. Validate warns that unsharded streaming is a single placement.
2. **CPU or blast-radius split (ops-only)**: multiple standalone pods, each mounting a **subset** of pipeline YAMLs, sharing MySQL metadata. No code change required.
3. **Kafka throughput scale-out**: set `parallelism.sharding.logical_shards` to **≤ topic partition count** (preflight recommends partition count). Shards share one `group_id`; excess shards idle. Under master-worker those shards are long-running worker tasks.
4. **Keep control plane light**: `role=master` places even single-shard streaming on workers so the API/UI host does not own every CDC consumer.

Not multi-active HA: losing the process (or the single worker holding the continuous task) stops that pipeline until restart/reassign + checkpoint resume. Absorb replay with upsert/PK sinks (see [etl-idempotency.md](./etl-idempotency.md)).

`POST /api/v2/specs/validate` surfaces placement warnings; Kafka preflight compares `logical_shards` to live topic partition metadata.

```sh
# Help is the executable manual
./openetl-go --help

# Standalone
./openetl-go --config ./manifest/config/config.yaml --port 8000 --etl-api-port 8001

# Master
./openetl-go --role master --storage mysql --storage-dsn 'user:pass@tcp(db:3306)/etl?parseTime=true'

# Worker
./openetl-go --role worker --master-url http://openetl-master:8001 \
  --worker-id worker-a --worker-labels zone=secure,gpu=false
```

Compose references:

- `docker-compose.yml` — production standalone (app + MySQL + Redis)
- `docker-compose.distributed.yml` — master + scalable workers (**beta / production-candidate**, PR-D1)
- `docker-compose.quickstart.yml` — demo path
- `docker-compose.dev.yml` — full local dependency harness

### Distributed maturity (PR-D1)

Distributed master/worker is a **beta / production-candidate** profile, independent of standalone production-ready claims.

Required for distributed deploys:

- Shared `ETL_API_TOKEN` on master and every worker (worker client sends `X-API-Token` + `Bearer`).
- Prefer `ETL_MASTER_URL=https://...` with verifiable TLS; `ETL_WORKER_TLS_INSECURE` is e2e-only.
- Task ownership uses `generation` CAS + `lease_expires_at`; stale owners receive HTTP 409 / `ErrTaskFenced`.
- Requeue on worker offline or lease expiry; attempts beyond `DefaultTaskMaxAttempts` become visible `failed`.

Still out of scope: multi-master consensus, cross-worker DAG, multi-active single continuous shard.

Evidence: `hack/e2e-distributed.sh`, `internal/etl/worker/transport_test.go`, `internal/etl/storage/sqlstore/task_fence_test.go`, `internal/etl/master/fence_test.go`.

## Smoke checks

```sh
# Unit-level CLI validation
go test ./internal/cmd -count=1

# Runtime smoke (help, invalid role, optional binary/container health)
bash hack/e2e-runtime-smoke.sh

# P5 production gate (profile + release assets + storage matrix)
bash hack/e2e-production-gate.sh
bash hack/check-release-assets.sh
```

Acceptance for a release:

1. `--help` exits 0 and documents priority + core flags.
2. Invalid `--role` fails before server start.
3. Standalone/master/worker compose examples start and pass health.
4. Production assets pass `hack/check-release-assets.sh` (no empty token / `change-me` / floating `latest` defaults).
5. Business health unit tests and `/api/v2/health` component checks are green.

Full ops procedures (upgrade / backup / restore / incidents): [ops-runbook.md](./ops-runbook.md).
Release sign-off: [release-checklist.md](./release-checklist.md).
Resource baselines: [resource-baseline.md](./resource-baseline.md).

## Production runbook (minimum)

### Backup / restore (SQLite)

Logical export (PR-1.3):

```go
// storage.BackupSQLStore(ctx, store, "./backup", []string{/* known plaintext to ban */})
// writes openetl-backup-<ts>/{manifest.json,*.jsonl}
// SecretScan.OK must be true before shipping a dump off-box.
```

Retention janitor helper: `storage.ApplyRetention` for aged `run_history` / `audit_logs` / `dead_letters`.

### Backup / restore (SQLite) — file copy

```sh
# Backup metadata DB while app is stopped or using a consistent copy
cp ./data/etl.db ./backup/etl.db.$(date +%Y%m%d)

# Restore
cp ./backup/etl.db.YYYYMMDD ./data/etl.db
```

MySQL/PostgreSQL: use vendor `mysqldump` / `pg_dump` on the storage DSN database. Specs under `pipes/` and plugin WASM under `data/plugins/` should be version-controlled or snapshotted separately.

### Retention

- DLQ: use `GET/DELETE /api/v2/dlq/{pipeline}` and storage TTL policies when configured.
- Audit: disable with `ETL_AUDIT_ENABLED=false` only when compliance allows.
- Finished tasks: monitor `task_assignments` growth; distributed mode reassigns stale tasks via master heartbeat.

### DLQ backlog

1. `GET /api/v2/dlq/{pipeline}?limit=100`
2. Fix sink/schema cause from `error` / `field_issues`
3. `POST /api/v2/dlq/{pipeline}/replay` or per-id replay
4. For DAG entries without `dag_node`, manual recovery is required (API returns 400)

### Worker scale-out

1. Start additional workers with unique `--worker-id` and matching `--worker-labels`
2. Ensure shared MySQL/PostgreSQL storage and Redis (if state transforms need cache)
3. Confirm `GET /api/v2/workers` shows heartbeats and free slots
4. Pipelines with `worker_selector.match_labels` stay pending until a matching worker appears

### Upgrade / rollback

1. Backup storage + `pipes/` + plugins
2. Deploy new image/binary (`make image TAG=...` or pack release)
3. Run `bash hack/e2e-runtime-smoke.sh` and a production-candidate e2e subset
4. Rollback: redeploy previous image and restore storage snapshot if schema migration fails

### Metrics to watch

- `source_read_latency_ms`, `sink_write_latency_ms`
- `checkpoint_age_seconds`, `cdc_lag_ms`
- `dlq_file_count`, `dlq_replay_count`
- worker heartbeats / free slots

## Illegal args

Invalid role/storage/port/slots fail fast:

```sh
./openetl-go --role sidecar   # error: must be standalone, master, or worker
./openetl-go --storage oracle # error: must be sqlite, mysql, or postgresql
```
