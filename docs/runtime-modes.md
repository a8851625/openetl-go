# Runtime Modes And Production Runbook

Lightweight self-hosted deployment modes for OpenETL-Go.

## Runtime modes

| Mode | Role flag | When to use |
| --- | --- | --- |
| Standalone | `--role standalone` (default) | Single binary: API + UI + worker in one process |
| Master-only | `--role master` | Control plane: API, dispatch, no local shard execution |
| Worker-only | `--role worker` | Data plane: polls master, runs shards |
| API-only / headless | standalone or master with no UI clients | Automate via REST; UI still embedded but unused |

Priority: **CLI flags > environment variables > config.yaml > built-in defaults**.

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
- `docker-compose.distributed.yml` — master + scalable workers
- `docker-compose.quickstart.yml` — demo path
- `docker-compose.dev.yml` — full local dependency harness

## Smoke checks

```sh
# Unit-level CLI validation
go test ./internal/cmd -count=1

# Runtime smoke (help, invalid role, optional binary/container health)
bash hack/e2e-runtime-smoke.sh
```

Acceptance for a release:

1. `--help` exits 0 and documents priority + core flags.
2. Invalid `--role` fails before server start.
3. Standalone/master/worker compose examples start and pass health.

## Production runbook (minimum)

### Backup / restore (SQLite)

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
