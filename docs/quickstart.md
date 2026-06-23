# ETL/CDC Quick Start

> Lightweight declarative ETL/CDC data sync platform. Define pipelines in YAML. 20+ connectors including MySQL CDC, Kafka, ClickHouse, PostgreSQL, Doris, Elasticsearch, S3.

---

## 1. Environment Setup (5 minutes)

### One-Click Docker Compose

```bash
# Clone the repo
git clone <repo-url> openetl-go
cd openetl-go

# Start all dependencies (MySQL, ClickHouse, MinIO, Redpanda) + ETL service
podman-compose -f docker-compose.dev.yml up -d

# Verify
curl http://localhost:8000/api/v2/health
# → {"status":"ok","storage":"ok",...}
```

### Configure API Token (Required for Production)

```bash
# Generate a random token
export ETL_API_TOKEN=$(openssl rand -hex 16)

# (Optional) Generate spec encryption key
export ETL_SPEC_ENCRYPTION_KEY=$(openssl rand -base64 32)

# Restart the service
podman-compose -f docker-compose.dev.yml restart openetl-go
```

---

## 2. Create Your First Pipeline (3 minutes)

### Option 1: Web UI Designer

Visit `http://localhost:8000/#/designer` and drag-and-drop to build pipelines visually.

### Option 2: YAML Declaration

Create `pipes/my-first-pipeline.yaml`:

```yaml
name: mysql-to-clickhouse

source:
  type: mysql_cdc
  config:
    host: etl-mysql-source
    port: 3306
    user: sync_user
    password: sync_password_123
    database: dzh3136_go
    tables: ["orders"]

transforms:
  - type: add_field
    config:
      field: _synced_at
      value: "now()"

sink:
  type: clickhouse
  config:
    host: etl-clickhouse
    port: 9000
    user: default
    password: dzh123456
    database: dzh3136_go
    auto_create: true
    schema_drift: add_columns

batch_size: 1000
flush_interval_ms: 1000
checkpoint_interval_sec: 30
```

Pipeline files are **hot-reloaded** (no restart needed). They appear at `http://localhost:8000/#/pipelines` within seconds.

### Option 3: API Creation

```bash
curl -X POST http://localhost:8000/api/v2/pipelines \
  -H "Content-Type: application/json" \
  -H "X-API-Token: $ETL_API_TOKEN" \
  -d '{"spec": {"name": "my-pipeline", "source": {"type": "file", "config": {"path": "/tmp/in.jsonl"}}, "sink": {"type": "file_sink", "config": {"output_dir": "/tmp"}}}}'
```

---

## 3. Pipeline Management

```bash
# List all pipelines
curl -H "X-API-Token: $TOKEN" http://localhost:8000/api/v2/pipelines

# Start / Stop / Pause / Resume
curl -X POST -H "X-API-Token: $TOKEN" http://localhost:8000/api/v2/pipelines/my-pipeline/start
curl -X POST -H "X-API-Token: $TOKEN" http://localhost:8000/api/v2/pipelines/my-pipeline/stop
curl -X POST -H "X-API-Token: $TOKEN" http://localhost:8000/api/v2/pipelines/my-pipeline/pause
curl -X POST -H "X-API-Token: $TOKEN" http://localhost:8000/api/v2/pipelines/my-pipeline/resume

# View metrics
curl -H "X-API-Token: $TOKEN" http://localhost:8000/api/v2/metrics

# Prometheus format
curl http://localhost:8000/metrics
```

---

## 4. Connector Reference

| Category | Connector | Description |
|----------|-----------|-------------|
| **Source** | `mysql_cdc` | MySQL binlog CDC (supports GTID) |
| | `mysql_snapshot_cdc` | MySQL full snapshot + incremental CDC handoff |
| | `mysql_batch` | MySQL batch read (supports custom SQL queries) |
| | `postgres_cdc` | PostgreSQL logical replication (pgoutput) |
| | `kafka` | Kafka consumer group |
| | `redis` | Redis SCAN full read |
| | `file` | File read (JSONL/CSV) |
| | `http` | HTTP API paginated polling |
| **Sink** | `clickhouse` | ClickHouse (auto-create / schema drift / DDL translation) |
| | `mysql` | MySQL (INSERT / UPSERT / DELETE, auto-create) |
| | `postgres` | PostgreSQL (INSERT / UPSERT, auto-create) |
| | `kafka` | Kafka producer (idempotent) |
| | `elasticsearch` | ES bulk indexing (multi-host round-robin, 429 retry) |
| | `doris` | Doris (Stream Load + MySQL protocol DELETE) |
| | `redis` | Redis (HASH/STRING/LIST modes) |
| | `s3` | S3/MinIO (Parquet/JSON, multipart upload) |
| | `jdbc` | Any JDBC database |
| | `file_sink` | Local file output |
| **Transform** | `filter`, `rename`, `add_field`, `drop_field`, `type_convert` | Basic transforms |
| | `deduplicate`, `validate` | Data cleansing |
| | `lua` | Lua scripting (inline, gopher-lua pure Go) |
| | `join`, `window` | Stream-stream JOIN / windowed aggregation |
| | `router`, `fanout`, `tap` | Conditional routing / fan-out / tap |
| | `enricher`, `lookup` | Data enrichment / dimension lookup |
| | `rate_limiter` | Rate limiting |

---

## 5. Key Configuration

| Config | Default | Description |
|--------|---------|-------------|
| `batch_size` | 1000 | Max records per batch |
| `flush_interval_ms` | 1000 | Batch flush interval (ms) |
| `checkpoint_interval_sec` | 30 | Checkpoint save interval (seconds) |
| `backpressure_buffer` | 100 | Source↔Sink channel buffer size |
| `parallelism.count` | 1 | Number of parallel shard instances |
| `parallelism.shard_strategy` | round_robin | Shard strategy |
| `retry.max_attempts` | 3 | Max retry attempts |
| `retry.initial_interval_ms` | 1000 | Initial retry interval |
| `retry.max_interval_ms` | 30000 | Max retry interval |
| `dlq.enable` | true | Enable dead-letter queue |

---

## 6. Production Deployment Checklist

- [ ] Set `ETL_API_TOKEN` environment variable
- [ ] Set `ETL_SPEC_ENCRYPTION_KEY` to encrypt specs at rest
- [ ] Configure TLS (`ETL_TLS_CERT`, `ETL_TLS_KEY`)
- [ ] Configure alert channels (`ALERT_DINGTALK_WEBHOOK` / `ALERT_FEISHU_WEBHOOK` / `ALERT_SLACK_WEBHOOK`)
- [ ] Set DLQ TTL (`ETL_DLQ_TTL=168h`)
- [ ] Verify all CDC pipelines use idempotent sinks (UPSERT mode)
- [ ] Grant replication privileges to database users (`REPLICATION SLAVE`, `REPLICATION CLIENT`)
- [ ] Configure MySQL `binlog_format=ROW` + `binlog_row_image=FULL`
- [ ] Configure PostgreSQL `wal_level=logical`
- [ ] Set resource limits (CPU/memory via Docker or systemd)

---

## 7. FAQ

### Q: Pipeline creation fails with "unsafe pipeline"?
CDC source + non-idempotent sink (file_sink/s3) is blocked by default. Use MySQL/ClickHouse/Doris UPSERT mode, or explicitly set `allow_unsafe: true` in the spec.

### Q: How to backfill data from a specific point in time?
```yaml
source:
  type: mysql_cdc
  config:
    start_from: "2026-06-01T00:00:00Z"  # RFC3339 timestamp
    # Or specify binlog position:
    # start_from: "binlog:mysql-bin.000003:12345"
    # Or specify GTID:
    # start_from: "gtid:3E11FA47-...:1-100"
```

### Q: How to pause a pipeline without losing data?
Use `pause` (not `stop`). Pause halts source reading but preserves the checkpoint; `resume` continues from the same position.

### Q: How to view and replay DLQ records?
The DLQ page shows each record's full JSON data via "▼ Data" expand. Filter and replay:
```bash
# Replay by error message
curl -X POST -H "X-API-Token: $TOKEN" \
  'http://localhost:8000/api/v2/dlq/my-pipeline/replay?error_contains=Duplicate'

# Replay by time range
curl -X POST -H "X-API-Token: $TOKEN" \
  'http://localhost:8000/api/v2/dlq/my-pipeline/replay?from=2026-06-01T00:00:00Z'
```

### Q: Pipeline starts but produces no data?
Run a preflight check:
```bash
curl -X POST -H "X-API-Token: $TOKEN" \
  -H "Content-Type: application/json" \
  -d @pipes/my-pipeline.yaml \
  http://localhost:8000/api/v2/specs/validate
```
Common causes: wrong binlog format, missing replication grants, source table doesn't exist, network issues.

### Q: How to monitor pipeline status?
- **Web UI**: Dashboard page shows real-time metrics
- **Prometheus**: `curl http://localhost:8000/metrics`
- **API**: `curl -H "X-API-Token: $TOKEN" http://localhost:8000/api/v2/metrics`
- **Logs**: Set `LOGGER_FORMAT=json` for structured JSON logging

### Q: ClickHouse auto-create produces wrong column types?
Auto-create infers types by sampling data values. For precise type mapping, create the table manually in ClickHouse first, then set `auto_create: false`.

### Q: How to configure distributed deployment?
```yaml
# Master node
etl:
  role: master
  storage:
    type: mysql
    # ... MySQL connection config

# Worker node
etl:
  role: worker
  storage:
    type: mysql
    # ... same MySQL connection config
```
Master schedules tasks; workers execute shards. Crashed worker shards are automatically reassigned.

---

## 8. Example Pipelines

The `pipes/` directory contains complete examples:
- `file-to-file.yaml` — Minimal file→file
- `mysql-batch-to-mysql.yaml` — MySQL batch sync
- `mysql-cdc-to-clickhouse.yaml` — MySQL CDC→ClickHouse (auto-create)
- `order-realtime-analytics.yaml` — Window aggregation + JOIN real-time analytics
- `ultimate-complex-demo.yaml` — DAG multi-source multi-sink complex scenario

---

## 9. Getting Help

- **GitHub Issues**: Bug reports / feature requests
- **API Docs**: `/api/v2/docs` (Swagger UI)
- **Example Pipelines**: `pipes/` directory
- **Config Reference**: [`docs/etl-config-schema.md`](./etl-config-schema.md)
- **API Reference**: [`docs/etl-api.md`](./etl-api.md)
