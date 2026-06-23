# OpenETL-Go — Lightweight ETL/CDC Engine (Single Binary)

> **OpenETL-Go** is a single-binary, plugin-based ETL/CDC engine: `Source → Transform → Sink`.
> Sources: MySQL binlog / PostgreSQL logical replication / Kafka / File / HTTP / Redis.
> Sinks: ClickHouse / MySQL / PostgreSQL / Doris / Elasticsearch / Kafka / Redis / S3 / JDBC.
> Standalone mode uses SQLite (zero external dependencies); scale-out uses MySQL/PostgreSQL shared storage + master-worker sharding.

## Capability Matrix

| Stage | Connectors / Operators |
|-------|----------------------|
| **Source** | `mysql_cdc` (binlog), `mysql_snapshot_cdc` (snapshot+CDC handoff), `postgres_cdc` (logical replication), `mysql_batch`, `kafka`, `redis` (SCAN), `http` (paginated+checkpoint), `file` (JSON/CSV) |
| **Transform** | `filter` (expression engine), `deduplicate`, `validate` (8 rule types), `rename`/`drop_field`/`add_field`, `type_convert`, `enricher`, `lookup`, `join`, `window`, `router` (conditional routing), `fanout`, `tap`, `rate_limiter`; scripting: `lua` (default, gopher-lua), `javascript`/`typescript` (QuickJS, CGO), WASM plugins (extism) |
| **Sink** | `clickhouse` (auto-create + DDL translation), `mysql`/`postgres` (batch + idempotent upsert), `doris` (Stream Load), `kafka` (idempotent producer), `elasticsearch` (bulk API, round-robin), `redis` (HASH/STRING/LIST), `s3`/minio (multipart, Parquet), `jdbc` (any JDBC DB), `file` |
| **Reliability** | at-least-once + idempotent sinks + DLQ dead-letter queue + 3-state circuit breaker + exponential backoff retry + checkpoint; **zero silent data loss** (SPEC §6.1) |
| **Execution Modes** | Linear pipeline / DAG multi-source multi-sink / ParallelRunner sharding / master-worker distributed (A11-redo verified) |
| **Operations** | REST API `/api/v2/*`, preflight checks, Prometheus `/metrics`, JSON structured logging, SQLite/MySQL/PostgreSQL metastore, Web management UI |

## 🚀 5-Minute Quickstart

```bash
# 1. Start dependencies (MySQL source + ClickHouse target)
podman compose -f docker-compose.quickstart.yml up -d

# 2. Example pipeline MySQL CDC → ClickHouse (auto-create) is in pipes-quickstart/
#    You can also use: pipes/mysql-cdc-to-clickhouse.yaml

# 3. Open the management UI: http://localhost:8000   (REST API: /api/v2/*)
```

- **Example specs**: `pipes-quickstart/mysql-to-clickhouse.yaml`, `pipes/mysql-cdc-to-clickhouse.yaml`
- **Full walkthrough**: [`docs/quickstart.md`](./docs/quickstart.md)

## Installation

### Download Release (Recommended)

Go to [Releases](../../releases) and download the archive for your platform (Linux/macOS/Windows × amd64/arm64), then:

```bash
tar -xzf openetl-go_*.tar.gz
./openetl-go                           # reads manifest/config/config.yaml by default
```

### Docker

```bash
docker run -d --name openetl-go -p 8000:8000 -p 8001:8001 \
  -v "$PWD/pipes:/app/pipes" \
  ghcr.io/a8851625/openetl-go:latest
```

### Build from Source

```bash
go build -o openetl-go .                # Default build (pure Go, includes Lua)
# Optional runtimes:
go build -tags=extism -o openetl-go .   # Enable WASM plugins (wazero, pure Go)
go build -tags=nolua -o openetl-go .    # Strip Lua runtime for a smaller binary
CGO_ENABLED=1 go build -o openetl-go .  # Enable JS/TS transforms (QuickJS, requires CGO)
```

## Build Tags

| Tag | Effect | Default |
|-----|--------|---------|
| *(none)* | Pure Go core + all sinks/sources + Lua (gopher-lua); no WASM, no QuickJS, no CGO | ✅ |
| `-tags=extism` | + WASM plugin runtime (wazero, pure Go) | — |
| `-tags=nolua` | Strip Lua runtime (`type:lua` returns a clear error); smaller binary | — |
| `CGO_ENABLED=1` | + JavaScript/TypeScript transform (QuickJS, requires C toolchain) | — |

## Running Modes

- **Standalone (default)**: SQLite storage, zero external dependencies, one process runs all pipelines.
- **Scalable**: Switch `etl.storage.type` to `mysql` or `postgresql`, then use `etl.role=master` + multiple `etl.role=worker` for true distributed sharding (linear specs distributed across workers with no overlap; worker crash triggers shard reassignment).

Minimal config at [`manifest/config/config.yaml`](./manifest/config/config.yaml); full field reference at [`docs/etl-config-schema.md`](./docs/etl-config-schema.md).

## Architecture

### Pipeline Model

```
┌──────────┐     ┌──────────────────┐     ┌──────────┐
│  Source   │ ──► │  Transform Chain  │ ──► │   Sink   │
│ (read)    │     │ (filter/enrich/…) │     │ (write)  │
└──────────┘     └──────────────────┘     └──────────┘
       │                                        │
       └──────────── checkpoint ◄───────────────┘
                         │
                    DLQ (failed records)
```

### Execution Modes

| Mode | Use Case | Config |
|------|----------|--------|
| **Linear Pipeline** | Single source → transforms → single sink | `spec.source` + `spec.sink` |
| **DAG** | Multi-source, multi-sink, conditional edges | `spec.dag.nodes` + `spec.dag.edges` |
| **ParallelRunner** | Single source, N parallel shards writing independently | `parallelism.count: N` |
| **Master-Worker** | Distributed across multiple processes/nodes | `etl.role: master\|worker` |

### Storage Backend

| Backend | Use Case | Config |
|---------|----------|--------|
| **SQLite** | Standalone, zero-dependency deployments | `etl.storage.type: sqlite` (default) |
| **MySQL** | Multi-node shared state | `etl.storage.type: mysql` |
| **PostgreSQL** | Multi-node shared state | `etl.storage.type: postgresql` |

### Reliability Stack

1. **At-least-once delivery**: Checkpoints advance only after sink write succeeds
2. **Idempotent sinks**: MySQL/PostgreSQL upsert, ClickHouse ReplacingMergeTree, ES document `_id`
3. **DLQ (Dead Letter Queue)**: Failed records persisted with error classification; list/replay/delete via API
4. **Circuit Breaker**: Per-sink 3-state breaker (closed→open→half-open) prevents cascading failures
5. **Exponential Backoff**: `retry.Do` with configurable initial/max intervals on transient errors
6. **Panic Recovery**: Per-goroutine recovery in readLoop/writeLoop; panics route to DLQ

## Security

### API Authentication
```bash
# Enable token auth (required for production)
export ETL_API_TOKEN=$(openssl rand -hex 32)

# Clients pass token via header
curl -H "X-API-Token: $ETL_API_TOKEN" http://localhost:8000/api/v2/pipelines
curl -H "Authorization: Bearer $ETL_API_TOKEN" http://localhost:8000/api/v2/pipelines
```

### Spec Encryption
```bash
# Encrypt pipeline specs at rest in the database
export ETL_SPEC_ENCRYPTION_KEY=$(openssl rand -base64 32)
```

### TLS
```bash
# Enable TLS on the API server
export ETL_TLS_CERT=/path/to/cert.pem
export ETL_TLS_KEY=/path/to/key.pem
```

### Alerting
```bash
# Configure alert channels for DLQ overflow / breaker trips
export ALERT_DINGTALK_WEBHOOK=https://oapi.dingtalk.com/robot/send?access_token=...
export ALERT_FEISHU_WEBHOOK=https://open.feishu.cn/open-apis/bot/v2/hook/...
export ALERT_SLACK_WEBHOOK=https://hooks.slack.com/services/...
```

## Documentation

- [`docs/quickstart.md`](./docs/quickstart.md) (EN) | [`docs/quickstart.zh.md`](./docs/quickstart.zh.md) (中文) — 5-minute walkthrough
- [`docs/etl-api.md`](./docs/etl-api.md) (EN) | [`docs/etl-api.zh.md`](./docs/etl-api.zh.md) (中文) — REST API reference
- [`docs/etl-config-schema.md`](./docs/etl-config-schema.md) (EN) | [`docs/etl-config-schema.zh.md`](./docs/etl-config-schema.zh.md) (中文) — Config field reference
- [`docs/etl-idempotency.md`](./docs/etl-idempotency.md) (EN) | [`docs/etl-idempotency.zh.md`](./docs/etl-idempotency.zh.md) (中文) — Idempotency & exactly-once semantics
- [`docs/parallelism-and-batching.md`](./docs/parallelism-and-batching.md) (EN) | [`docs/parallelism-and-batching.zh.md`](./docs/parallelism-and-batching.zh.md) (中文) — Parallelism & batching
- [`SPEC.md`](./SPEC.md) — Architecture & production-readiness standard
- [`CHANGELOG.md`](./CHANGELOG.md) (EN) | [`CHANGELOG.zh.md`](./CHANGELOG.zh.md) (中文) — Release notes

## Contributing

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) (EN) | [`CONTRIBUTING.zh.md`](./CONTRIBUTING.zh.md) (中文). Please read SPEC's code style and testing conventions (§3-§5) before making changes; tests run with `-race` by default.

## License

MIT, see [`LICENSE`](./LICENSE).
