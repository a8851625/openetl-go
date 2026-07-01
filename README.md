# OpenETL-Go

A lightweight, self-hosted, open-source CDC/ETL runtime for data
synchronization, cleansing, and aggregation.

[中文 README](./README.zh.md)

OpenETL-Go runs `Source -> Transform -> Sink` pipelines from one binary, with
YAML, API, and Web UI operating the same spec. It can stay simple for file or
database sync jobs, then grow into DAG orchestration, parallel execution, and
master-worker distributed processing when a pipeline needs multiple sources,
branches, lookup/join, tumbling windows, or sinks.

The project does not keep a separate wide-table product area. Denormalized
detail tables and real-time aggregate tables are expressed as normal pipeline or
DAG specs using sources, transforms, state, and sinks.

OpenETL-Go is not trying to replace Flink/Spark for complex stateful stream
processing, Airflow/Dagster for general workflow orchestration, Airbyte for
SaaS-first ELT catalogs, or Debezium/Kafka Connect for CDC infrastructure. It is
better suited for replacing hand-written sync jobs, lightweight
DataX/Canal/Kafka consumer programs, and self-hosted CDC/ETL pipelines where a
heavier platform would be unnecessary. See [product positioning](./docs/positioning.md).

## What It Does

| Area | Capability |
| --- | --- |
| Pipeline orchestration | Linear pipelines, DAG nodes/edges, conditional routing, fanout, parallel shards, scheduled/streaming execution |
| Data movement | CDC, batch, stream, file, HTTP, Redis, object storage, warehouse and search/index sinks |
| Data shaping | Filter, validate, type conversion, projection/field selection, one-to-many `flat_map`/`udtf`, Debezium CDC policy, rename/drop/add fields, envelope normalization, lookup/enrichment, join, tumbling windows, deduplicate, Lua/JS/TS/WASM extension points |
| Operations | Web UI, REST API, saved connection catalog, pipeline validation, connection test, transform dry-run, Prometheus metrics, audit log |
| Reliability | At-least-once delivery by default, checkpoints, retry/backoff, DLQ list/replay/delete, idempotent sink modes where supported |
| Runtime | SQLite standalone mode, MySQL/PostgreSQL shared storage, master-worker distributed dispatch |

Connector coverage is broad, but maturity is not identical across every
connector and edge case. Treat the default contract as at-least-once, then use
business keys, versions, upserts, or sink-specific idempotency to remove
duplicate effects. See [idempotency](./docs/etl-idempotency.md) and the
[roadmap](./docs/ROADMAP.zh.md) for current production-readiness notes.

## When To Use It

Good fits:

- Database/Kafka/file/HTTP/object-storage pipelines into OLAP, search,
  databases, Kafka, or object storage.
- MySQL/PostgreSQL batch, CDC, and snapshot+CDC into ClickHouse, MySQL,
  PostgreSQL, Doris, Elasticsearch, S3, or Kafka.
- Kafka JSON/Debezium enrichment, deduplication, and tumbling-window aggregation
  into detail or aggregate tables.
- Self-hosted pipelines that need checkpoints, visible DLQ replay, idempotency
  guidance, preflight checks, and a lightweight UI/API.

Poor fits:

- Flink-style realtime business computation with arbitrary keyed state,
  processing-time timers, multi-stream state machines, or alert lifecycles.
- Stream processing that requires exactly-once savepoints, SQL planners,
  sliding/session windows, late side outputs, or retractions.
- ELT platforms whose primary value is a very large SaaS connector catalog.

## Quick Start

Run the bundled MySQL CDC to ClickHouse demo:

```bash
CONTAINER_CLI="${CONTAINER_CLI:-$(command -v docker || command -v podman)}"
"$CONTAINER_CLI" compose -f docker-compose.quickstart.yml up -d
```

Then open:

- Web UI and proxied API: <http://localhost:8000>
- Direct ETL API: <http://localhost:8001>

Example specs are loaded from [`pipes-quickstart/`](./pipes-quickstart). The
full walkthrough is in [docs/quickstart.md](./docs/quickstart.md).

## Minimal Pipeline Spec

Pipeline specs are YAML files under `pipes/` or the configured
`etl.specsDir`.

```yaml
name: file-to-file
source:
  type: file
  config:
    path: /app/data/input/orders.jsonl
    format: json
sink:
  type: file_sink
  config:
    output_dir: /app/data/output
```

Full connector fields are documented in
[docs/etl-config-schema.md](./docs/etl-config-schema.md).

## Reusing Saved Connections

Connections created through the UI or `POST /api/v2/connections` can be used by
linear pipelines and DAG nodes. The saved connection supplies `kind`, `type`, and
shared config; inline `config` overrides per-pipeline fields such as table,
topic, query, or output path.

```yaml
source:
  connection: orders-mysql
  config:
    table: orders
sink:
  connection_ref: warehouse-clickhouse
  config:
    table: orders_wide
```

The DAG designer can select saved connections directly, and connection tests can
run before a spec is saved or started.

## Advanced Aggregation By Orchestration

Wide-table and real-time aggregate use cases are built from ordinary pipeline
pieces:

```text
Kafka/MySQL CDC facts
  -> normalize_envelope / filter / type_convert
  -> lookup or join dimension data
  -> optional deduplicate and tumbling window aggregate
  -> ClickHouse / MySQL / PostgreSQL / Doris / S3 / Kafka sink
```

Current examples:

- [`testdata/pipes-wide-table/kafka-orders-detail-clickhouse.yaml`](./testdata/pipes-wide-table/kafka-orders-detail-clickhouse.yaml) -
  Kafka order events enriched with MySQL dimensions and written to ClickHouse.
- [`testdata/pipes-wide-table/kafka-orders-aggregate-clickhouse.yaml`](./testdata/pipes-wide-table/kafka-orders-aggregate-clickhouse.yaml) -
  Kafka order events deduplicated, enriched, window-aggregated, and written to
  ClickHouse.
- [`docs/adr/0001-kafka-wide-table.zh.md`](./docs/adr/0001-kafka-wide-table.zh.md) -
  design note for the orchestration approach.

This means the pipeline engine has the building blocks for denormalized detail
tables and tumbling-window aggregates today. More complex stream-stream joins,
sliding/session windows, CDC dimension updates, late-data handling, and
DAG/stateful replay still need tighter production certification; those gaps are
tracked in the roadmap rather than in a separate module.

## Connectors And Operators

| Stage | Built-in surface |
| --- | --- |
| Sources | `mysql_cdc`, `mysql_snapshot_cdc`, `postgres_cdc`, `mysql_batch`, `kafka`, `file`, `http`, `redis` |
| Transforms | `normalize_envelope`, `debezium_cdc`, `cdc_policy`, `ddl_guard`, `filter`, `validate`, `project`, `select_fields`, `flat_map`, `udtf`, `type_convert`, `rename`, `drop_field`, `add_field`, `deduplicate`, `lookup`, `enricher`, `join`, `window`, `router`, `fanout`, `tap`, `rate_limiter`, `lua`, `javascript`, `typescript`, WASM plugins |
| Sinks | `clickhouse`, `mysql`, `postgres`/`postgresql`, `doris`, `elasticsearch`/`es`, `kafka`, `redis`, `s3`, `file_sink`, `jdbc` |

For exact fields, defaults, secret markers, and examples, use the plugin schema
API (`GET /api/v2/plugins/schema`) or
[docs/etl-config-schema.md](./docs/etl-config-schema.md).

## Run And Build

Download a release archive from [Releases](../../releases), or run the container
image:

```bash
CONTAINER_CLI="${CONTAINER_CLI:-$(command -v docker || command -v podman)}"
"$CONTAINER_CLI" run -d --name openetl-go -p 8000:8000 -p 8001:8001 \
  -v "$PWD/pipes:/app/pipes" \
  ghcr.io/a8851625/openetl-go:latest
```

Build from source:

```bash
make build
```

Useful development commands:

```bash
make test          # unit tests with -race
make test-quick    # faster internal ETL test loop

cd web
npm install
npm run build      # rebuild resource/public
```

Optional runtime builds:

| Build option | Effect |
| --- | --- |
| default | Pure Go core plus built-in connectors and Lua |
| `-tags=extism` | Enable WASM plugin runtime |
| `-tags=nolua` | Remove Lua runtime for a smaller binary |
| `CGO_ENABLED=1` | Enable JavaScript/TypeScript transforms through QuickJS |

## Runtime Model

- Config: [`manifest/config/config.yaml`](./manifest/config/config.yaml).
- Startup flags: run `./openetl-go --help` for `--config`, local directory
  flags, bind host/port, storage, TLS/auth/audit, and master/worker role
  options. Priority is CLI flags > environment variables > config file >
  built-in defaults.
- Specs: YAML files under `pipes/` or `etl.specsDir`, hot-reloaded by file watch.
- Storage: SQLite by default; MySQL/PostgreSQL for shared state and distributed
  mode.
- API auth: set `ETL_API_TOKEN`, `etl.apiToken`, or `--api-token`, then use
  `X-API-Token` or `Authorization: Bearer <token>`.
- Metrics: Prometheus endpoint at `/metrics`.
- UI/API: GoFrame serves the Web UI on `:8000` and proxies `/api/v2/*` to the
  ETL API server on `:8001`.

## Documentation

- [Quick start](./docs/quickstart.md) / [中文](./docs/quickstart.zh.md)
- [Product positioning](./docs/positioning.md) / [中文](./docs/positioning.zh.md)
- [REST API](./docs/etl-api.md) / [中文](./docs/etl-api.zh.md)
- [YAML config reference](./docs/etl-config-schema.md) / [中文](./docs/etl-config-schema.zh.md)
- [Idempotency and delivery semantics](./docs/etl-idempotency.md) / [中文](./docs/etl-idempotency.zh.md)
- [Parallelism and batching](./docs/parallelism-and-batching.md) / [中文](./docs/parallelism-and-batching.zh.md)
- [Roadmap and maturity notes](./docs/ROADMAP.zh.md)
- [Architecture standard](./SPEC.md)
- [Contributing](./CONTRIBUTING.md) / [中文](./CONTRIBUTING.zh.md)

## License

MIT, see [LICENSE](./LICENSE).
