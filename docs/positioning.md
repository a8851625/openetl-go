# Product Positioning

OpenETL-Go is:

> A lightweight, self-hosted, open-source CDC/ETL runtime for data synchronization, cleansing, and aggregation.

It targets common pipelines across databases, Kafka, files, HTTP APIs, object storage, OLAP systems, and search indexes. The core model is `Source -> Transform -> Sink`, operated through equivalent YAML, API, and Web UI surfaces, with built-in checkpoints, retries, DLQ, metrics, audit logs, saved connections, schema/preflight checks, and extensible transforms.

## Good Fits

- MySQL/PostgreSQL batch, CDC, and snapshot+CDC replication into ClickHouse, MySQL, PostgreSQL, Doris, Elasticsearch, S3, or Kafka.
- Kafka JSON/Debezium cleansing, lookup enrichment, deduplication, and tumbling-window aggregation into detail or aggregate tables.
- Lightweight extraction from files, HTTP APIs, Redis, and object storage.
- Replacing hand-written sync scripts, lightweight DataX/Canal/Kafka consumer jobs, or small self-hosted pipelines where Flink, Spark, or a full ELT platform would be too heavy.
- Running as a data-pipeline task launched by Airflow, Dagster, Prefect, Kestra, or similar orchestrators.

## Poor Fits

- Complex stateful stream processing with arbitrary keyed state, processing-time timers, CoProcessFunction-style multi-stream state machines, or alert lifecycle logic.
- Workloads that require Flink/Spark-grade savepoints, exactly-once state snapshots, SQL planners, sliding/session windows, late side outputs, or retractions.
- SaaS-first ELT platforms where connector count is the main value.
- Large Kafka Connect/Debezium CDC estates that depend on standard Debezium envelopes, Kafka Connect offset management, and connector operations.

## Comparison

| Project | Strongest Area | OpenETL-Go Difference |
| --- | --- | --- |
| Airbyte / Meltano / dlt | ELT connector catalogs for SaaS/database-to-warehouse sync | Lighter, more focused on CDC/realtime sync, operations, DLQ replay, and self-hosted pipelines |
| Airflow / Dagster / Prefect | Workflow orchestration | Built-in source/sink execution, checkpoints, DLQ, and idempotency guidance; can also be orchestrated by them |
| Apache SeaTunnel / ChunJun | Heavier batch/stream data integration and distributed execution | Smaller default footprint, simpler deployment, Go single-binary first |
| Kafka Connect / Debezium | CDC connector runtime and Kafka ecosystem | More integrated transform/sink/DLQ/UI/API surface; should not overclaim CDC protocol maturity |
| DataX | Stable offline batch sync | More realtime, with CDC, checkpoints, DLQ, UI/API, and lightweight aggregation |
| Flink / Spark Streaming | Complex stateful stream computation | Not a replacement; OpenETL-Go covers lightweight cleansing, lookup, deduplication, and tumbling aggregation |

## Principles

- Reliability first: failed records must be visible through DLQ, retryable errors, or explicit audited drop policies.
- Lightweight first: the default path should run as a single binary/container, with SQLite for standalone use and MySQL/PostgreSQL storage for shared metadata/checkpoints and master-worker mode.
- Usability first: common tasks should be achievable through UI/API/YAML, with preflight explaining risks, schema issues, and idempotency choices before start.
- Extensibility first: connectors, transforms, and plugins should share descriptor, schema, preflight, metrics, DLQ, and certification contracts.
- Honest semantics: the default delivery contract is at-least-once; production pipelines absorb replay with business keys, versions, upserts, ReplacingMergeTree-style sinks, or deduplication.
