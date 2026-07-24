# transform/dbt

## Purpose
Bridge OpenETL-Go batches into an existing dbt project so CDC/batch data can be
transformed by community dbt models before writing to a sink.

## Prerequisites (optional capability)
dbt is **not** a core OpenETL-Go dependency. The runtime host must provide:

- Python 3.9+
- `dbt-core`
- Adapter matching `adapter` config:
  - `postgres` → `dbt-postgres`
  - `duckdb` → `dbt-duckdb`
- A valid dbt project (`dbt_project.yml` + models)
- Network access from the OpenETL-Go process to the staging/output database

Example install:

```bash
pip install dbt-core dbt-postgres
# or
pip install dbt-core dbt-duckdb
```

## Config Fields
- `project_dir`, `model_name`, `source_table` (required)
- `source_schema`, `target_schema`, `target_table`
- `adapter` (`postgres` | `duckdb`, phase 1)
- `dsn` (postgres) / `path` (duckdb)
- `threads`, `target`, `dbt_binary`, `profiles_dir`
- `exec_timeout_sec`, `write_mode`, `full_refresh`, `vars`

When `profiles_dir` is omitted, OpenETL-Go writes a temporary `profiles.yml`
from the configured DSN/path and cleans it up on transform close.

## Record Shape
Consumes a batch of records, stages their `data` fields into
`source_schema.source_table`, runs `dbt run --select <model_name>`, then emits
one record per row in `target_schema.target_table`. Input metadata lineage
(`source`, `key`, event timestamp) is preserved when batch sizes align.

## Checkpoint, DLQ, Idempotency
- dbt non-zero exit / timeout → whole batch transform error → pipeline retry/DLQ
- Checkpoint advances only after the downstream sink write succeeds
- `write_mode=replace` makes staging table contents match the current batch
- dbt model side effects (incremental models, external macros) are outside
  OpenETL-Go's transactional boundary — design models to be re-runnable

## Fits
- Reuse existing dbt models after CDC/batch capture
- Postgres OLTP staging → dbt transform → OLAP/ODS sink
- Local DuckDB analytical transforms in lightweight pipelines

## Does Not Fit
- Streaming per-record SQL without a batch boundary
- Phase 1: MySQL / ClickHouse dbt adapters (planned later)
- Environments that cannot install a dbt CLI

## Example
```yaml
transforms:
  - type: dbt
    config:
      project_dir: /etc/etl/dbt/orders
      model_name: transformed_orders
      source_schema: etl_staging
      source_table: orders_raw
      target_schema: etl_output
      target_table: transformed_orders
      adapter: postgres
      dsn: postgres://etl:pass@postgres:5432/etl?sslmode=disable
      exec_timeout_sec: 600
```

## Evidence
- Unit tests: `internal/etl/transform/dbt_test.go` (command build, profiles
  generation, mocked dbt subprocess success/failure/timeout, registry schema)
- Schema registration: `internal/etl/server/schema.go` + `schema_test.go`
- Optional E2E (skippable without dbt): `hack/e2e-dbt.sh`
