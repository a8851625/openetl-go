# OpenETL-Go Release Notes

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 格式。

## [Unreleased]

## [v0.2.12-beta.15] — 2026-08-12 — CDC DELETE primary-key fix (kafka → clickhouse)

### Fixed (CDC delete routing)

- **mysql_cdc now fills `Metadata.Key` with the per-table primary-key JSON
  object for every binlog event (INSERT/UPDATE/DELETE).** Previously only the
  mysql_snapshot_cdc snapshot phase filled it; the CDC phase left
  `Metadata.Key` empty, so when a CDC record flowed through kafka → clickhouse
  with `pk_columns_from_metadata: true`, the clickhouse sink could not derive
  the PK and fell back to the static `pk_columns` (often `id`), producing
  `delete record missing primary-key column "id"` for tables whose PK is not
  `id` (e.g. `session` keyed by `session_id`). With this fix the CDC record
  carries `{"session_id":"..."}` and the sink DELETE resolves the correct
  column. Composite primary keys are now also supported on the CDC path.
- **kafka source Debezium parser no longer overwrites `Metadata.Key` with
  `source.event_id`.** The Debezium envelope carries a dedup-only `event_id`
  in `source`, and beta.12 mistakenly promoted it to `Metadata.Key`,
  clobbering the per-table PK JSON object that travels in the Kafka message
  key. The per-table PK is now correctly recovered from the Kafka message
  key (set by the sink from `rec.Metadata.Key`), so
  `pk_columns_from_metadata` works end-to-end through the Debezium envelope
  path. This was the same root cause as the `missing primary-key column`
  DELETE failures on kafka → clickhouse.

### Tests

- `TestMetadataKeyJSONMulti` (single/composite PK, missing/nil PK, no cols).
- `TestPkColumnNames` (canal schema.Table PK index → name resolution).
- `TestMysqlCDCHandlerFillsMetadataKey` (regression guard for all ops).
- `TestKafkaHandlerEnvelopeParsesDebeziumStyle` updated to assert the
  per-table PK JSON is recovered from the Kafka message key (not event_id).
- `go test ./internal/etl/...` pass; `go vet` clean.

### Evidence

- `bash hack/check-release-assets.sh` passed.
- Container-level DELETE-through-kafka e2e pending a clean image build
  (host disk pressure); the fix is validated via unit tests covering the
  full chain: mysql_cdc fills Metadata.Key → kafka sink serializes it as
  msg.Key → kafka source restores it → clickhouse sink pk_columns_from_metadata
  resolves the correct DELETE column.

### Residuals

- BUG-1 (mysql_batch string-PK cursor) still queued.
- Host disk/IO pressure prevented a fresh container image build this round;
  rebuild with `docker build` once the host recovers.

## [v0.2.12-beta.14] — 2026-08-12 — SQLite read/write split + default retention (BUG-3)

### Fixed (storage contention)

- **SQLite storage backend now uses a separate read connection pool**, so
  checkpoint saves (short writes on the hot path) no longer queue behind
  long SELECT queries (`ListPipelines`, `ListAudit`, health/metrics). SQLite
  WAL mode allows concurrent readers that never block the single writer;
  the backend now opens a 1-connection writer pool plus a 4-connection
  reader pool and routes all `query`/`queryRow` through the reader.
  Previously `MaxOpenConns(1)` serialized every storage call (checkpoint,
  DLQ, audit, spec, API queries) onto one connection, so any slow query
  could push a checkpoint save past its 30s commit deadline and block the
  pipeline until restart (BUG-3). MySQL/PostgreSQL backends (already
  `MaxOpenConns(20)`) are unaffected.
- **docker-compose now ships default `ETL_AUDIT_TTL=720h` (30d) and
  `ETL_RUN_HISTORY_TTL=168h` (7d).** The janitor was default-disabled
  (TTL=0), so audit_logs / run_history grew unbounded and ballooned the
  control-plane sqlite until checkpoint contention became chronic. The new
  defaults keep the db bounded on fresh deployments.

### Docs

- Production checklist (`docs/quickstart.zh.md`) now warns that multi-
  streaming-pipeline or slow-disk deployments should use the MySQL or
  PostgreSQL control-plane backend rather than SQLite, citing BUG-3.

### Tests

- `TestStoreReadDBRoutesReads`, `TestStoreExecUsesWriteConn`: verify SELECT
  routes to readDB and writes always use the writer.
- `go test ./internal/etl/...` pass (incl. full sqlstore/sqlite suites).

### Evidence

- `bash hack/e2e-kafka-multitable-clickhouse.sh` exit 0 PASS (storage path
  regression with the new split).
- `bash hack/check-release-assets.sh` passed.

### Residuals

- BUG-1 (mysql_batch string-PK cursor) still queued.
- Slow-disk pressure-test (simulating the user's 688MB db + load 13) is
  not reproduced in CI; the fix is validated via unit tests of the routing
  logic and the rationale that WAL readers do not block the writer.

## [v0.2.12-beta.13] — 2026-08-12 — CDC binlog-purge recovery + ClickHouse Date/YEAR fixes

### Added (CDC robustness)

- **mysql_cdc / mysql_snapshot_cdc now detect MySQL binlog-purge (ERROR 1236)
  and recover per a configurable policy instead of looping forever.** When the
  checkpointed binlog file no longer exists on the server (purged by
  `binlog_expire_logs_seconds`, `PURGE BINARY LOGS`, or `RESET MASTER`), the
  canal reconnect loop previously retried the same stale position forever
  (`Read error (x13): canal disconnected: ERROR 1236 ...`). New
  `cdc_on_binlog_purged` source config key:
  - `fail` (default, fail-closed): stops the pipeline and surfaces the
    `ErrBinlogPurged` sentinel error for manual checkpoint reset — no silent
    data loss.
  - `resume_from_current`: advances the CDC resume position to the current
    MySQL master position and continues. **All changes between the stale
    checkpoint and now are dropped** (explicit RPO loss).
  - `resnapshot` (`mysql_snapshot_cdc` only): falls back to the snapshot phase
    from the last per-table cursors and re-enters CDC at the new handoff.
  Detection covers both the typed `*mysql.MyError{Code:1236}` and the
  user-visible text variants ("Could not find first log file name in binary
  log index", "Client requested source to start replication from position >
  file size").

### Fixed (ClickHouse type handling)

- **`Date` columns** now accept RFC3339 / datetime source values
  (`"2020-01-01T00:00:00+08:00"`) by truncating to the calendar date, instead
  of failing with `parsing time "..." extra text: "T00:00:00+08:00"`. Source
  pipelines that round-trip MySQL `DATE` through kafka envelopes serialize it
  as RFC3339, which the clickhouse-go driver previously rejected for a `Date`
  column. Empty strings map to NULL (Nullable) or epoch day.
- **MySQL `YEAR` columns** now map to `UInt16` (ClickHouse) / `SMALLINT`
  (MySQL/Postgres/Doris) instead of `DateTime64`. A `YEAR` value like `2026`
  is not a parseable datetime, so the previous mapping crashed writes with
  `converting float64 to Datetime64 is unsupported`.

### Tests

- `TestIsBinlogPurgedError`, `TestParseBinlogPurgedPolicy`,
  `TestBinlogPurgedRecoveryResumeFromCurrent`, `TestBinlogPurgedRecoveryFail`,
  `TestErrBinlogPurgedIsSentinel`.
- `TestConvertClickHouseValueDateColumn` (RFC3339/plain/space-sep/empty/junk).
- `TestConvertClickHouseValueEdgeTypes` (IPv4/IPv6/FixedString/Enum/LowCardinality).
- `TestMapSourceTypeYear` (year/year(4) for all dialects).
- `go test ./internal/etl/...` pass; `go vet` clean.

### Evidence

- Container-level binlog-purge e2e (`hack/e2e-binlog-purged.sh`): ran
  mysql_snapshot_cdc to a real checkpoint (`mysql-bin.000001:1131`), stopped
  it, ran `RESET MASTER` on the source, restarted; the `fail` policy detected
  ERROR 1236 and stopped the pipeline with the sentinel error on the FIRST
  retry (x1, not the previous x13 infinite loop).
- Container-level snapshot_cdc -> Redpanda -> ClickHouse with YEAR/TIME/Date/
  datetime/decimal/tinyint(1) source columns: all auto-created correctly
  (`birth_year UInt16`, `work_time String`, `date_end Date`, `created_at
  DateTime64(3)`, `score Decimal(18,2)`, `active UInt8`), 3 rows written with
  zero failures/DLQ.
- `bash hack/e2e-kafka-multitable-clickhouse.sh` exit 0 PASS (regression).
- `bash hack/check-release-assets.sh` passed.

### Residuals

- BUG-3 (sqlite `MaxOpenConns(1)` checkpoint contention) still queued; the
  binlog-purge recovery reduces the blast radius of the checkpoint-blocked
  condition but does not fix the underlying single-connection bottleneck.
- BUG-1 (mysql_batch string-PK cursor) still queued.

## [v0.2.12-beta.12] — 2026-08-11 — Kafka sink emits Debezium-style envelopes

### Changed (envelope format)

- **The kafka sink now writes Debezium-style envelopes** instead of the legacy
  `{event_id,op,table,key,data,timestamp,column_types}` shape. This unifies
  the CDC contract so downstream consumers can use the existing
  `debezium_cdc` transform or any standard Debezium client, and the real
  source schema flows through via Connect schema fields. New envelope:
  - `payload.before` / `payload.after` (row images; DELETE carries `before`)
  - `payload.source` (db/table/ts_ms/event_id/binlog file+pos/offset)
  - `payload.op` (`c`=insert, `u`=update, `d`=delete)
  - `schema.fields[after].fields` carry each column's raw MySQL COLUMN_TYPE
    (e.g. `varchar(32)`, `datetime`, `decimal(10,2)`), which the kafka source
    restores into `Metadata.ColumnTypes` and sinks map via `MapSourceType`.
- **The kafka source parses the new Debezium envelope** (restoring op, before,
  after, source db/table/binlog, and column types from schema) and still
  accepts the legacy OpenETL envelope as a fallback, so staged rollouts work.
- `mysql_snapshot_cdc` snapshot phase attaches `Metadata.ColumnTypes` per table
  (queried once from `information_schema.columns`), so the CDC → kafka hop now
  carries the real source schema end to end.

### Fixed

- `InferFromValues` inspects ALL samples (not just the first): any non-empty
  sample disproving a numeric/temporal hint downgrades the column to String,
  so a `work_time` column carrying `""` on some rows and `"[1,2,3]"` on others
  builds String instead of DateTime64.

### Tests

- Extended `TestKafkaSinkWriteSendsEnvelopeAndRecordsMetrics`: asserts the
  Debezium payload (op=after=source.event_id=ts_ms) and schema after.fields
  carry the MySQL column types.
- New `TestKafkaHandlerEnvelopeParsesDebeziumStyle`: op/db/table/binlog and
  column types recovered from schema; legacy envelope still parsed.
- Extended `TestInferFromValues` (multi-sample hint safety).
- `go test ./internal/etl/...` pass; `go vet` clean.

### Evidence

- Container-level two-stage `mysql:8.0 snapshot_cdc -> Redpanda -> ClickHouse
  24.3`: source `staff(work_time varchar(32))` carrying `""`, `"[1,2,3]"` and a
  real time range; kafka message verified to be the Debezium shape; auto-created
  ClickHouse table `work_time String, amount Decimal(18,2), created_at
  DateTime64(3), id Int32, name String` (all from source schema, not value
  inference); 3 rows write cleanly, zero failed/DLQ.
- `bash hack/e2e-kafka-multitable-clickhouse.sh` exit 0 PASS (regression).
- `bash hack/check-release-assets.sh` passed.

### Residuals

- `mysql_batch` string-PK cursor (BUG-1) unrelated.

## [v0.2.12-beta.11] — 2026-08-10 — Propagate source schema through kafka; string-safe multi-sample inference

### Added (root-cause fix for kafka -> clickhouse type errors)

- **Source column types now flow through the kafka hop.** Previously a two-stage pipeline (`mysql snapshot_cdc -> kafka -> clickhouse`) lost the real source schema at the kafka boundary: the snapshot phase did not attach column types to records, and the kafka envelope only serialized row values. Downstream sinks then guessed types from sample values and mis-built columns (e.g. a `varchar` `work_time` carrying `[1,2,3]` or `""` became `DateTime64(3)` and every write failed). Now:
  - `mysql_snapshot_cdc` snapshot phase queries `information_schema.columns` once per table and attaches `COLUMN_TYPE` for every column to each record's `Metadata.ColumnTypes` (the CDC phase already did this via canal `RawType`).
  - The kafka envelope carries a new `column_types` field (`map[string]string`); the kafka sink serializes it and the kafka source restores it into `Metadata.ColumnTypes`.
  - Sinks that honor `Metadata.ColumnTypes` (clickhouse auto-create/schema-drift, doris, mysql, postgresql) now receive the real source schema end to end, so a `varchar`/`json`/`text` source column is built as String instead of being guessed from values.
  - Backward incompatibility (per request): the envelope format changed; old envelopes without `column_types` still parse (the field is optional), but producers and consumers should be upgraded together.

### Fixed

- **`InferFromValues` now inspects all samples, not just the first.** A single non-empty sample that disproves a numeric/temporal name hint (e.g. `work_time` carries `""` on some rows but `"[1,2,3]"` on others) now downgrades the whole column to String. Previously only the first sample decided the type, so an empty first row built `DateTime64(3)` and every subsequent `[1,2,3]` row failed. This is the value-inference safety net beneath the schema-propagation fix above.

### Tests

- Extended `TestKafkaSinkWriteSendsEnvelopeAndRecordsMetrics`: record `ColumnTypes` round-trips through the kafka envelope.
- Extended `TestKafkaHandlerEnvelopeRestoresCDCSemantics`: `column_types` in the envelope is restored into `Metadata.ColumnTypes`.
- Extended `TestInferFromValues`: multi-sample hint safety (empty + junk -> String, empty + real timestamp -> DateTime, all-numeric -> Decimal).
- `go test ./internal/etl/...` pass; `go vet` clean.

### Evidence

- Container-level two-stage pipeline (mysql:8.0 `snapshot_cdc` -> Redpanda -> ClickHouse 24.3): source `staff(work_time varchar(32))` carrying `""`, `"[1,2,3]"` and a real time range; kafka message verified to carry `column_types:{work_time:"varchar(32)",...}`; auto-created ClickHouse table is `work_time String, amount Decimal(18,2), created_at DateTime64(3), id Int32, name String`; all 3 rows write cleanly (empty created_at -> epoch), zero failed/DLQ.
- `bash hack/e2e-kafka-multitable-clickhouse.sh` exit 0 PASS (regression).

### Residuals

- `mysql_batch` string-PK cursor does not advance (BUG-1 in roadmap); unrelated to this fix.

## [v0.2.12-beta.10] — 2026-08-09 — ClickHouse string-safe temporal inference; empty-string DateTime coercion

### Fixed

- **Auto-create no longer builds DateTime columns for non-timestamp text**. Temporal column-name hints (`_at`/`_time`/`date`/`timestamp`/…) were applied to any string value, so a `work_time` column carrying `""`, `"[1,2,3]"` or other non-date text was built as `DateTime64(3)` and every write failed with `parsing time "[1,2,3]" as "2006-01-02 15:04:05…`: cannot parse`. `InferFromValue` now applies temporal hints only when the string parses as a timestamp (empty strings keep the hint, empty rows coerce at write time); hex/array/junk text falls back to String. Same rule as the numeric hint fix in beta.9, applied to `_at`/`_time`/`date`/`timestamp` families. Protects Kafka/envelope pipelines that carry no ColumnTypes.
- **Empty-string writes to existing DateTime/DateTime64 columns no longer abort the batch**. `convertClickHouseValue` now maps empty/blank strings to `NULL` for `Nullable(DateTime*)` and to epoch `1970-01-01 00:00:00 UTC` for non-nullable `DateTime`/`DateTime64`, mirroring the numeric empty-string→0 rule. Non-empty unparseable strings (e.g. `"[1,2,3]"`) stay as-is so the driver fails loudly and the row lands in the DLQ (no silent data loss).
- Fixed a fragile hardcoded assertion in `TestConnectorEvidenceManifestLoadsAndCoversProductionConnectors` (manifest record count is now compared against the live production connector set instead of a hardcoded 14).

### Tests

- New `TestInferFromValueTemporalHintRequiresParseableString` (typing): work_time `""` → DateTime64(3), `"[1,2,3]"`/`"hello"` → String, parseable timestamps keep the hint, across ClickHouse/MySQL dialects.
- New `TestConvertClickHouseValueEmptyStringToDateTime` (sink): empty/blank → epoch for DateTime/DateTime64, → nil for Nullable(DateTime*), parseable strings still parse, junk text stays as-is.
- `go test ./internal/etl/...` pass; `go vet ./internal/etl/sink/...` clean.

### Evidence

- Container-level (mysql:8.0 → ClickHouse 24.3, dev image): source `staff(work_time varchar)` carrying `''`, `'[1,2,3]'` and a real time string → auto-created `work_time String` + `created_at DateTime64(3)`; all 3 rows write cleanly (empty created_at → epoch), zero failed/DLQ.
- `bash hack/e2e-kafka-multitable-clickhouse.sh` exit 0 PASS (regression).

## [v0.2.12-beta.9] — 2026-08-09 — ClickHouse auto-create honors source metainfo; string-safe numeric type hints

### Fixed

- **Auto-created ClickHouse tables now prefer the real source schema**: the clickhouse sink built DDL purely from sample values + column-name heuristics (`*_id` suffix → Int64), so a MySQL `varchar` `request_id` carrying hex ids produced an Int64 column and every write failed with `converting string to Int64 is unsupported`. `inferClickHouseType` now takes the source-declared type (`Metadata.ColumnTypes`) first and falls back to value/name inference only when absent/unmapped. `mysql_batch` attaches driver-reported column types to every record (zero extra queries) and `mysql_cdc` attaches canal `RawType` (+ `unsigned` qualifier), so the previously Debezium-only metainfo path now covers the native MySQL sources end to end.
- **String values no longer force numeric columns via name hints**: `InferFromValue` numeric DDL hints (`id`/`*_id`/`is_*`/`deleted`/flags/amount…) now apply only when the string parses as a number; hex/uuid text falls back to the dialect string type. Empty strings keep the hint (coerced to 0 at write time). This also protects Kafka/envelope pipelines that carry no ColumnTypes at all.
- New tests: `TestInferFromValueNumericHintRequiresParseableString` (typing), `TestInferClickHouseTypeDeclaredPriority` (sink).

### Evidence

- `go test ./internal/etl/...` pass; `go vet` clean.
- Container-level (mysql:8.0 → ClickHouse 24.3, dev image): auto-created table has `request_id String`, `amount Decimal(18, 2)`, `created_at DateTime64(3)`, `user_id Int32`, `ORDER BY (request_id)`; two rows with hex `request_id` write cleanly, zero failed/DLQ.
- `bash hack/e2e-kafka-multitable-clickhouse.sh` exit 0 PASS (regression).

### Residuals (bounded, not in scope)

- `mysql_batch` string-PK cursor does not advance (`updateLastID` is numeric-only); varchar primary keys cause an endless re-read loop. Tracked as BUG-1 in the roadmap; needs an any-typed cursor + checkpoint format change.

## [v0.2.12-beta.8] — 2026-08-09 — ClickHouse numeric columns coerce empty-string source values

### Fixed

- **ClickHouse sink no longer aborts whole batches on empty-string values**: MySQL/CDC sources often carry `''` in varchar columns mapped onto existing numeric ClickHouse columns. The native driver's `AppendRow` failed with `converting string to Int64 is unsupported`, failing the ENTIRE batch; single-row isolation recovered only the rows without empty strings and the rest went to the DLQ. `convertClickHouseValue` now coerces blank strings to numeric zero for Int/UInt/Float/Decimal target types (shared by native and HTTP write paths). Non-numeric garbage strings still fail loudly at `AppendRow` and land in the DLQ — no silent data coercion.
- New unit test `TestConvertClickHouseValueEmptyStringToNumeric` (empty/blank -> 0 for Int64/UInt64/Nullable/LowCardinality/Float64/Decimal; garbage stays a string; normal parses unchanged).

### Evidence

- `go test ./internal/etl/sink/` pass; `go vet` clean.
- `bash hack/e2e-kafka-multitable-clickhouse.sh` exit 0 PASS (regression on rebuilt image).

### Residuals (bounded, not in scope)

- Empty-string -> 0 is the at-least-once sync semantic; users requiring explicit NULL semantics should add a `type_convert`/`validate` transform before the sink.

## [v0.2.12-beta.7] — 2026-08-09 — ClickHouse sink multi-table fan-out (`table_template` + `pk_columns_from_metadata`)

### Added

- **ClickHouse sink multi-table fan-out**: the `clickhouse` sink now supports `table_template` (e.g. `ods_{table}`, with `{db}` placeholder) and `pk_columns_from_metadata`. A single sink can route a multi-table Kafka envelope stream (one topic, many tables) into per-table ClickHouse targets — the same capability the Doris sink already had:
  - `table_template` substitutes `{table}`/`{db}` from each record's envelope metadata; a template referencing missing metadata is a configuration error instead of a silently malformed table name.
  - `pk_columns_from_metadata` derives per-table primary-key columns from the JSON-object `Metadata.Key`; auto-created tables get heterogeneous `ORDER BY` keys (e.g. `ods_orders` ORDER BY order_id, `ods_users` ORDER BY user_no) on `ReplacingMergeTree(_version)`.
  - Batch compaction, auto-create ORDER BY, UPDATE routing and DELETE conditions all use the per-table key; a key-set change within one batch for the same table is a write error (no silent key mixing).
  - Legacy single-table behavior (static `table`/`pk_columns`/metadata fallback) is unchanged when the new fields are absent.
- Shared metadata-key parsing extracted to `internal/etl/sink/metadata_pk.go` (used by both Doris and ClickHouse sinks; Doris behavior unchanged).
- New unit tests `internal/etl/sink/clickhouse_table_template_test.go`: resolve static/template/error paths, config parsing, per-table PK derivation, key-change rejection, missing-key rejection, per-table compaction.
- New end-to-end `hack/e2e-kafka-multitable-clickhouse.sh`: Kafka envelope multi-table → ClickHouse fan-out, heterogeneous auto-create DDL, update absorption via ReplacingMergeTree, delete mutation, schema drift add_columns, checkpoint reset replay absorption (uniqExact stable after replay).
- New spec example `testdata/pipes-kafka-ch-multitable/kafka-multitable-to-clickhouse.yaml`.
- `docs/etl-config-schema.md` documents `table_template`/`pk_columns_from_metadata` for the ClickHouse sink.
- `docs/myduckserver-verification.zh.md`: reproducible evidence for the MySQL → MyDuckServer integration assessment (three OpenETL channels currently unusable, engine-level capability baseline).

### Evidence

- `go test ./internal/etl/sink/` (incl. new tests) and `go test ./internal/etl/...` pass; `go vet ./internal/etl/...` clean.
- `bash hack/e2e-kafka-multitable-clickhouse.sh` exit 0 PASS: ods_orders/ods_users fan-out, heterogeneous ORDER BY DDL, upsert absorption (FINAL), mid-stream add-column, DELETE mutation, checkpoint reset replay without duplicate inflation.
- `bash hack/check-release-assets.sh` passed.

### Residuals (bounded, not in scope)

- Kafka source still starts at `OffsetNewest` unless `initial_offset: oldest` is set — new consumer groups skip pre-existing messages by default (documented in the new e2e spec; changing the default would be a separate item).
- ClickHouse multi-table routing is e2e-covered on the native protocol; the HTTP write path shares the same routing (unit-level) but was not re-certified this round.

## [v0.2.12-beta.6] — 2026-08-08 — mysql_snapshot_cdc emits JSON primary-key, enabling kafka→doris/es auto PK detection

### Fixed

- **mysql_snapshot_cdc no longer emits an empty `Metadata.Key` (important)**: previously neither the snapshot nor the CDC phase populated `Metadata.Key`, so after a Kafka relay the downstream doris/elasticsearch sink with `pk_columns_from_metadata` received an empty string and failed with `requires Metadata.Key to be a non-empty JSON object`. Fixed by a new `metadataKeyJSON` helper that builds a JSON object from the primary-key column name and value (e.g. `{"id":123}` / `{"audit_log_id":99}`). The snapshot path uses the resolved `rpk.column`; the CDC path looks up `resolvedPKs[table]` first, then falls back to canal's `e.Table.PKColumns` (covering tables skipped during snapshot but still streamed via CDC). insert/update use the after-image, delete uses the deleted row — all carry the PK. This makes the "native source → kafka → native sink" auto-PK path work end to end without a static `pk_columns` config.

### Verification Boundary

- `go test -race -count=1 ./internal/etl/...` passes all packages.
- New unit tests: `TestMetadataKeyJSON` covers int/string/bigint unsigned/empty-column/missing-value/nil-value cases; `TestDorisMetadataKeyColumnsFromSnapshotCDCSource` verifies the doris sink derives `[address_id]` from `{"address_id":12345}`.
- End-to-end proof: after `mysql_snapshot_cdc → kafka → kafka(envelope)`, the captured Kafka message partition key and envelope `key` field are both `{"id":1}` (empty before the fix), exactly the format doris sink `pk_columns_from_metadata` consumes.
- New `hack/e2e-snapshot-cdc-kafka-doris.sh` script (could not pass locally due to an environmental Doris BE black-list failure; script logic mirrors `e2e-doris.sh` and validates snapshot dump 5 rows + auto-create `UNIQUE KEY(id)` + CDC upsert when BE is healthy).
- 13-script / 14-record connector certification suite re-run at this release commit; see `docs/connector-certification.md`.

## [v0.2.12-beta.5] — 2026-08-08 — mysql_snapshot_cdc unsigned PK classification fix + Kafka brokers parsing hardening

### Fixed

- **mysql_snapshot_cdc misclassified unsigned integer primary keys (critical)**: `pkKindForType` stripped the column length but not the `unsigned`/`zerofill` suffix, so `bigint unsigned`, `int unsigned` and other display-width-less integer PKs fell through to `pkKindOrdered` (string cursor). Consequences: the snapshot paged with a string cursor (`WHERE pk > '99'`), which misses/replays rows across pages because `'100' < '99'` as strings, and that path disables MOD sharding so multi-million-row tables ran single-threaded; repeated full rescans stacked duplicates into the target topic. Fixed by also trimming the qualifier after the length so `bigint unsigned` → `bigint` → numeric cursor; added a checkpoint migration bridge that back-fills integer-valued string cursors (e.g. `"99"`) from legacy `last_strs` into `last_ids` so the snapshot resumes instead of replaying after the fix.
- **Kafka brokers config parsing hardening**: `stringSliceField` (preflight) and `readStringSlice` (runtime) only decoded JSON array text on the string-scalar branch and left two shapes unhandled — a slice whose element is itself JSON array text (e.g. `[]interface{}{"[\"redpanda:9092\"]"}`) and a double-wrapped JSON string — so sarama dialled the literal `["redpanda:9092"]` as a single broker address and failed with `missing port in address`. Fixed by a recursive flatten helper covering plain addresses, JSON arrays, and JSON-string wrapping; the preflight path keeps empty elements so "empty partition field" / "empty column name" diagnostics still fire.

### Verification Boundary

- `go vet ./internal/etl/...` clean; `go test -race -count=1 ./internal/etl/...` passes all 24 packages.
- New unit tests: `TestPKKindForType` covers width-less unsigned integer types; `TestMigrateStringCursorsToNumeric` covers the checkpoint bridge; `TestStringSliceFieldFlattensNestedJSON`, `TestSourceConfigStringSliceNestedJSON`, `TestFlattenBrokerListText` cover broker parsing.
- `CONTAINER_CLI=podman bash hack/e2e-snapshot-cdc-heteropk.sh` extended with an `audit_id BIGINT UNSIGNED` table (ids 100/101 force three-digit cursor advance that the legacy string path breaks on); the fixed image writes all 16 records (records_written=16) while the buggy image times out (got=0).
- 13-script / 14-record connector certification suite re-run at this release commit; see `docs/connector-certification.md`.

## [v0.2.12-beta.4] — 2026-08-08 — Checkpoint restore fail-closed + connector certification evidence gate (beta)

### Added

- **Connector certification evidence gate**: a new evidence freshness manifest
  (`internal/etl/server/evidence/connector-evidence.json`) binds every
  production source/sink record to a certified commit/image, dependency set,
  execution window, expiry, and named cases; the descriptor readiness gate
  exposes the same metadata. `hack/check-connector-evidence.sh -strict` fails
  on unverified/expired records and on any runtime, connector, script, or
  workflow change after the certified revision. The gate is wired into `main`
  pushes and both release workflows, so release tags must be bound to a
  certified revision (or a descendant updating only the evidence manifest and
  certification docs).
- **Certification reproducibility hardening**: connector e2e fixtures are
  isolated per script, reruns are deterministic, evidence is bound to source
  ancestry, and 14 production records are recorded with verified evidence.
- **Production readiness audit doc**: `docs/PRODUCTION-READINESS-AUDIT-2026-07-26.zh.md`
  captures the checkpoint/DLQ/UI/certification audit findings.

### Fixed

- **Checkpoint restore fail-closed (PR-2.4.1)**: checkpoint-store load errors,
  malformed JSON, unknown envelope versions, and envelopes without a source
  position fail linear/DAG startup as a visible `failed` pipeline; the source
  is never opened with an empty position. Valid legacy positions keep working.
- **Checkpoint commit ordering (PR-2.4.2)**: `CheckpointForRecord` no longer
  triggers Kafka MarkOffset/Commit or PostgreSQL committedLsn/standby ack
  side effects; external acknowledgements happen only after the durable Save;
  an external-ack failure blocks later checkpoints and fails the pipeline
  (safe replay); Kafka auto-commit is off; PG keepalives no longer report
  read-ahead progress as durable.
- **Snapshot cursors bound to durable batches (PR-2.4.3)**: mysql_snapshot_cdc
  no longer advances a lossy source cursor before the checkpoint is durable;
  linear/DAG pipelines checkpoint complete source batches, numeric/string
  cursors and the snapshot→CDC handoff agree after Save; missing/invalid
  cursors, missing handoff, and closed channels fail closed.
- **Source position validation before startup (PR-2.4.4)**: semantically
  corrupt positions (missing fields, negative offset/page/cursor, topic/source
  mismatch, invalid LSN/phase) fail closed before `Source.Open`; API/health
  expose stable `last_error_code`/`last_error_remediation`; the WebUI pipeline
  detail/issues panel shows checkpoint remediation with a safe retry entry
  (reset is not the default fix).
- **Doris/Kafka preflight and runtime contract**: Doris `table_template` no
  longer triggers a false missing-static-table error. With
  `pk_columns_from_metadata: true`, Doris derives composite keys from
  JSON-object Kafka envelope keys for compaction, upsert, DELETE, and
  auto-create DDL; scalar keys fail with actionable remediation. A
  `debezium_cdc` transform alone no longer bypasses the stable-key check.
- **Kafka broker array compatibility**: source/sink runtime, connection
  context, and preflight now parse JSON-string arrays such as
  `"[\"broker:9092\"]"`, while preserving YAML arrays, ordinary single-string
  values, and IPv6 brokers.
- **Lookup excludes DLQ records**: lookup/enricher batch output no longer
  re-mixes already-DLQed records into later batches (avoids replay
  duplicates).
- **UI error contract reconciliation**: pipeline create/config failures use
  structured error codes + remediation, `dag_validation` emits consistent
  error paths, and the WebUI shows actionable fix suggestions.
- **Preflight multi-table schema boundaries**: schema checks for multi-table
  mapping/wide-table scenarios are explicit (`pipelines_preflight_test.go`).
- **Connector descriptors derived from schema**: descriptor and schema share
  one source of truth to avoid drift
  (`ConnectorDescriptorConfigContractMatchesSchemaExactly`).

### Verification Boundary

- `go vet ./internal/etl/... ./internal/logic/... ./internal/cmd/...` — passed
- `go test -race -count=1 ./internal/etl/... ./internal/logic/...` (24 packages) — passed
- `bash ./hack/check-release-assets.sh` — passed
- `bash ./hack/check-connector-evidence.sh -strict -commit <release commit>` — passed (fresh certification window in `docs/connector-certification.md`)
- Certification suite re-run at the release commit: 13 scripts / 14 production records passed

## [v0.2.12-beta.1] — 2026-08-06 — Lightweight DWH scenario + Kafka CDC relay link (beta)

First beta targeting the "lightweight data warehouse" scenario. Adds the
minimal MySQL → OpenETL-Go → Doris topology plus batch spec generators, and
fills the three engine gaps needed to relay CDC over Kafka without losing
INSERT/UPDATE/DELETE semantics. This is scenario work and is **not** bound to a
`PR-*` gate in `docs/ROADMAP.zh.md`; the Kafka relay link has unit and
e2e evidence but not yet certification-grade crash/replay reconciliation, so
it ships as beta.

### Added

- **Lightweight DWH orchestration & tooling**
  - `docker-compose.lightweight-dwh.yml` + `docs/lightweight-dwh.md`: minimal
    two-component topology (OpenETL-Go + Doris, no Kafka / MinIO / Airflow /
    standalone BI), with deploy, target-DB, spec, and common-adjustment notes
    (remote source, multiple source DBs, fragmented primary keys) plus an
    optional Kafka relay section.
  - `hack/gen-doris-specs-by-table.sh`: for DBs with highly fragmented PKs,
    generates one `mysql_batch → doris` spec per table (query-only, no binlog,
    cron staggered per table).
  - `hack/gen-doris-specs-grouped.sh`: same scenario but groups tables by real
    PK into `mysql_snapshot_cdc → doris` specs, collapsing hundreds of tables to
    "number of distinct PK shapes" specs.
- **Kafka CDC relay link (mysql_cdc → kafka → mysql/doris)**
  - `kafka` sink: new `topic_template` (e.g. `cdc-{db}-{table}`) routes each
    record to a per-source-table topic instead of one static topic.
  - `kafka` source: new `format: envelope` parses OpenETL's own envelope
    (`{event_id,op,table,key,data,timestamp}`) and restores
    INSERT/UPDATE/DELETE so the relayed chain behaves like a direct CDC
    consumer (Doris / MySQL upsert+delete).
  - `mysql_batch` source: now emits `Metadata.Database` so downstream
    (`topic_template={db}-{table}`, table_mapping routing, audit) gets the
    source DB name.

### Semantic boundaries (Kafka relay link)

- **The envelope carries `Data` (after-image) only, not `Before` (pre-image).**
  MySQL CDC update/delete therefore have only the after-image after relay;
  downstream transforms/sinks that depend on `Before` (e.g. compact back-fill
  from pre-image) are unsupported across a Kafka relay. Doris / MySQL
  upsert+delete use the primary-key row in `Data` and are unaffected.
- **`topic_template` mode skips the sink's topic validation/auto-create**, relying
  on broker-side `auto.create.topics.enable=true` (Redpanda default) or
  pre-created topics; if the template references `{db}`/`{table}` but the record
  lacks that metadata, `Write` returns a hard error rather than silently
  falling back to the static topic.
- Default remains **checkpointed at-least-once**; replay boundaries of the
  relayed chain match a direct CDC link.

### Robustness fixes (new code this round)

- `kafka` sink `topic_template`: a placeholder referencing `{db}`/`{table}`
  with missing metadata now returns a hard error (previously it silently fell
  back to the static topic, mixing unrelated tables into one destination);
  `Open` emits a warning when it skips topic validation, prompting broker
  auto-create or topic pre-creation.

### Verification (local closeout)

- `go vet ./internal/etl/...` — passed
- `go test ./internal/etl/source/ ./internal/etl/sink/ -count=1` (with `-race`) — passed
- Unit: `TestKafkaSinkTopicTemplateRoutesByMetadata`,
  `TestKafkaSinkTopicTemplateEmptyResolutionErrors`,
  `TestKafkaHandlerEnvelopeRestoresCDCSemantics`,
  `TestKafkaHandlerEnvelopeMalformedFallsBackToValue`,
  `TestMySQLBatchReaderFillsDatabaseMetadata` — passed
- `hack/e2e-cdc-kafka-relay.sh`: MySQL CDC → Kafka(topic_template) →
  Kafka(envelope) → MySQL, verifying INSERT/UPDATE/DELETE reach the target
  consistently with the source — passed (Redpanda + MySQL containers, live run)

### Out of scope / residual boundaries

- The Kafka relay link has **no certification-grade crash/replay
  reconciliation** yet (e.g. SIGKILL mid-producer, checkpoint-reset replay,
  consumer-group rebalance); this round covers only the happy-path
  INSERT/UPDATE/DELETE semantic restoration. Full certification is a follow-up.
- `mysql_batch.Database` e2e coverage is provided by a unit test (sqlmock); the
  lightweight-DWH `mysql_batch → doris` path has no live Doris e2e (requires an
  external Doris image).
- This work is **not bound to a `PR-*` gate** in `docs/ROADMAP.zh.md`; it is
  scenario delivery. Promoting it to a certified path requires a roadmap
  increment plus crash/replay evidence.
- Distributed (master-worker) remains beta / production-candidate; MaxCompute
  writer still unimplemented.

## [v0.2.11] — 2026-08-04 — Standalone production-ready release (control plane / reliability closeout)

This is the first non-beta release. It promotes the `v0.2.11-beta.*` control-plane,
reliability, backup/restore, path-contract, and operations work to a **standalone
production-ready** release. Distributed (master-worker) mode remains
**beta / production-candidate** and is gated by `PR-D1`; experimental connectors
(MaxCompute writer) remain unimplemented.

### Production-ready scope (standalone)

- **PR-0 control-plane persistence & security defaults** (delivered): API/memory/DB
  consistency, encrypted spec restart/rollback, atomic current/version/checkpoint
  boundaries, scheduler prepare/commit compensation, production fail-closed profile,
  CORS / trusted-proxy / security headers, two-port TLS topology.
- **PR-1 maintainability & security** (delivered, incl. 1.3): connection/settings
  secret envelope, migration conformance, backup/restore/upgrade/rollback runbooks,
  retention janitor (DLQ/audit/run/task) with hard caps, health status, failure alerts.
- **PR-2 data consistency** (delivered): two forced main paths certified against
  crash / checkpoint-reset / sink-outage / DLQ-replay with `silent_loss=0`:
  - `mysql_cdc__mysql_upsert` — MySQL CDC → MySQL upsert (stable PK absorbs replay).
  - `mysql_snap_cdc__ch_rmt` — MySQL snapshot+CDC → ClickHouse ReplacingMergeTree
    (`pk_columns` + `_version`; `FINAL`/materialized for current state).
- **P5 operations & release gates** (delivered): `/api/v2/health` business health,
  CI production gate, `docs/ops-runbook.md`, `docs/release-checklist.md`,
  `docs/resource-baseline.md`.

### Semantics (unchanged)

- Default remains **checkpointed at-least-once**; a crash after sink acknowledgement
  and before checkpoint commit may replay the last batch — absorb via business keys /
  upsert / RMT / explicit deduplicate.
- PostgreSQL CDC `on_truncate` defaults to `error`; multi-sink fanout and
  CDC → file/S3 stay blocked unless `allow_unsafe`.
- Source binlog and sink are not in a distributed transaction; non-atomic fanout is
  documented as a residual boundary.

### Out of scope / residual boundaries (not production)

- **Distributed (master-worker) mode: beta / production-candidate.** Authenticated
  worker HTTP client, task generation/attempt/lease CAS ownership, stale-owner 409
  fencing, and bounded requeue are delivered (`PR-D1`), but multi-master and
  cross-worker strong consistency are out of scope; `docker-compose.distributed.yml`
  requires `ETL_API_TOKEN`.
- **MaxCompute / ODPS**: registered for descriptor/config/schema/partition
  validation and preflight blocks writer-disabled pipelines; the SDK-backed batch
  writer, remote permission/table checks, DLQ/retry e2e, and production maturity
  are not implemented.
- **External storage-backend e2e** (MySQL/PostgreSQL backup/restore conformance):
  skipped without credentials; SQLite is certified by unit + script. Skipped
  external backends do not count as production certification for that backend.
- Distributed multi-master and cross-worker strong consistency remain out of scope.

### Validation (release closeout)

- `go vet ./internal/etl/... ./internal/logic/... ./internal/cmd/...` — passed
- `go test -count=1 ./internal/etl/... ./internal/logic/...` — passed
- `go test ./internal/etl/telemetry ./internal/etl/alert -count=1` — passed
- `./hack/check-release-assets.sh` — passed
- `go build .` (pure Go, Lua included) — passed

### Evidence matrix

| Item | Result |
| --- | --- |
| Code gates (vet / unit / health / assets) | passed |
| PR-0 control-plane persistence & security defaults | delivered |
| PR-1 secret / migration / backup-restore / upgrade | delivered (incl. 1.3) |
| PR-2 path contracts (forced main paths) | delivered; e2e scripts referenced in `docs/path-contract.md` |
| SQLite backup/upgrade conformance | passed (unit + script) |
| MySQL/PostgreSQL storage/backup | skip without external env; not certified by skip |
| Distributed PR-D1 (auth/fencing/requeue) | delivered as beta; `e2e-distributed` covers fence/auth |

### Increments since v0.2.11-beta.4

- fix(ui): highlight only changed lines in pipeline version diff
- fix(docker): rebuild frontend and embed UI into image binary
- fix(ci): stage binary outside dockerignore for beta image publish

## [v0.2.11-beta.4] — 2026-07-26 — Production-ready control plane / reliability / distributed beta closeout

### Highlights

- **PR-0 / PR-1 control-plane security base**: encrypted spec restart/rollback, atomic current/version/checkpoint boundaries, scheduler prepare/commit compensation, production fail-closed profile, CORS/trusted-proxy/security headers, two-port TLS topology; UI API token stays page-memory only.
- **PR-1.3 backup/restore/upgrade/janitor**: control-plane JSON backup covers 11 object classes with reconcile; legacy SQLite forward upgrade fails closed; retention janitor (DLQ/audit/run/task) with hard caps, health status, and failure alerts.
- **PR-2 main-path failure reconciliation + path contract**: `docs/path-contract.md` + `GET /api/v2/paths/contracts`; forced paths `mysql_cdc__mysql_upsert` / `mysql_snap_cdc__ch_rmt` cover happy / crash / checkpoint reset / sink outage+DLQ replay (silent_loss=0).
- **P5 business health and release gates**: `/api/v2/health` business health, CI production gate, `docs/ops-runbook.md` / `docs/release-checklist.md` / `docs/resource-baseline.md`.
- **PR-D1 distributed worker auth & fencing (still beta)**: authenticated worker HTTP client, task generation/attempt/lease CAS ownership, stale-owner 409 fencing, bounded requeue; `docker-compose.distributed.yml` requires `ETL_API_TOKEN`.
- **Connector / transform increments**: dbt transform Phase 1 (postgres/duckdb); `rest_source` + SaaS template connectors; distinct/sort/cast/coalesce/limit/skip/sample; Kafka source fetch tuning; typed auto_create / soft-delete type fix; streaming placement scale-out.

### Semantics

- Default remains **checkpointed at-least-once**; crash may replay the last batch; absorb duplicates via business keys / upsert / RMT.
- PostgreSQL CDC `on_truncate` defaults to `error`; multi-sink fanout and CDC→file/S3 stay blocked unless `allow_unsafe`.
- Distributed remains **beta / non multi-master**; MaxCompute writer is still unimplemented.

### Validation (local closeout)

- `go vet ./internal/etl/... ./internal/logic/... ./internal/cmd/...`
- `go test -count=1 ./internal/etl/... ./internal/logic/...`
- `go test ./internal/etl/telemetry ./internal/etl/alert -count=1`
- `./hack/check-release-assets.sh`

### Evidence matrix

| Item | Result |
| --- | --- |
| Unit + package tests | passed |
| Release assets pin / secrets | passed |
| SQLite backup/upgrade e2e | historical evidence under PR-1.3 scripts |
| MySQL/Postgres storage/backup | skip without external env; not production-certified by skip |
| Path contract forced paths | PR-2 e2e script evidence |
| Distributed PR-D1 | beta; e2e-distributed covers fence/auth |

### Residual

- MaxCompute writer / remote permission / production maturity not implemented
- Distributed multi-master and cross-worker strong consistency out of scope
- External-backend e2e requires credentials for certification

## [v0.2.11-beta.2] — 2026-07-22 — UI prototype alignment and IA cleanup

### Highlights

- **Pipeline list**: full-width (no master-detail right pane); hash query filters (search/status/mode/tag/sort); selection toolbar for bulk start/stop; Start/Stop in row overflow; list actions right-aligned.
- **Create wizard**: full-page `#/pipelines/new` with 6-step rail, live summary, `?step=` + localStorage draft (skipped under e2e), confirm path to advanced designer.
- **DLQ closed loop**: three-column layout; pipeline picker with filter/backlog-only/sort; Replay confirm panel (target count, filter, sink idempotency, dry-run); Lucide actions + aria-labels.
- **Pipeline detail**: write-semantics + lifecycle cards; **Logs** tab (card surface, not terminal chrome); **Topology** read-only DAG (nodes/edges/config); schedule edit dialog from lifecycle; sole write path for topology is designer.
- **IA cleanup**: list no longer exposes composite DAG+Logs modal; connector catalog merges built-in matrix (cards/matrix toggle); schedule fleet page uses shared dialog; fewer duplicate “edit pipeline” entries.
- **AppShell**: topbar search, auto-refresh label, language toggle, reload-specs anchors for e2e; plugins under Extensions (myPlugins only for WASM).

### Validation

- `npm --prefix web run build`
- `./hack/e2e-ui.sh` → **108 passed, 0 failed** (prototype alignment)

### Residual

- DAG empty-canvas template strip, mobile table→info-row polish, refreshed light/dark screenshots, multi-run history depth when API allows. See `docs/UI-REDESIGN-TODO.zh.md`.

## [v0.2.11-beta.1] — 2026-07-21 — Task-oriented Web UI redesign (P4 landing)

### Highlights

- Primary navigation is now task-grouped: **Overview / Run / Resources / System**, with **New pipeline** as the primary action.
- **Designer/DAG is demoted, not removed**: day-to-day create uses the Source → Transform → Sink wizard; multi-source/router/fanout still use the same pipeline/DAG canvas for advanced edit.
- Shared pipeline health view-model: `healthy` / `degraded` / `failed` / `paused` / `scheduled` / `completed` derived from runtime state, lag, checkpoint age, DLQ, and last error. Overview is issue-first and no longer treats running/total as health.
- Shareable hash routes: `#/overview`, `#/pipelines`, `#/pipelines/new`, `#/pipelines/:id/:tab`, `#/issues`, `#/dlq`, `#/connections`, `#/connectors`, `#/designer`, and more.
- New Issues center, Connector catalog (separated from Connection instances), and pipeline detail tabs (Overview / Runs / Issues / Checkpoints / Spec).
- DLQ aggregates by error class / DAG node; bulk destructive actions stay hidden on empty backlog; replay reports remaining backlog.
- Visual tokens (cool canvas + teal accent), bilingual IA copy, and progressive disclosure of Workers only in distributed mode.
- Design baseline: `docs/UI-REDESIGN.zh.md`, `docs/UI-REDESIGN-PROTOTYPE.html`.

### Validation

- `npm --prefix web run typecheck`
- `npm --prefix web run build`
- `./hack/e2e-ui.sh` → **108 passed, 0 failed**（2026-07-21 原型对齐后）

### Boundary

- Default delivery remains **at-least-once**; the UI does not introduce a separate execution model or UI-only spec.
- Remaining P4 work (step-wizard reorganization, some field-level remediation, fuller a11y matrix) stays tracked in the roadmap.

## [v0.2.10-beta.1] — 2026-07-14 — Reliability certification and real WASM plugin path

### P1: Reliability certification closure

- Unified linear pipeline and DAG checkpoint envelopes around source position, StateStore snapshot versions, and sink acknowledgement metadata while retaining at-least-once delivery rather than cross-system transactions.
- Made checkpoint advancement fail closed: DLQ persistence, state snapshot collection, sink acknowledgement metadata collection, or checkpoint storage failures no longer silently advance the source position.
- Fixed Kafka offset `0` being skipped by zero-value checkpoint checks.
- Fixed the final sink-acknowledged batch remaining uncheckpointed after interval throttling when traffic becomes idle; pending boundaries are now flushed by a timer and on Stop/EOF.
- Made `allow_unsafe` an executable spec field. Kafka/CDC to file/S3 remains blocked by default and requires an explicit opt-in limited to paths whose replay-duplicate boundary has been tested and documented.
- Added the [reliability certification matrix](docs/reliability-certification.md) and expanded Kafka/wide-table coverage for process crashes, broker restarts, consumer group rebalances, offset replay, state restoration, and sink acknowledgement envelopes.

### P2: Real WASM plugin end-to-end path

- Added a real TypeScript transform fixture and `hack/e2e-wasm-plugin.sh` covering real WASM compilation, ABI v1 manifest installation, 0/1/N outputs, secret configuration, DLQ routing, replay after upgrade, and restart reload.
- Added a compiler image with architecture checksum validation and pinned esbuild 0.25.6, Extism JS PDK 1.6.0, and Binaryen 130, including build-time checks for `wasm-merge` and `wasm-opt`.
- Updated the Extism JS SDK bridge to current `Host`, `Config`, and `Var` globals, with WASI, per-call configuration updates, state bridging, and concurrency-safe install/unload/exec behavior.
- Fixed the server-side transform-only compilation path and public docs: TypeScript is bundled to CommonJS by esbuild before the current `extism-js input.js -i interface.d.ts -o output.wasm` CLI is invoked; source and sink plugins remain offline-compile/install flows.
- Added static certification gates for real WASM fixtures, manifests, and compiler inputs. Third-party plugins without independent fault/replay evidence remain beta/dev-only.

### Release Boundary

- Default delivery semantics remain **at-least-once**; this release does not provide cross-system exactly-once transactions. Production pipelines should use stable business keys, versions, upserts, ReplacingMergeTree-style sinks, or explicit deduplication to absorb replay.
- MaxCompute/ODPS remains experimental and externally blocked until SDK-backed writes, real permission/table checks, and DLQ/retry end-to-end evidence are available.
- Uncertified third-party plugins and Feishu plugin samples remain beta/dev-only until they have real-environment fault injection and replay evidence.

### Validation

- `go test ./... -count=1`
- `go test -tags=extism ./internal/etl/plugin/pluginsystem ./internal/etl/server -count=1`
- `npm --prefix web/plugin-sdk run build`
- `npm --prefix web run build`
- `./hack/e2e-kafka.sh`
- `E2E_SKIP_BUILD=1 ./hack/e2e-wide-table.sh`
- `E2E_SKIP_BUILD=1 ./hack/e2e-lookup-state.sh`
- `./hack/e2e-wasm-plugin.sh`

## [v0.2.9] — 2026-07-13 — Multi-table mapping sync, CDC wide-table path, UI scenario entry, connection scope

### Highlights
- **Multi-table A→B sync with table name mapping**:
  - Pipeline-level `table_mapping` supports `template` / `rules` / `regex` with `{source_table}` and `{source_db}` tokens.
  - Mapping preserves `_source_table` / `_source_database` before rewrite.
  - `mysql_cdc` / `mysql_snapshot_cdc` now populate `Metadata.Database` for qualified mapping and CDC policy filters.
  - Snapshot checkpoint cursors remain keyed by original source table after mapping.
  - New e2e: `hack/e2e-multi-table-map.sh` + `testdata/pipes-multi-table-map/`.
- **Multi-table binlog → wide table**:
  - Production-candidate path: `mysql_cdc` + `cdc_policy` + `lookup` + rename/type_convert → ClickHouse wide table.
  - New e2e: `hack/e2e-mysql-cdc-wide.sh` + `testdata/pipes-mysql-cdc-wide/`.
- **UI productization for the two core scenarios**:
  - Wizard adds recommended templates: multi-table DB sync + mapping, and CDC wide table (lookup).
  - Wizard exposes editable `table_mapping` and generates ordinary pipeline YAML.
  - Connection Catalog / Wizard / DAG forms use connection vs task-parameter field scope with clearer labels.
  - Designer toolbar labels, empty-state copy, and empty pipeline/connection/DLQ/audit/WASM hints improved.
  - Fixed WASM Plugins and Workers i18n bare keys (EN/ZH).
- **Extension and ops packaging**:
  - Official Feishu sheet source plugin sample under `web/plugin-sdk/examples/feishu-sheet-source/` (beta/dev-only).
  - Lightweight runtime modes doc + smoke: `docs/runtime-modes.md`, `hack/e2e-runtime-smoke.sh`.
  - Descriptor/schema field `scope` annotation and certification kit sample checks extended.
- **Warehouse ETL residual evidence** (carried from mainline work):
  - Relational write modes, generated-column skip, Debezium metadata PK, DAG load/DLQ replay, and related e2e coverage remain part of the release surface.

### Release Boundary
- Default delivery semantics remain **at-least-once**. Use upsert, stable business keys, version columns, ReplacingMergeTree, or explicit deduplication to absorb replay.
- MaxCompute/ODPS remains experimental without real-environment write/DLQ/replay evidence.
- Built-in `feishu_sheet` and the Feishu WASM plugin sample remain beta/dev-only until real Feishu fault-injection evidence exists.
- Complex multi-fact real-time merge / Flink-style wide-table semantics are still out of scope; the certified wide-table path is fact stream + dimension lookup (+ optional tumbling aggregate).

### Validation
- `go test ./internal/etl/server ./internal/etl/pipeline ./internal/etl/source ./internal/cmd -count=1`
- `npm --prefix web run build`
- `./hack/pack.sh` (or `SKIP_UI=1 ./hack/pack.sh` after UI build)
- `bash hack/e2e-runtime-smoke.sh`
- `E2E_SKIP_BUILD=1 bash hack/e2e-multi-table-map.sh`
- `E2E_SKIP_BUILD=1 bash hack/e2e-mysql-cdc-wide.sh`
- Playwright UI spot-check: Wizard templates, table_mapping panel, WASM/Workers ZH i18n

## [v0.2.8] — 2026-07-06 — Lookup query-mode certification, Plugin ABI v1 production boundary, Doris/UI release closure

### Highlights
- **Lookup query-mode and state certification**:
  - Closed the first lookup asynchronous I/O loop with query-mode validation, Redis-only cache gate, preflight/schema/spec checks, and `hack/e2e-lookup-query.sh`.
  - Added lookup query fixtures covering successful lookup, miss, timeout, and lock-wait/replay behavior.
  - Added runner DLQ context regression coverage so DLQ write failures do not silently advance checkpoints.
- **Connector certification kit expansion**:
  - Added/extended certification checks for descriptor/schema/readiness/e2e evidence and component docs.
  - Added production-candidate evidence for MySQL, ClickHouse, Kafka, S3/File and ongoing Doris certification.
  - Updated certification docs with plugin ABI rules and production plugin gates.
- **Plugin ABI v1 production boundary**:
  - Centralized plugin name/kind/manifest validation in `internal/etl/plugin/pluginsystem`.
  - `/api/v2/plugins/install` now accepts an optional Plugin ABI v1 `manifest` field and validates explicit manifests before writing/loading WASM.
  - Plugin metadata persisted in storage now includes ABI, minimum runtime version, manifest JSON, and `manifest_validated`.
  - `/api/v2/plugins` and `/api/v2/plugins/schema` expose the current `plugin_abi` contract.
  - TypeScript SDK exports ABI constants, manifest types, and `definePluginManifest`; the VIP example now declares a manifest.
  - Added `docs/plugin-abi-v1.md` with the manifest shape, compatibility matrix, deprecation policy, and certification boundary.
- **Doris production-candidate certification hardening**:
  - Expanded `hack/e2e-doris.sh` to use an independent MySQL source port and cover MySQL CDC -> Doris plus MySQL snapshot+CDC -> Doris.
  - Added restart/replay evidence: app restart continuation, checkpoint reset replay absorption, schema drift add-column, and Doris BE outage -> DLQ -> recovery replay.
- **Phase 1 verification and UI productization closure**:
  - Fixed PostgreSQL CDC e2e MySQL client host usage.
  - Completed Wizard transform-chain productization for add/remove, type switch, reorder, per-stage dry-run, and stage-positioned partial errors.
  - UI e2e now covers the transform-chain controls and remains at 99 passing checks.
- **Operational polish**:
  - Added distributed worker label HTTP e2e coverage.
  - Added logging regression coverage.
  - Refreshed packed UI assets and release version metadata.

### Release Boundary
- Plugin ABI v1 infrastructure is production-ready as an extension boundary. Individual third-party plugins are not production-certified unless they provide their own manifest, docs, tests, and runtime evidence.
- Feishu/Lark spreadsheet plugin integration is recorded as the next official plugin-sample item in the roadmap; the existing built-in `feishu_sheet` source remains beta until more real-environment evidence is available.
- Default delivery semantics remain at-least-once; production guidance continues to rely on upserts, stable business keys, version columns, and sink-specific replay absorption.

### Validation
- `go test ./internal/etl/plugin/pluginsystem ./internal/etl/server ./internal/etl/storage/... -count=1`
- `go test ./internal/etl/... ./internal/cmd -count=1`
- `go test ./... -count=1`
- `npm --prefix web/plugin-sdk run build`
- `npm --prefix web run build`
- `SKIP_UI=1 ./hack/pack.sh`
- `CONTAINER_CLI=podman ./hack/e2e-ui.sh` — 99 passed, 0 failed
- `git diff --check`

## [v0.2.7] — 2026-07-03 — Debezium CDC preflight fix, enricher async I/O enhancement, Phase 1 数仓 ETL 场景闭环

### Highlights
- **Debezium CDC preflight fix**: Added `hasDebeziumCDCTransform()` helper; `checkRelationalSinkConfig` and `checkDorisSinkConfig` now skip `table` and `pk_columns` static requirements when the pipeline carries a `debezium_cdc` transform with `auto_create: true` / `pk_columns_from_metadata: true`. Suppressed `pk_columns` recommendation for CDC pipelines.
- **enricher async I/O enhancement** (Phase 1 "异步 I/O 维表查询增强"): Rewrote `EnricherTransform` with:
  - `concurrency` / `max_in_flight` controls for parallel in-flight enrichment calls within a batch via `BatchTransform`.
  - `max_retries` / `retry_base_ms` with exponential backoff for transient errors (HTTP 429/5xx, network timeouts).
  - HTTP 429 `Retry-After` header honored during retry.
  - Explicit failure classification: 429/5xx → `transient`, 401/403 → `auth`, other 4xx → `data`.
  - Full `TransformMetricsProvider` with 10 counters (`processed`, `hits`, `misses`, `cache_hits`, `cache_misses`, `timeouts`, `retries`, `errors`, `succeeded`, `in_flight`).
  - SQL mode now benefits from `timeout_seconds` context deadline (previously only HTTP).
  - `hub e2e-enricher.sh` with 4 scenarios: happy path, 429+Retry-After retry, timeout→DLQ, batch partial failure→DLQ.
- **Phase 1 数仓 ETL 场景闭环** delivered in full:
  - pre_write action (MySQL/PostgreSQL: delete/truncate/truncate_partition with parameterized condition).
  - map_fields transform (declarative enum/status code mapping).
  - Post-Commit Trigger via `schedule.type: dependency` for CDC→recalculation patterns.
  - increment batch_mode for accumulator columns (MySQL/PostgreSQL).
  - extract transform (regex `pattern`+`group` and `template` join).
  - feishu_sheet source connector (OAuth2 client_credentials + sheet polling).
  - HTTP source OAuth2 client_credentials auth enhancement.
  - Connection config responsibility consolidation (behavior fields deprecation warning).
  - Sink metadata-driven column set: generated column skipping and `pk_columns_from_metadata` for Debezium key PK derivation.

### Validation
- `go test -count=1 -run TestRunPreflight ./internal/etl/server/`
- `go test -count=1 -run TestEnricher ./internal/etl/transform/`
- `go test ./internal/etl/transform/ ./internal/etl/server/ ./internal/cmd -count=1`
- `go vet ./internal/etl/... ./internal/cmd`
- `E2E_SKIP_BUILD=1 ./hack/e2e-enricher.sh` — 4 scenarios passed
- `go build -buildvcs=false ./...`

## [v0.2.6-beta-2] — 2026-07-01 — Wire runtime Scheduler into Server

### Highlights
- Wired `orchestrator.Scheduler` (cron/periodic/dependency schedule engine) into `Server.StartAll` so deferred-schedule pipelines are no longer started immediately but registered with the scheduler.
- Added `s.scheduler` field to the Server struct, initialized in `NewServer`, with `StartAll` registering each deferred pipeline and calling `go s.scheduler.Run(ctx)`.
- All runtime API paths (create, update, import, schedule PUT/DELETE, pipeline delete) now register or unregister the schedule entry on the fly without requiring a restart.
- Added a `schedulerScheduleFor` helper that resolves pipeline display-name references to stable IDs for dependency schedules.
- Refactored `Scheduler` to accept `pipeline.RunnerInterface` instead of `*DAGExecutor`, so linear runners, parallel runners, and DAG runners are all schedulable.
- Added integration tests covering cron schedules not starting immediately on boot, and periodic schedules actually triggering the runner.

### Validation
- `go test ./internal/etl/... ./internal/cmd -count=1`

## [v0.2.5-beta.1] — 2026-06-29 — AI context pack and reviewed DAG generation

### Highlights
- Added an AI context pack generated from connector descriptors, plugin schema, maturity metadata, component docs, product boundaries, DAG rules, examples, and common error patterns.
- Added `GET /api/v2/ai/context` and updated `POST /api/v2/ai/generate` to use the context pack instead of a hard-coded prompt; generated drafts now return `context_pack_version`, `validation`, and `review` metadata.
- Added AI review flags for missing required fields, secret confirmation, experimental/dev-only maturity, CDC-to-append replay risk, MaxCompute/ODPS writer-disabled paths, DDL apply, script transforms, and disabled DLQ.
- Updated the DAG editor AI drawer to show validation status, missing fields, risk flags, required confirmations, and current-vs-generated YAML before the user applies the draft to the canvas.
- Improved the first-task wizard transform chain with add/remove/reorder controls, transform type switching, and per-stage dry-run while preserving the ordinary `transforms` array spec.
- Added first-batch component docs under `docs/components/` for core production-candidate sources, sinks, and transforms, with purpose, fields, record shape, checkpoint/DLQ/idempotency boundaries, examples, and evidence.
- Refreshed API/OpenAPI/Quickstart docs and UI assets for AI-assisted generation boundaries.

### Validation
- `npm --prefix web run build`
- `go test ./internal/etl/server ./internal/etl/transform -count=1`
- `podman run --rm -v "$PWD:/workspace" -v openetl-go_go-cache:/go -v openetl-go_go-build-cache:/root/.cache/go-build -w /workspace etl-go-dev:latest sh -c 'go test ./internal/etl/server ./internal/etl/transform -count=1'`
- `./hack/e2e-ui.sh` — 92 passed, 0 failed
- `./hack/pack.sh`

## [v0.2.4-beta.1] — 2026-06-29 — Connection context and schema introspection

### Highlights
- Added `GET /api/v2/connections/{name}/context`, returning a saved connection, connector descriptor, recommended schedule/batch/checkpoint settings, and best-effort source introspection.
- Added source introspection adapters for file/HTTP/demo samples, MySQL/PostgreSQL database/table/column/primary-key metadata, and Kafka topic/partition metadata.
- Updated the first-task wizard to select saved source/sink connections, render health/schema/sample/topic/table context, and generate ordinary specs with `connection` references plus recommended batch/checkpoint values.
- Updated the DAG editor node properties to show saved connection context while preserving the existing `connection` field in DAG specs.
- Refreshed API docs, OpenAPI metadata, embedded UI assets, and UI e2e coverage for saved connection context.

### Validation
- `go test ./internal/etl/server -count=1`
- `npm run build` in `web/`
- `./hack/pack.sh`
- `./hack/e2e-ui.sh` — 92 passed, 0 failed

## [v0.2.3-beta-1] — 2026-06-27 — First-task UI and runtime flags

### Highlights
- Added a first-task wizard in the React UI for database sync, Kafka detail/aggregation, Debezium CDC sync, Kafka protocol parsing, and file/HTTP landing tasks. The wizard emits ordinary pipeline specs and keeps YAML as the auditable source of truth.
- Added schema-driven source/sink/transform configuration forms, generated YAML editing, YAML-to-form sync, transform dry-run, validate + preflight, and create-and-start flow in the wizard.
- Extended the DAG editor with YAML-to-canvas/form roundtrip, validate + preflight actions, and structured rendering for errors, warnings, preflight issues, field issues, remediation, and DDL preview.
- Added runtime CLI flags for config path, local data/log/plugin/schema/spec directories, HTTP and ETL API bind addresses, storage, TLS, API token, audit, logger format, and standalone/master/worker role settings. Runtime precedence is now CLI flags > environment variables > config file > built-in defaults.
- Added shared Podman/Docker detection for hack scripts via `hack/container-cli.sh`, and updated e2e scripts and docs around the new container runtime selection.

### Validation
- `go test ./internal/cmd ./internal/etl/server ./internal/etl/sink`
- `go run . --help`
- Invalid `--role` startup check
- `E2E_SKIP_BUILD=1 ./hack/e2e-ui.sh` — 88 passed, 0 failed

## [v0.2.3-beta] — Doris validation and schedule constraints

### Highlights
- Promoted the Doris sink contract with safer defaults and real FE/BE validation: `ddl_policy` now defaults to `reject`, schema validation checks table existence, field compatibility, and Unique Key / `pk_columns` alignment, and `ddl_policy=apply` is limited to safe add-column changes.
- Hardened Doris writes and DDL for Doris 2.1: Stream Load labels are deterministic, JSON/CSV headers are explicit, errors are classified for retry/DLQ behavior, auto-create requires a stable key, and generated Unique Key DDL uses Doris-compatible column ordering and type inference.
- Added `hack/e2e-doris.sh` and included it in `hack/e2e-all.sh`; the script runs with Podman or Docker and validates MySQL batch -> Doris Stream Load JSON, Stream Load CSV, MySQL insert fallback, auto-create Unique Key, decimal inference, and zero failed records against official Doris FE/BE 2.1.11 images.
- Added source-bound scheduling metadata: source descriptors now expose `supported_schedules` and `default_schedule`, specs apply default schedules, and validation rejects unsupported `schedule.type` values with required-field checks for `cron`, `periodic`, and `dependency`.
- Updated the DAG editor to load connector descriptors, filter schedule types by the selected source set, support dependency schedules, and reset unsupported schedule selections when sources change.

### Validation
- `CONTAINER_CLI="${CONTAINER_CLI:-$(command -v podman || command -v docker)}"; "$CONTAINER_CLI" run --rm -v "$PWD:/workspace" -v openetl-go_go-cache:/go -v openetl-go_go-build-cache:/root/.cache/go-build -w /workspace localhost/etl-go-dev:latest sh -c 'go test ./internal/etl/...'`
- `npm run build` in `web/`
- `E2E_SKIP_BUILD=1 ./hack/e2e-doris.sh`

## [v0.2.1] — Pipeline orchestration cleanup and connection reuse

### Highlights
- Removed the standalone wide-table preview API and dedicated frontend page. Wide-table detail and aggregate use cases are now documented as ordinary pipeline/DAG orchestration patterns built from source, transform, state, and sink capabilities.
- Added saved connection references to linear pipeline specs and DAG nodes through `connection` / `connection_ref`, allowing shared connector credentials and base configs to live in the connection catalog while per-pipeline fields stay inline.
- Reworked the English and Chinese READMEs into a clearer product entrypoint covering quick start, minimal specs, connection reuse, orchestration-based wide-table aggregation, connector surfaces, runtime model, and documentation links.

### Validation
- `go test ./internal/etl/server ./internal/etl/pipeline ./internal/etl/orchestrator`
- `npm run build` in `web/`

## [v0.2.0] — Pipeline orchestration and reliability release

### Highlights
- Fixed React production bundle blank-page regressions caused by undefined runtime variables in routed pages, and refreshed the packed `resource/public` assets used by the Go server.
- Added a pipeline orchestration path around Kafka facts, lookup dimensions, tumbling aggregation, and ClickHouse output, including UI entries for orchestration preview, Connections, and Schedules.
- Added stable DLQ IDs for replay/delete flows, improved stateful transform metrics, and introduced state/checkpoint envelopes for state-backed deduplicate, lookup, join, and window paths.
- Added connector/roadmap maturity guidance so source, sink, transform, storage, and plugin capabilities are presented with explicit maturity instead of over-claiming production readiness.

### Pipeline Validation
- Added `hack/e2e-wide-table.sh` for Docker-based Redpanda + MySQL + ClickHouse validation.
- Covered Kafka -> lookup -> ClickHouse detail pipelines, Kafka -> deduplicate -> lookup -> tumbling aggregate -> ClickHouse pipelines, duplicate Kafka message absorption, schema drift DLQ, lookup miss DLQ and replay, lookup refresh failure DLQ, and ClickHouse outage DLQ/replay.

### Release Boundary
- This is a 0.2.0 release. Kafka orchestration-based aggregation, ClickHouse sink usage, lookup stream-table joins, tumbling aggregation, and SQLite-backed state are available as validated building blocks, not a blanket production-ready guarantee.
- Default delivery semantics remain at-least-once. Exactly-once, Kafka rebalance/crash guarantees, DAG/stateful replay, stream-stream production joins, complex windows, and full connector certification remain roadmap items.

### Verification
- `./hack/e2e-wide-table.sh`
- `./hack/e2e-ui.sh` — 73 passed, 0 failed
- Docker: `go test -timeout 120s ./internal/etl/...`

## [v0.1.0-beta2] — Phase 5 reliability and usability release

### Highlights
- Closed the beta2 P0/P1 reliability bar: standalone runner creation, file-source resume, zero-survivor checkpoint safety, Postgres CDC pgoutput parsing, worker slot accounting, sink error metrics, and preflight rejection for hard pipeline misconfigurations.
- Reworked the public quickstart surface around OpenETL-Go: canonical MySQL CDC -> ClickHouse examples, aligned Docker compose settings, richer `/api/v2/plugins/schema` metadata, and updated README/quickstart/deployment docs.
- Improved the lightweight release shape by excluding test fixtures from runtime images and publishing `-tags=nolua` as the Lua-free build option while keeping default Lua compatibility.

### Verification
- Added/updated focused tests for server preflight behavior, plugin schema coverage, runner checkpoint safety, Postgres CDC non-row messages, and worker slot limits.
- Verified affected packages with `go test -race -count=1 -timeout=120s ./internal/etl/server ./internal/etl/pipeline ./internal/etl/source ./internal/etl/worker`.

## [v0.1.0-beta] — 首个公开测试版

### 亮点
- **单二进制 ETL/CDC 引擎**,纯 Go 默认构建,零外部运行时依赖
- 8 种 Source + 9 种 Sink + 19 种 Transform,覆盖主流数据同步/清洗/轻度加工场景
- MySQL CDC (binlog) + PostgreSQL CDC (逻辑复制) + 快照增量衔接
- JDBC Sink (支持任意 JDBC 数据库,含 Oracle/SQL Server/DB2 等)
- 22 个 E2E 脚本验证(CDC 崩溃恢复 / DLQ / 分布式分片 / ClickHouse 自动建表 …)
- 单机 SQLite(零依赖) / 可扩展 MySQL·PG + master-worker 真分布式

### 连接器 (Sources)
- `mysql_cdc` — MySQL binlog CDC (行级增删改,含 GTID/position checkpoint)
- `mysql_snapshot_cdc` — MySQL 快照(全量) + 增量 CDC 无缝衔接
- `postgres_cdc` — PostgreSQL 逻辑复制 (pgoutput)
- `mysql_batch` — MySQL 全量批量读取
- `kafka` — Kafka 消费者组 (at-least-once,offset 断点)
- `redis` — Redis SCAN 全量
- `http` — HTTP API 分页读取(断点续传,429/5xx 指数退避)
- `file` — JSON Lines / CSV 文件(byte-offset checkpoint)

### 连接器 (Sinks)
- `clickhouse` — 原生协议 + HTTP 协议,自动建表(DDL 翻译),ReplacingMergeTree 裁剪
- `mysql` — 批量 INSERT / upsert(INSERT … ON DUPLICATE KEY UPDATE),幂等,自动建表
- `postgres` — 批量 INSERT / upsert(INSERT … ON CONFLICT),自动建表
- `doris` — Stream Load + MySQL DELETE,auto-create,DDL 翻译
- `kafka` — 同步生产者(支持幂等),auto-create topic
- `elasticsearch` — Bulk API,动态索引,多 host 轮询,429 Retry-After
- `redis` — HASH/STRING/LIST 三种模式
- `s3` — MinIO/S3 对象存储(分片上传,断点重试)
- `jdbc` — 任意 JDBC 数据库 (MySQL/PostgreSQL/Oracle/SQL Server/DB2/…)

### 转换 (Transforms)
- **清洗**: `filter`(表达式)、`deduplicate`、`validate`、`type_convert`
- **加工**: `rename`/`drop_field`/`add_field`、`enricher`、`lookup`、`join`、`window`
- **路由**: `router`(条件分流)、`fanout`(一对多) `tap`(旁路) `rate_limiter`
- **脚本**: `lua`(默认,gopher-lua)、`javascript`/`ts`(QuickJS,CGO)、WASM 插件(extism,wazero)

### 执行模式
- 线性 Pipeline — 串行 Source→Transform→Sink
- DAG — 多源多汇有向无环图,条件边路由
- ParallelRunner — 单源表分片并行写入
- master-worker 分布式 — MySQL/PG 共享存储,分片跨 worker 不重叠分发,worker 崩溃重分配

### 可靠性
- at-least-once + 幂等 sink (upsert / 版本列)
- DLQ 死信队列 (SQLite/MySQL/PG,`/api/v2/dlq/*` 查看重放删除)
- 三态断路器 (closed→open→half-open),基于 sink 独立隔离
- 指数退避重试 (`retry.Do` + 可重试错误分类)
- `-race` 默认跑测试;零静默数据丢失 (SPEC §4.2/§6.1)

### 运维
- REST API `/api/v2/*` (CRUD pipeline,上传下载 YAML,启停,查看状态/DLQ/preflight)
- Prometheus `/metrics` (每 sink 指标:rows/batches/errors/latency,断路器状态,lineage)
- JSON 结构化日志 (`LOGGER_FORMAT=json`)
- SQLite / MySQL / PostgreSQL 存储后端 (pipeline 定义/checkpoint/DLQ/audit)
- Web 管理界面 (Svelte,GoFrame resource-pack)
- `make test` / `make test-quick` / `make test-integration`

### 平台
- Linux (amd64, arm64)
- macOS (amd64, arm64 / Apple Silicon)
- Windows (amd64)

### 构建标签
| 标签 | 效果 | 默认? |
|------|------|------|
| *(无)* | 纯 Go 核心 + 全部 Sink/Source + Lua(gopher-lua,纯 Go) | ✅ |
| `-tags=extism` | + WASM 插件运行时(wazero,纯 Go) | — |
| `-tags=nolua` | 剥离 Lua 运行时,进一步瘦身 | — |
| `CGO_ENABLED=1` | + JavaScript/TypeScript transform(QuickJS,CGO) | — |

### 文档
- `docs/quickstart.md` — 5 分钟入门
- `docs/etl-api.md` — REST API
- `docs/etl-config-schema.md` — 配置字段
- `docs/etl-idempotency.md` — 幂等与 exactly-once 语义
- `docs/parallelism-and-batching.md` — 并行与批处理
- `SPEC.md` — 架构与生产就绪标准 (Phase 0-5 全部完成)
