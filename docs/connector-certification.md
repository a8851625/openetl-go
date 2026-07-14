# Connector And Plugin Certification Test Kit

This kit turns connector and plugin maturity claims into executable checks. It is focused on the first production-candidate connector set: MySQL, ClickHouse, Kafka, S3, and File. Plugin ABI checks cover the extension boundary; each third-party plugin still needs its own evidence before it can be called production-certified.

Cross-connector crash, replay, DLQ, state, and sink-commit evidence is tracked in [reliability-certification.md](./reliability-certification.md).

## Scope

The current kit checks:

- connector descriptor exists and is registered
- maturity is `production`
- typed config schema is present
- expected secret fields are marked secret
- readiness gates have the expected status
- partial gates include evidence and remediation
- descriptor evidence references exact e2e scripts
- component docs contain an `Evidence` section with the same script references
- referenced `hack/e2e-*.sh` scripts exist
- Plugin ABI v1 constants, manifest requirements, compatibility matrix, and TypeScript SDK helpers are documented

The first certified set is:

| Area | Connectors | Evidence |
| --- | --- | --- |
| MySQL | `mysql_batch`, `mysql_cdc`, `mysql_snapshot_cdc`, `mysql` sink | `hack/e2e.sh`, MySQL CDC/batch e2e, Debezium MySQL e2e |
| ClickHouse | `clickhouse` sink | ClickHouse CDC/autocreate/snapshot+CDC e2e |
| Kafka | `kafka` source/sink | Kafka source/sink, raw ODS, Debezium, wide-table e2e |
| S3/File | `file` source, `file_sink`, `s3` sink | file smoke e2e and S3 MinIO replay/outage e2e |

Plugin ABI v1 evidence:

| Area | Contract | Evidence |
| --- | --- | --- |
| WASM ABI | `openetl.plugin.abi/v1`, `openetl-runtime/v1`, required entrypoints per kind | `docs/plugin-abi-v1.md`, `internal/etl/plugin/pluginsystem/abi_test.go` |
| Install API | explicit manifest is validated before WASM load; legacy uploads are marked `manifest_validated=false` | `internal/etl/server/plugin_contract_test.go` |
| SDK | TypeScript SDK exports ABI constants, manifest types, and `definePluginManifest` | `web/plugin-sdk/src/index.ts`, `web/plugin-sdk/examples/vip-order-enricher.ts` |
| Source plugin sample | Feishu sheet source plugin with offline compile + install docs | `web/plugin-sdk/examples/feishu-sheet-source/`, `TestFeishuSheetSourcePluginSampleCertification` |
| Real transform runtime | real TypeScript→WASM, 0/1/N output, secret config, DLQ/replay, upgrade, restart reload | `hack/e2e-wasm-plugin.sh`, `hack/wasm-compiler.Dockerfile`, `web/plugin-sdk/examples/replay-matrix-transform/`, `TestWASMPluginCertificationFixture` |

## Running

Run the descriptor/doc certification checks:

```sh
go test ./internal/etl/server -run TestConnectorCertificationKitProductionSet -count=1
go test ./internal/etl/server -run TestPluginABIV1CertificationDocs -count=1
go test ./internal/etl/server -run TestFeishuSheetSourcePluginSampleCertification -count=1
go test ./internal/etl/server -run TestWASMPluginCertificationFixture -count=1
```


Run the main behavioral evidence used by this kit:

```sh
bash hack/e2e-s3-minio.sh
bash hack/e2e-kafka.sh
bash hack/e2e-clickhouse.sh
bash hack/e2e-cdc-mysql.sh
bash hack/e2e-wasm-plugin.sh
```

Use `E2E_SKIP_BUILD=1` only after rebuilding `openetl-go-etl:dev` from the current tree.

## Rules For New Production Connectors

A connector should not be marked `production` until it has descriptor metadata, typed schema, readiness gates, component docs, and at least one repeatable e2e or certification script. Partial gates are allowed only when the descriptor includes concrete remediation and the public maturity text describes the operator review boundary.

## Rules For Production Plugins

A plugin should not be marked production-certified until it has a validated ABI v1 manifest, typed config fields, docs, failure/restart evidence, and DLQ/replay/idempotency notes appropriate for its kind. The plugin runtime and install API can be production-ready while individual plugins remain `dev-only`, `experimental`, or `beta`.
