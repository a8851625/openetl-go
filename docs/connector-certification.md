# Connector And Plugin Certification Test Kit

This kit turns connector and plugin maturity claims into executable checks. It covers every built-in source or sink currently marked `production`; adding an unknown production connector without a certification target fails the suite. Plugin ABI checks cover the extension boundary; each third-party plugin still needs its own evidence before it can be called production-certified.

Cross-connector crash, replay, DLQ, state, and sink-commit evidence is tracked in [reliability-certification.md](./reliability-certification.md).

## Scope

The current kit checks:

- connector descriptor exists and is registered
- maturity is `production`
- typed config schema is present
- descriptor fields, required markers, secret flags, defaults, and connection/behavior scopes come from the same config schema
- legacy `/api/v2/plugins.metadata.required` values are derived from schema instead of authored separately
- expected secret fields are marked secret
- readiness gates have the expected status
- partial gates include evidence and remediation
- descriptor evidence references exact e2e scripts
- component docs contain an `Evidence` section with the same script references
- referenced `hack/e2e-*.sh` scripts exist
- Plugin ABI v1 constants, manifest requirements, compatibility matrix, and TypeScript SDK helpers are documented

## Evidence Freshness Manifest

Production source/sink evidence is recorded in
`internal/etl/server/evidence/connector-evidence.json`. Each record carries the
certified commit and image, dependency versions, execution window, expiry,
scripts, and named cases. The descriptor API exposes the same metadata in the
`evidence_metadata` object on the `e2e_evidence` readiness gate.

The checked-in records are `verified: true` only because every listed script
and required case was executed against the recorded commit and image. An
unverified record is reported as `partial` / `production_with_review` without
changing connector maturity. Missing or malformed records are `missing`, and
an expired verified record is `partial`.

Run the structural checker from the repository root:

```sh
./hack/check-connector-evidence.sh
```

For a release gate, require every record to be verified and bind the manifest
to the exact build/image. The command exits non-zero for unverified or expired
records in strict mode:

```sh
./hack/check-connector-evidence.sh \
  -strict \
  -commit "$(git rev-parse HEAD)" \
  -image "$OPENETL_IMAGE_DIGEST"
```

Use `-now <RFC3339>` in tests or incident review to reproduce an expiry
decision deterministically. The manifest checker validates script paths and
does not treat the mere existence of an e2e script as a successful run.

Pull requests run the structural check so malformed or incomplete records are
reported without pretending that external connector services are available.
Pushes to `main` and both release workflows run the strict check against the
current source revision. A certified revision may have descendants only when
they change the evidence manifest or these certification documents; runtime,
connector, script, and workflow changes require a fresh certification run.
Image binding is checked when the release environment supplies a certified
image digest.

Latest checked-in certification (2026-08-08 UTC):

- source commit: `4d16ff318a7583b8c9e51dd95cfcc0e940eb8a80`
- image: `sha256:876ae664e462e695ef0682b523157a1943caceac98c4c77e1c361bc57fa774d2`
- environment: Linux/arm64 image, Podman `5.8.2`, Go `1.24.13`
- dependency set: MySQL `8.0.46`, PostgreSQL `16.14`, ClickHouse `24.3.18.7`, Redpanda `24.1.1`, Doris `2.1.11`, MinIO `RELEASE.2024-07-16T23-46-41Z`
- result: 13 unique scripts passed; all 14 production source/sink records are verified through their per-record `expires_at`

| Script | UTC window | Result |
| --- | --- | --- |
| `hack/e2e.sh` | 07:15:45-07:15:49 | passed |
| `hack/e2e-http-source.sh` | 07:15:50-07:15:53 | passed |
| `hack/e2e-mysql-postgres.sh` | 07:16:04-07:16:12 | passed |
| `hack/e2e-cdc-mysql.sh` | 07:16:12-07:16:16 | passed |
| `hack/e2e-cdc-postgres.sh` | 07:16:17-07:16:29 | passed |
| `hack/e2e-snapshot-cdc.sh` | 07:16:30-07:16:36 | passed |
| `hack/e2e-clickhouse.sh` | 07:16:37-07:16:42 | passed |
| `hack/e2e-snapshot-cdc-clickhouse.sh` | 07:16:43-07:17:57 | passed |
| `hack/e2e-kafka.sh` | 07:17:57-07:18:34 | passed |
| `hack/e2e-kafka-raw-ods.sh` | 07:18:36-07:18:47 | passed |
| `hack/e2e-debezium-mysql.sh` | 07:18:48-07:19:16 | passed |
| `hack/e2e-s3-minio.sh` | 07:19:17-07:20:00 | passed |
| `hack/e2e-doris.sh` | 07:20:01-07:21:59 | passed |

The certified production connector set is:

| Area | Connectors | Evidence |
| --- | --- | --- |
| MySQL | `mysql_batch`, `mysql_cdc`, `mysql_snapshot_cdc`, `mysql` sink | `hack/e2e.sh`, MySQL CDC/batch e2e, Debezium MySQL e2e |
| ClickHouse | `clickhouse` sink | ClickHouse CDC/autocreate/snapshot+CDC e2e |
| Kafka | `kafka` source/sink | Kafka source/sink, raw ODS, Debezium, wide-table e2e |
| S3/File | `file` source, `file_sink`, `s3` sink | file smoke e2e and S3 MinIO replay/outage e2e |
| HTTP | `http` source | pagination/auth-header e2e plus typed schema/sample preflight |
| PostgreSQL | `postgres` / `postgresql` sink aliases | MySQL batch and CDC to PostgreSQL e2e |
| Doris | `doris` sink | Stream Load/upsert, outage, DLQ, replay, and schema-drift e2e |

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
go test ./internal/etl/server -run 'Test(ConnectorDescriptorConfigContractMatchesSchemaExactly|PluginMetadataRequiredFieldsAreDerivedFromSchema|ConnectorCertificationKitProductionSet)' -count=1
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
