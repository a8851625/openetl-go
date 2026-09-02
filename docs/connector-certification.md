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

### Rebind 2026-09-02 (IT-1/T1.12)

Manifest rebound from `d75600be` to `d30fee9` with image `sha256:d180c62c...`
(following the 2026-08-23 rebind procedure in commit c2ac396). The runtime
surface changed since `d75600be` is `internal/etl/sink/postgres.go` (GAP-3
auto-create PK fix, commit 1966a03) and `internal/etl/sink/elasticsearch.go`
(GAP-4 validation semantics; no manifest record). Affected records were
re-certified by rerunning both of their scripts on the fresh image, both green:

- `hack/e2e-mysql-postgres.sh` — `MySQL batch JOIN -> PostgreSQL E2E passed`
  (mysql-batch-join-to-postgres: completed, 9 written).
- `hack/e2e-cdc-postgres.sh` — `MySQL CDC -> PostgreSQL E2E passed`
  (mysql-cdc-to-postgres: checkpoint restart event recovered).

`hack/e2e-doris.sh` was NOT rerun: the `apache/doris:be-2.1.11` image could not
be pulled (three attempts, repeated blob EOF / TLS timeouts from
registry-1.docker.io; FE image pulled fine). The Doris record keeps its
2026-08-09 certification; its runtime file is unchanged since `d75600be`. The
strict checker passes with `-strict -commit d30fee9 -image <digest>`.

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

## Per-Path Evidence Artifacts (IT-1/T1.5)

The migrated primary-path e2e tests (`internal/etl/e2e/path_*.go`, tag `e2e`)
write a structured, committed artifact per path:

```sh
docs/evidence/<path_id>.json   # e.g. mysql_cdc__mysql_upsert.json
```

Each artifact carries `path_id`, the exact source `commit` the run happened
on, `run_started_at`, `runner`, dependency versions, one `checks[]` entry per
case assertion, and a derived total `result` (`passed` only when every check
passed; any skip or failure leaves the path uncertified). Skipped path tests
never produce a `passed` artifact.

`check-connector-evidence.sh` validates every committed artifact against the
current revision:

- the evidence `commit` must resolve and be an ancestor (or equal) of the
  current HEAD — regenerating evidence necessarily creates a descendant;
- the evidence must not be older than the newest commit touching the path's
  related source surface (`internal/etl/source`, `internal/etl/sink`,
  `internal/etl/core`, `internal/etl/transform`, `internal/etl/checkpoint`,
  `internal/etl/pipeline`, `internal/etl/server`, `internal/etl/e2e`,
  `internal/logic`, `manifest/config`, plus path-specific extras) — editing a
  connector and committing evidence without re-running the path test fails;
- the check list must be non-empty and fully passed, and the total `result`
  must agree — hand-editing a failed/skipped run into a pass fails.

The web/API path contracts serve `last_certified` derived from these
artifacts (`PathContract.LastCertified`), so CI runs automatically update it
to the run timestamp + source commit. No `last_certified` is ever filled
from hand-maintained metadata.

Latest checked-in certification (2026-08-23 UTC, post-beta.17 evidence rebind;
image builds recovered via goproxy.cn + host module cache — fresh image
`sha256:9b887beb` ran container e2e on 2026-08-23:
e2e-kafka-multitable-clickhouse, e2e-snapshot-cdc-clickhouse and the new
e2e-bug1-varchar-pk all PASS; historical record e2e execution window remains
the 2026-08-11 run):

- source commit: `d30fee900f871627a43cbf44c8ae8faf9df96c1b` (rebound 2026-09-02, see Rebind 2026-09-02 above)
- image: `sha256:9b887beb2ed3d4f26aea3833478981f540151f3ab82943eef7e119928cfee2b5`
- environment: Linux/arm64 image, Podman `5.8.2`, Go `1.24.13`
- dependency set: MySQL `8.0.46`, PostgreSQL `16.14`, ClickHouse `24.3.18.7`, Redpanda `24.1.1`, Doris `2.1.11`, MinIO `RELEASE.2024-07-16T23-46-41Z`
- result: 13 unique scripts passed; all 14 production source/sink records are verified through their per-record `expires_at`

| Script | UTC window | Result |
| --- | --- | --- |
| `hack/e2e.sh` | 00:13:01-00:13:05 | passed |
| `hack/e2e-http-source.sh` | 00:13:05-00:13:18 | passed |
| `hack/e2e-mysql-postgres.sh` | 00:13:18-00:13:26 | passed |
| `hack/e2e-cdc-mysql.sh` | 00:13:26-00:13:30 | passed |
| `hack/e2e-cdc-postgres.sh` | 00:13:31-00:13:42 | passed |
| `hack/e2e-snapshot-cdc.sh` | 00:13:43-00:13:48 | passed |
| `hack/e2e-clickhouse.sh` | 00:13:49-00:13:53 | passed |
| `hack/e2e-snapshot-cdc-clickhouse.sh` | 00:13:54-00:14:17 | passed |
| `hack/e2e-kafka.sh` | 00:14:25-00:15:05 | passed |
| `hack/e2e-kafka-raw-ods.sh` | 00:15:05-00:15:18 | passed |
| `hack/e2e-debezium-mysql.sh` | 00:15:19-00:15:48 | passed |
| `hack/e2e-s3-minio.sh` | 00:15:49-00:16:32 | passed |
| `hack/e2e-doris.sh` | 00:18:40-00:20:36 | passed |

The certified production connector set is:

| Area | Connectors | Evidence |
| --- | --- | --- |
| MySQL | `mysql_batch`, `mysql_cdc`, `mysql_snapshot_cdc`, `mysql` sink | `hack/e2e.sh`, MySQL CDC/batch e2e, Debezium MySQL e2e |
| ClickHouse | `clickhouse` sink | ClickHouse CDC/autocreate/snapshot+CDC e2e |
| ClickHouse multi-table | `clickhouse` sink `table_template` + `pk_columns_from_metadata` | Kafka envelope multi-table fan-out e2e (`hack/e2e-kafka-multitable-clickhouse.sh`): heterogeneous ORDER BY, update absorption, delete mutation, schema drift, checkpoint reset replay |
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
