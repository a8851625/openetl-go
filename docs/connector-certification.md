# Connector Certification Test Kit

This kit turns connector maturity claims into executable checks. It is focused on the first production-candidate set: MySQL, ClickHouse, Kafka, S3, and File.

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

The first certified set is:

| Area | Connectors | Evidence |
| --- | --- | --- |
| MySQL | `mysql_batch`, `mysql_cdc`, `mysql_snapshot_cdc`, `mysql` sink | `hack/e2e.sh`, MySQL CDC/batch e2e, Debezium MySQL e2e |
| ClickHouse | `clickhouse` sink | ClickHouse CDC/autocreate/snapshot+CDC e2e |
| Kafka | `kafka` source/sink | Kafka source/sink, raw ODS, Debezium, wide-table e2e |
| S3/File | `file` source, `file_sink`, `s3` sink | file smoke e2e and S3 MinIO replay/outage e2e |

## Running

Run the descriptor/doc certification checks:

```sh
go test ./internal/etl/server -run TestConnectorCertificationKitProductionSet -count=1
```

Run the main behavioral evidence used by this kit:

```sh
bash hack/e2e-s3-minio.sh
bash hack/e2e-kafka.sh
bash hack/e2e-clickhouse.sh
bash hack/e2e-cdc-mysql.sh
```

Use `E2E_SKIP_BUILD=1` only after rebuilding `openetl-go-etl:dev` from the current tree.

## Rules For New Production Connectors

A connector should not be marked `production` until it has descriptor metadata, typed schema, readiness gates, component docs, and at least one repeatable e2e or certification script. Partial gates are allowed only when the descriptor includes concrete remediation and the public maturity text describes the operator review boundary.
