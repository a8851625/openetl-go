# PR-0-C1 acceptance evidence — 2026-11-26

Round: 3/5. Profile: standalone control plane and shared SQLite/MySQL/PostgreSQL metadata.

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| current source/transform/sink/DLQ/DAG references, stable IDs and no config secrets | `Test*Conformance/ConnectionReferences`; `TestConnectionReferenceAPIUsesCurrentPersistedSpecs`; real UI JSON | pass | historical versions intentionally excluded |
| delete/type change conflicts and dereference release | same conformance and real API/UI run; HTTP 409 with affected endpoints | pass | unreadable legacy specs require repair/key |
| cross-pool create/update vs delete/type mutation is atomic | `TestConnectionReferenceTransactionsAcrossBackends`: 12 races per backend using independent SQL pools; outer transaction rollback test | pass | raw offline maintenance can preserve damaged legacy data |
| encrypted restart and portable backup fidelity | `TestBackupRestoreConformance/encrypted_connection_references_after_restore`; keyed/unkeyed reopen across all backends; real process restart | pass | coordination row is operational, not backup payload |
| runtime integration | `hack/e2e-connection-references.sh`, actual image `2ca33dd33a4a67b587837260eafbe9bddd355963767f28f3651c6d873b5b01fe`, 9 PASS, exit 0; `evidence/references-ui.json` | pass | working-tree increment image, not final release candidate |
| concurrency/regression | Go 1.24.13 linux/arm64 containers: `go test -race -count=1 ./internal/etl/storage ./internal/etl/storage/sqlstore`, `./internal/etl/storage/backup`, `./internal/etl/server`, all exit 0 | pass | dedicated MySQL/PG DSNs supplied for storage and separate backup databases |
| UI/schema/assets | `npm run typecheck`, `npm run lint`, `npm run build`, OpenAPI YAML check via Go yaml.v3, container build, diff check — exit 0 | pass | existing 29 lint warnings/chunk warning |

Raw successful Go outputs and UI result are in `evidence/references-*.txt/json`. UI scripts/logs and reviewed screenshot remain in `/private/tmp/openetl-p1-refs-ui-r4`. Earlier failed attempts remain separate: host-side fsnotify injection, incorrect update route, and a YAML fixture left on disk recreating its pipeline after restart. The successful run emits filesystem events inside the VM and removes only its own fixture. Initial server regression failures were intentional damaged-metadata fixtures rejected by the new normal-write guard; those fixtures now use offline fidelity/corruption setup and retain the original version/checkpoint assertions.

Change inventory:

- `storage/connection_references.go`, `sqlstore/connection_references.go`: config-free spec identity extraction and a shared database transaction guard.
- `storage/sqlstore/store.go`, `mysql/mysql.go`, `postgres/postgres.go`: additive migration 25; current/version/checkpoint transactions, connection mutation/delete and pipeline delete use the guard. Existing transaction contexts are reused.
- `storage/adapters.go`, `sqlstore/backup.go`: cipher policy forwarding, optional reference capability and fidelity restore coordination.
- `storage/connection_references_test.go`, `conformance_test.go`, `conformance_backends_test.go`, `backup/conformance_test.go`: three backend reference/race/rollback/restart/fidelity acceptance checks.
- `server/connection_reference_api.go`, `connection_reference_api_test.go`, `connection_catalog.go`, `connection_refs.go`, `server.go`: authoritative read API, structured 409 and existing lifecycle compensation.
- `server/legacy_metadata_fixture_test.go`, `spec_crypto_recovery_test.go`, `restore_visibility_test.go`: preserve pre-upgrade corruption and missing-reference recovery tests without bypassing normal runtime writes.
- `web/src/ConnectionsPage.tsx`, `components/shared/confirm-dialog.tsx`, `i18n.ts`: impact preview, repair links, loading/unknown/conflict blocking, localized explanations; other dialog callers retain defaults.
- rebuilt `resource/public/index.html` and JS/CSS assets: production UI matches the source.
- `hack/e2e-connection-references.sh`, `connection_reference_ui_e2e.py`, shared `health_ui_e2e.py`: isolated encrypted/API/YAML/UI/restart fixtures, exact result checks and own-resource cleanup.
- API/OpenAPI/runbook, roadmap/iteration plan/tasks, changelogs and these evidence files: behavior and acceptance trace.

Regression surface: current/version/checkpoint atomicity; secret storage and key rotation; lifecycle compensation; backup restore; same-name pipelines; unchanged/inline connection configs; other confirmation dialogs. Affected full Go packages and the real encrypted YAML/API/UI run passed. No connector maturity or delivery semantics upgrade.

Verdict: DELIVERABLE
