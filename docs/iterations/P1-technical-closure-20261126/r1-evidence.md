# UI-E-R1 acceptance evidence

Round: 4/5
Roadmap item: UI-E-R1
Profile/path: standalone recovery; SQLite/MySQL/PostgreSQL; DAG node capture only
Result: delivered
External UI-E research: blocked_external; no participants available

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| Stable persisted failure identity and known scope | Three-backend Recovery conformance; API scope/invalid-input/exact-int64 tests; shard/DAG capture tests; real MySQL service outage | pass | Multi-transform connection remains explicitly unknown; runtime version remains 0. |
| Replay failure, acknowledgement, deletion, TTL/eviction and restart | Recovery conformance on all three backends; completed-only SQL trigger and combined ACK/result faults; server restart tests; real outage/restart drill | pass | A crash before durable sink_acked remains at-least-once. |
| Capture metadata failure cannot break DLQ durability; result persistence failure cannot delete an unrecorded outcome | Real metadata INSERT/UPDATE fault triggers; durable file checkpoint after DLQ fallback; 503 result failure + restart cleanup without another MySQL write | pass | Missing association remains explicit, not inferred. |
| Legacy migration and v3 backup fidelity | RecoveryLegacyMigration on all three backends; v3 streaming fidelity/rollback plus v1/v2 existing fixtures; legacy JSONL inclusion; object reconciliation | pass | Keep additive tables on rollback; history is retained independently of DLQ TTL. |
| API/UI refresh and deep links | Real container 21/21; 15 frontend contracts; typecheck/lint/build; OpenAPI YAML parsed | pass | Existing lint warnings and bundle-size advisory remain; no new error. |
| Human research status | Parent UI-E remains blocked_external | pass | The user must supply 3–5 target participants for its separate acceptance. |

Commands executed against Go 1.24.13 linux/arm64 in etl-go-dev:latest:

    go test -race -count=1 -v ./internal/etl/storage ./internal/etl/storage/sqlstore ./internal/etl/storage/backup ./internal/etl/server ./internal/etl/pipeline ./internal/etl/orchestrator
    go test -race -count=1 -v ./internal/etl/server
    go test -race -count=1 -v -run "TestApplyRetention|TestLegacyJSONLBackup|TestDAGRecoveryCapture" ./internal/etl/storage ./internal/etl/orchestrator
    go vet ./internal/etl/core ./internal/etl/pipeline ./internal/etl/orchestrator ./internal/etl/storage/... ./internal/etl/server

All exit 0. The second full server run follows the final API error-classification
change. Dedicated MySQL/PostgreSQL storage DSNs and separate fresh backup databases
were supplied. Every Go source file in the isolated writable test worktree matched
the working source (comparison output: Go source differences: 0).

    cd web
    npm run typecheck && npm test && npm run lint && npm run build

Exit 0; 15 passed, 0 failed, 0 skipped; lint 0 errors / 29 existing warnings.
OpenAPI YAML parsing and git diff --check exit 0.

    CONTAINER_CLI=podman python3 hack/recovery_ui_e2e.py --cli podman --image localhost/openetl-go:openetl-p1-r4-verified --out /private/tmp/openetl-p1-r4-e2e7

Exit 0; 21 passed, 0 failed. Image ID:
2788045b496747e4671deb2ea0a989eb7fcda86fb353d69414a8b84df856c62e.
This is an increment image, not the final release candidate.

Captured outputs (trailing whitespace normalized; original logs retained in the temporary run directories) are retained in [evidence](./evidence/), including
[UI result](./evidence/recovery-ui.json) and
[retained recovery screenshot](./evidence/recovery-retained.png).
Earlier attempts exposed a missing import, an invalid regexp lint rule, incomplete
test-fixture configuration, time/hash fidelity errors and the completed-only result
error classification and an untranslated refresh label. They were corrected and the relevant checks rerun; their
failed attempts are not counted as passes.

Change inventory: core/pipeline/DAG capture preserves actual runtime scope;
SQL storage/adapters/migrations record identity and failure-atomic outcomes;
backup retains new tables and associations; API journals preview/replay and
validates destructive scopes; the existing four UI surfaces retain typed task
context and show missing/unknown objects; generated frontend assets, bilingual
API text, OpenAPI and recovery/runbook documentation match that behavior.

Regression surface: checkpoint gating, metadata-PK replay, old DLQ identity,
atomic connection reference protection, retention, backup artifacts, scheduler
and lifecycle were covered by the complete affected package race runs. No
distributed-profile or connector-maturity upgrade is claimed.

Verdict: DELIVERABLE
