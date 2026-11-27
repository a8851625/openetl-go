# P5-H1 acceptance evidence — 2026-11-26

Round: 2/5. Profile: standalone API + production UI.

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| open breaker / restore_failed / unknown DLQ are actionable and consistent | `health_contract_test.go`; `npm test` 9/9; `evidence/health-ui.json` three states × four views | pass | none |
| recovered current signals return to healthy | real MySQL schema fault + replay, SQLite count outage + restore; two recovery checks in JSON | pass | historical counters remain diagnostic |
| shared thresholds / stable ID metrics | frontend contract tests and API health contract tests | pass | legacy metrics retain conservative fallback |
| lifecycle concurrency and checkpoint safety | Go 1.24.13 linux/arm64: `go test -race -count=1 ./internal/etl/server ./internal/etl/telemetry`, exit 0; `go test -race -count=1 ./internal/etl/pipeline`, exit 0 | pass | raw command results in evidence/*.txt |
| UI checks and built assets | `npm test`, `npm run typecheck`, `npm run lint`, `npm run build`, all exit 0; Docker-format container build exit 0 | pass | 29 pre-existing ESLint warnings and existing chunk-size warning |
| final diff | `git diff --check`, exit 0 | pass | unrelated starting changes remain unstaged |

Actual UI command:

```sh
CONTAINER_CLI=podman CONTAINER_HOST=<podman machine unix socket> \
  E2E_SKIP_BUILD=1 \
  E2E_IMAGE=03c991b9a2cf2ebc731e870ba7b490e5437ce3110cae852216af819c491691c5 \
  E2E_OUTPUT_DIR=/private/tmp/openetl-p1-health-evidence-r5 \
  ./hack/e2e-health-ui.sh
# 14 PASS, exit 0. Window: 2026-11-25T17:31:41Z–17:32:48Z.
```

The image was built from the repair working tree (`986200f` plus the recorded cooldown prerequisite and starting user changes), not from a clean release commit. This is H1 evidence; GA-C1 must certify the final candidate separately. Raw browser scripts, application logs, and a reviewed screenshot remain in the output directory.

Earlier attempts were failures, not evidence of success: a missing breaker test config, a default cooldown exhausting the commit deadline (the runtime prerequisite repaired here), and host-side SQLite WAL injection across the macOS/VM lock boundary. Fault injection now uses a separate SQLite client container in the same VM kernel. Two initial race runs failed on an incorrect new test fixture; the corrected full pipeline run passed. The original read-only server workspace was replaced by an isolated writable test worktree.

Change inventory (all serve the criteria above):

- `telemetry/health.go`, `health_test.go`, `telemetry.go`: unknown delivery signal, current-state aggregation, typed threshold snapshot.
- `server/server.go`, `health_contract_test.go`: same server decision in metrics and health; restoration/open/unknown API tests.
- `pipeline/pipeline.go`, `circuit_breaker.go`, `circuit_recovery_test.go`: bounded cooldown prerequisite; fresh commit deadline and cancelled-wait checkpoint test.
- `web/src/lib/pipeline-health.ts`, `types.ts`: one derived health contract, stable ID matching, current issues.
- `web/src/main.tsx`, `pages/DashboardPage.tsx`, `IssuesPage.tsx`, `pipelines/PipelinesPage.tsx`, `PipelineDetailPage.tsx`: existing consumer alignment.
- `components/shared/pipeline-health-badge.tsx`, `i18n.ts`: visible unknown/restore/breaker states and localized explanations.
- `web/package.json`, `web/tests/load-typescript.cjs`, `pipeline-health.test.cjs`, `.github/workflows/_gate.yml`: executable frontend contracts in the aggregate gate.
- `resource/public/index.html` and replaced JS/CSS assets: rebuilt production UI.
- `hack/e2e-health-ui.sh`, `health_ui_e2e.py`: isolated real API/browser faults, strict assertion results and cleanup.
- `docs/etl-api.md`, roadmap/iteration index/spec/plan/tasks, this evidence and JSON/text records, both changelogs: contract and acceptance trace.

Regression surface: Stop during an admitted sink/DLQ commit, cancellation during cooldown, DLQ durability/checkpoint fencing, custom health thresholds, duplicate display names and historical error counters. Full affected Go packages, nine frontend contracts and the real container recovery run cover these surfaces. No persistence format or connector maturity change.

Verdict: DELIVERABLE
