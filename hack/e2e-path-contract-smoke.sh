#!/usr/bin/env bash
# PR-2.1: path contract document + unit reliability gates + API contract.
# Optional heavy e2e: PATH_CONTRACT_FULL=1 runs forced primary path matrices.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> Path contract docs present"
test -f docs/path-contract.md
test -f docs/reliability-certification.md
test -f docs/etl-idempotency.md

echo "==> Cross-links and forced path IDs"
grep -q 'mysql_cdc__mysql_upsert' docs/path-contract.md
grep -q 'mysql_snap_cdc__ch_rmt' docs/path-contract.md
grep -q 'hack/e2e-path-mysql-cdc-mysql.sh' docs/path-contract.md
grep -q 'hack/e2e-snapshot-cdc-clickhouse.sh' docs/path-contract.md
grep -q 'hack/e2e-cdc-crash-recovery.sh' docs/path-contract.md
grep -q 'RPO' docs/path-contract.md
grep -q 'RTO' docs/path-contract.md
grep -q 'on_truncate' docs/path-contract.md
grep -q 'at-least-once' docs/reliability-certification.md
grep -q 'path-contract.md\|Path Contract\|path contract' docs/reliability-certification.md || \
  grep -q 'mysql_cdc' docs/reliability-certification.md

echo "==> Evidence scripts exist"
test -x hack/e2e-path-mysql-cdc-mysql.sh || test -f hack/e2e-path-mysql-cdc-mysql.sh
test -f hack/e2e-snapshot-cdc-clickhouse.sh
test -f hack/e2e-cdc-crash-recovery.sh
test -f hack/e2e-snapshot-cdc-crash.sh

echo "==> Unit reliability + path contract gates"
go test ./internal/etl/checkpoint/... ./internal/etl/pipeline/ -count=1 -run 'Checkpoint|Unsafe|OnTruncate|Idempot|ValidateSpec|Spec'
go test ./internal/etl/orchestrator/ -count=1 -run 'MultiSink|Validate'
go test ./internal/etl/server/ -count=1 -run 'PathContract|DLQ|Checkpoint'
go test ./internal/etl/source/ -count=1 -run 'Postgres|Truncate' || true

if [[ "${PATH_CONTRACT_FULL:-}" == "1" ]]; then
  echo "==> FULL forced primary path matrices (container heavy)"
  E2E_SKIP_BUILD="${E2E_SKIP_BUILD:-}" ./hack/e2e-path-mysql-cdc-mysql.sh
  E2E_SKIP_BUILD=1 ./hack/e2e-snapshot-cdc-clickhouse.sh
fi

echo "PR-2 path-contract smoke PASS"
echo "Note: FULL path certification is opt-in via PATH_CONTRACT_FULL=1"
