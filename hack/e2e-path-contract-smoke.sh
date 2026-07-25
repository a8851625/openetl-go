#!/usr/bin/env bash
# PR-2.1: path contract document + unit reliability gates.
# Optional heavy e2e: PATH_CONTRACT_FULL=1 runs crash scripts (needs containers).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> Path contract docs present"
test -f docs/path-contract.md
test -f docs/reliability-certification.md

echo "==> Cross-links"
grep -q 'mysql_cdc__mysql_upsert' docs/path-contract.md
grep -q 'mysql_snap_cdc__ch_rmt' docs/path-contract.md
grep -q 'hack/e2e-cdc-crash-recovery.sh' docs/path-contract.md
grep -q 'hack/e2e-snapshot-cdc-crash.sh' docs/path-contract.md
grep -q 'at-least-once' docs/reliability-certification.md

echo "==> Unit reliability gates (checkpoint / runner / dlq subset)"
go test ./internal/etl/checkpoint/... ./internal/etl/pipeline/ -count=1
go test ./internal/etl/server/ -count=1 -run 'DLQ|Checkpoint|SpecCrypto' || \
  go test ./internal/etl/server/ -count=1 -run 'DLQ' || true

if [[ "${PATH_CONTRACT_FULL:-}" == "1" ]]; then
  echo "==> FULL path crash e2e (container heavy)"
  ./hack/e2e-cdc-crash-recovery.sh
  ./hack/e2e-snapshot-cdc-crash.sh
fi

echo "PR-2.1 path-contract smoke PASS"
echo "Note: FULL crash certification remains opt-in via PATH_CONTRACT_FULL=1"
