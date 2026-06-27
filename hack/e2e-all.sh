#!/usr/bin/env bash
# Run all E2E tests sequentially and report results.
# Usage: ./hack/e2e-all.sh [--skip-ui]
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SKIP_UI=false
[[ "${1:-}" == "--skip-ui" ]] && SKIP_UI=true

# Ordered by dependency: basic -> connectors -> reliability -> ops -> UI
TESTS=(
  "e2e.sh|file->file, MySQL batch->file, MySQL batch->MySQL"
  "e2e-mysql-postgres.sh|MySQL batch JOIN->PostgreSQL"
  "e2e-cdc-mysql.sh|MySQL CDC->MySQL"
  "e2e-cdc-postgres.sh|MySQL CDC->PostgreSQL"
  "e2e-clickhouse.sh|MySQL CDC->ClickHouse"
  "e2e-clickhouse-autocreate.sh|ClickHouse auto-create + schema drift"
  "e2e-snapshot-cdc-clickhouse.sh|MySQL snapshot+CDC->ClickHouse"
  "e2e-doris.sh|MySQL batch->Doris Stream Load/insert"
  "e2e-snapshot-cdc.sh|MySQL snapshot+CDC->MySQL"
  "e2e-dlq.sh|DLQ list/replay/delete"
  "e2e-s3-minio.sh|S3/MinIO sink + deterministic replay"
  "e2e-http-source.sh|HTTP source pagination/auth"
  "e2e-kafka.sh|Kafka source/sink (Redpanda)"
  "e2e-kafka-raw-ods.sh|Kafka raw parser/flat_map -> lookup -> Kafka ODS"
  "e2e-debezium-mysql.sh|Debezium Kafka CDC->MySQL ODS"
  "e2e-lookup-state.sh|Kafka lookup StateStore crash recovery"
  "e2e-wide-table.sh|Kafka lookup/deduplicate/window->ClickHouse + state recovery"
  "e2e-elasticsearch.sh|Elasticsearch/OpenSearch bulk indexing + mapping DLQ"
  "e2e-crash-recovery.sh|Checkpoint crash recovery"
  "e2e-cdc-crash-recovery.sh|CDC checkpoint restart"
  "e2e-snapshot-cdc-crash.sh|Snapshot+CDC crash recovery"
  "e2e-auth-audit.sh|API token auth + audit log"
  "e2e-duplicate-spec.sh|Duplicate spec detection"
  "e2e-api-conflict.sh|Duplicate pipeline 409 conflict"
)
if [[ "$SKIP_UI" == false ]]; then
  TESTS+=("e2e-ui.sh|UI Playwright (80 tests)")
fi

PASS=0
FAIL=0
FAIL_DETAILS=()
START_TS=$(date +%s)

echo "╔═══════════════════════════════════════════════════════════╗"
echo "║         ETL E2E Test Suite — Full Coverage Report         ║"
echo "╚═══════════════════════════════════════════════════════════╝"
echo ""

for entry in "${TESTS[@]}"; do
  script="${entry%%|*}"
  desc="${entry##*|}"
  printf "  ▸ %-45s " "$desc"
  
  log_file="logs/e2e-${script}.log"
  mkdir -p logs
  
  if bash "hack/$script" >"$log_file" 2>&1; then
    echo "✅ PASS"
    PASS=$((PASS + 1))
  else
    echo "❌ FAIL (see $log_file)"
    FAIL=$((FAIL + 1))
    FAIL_DETAILS+=("$script: $desc")
  fi
  echo ""
done

END_TS=$(date +%s)
DURATION=$((END_TS - START_TS))

echo ""
echo "═══════════════════════════════════════"
echo "  Results: $PASS passed, $FAIL failed  ($(( ${#TESTS[@]} )) total)"
echo "  Duration: ${DURATION}s"
echo "═══════════════════════════════════════"

if [[ "$FAIL" -gt 0 ]]; then
  echo ""
  echo "Failed tests:"
  for d in "${FAIL_DETAILS[@]}"; do
    echo "  ❌ $d"
  done
  exit 1
fi

echo ""
echo "All E2E tests passed ✅"
