#!/bin/sh

# E2E for the enricher transform (Phase 1 "异步 I/O 维表查询增强").
#
# Covers: HTTP happy path, 429 + Retry-After retry-then-succeed, timeout → DLQ,
# and batch partial-failure routing (only failed records enter DLQ). The mock
# fixture (testdata/enricher-fixture/server.py) returns deterministic status
# codes per URL kind and tracks peak concurrency + per-key call counts.

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
FIXTURE_CONTAINER="etl-enricher-fixture"
APP_CONTAINER="etl-openetl-go-enricher"
APP_PORT=8021

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 60 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 1
  done
  return 1
}

wait_pipeline_records() {
  name="$1"; expected="$2"; i=0
  while [ "$i" -lt 60 ]; do
    body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines")"
    echo "$body" | grep "\"name\":\"$name\"" | grep "\"records_written\":$expected" >/dev/null 2>&1 && return 0
    i=$((i + 1)); sleep 1
  done
  echo "pipeline $name did not reach records_written=$expected"
  curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines"
  return 1
}

wait_pipeline_dlq() {
  name="$1"; expected="$2"; i=0
  while [ "$i" -lt 60 ]; do
    body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines")"
    echo "$body" | grep "\"name\":\"$name\"" | grep "\"records_dlq\":$expected" >/dev/null 2>&1 && return 0
    i=$((i + 1)); sleep 1
  done
  echo "pipeline $name did not reach records_dlq=$expected"
  curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines"
  return 1
}

transform_metric() {
  # transform_metric <pipeline> <metric-name> -> prints integer value
  pipeline="$1"; metric="$2"
  curl -fsS "http://127.0.0.1:$APP_PORT/metrics" 2>/dev/null | \
    grep "etl_transform_metric_total{pipeline=\"$pipeline\",.*transform=\"enricher\",metric=\"$metric\"}" | \
    awk -F' ' '{print $NF}' | tail -1
}

echo "==> Build image"
if [ "${E2E_SKIP_BUILD:-0}" != "1" ]; then
  "$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile .
fi

echo "==> Start enricher fixture"
"$CONTAINER_CLI" rm -f "$FIXTURE_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$FIXTURE_CONTAINER" \
  -p 18080:8080 \
  -v "$ROOT_DIR/testdata/enricher-fixture:/fixture:ro" \
  docker.io/library/python:3.12-alpine \
  python /fixture/server.py

wait_http "http://127.0.0.1:18080/health"

echo "==> Reset ETL data"
rm -rf data-enricher
mkdir -p data-enricher logs
chmod -R a+rwX data-enricher
chmod a+rwX logs

echo "==> Run enricher pipelines"
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p "$APP_PORT:8001" \
  -v "$ROOT_DIR/testdata/pipes-enricher:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-enricher:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:$APP_PORT/api/v2/health"

# Give the scheduler a moment to start the 4 pipelines and consume input.
sleep 3

# ── Scenario 1: happy path ────────────────────────────────────────────────
echo "==> Wait enricher-happy (3 records)"
wait_pipeline_records "enricher-happy" 3
echo "==> Verify happy output contains enrichment"
grep -R '"tier":"vip"' data-enricher/output/enricher-happy >/dev/null
grep -R '"user_id":"u1"' data-enricher/output/enricher-happy >/dev/null

# ── Scenario 2: 429 + Retry-After then success ────────────────────────────
echo "==> Wait enricher-429-retry (3 records after retry)"
wait_pipeline_records "enricher-429-retry" 3
echo "==> Verify 429 retry happened (fixture saw >1 call per key)"
# The first call per key returns 429, subsequent calls succeed → >1 call per key.
stats="$(curl -fsS http://127.0.0.1:18080/stats)"
echo "$stats"
# u1/u2 each retried once → call count >= 2 (use >= since counters may accumulate
# across repeated e2e runs in the same fixture container).
u1_calls="$(echo "$stats" | python3 -c "import sys,json; print(json.load(sys.stdin)['call_counts'].get('u1',0))")"
test "${u1_calls}" -ge 2
# Verify retries metric was incremented (>=1 retry occurred; 3 keys each retry once).
retries="$(transform_metric "enricher-429-retry" "retries")"
echo "retries metric = ${retries:-0}"
test "${retries:-0}" -ge 1
grep -R '"tier":"vip"' data-enricher/output/enricher-429 >/dev/null

# ── Scenario 3: timeout → DLQ ─────────────────────────────────────────────
echo "==> Wait enricher-timeout (3 records to DLQ)"
# Failed records are still written (unchanged, no enrichment) AND go to DLQ,
# so assert records_dlq=3 rather than records_written=0.
wait_pipeline_dlq "enricher-timeout" 3
timeouts="$(transform_metric "enricher-timeout" "timeouts")"
echo "timeouts metric = ${timeouts:-0}"
test "${timeouts:-0}" -ge 1
# Timeout output should NOT contain enrichment.
if grep -R '"tier"' data-enricher/output/enricher-timeout 2>/dev/null; then
  echo "FAIL: timeout output should not contain enrichment"; exit 1
fi

# ── Scenario 4: batch partial failure ─────────────────────────────────────
echo "==> Wait enricher-partial-failure (1 record to DLQ)"
wait_pipeline_dlq "enricher-partial-failure" 1
echo "==> Verify the failed record is the 'bad' one"
dlq_body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/dlq/enricher-partial-failure")"
echo "$dlq_body" | grep '"user_id":"bad"' >/dev/null
echo "==> Verify successful records ok1/ok2 are enriched"
grep -R '"user_id":"ok1"' data-enricher/output/enricher-partial >/dev/null
grep -R '"user_id":"ok2"' data-enricher/output/enricher-partial >/dev/null
grep -R '"tier":"vip"' data-enricher/output/enricher-partial >/dev/null

echo "Enricher E2E passed"
