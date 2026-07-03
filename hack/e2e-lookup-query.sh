#!/bin/sh

# E2E for lookup mode=query async SQL dimension lookups.
#
# Covers: happy path with batch backpressure metrics, lookup miss -> DLQ,
# query timeout -> DLQ, MySQL lock-wait transient error -> DLQ, and replay
# after the temporary lock is released.

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
MYSQL_CONTAINER="etl-mysql-source"
APP_CONTAINER="etl-openetl-go-lookup-query"
APP_PORT=8029
MYSQL_DB="dzh3136_go"
PIPE_DIR="$ROOT_DIR/data-lookup-query/pipes"

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 90 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 1
  done
  return 1
}

wait_mysql_healthy() {
  i=0
  while [ "$i" -lt 90 ]; do
    status="$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
    if [ "$status" = "healthy" ]; then return 0; fi
    i=$((i + 1)); sleep 2
  done
  "$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER"
  return 1
}

wait_pipeline_records() {
  name="$1"; expected="$2"; i=0
  while [ "$i" -lt 90 ]; do
    body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines")"
    echo "$body" | grep "\"name\":\"$name\"" | grep "\"records_written\":$expected" >/dev/null 2>&1 && return 0
    i=$((i + 1)); sleep 1
  done
  echo "$body"
  return 1
}

wait_pipeline_dlq() {
  name="$1"; expected="$2"; i=0
  while [ "$i" -lt 90 ]; do
    body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines")"
    echo "$body" | grep "\"name\":\"$name\"" | grep "\"records_dlq\":$expected" >/dev/null 2>&1 && return 0
    i=$((i + 1)); sleep 1
  done
  echo "$body"
  return 1
}

transform_metric() {
  pipeline="$1"; metric="$2"
  curl -fsS "http://127.0.0.1:$APP_PORT/metrics" 2>/dev/null | \
    grep "etl_transform_metric_total{pipeline=\"$pipeline\",.*transform=\"lookup\",metric=\"$metric\"}" | \
    awk -F' ' '{print $NF}' | tail -1
}

wait_metric_ge() {
  pipeline="$1"; metric="$2"; expected="$3"; i=0
  while [ "$i" -lt 30 ]; do
    value="$(transform_metric "$pipeline" "$metric" || true)"
    if [ "${value:-0}" -ge "$expected" ]; then
      echo "$value"
      return 0
    fi
    i=$((i + 1)); sleep 1
  done
  echo "${value:-0}"
  return 1
}

wait_dlq_contains() {
  pipeline="$1"; contains="$2"; pattern="$3"; i=0
  while [ "$i" -lt 30 ]; do
    body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/dlq/$pipeline?contains=$contains" 2>/dev/null || true)"
    if echo "$body" | grep -E "$pattern" >/dev/null 2>&1; then
      echo "$body"
      return 0
    fi
    i=$((i + 1)); sleep 1
  done
  echo "$body"
  return 1
}

write_pipe() {
  name="$1"
  input="$2"
  query="$3"
  extra="$4"
  cat >"$PIPE_DIR/$name.yaml" <<EOF
name: "$name"
source:
  type: file
  config:
    path: "/app/testdata/files/$input"
    format: "json"

transforms:
  - type: lookup
    config:
      mode: "query"
      dsn: "sync_user:sync_password_123@tcp(host.docker.internal:13306)/$MYSQL_DB?parseTime=true&timeout=5s&readTimeout=5s"
      query: "$query"
      join_key: "user_id"
      fields: ["tier", "region"]
      timeout_seconds: 2
      concurrency: 2
      max_in_flight: 1
      max_retries: 0
      retry_base_ms: 50
$extra

sink:
  type: file_sink
  config:
    output_dir: "/app/data/output/$name"
    format: "jsonl"
    prefix: "out_"

batch_size: 2
checkpoint_interval_sec: 1
backpressure_buffer: 10
retry:
  max_attempts: 1
  initial_interval_ms: 100
  max_interval_ms: 1000
dlq:
  enable: true
EOF
}

if [ "${E2E_SKIP_BUILD:-0}" = "1" ]; then
  echo "==> Skip image build (E2E_SKIP_BUILD=1, using $IMAGE)"
else
  echo "==> Build image"
  "$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile .
fi

echo "==> Start MySQL source"
if has_compose; then
  compose -f docker-compose.dev.yml up -d mysql-source
else
  echo "$CONTAINER_CLI compose is required for lookup-query e2e" >&2
  exit 2
fi
wait_mysql_healthy

echo "==> Prepare lookup dimension table"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 "$MYSQL_DB" -e "
SET GLOBAL innodb_lock_wait_timeout=1;
DROP TABLE IF EXISTS dim_users_lookup_query;
CREATE TABLE dim_users_lookup_query (
  id VARCHAR(64) PRIMARY KEY,
  tier VARCHAR(32),
  region VARCHAR(32)
);
INSERT INTO dim_users_lookup_query (id, tier, region) VALUES
  ('u1', 'vip', 'east'),
  ('u2', 'standard', 'west'),
  ('locked', 'locked-tier', 'south');
"

echo "==> Reset ETL data"
if [ -d data-lookup-query ]; then
  "$CONTAINER_CLI" run --rm -v "$ROOT_DIR/data-lookup-query:/cleanup" docker.io/library/alpine:3.19 sh -c 'rm -rf /cleanup/*'
fi
rm -rf data-lookup-query
mkdir -p "$PIPE_DIR" data-lookup-query/output data-lookup-query/checkpoint data-lookup-query/dlq logs
chmod -R a+rwX data-lookup-query
chmod a+rwX logs 2>/dev/null || true

echo "==> Generate lookup query specs"
write_pipe "lookup-query-happy" "lookup-query-input.jsonl" "SELECT tier, region FROM dim_users_lookup_query WHERE id={{.user_id}} AND SLEEP(0.2)=0" ""
write_pipe "lookup-query-miss-dlq" "lookup-query-miss.jsonl" "SELECT tier, region FROM dim_users_lookup_query WHERE id={{.user_id}}" "      on_miss: \"dlq\""
write_pipe "lookup-query-timeout" "lookup-query-timeout.jsonl" "SELECT tier, region FROM dim_users_lookup_query WHERE id={{.user_id}} AND SLEEP(3)=0" "      on_refresh_error: \"error\""
write_pipe "lookup-query-lock-wait" "lookup-query-locked.jsonl" "SELECT tier, region FROM dim_users_lookup_query WHERE id={{.user_id}} FOR UPDATE" "      on_refresh_error: \"error\""

echo "==> Hold MySQL row lock for lock-wait transient error"
"$CONTAINER_CLI" exec -d "$MYSQL_CONTAINER" sh -c "mysql -uroot -proot123456 $MYSQL_DB -e \"SET autocommit=0; BEGIN; SELECT * FROM dim_users_lookup_query WHERE id='locked' FOR UPDATE; DO SLEEP(12); COMMIT;\""
sleep 2

echo "==> Run lookup query pipelines"
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p "$APP_PORT:8001" \
  -v "$PIPE_DIR:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-lookup-query:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE" >/dev/null

wait_http "http://127.0.0.1:$APP_PORT/api/v2/health"

echo "==> Verify happy path output and backpressure metric"
wait_pipeline_records "lookup-query-happy" 2
grep -R '"tier":"vip"' data-lookup-query/output/lookup-query-happy >/dev/null
grep -R '"region":"west"' data-lookup-query/output/lookup-query-happy >/dev/null
backpressure="$(wait_metric_ge "lookup-query-happy" "backpressure_waits" 1)"
echo "backpressure_waits metric = ${backpressure:-0}"

echo "==> Verify lookup miss DLQ"
wait_pipeline_dlq "lookup-query-miss-dlq" 1
miss_dlq="$(wait_dlq_contains "lookup-query-miss-dlq" "missing" "no dimension match")"
misses="$(wait_metric_ge "lookup-query-miss-dlq" "miss_dlq" 1)"
echo "miss_dlq metric = ${misses:-0}"

echo "==> Verify SQL timeout DLQ"
wait_pipeline_dlq "lookup-query-timeout" 1
timeout_dlq="$(wait_dlq_contains "lookup-query-timeout" "u1" "query failed")"
timeouts="$(wait_metric_ge "lookup-query-timeout" "timeouts" 1)"
echo "timeouts metric = ${timeouts:-0}"

echo "==> Verify lock-wait transient error DLQ"
wait_pipeline_dlq "lookup-query-lock-wait" 1
lock_dlq="$(wait_dlq_contains "lookup-query-lock-wait" "locked" "Lock wait timeout|try restarting transaction|query failed")"

echo "==> Wait for lock release and replay lock-wait DLQ"
sleep 12
replay="$(curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/dlq/lookup-query-lock-wait/replay?contains=locked")"
echo "$replay"
echo "$replay" | grep '"replayed":1' >/dev/null
grep -R '"tier":"locked-tier"' data-lookup-query/output/lookup-query-lock-wait >/dev/null
dlq_after="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/dlq/lookup-query-lock-wait?contains=locked")"
if ! echo "$dlq_after" | grep '"items":\[\]' >/dev/null; then
	echo "lock-wait DLQ record was not deleted after replay" >&2
	exit 1
fi

echo "Lookup query async I/O E2E passed"
