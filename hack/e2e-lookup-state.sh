#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

IMAGE="openetl-go-etl:dev"
REDPANDA_CONTAINER="etl-redpanda"
MYSQL_CONTAINER="etl-mysql-source"
CH_CONTAINER="etl-clickhouse"
APP_CONTAINER="etl-openetl-go-lookup-state"
APP_PORT="8023"
PIPELINE="kafka-lookup-state-clickhouse"

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 90 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

wait_pipeline_running() {
  i=0
  while [ "$i" -lt 60 ]; do
    body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines")"
    echo "$body" | grep "\"name\":\"$PIPELINE\"" | grep '"status":"running"' >/dev/null 2>&1 && return 0
    i=$((i + 1))
    sleep 1
  done
  echo "$body"
  return 1
}

run_lookup_app() {
  docker run -d \
    --add-host host.docker.internal:host-gateway \
    --name "$APP_CONTAINER" \
    -p "$APP_PORT:8001" \
    -v "$ROOT_DIR/testdata/pipes-lookup-state:/app/pipes:ro" \
    -v "$ROOT_DIR/testdata:/app/testdata:ro" \
    -v "$ROOT_DIR/data-lookup-state:/app/data" \
    -v "$ROOT_DIR/logs:/app/logs" \
    "$IMAGE" >/dev/null

  wait_http "http://127.0.0.1:$APP_PORT/api/v2/health"
  wait_pipeline_running
}

if [ "${E2E_SKIP_BUILD:-0}" = "1" ]; then
  echo "==> Skip image build (E2E_SKIP_BUILD=1, using $IMAGE)"
else
  echo "==> Build image"
  docker build -t "$IMAGE" -f Dockerfile .
fi

echo "==> Start Redpanda, MySQL, and ClickHouse"
docker compose -f docker-compose.dev.yml up -d redpanda mysql-source clickhouse

echo "==> Wait Redpanda"
i=0
while [ "$i" -lt 90 ]; do
  if docker exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 2
done
docker exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null

echo "==> Wait MySQL healthy"
i=0
while [ "$i" -lt 90 ]; do
  status="$(docker inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
  if [ "$status" = "healthy" ]; then
    break
  fi
  i=$((i + 1))
  sleep 2
done
[ "$(docker inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER")" = "healthy" ]

echo "==> Wait ClickHouse HTTP"
wait_http "http://127.0.0.1:8123/ping"

echo "==> Prepare Kafka topic"
docker exec "$REDPANDA_CONTAINER" rpk topic delete orders.lookup_state >/dev/null 2>&1 || true
docker exec "$REDPANDA_CONTAINER" rpk topic create orders.lookup_state --brokers localhost:9092 >/dev/null

echo "==> Prepare MySQL dimension table"
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "
DROP TABLE IF EXISTS dim_users_lookup_state_unavailable;
DROP TABLE IF EXISTS dim_users_lookup_state;
CREATE TABLE dim_users_lookup_state (
  id BIGINT PRIMARY KEY,
  name VARCHAR(128),
  tier VARCHAR(32),
  region VARCHAR(32)
);
INSERT INTO dim_users_lookup_state (id, name, tier, region) VALUES
  (1001, 'Lookup Cached Alice', 'cached', 'east'),
  (1002, 'Lookup Cached Bob', 'cached', 'west');
"

echo "==> Prepare ClickHouse target"
docker exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --multiquery "
CREATE DATABASE IF NOT EXISTS wide;
DROP TABLE IF EXISTS wide.lookup_state_recovery;
"

echo "==> Reset ETL data"
rm -rf data-lookup-state
mkdir -p data-lookup-state/output data-lookup-state/checkpoint data-lookup-state/dlq logs
chmod -R a+rwX data-lookup-state
chmod a+rwX logs

echo "==> Run lookup-state pipeline"
docker rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
run_lookup_app

echo "==> Produce event that loads lookup cache and persists state"
cat <<'JSON' | docker exec -i "$REDPANDA_CONTAINER" rpk topic produce orders.lookup_state --brokers localhost:9092 >/dev/null
{"payload":{"op":"c","ts_ms":1710000010000,"source":{"table":"orders"},"after":{"id":41001,"user_id":1001,"amount":11.11,"_version":1}}}
JSON

i=0
while [ "$i" -lt 90 ]; do
  first_count="$(docker exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM wide.lookup_state_recovery FINAL WHERE id = 41001 AND user_tier = 'cached' AND user_region = 'east'" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "$first_count" = "1" ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "$first_count" = "1"

i=0
while [ "$i" -lt 90 ]; do
  lookup_state_keys="$(curl -fsS "http://127.0.0.1:$APP_PORT/metrics" 2>/dev/null | grep 'etl_state_keys{pipeline="kafka-lookup-state-clickhouse",node="lookup-1"}' | awk '{print $2}' | tr -d '[:space:]' || true)"
  if [ "$lookup_state_keys" != "" ] && [ "$lookup_state_keys" -ge 2 ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "${lookup_state_keys:-0}" -ge 2

echo "==> Kill app and make dimension query fail"
docker kill "$APP_CONTAINER" >/dev/null
docker rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "
RENAME TABLE dim_users_lookup_state TO dim_users_lookup_state_unavailable;
"

echo "==> Restart app and verify lookup restores cache from StateStore"
run_lookup_app

cat <<'JSON' | docker exec -i "$REDPANDA_CONTAINER" rpk topic produce orders.lookup_state --brokers localhost:9092 >/dev/null
{"payload":{"op":"c","ts_ms":1710000011000,"source":{"table":"orders"},"after":{"id":41002,"user_id":1001,"amount":22.22,"_version":1}}}
JSON

i=0
while [ "$i" -lt 90 ]; do
  restored_count="$(docker exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM wide.lookup_state_recovery FINAL WHERE id = 41002 AND user_name = 'Lookup Cached Alice' AND user_tier = 'cached' AND user_region = 'east'" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "$restored_count" = "1" ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "$restored_count" = "1"

i=0
while [ "$i" -lt 30 ]; do
  restore_successes="$(curl -fsS "http://127.0.0.1:$APP_PORT/metrics" 2>/dev/null | grep 'etl_transform_metric_total{pipeline="kafka-lookup-state-clickhouse",node="lookup-1",transform="lookup",metric="restore_success"}' | awk '{print $2}' | tr -d '[:space:]' || true)"
  if [ "$restore_successes" != "" ] && [ "$restore_successes" -ge 1 ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "${restore_successes:-0}" -ge 1

body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines")"
echo "$body"
echo "$body" | grep "\"name\":\"$PIPELINE\"" | grep '"status":"running"'
echo "$body" | grep "\"name\":\"$PIPELINE\"" | grep '"records_failed":0'

echo "Lookup StateStore crash recovery E2E passed"
