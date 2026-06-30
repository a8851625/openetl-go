#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
REDPANDA_CONTAINER="etl-redpanda"
MYSQL_CONTAINER="etl-mysql-source"
CH_CONTAINER="etl-clickhouse"
REDIS_CONTAINER="etl-redis-state"
APP_CONTAINER="etl-openetl-go-wide-table"

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

wait_http_down() {
  url="$1"
  i=0
  while [ "$i" -lt 90 ]; do
    if ! curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

run_wide_app() {
  "$CONTAINER_CLI" run -d \
    --add-host host.docker.internal:host-gateway \
    --name "$APP_CONTAINER" \
    -e ETL_STATE_REDIS_ADDR=host.docker.internal:16379 \
    -p 8018:8001 \
    -v "$ROOT_DIR/testdata/pipes-wide-table:/app/pipes:ro" \
    -v "$ROOT_DIR/testdata:/app/testdata:ro" \
    -v "$ROOT_DIR/data-wide-table:/app/data" \
    -v "$ROOT_DIR/logs:/app/logs" \
    "$IMAGE" >/dev/null

  wait_http "http://127.0.0.1:8018/api/v2/health"
}

if [ "${E2E_SKIP_BUILD:-0}" = "1" ]; then
  echo "==> Skip image build (E2E_SKIP_BUILD=1, using $IMAGE)"
else
  echo "==> Build image"
  "$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile .
fi

echo "==> Start Redpanda, MySQL, and ClickHouse"
compose -f docker-compose.dev.yml up -d redpanda mysql-source clickhouse

echo "==> Start Redis state backend"
"$CONTAINER_CLI" rm -f "$REDIS_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d --name "$REDIS_CONTAINER" -p 16379:6379 docker.io/redis:7-alpine >/dev/null
i=0
while [ "$i" -lt 60 ]; do
  if "$CONTAINER_CLI" exec "$REDIS_CONTAINER" redis-cli ping 2>/dev/null | grep PONG >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
"$CONTAINER_CLI" exec "$REDIS_CONTAINER" redis-cli ping | grep PONG >/dev/null
"$CONTAINER_CLI" exec "$REDIS_CONTAINER" redis-cli FLUSHDB >/dev/null

echo "==> Wait Redpanda"
i=0
while [ "$i" -lt 90 ]; do
  if "$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 2
done
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null

echo "==> Wait MySQL healthy"
i=0
while [ "$i" -lt 90 ]; do
  status="$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
  if [ "$status" = "healthy" ]; then
    break
  fi
  i=$((i + 1))
  sleep 2
done
[ "$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER")" = "healthy" ]

echo "==> Wait ClickHouse HTTP"
wait_http "http://127.0.0.1:8123/ping"

echo "==> Prepare Kafka topic"
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic delete orders.cdc >/dev/null 2>&1 || true
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic delete orders.clickhouse_failure >/dev/null 2>&1 || true
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic delete orders.lookup_miss >/dev/null 2>&1 || true
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic delete orders.lookup_failure >/dev/null 2>&1 || true
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic create orders.cdc --brokers localhost:9092 >/dev/null
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic create orders.clickhouse_failure --brokers localhost:9092 >/dev/null
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic create orders.lookup_miss --brokers localhost:9092 >/dev/null
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic create orders.lookup_failure --brokers localhost:9092 >/dev/null

echo "==> Prepare MySQL dimension table"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "
CREATE TABLE IF NOT EXISTS dim_users (
  id BIGINT PRIMARY KEY,
  name VARCHAR(128),
  tier VARCHAR(32),
  region VARCHAR(32)
);
DELETE FROM dim_users WHERE id IN (1001,1002,9999);
INSERT INTO dim_users (id, name, tier, region) VALUES
  (1001, 'Alice Wide', 'vip', 'east'),
  (1002, 'Bob Wide', 'standard', 'west');
"

echo "==> Prepare ClickHouse database"
"$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --multiquery "
CREATE DATABASE IF NOT EXISTS wide;
DROP TABLE IF EXISTS wide.order_detail_wide;
DROP TABLE IF EXISTS wide.order_minute_aggregate;
DROP TABLE IF EXISTS wide.clickhouse_write_failure_sink;
DROP TABLE IF EXISTS wide.lookup_miss_dlq_sink;
DROP TABLE IF EXISTS wide.lookup_refresh_failure_sink;
CREATE TABLE wide.clickhouse_write_failure_sink (
  id Int64,
  _version Int64
) ENGINE = ReplacingMergeTree(_version) ORDER BY id;
"

echo "==> Reset ETL data"
rm -rf data-wide-table
mkdir -p data-wide-table/output data-wide-table/checkpoint data-wide-table/dlq logs
chmod -R a+rwX data-wide-table
chmod a+rwX logs

echo "==> Run wide-table pipelines"
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
run_wide_app

echo "==> Produce Debezium-like order events"
cat <<'JSON' | "$CONTAINER_CLI" exec -i "$REDPANDA_CONTAINER" rpk topic produce orders.cdc --brokers localhost:9092 >/dev/null
{"payload":{"op":"c","ts_ms":1710000000123,"source":{"table":"orders"},"after":{"id":10001,"user_id":1001,"amount":12.5,"_version":1}}}
{"payload":{"op":"c","ts_ms":1710000000123,"source":{"table":"orders"},"after":{"id":10001,"user_id":1001,"amount":12.5,"_version":1}}}
{"payload":{"op":"c","ts_ms":1710000001123,"source":{"table":"orders"},"after":{"id":10002,"user_id":1002,"amount":20.0,"_version":1}}}
JSON

echo "==> Verify ClickHouse detail wide table"
i=0
while [ "$i" -lt 90 ]; do
  detail_count="$("$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM wide.order_detail_wide FINAL WHERE id IN (10001,10002)" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "$detail_count" = "2" ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "$detail_count" = "2"

echo "==> Verify ClickHouse aggregate wide table"
i=0
while [ "$i" -lt 90 ]; do
  aggregate_count="$("$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM wide.order_minute_aggregate FINAL" 2>/dev/null | tr -d '[:space:]' || true)"
  aggregate_east="$("$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT concat(toString(order_count), '|', toString(total_amount)) FROM wide.order_minute_aggregate FINAL WHERE region = 'east' AND tier = 'vip' ORDER BY window_start DESC LIMIT 1" 2>/dev/null | tr -d '[:space:]' || true)"
  aggregate_west="$("$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT concat(toString(order_count), '|', toString(total_amount)) FROM wide.order_minute_aggregate FINAL WHERE region = 'west' AND tier = 'standard' ORDER BY window_start DESC LIMIT 1" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "$aggregate_count" != "" ] && [ "$aggregate_count" -ge 2 ] && [ "$aggregate_east" = "1|12.5" ] && [ "$aggregate_west" = "1|20" ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "$aggregate_count" -ge 2
test "$aggregate_east" = "1|12.5"
test "$aggregate_west" = "1|20"

echo "==> Verify ClickHouse detail wide table absorbs duplicate Kafka message"
cat <<'JSON' | "$CONTAINER_CLI" exec -i "$REDPANDA_CONTAINER" rpk topic produce orders.cdc --brokers localhost:9092 >/dev/null
{"payload":{"op":"c","ts_ms":1710000000123,"source":{"table":"orders"},"after":{"id":10001,"user_id":1001,"amount":12.5,"_version":1}}}
JSON

i=0
while [ "$i" -lt 90 ]; do
  duplicate_raw_count="$("$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM wide.order_detail_wide WHERE id = 10001" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "$duplicate_raw_count" != "" ] && [ "$duplicate_raw_count" -ge 2 ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "$duplicate_raw_count" -ge 2

duplicate_final_count="$("$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM wide.order_detail_wide FINAL WHERE id = 10001" | tr -d '[:space:]')"
test "$duplicate_final_count" = "1"
detail_count_after_duplicate="$("$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM wide.order_detail_wide FINAL WHERE id IN (10001,10002)" | tr -d '[:space:]')"
test "$detail_count_after_duplicate" = "2"

echo "==> Verify stateful deduplicate/window recovery after app crash"
crash_ms="$(date +%s)000"
cat <<JSON | "$CONTAINER_CLI" exec -i "$REDPANDA_CONTAINER" rpk topic produce orders.cdc --brokers localhost:9092 >/dev/null
{"payload":{"op":"c","ts_ms":${crash_ms},"source":{"table":"orders"},"after":{"id":30001,"user_id":1001,"amount":77.77,"_version":1}}}
JSON

i=0
while [ "$i" -lt 90 ]; do
  crash_detail_count="$("$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM wide.order_detail_wide FINAL WHERE id = 30001" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "$crash_detail_count" = "1" ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "$crash_detail_count" = "1"

pre_crash_aggregate_count="$("$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM wide.order_minute_aggregate FINAL WHERE region = 'east' AND tier = 'vip' AND order_count = 1 AND abs(total_amount - 77.77) < 0.001" | tr -d '[:space:]')"
test "$pre_crash_aggregate_count" = "0"

i=0
while [ "$i" -lt 90 ]; do
  window_state_keys="$(curl -fsS http://127.0.0.1:8018/metrics 2>/dev/null | grep 'etl_state_keys{pipeline="kafka-orders-aggregate-clickhouse",node="window-3"}' | awk '{print $2}' | tr -d '[:space:]' || true)"
  if [ "$window_state_keys" != "" ] && [ "$window_state_keys" -ge 1 ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "${window_state_keys:-0}" -ge 1

"$CONTAINER_CLI" kill "$APP_CONTAINER" >/dev/null
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
run_wide_app

cat <<JSON | "$CONTAINER_CLI" exec -i "$REDPANDA_CONTAINER" rpk topic produce orders.cdc --brokers localhost:9092 >/dev/null
{"payload":{"op":"c","ts_ms":${crash_ms},"source":{"table":"orders"},"after":{"id":30001,"user_id":1001,"amount":77.77,"_version":1}}}
JSON

now_sec="$(date +%S | sed 's/^0//')"
if [ -z "$now_sec" ]; then
  now_sec=0
fi
sleep_for=$((65 - now_sec))
if [ "$sleep_for" -lt 3 ]; then
  sleep_for=3
fi
sleep "$sleep_for"
tick_ms="$(date +%s)000"
cat <<JSON | "$CONTAINER_CLI" exec -i "$REDPANDA_CONTAINER" rpk topic produce orders.cdc --brokers localhost:9092 >/dev/null
{"payload":{"op":"c","ts_ms":${tick_ms},"source":{"table":"orders"},"after":{"id":30002,"user_id":1002,"amount":1.23,"_version":1}}}
JSON

i=0
while [ "$i" -lt 90 ]; do
  recovered_count="$("$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM wide.order_minute_aggregate FINAL WHERE region = 'east' AND tier = 'vip' AND order_count = 1 AND abs(total_amount - 77.77) < 0.001" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "$recovered_count" = "1" ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "$recovered_count" = "1"

body="$(curl -fsS http://127.0.0.1:8018/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"name":"kafka-orders-detail-clickhouse"'
echo "$body" | grep '"name":"kafka-orders-aggregate-clickhouse"'
echo "$body" | grep '"name":"kafka-orders-clickhouse-write-failure"'
echo "$body" | grep '"name":"kafka-orders-lookup-miss-dlq"'
echo "$body" | grep '"name":"kafka-orders-lookup-refresh-failure"'

echo "==> Verify ClickHouse write failure routes to DLQ"
cat <<'JSON' | "$CONTAINER_CLI" exec -i "$REDPANDA_CONTAINER" rpk topic produce orders.clickhouse_failure --brokers localhost:9092 >/dev/null
{"payload":{"op":"c","ts_ms":1710000001923,"source":{"table":"orders"},"after":{"id":17001,"user_id":1001,"amount":7.77,"_version":1}}}
JSON

i=0
while [ "$i" -lt 90 ]; do
  dlq_body="$(curl -fsS 'http://127.0.0.1:8018/api/v2/dlq/kafka-orders-clickhouse-write-failure?error_contains=clickhouse%20schema%20drift&limit=10' 2>/dev/null || true)"
  if echo "$dlq_body" | grep -q 'clickhouse schema drift'; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
echo "$dlq_body"
echo "$dlq_body" | grep 'clickhouse schema drift'
echo "$dlq_body" | grep 'missing column amount'

echo "==> Verify lookup miss routes to DLQ"
cat <<'JSON' | "$CONTAINER_CLI" exec -i "$REDPANDA_CONTAINER" rpk topic produce orders.lookup_miss --brokers localhost:9092 >/dev/null
{"payload":{"op":"c","ts_ms":1710000002023,"source":{"table":"orders"},"after":{"id":18001,"user_id":9999,"amount":5.55,"_version":1}}}
JSON

i=0
while [ "$i" -lt 90 ]; do
  dlq_body="$(curl -fsS 'http://127.0.0.1:8018/api/v2/dlq/kafka-orders-lookup-miss-dlq?error_contains=no%20dimension%20match&limit=10' 2>/dev/null || true)"
  if echo "$dlq_body" | grep -q 'lookup: no dimension match'; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
echo "$dlq_body"
echo "$dlq_body" | grep 'lookup: no dimension match'
echo "$dlq_body" | grep 'key=9999'
lookup_miss_dlq_id="$(echo "$dlq_body" | grep -o '"id":[0-9][0-9]*' | head -n1 | sed 's/[^0-9]//g')"
test "$lookup_miss_dlq_id" != ""

echo "==> Repair lookup miss dimension and replay DLQ by stable ID"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "
INSERT INTO dim_users (id, name, tier, region)
VALUES (9999, 'Replay User', 'replay', 'north')
ON DUPLICATE KEY UPDATE name=VALUES(name), tier=VALUES(tier), region=VALUES(region);
"
replay_body="$(curl -fsS -X POST "http://127.0.0.1:8018/api/v2/dlq/kafka-orders-lookup-miss-dlq/${lookup_miss_dlq_id}/replay")"
echo "$replay_body"
echo "$replay_body" | grep '"replayed":1'

i=0
while [ "$i" -lt 90 ]; do
  replayed_count="$("$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM wide.lookup_miss_dlq_sink FINAL WHERE id = 18001 AND tier = 'replay' AND region = 'north'" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "$replayed_count" = "1" ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "$replayed_count" = "1"

dlq_body_after_replay="$(curl -fsS 'http://127.0.0.1:8018/api/v2/dlq/kafka-orders-lookup-miss-dlq?error_contains=no%20dimension%20match&limit=10')"
echo "$dlq_body_after_replay"
if echo "$dlq_body_after_replay" | grep -q "\"id\":${lookup_miss_dlq_id}"; then
  echo "replayed DLQ id ${lookup_miss_dlq_id} was not deleted" >&2
  exit 1
fi

echo "==> Verify lookup refresh failure routes to DLQ"
cat <<'JSON' | "$CONTAINER_CLI" exec -i "$REDPANDA_CONTAINER" rpk topic produce orders.lookup_failure --brokers localhost:9092 >/dev/null
{"payload":{"op":"c","ts_ms":1710000002123,"source":{"table":"orders"},"after":{"id":19001,"user_id":1999,"amount":9.99,"_version":1}}}
JSON

i=0
while [ "$i" -lt 90 ]; do
  dlq_body="$(curl -fsS 'http://127.0.0.1:8018/api/v2/dlq/kafka-orders-lookup-refresh-failure?error_contains=refresh%20failed&limit=10' 2>/dev/null || true)"
  if echo "$dlq_body" | grep -q 'lookup: refresh failed'; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
echo "$dlq_body"
echo "$dlq_body" | grep 'lookup: refresh failed'
echo "$dlq_body" | grep 'dim_users_missing_for_e2e'

echo "==> Verify ClickHouse infrastructure failure routes to DLQ"
"$CONTAINER_CLI" stop "$CH_CONTAINER" >/dev/null
wait_http_down "http://127.0.0.1:8123/ping"
cat <<'JSON' | "$CONTAINER_CLI" exec -i "$REDPANDA_CONTAINER" rpk topic produce orders.cdc --brokers localhost:9092 >/dev/null
{"payload":{"op":"c","ts_ms":1710000003123,"source":{"table":"orders"},"after":{"id":20001,"user_id":1001,"amount":33.33,"_version":1}}}
JSON

i=0
while [ "$i" -lt 90 ]; do
  dlq_body="$(curl -fsS 'http://127.0.0.1:8018/api/v2/dlq/kafka-orders-detail-clickhouse?contains=20001&limit=10' 2>/dev/null || true)"
  if echo "$dlq_body" | grep -q '20001'; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
echo "$dlq_body"
echo "$dlq_body" | grep '20001'
echo "$dlq_body" | grep -E 'clickhouse|connection refused|broken pipe|reset by peer|EOF'
clickhouse_down_dlq_id="$(echo "$dlq_body" | grep -o '"id":[0-9][0-9]*' | head -n1 | sed 's/[^0-9]//g')"
test "$clickhouse_down_dlq_id" != ""

echo "==> Restart ClickHouse and replay infrastructure-failure DLQ by stable ID"
compose -f docker-compose.dev.yml up -d clickhouse
wait_http "http://127.0.0.1:8123/ping"
replay_body="$(curl -fsS -X POST "http://127.0.0.1:8018/api/v2/dlq/kafka-orders-detail-clickhouse/${clickhouse_down_dlq_id}/replay")"
echo "$replay_body"
echo "$replay_body" | grep '"replayed":1'

i=0
while [ "$i" -lt 90 ]; do
  replayed_detail_count="$("$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM wide.order_detail_wide FINAL WHERE id = 20001 AND user_name = 'Alice Wide' AND user_region = 'east' AND user_tier = 'vip'" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "$replayed_detail_count" = "1" ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "$replayed_detail_count" = "1"

dlq_body_after_replay="$(curl -fsS 'http://127.0.0.1:8018/api/v2/dlq/kafka-orders-detail-clickhouse?contains=20001&limit=10')"
echo "$dlq_body_after_replay"
if echo "$dlq_body_after_replay" | grep -q "\"id\":${clickhouse_down_dlq_id}"; then
  echo "replayed ClickHouse DLQ id ${clickhouse_down_dlq_id} was not deleted" >&2
  exit 1
fi

echo "Wide-table E2E passed"
