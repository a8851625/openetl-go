#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

IMAGE="openetl-go-etl:dev"
REDPANDA_CONTAINER="etl-redpanda"
MYSQL_CONTAINER="etl-mysql-source"
CH_CONTAINER="etl-clickhouse"
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

echo "==> Build image"
docker build -t "$IMAGE" -f Dockerfile .

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
docker exec "$REDPANDA_CONTAINER" rpk topic delete orders.cdc >/dev/null 2>&1 || true
docker exec "$REDPANDA_CONTAINER" rpk topic delete orders.clickhouse_failure >/dev/null 2>&1 || true
docker exec "$REDPANDA_CONTAINER" rpk topic delete orders.lookup_miss >/dev/null 2>&1 || true
docker exec "$REDPANDA_CONTAINER" rpk topic delete orders.lookup_failure >/dev/null 2>&1 || true
docker exec "$REDPANDA_CONTAINER" rpk topic create orders.cdc --brokers localhost:9092 >/dev/null
docker exec "$REDPANDA_CONTAINER" rpk topic create orders.clickhouse_failure --brokers localhost:9092 >/dev/null
docker exec "$REDPANDA_CONTAINER" rpk topic create orders.lookup_miss --brokers localhost:9092 >/dev/null
docker exec "$REDPANDA_CONTAINER" rpk topic create orders.lookup_failure --brokers localhost:9092 >/dev/null

echo "==> Prepare MySQL dimension table"
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "
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
docker exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --multiquery "
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
docker rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
docker run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p 8018:8001 \
  -v "$ROOT_DIR/testdata/pipes-wide-table:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-wide-table:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8018/api/v2/health"

echo "==> Produce Debezium-like order events"
cat <<'JSON' | docker exec -i "$REDPANDA_CONTAINER" rpk topic produce orders.cdc --brokers localhost:9092 >/dev/null
{"payload":{"op":"c","ts_ms":1710000000123,"source":{"table":"orders"},"after":{"id":10001,"user_id":1001,"amount":12.5,"_version":1}}}
{"payload":{"op":"c","ts_ms":1710000000123,"source":{"table":"orders"},"after":{"id":10001,"user_id":1001,"amount":12.5,"_version":1}}}
{"payload":{"op":"c","ts_ms":1710000001123,"source":{"table":"orders"},"after":{"id":10002,"user_id":1002,"amount":20.0,"_version":1}}}
JSON

echo "==> Verify ClickHouse detail wide table"
i=0
while [ "$i" -lt 90 ]; do
  detail_count="$(docker exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM wide.order_detail_wide FINAL WHERE id IN (10001,10002)" 2>/dev/null | tr -d '[:space:]' || true)"
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
  aggregate_count="$(docker exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM wide.order_minute_aggregate FINAL" 2>/dev/null | tr -d '[:space:]' || true)"
  aggregate_east="$(docker exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT concat(toString(order_count), '|', toString(total_amount)) FROM wide.order_minute_aggregate FINAL WHERE region = 'east' AND tier = 'vip' ORDER BY window_start DESC LIMIT 1" 2>/dev/null | tr -d '[:space:]' || true)"
  aggregate_west="$(docker exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT concat(toString(order_count), '|', toString(total_amount)) FROM wide.order_minute_aggregate FINAL WHERE region = 'west' AND tier = 'standard' ORDER BY window_start DESC LIMIT 1" 2>/dev/null | tr -d '[:space:]' || true)"
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
cat <<'JSON' | docker exec -i "$REDPANDA_CONTAINER" rpk topic produce orders.cdc --brokers localhost:9092 >/dev/null
{"payload":{"op":"c","ts_ms":1710000000123,"source":{"table":"orders"},"after":{"id":10001,"user_id":1001,"amount":12.5,"_version":1}}}
JSON

i=0
while [ "$i" -lt 90 ]; do
  duplicate_raw_count="$(docker exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM wide.order_detail_wide WHERE id = 10001" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "$duplicate_raw_count" != "" ] && [ "$duplicate_raw_count" -ge 2 ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "$duplicate_raw_count" -ge 2

duplicate_final_count="$(docker exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM wide.order_detail_wide FINAL WHERE id = 10001" | tr -d '[:space:]')"
test "$duplicate_final_count" = "1"
detail_count_after_duplicate="$(docker exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM wide.order_detail_wide FINAL WHERE id IN (10001,10002)" | tr -d '[:space:]')"
test "$detail_count_after_duplicate" = "2"

body="$(curl -fsS http://127.0.0.1:8018/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"name":"kafka-orders-detail-clickhouse"'
echo "$body" | grep '"name":"kafka-orders-aggregate-clickhouse"'
echo "$body" | grep '"name":"kafka-orders-clickhouse-write-failure"'
echo "$body" | grep '"name":"kafka-orders-lookup-miss-dlq"'
echo "$body" | grep '"name":"kafka-orders-lookup-refresh-failure"'

echo "==> Verify ClickHouse write failure routes to DLQ"
cat <<'JSON' | docker exec -i "$REDPANDA_CONTAINER" rpk topic produce orders.clickhouse_failure --brokers localhost:9092 >/dev/null
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
cat <<'JSON' | docker exec -i "$REDPANDA_CONTAINER" rpk topic produce orders.lookup_miss --brokers localhost:9092 >/dev/null
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
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "
INSERT INTO dim_users (id, name, tier, region)
VALUES (9999, 'Replay User', 'replay', 'north')
ON DUPLICATE KEY UPDATE name=VALUES(name), tier=VALUES(tier), region=VALUES(region);
"
replay_body="$(curl -fsS -X POST "http://127.0.0.1:8018/api/v2/dlq/kafka-orders-lookup-miss-dlq/${lookup_miss_dlq_id}/replay")"
echo "$replay_body"
echo "$replay_body" | grep '"replayed":1'

i=0
while [ "$i" -lt 90 ]; do
  replayed_count="$(docker exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM wide.lookup_miss_dlq_sink FINAL WHERE id = 18001 AND tier = 'replay' AND region = 'north'" 2>/dev/null | tr -d '[:space:]' || true)"
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
cat <<'JSON' | docker exec -i "$REDPANDA_CONTAINER" rpk topic produce orders.lookup_failure --brokers localhost:9092 >/dev/null
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
docker stop "$CH_CONTAINER" >/dev/null
wait_http_down "http://127.0.0.1:8123/ping"
cat <<'JSON' | docker exec -i "$REDPANDA_CONTAINER" rpk topic produce orders.cdc --brokers localhost:9092 >/dev/null
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
docker compose -f docker-compose.dev.yml up -d clickhouse
wait_http "http://127.0.0.1:8123/ping"
replay_body="$(curl -fsS -X POST "http://127.0.0.1:8018/api/v2/dlq/kafka-orders-detail-clickhouse/${clickhouse_down_dlq_id}/replay")"
echo "$replay_body"
echo "$replay_body" | grep '"replayed":1'

i=0
while [ "$i" -lt 90 ]; do
  replayed_detail_count="$(docker exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM wide.order_detail_wide FINAL WHERE id = 20001 AND user_name = 'Alice Wide' AND user_region = 'east' AND user_tier = 'vip'" 2>/dev/null | tr -d '[:space:]' || true)"
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
