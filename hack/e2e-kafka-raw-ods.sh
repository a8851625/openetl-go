#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

IMAGE="openetl-go-etl:dev"
REDPANDA_CONTAINER="etl-redpanda"
MYSQL_CONTAINER="etl-mysql-source"
APP_CONTAINER="etl-openetl-go-kafka-raw-ods"
RAW_TOPIC="raw-device-protocol"
ODS_TOPIC="raw-device-ods"
GROUP_ID="raw-ods-e2e-$(date +%s)"

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 60 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 1
  done
  return 1
}

if [ "${E2E_SKIP_BUILD:-0}" = "1" ]; then
  echo "==> Skip image build (E2E_SKIP_BUILD=1, using $IMAGE)"
else
  echo "==> Build image"
  docker build -t "$IMAGE" -f Dockerfile .
fi

echo "==> Start Redpanda and MySQL"
docker compose -f docker-compose.dev.yml up -d redpanda mysql-source

echo "==> Wait Redpanda"
i=0
while [ "$i" -lt 90 ]; do
  if docker exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null 2>&1; then break; fi
  i=$((i + 1)); sleep 2
done
docker exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null

echo "==> Wait MySQL healthy"
i=0
while [ "$i" -lt 60 ]; do
  status="$(docker inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
  if [ "$status" = "healthy" ]; then break; fi
  i=$((i + 1)); sleep 2
done
[ "$(docker inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER")" = "healthy" ]

docker rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true

echo "==> Prepare Kafka topics"
docker exec "$REDPANDA_CONTAINER" rpk topic delete "$RAW_TOPIC" "$ODS_TOPIC" --brokers localhost:9092 >/dev/null 2>&1 || true
docker exec "$REDPANDA_CONTAINER" rpk topic create "$RAW_TOPIC" "$ODS_TOPIC" --brokers localhost:9092 >/dev/null

echo "==> Prepare MySQL dimension table"
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "
CREATE TABLE IF NOT EXISTS dim_devices (
  device_id VARCHAR(64) PRIMARY KEY,
  model VARCHAR(64),
  region VARCHAR(64)
);
DELETE FROM dim_devices WHERE device_id IN ('dev-1', 'dev-missing');
INSERT INTO dim_devices (device_id, model, region)
VALUES ('dev-1', 'sedan-probe', 'east')
ON DUPLICATE KEY UPDATE model=VALUES(model), region=VALUES(region);
"

echo "==> Reset ETL data"
rm -rf data-kafka-raw-ods
mkdir -p data-kafka-raw-ods/output data-kafka-raw-ods/checkpoint data-kafka-raw-ods/dlq logs
chmod -R a+rwX data-kafka-raw-ods
chmod a+rwX logs

echo "==> Run Kafka raw -> ODS Kafka pipeline"
docker run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p 8020:8001 \
  -e RAW_ODS_GROUP_ID="$GROUP_ID" \
  -v "$ROOT_DIR/testdata/pipes-kafka-raw-ods:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-kafka-raw-ods:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8020/api/v2/health"

echo "==> Wait pipeline running"
i=0
while [ "$i" -lt 60 ]; do
  body="$(curl -fsS http://127.0.0.1:8020/api/v2/pipelines)"
  echo "$body" | grep '"name":"kafka-raw-to-ods-kafka"' | grep '"status":"running"' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done

echo "==> Produce raw protocol messages"
cat <<'EOF' | docker exec -i "$REDPANDA_CONTAINER" rpk topic produce "$RAW_TOPIC" --brokers localhost:9092 >/dev/null
device=dev-1;ts=2026-06-26T10:00:00Z;events=speed:12|soc:80
device=dev-missing;ts=2026-06-26T10:01:00Z;events=speed:7
bad-payload-without-required-fields
EOF

echo "==> Verify ODS Kafka writes"
i=0
while [ "$i" -lt 90 ]; do
  body="$(curl -fsS http://127.0.0.1:8020/api/v2/pipelines)"
  echo "$body" | grep '"name":"kafka-raw-to-ods-kafka"' | grep '"records_written":2' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done
body="$(curl -fsS http://127.0.0.1:8020/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"name":"kafka-raw-to-ods-kafka"' | grep '"records_written":2'

docker exec "$REDPANDA_CONTAINER" rpk topic consume "$ODS_TOPIC" -n 2 --brokers localhost:9092 > data-kafka-raw-ods/ods-first.jsonl
grep 'metric_type.*speed' data-kafka-raw-ods/ods-first.jsonl
grep 'metric_type.*soc' data-kafka-raw-ods/ods-first.jsonl
grep 'device_region.*east' data-kafka-raw-ods/ods-first.jsonl
grep 'device_model.*sedan-probe' data-kafka-raw-ods/ods-first.jsonl
grep 'source_system.*raw-device-protocol' data-kafka-raw-ods/ods-first.jsonl

echo "==> Verify parser and lookup misses are visible in DLQ"
i=0
while [ "$i" -lt 60 ]; do
  parse_dlq="$(curl -fsS 'http://127.0.0.1:8020/api/v2/dlq/kafka-raw-to-ods-kafka?contains=bad-payload-without-required-fields&limit=10')"
  echo "$parse_dlq" | grep 'raw parser: required fields' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done
echo "$parse_dlq"
echo "$parse_dlq" | grep 'bad-payload-without-required-fields'
echo "$parse_dlq" | grep 'raw parser: required fields'
echo "$parse_dlq" | grep '"error_class":"data"'

i=0
while [ "$i" -lt 60 ]; do
  miss_dlq="$(curl -fsS 'http://127.0.0.1:8020/api/v2/dlq/kafka-raw-to-ods-kafka?contains=dev-missing&limit=10')"
  echo "$miss_dlq" | grep 'no dimension match' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done
echo "$miss_dlq"
echo "$miss_dlq" | grep 'dev-missing'
echo "$miss_dlq" | grep 'no dimension match'
echo "$miss_dlq" | grep '"error_class":"data"'

echo "==> Replay from Kafka offset 0 and verify append duplicate boundary"
curl -fsS -X POST http://127.0.0.1:8020/api/v2/pipelines/kafka-raw-to-ods-kafka/stop >/dev/null
sleep 3
curl -fsS -X POST \
  -H 'Content-Type: application/json' \
  -d '{"source":"kafka","topic":"raw-device-protocol","partition":0,"offset":0}' \
  http://127.0.0.1:8020/api/v2/pipelines/kafka-raw-to-ods-kafka/checkpoint/set >/dev/null
curl -fsS -X POST http://127.0.0.1:8020/api/v2/pipelines/kafka-raw-to-ods-kafka/start >/dev/null

i=0
while [ "$i" -lt 90 ]; do
  body="$(curl -fsS http://127.0.0.1:8020/api/v2/pipelines)"
  echo "$body" | grep '"name":"kafka-raw-to-ods-kafka"' | grep '"records_written":2' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done
body="$(curl -fsS http://127.0.0.1:8020/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"name":"kafka-raw-to-ods-kafka"' | grep '"records_written":2'

docker exec "$REDPANDA_CONTAINER" rpk topic consume "$ODS_TOPIC" -n 4 --brokers localhost:9092 > data-kafka-raw-ods/ods-replayed.jsonl
speed_count="$(grep -c '"key": "dev-1:2026-06-26T10:00:00Z:speed"' data-kafka-raw-ods/ods-replayed.jsonl | tr -d '[:space:]')"
soc_count="$(grep -c '"key": "dev-1:2026-06-26T10:00:00Z:soc"' data-kafka-raw-ods/ods-replayed.jsonl | tr -d '[:space:]')"
test "$speed_count" = "2"
test "$soc_count" = "2"

curl -fsS -X POST http://127.0.0.1:8020/api/v2/pipelines/kafka-raw-to-ods-kafka/stop >/dev/null

echo "Kafka raw -> ODS Kafka E2E passed"
