#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

IMAGE="openetl-go-etl:dev"
REDPANDA_CONTAINER="etl-redpanda"
KAFKA_SINK_APP="etl-openetl-go-kafka-sink"
KAFKA_SOURCE_APP="etl-openetl-go-kafka-source"

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 60 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 1
  done
  return 1
}

echo "==> Build image"
podman build -t "$IMAGE" -f Dockerfile .

echo "==> Start Redpanda"
podman-compose -f docker-compose.dev.yml up -d redpanda

echo "==> Wait Redpanda"
i=0
while [ "$i" -lt 90 ]; do
  if podman exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null 2>&1; then break; fi
  i=$((i + 1)); sleep 2
done
podman exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null

echo "==> Prepare topics"
podman exec "$REDPANDA_CONTAINER" rpk topic delete etl-sink-topic etl-source-topic >/dev/null 2>&1 || true
podman exec "$REDPANDA_CONTAINER" rpk topic create etl-sink-topic etl-source-topic --brokers localhost:9092 >/dev/null

echo "==> Reset Kafka sink data"
rm -rf data-kafka-sink data-kafka-source
mkdir -p data-kafka-sink/output data-kafka-sink/checkpoint data-kafka-sink/dlq data-kafka-source/output data-kafka-source/checkpoint data-kafka-source/dlq logs

echo "==> Run file->Kafka pipeline"
podman rm -f "$KAFKA_SINK_APP" >/dev/null 2>&1 || true
podman run -d \
  --name "$KAFKA_SINK_APP" \
  -p 8009:8001 \
  -v "$ROOT_DIR/testdata/pipes-kafka-sink:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-kafka-sink:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8009/api/v2/health"
i=0
while [ "$i" -lt 60 ]; do
  body="$(curl -fsS http://127.0.0.1:8009/api/v2/pipelines)"
  echo "$body" | grep '"name":"file-to-kafka"' | grep '"records_written":3' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done
body="$(curl -fsS http://127.0.0.1:8009/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"records_written":3'

echo "==> Verify Kafka sink topic"
podman exec "$REDPANDA_CONTAINER" rpk topic consume etl-sink-topic -n 3 --brokers localhost:9092 > data-kafka-sink/messages.jsonl
grep 'Ada' data-kafka-sink/messages.jsonl
grep 'Alan' data-kafka-sink/messages.jsonl
grep 'Grace' data-kafka-sink/messages.jsonl

echo "==> Run Kafka->file pipeline"
podman rm -f "$KAFKA_SOURCE_APP" >/dev/null 2>&1 || true
podman run -d \
  --name "$KAFKA_SOURCE_APP" \
  -p 8010:8001 \
  -v "$ROOT_DIR/testdata/pipes-kafka-source:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-kafka-source:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8010/api/v2/health"
sleep 3
printf '%s\n%s\n' '{"id":101,"name":"Kafka Alice"}' '{"id":102,"name":"Kafka Bob"}' | podman exec -i "$REDPANDA_CONTAINER" rpk topic produce etl-source-topic --brokers localhost:9092 >/dev/null

i=0
while [ "$i" -lt 60 ]; do
  body="$(curl -fsS http://127.0.0.1:8010/api/v2/pipelines)"
  echo "$body" | grep '"name":"kafka-to-file"' | grep '"records_written":2' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done
body="$(curl -fsS http://127.0.0.1:8010/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"records_written":2'
grep -R 'Kafka Alice' data-kafka-source/output/kafka
grep -R 'Kafka Bob' data-kafka-source/output/kafka

echo "Kafka E2E passed"
