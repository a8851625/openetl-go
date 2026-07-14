#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

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

wait_redpanda() {
  i=0
  while [ "$i" -lt 90 ]; do
    if "$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 2
  done
  return 1
}

wait_consumer_group() {
  i=0
  while [ "$i" -lt 90 ]; do
    group_body="$("$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk group describe etl-source-e2e --brokers localhost:9092 2>/dev/null || true)"
    if echo "$group_body" | grep 'STATE.*Stable' >/dev/null 2>&1 \
      && echo "$group_body" | grep 'etl-source-topic' >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1)); sleep 1
  done
  echo "$group_body"
  return 1
}

run_kafka_source_app() {
  "$CONTAINER_CLI" run -d \
    --add-host host.docker.internal:host-gateway \
    --name "$KAFKA_SOURCE_APP" \
    -p 8010:8001 \
    -v "$ROOT_DIR/testdata/pipes-kafka-source:/app/pipes:ro" \
    -v "$ROOT_DIR/testdata:/app/testdata:ro" \
    -v "$ROOT_DIR/data-kafka-source:/app/data" \
    -v "$ROOT_DIR/logs:/app/logs" \
    "$IMAGE" >/dev/null
  wait_http "http://127.0.0.1:8010/api/v2/health"
}

wait_source_written() {
  expected="$1"
  i=0
  while [ "$i" -lt 90 ]; do
    body="$(curl -fsS http://127.0.0.1:8010/api/v2/pipelines)"
    echo "$body" | grep '"name":"kafka-to-file"' | grep "\"records_written\":$expected" >/dev/null 2>&1 && return 0
    i=$((i + 1)); sleep 1
  done
  echo "$body"
  return 1
}

wait_checkpoint_offset() {
  expected="$1"
  i=0
  while [ "$i" -lt 90 ]; do
    checkpoint_body="$(curl -fsS http://127.0.0.1:8010/api/v2/pipelines/kafka-to-file/checkpoint)"
    echo "$checkpoint_body" | grep "\"offsets\":{\"0\":$expected}" | grep "\"last_record\":{\"offset\":$expected" >/dev/null 2>&1 && return 0
    i=$((i + 1)); sleep 1
  done
  echo "$checkpoint_body"
  return 1
}

if [ "${E2E_SKIP_BUILD:-0}" = "1" ]; then
  echo "==> Skip image build (E2E_SKIP_BUILD=1, using $IMAGE)"
else
  echo "==> Build image"
  "$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile .
fi

echo "==> Start Redpanda"
compose -f docker-compose.dev.yml up -d redpanda

echo "==> Wait Redpanda"
wait_redpanda

echo "==> Prepare topics"
"$CONTAINER_CLI" rm -f "$KAFKA_SINK_APP" "$KAFKA_SOURCE_APP" >/dev/null 2>&1 || true
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk group delete etl-source-e2e --brokers localhost:9092 >/dev/null 2>&1 || true
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic delete etl-sink-topic etl-source-topic >/dev/null 2>&1 || true
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic create etl-sink-topic etl-source-topic --brokers localhost:9092 >/dev/null

echo "==> Reset Kafka sink data"
rm -rf data-kafka-sink data-kafka-source
mkdir -p data-kafka-sink/output data-kafka-sink/checkpoint data-kafka-sink/dlq data-kafka-source/output data-kafka-source/checkpoint data-kafka-source/dlq logs
chmod -R a+rwX data-kafka-sink data-kafka-source
chmod a+rwX logs

echo "==> Run file->Kafka pipeline"
"$CONTAINER_CLI" rm -f "$KAFKA_SINK_APP" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
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
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic consume etl-sink-topic -n 3 --brokers localhost:9092 > data-kafka-sink/messages.jsonl
grep 'Ada' data-kafka-sink/messages.jsonl
grep 'Alan' data-kafka-sink/messages.jsonl
grep 'Grace' data-kafka-sink/messages.jsonl

echo "==> Run Kafka->file pipeline"
"$CONTAINER_CLI" rm -f "$KAFKA_SOURCE_APP" >/dev/null 2>&1 || true
run_kafka_source_app
wait_consumer_group
printf '%s\n%s\n' '{"id":101,"name":"Kafka Alice"}' '{"id":102,"name":"Kafka Bob"}' | "$CONTAINER_CLI" exec -i "$REDPANDA_CONTAINER" rpk topic produce etl-source-topic --brokers localhost:9092 >/dev/null

wait_source_written 2
body="$(curl -fsS http://127.0.0.1:8010/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"records_written":2'
grep -R 'Kafka Alice' data-kafka-source/output/kafka
grep -R 'Kafka Bob' data-kafka-source/output/kafka

echo "==> Verify checkpoint binds Kafka offset to sink acknowledgement"
wait_checkpoint_offset 1
checkpoint_json="$(curl -fsS http://127.0.0.1:8010/api/v2/pipelines/kafka-to-file/checkpoint)"
echo "$checkpoint_json" | grep '"sink_commit"'
echo "$checkpoint_json" | grep '"acknowledged":true'
echo "$checkpoint_json" | grep '"sink":"file_sink"'

echo "==> SIGKILL Kafka consumer, produce while down, and recover from committed checkpoint"
"$CONTAINER_CLI" kill "$KAFKA_SOURCE_APP" >/dev/null
"$CONTAINER_CLI" rm -f "$KAFKA_SOURCE_APP" >/dev/null 2>&1 || true
printf '%s\n' '{"id":103,"name":"Kafka Crash Recovery"}' | "$CONTAINER_CLI" exec -i "$REDPANDA_CONTAINER" rpk topic produce etl-source-topic --brokers localhost:9092 >/dev/null
run_kafka_source_app
wait_source_written 1
grep -R 'Kafka Crash Recovery' data-kafka-source/output/kafka

echo "==> Restart broker and verify ordinary Kafka source reconnects"
compose -f docker-compose.dev.yml restart redpanda >/dev/null
wait_redpanda
wait_consumer_group
printf '%s\n' '{"id":104,"name":"Kafka Broker Recovery"}' | "$CONTAINER_CLI" exec -i "$REDPANDA_CONTAINER" rpk topic produce etl-source-topic --brokers localhost:9092 >/dev/null
wait_source_written 2
grep -R 'Kafka Broker Recovery' data-kafka-source/output/kafka

echo "==> Trigger consumer group rebalance and verify source resumes"
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" sh -c "rpk topic consume etl-source-topic --brokers localhost:9092 --group etl-source-e2e --offset end --num 1 >/tmp/openetl-kafka-rebalance.log 2>&1 & pid=\$!; sleep 5; kill \"\$pid\" >/dev/null 2>&1 || true; wait \"\$pid\" >/dev/null 2>&1 || true"
wait_consumer_group
printf '%s\n' '{"id":105,"name":"Kafka Rebalance Recovery"}' | "$CONTAINER_CLI" exec -i "$REDPANDA_CONTAINER" rpk topic produce etl-source-topic --brokers localhost:9092 >/dev/null
wait_source_written 3
grep -R 'Kafka Rebalance Recovery' data-kafka-source/output/kafka

echo "==> Replay from Kafka offset 0 and verify deterministic file sink absorbs duplicates"
objects_before="$(find data-kafka-source/output/kafka -type f | wc -l | tr -d '[:space:]')"
curl -fsS -X POST http://127.0.0.1:8010/api/v2/pipelines/kafka-to-file/stop >/dev/null
sleep 3
curl -fsS -X POST \
  -H 'Content-Type: application/json' \
  -d '{"source":"kafka","topic":"etl-source-topic","partition":0,"offset":0}' \
  http://127.0.0.1:8010/api/v2/pipelines/kafka-to-file/checkpoint/set >/dev/null
curl -fsS -X POST http://127.0.0.1:8010/api/v2/pipelines/kafka-to-file/start >/dev/null
wait_source_written 5
objects_after="$(find data-kafka-source/output/kafka -type f | wc -l | tr -d '[:space:]')"
test "$objects_after" = "$objects_before"
grep -R 'Kafka Alice' data-kafka-source/output/kafka
grep -R 'Kafka Rebalance Recovery' data-kafka-source/output/kafka

echo "Kafka E2E passed"
