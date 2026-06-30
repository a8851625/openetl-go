#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

missing=""
for key in MAXCOMPUTE_ENDPOINT MAXCOMPUTE_PROJECT MAXCOMPUTE_TABLE MAXCOMPUTE_ACCESS_KEY_ID MAXCOMPUTE_ACCESS_KEY_SECRET; do
  eval "value=\${$key:-}"
  if [ -z "$value" ]; then
    missing="$missing $key"
  fi
done

if [ -n "$missing" ]; then
  echo "SKIP MaxCompute E2E: missing env:$missing"
  exit 0
fi

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
REDPANDA_CONTAINER="etl-redpanda"
APP_CONTAINER="etl-openetl-go-maxcompute"
TOPIC="maxcompute-ods-events"
PIPE_DIR="$ROOT_DIR/data-maxcompute/pipes"

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 60 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 1
  done
  return 1
}

cleanup() {
  "$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

echo "==> Build image"
"$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile .

echo "==> Start Redpanda"
compose -f docker-compose.dev.yml up -d redpanda

echo "==> Wait Redpanda"
i=0
while [ "$i" -lt 90 ]; do
  if "$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null 2>&1; then break; fi
  i=$((i + 1)); sleep 2
done
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null

echo "==> Prepare topic"
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic delete "$TOPIC" >/dev/null 2>&1 || true
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic create "$TOPIC" --brokers localhost:9092 >/dev/null

echo "==> Generate MaxCompute pipeline spec"
rm -rf data-maxcompute
mkdir -p "$PIPE_DIR" data-maxcompute/checkpoint data-maxcompute/dlq logs
chmod -R a+rwX data-maxcompute logs

cat > "$PIPE_DIR/kafka-ods-to-maxcompute.yaml" <<EOF_SPEC
name: kafka-ods-to-maxcompute
source:
  type: kafka
  config:
    brokers:
      - host.docker.internal:19092
    topic: "$TOPIC"
    group_id: "openetl-maxcompute-ods-e2e"
    initial_offset: oldest
    format: json
transforms:
  - type: project
    config:
      fields: [id, amount, event_time, payload, dt]
      keep_unmapped: false
  - type: type_convert
    config:
      conversions:
        id: int
        amount: float
        event_time: timestamp
sink:
  type: maxcompute
  config:
    endpoint: "$MAXCOMPUTE_ENDPOINT"
    tunnel_endpoint: "${MAXCOMPUTE_TUNNEL_ENDPOINT:-}"
    project: "$MAXCOMPUTE_PROJECT"
    table: "$MAXCOMPUTE_TABLE"
    access_key_id: "$MAXCOMPUTE_ACCESS_KEY_ID"
    access_key_secret: "$MAXCOMPUTE_ACCESS_KEY_SECRET"
    quota_name: "${MAXCOMPUTE_QUOTA_NAME:-}"
    columns:
      id: BIGINT
      amount: DOUBLE
      event_time: TIMESTAMP
      payload: STRING
    partition_fields: [dt]
    write_mode: append
    auto_create_partition: true
    batch_size: 100
    max_retries: 3
    retry_base_ms: 500
batch_size: 10
checkpoint_interval_sec: 1
backpressure_buffer: 100
retry:
  max_attempts: 1
  initial_interval_ms: 500
  max_interval_ms: 5000
dlq:
  enable: true
EOF_SPEC

echo "==> Start OpenETL with MaxCompute pipeline"
cleanup
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p 8018:8001 \
  -v "$PIPE_DIR:/app/pipes:ro" \
  -v "$ROOT_DIR/data-maxcompute:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8018/api/v2/health"

echo "==> Produce Kafka ODS records"
printf '%s\n%s\n%s\n' \
  '{"id":1001,"amount":"12.5","event_time":"2026-06-26T10:11:12Z","payload":"alpha","dt":"2026-06-26"}' \
  '{"id":1002,"amount":"18.0","event_time":"2026-06-26T10:12:12Z","payload":"beta","dt":"2026-06-26"}' \
  '{"id":1003,"amount":"9.75","event_time":"2026-06-27T00:01:02Z","payload":"gamma","dt":"2026-06-27"}' \
  | "$CONTAINER_CLI" exec -i "$REDPANDA_CONTAINER" rpk topic produce "$TOPIC" --brokers localhost:9092 >/dev/null

echo "==> Wait for committed writes"
i=0
while [ "$i" -lt 120 ]; do
  body="$(curl -fsS http://127.0.0.1:8018/api/v2/pipelines)"
  echo "$body" | grep '"name":"kafka-ods-to-maxcompute"' | grep '"records_written":3' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 2
done
body="$(curl -fsS http://127.0.0.1:8018/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"records_written":3'

echo "MaxCompute E2E passed"
