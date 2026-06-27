#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
APP_CONTAINER="etl-openetl-go-api-conflict"

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
"$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile .

echo "==> Reset ETL data"
rm -rf data-api-conflict
mkdir -p data-api-conflict/output data-api-conflict/checkpoint data-api-conflict/dlq logs
chmod -R a+rwX data-api-conflict
chmod a+rwX logs

echo "==> Run API conflict fixture"
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p 8015:8001 \
  -v "$ROOT_DIR/testdata/pipes-auth:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-api-conflict:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8015/api/v2/health"

i=0
while [ "$i" -lt 60 ]; do
  body="$(curl -fsS http://127.0.0.1:8015/api/v2/pipelines)"
  echo "$body" | grep '"name":"auth-file-to-file"' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done

echo "==> Try duplicate runtime create"
payload='{"spec":{"name":"auth-file-to-file","source":{"type":"file","config":{"path":"/app/testdata/files/customers.jsonl","format":"json"}},"sink":{"type":"file_sink","config":{"output_dir":"/app/data/output/conflict","format":"jsonl"}},"batch_size":1,"checkpoint_interval_sec":1,"backpressure_buffer":10}}'
status="$(curl -s -o data-api-conflict/conflict-response.json -w '%{http_code}' -H 'Content-Type: application/json' -d "$payload" http://127.0.0.1:8015/api/v2/pipelines)"
test "$status" = "409"
grep 'pipeline already exists' data-api-conflict/conflict-response.json

body="$(curl -fsS http://127.0.0.1:8015/api/v2/pipelines)"
count="$(echo "$body" | grep -o '"name":"auth-file-to-file"' | wc -l | tr -d '[:space:]')"
test "$count" = "1"

echo "API conflict E2E passed"
