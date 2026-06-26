#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

IMAGE="openetl-go-etl:dev"
APP_CONTAINER="etl-openetl-go-duplicate"

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
docker build -t "$IMAGE" -f Dockerfile .

echo "==> Reset ETL data"
rm -rf data-duplicate
mkdir -p data-duplicate/output data-duplicate/checkpoint data-duplicate/dlq logs
chmod -R a+rwX data-duplicate
chmod a+rwX logs

echo "==> Run duplicate spec pipeline"
docker rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
docker run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p 8013:8001 \
  -v "$ROOT_DIR/testdata/pipes-duplicate:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-duplicate:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8013/api/v2/health"

i=0
while [ "$i" -lt 60 ]; do
  body="$(curl -fsS http://127.0.0.1:8013/api/v2/pipelines)"
  echo "$body" | grep '"name":"duplicate-pipeline"' | grep '"records_written":3' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done
body="$(curl -fsS http://127.0.0.1:8013/api/v2/pipelines)"
echo "$body"
count="$(echo "$body" | grep -o '"name":"duplicate-pipeline"' | wc -l | tr -d '[:space:]')"
test "$count" = "1"
docker logs "$APP_CONTAINER" 2>&1 | grep 'Skip duplicate pipeline duplicate-pipeline'
test -n "$(ls data-duplicate/output/duplicate/first_*.jsonl 2>/dev/null)"
test -z "$(ls data-duplicate/output/duplicate/second_*.jsonl 2>/dev/null || true)"

echo "Duplicate spec E2E passed"
