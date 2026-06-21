#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

IMAGE="openetl-go-etl:dev"
CLICKHOUSE_CONTAINER="etl-clickhouse"
APP_CONTAINER="etl-openetl-go-clickhouse-autocreate"

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

echo "==> Start ClickHouse"
podman-compose -f docker-compose.dev.yml up -d clickhouse

echo "==> Wait ClickHouse HTTP"
i=0
while [ "$i" -lt 90 ]; do
  if curl -fsS http://127.0.0.1:8123/ping >/dev/null 2>&1; then break; fi
  i=$((i + 1)); sleep 2
done
curl -fsS http://127.0.0.1:8123/ping >/dev/null

echo "==> Reset ClickHouse target"
podman exec "$CLICKHOUSE_CONTAINER" clickhouse-client --query "CREATE DATABASE IF NOT EXISTS dzh3136_go"
podman exec "$CLICKHOUSE_CONTAINER" clickhouse-client --query "DROP TABLE IF EXISTS dzh3136_go.auto_customers"

echo "==> Reset ETL data"
rm -rf data-clickhouse-autocreate
mkdir -p data-clickhouse-autocreate/output data-clickhouse-autocreate/checkpoint data-clickhouse-autocreate/dlq logs

echo "==> Run auto-create pipeline"
podman rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
podman run -d \
  --name "$APP_CONTAINER" \
  -p 8006:8001 \
  -v "$ROOT_DIR/testdata/pipes-clickhouse-autocreate:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-clickhouse-autocreate:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8006/api/v2/health"

echo "==> Wait pipeline complete"
i=0
while [ "$i" -lt 60 ]; do
  body="$(curl -fsS http://127.0.0.1:8006/api/v2/pipelines)"
  echo "$body" | grep '"name":"file-to-clickhouse-autocreate"' | grep '"records_written":2' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done
body="$(curl -fsS http://127.0.0.1:8006/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"records_written":2'

count="$(podman exec "$CLICKHOUSE_CONTAINER" clickhouse-client --query "SELECT count() FROM dzh3136_go.auto_customers FINAL" | tr -d '[:space:]')"
test "$count" = "2"

level_count="$(podman exec "$CLICKHOUSE_CONTAINER" clickhouse-client --query "SELECT count() FROM system.columns WHERE database='dzh3136_go' AND table='auto_customers' AND name='level'" | tr -d '[:space:]')"
test "$level_count" = "1"

gold="$(podman exec "$CLICKHOUSE_CONTAINER" clickhouse-client --query "SELECT count() FROM dzh3136_go.auto_customers FINAL WHERE id=2 AND level='gold'" | tr -d '[:space:]')"
test "$gold" = "1"

echo "ClickHouse auto-create E2E passed"
