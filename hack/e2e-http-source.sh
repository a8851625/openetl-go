#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
FIXTURE_CONTAINER="etl-http-fixture"
APP_CONTAINER="etl-openetl-go-http"

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

echo "==> Start HTTP fixture"
"$CONTAINER_CLI" rm -f "$FIXTURE_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$FIXTURE_CONTAINER" \
  -p 18080:8080 \
  -v "$ROOT_DIR/testdata/http-fixture:/fixture:ro" \
  docker.io/library/python:3.12-alpine \
  python /fixture/server.py

wait_http "http://127.0.0.1:18080/health"

echo "==> Reset ETL data"
rm -rf data-http
mkdir -p data-http/output data-http/checkpoint data-http/dlq logs
chmod -R a+rwX data-http
chmod a+rwX logs

echo "==> Run HTTP pipeline"
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p 8008:8001 \
  -v "$ROOT_DIR/testdata/pipes-http:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-http:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8008/api/v2/health"

echo "==> Wait pipeline complete"
i=0
while [ "$i" -lt 60 ]; do
  body="$(curl -fsS http://127.0.0.1:8008/api/v2/pipelines)"
  echo "$body" | grep '"name":"http-to-file"' | grep '"records_written":3' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done
body="$(curl -fsS http://127.0.0.1:8008/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"records_written":3'

echo "==> Verify output files"
grep -R 'HTTP Ada' data-http/output/http
grep -R 'HTTP Alan' data-http/output/http
grep -R 'HTTP Grace' data-http/output/http

echo "HTTP source E2E passed"
