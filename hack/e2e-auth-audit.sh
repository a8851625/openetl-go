#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
APP_CONTAINER="etl-openetl-go-auth"
TOKEN="test-token-123"

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
if ! "$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile .; then
  "$CONTAINER_CLI" image inspect "$IMAGE" >/dev/null
  echo "==> Build failed; reusing existing $IMAGE"
fi

echo "==> Reset ETL data"
rm -rf data-auth
mkdir -p data-auth/output data-auth/checkpoint data-auth/dlq logs
chmod -R a+rwX data-auth
chmod a+rwX logs

echo "==> Run auth-enabled pipeline"
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p 8011:8001 \
  -e ETL_API_TOKEN="$TOKEN" \
  -v "$ROOT_DIR/testdata/pipes-auth:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-auth:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8011/api/v2/health"

echo "==> Verify unauthorized request"
status="$(curl -s -o /tmp/etl-auth-body -w '%{http_code}' http://127.0.0.1:8011/api/v2/pipelines)"
test "$status" = "401"

echo "==> Verify authorized request"
i=0
while [ "$i" -lt 60 ]; do
  body="$(curl -fsS -H "X-API-Token: $TOKEN" http://127.0.0.1:8011/api/v2/pipelines)"
  echo "$body" | grep '"name":"auth-file-to-file"' | grep '"records_written":3' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done
echo "$body"
echo "$body" | grep '"records_written":3'

echo "==> Trigger audited operation"
curl -fsS -X POST -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8011/api/v2/pipelines/auth-file-to-file/stop >/dev/null

echo "==> Verify audit log in storage"
sleep 1
audit_body="$(curl -fsS -H "X-API-Token: $TOKEN" http://127.0.0.1:8011/api/v2/audit?limit=5)"
echo "$audit_body" | grep '"action":"pipeline.stop"'
echo "$audit_body" | grep '"target":"auth-file-to-file"'

echo "Auth audit E2E passed"
