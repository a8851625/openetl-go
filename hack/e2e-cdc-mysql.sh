#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

IMAGE="openetl-go-etl:dev"
MYSQL_CONTAINER="etl-mysql-source"
APP_CONTAINER="etl-openetl-go-cdc"

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 60 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

echo "==> Build image"
podman build -t "$IMAGE" -f Dockerfile .

echo "==> Start MySQL source"
podman-compose -f docker-compose.dev.yml up -d mysql-source

echo "==> Wait MySQL healthy"
i=0
while [ "$i" -lt 60 ]; do
  status="$(podman inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
  if [ "$status" = "healthy" ]; then
    break
  fi
  i=$((i + 1))
  sleep 2
done
[ "$(podman inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER")" = "healthy" ]

echo "==> Prepare CDC target"
podman exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "CREATE DATABASE IF NOT EXISTS dzh3136_target; CREATE TABLE IF NOT EXISTS dzh3136_target.customers LIKE dzh3136_go.customers; DELETE FROM dzh3136_target.customers WHERE id >= 9000; GRANT ALL PRIVILEGES ON dzh3136_target.* TO 'sync_user'@'%'; FLUSH PRIVILEGES;"

echo "==> Reset ETL data"
rm -rf data-cdc
mkdir -p data-cdc/output data-cdc/checkpoint data-cdc/dlq logs

echo "==> Run CDC pipeline"
podman rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
podman run -d \
  --name "$APP_CONTAINER" \
  -p 8002:8001 \
  -v "$ROOT_DIR/testdata/pipes-cdc:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-cdc:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8002/api/v2/health"

echo "==> Wait CDC pipeline running"
i=0
while [ "$i" -lt 60 ]; do
  body="$(curl -fsS http://127.0.0.1:8002/api/v2/pipelines)"
  echo "$body" | grep '"name":"mysql-cdc-to-mysql"' | grep '"status":"running"' >/dev/null 2>&1 && break
  i=$((i + 1))
  sleep 1
done

echo "==> Emit CDC insert/update/delete"
podman exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "DELETE FROM customers WHERE id=9001; INSERT INTO customers (id, name, email, phone, status, amount) VALUES (9001, 'CDC Alice', 'cdc-alice@example.com', '13900009001', 'active', 123.45); UPDATE customers SET amount=678.90 WHERE id=9001;"

echo "==> Verify CDC target row"
i=0
while [ "$i" -lt 60 ]; do
  copied="$(podman exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.customers WHERE id=9001 AND amount=678.90;" 2>/dev/null | tr -d '[:space:]')"
  if [ "$copied" = "1" ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "$copied" = "1"

body="$(curl -fsS http://127.0.0.1:8002/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"name":"mysql-cdc-to-mysql"' | grep '"records_written"'

echo "CDC E2E passed"
