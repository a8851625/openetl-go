#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

IMAGE="openetl-go-etl:dev"
MYSQL_CONTAINER="etl-mysql-source"
APP_CONTAINER="etl-openetl-go-cdc-crash"

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 60 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 1
  done
  return 1
}

target_count() {
  podman exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.customers WHERE id IN (9301,9302);" 2>/dev/null | tr -d '[:space:]'
}

echo "==> Build image"
podman build -t "$IMAGE" -f Dockerfile .

echo "==> Start MySQL source"
podman-compose -f docker-compose.dev.yml up -d mysql-source

echo "==> Wait MySQL healthy"
i=0
while [ "$i" -lt 60 ]; do
  status="$(podman inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
  if [ "$status" = "healthy" ]; then break; fi
  i=$((i + 1)); sleep 2
done
[ "$(podman inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER")" = "healthy" ]

echo "==> Prepare CDC crash target"
podman exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "CREATE DATABASE IF NOT EXISTS dzh3136_target; CREATE TABLE IF NOT EXISTS dzh3136_target.customers LIKE dzh3136_go.customers; DELETE FROM dzh3136_go.customers WHERE id IN (9301,9302); DELETE FROM dzh3136_target.customers WHERE id IN (9301,9302); GRANT ALL PRIVILEGES ON dzh3136_target.* TO 'sync_user'@'%'; FLUSH PRIVILEGES;"

echo "==> Reset ETL data"
rm -rf data-cdc-crash
mkdir -p data-cdc-crash/output data-cdc-crash/checkpoint data-cdc-crash/dlq logs

echo "==> Start CDC pipeline"
podman rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
podman run -d \
  --name "$APP_CONTAINER" \
  -p 8014:8001 \
  -v "$ROOT_DIR/testdata/pipes-cdc-crash:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-cdc-crash:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8014/api/v2/health"

echo "==> Emit first CDC change"
podman exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "INSERT INTO customers (id, name, email, phone, status, amount) VALUES (9301, 'CDC Crash One', 'cdc-crash-1@example.com', '13900009301', 'active', 101.00);"

i=0
while [ "$i" -lt 60 ]; do
  if [ "$(target_count)" = "1" ]; then break; fi
  i=$((i + 1)); sleep 1
done
test "$(target_count)" = "1"
test -f data-cdc-crash/etl.db

echo "==> Kill CDC pipeline"
podman kill "$APP_CONTAINER" >/dev/null

echo "==> Restart CDC pipeline with checkpoint"
podman rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
podman run -d \
  --name "$APP_CONTAINER" \
  -p 8014:8001 \
  -v "$ROOT_DIR/testdata/pipes-cdc-crash:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-cdc-crash:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8014/api/v2/health"
sleep 2

echo "==> Emit second CDC change"
podman exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "INSERT INTO customers (id, name, email, phone, status, amount) VALUES (9302, 'CDC Crash Two', 'cdc-crash-2@example.com', '13900009302', 'active', 202.00); UPDATE customers SET amount=303.00 WHERE id=9302;"

i=0
while [ "$i" -lt 60 ]; do
  count="$(target_count)"
  amount="$(podman exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.customers WHERE id=9302 AND amount=303.00;" 2>/dev/null | tr -d '[:space:]')"
  if [ "$count" = "2" ] && [ "$amount" = "1" ]; then break; fi
  i=$((i + 1)); sleep 1
done
test "$(target_count)" = "2"
amount="$(podman exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.customers WHERE id=9302 AND amount=303.00;" 2>/dev/null | tr -d '[:space:]')"
test "$amount" = "1"

body="$(curl -fsS http://127.0.0.1:8014/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"name":"mysql-cdc-crash-to-mysql"'

echo "CDC crash recovery E2E passed"
