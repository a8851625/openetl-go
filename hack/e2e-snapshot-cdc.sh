#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

IMAGE="openetl-go-etl:dev"
MYSQL_CONTAINER="etl-mysql-source"
APP_CONTAINER="etl-openetl-go-snapshot-cdc"

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

echo "==> Start MySQL source"
docker compose -f docker-compose.dev.yml up -d mysql-source

echo "==> Wait MySQL healthy"
i=0
while [ "$i" -lt 60 ]; do
  status="$(docker inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
  if [ "$status" = "healthy" ]; then break; fi
  i=$((i + 1)); sleep 2
done
[ "$(docker inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER")" = "healthy" ]

echo "==> Prepare source and target"
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "CREATE DATABASE IF NOT EXISTS dzh3136_target; CREATE TABLE IF NOT EXISTS dzh3136_target.customers LIKE dzh3136_go.customers; DELETE FROM dzh3136_go.customers WHERE id >= 9000; TRUNCATE TABLE dzh3136_target.customers; GRANT ALL PRIVILEGES ON dzh3136_target.* TO 'sync_user'@'%'; FLUSH PRIVILEGES;"

echo "==> Reset ETL data"
rm -rf data-snapshot-cdc
mkdir -p data-snapshot-cdc/output data-snapshot-cdc/checkpoint data-snapshot-cdc/dlq logs
chmod -R a+rwX data-snapshot-cdc
chmod a+rwX logs

echo "==> Run snapshot+CDC pipeline"
docker rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
docker run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p 8005:8001 \
  -v "$ROOT_DIR/testdata/pipes-snapshot-cdc:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-snapshot-cdc:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8005/api/v2/health"

echo "==> Wait snapshot copied"
i=0
while [ "$i" -lt 60 ]; do
  copied="$(docker exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.customers;" 2>/dev/null | tr -d '[:space:]')"
  if [ "$copied" = "5" ]; then break; fi
  i=$((i + 1)); sleep 1
done
test "$copied" = "5"

echo "==> Emit CDC after snapshot"
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "INSERT INTO customers (id, name, email, phone, status, amount) VALUES (9201, 'Snapshot CDC Alice', 'snapshot-cdc@example.com', '13900009201', 'active', 321.00); UPDATE customers SET amount=654.00 WHERE id=9201;"

echo "==> Verify CDC copied"
i=0
while [ "$i" -lt 60 ]; do
  copied="$(docker exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.customers WHERE id=9201 AND amount=654.00;" 2>/dev/null | tr -d '[:space:]')"
  if [ "$copied" = "1" ]; then break; fi
  i=$((i + 1)); sleep 1
done
test "$copied" = "1"

body="$(curl -fsS http://127.0.0.1:8005/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"name":"mysql-snapshot-cdc-to-mysql"' | grep '"status":"running"'
echo "$body" | grep '"records_written":7'

test -f data-snapshot-cdc/etl.db

echo "Snapshot+CDC E2E passed"
