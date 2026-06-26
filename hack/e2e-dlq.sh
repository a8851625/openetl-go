#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

IMAGE="openetl-go-etl:dev"
MYSQL_CONTAINER="etl-mysql-source"
APP_CONTAINER="etl-openetl-go-dlq"

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
docker build -t "$IMAGE" -f Dockerfile .

echo "==> Start MySQL source"
docker compose -f docker-compose.dev.yml up -d mysql-source

echo "==> Wait MySQL healthy"
i=0
while [ "$i" -lt 60 ]; do
  status="$(docker inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
  if [ "$status" = "healthy" ]; then break; fi
  i=$((i + 1))
  sleep 2
done
[ "$(docker inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER")" = "healthy" ]

echo "==> Prepare missing sink table"
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "CREATE DATABASE IF NOT EXISTS dzh3136_target; DROP TABLE IF EXISTS dzh3136_target.dlq_customers; GRANT ALL PRIVILEGES ON dzh3136_target.* TO 'sync_user'@'%'; FLUSH PRIVILEGES;"

echo "==> Reset ETL data"
rm -rf data-dlq
mkdir -p data-dlq/output data-dlq/checkpoint data-dlq/dlq logs
chmod -R a+rwX data-dlq
chmod a+rwX logs

echo "==> Run failing pipeline"
docker rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
docker run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p 8004:8001 \
  -v "$ROOT_DIR/testdata/pipes-dlq:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-dlq:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8004/api/v2/health"

echo "==> Wait DLQ record"
i=0
while [ "$i" -lt 60 ]; do
  body="$(curl -fsS http://127.0.0.1:8004/api/v2/pipelines)"
  echo "$body" | grep '"name":"file-to-missing-mysql"' | grep '"records_dlq":2' >/dev/null 2>&1 && break
  i=$((i + 1))
  sleep 1
done
body="$(curl -fsS http://127.0.0.1:8004/api/v2/dlq/file-to-missing-mysql)"
echo "$body"
echo "$body" | grep 'DLQ Alice'
echo "$body" | grep 'DLQ Bob'

echo "==> Repair sink and selectively replay"
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "CREATE TABLE dzh3136_target.dlq_customers LIKE dzh3136_go.customers;"
replay="$(curl -fsS -X POST 'http://127.0.0.1:8004/api/v2/dlq/file-to-missing-mysql/replay?contains=9901')"
echo "$replay"
echo "$replay" | grep '"replayed":1'

copied="$(docker exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.dlq_customers WHERE id=9901;" 2>/dev/null | tr -d '[:space:]')"
test "$copied" = "1"
not_copied="$(docker exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.dlq_customers WHERE id=9902;" 2>/dev/null | tr -d '[:space:]')"
test "$not_copied" = "0"

body="$(curl -fsS http://127.0.0.1:8004/api/v2/dlq/file-to-missing-mysql)"
echo "$body"
echo "$body" | grep 'DLQ Bob'

echo "==> Replay remaining DLQ"
replay="$(curl -fsS -X POST http://127.0.0.1:8004/api/v2/dlq/file-to-missing-mysql/replay)"
echo "$replay"
echo "$replay" | grep '"replayed":1'
copied="$(docker exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.dlq_customers WHERE id IN (9901,9902);" 2>/dev/null | tr -d '[:space:]')"
test "$copied" = "2"
body="$(curl -fsS http://127.0.0.1:8004/api/v2/dlq/file-to-missing-mysql)"
echo "$body"
echo "$body" | grep '"items":\[\]'

echo "DLQ E2E passed"
