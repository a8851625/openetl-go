#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

IMAGE="openetl-go-etl:dev"
MYSQL_CONTAINER="etl-mysql-source"
APP_CONTAINER="etl-openetl-go-crash"
TOTAL=3000

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 60 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 1
  done
  return 1
}

mysql_count() {
  docker exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.crash_customers;" 2>/dev/null | tr -d '[:space:]'
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

echo "==> Prepare crash source and target"
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "CREATE DATABASE IF NOT EXISTS dzh3136_target; DROP TABLE IF EXISTS dzh3136_go.crash_customers; CREATE TABLE dzh3136_go.crash_customers LIKE dzh3136_go.customers; DROP TABLE IF EXISTS dzh3136_target.crash_customers; CREATE TABLE dzh3136_target.crash_customers LIKE dzh3136_go.customers; GRANT ALL PRIVILEGES ON dzh3136_target.* TO 'sync_user'@'%'; FLUSH PRIVILEGES; SET SESSION cte_max_recursion_depth=$((TOTAL + 10)); INSERT INTO dzh3136_go.crash_customers (id, name, email, phone, status, amount) WITH RECURSIVE seq AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM seq WHERE n < $TOTAL) SELECT n, CONCAT('Crash User ', n), CONCAT('crash', n, '@example.com'), CONCAT('139', LPAD(n, 8, '0')), 'active', n / 10 FROM seq;"

echo "==> Reset ETL data"
rm -rf data-crash
mkdir -p data-crash/output data-crash/checkpoint data-crash/dlq logs
chmod -R a+rwX data-crash
chmod a+rwX logs

echo "==> Run pipeline and kill mid-flight"
docker rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
docker run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p 8012:8001 \
  -v "$ROOT_DIR/testdata/pipes-crash:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-crash:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8012/api/v2/health"

i=0
partial=0
while [ "$i" -lt 60 ]; do
  partial="$(mysql_count)"
  if [ "$partial" -gt 20 ] && [ "$partial" -lt "$TOTAL" ]; then break; fi
  i=$((i + 1)); sleep 1
done
test "$partial" -gt 20
test "$partial" -lt "$TOTAL"
echo "==> Killing at target count=$partial"
docker kill "$APP_CONTAINER" >/dev/null

test -f data-crash/etl.db

echo "==> Restart with same checkpoint directory"
docker rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
docker run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p 8012:8001 \
  -v "$ROOT_DIR/testdata/pipes-crash:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-crash:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8012/api/v2/health"

echo "==> Wait final recovery"
i=0
final=0
while [ "$i" -lt 180 ]; do
  final="$(mysql_count)"
  if [ "$final" = "$TOTAL" ]; then break; fi
  i=$((i + 1)); sleep 1
done
test "$final" = "$TOTAL"

body="$(curl -fsS http://127.0.0.1:8012/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"name":"mysql-batch-crash-recovery"'

echo "Crash recovery E2E passed"
