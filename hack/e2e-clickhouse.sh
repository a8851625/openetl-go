#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
MYSQL_CONTAINER="etl-mysql-source"
CH_CONTAINER="etl-clickhouse"
APP_CONTAINER="etl-openetl-go-clickhouse"

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 90 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

if ! "$CONTAINER_CLI" image inspect docker.io/clickhouse/clickhouse-server:24.3-alpine >/dev/null 2>&1; then
  echo "ClickHouse image is not available locally. Pull it first: $CONTAINER_CLI pull docker.io/clickhouse/clickhouse-server:24.3-alpine" >&2
  exit 2
fi

echo "==> Build image"
"$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile .

echo "==> Start MySQL and ClickHouse"
compose -f docker-compose.dev.yml up -d mysql-source clickhouse

echo "==> Wait MySQL healthy"
i=0
while [ "$i" -lt 60 ]; do
  status="$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
  if [ "$status" = "healthy" ]; then
    break
  fi
  i=$((i + 1))
  sleep 2
done
[ "$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER")" = "healthy" ]

echo "==> Wait ClickHouse HTTP"
wait_http "http://127.0.0.1:8123/ping"

echo "==> Prepare ClickHouse target"
"$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --multiquery < testdata/clickhouse/init/01-init.sql
"$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "TRUNCATE TABLE dzh3136_go.customers"

echo "==> Reset test row in MySQL"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "DELETE FROM customers WHERE id=9101;"

echo "==> Reset ETL data"
rm -rf data-clickhouse
mkdir -p data-clickhouse/output data-clickhouse/checkpoint data-clickhouse/dlq logs
chmod -R a+rwX data-clickhouse
chmod a+rwX logs

echo "==> Run CDC to ClickHouse pipeline"
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p 8003:8001 \
  -v "$ROOT_DIR/testdata/pipes-clickhouse:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-clickhouse:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8003/api/v2/health"

echo "==> Wait pipeline running"
i=0
while [ "$i" -lt 60 ]; do
  body="$(curl -fsS http://127.0.0.1:8003/api/v2/pipelines)"
  echo "$body" | grep '"name":"mysql-cdc-to-clickhouse"' | grep '"status":"running"' >/dev/null 2>&1 && break
  i=$((i + 1))
  sleep 1
done

echo "==> Emit MySQL CDC event"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "INSERT INTO customers (id, name, email, phone, status, amount) VALUES (9101, 'CH CDC Alice', 'ch-cdc@example.com', '13900009101', 'active', 111.11); UPDATE customers SET amount=222.22 WHERE id=9101;"

echo "==> Verify ClickHouse row"
i=0
while [ "$i" -lt 90 ]; do
  copied="$("$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM dzh3136_go.customers FINAL WHERE id=9101 AND amount=222.22" 2>/dev/null | tr -d '[:space:]')"
  if [ "$copied" = "1" ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "$copied" = "1"

body="$(curl -fsS http://127.0.0.1:8003/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"name":"mysql-cdc-to-clickhouse"' | grep '"records_written"'

echo "ClickHouse CDC E2E passed"
