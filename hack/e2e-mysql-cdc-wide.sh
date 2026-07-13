#!/bin/sh
# Scenario 2: multi-table mysql_cdc -> cdc_policy(orders) -> lookup -> ClickHouse wide table.
set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
MYSQL_CONTAINER="etl-mysql-source"
CH_CONTAINER="etl-clickhouse"
APP_CONTAINER="etl-openetl-go-mysql-cdc-wide"
API_PORT=8022

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

if [ "${E2E_SKIP_BUILD:-0}" = "1" ]; then
  echo "==> Skip image build (E2E_SKIP_BUILD=1)"
else
  echo "==> Build image"
  "$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile .
fi

echo "==> Start MySQL and ClickHouse"
compose -f docker-compose.dev.yml up -d mysql-source clickhouse

echo "==> Wait MySQL healthy"
i=0
while [ "$i" -lt 90 ]; do
  status="$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
  if [ "$status" = "healthy" ]; then
    break
  fi
  i=$((i + 1))
  sleep 2
done
[ "$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER")" = "healthy" ]

echo "==> Wait ClickHouse"
wait_http "http://127.0.0.1:8123/ping"

echo "==> Prepare dim + fact tables"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "
CREATE TABLE IF NOT EXISTS dim_users (
  id BIGINT PRIMARY KEY,
  name VARCHAR(128),
  tier VARCHAR(32),
  region VARCHAR(32)
);
DELETE FROM dim_users WHERE id IN (2001,2002);
INSERT INTO dim_users (id, name, tier, region) VALUES
  (2001, 'Wide Alice', 'vip', 'east'),
  (2002, 'Wide Bob', 'standard', 'west');
DELETE FROM orders WHERE id >= 9400;
INSERT INTO customers (id, name, email, phone, status, amount)
VALUES (2001, 'Wide Alice', 'wa@example.com', '13900002001', 'active', 1.00)
ON DUPLICATE KEY UPDATE name=VALUES(name);
INSERT INTO customers (id, name, email, phone, status, amount)
VALUES (2002, 'Wide Bob', 'wb@example.com', '13900002002', 'active', 1.00)
ON DUPLICATE KEY UPDATE name=VALUES(name);
"

"$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --multiquery "
CREATE DATABASE IF NOT EXISTS wide;
DROP TABLE IF EXISTS wide.order_detail_wide;
"

echo "==> Reset ETL data"
rm -rf data-mysql-cdc-wide
mkdir -p data-mysql-cdc-wide/output data-mysql-cdc-wide/checkpoint data-mysql-cdc-wide/dlq logs
chmod -R a+rwX data-mysql-cdc-wide
chmod a+rwX logs

echo "==> Run mysql-cdc wide-table pipeline"
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p "${API_PORT}:8001" \
  -v "$ROOT_DIR/testdata/pipes-mysql-cdc-wide:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-mysql-cdc-wide:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE" >/dev/null

wait_http "http://127.0.0.1:${API_PORT}/api/v2/health"

echo "==> Wait pipeline running"
i=0
while [ "$i" -lt 60 ]; do
  body="$(curl -fsS "http://127.0.0.1:${API_PORT}/api/v2/pipelines" 2>/dev/null || true)"
  echo "$body" | grep '"name":"mysql-cdc-wide-detail-clickhouse"' | grep '"status":"running"' >/dev/null 2>&1 && break
  i=$((i + 1))
  sleep 1
done

echo "==> Emit multi-table CDC (orders + noise on customers/products)"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "
INSERT INTO orders (id, order_no, customer_id, product_name, quantity, price, order_status)
VALUES (9401, 'ORD-WIDE-9401', 2001, 'Wide Widget', 2, 12.50, 'paid');
INSERT INTO orders (id, order_no, customer_id, product_name, quantity, price, order_status)
VALUES (9402, 'ORD-WIDE-9402', 2002, 'Wide Gadget', 1, 20.00, 'paid');
UPDATE customers SET amount=999.00 WHERE id=1;
UPDATE products SET stock=stock+1 WHERE id=1;
"

echo "==> Verify ClickHouse wide table with lookup fields"
i=0
while [ "$i" -lt 90 ]; do
  detail_count="$("$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM wide.order_detail_wide FINAL WHERE id IN (9401,9402)" 2>/dev/null | tr -d '[:space:]' || true)"
  alice="$("$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM wide.order_detail_wide FINAL WHERE id=9401 AND user_name='Wide Alice' AND user_tier='vip' AND user_region='east'" 2>/dev/null | tr -d '[:space:]' || true)"
  bob="$("$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "SELECT count() FROM wide.order_detail_wide FINAL WHERE id=9402 AND user_name='Wide Bob' AND user_tier='standard' AND user_region='west'" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "$detail_count" = "2" ] && [ "$alice" = "1" ] && [ "$bob" = "1" ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "$detail_count" = "2"
test "$alice" = "1"
test "$bob" = "1"

body="$(curl -fsS "http://127.0.0.1:${API_PORT}/api/v2/pipelines")"
echo "$body"
echo "$body" | grep '"name":"mysql-cdc-wide-detail-clickhouse"'

echo "MySQL CDC wide-table E2E passed"
