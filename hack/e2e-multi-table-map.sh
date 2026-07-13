#!/bin/sh
# Scenario 1: multi-table snapshot+CDC A->B with table_mapping rename.
set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
MYSQL_CONTAINER="etl-mysql-source"
APP_CONTAINER="etl-openetl-go-multi-table-map"
API_PORT=8021

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

echo "==> Start MySQL source"
compose -f docker-compose.dev.yml up -d mysql-source

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

echo "==> Prepare multi-table source/target"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "
CREATE DATABASE IF NOT EXISTS dzh3136_target;
DROP TABLE IF EXISTS dzh3136_target.ods_customers;
DROP TABLE IF EXISTS dzh3136_target.ods_products;
DELETE FROM dzh3136_go.customers WHERE id >= 9300;
DELETE FROM dzh3136_go.products WHERE id >= 9300;
GRANT ALL PRIVILEGES ON dzh3136_target.* TO 'sync_user'@'%';
FLUSH PRIVILEGES;
"

echo "==> Reset ETL data"
rm -rf data-multi-table-map
mkdir -p data-multi-table-map/output data-multi-table-map/checkpoint data-multi-table-map/dlq logs
chmod -R a+rwX data-multi-table-map
chmod a+rwX logs

echo "==> Run multi-table mapping pipeline"
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p "${API_PORT}:8001" \
  -v "$ROOT_DIR/testdata/pipes-multi-table-map:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-multi-table-map:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE" >/dev/null

wait_http "http://127.0.0.1:${API_PORT}/api/v2/health"

echo "==> Wait snapshot mapped tables populated"
i=0
while [ "$i" -lt 90 ]; do
  cust="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.ods_customers;" 2>/dev/null | tr -d '[:space:]' || true)"
  prod="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.ods_products;" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "${cust:-0}" -ge 1 ] && [ "${prod:-0}" -ge 1 ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "${cust:-0}" -ge 1
test "${prod:-0}" -ge 1

echo "==> Emit CDC on both mapped tables"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "
INSERT INTO customers (id, name, email, phone, status, amount)
VALUES (9301, 'Map Cust', 'map@example.com', '13900009301', 'active', 11.00)
ON DUPLICATE KEY UPDATE amount=22.00, name='Map Cust Updated';
UPDATE customers SET amount=33.50, name='Map Cust CDC' WHERE id=9301;
INSERT INTO products (id, sku, name, category, price, stock)
VALUES (9301, 'SKU-MAP-9301', 'Mapped Product', 'e2e', 9.99, 5)
ON DUPLICATE KEY UPDATE price=19.99, name='Mapped Product CDC';
UPDATE products SET price=29.99, stock=7, name='Mapped Product CDC2' WHERE id=9301;
"

echo "==> Verify CDC landed on mapped target tables"
i=0
while [ "$i" -lt 90 ]; do
  cust_cdc="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.ods_customers WHERE id=9301 AND amount=33.50;" 2>/dev/null | tr -d '[:space:]' || true)"
  prod_cdc="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.ods_products WHERE id=9301 AND price=29.99;" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "$cust_cdc" = "1" ] && [ "$prod_cdc" = "1" ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "$cust_cdc" = "1"
test "$prod_cdc" = "1"

# Ensure unmapped original table names were not used as target tables.
unmapped="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='dzh3136_target' AND table_name IN ('customers','products');" 2>/dev/null | tr -d '[:space:]' || true)"
test "$unmapped" = "0"

body="$(curl -fsS "http://127.0.0.1:${API_PORT}/api/v2/pipelines")"
echo "$body"
echo "$body" | grep '"name":"multi-table-map-to-mysql"'

echo "Multi-table mapping E2E passed"
