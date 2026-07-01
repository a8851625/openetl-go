#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
MYSQL_CONTAINER="etl-mysql-postgres-cdc-target"
PG_CONTAINER="etl-postgres-cdc-source"
APP_CONTAINER="etl-openetl-go-postgres-cdc"
PG_PORT="15435"
MYSQL_PORT="15436"
APP_PORT="8035"
PIPELINE="postgres-cdc-to-mysql"

cleanup() {
  "$CONTAINER_CLI" rm -f "$APP_CONTAINER" "$PG_CONTAINER" "$MYSQL_CONTAINER" >/dev/null 2>&1 || true
}

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

wait_pg() {
  i=0
  while [ "$i" -lt 60 ]; do
    if "$CONTAINER_CLI" exec "$PG_CONTAINER" pg_isready -U etl -d analytics >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 2
  done
  return 1
}

wait_mysql() {
  i=0
  while [ "$i" -lt 60 ]; do
    if "$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysqladmin ping -uroot -proot123456 --silent >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 2
  done
  return 1
}

wait_pipeline_status() {
  status="$1"
  i=0
  body=""
  while [ "$i" -lt 60 ]; do
    body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines")"
    echo "$body" | grep "\"name\":\"$PIPELINE\"" | grep "\"status\":\"$status\"" >/dev/null 2>&1 && return 0
    i=$((i + 1))
    sleep 1
  done
  echo "$body"
  return 1
}

start_pipeline() {
  i=0
  while [ "$i" -lt 30 ]; do
    body="$(curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/start")"
    echo "$body"
    if echo "$body" | grep '"error"' >/dev/null 2>&1; then
      i=$((i + 1))
      sleep 1
      continue
    fi
    return 0
  done
  return 1
}

if [ "${E2E_SKIP_BUILD:-0}" = "1" ]; then
  echo "==> Skip image build (E2E_SKIP_BUILD=1, using $IMAGE)"
else
  echo "==> Build image"
  "$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile .
fi

echo "==> Start MySQL target"
cleanup
"$CONTAINER_CLI" run -d --name "$MYSQL_CONTAINER" \
  --add-host host.docker.internal:host-gateway \
  -e MYSQL_ROOT_PASSWORD=root123456 \
  -e MYSQL_DATABASE=dzh3136_target \
  -e MYSQL_USER=sync_user \
  -e MYSQL_PASSWORD=sync_password_123 \
  -p "$MYSQL_PORT:3306" \
  docker.io/library/mysql:8.0 \
  --default-authentication-plugin=mysql_native_password >/dev/null
wait_mysql

echo "==> Start PostgreSQL CDC source"
"$CONTAINER_CLI" run -d --name "$PG_CONTAINER" \
  --add-host host.docker.internal:host-gateway \
  -e POSTGRES_DB=analytics \
  -e POSTGRES_USER=etl \
  -e POSTGRES_PASSWORD=etl123 \
  -p "$PG_PORT:5432" \
  docker.io/library/postgres:16-alpine \
  postgres -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10 >/dev/null
wait_pg

echo "==> Prepare PostgreSQL source table"
"$CONTAINER_CLI" exec "$PG_CONTAINER" psql -U etl -d analytics -v ON_ERROR_STOP=1 -c "
DROP PUBLICATION IF EXISTS etl_pub;
SELECT pg_drop_replication_slot(slot_name)
  FROM pg_replication_slots
 WHERE slot_name = 'openetl_pg_cdc_e2e';
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
  id INT PRIMARY KEY,
  customer_name VARCHAR(100) NOT NULL,
  status VARCHAR(20) NOT NULL,
  amount NUMERIC(12,2) NOT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"

echo "==> Prepare MySQL target table"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "
CREATE DATABASE IF NOT EXISTS dzh3136_target;
GRANT ALL PRIVILEGES ON dzh3136_target.* TO 'sync_user'@'%';
DROP TABLE IF EXISTS dzh3136_target.orders;
CREATE TABLE dzh3136_target.orders (
  id INT PRIMARY KEY,
  customer_name VARCHAR(100) NOT NULL,
  status VARCHAR(20) NOT NULL,
  amount DECIMAL(12,2) NOT NULL,
  updated_at DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
FLUSH PRIVILEGES;
"

echo "==> Reset ETL data"
rm -rf data-postgres-cdc
mkdir -p data-postgres-cdc/output data-postgres-cdc/checkpoint data-postgres-cdc/dlq logs
chmod -R a+rwX data-postgres-cdc
chmod a+rwX logs

echo "==> Run PostgreSQL CDC -> MySQL pipeline"
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p "$APP_PORT:8001" \
  -v "$ROOT_DIR/testdata/pipes-postgres-cdc:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-postgres-cdc:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE" >/dev/null

wait_http "http://127.0.0.1:$APP_PORT/api/v2/health"
wait_pipeline_status "running"

echo "==> Emit PostgreSQL insert and update"
"$CONTAINER_CLI" exec "$PG_CONTAINER" psql -U etl -d analytics -v ON_ERROR_STOP=1 -c "
INSERT INTO orders (id, customer_name, status, amount) VALUES
  (9301, 'PG CDC Alice', 'new', 100.25);
UPDATE orders
   SET status = 'paid', amount = 321.50, updated_at = CURRENT_TIMESTAMP
 WHERE id = 9301;
"

echo "==> Verify MySQL upserted row"
i=0
row=""
while [ "$i" -lt 60 ]; do
  row="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -uroot -proot123456 dzh3136_target -e "SELECT CONCAT(status, '|', amount) FROM orders WHERE id=9301;" | tr -d '[:space:]')"
  if [ "$row" = "paid|321.50" ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "$row" = "paid|321.50"

echo "==> Emit PostgreSQL delete"
"$CONTAINER_CLI" exec "$PG_CONTAINER" psql -U etl -d analytics -v ON_ERROR_STOP=1 -c "DELETE FROM orders WHERE id=9301;"
i=0
deleted=""
while [ "$i" -lt 60 ]; do
  deleted="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -uroot -proot123456 dzh3136_target -e "SELECT COUNT(*) FROM orders WHERE id=9301;" | tr -d '[:space:]')"
  if [ "$deleted" = "0" ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "$deleted" = "0"

echo "==> Stop pipeline, emit event, restart from checkpoint"
curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/stop" >/dev/null
sleep 3
"$CONTAINER_CLI" exec "$PG_CONTAINER" psql -U etl -d analytics -v ON_ERROR_STOP=1 -c "
INSERT INTO orders (id, customer_name, status, amount) VALUES
  (9302, 'PG CDC Bob', 'pending', 456.75);
"
start_pipeline
wait_pipeline_status "running"

echo "==> Verify checkpoint restart consumed stopped-period event"
i=0
row2=""
while [ "$i" -lt 60 ]; do
  row2="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -uroot -proot123456 dzh3136_target -e "SELECT CONCAT(status, '|', amount) FROM orders WHERE id=9302;" | tr -d '[:space:]')"
  if [ "$row2" = "pending|456.75" ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "$row2" = "pending|456.75"

body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines")"
echo "$body"
echo "$body" | grep "\"name\":\"$PIPELINE\"" | grep '"status":"running"'
echo "$body" | grep "\"name\":\"$PIPELINE\"" | grep '"records_failed":0'

echo "PostgreSQL CDC source E2E passed"
