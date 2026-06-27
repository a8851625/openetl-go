#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
MYSQL_CONTAINER="etl-mysql-source"
PG_CONTAINER="etl-postgres-cdc-target"
APP_CONTAINER="etl-openetl-go-cdc-postgres"
PG_PORT="15434"
APP_PORT="8022"
PIPELINE="mysql-cdc-to-postgres"

cleanup() {
  "$CONTAINER_CLI" rm -f "$APP_CONTAINER" "$PG_CONTAINER" >/dev/null 2>&1 || true
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

wait_pipeline_status() {
  status="$1"
  i=0
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

echo "==> Start MySQL source"
compose -f docker-compose.dev.yml up -d mysql-source

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

echo "==> Start PostgreSQL target"
cleanup
"$CONTAINER_CLI" run -d --name "$PG_CONTAINER" \
  --add-host host.docker.internal:host-gateway \
  -e POSTGRES_DB=analytics \
  -e POSTGRES_USER=etl \
  -e POSTGRES_PASSWORD=etl123 \
  -p "$PG_PORT:5432" \
  docker.io/library/postgres:16-alpine >/dev/null
wait_pg

echo "==> Prepare MySQL CDC source table"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "
CREATE DATABASE IF NOT EXISTS dzh3136_go;
DROP TABLE IF EXISTS dzh3136_go.pg_cdc_customers;
CREATE TABLE dzh3136_go.pg_cdc_customers (
  id INT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  email VARCHAR(200),
  status VARCHAR(20) DEFAULT 'active',
  amount DECIMAL(12,2) DEFAULT 0.00,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
GRANT SELECT ON dzh3136_go.pg_cdc_customers TO 'sync_user'@'%';
FLUSH PRIVILEGES;
"

echo "==> Prepare PostgreSQL target table"
"$CONTAINER_CLI" exec "$PG_CONTAINER" psql -U etl -d analytics -v ON_ERROR_STOP=1 -c "
DROP TABLE IF EXISTS pg_cdc_customers;
CREATE TABLE pg_cdc_customers (
  id INT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  email VARCHAR(200),
  status VARCHAR(20),
  amount NUMERIC(12,2),
  updated_at TIMESTAMP
);
"

echo "==> Reset ETL data"
rm -rf data-cdc-postgres
mkdir -p data-cdc-postgres/output data-cdc-postgres/checkpoint data-cdc-postgres/dlq logs
chmod -R a+rwX data-cdc-postgres
chmod a+rwX logs

echo "==> Run MySQL CDC -> PostgreSQL pipeline"
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p "$APP_PORT:8001" \
  -v "$ROOT_DIR/testdata/pipes-cdc-postgres:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-cdc-postgres:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE" >/dev/null

wait_http "http://127.0.0.1:$APP_PORT/api/v2/health"
wait_pipeline_status "running"

echo "==> Emit CDC insert and update"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "
INSERT INTO pg_cdc_customers (id, name, email, status, amount) VALUES
  (9101, 'CDC Postgres Alice', 'cdc-pg-alice@example.com', 'active', 100.25);
UPDATE pg_cdc_customers SET amount=321.50, status='vip' WHERE id=9101;
"

echo "==> Verify PostgreSQL upserted row"
i=0
while [ "$i" -lt 60 ]; do
  row="$("$CONTAINER_CLI" exec "$PG_CONTAINER" psql -U etl -d analytics -At -c "SELECT status || '|' || amount::text FROM pg_cdc_customers WHERE id=9101;" | tr -d '[:space:]')"
  if [ "$row" = "vip|321.50" ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "$row" = "vip|321.50"

echo "==> Emit CDC delete"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "DELETE FROM pg_cdc_customers WHERE id=9101;"
i=0
while [ "$i" -lt 60 ]; do
  deleted="$("$CONTAINER_CLI" exec "$PG_CONTAINER" psql -U etl -d analytics -At -c "SELECT COUNT(*) FROM pg_cdc_customers WHERE id=9101;" | tr -d '[:space:]')"
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
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "
INSERT INTO pg_cdc_customers (id, name, email, status, amount) VALUES
  (9102, 'CDC Postgres Bob', 'cdc-pg-bob@example.com', 'pending', 456.75);
"
start_pipeline
wait_pipeline_status "running"

echo "==> Verify checkpoint restart consumed stopped-period event"
i=0
while [ "$i" -lt 60 ]; do
  row2="$("$CONTAINER_CLI" exec "$PG_CONTAINER" psql -U etl -d analytics -At -c "SELECT status || '|' || amount::text FROM pg_cdc_customers WHERE id=9102;" | tr -d '[:space:]')"
  if [ "$row2" = "pending|456.75" ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "$row2" = "pending|456.75"
deleted_after_restart="$("$CONTAINER_CLI" exec "$PG_CONTAINER" psql -U etl -d analytics -At -c "SELECT COUNT(*) FROM pg_cdc_customers WHERE id=9101;" | tr -d '[:space:]')"
test "$deleted_after_restart" = "0"

body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines")"
echo "$body"
echo "$body" | grep "\"name\":\"$PIPELINE\"" | grep '"status":"running"'
echo "$body" | grep "\"name\":\"$PIPELINE\"" | grep '"records_failed":0'

echo "MySQL CDC -> PostgreSQL E2E passed"
