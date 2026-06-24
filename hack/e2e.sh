#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

IMAGE="openetl-go-etl:dev"
MYSQL_CONTAINER="etl-mysql-source"
APP_CONTAINER="etl-openetl-go"
E2E_PIPES_DIR="$ROOT_DIR/data/e2e-pipes"

cleanup_app() {
  podman rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
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

if [ "${E2E_SKIP_BUILD:-0}" = "1" ]; then
  echo "==> Skip image build (E2E_SKIP_BUILD=1, using $IMAGE)"
else
  echo "==> Build image"
  podman build -t "$IMAGE" -f Dockerfile .
fi

echo "==> Start MySQL source"
podman compose -f docker-compose.dev.yml up -d mysql-source

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

echo "==> Prepare MySQL target"
podman exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "CREATE DATABASE IF NOT EXISTS dzh3136_target; CREATE TABLE IF NOT EXISTS dzh3136_target.customers LIKE dzh3136_go.customers; DELETE FROM dzh3136_go.customers WHERE id >= 9000; TRUNCATE TABLE dzh3136_target.customers; GRANT ALL PRIVILEGES ON dzh3136_target.* TO 'sync_user'@'%'; FLUSH PRIVILEGES;"

echo "==> Reset ETL data"
rm -rf data/output data/checkpoint data/dlq data/etl.db data/etl.db-* "$E2E_PIPES_DIR"
mkdir -p data/output data/checkpoint data/dlq data/input "$E2E_PIPES_DIR" logs
cp testdata/files/customers.jsonl data/input/customers.jsonl
cp pipes/file-to-file.yaml "$E2E_PIPES_DIR/file-to-file.yaml"
cp pipes/mysql-batch-to-file.yaml "$E2E_PIPES_DIR/mysql-batch-to-file.yaml"
cp pipes/mysql-batch-to-mysql.yaml "$E2E_PIPES_DIR/mysql-batch-to-mysql.yaml"

echo "==> Run ETL service"
cleanup_app
podman run -d \
  --name "$APP_CONTAINER" \
  -p 8001:8001 \
  -v "$E2E_PIPES_DIR:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8001/api/v2/health"

echo "==> Wait pipelines complete"
i=0
while [ "$i" -lt 60 ]; do
  body="$(curl -fsS http://127.0.0.1:8001/api/v2/pipelines)"
  echo "$body" | grep '"name":"file-to-file"' | grep '"status":"completed"' >/dev/null 2>&1 && \
  echo "$body" | grep '"name":"mysql-batch-to-file"' | grep '"status":"completed"' >/dev/null 2>&1 && \
  echo "$body" | grep '"name":"mysql-batch-to-mysql"' | grep '"status":"completed"' >/dev/null 2>&1 && break
  i=$((i + 1))
  sleep 1
done

body="$(curl -fsS http://127.0.0.1:8001/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"name":"file-to-file"' | grep '"records_written":3'
echo "$body" | grep '"name":"mysql-batch-to-file"' | grep '"records_written":5'
echo "$body" | grep '"name":"mysql-batch-to-mysql"' | grep '"records_written":5'

echo "==> Verify output files"
file_to_file_out="$(find data/output -type f \( -path '*/customers_*/*.jsonl' -o -name 'customers_*.jsonl' \) | sort | tr '\n' ' ')"
mysql_file_out="$(find data/output -type f \( -path '*/mysql_customers_*/*.jsonl' -o -name 'mysql_customers_*.jsonl' \) | sort | tr '\n' ' ')"
[ -n "$file_to_file_out" ]
[ -n "$mysql_file_out" ]
test "$(cat $file_to_file_out | wc -l | tr -d ' ')" = "3"
test "$(cat $mysql_file_out | wc -l | tr -d ' ')" = "5"
grep 'Ada Lovelace' $file_to_file_out >/dev/null

echo "==> Verify MySQL sink"
copied="$(podman exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.customers;" 2>/dev/null | tr -d '[:space:]')"
test "$copied" = "5"

echo "==> Verify checkpoints in storage"
test -f data/etl.db

echo "E2E passed"
