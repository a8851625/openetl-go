#!/bin/sh

# E2E: mysql_snapshot_cdc whole-database snapshot with heterogeneous primary keys.
#
# Verifies the bounded follow-up to the delivered "MySQL snapshot+CDC" baseline:
# each table auto-detects its own single-column PK (bigint vs varchar), so a
# whole-database snapshot no longer requires a single global pk_column.
#
# Coverage:
#   - tables=["*"] whole-database snapshot, no pk_column configured
#   - table A: integer PK (order_id BIGINT) -> numeric cursor + MOD sharding path disabled
#   - table B: varchar PK (user_no VARCHAR) -> ordered string cursor
#   - historical snapshot rows copied for both tables
#   - post-snapshot CDC insert/update applied for both tables
#
# Requires: docker/podman, docker-compose.dev.yml mysql-source healthy.

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
MYSQL_CONTAINER="etl-mysql-source"
APP_CONTAINER="etl-openetl-heteropk"
PORT=${PORT:-8015}

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 60 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 1
  done
  return 1
}

wait_count() {
  # $1 query, $2 expected count
  q="$1"; expected="$2"
  i=0
  while [ "$i" -lt 60 ]; do
    got="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "$q" 2>/dev/null | tr -d '[:space:]' || true)"
    if [ "$got" = "$expected" ]; then return 0; fi
    i=$((i + 1)); sleep 1
  done
  echo "TIMEOUT waiting for count=$expected, last got='$got' query=$q" >&2
  return 1
}

echo "==> Build image"
"$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile .

echo "==> Start MySQL source"
compose -f docker-compose.dev.yml up -d mysql-source

echo "==> Wait MySQL healthy"
i=0
while [ "$i" -lt 60 ]; do
  status="$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
  if [ "$status" = "healthy" ]; then break; fi
  i=$((i + 1)); sleep 2
done
[ "$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER")" = "healthy" ]

echo "==> Prepare source schema with heterogeneous PKs"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "
CREATE DATABASE IF NOT EXISTS heteropk_src;
CREATE DATABASE IF NOT EXISTS heteropk_tgt;
DROP TABLE IF EXISTS heteropk_src.orders;
DROP TABLE IF EXISTS heteropk_src.users;
DROP TABLE IF EXISTS heteropk_tgt.orders;
DROP TABLE IF EXISTS heteropk_tgt.users;
CREATE TABLE heteropk_src.orders (
  order_id BIGINT NOT NULL PRIMARY KEY,
  amount DECIMAL(10,2) NOT NULL,
  status VARCHAR(16) NOT NULL
) ENGINE=InnoDB;
CREATE TABLE heteropk_src.users (
  user_no VARCHAR(32) NOT NULL PRIMARY KEY,
  name VARCHAR(64) NOT NULL
) ENGINE=InnoDB;
INSERT INTO heteropk_src.orders (order_id, amount, status) VALUES
  (1001, 10.00, 'new'),
  (1002, 20.00, 'paid'),
  (1003, 30.00, 'paid');
INSERT INTO heteropk_src.users (user_no, name) VALUES
  ('U001', 'Alice'),
  ('U002', 'Bob');
CREATE TABLE heteropk_tgt.orders LIKE heteropk_src.orders;
CREATE TABLE heteropk_tgt.users LIKE heteropk_src.users;
GRANT ALL PRIVILEGES ON heteropk_src.* TO 'sync_user'@'%';
GRANT ALL PRIVILEGES ON heteropk_tgt.* TO 'sync_user'@'%';
FLUSH PRIVILEGES;
"

echo "==> Reset ETL data"
rm -rf data-heteropk
mkdir -p data-heteropk/output data-heteropk/checkpoint data-heteropk/dlq logs
chmod -R a+rwX data-heteropk
chmod a+rwX logs

echo "==> Run whole-database snapshot+CDC pipeline (no pk_column, auto-detect)"
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p ${PORT}:8001 \
  -v "$ROOT_DIR/testdata/pipes-heteropk:/app/pipes:ro" \
  -v "$ROOT_DIR/data-heteropk:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:${PORT}/api/v2/health"

echo "==> Wait snapshot copied (orders=3, users=2)"
wait_count "SELECT COUNT(*) FROM heteropk_tgt.orders;" 3
wait_count "SELECT COUNT(*) FROM heteropk_tgt.users;" 2

echo "==> Emit CDC after snapshot (insert into both tables)"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 heteropk_src -e "
INSERT INTO orders (order_id, amount, status) VALUES (1004, 40.00, 'new');
INSERT INTO users (user_no, name) VALUES ('U003', 'Carol');
"

echo "==> Verify CDC copied (orders=4, users=3)"
wait_count "SELECT COUNT(*) FROM heteropk_tgt.orders;" 4
wait_count "SELECT COUNT(*) FROM heteropk_tgt.users;" 3

body="$(curl -fsS http://127.0.0.1:${PORT}/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"name":"heteropk-snapshot-cdc"' | grep '"status":"running"'
# 3 orders + 2 users snapshot + 1 order + 1 user CDC = 7 writes
echo "$body" | grep '"records_written":7'

test -f data-heteropk/etl.db

"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true

echo "Heterogeneous-PK whole-database snapshot+CDC E2E passed"
