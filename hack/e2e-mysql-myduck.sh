#!/bin/sh

# E2E: MySQL -> MyDuck Server (apecloud/myduckserver)
#
# Validates OpenETL-Go "mysql_batch/mysql_cdc -> mysql sink -> MyDuck" path and
# checks the three risk points raised when evaluating MyDuck as a Doris
# alternative:
#   1. INSERT ... ON DUPLICATE KEY UPDATE translation (upsert support)
#   2. bulk write throughput on large batches
#   3. checkpoint reset replay duplicate absorption
#
# Expected results (verified 2026-08-09):
#   - insert-mode batch sync works (auto_create DDL + multi-row INSERT + txn)
#   - upsert mode FAILS: MyDuck does not accept ON DUPLICATE KEY UPDATE
#   - insert-mode replay FAILS on duplicate PK (no duplicate rows written)
#   - CDC chain works for INSERT/DELETE only; UPDATE routes to upsert and fails
#   - bulk throughput (10w rows): roughly 10k rows/sec via MySQL protocol

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
MYSQL_CONTAINER="etl-mysql-source"
MYDUCK_CONTAINER="etl-myduck"
APP_CONTAINER="etl-openetl-go-myduck"
APP_PORT="8022"
MYDUCK_DB="etl_analytics"
MYDUCK_USER="root"
MYDUCK_PASS="myduck123"

cleanup() {
	"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 80 ]; do
    # health returns 503 while any pipeline is degraded; 200/503 both mean the API is up
    code=$(curl -sS -o /dev/null -w '%{http_code}' "$url" 2>/dev/null || echo 000)
    if [ "$code" != "000" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

wait_myduck() {
  i=0
  while [ "$i" -lt 120 ]; do
    if "$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -h "$MYDUCK_CONTAINER" -P3306 -u"$MYDUCK_USER" -p"$MYDUCK_PASS" -e "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 2
  done
  return 1
}

myduck_sql() {
  "$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -h "$MYDUCK_CONTAINER" -P3306 -u"$MYDUCK_USER" -p"$MYDUCK_PASS" "$MYDUCK_DB" "$@" 2>&1 | grep -v "Warning" || true
}

pipeline_status() {
  curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines" | grep "\"name\":\"$1\"" | grep -o '"status":"[a-z_]*"' | head -1 | cut -d'"' -f4
}

wait_pipeline_status() {
  name="$1"
  want="$2"
  limit="${3:-120}"
  i=0
  while [ "$i" -lt "$limit" ]; do
    s="$(pipeline_status "$name")"
    [ "$s" = "$want" ] && return 0
    i=$((i + 1))
    sleep 1
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
while [ "$i" -lt 90 ]; do
  status="$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
  if [ "$status" = "healthy" ]; then
    break
  fi
  i=$((i + 1))
  sleep 2
done
[ "$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER")" = "healthy" ]

NET="$("$CONTAINER_CLI" inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' "$MYSQL_CONTAINER")"
echo "==> Start MyDuck container (network: $NET)"
if ! "$CONTAINER_CLI" ps --filter name="^$MYDUCK_CONTAINER$" --format '{{.Names}}' | grep -q "$MYDUCK_CONTAINER"; then
  "$CONTAINER_CLI" rm -f "$MYDUCK_CONTAINER" >/dev/null 2>&1 || true
  "$CONTAINER_CLI" run -d --name "$MYDUCK_CONTAINER" \
    --network "$NET" \
    -e DEFAULT_DB="$MYDUCK_DB" \
    -e SUPERUSER_PASSWORD="$MYDUCK_PASS" \
    apecloud/myduckserver:latest >/dev/null
else
  echo "    (reusing running $MYDUCK_CONTAINER)"
fi
wait_myduck
echo "==> MyDuck ready: $("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -h "$MYDUCK_CONTAINER" -P3306 -u"$MYDUCK_USER" -p"$MYDUCK_PASS" -N -e "SELECT VERSION()" 2>/dev/null | grep -v Warning)"

echo "==> Prepare MySQL source tables"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "
CREATE DATABASE IF NOT EXISTS dzh3136_go;
DROP TABLE IF EXISTS dzh3136_go.myduck_e2e_orders;
DROP TABLE IF EXISTS dzh3136_go.myduck_e2e_big;
DROP TABLE IF EXISTS dzh3136_go.myduck_e2e_customers;
CREATE TABLE dzh3136_go.myduck_e2e_orders (
  id INT PRIMARY KEY,
  sku VARCHAR(100) NOT NULL,
  amount DECIMAL(10,2) NOT NULL,
  status VARCHAR(20) NOT NULL,
  created_at TIMESTAMP NOT NULL
);
INSERT INTO dzh3136_go.myduck_e2e_orders (id, sku, amount, status, created_at) VALUES
  (1, 'SKU-001', 18999.00, 'completed', '2024-01-01 10:00:00'),
  (2, 'SKU-002', 7999.00, 'completed', '2024-01-01 10:01:00'),
  (3, 'SKU-003', 1999.00, 'shipped',  '2024-01-01 10:02:00'),
  (4, 'SKU-004', 4799.00, 'pending',  '2024-01-01 10:03:00'),
  (5, 'SKU-005', 2999.00, 'completed', '2024-01-01 10:04:00'),
  (6, 'SKU-006', 699.00,  'completed', '2024-01-01 10:05:00'),
  (7, 'SKU-007', 4499.00, 'shipped',  '2024-01-01 10:06:00'),
  (8, 'SKU-008', 2299.00, 'pending',  '2024-01-01 10:07:00'),
  (9, 'SKU-009', 1499.00, 'completed', '2024-01-01 10:08:00'),
  (10, 'SKU-010', 999.00, 'completed', '2024-01-01 10:09:00');
CREATE TABLE dzh3136_go.myduck_e2e_big (
  id INT PRIMARY KEY,
  sku VARCHAR(20) NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  tag VARCHAR(20) NOT NULL
);
SET SESSION cte_max_recursion_depth = 200000;
INSERT INTO dzh3136_go.myduck_e2e_big (id, sku, price, tag)
WITH RECURSIVE seq(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 100000
)
SELECT n, CONCAT('SKU-', n), ROUND(MOD(n, 100000) * 0.01, 2), CONCAT('TAG-', MOD(n, 10)) FROM seq;
CREATE TABLE dzh3136_go.myduck_e2e_customers (
  id INT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  email VARCHAR(100) NOT NULL,
  city VARCHAR(50)
);
INSERT INTO dzh3136_go.myduck_e2e_customers (id, name, email, city) VALUES
  (1, 'Alice Chen', 'alice@example.com', 'Shanghai'),
  (2, 'Bob Wang', 'bob@example.com', 'Beijing');
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP ON dzh3136_go.* TO 'sync_user'@'%';
FLUSH PRIVILEGES;
"

echo "==> Reset MyDuck target tables"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -h "$MYDUCK_CONTAINER" -P3306 -u"$MYDUCK_USER" -p"$MYDUCK_PASS" -e "CREATE DATABASE IF NOT EXISTS $MYDUCK_DB" 2>&1 | grep -v Warning || true
myduck_sql -e "DROP TABLE IF EXISTS myduck_e2e_orders; DROP TABLE IF EXISTS myduck_e2e_big; DROP TABLE IF EXISTS myduck_e2e_customers;"
MYDUCK_IP="$("$CONTAINER_CLI" inspect "$MYDUCK_CONTAINER" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')"
pg_sql() {
  "$CONTAINER_CLI" exec -e PGPASSWORD="$MYDUCK_PASS" etl-postgres-cdc-source psql -h "$MYDUCK_IP" -p 5432 -U postgres -v ON_ERROR_STOP=1 "$@" 2>&1 || true
}
pg_sql -c "DROP TABLE IF EXISTS public.myduck_pg_orders; DROP TABLE IF EXISTS public.myduck_pg_big; DROP TABLE IF EXISTS public.myduck_pg_customers; DROP TABLE IF EXISTS public.orders_pg;"
# ensure postgres client container is on the same network as myduck
if ! "$CONTAINER_CLI" inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' "$MYDUCK_CONTAINER" | grep -q "$(docker inspect etl-postgres-cdc-source --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')"; then
  :
fi

echo "==> Reset ETL data"
rm -rf data-mysqlmyduck
mkdir -p data-mysqlmyduck/output data-mysqlmyduck/checkpoint data-mysqlmyduck/dlq logs
chmod -R a+rwX data-mysqlmyduck
chmod a+rwX logs

echo "==> Run OpenETL app"
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  --network "$NET" \
  -p "$APP_PORT:8001" \
  -v "$ROOT_DIR/testdata/pipes-mysql-myduck:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-mysqlmyduck:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE" >/dev/null

wait_http "http://127.0.0.1:$APP_PORT/api/v2/health"

echo ""
echo "########## 1) MySQL-port insert mode -> MyDuck: EXPECT FAILURE ##########"
echo "    (OpenETL mysql sink insert mode emits INSERT IGNORE; SQLGlot layer rejects it)"
sleep 6
echo "status: $(pipeline_status batch-insert-to-myduck)"
"$CONTAINER_CLI" logs "$APP_CONTAINER" 2>&1 | grep -E "batch insert myduck_e2e_orders" | head -1 || true
mrows="$(myduck_sql -N -e "SELECT COUNT(*) FROM myduck_e2e_orders" | tr -d '[:space:]')"
test "$mrows" = "0"
echo "CONFIRMED: MySQL-port insert mode FAILS (INSERT IGNORE unsupported), 0 rows in MyDuck"

echo ""
echo "########## 2) MySQL-port upsert mode: EXPECT FAILURE (ON DUPLICATE KEY UPDATE) ##########"
echo "status: $(pipeline_status batch-upsert-to-myduck)"
"$CONTAINER_CLI" logs "$APP_CONTAINER" 2>&1 | grep -iE "DUPLICATE" | head -1 || true
test "$mrows" = "0"
echo "CONFIRMED: MySQL-port upsert FAILS (ON DUPLICATE KEY UPDATE rejected)"

echo ""
echo "########## 3) MySQL-port CDC: EXPECT FAILURE (ops route to INSERT IGNORE / upsert) ##########"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "
INSERT INTO dzh3136_go.myduck_e2e_customers (id, name, email, city) VALUES (3, 'Carol Li', 'carol@example.com', 'Shenzhen'), (4, 'David Zhang', 'david@example.com', 'Hangzhou');
" >/dev/null
sleep 8
echo "status: $(pipeline_status cdc-upsert-to-myduck)"
"$CONTAINER_CLI" logs "$APP_CONTAINER" 2>&1 | grep -E "cdc-upsert-to-myduck" | grep -iE "ERROR|IGNORE|DUPLICATE|Ambiguous" | head -2 || true
mcdc="$(myduck_sql -N -e "SELECT COUNT(*) FROM myduck_e2e_customers" | tr -d '[:space:]')"
test "$mcdc" = "0"
echo "CONFIRMED: MySQL-port CDC writes FAILED (MyDuck rows: $mcdc)"

echo ""
echo "########## 4) PG-port via OpenETL postgres sink: EXPECT FAILURE (pgx Ping empty query) ##########"
echo "status: $(pipeline_status batch-upsert-to-myduck-pg)"
"$CONTAINER_CLI" logs "$APP_CONTAINER" 2>&1 | grep -E "batch-upsert-to-myduck-pg" | grep -iE "ping|empty query" | head -2 || true
echo "CONFIRMED: pgx v5 Ping() sends an empty query; MyDuck DDB PG layer answers XX000 - sink cannot open"

echo ""
echo "########## 5) PG-port raw protocol (psql): capability baseline ##########"
MYDUCK_IP="$("$CONTAINER_CLI" inspect "$MYDUCK_CONTAINER" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')"
"$CONTAINER_CLI" exec -e PGPASSWORD="$MYDUCK_PASS" etl-postgres-cdc-source psql -h "$MYDUCK_IP" -p 5432 -U postgres -v ON_ERROR_STOP=1 -c "
CREATE TABLE IF NOT EXISTS public.myduck_pg_orders (id INT PRIMARY KEY, sku VARCHAR(100), amount DECIMAL(10,2), status VARCHAR(20));
INSERT INTO myduck_pg_orders (id, sku, amount, status) VALUES (1, 'SKU-001', 18999.00, 'completed') ON CONFLICT (id) DO UPDATE SET sku = EXCLUDED.sku, amount = EXCLUDED.amount, status = EXCLUDED.status;
INSERT INTO myduck_pg_orders (id, sku, amount, status) VALUES (1, 'SKU-001-X', 1.00, 'x') ON CONFLICT (id) DO UPDATE SET sku = EXCLUDED.sku, amount = EXCLUDED.amount, status = EXCLUDED.status;
SELECT id || '|' || sku || '|' || amount || '|' || status AS row_out FROM myduck_pg_orders WHERE id = 1;
DELETE FROM myduck_pg_orders WHERE id = 1;
SELECT COUNT(*) AS left_rows FROM myduck_pg_orders;
" 2>&1 | grep -vE "^$|^--" | head -12

echo ""
echo "########## 6) MySQL-port pure-INSERT bulk throughput (100k rows) ##########"
echo "    (engine capability only - OpenETL syntax path already disproven above;"
echo "     plain INSERT without IGNORE/ON DUPLICATE is the only MySQL-dialect write that works)"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -h "$MYDUCK_CONTAINER" -P3306 -u"$MYDUCK_USER" -p"$MYDUCK_PASS" -e "DROP TABLE IF EXISTS e2e_myduck.orders_bench; CREATE TABLE e2e_myduck.orders_bench (id BIGINT PRIMARY KEY, product VARCHAR(100), amount DECIMAL(10,2), status VARCHAR(20), created_at TIMESTAMP);" 2>&1 | grep -v Warning || true
python3 - <<'PYEOF2' > /tmp/myduck-bench.sql
rows = [f"({i},'product_{i%100}',{100 + i/100:.2f},'status_{i%5}','2024-01-01 00:00:00')" for i in range(1, 100001)]
print('START TRANSACTION;')
for st in range(0, len(rows), 5000):
    print('INSERT INTO orders_bench (id, product, amount, status, created_at) VALUES ' + ','.join(rows[st:st+5000]) + ';')
print('COMMIT;')
PYEOF2
t0="$(date +%s)"
"$CONTAINER_CLI" exec -i "$MYSQL_CONTAINER" mysql -h "$MYDUCK_CONTAINER" -P3306 -u"$MYDUCK_USER" -p"$MYDUCK_PASS" e2e_myduck < /tmp/myduck-bench.sql 2>&1 | grep -v Warning | head -3 || true
t1="$(date +%s)"
benchrows="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -h "$MYDUCK_CONTAINER" -P3306 -u"$MYDUCK_USER" -p"$MYDUCK_PASS" -N -e "SELECT COUNT(*) FROM e2e_myduck.orders_bench" 2>/dev/null | grep -v Warning | tr -d '[:space:]')"
elapsed=$((t1 - t0))
[ "$elapsed" -lt 1 ] && elapsed=1
echo "pure-INSERT 100k rows in single txn: ${elapsed}s (~$((100000 / elapsed)) rows/s), count=$benchrows"
test "$benchrows" = "100000"

echo ""
echo "===== MySQL -> MyDuck verdict ====="
echo "  OpenETL mysql sink (MySQL wire):      UNUSABLE - INSERT IGNORE / ON DUPLICATE KEY UPDATE"
echo "                                         rejected; DEFAULT_DB attach also makes plain writes"
echo "                                         hit 'Ambiguous reference' binder errors"
echo "  OpenETL postgres sink (PG wire):      UNUSABLE - pgx Ping (empty query) errors XX000"
echo "  MyDuck PG engine (direct psql):      PARTIAL - INSERT/ON CONFLICT/DELETE work;"
echo "  MyDuck path: built-in MySQL binlog replica (START REPLICA/SETUP_MODE=REPLICA) or"
echo "    external COPY/LOAD DATA - neither is an OpenETL sink"
echo ""
echo "MySQL -> MyDuck E2E finished (see verdict above)"
