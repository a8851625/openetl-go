#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="${IMAGE:-openetl-go-etl:dev}"
MYSQL_CONTAINER="etl-mysql-write-modes"
MYSQL_PORT="15436"
PG_CONTAINER="etl-postgres-write-modes"
PG_PORT="15437"
APP_CONTAINER="etl-openetl-go-write-modes"
APP_PORT="8034"
DATA_DIR="$ROOT_DIR/data-write-modes"
PIPES_DIR="$DATA_DIR/pipes"
INPUT_DIR="$DATA_DIR/input"

PIPE_PRE_WRITE="mysql-pre-write-delete-rewrite"
PIPE_PG_PRE_WRITE="postgres-pre-write-delete-rewrite"
PIPE_INCREMENT="mysql-increment-stock"
PIPE_GENERATED="mysql-generated-columns"

cleanup() {
  "$CONTAINER_CLI" rm -f "$APP_CONTAINER" "$MYSQL_CONTAINER" "$PG_CONTAINER" >/dev/null 2>&1 || true
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

wait_mysql_ready() {
  i=0
  while [ "$i" -lt 60 ]; do
    if "$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 2
  done
  return 1
}

wait_pipeline_complete() {
  pipeline="$1"
  expected_written="$2"
  i=0
  while [ "$i" -lt 90 ]; do
    body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines")"
    if echo "$body" | grep "\"name\":\"$pipeline\"" | grep '"status":"completed"' >/dev/null 2>&1; then
      if [ -z "$expected_written" ] || echo "$body" | grep "\"name\":\"$pipeline\"" | grep "\"records_written\":$expected_written" >/dev/null 2>&1; then
        return 0
      fi
    fi
    i=$((i + 1))
    sleep 1
  done
  curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines" || true
  return 1
}

wait_pg_ready() {
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

mysql_root() {
  "$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "$1"
}

mysql_value() {
  "$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -B -usync_user -psync_password_123 -e "$1" 2>/dev/null | tr -d '\r'
}

wait_mysql_value() {
  sql="$1"
  expected="$2"
  i=0
  while [ "$i" -lt 60 ]; do
    got="$(mysql_value "$sql" | tail -n 1)"
    if [ "$got" = "$expected" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  got="$(mysql_value "$sql" | tail -n 1)"
  echo "SQL did not reach expected value: $sql" >&2
  echo "got: $got" >&2
  echo "want: $expected" >&2
  return 1
}

pg_sql() {
  "$CONTAINER_CLI" exec "$PG_CONTAINER" psql -U etl -d analytics -v ON_ERROR_STOP=1 -c "$1"
}

pg_value() {
  "$CONTAINER_CLI" exec "$PG_CONTAINER" psql -U etl -d analytics -At -c "$1" 2>/dev/null | tr -d '\r'
}

wait_pg_value() {
  sql="$1"
  expected="$2"
  i=0
  while [ "$i" -lt 60 ]; do
    got="$(pg_value "$sql" | tail -n 1)"
    if [ "$got" = "$expected" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  got="$(pg_value "$sql" | tail -n 1)"
  echo "PostgreSQL SQL did not reach expected value: $sql" >&2
  echo "got: $got" >&2
  echo "want: $expected" >&2
  return 1
}

if [ "${E2E_SKIP_BUILD:-0}" = "1" ]; then
  echo "==> Skip image build (E2E_SKIP_BUILD=1, using $IMAGE)"
else
  echo "==> Build image"
  "$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile .
fi

echo "==> Start MySQL"
cleanup
"$CONTAINER_CLI" run -d --name "$MYSQL_CONTAINER" \
  -e MYSQL_ROOT_PASSWORD=root123456 \
  -e MYSQL_DATABASE=dzh3136_target \
  -e MYSQL_USER=sync_user \
  -e MYSQL_PASSWORD=sync_password_123 \
  -e TZ=Asia/Shanghai \
  -p "$MYSQL_PORT:3306" \
  docker.io/library/mysql:8.0 \
  --default-authentication-plugin=mysql_native_password >/dev/null
wait_mysql_ready

echo "==> Start PostgreSQL"
"$CONTAINER_CLI" run -d --name "$PG_CONTAINER" \
  -e POSTGRES_DB=analytics \
  -e POSTGRES_USER=etl \
  -e POSTGRES_PASSWORD=etl123 \
  -e TZ=Asia/Shanghai \
  -p "$PG_PORT:5432" \
  docker.io/library/postgres:16-alpine >/dev/null
wait_pg_ready

echo "==> Prepare MySQL target tables"
mysql_root "
CREATE DATABASE IF NOT EXISTS dzh3136_target;
DROP TABLE IF EXISTS dzh3136_target.pre_write_orders;
DROP TABLE IF EXISTS dzh3136_target.stock_increment;
DROP TABLE IF EXISTS dzh3136_target.generated_customers;
CREATE TABLE dzh3136_target.pre_write_orders (
  id INT PRIMARY KEY,
  dt DATE NOT NULL,
  amount INT NOT NULL,
  note VARCHAR(64)
);
INSERT INTO dzh3136_target.pre_write_orders (id, dt, amount, note) VALUES
  (10, '2026-07-07', 999, 'stale-target-partition'),
  (20, '2026-07-06', 100, 'keep-other-partition');
CREATE TABLE dzh3136_target.stock_increment (
  sku VARCHAR(32) PRIMARY KEY,
  qty INT NOT NULL DEFAULT 0,
  updated_by VARCHAR(32)
);
CREATE TABLE dzh3136_target.generated_customers (
  id INT PRIMARY KEY,
  first_name VARCHAR(50) NOT NULL,
  last_name VARCHAR(50) NOT NULL,
  full_name VARCHAR(120) GENERATED ALWAYS AS (CONCAT(first_name, ' ', last_name)) STORED,
  name_len INT GENERATED ALWAYS AS (CHAR_LENGTH(CONCAT(first_name, ' ', last_name))) VIRTUAL
);
GRANT ALL PRIVILEGES ON dzh3136_target.* TO 'sync_user'@'%';
FLUSH PRIVILEGES;
"

echo "==> Prepare PostgreSQL target tables"
pg_sql "
DROP TABLE IF EXISTS pg_pre_write_orders;
CREATE TABLE pg_pre_write_orders (
  id INT PRIMARY KEY,
  dt DATE NOT NULL,
  amount INT NOT NULL,
  note TEXT
);
INSERT INTO pg_pre_write_orders (id, dt, amount, note) VALUES
  (110, '2026-07-07', 999, 'stale-target-partition'),
  (120, '2026-07-06', 220, 'keep-other-partition');
"

echo "==> Prepare pipeline specs and input files"
rm -rf "$DATA_DIR"
mkdir -p "$PIPES_DIR" "$INPUT_DIR" "$DATA_DIR/checkpoint" "$DATA_DIR/dlq" logs
chmod -R a+rwX "$DATA_DIR" logs

cat >"$INPUT_DIR/pre_write_orders.json" <<'JSON'
{"id":1,"dt":"2026-07-07","amount":100,"note":"fresh-a"}
{"id":2,"dt":"2026-07-07","amount":200,"note":"fresh-b"}
JSON

cat >"$INPUT_DIR/pg_pre_write_orders.json" <<'JSON'
{"id":101,"dt":"2026-07-07","amount":100,"note":"pg-fresh-a"}
{"id":102,"dt":"2026-07-07","amount":200,"note":"pg-fresh-b"}
JSON

cat >"$INPUT_DIR/stock_increment.json" <<'JSON'
{"sku":"A","qty":2,"updated_by":"e2e"}
{"sku":"B","qty":5,"updated_by":"e2e"}
JSON

cat >"$INPUT_DIR/generated_customers.json" <<'JSON'
{"id":1,"first_name":"Ada","last_name":"Lovelace","full_name":"SHOULD_NOT_WRITE","name_len":999}
{"id":2,"first_name":"Grace","last_name":"Hopper","full_name":"SHOULD_NOT_WRITE","name_len":999}
JSON

cat >"$PIPES_DIR/pre-write.yaml" <<'YAML'
name: mysql-pre-write-delete-rewrite
source:
  type: file
  config:
    path: /app/data/input/pre_write_orders.json
    format: json
sink:
  type: mysql
  config:
    host: host.docker.internal
    port: 15436
    user: sync_user
    password: sync_password_123
    database: dzh3136_target
    table: pre_write_orders
    batch_mode: insert
    pk_columns: ["id"]
    pre_write:
      action: delete
      condition: "dt = '${params.dt}'"
      params:
        dt: "2026-07-07"
batch_size: 10
checkpoint_interval_sec: 1
retry:
  max_attempts: 2
  initial_interval_ms: 100
  max_interval_ms: 1000
dlq:
  enable: true
YAML

cat >"$PIPES_DIR/postgres-pre-write.yaml" <<'YAML'
name: postgres-pre-write-delete-rewrite
source:
  type: file
  config:
    path: /app/data/input/pg_pre_write_orders.json
    format: json
sink:
  type: postgres
  config:
    host: host.docker.internal
    port: 15437
    user: etl
    password: etl123
    database: analytics
    table: pg_pre_write_orders
    batch_mode: insert
    pk_columns: ["id"]
    pre_write:
      action: delete
      condition: "dt = '${params.dt}'"
      params:
        dt: "2026-07-07"
batch_size: 10
checkpoint_interval_sec: 1
retry:
  max_attempts: 2
  initial_interval_ms: 100
  max_interval_ms: 1000
dlq:
  enable: true
YAML

cat >"$PIPES_DIR/increment.yaml" <<'YAML'
name: mysql-increment-stock
source:
  type: file
  config:
    path: /app/data/input/stock_increment.json
    format: json
sink:
  type: mysql
  config:
    host: host.docker.internal
    port: 15436
    user: sync_user
    password: sync_password_123
    database: dzh3136_target
    table: stock_increment
    batch_mode: increment
    pk_columns: ["sku"]
    increment_columns:
      qty: qty
batch_size: 10
checkpoint_interval_sec: 1
retry:
  max_attempts: 2
  initial_interval_ms: 100
  max_interval_ms: 1000
dlq:
  enable: true
YAML

cat >"$PIPES_DIR/generated.yaml" <<'YAML'
name: mysql-generated-columns
source:
  type: file
  config:
    path: /app/data/input/generated_customers.json
    format: json
sink:
  type: mysql
  config:
    host: host.docker.internal
    port: 15436
    user: sync_user
    password: sync_password_123
    database: dzh3136_target
    table: generated_customers
    batch_mode: upsert
    pk_columns: ["id"]
    schema_drift: ignore
batch_size: 10
checkpoint_interval_sec: 1
retry:
  max_attempts: 2
  initial_interval_ms: 100
  max_interval_ms: 1000
dlq:
  enable: true
YAML

echo "==> Run ETL service"
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p "$APP_PORT:8001" \
  -v "$PIPES_DIR:/app/pipes:ro" \
  -v "$DATA_DIR:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE" >/dev/null

wait_http "http://127.0.0.1:$APP_PORT/api/v2/health"

echo "==> Wait initial pipelines complete"
wait_pipeline_complete "$PIPE_PRE_WRITE" 2
wait_pipeline_complete "$PIPE_PG_PRE_WRITE" 2
wait_pipeline_complete "$PIPE_INCREMENT" 2
wait_pipeline_complete "$PIPE_GENERATED" 2

echo "==> Verify initial MySQL results"
prewrite_sql="SELECT CONCAT_WS('|', COUNT(*), COALESCE(SUM(amount),0), COALESCE(SUM(id=10),0), COALESCE(SUM(id=20),0), COALESCE(SUM(id=11),0), COALESCE(MAX(CASE WHEN id=1 THEN amount END),0)) FROM dzh3136_target.pre_write_orders;"
increment_sql="SELECT CONCAT_WS('|', COUNT(*), COALESCE(MAX(CASE WHEN sku='A' THEN qty END),0), COALESCE(MAX(CASE WHEN sku='B' THEN qty END),0), COALESCE(MAX(updated_by),'')) FROM dzh3136_target.stock_increment;"
generated_sql="SELECT CONCAT_WS('|', COUNT(*), COALESCE(MAX(CASE WHEN id=1 THEN full_name END),''), COALESCE(MAX(CASE WHEN id=1 THEN name_len END),0), COALESCE(MAX(CASE WHEN id=2 THEN full_name END),'')) FROM dzh3136_target.generated_customers;"
pg_prewrite_sql="SELECT COUNT(*)::text || '|' || COALESCE(SUM(amount),0)::text || '|' || COALESCE(SUM(CASE WHEN id=110 THEN 1 ELSE 0 END),0)::text || '|' || COALESCE(SUM(CASE WHEN id=120 THEN 1 ELSE 0 END),0)::text || '|' || COALESCE(SUM(CASE WHEN id=111 THEN 1 ELSE 0 END),0)::text || '|' || COALESCE(MAX(CASE WHEN id=101 THEN amount END),0)::text FROM pg_pre_write_orders;"
wait_mysql_value "$prewrite_sql" "3|400|0|1|0|100"
wait_mysql_value "$increment_sql" "2|2|5|e2e"
wait_mysql_value "$generated_sql" "2|Ada Lovelace|12|Grace Hopper"
wait_pg_value "$pg_prewrite_sql" "3|520|0|1|0|100"

echo "==> Pollute pre_write partition and verify checkpoint reset replay cleans it"
mysql_root "
INSERT INTO dzh3136_target.pre_write_orders (id, dt, amount, note)
VALUES (11, '2026-07-07', 999, 'polluted-before-replay')
ON DUPLICATE KEY UPDATE amount=VALUES(amount), note=VALUES(note);
UPDATE dzh3136_target.pre_write_orders SET amount=777, note='mutated-before-replay' WHERE id=1;
"
curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPE_PRE_WRITE/checkpoint/reset" >/dev/null
curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPE_PRE_WRITE/start" >/dev/null
wait_mysql_value "$prewrite_sql" "3|400|0|1|0|100"

echo "==> Pollute PostgreSQL pre_write partition and verify checkpoint reset replay cleans it"
pg_sql "
INSERT INTO pg_pre_write_orders (id, dt, amount, note)
VALUES (111, '2026-07-07', 999, 'polluted-before-replay')
ON CONFLICT (id) DO UPDATE SET amount=EXCLUDED.amount, note=EXCLUDED.note;
UPDATE pg_pre_write_orders SET amount=777, note='mutated-before-replay' WHERE id=101;
"
curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPE_PG_PRE_WRITE/checkpoint/reset" >/dev/null
curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPE_PG_PRE_WRITE/start" >/dev/null
wait_pg_value "$pg_prewrite_sql" "3|520|0|1|0|100"

echo "==> Reset increment checkpoint and verify additive replay boundary"
curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPE_INCREMENT/checkpoint/reset" >/dev/null
curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPE_INCREMENT/start" >/dev/null
wait_mysql_value "$increment_sql" "2|4|10|e2e"

echo "==> Verify no DLQ records were produced"
dlq_body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPE_PRE_WRITE?limit=10")"
echo "$dlq_body" | grep '"items":\[\]'
dlq_body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPE_PG_PRE_WRITE?limit=10")"
echo "$dlq_body" | grep '"items":\[\]'
dlq_body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPE_INCREMENT?limit=10")"
echo "$dlq_body" | grep '"items":\[\]'
dlq_body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPE_GENERATED?limit=10")"
echo "$dlq_body" | grep '"items":\[\]'

echo "Relational write modes E2E passed"
