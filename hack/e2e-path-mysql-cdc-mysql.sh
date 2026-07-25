#!/bin/sh
# PR-2.2 forced primary path: mysql_cdc → mysql upsert
# Cases: happy / crash_restart / checkpoint_reset / sink_outage_dlq_replay
# Delivery: checkpointed at-least-once; silent loss target = 0.
set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="${IMAGE:-openetl-go-etl:dev}"
MYSQL_CONTAINER="etl-mysql-source"
APP_CONTAINER="etl-openetl-go-path-mysql-cdc"
APP_PORT="8042"
PIPELINE="mysql-cdc-path-matrix"
DATA_DIR="$ROOT_DIR/data-path-mysql-cdc-mysql"
PIPES_DIR="$ROOT_DIR/testdata/pipes-path-mysql-cdc-mysql"
SRC_DB="dzh3136_go"
TGT_DB="dzh3136_target"
TABLE="path_matrix_customers"
REPORT_DIR="$DATA_DIR/report"
COMMIT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

cleanup() {
  "$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
  # Ensure MySQL is up for other e2e (outage case may stop network alias only).
  compose -f docker-compose.dev.yml up -d mysql-source >/dev/null 2>&1 || true
}
trap cleanup EXIT

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

wait_mysql_healthy() {
  i=0
  while [ "$i" -lt 90 ]; do
    status="$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
    if [ "$status" = "healthy" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 2
  done
  return 1
}

wait_pipeline_running() {
  i=0
  body=""
  while [ "$i" -lt 60 ]; do
    body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines" 2>/dev/null || true)"
    echo "$body" | grep "\"name\":\"$PIPELINE\"" | grep '"status":"running"' >/dev/null 2>&1 && return 0
    i=$((i + 1))
    sleep 1
  done
  echo "$body"
  return 1
}

mysql_root() {
  "$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "$1"
}

mysql_value() {
  "$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -B -usync_user -psync_password_123 -e "$1" 2>/dev/null | tr -d '\r' | tail -n 1
}

wait_mysql_value() {
  sql="$1"
  expected="$2"
  i=0
  while [ "$i" -lt 90 ]; do
    got="$(mysql_value "$sql")"
    if [ "$got" = "$expected" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  echo "timeout waiting for SQL result: $sql (got=${got:-} want=$expected)" >&2
  return 1
}

run_app() {
  "$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
  "$CONTAINER_CLI" run -d \
    --add-host host.docker.internal:host-gateway \
    --name "$APP_CONTAINER" \
    -p "$APP_PORT:8001" \
    -v "$PIPES_DIR:/app/pipes:ro" \
    -v "$ROOT_DIR/testdata:/app/testdata:ro" \
    -v "$DATA_DIR:/app/data" \
    -v "$ROOT_DIR/logs:/app/logs" \
    "$IMAGE" >/dev/null
  wait_http "http://127.0.0.1:$APP_PORT/api/v2/health"
  wait_pipeline_running
}

case_result() {
  name="$1"
  ok="$2"
  extra="$3"
  # extra is a JSON object fragment without surrounding braces, e.g. "source_count":3
  if [ -n "$extra" ]; then
    printf '{"name":"%s","ok":%s,%s}' "$name" "$ok" "$extra"
  else
    printf '{"name":"%s","ok":%s}' "$name" "$ok"
  fi
}

echo "==> Build image (skip with E2E_SKIP_BUILD=1)"
if [ "${E2E_SKIP_BUILD:-}" != "1" ]; then
  "$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile .
fi

echo "==> Start MySQL source"
compose -f docker-compose.dev.yml up -d mysql-source >/dev/null 2>&1 || true
wait_mysql_healthy

echo "==> Prepare source/target tables"
mysql_root "
CREATE DATABASE IF NOT EXISTS $TGT_DB;
DROP TABLE IF EXISTS $SRC_DB.$TABLE;
CREATE TABLE $SRC_DB.$TABLE (
  id INT PRIMARY KEY,
  name VARCHAR(255),
  email VARCHAR(255),
  status VARCHAR(50),
  amount DECIMAL(10,2)
);
DROP TABLE IF EXISTS $TGT_DB.$TABLE;
CREATE TABLE $TGT_DB.$TABLE LIKE $SRC_DB.$TABLE;
GRANT ALL PRIVILEGES ON $TGT_DB.* TO 'sync_user'@'%';
FLUSH PRIVILEGES;
"

echo "==> Write path matrix pipeline spec"
mkdir -p "$PIPES_DIR" "$DATA_DIR" "$REPORT_DIR" logs
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR/output" "$DATA_DIR/checkpoint" "$DATA_DIR/dlq" "$REPORT_DIR"
chmod -R a+rwX "$DATA_DIR" logs

cat > "$PIPES_DIR/$PIPELINE.yaml" <<SPEC
name: "$PIPELINE"
source:
  type: mysql_cdc
  config:
    host: "host.docker.internal"
    port: 13306
    user: "sync_user"
    password: "sync_password_123"
    database: "$SRC_DB"
    server_id: 4201
    tables:
      - "$TABLE"

transforms:
  - type: identity
    config: {}

sink:
  type: mysql
  config:
    host: "host.docker.internal"
    port: 13306
    user: "sync_user"
    password: "sync_password_123"
    database: "$TGT_DB"
    table: "$TABLE"
    batch_mode: "upsert"
    pk_columns:
      - "id"

batch_size: 1
checkpoint_interval_sec: 1
backpressure_buffer: 20

retry:
  max_attempts: 3
  initial_interval_ms: 100
  max_interval_ms: 1000

dlq:
  enable: true
SPEC

CASES=""
append_case() {
  if [ -z "$CASES" ]; then
    CASES="$1"
  else
    CASES="$CASES,$1"
  fi
}

echo "==> Case happy: CDC insert + update (emit after pipeline is running)"
run_app
mysql_root "DELETE FROM $SRC_DB.$TABLE WHERE id IN (9101,9102,9103,9104,9201);
INSERT INTO $SRC_DB.$TABLE (id, name, email, status, amount) VALUES
  (9101, 'Path Alice', 'path-alice@example.com', 'active', 10.10),
  (9102, 'Path Bob', 'path-bob@example.com', 'active', 20.20),
  (9103, 'Path Carol', 'path-carol@example.com', 'active', 30.30);"
wait_mysql_value "SELECT COUNT(*) FROM $TGT_DB.$TABLE WHERE id IN (9101,9102,9103)" "3"
mysql_root "UPDATE $SRC_DB.$TABLE SET amount=11.11, status='vip' WHERE id=9101;"
wait_mysql_value "SELECT COUNT(*) FROM $TGT_DB.$TABLE WHERE id=9101 AND amount=11.11 AND status='vip'" "1"
append_case "$(case_result happy true '"source_count":3,"sink_count":3,"silent_loss":0')"
echo "happy OK"

echo "==> Case crash_restart: SIGKILL after sink ack, resume from checkpoint"
"$CONTAINER_CLI" kill "$APP_CONTAINER" >/dev/null
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
mysql_root "INSERT INTO $SRC_DB.$TABLE (id, name, email, status, amount) VALUES (9104, 'Path Dave', 'path-dave@example.com', 'active', 40.40);"
run_app
wait_mysql_value "SELECT COUNT(*) FROM $TGT_DB.$TABLE WHERE id=9104 AND amount=40.40" "1"
# prior rows still present (no silent loss)
wait_mysql_value "SELECT COUNT(*) FROM $TGT_DB.$TABLE WHERE id IN (9101,9102,9103,9104)" "4"
append_case "$(case_result crash_restart true '"replay_duplicates_absorbed":true,"silent_loss":0')"
echo "crash_restart OK"

echo "==> Case checkpoint_reset: full replay absorbed by upsert"
curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/stop" >/dev/null
curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/checkpoint/reset" >/dev/null
curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/start" >/dev/null
wait_pipeline_running
# CDC from earliest may re-deliver; upsert keeps one row per business key.
wait_mysql_value "SELECT COUNT(*) FROM $TGT_DB.$TABLE WHERE id IN (9101,9102,9103,9104)" "4"
wait_mysql_value "SELECT COUNT(*) FROM $TGT_DB.$TABLE WHERE id=9101 AND amount=11.11" "1"
append_case "$(case_result checkpoint_reset true '"replay_duplicates_absorbed":true,"silent_loss":0')"
echo "checkpoint_reset OK"

echo "==> Case sink_outage_dlq_replay"
# Simulate sink outage by renaming the target table (source remains readable).
mysql_root "RENAME TABLE $TGT_DB.$TABLE TO $TGT_DB.${TABLE}_outage_backup;"
mysql_root "INSERT INTO $SRC_DB.$TABLE (id, name, email, status, amount) VALUES (9201, 'Path Outage', 'path-outage@example.com', 'active', 99.99);"

i=0
dlq_body=""
while [ "$i" -lt 90 ]; do
  dlq_body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPELINE?contains=9201&limit=10" 2>/dev/null || true)"
  if echo "$dlq_body" | grep -q '9201'; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
echo "$dlq_body"
echo "$dlq_body" | grep '9201'
dlq_id="$(echo "$dlq_body" | grep -o '"id":[0-9][0-9]*' | head -n1 | sed 's/[^0-9]//g')"
test "$dlq_id" != ""

# Restore target table (with prior rows) and replay DLQ.
mysql_root "RENAME TABLE $TGT_DB.${TABLE}_outage_backup TO $TGT_DB.$TABLE;"
curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/stop" >/dev/null || true
replay_body="$(curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPELINE/$dlq_id/replay")"
echo "$replay_body"
echo "$replay_body" | grep '"replayed":1'
wait_mysql_value "SELECT COUNT(*) FROM $TGT_DB.$TABLE WHERE id=9201 AND amount=99.99" "1"
dlq_after="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPELINE?contains=9201&limit=10")"
if echo "$dlq_after" | grep -q "\"id\":${dlq_id}"; then
  echo "replayed DLQ id ${dlq_id} was not deleted" >&2
  exit 1
fi
# full business key set still consistent
wait_mysql_value "SELECT COUNT(*) FROM $TGT_DB.$TABLE WHERE id IN (9101,9102,9103,9104,9201)" "5"
append_case "$(case_result sink_outage_dlq_replay true '"silent_loss":0')"
echo "sink_outage_dlq_replay OK"

FINISHED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
REPORT_FILE="$REPORT_DIR/mysql_cdc__mysql_upsert.json"
cat > "$REPORT_FILE" <<EOF
{
  "path_id": "mysql_cdc__mysql_upsert",
  "commit": "$COMMIT_SHA",
  "started_at": "$STARTED_AT",
  "finished_at": "$FINISHED_AT",
  "cases": [$CASES],
  "rpo": "last durable checkpoint; in-flight batch may replay",
  "rto": "process restart + source reconnect + one checkpoint interval",
  "residuals": ["no cross-sink atomicity", "not exactly-once", "source binlog and sink are not a distributed transaction"],
  "silent_loss": 0
}
EOF

echo "==> Reconciliation report"
cat "$REPORT_FILE"

body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines")"
echo "$body" | grep "\"name\":\"$PIPELINE\""

"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true

echo "PR-2 path matrix mysql_cdc__mysql_upsert PASS"
