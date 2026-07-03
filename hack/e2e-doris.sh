#!/bin/sh
set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"

IMAGE="openetl-go-etl:dev"
DORIS_FE_CONTAINER="etl-doris-fe"
DORIS_BE_CONTAINER="etl-doris-be"
DORIS_NETWORK="etl-doris-net"
MYSQL_CONTAINER="etl-mysql-source"
APP_CONTAINER="etl-openetl-go-doris"
DORIS_VERSION="${DORIS_VERSION:-2.1.11}"
DORIS_FE_IMAGE="${DORIS_FE_IMAGE:-docker.io/apache/doris:fe-$DORIS_VERSION}"
DORIS_BE_IMAGE="${DORIS_BE_IMAGE:-docker.io/apache/doris:be-$DORIS_VERSION}"
PIPE_DIR="$ROOT_DIR/data-doris/pipes"
API_PORT="${DORIS_API_PORT:-8024}"
MYSQL_DB="dzh3136_go"

start_mysql_source() {
  if has_compose; then
    compose -f docker-compose.dev.yml up -d mysql-source
    return
  fi
  echo "==> $CONTAINER_CLI compose unavailable; starting MySQL source directly"
  if "$CONTAINER_CLI" container inspect "$MYSQL_CONTAINER" >/dev/null 2>&1; then
    "$CONTAINER_CLI" start "$MYSQL_CONTAINER" >/dev/null
    return
  fi
  if ! "$CONTAINER_CLI" image inspect docker.io/library/mysql:8.0 >/dev/null 2>&1; then
    "$CONTAINER_CLI" pull docker.io/library/mysql:8.0
  fi
  "$CONTAINER_CLI" run -d \
    --add-host host.docker.internal:host-gateway \
    --name "$MYSQL_CONTAINER" \
    -e MYSQL_ROOT_PASSWORD=root123456 \
    -e MYSQL_DATABASE="$MYSQL_DB" \
    -e MYSQL_USER=sync_user \
    -e MYSQL_PASSWORD=sync_password_123 \
    -e TZ=Asia/Shanghai \
    -p 13306:3306 \
    -v "$ROOT_DIR/testdata/mysql/init:/docker-entrypoint-initdb.d:ro" \
    -v "$ROOT_DIR/testdata/mysql/conf.d:/etc/mysql/conf.d:ro" \
    --health-cmd='mysql -h localhost -u root -proot123456 -e "SELECT 1"' \
    --health-interval=5s \
    --health-timeout=5s \
    --health-retries=30 \
    --health-start-period=30s \
    docker.io/library/mysql:8.0 \
    --server-id=1 \
    --log-bin=mysql-bin \
    --binlog-format=ROW \
    --binlog-row-image=FULL \
    --gtid-mode=ON \
    --enforce-gtid-consistency=ON \
    --binlog-expire-logs-seconds=604800 \
    --default-authentication-plugin=mysql_native_password >/dev/null
}

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 120 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 2
  done
  return 1
}

doris_sql() {
  "$CONTAINER_CLI" exec "$DORIS_FE_CONTAINER" mysql -h127.0.0.1 -P9030 -uroot "$@"
}

wait_doris_sql() {
  i=0
  while [ "$i" -lt 120 ]; do
    if doris_sql -e "SELECT 1" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 2
  done
  return 1
}

wait_doris_backend_alive() {
  i=0
  while [ "$i" -lt 120 ]; do
    alive="$(doris_sql -N -e "SHOW BACKENDS;" 2>/dev/null | grep -c 'true' || true)"
    if [ "${alive:-0}" -ge 1 ]; then return 0; fi
    i=$((i + 1)); sleep 2
  done
  doris_sql -e "SHOW BACKENDS;" || true
  return 1
}

wait_pipeline_dlq() {
  name="$1"
  expected="$2"
  i=0
  body=""
  while [ "$i" -lt 120 ]; do
    body="$(curl -fsS "http://127.0.0.1:$API_PORT/api/v2/dlq/$name?limit=$expected")"
    count="$(echo "$body" | grep -o '"id":' | wc -l | tr -d ' ')"
    if [ "${count:-0}" -ge "$expected" ]; then return 0; fi
    i=$((i + 1)); sleep 1
  done
  echo "$body"
  return 1
}

wait_doris_count() {
  table="$1"
  where_clause="$2"
  expected="$3"
  i=0
  count=""
  body=""
  while [ "$i" -lt 120 ]; do
    count="$(doris_sql -N "$MYSQL_DB" -e "SELECT COUNT(*) FROM $table WHERE $where_clause;" 2>/dev/null | tr -d '[:space:]' || true)"
    if [ "$count" = "$expected" ]; then return 0; fi
    body="$(curl -fsS "http://127.0.0.1:$API_PORT/api/v2/pipelines" 2>/dev/null || true)"
    if echo "$body" | grep -q '"records_failed":[1-9]'; then
      echo "$body"
      return 1
    fi
    i=$((i + 1)); sleep 1
  done
  echo "Timed out waiting for $table where $where_clause count=$expected; last count=${count:-<empty>}" >&2
  echo "$body"
  return 1
}

write_pipe() {
  name="$1"
  source_type="$2"
  sink_table="$3"
  write_mode="$4"
  stream_format="$5"
  query="$6"
  cat >"$PIPE_DIR/$name.yaml" <<EOF
name: "$name"
source:
  type: $source_type
  config:
    host: "host.docker.internal"
    port: 13306
    user: "sync_user"
    password: "sync_password_123"
    database: "$MYSQL_DB"
    table: "customers"
    query: "$query"
    pk_column: "id"
    limit: 1000

transforms:
  - type: identity
    config: {}

sink:
  type: doris
  config:
    host: "host.docker.internal"
    port: 9030
    http_port: 8030
    user: "root"
    database: "$MYSQL_DB"
    table: "$sink_table"
    write_mode: "$write_mode"
    stream_load_format: "$stream_format"
    batch_mode: "upsert"
    pk_columns: ["id"]
    auto_create: true
    schema_drift: "add_columns"
    ddl_policy: "reject"

batch_size: 200
checkpoint_interval_sec: 1
backpressure_buffer: 500
dlq:
  enable: true
EOF
}

detect_container_cli

if [ "${E2E_SKIP_BUILD:-0}" = "1" ]; then
  echo "==> Skip image build (E2E_SKIP_BUILD=1, using $IMAGE)"
else
  echo "==> Build image"
  "$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile . 2>&1 | tail -1
fi

echo "==> Start MySQL source"
start_mysql_source
i=0
while [ "$i" -lt 60 ]; do
  status="$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
  if [ "$status" = "healthy" ]; then break; fi
  i=$((i + 1)); sleep 2
done
[ "$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER")" = "healthy" ]

echo "==> Start Doris FE/BE"
if ! "$CONTAINER_CLI" image inspect "$DORIS_FE_IMAGE" >/dev/null 2>&1; then
  "$CONTAINER_CLI" pull "$DORIS_FE_IMAGE"
fi
if ! "$CONTAINER_CLI" image inspect "$DORIS_BE_IMAGE" >/dev/null 2>&1; then
  "$CONTAINER_CLI" pull "$DORIS_BE_IMAGE"
fi
"$CONTAINER_CLI" rm -f "$DORIS_FE_CONTAINER" "$DORIS_BE_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" network rm "$DORIS_NETWORK" >/dev/null 2>&1 || true
"$CONTAINER_CLI" network create --subnet=172.31.90.0/24 "$DORIS_NETWORK" >/dev/null
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$DORIS_FE_CONTAINER" \
  --network "$DORIS_NETWORK" \
  --ip 172.31.90.2 \
  -e FE_SERVERS="fe1:172.31.90.2:9010" \
  -e FE_ID=1 \
  -p 8030:8030 \
  -p 9030:9030 \
  "$DORIS_FE_IMAGE" >/dev/null
"$CONTAINER_CLI" run -d \
  --name "$DORIS_BE_CONTAINER" \
  --network "$DORIS_NETWORK" \
  --ip 172.31.90.3 \
  -e FE_SERVERS="fe1:172.31.90.2:9010" \
  -e BE_ADDR="172.31.90.3:9050" \
  "$DORIS_BE_IMAGE" >/dev/null

echo "==> Wait Doris SQL ready"
wait_doris_sql
echo "==> Wait Doris BE alive"
wait_doris_backend_alive
doris_sql -e "CREATE DATABASE IF NOT EXISTS $MYSQL_DB;"

echo "==> Prepare source rows"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 "$MYSQL_DB" -e "
DELETE FROM customers WHERE id BETWEEN 9700 AND 9710;
INSERT INTO customers (id, name, email, phone, status, amount) VALUES
  (9701, 'Doris JSON', 'doris-json@example.com', '13900009701', 'active', 101.10),
  (9702, 'Doris CSV', 'doris-csv@example.com', '13900009702', 'active', 202.20),
  (9703, 'Doris Insert', 'doris-insert@example.com', '13900009703', 'active', 303.30);
"

echo "==> Generate Doris batch specs"
rm -rf data-doris
mkdir -p "$PIPE_DIR" data-doris/output data-doris/checkpoint data-doris/dlq logs
chmod -R a+rwX data-doris
chmod a+rwX logs 2>/dev/null || true
write_pipe "mysql-batch-to-doris-json" "mysql_batch" "customers_json" "stream_load" "json" "SELECT id,name,email,phone,status,amount FROM customers WHERE id=9701"
write_pipe "mysql-batch-to-doris-csv" "mysql_batch" "customers_csv" "stream_load" "csv" "SELECT id,name,email,phone,status,amount FROM customers WHERE id=9702"
write_pipe "mysql-batch-to-doris-insert" "mysql_batch" "customers_insert" "insert" "json" "SELECT id,name,email,phone,status,amount FROM customers WHERE id=9703"

echo "==> Run ETL pipelines"
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  --network "$DORIS_NETWORK" \
  --ip 172.31.90.4 \
  -p "$API_PORT:8001" \
  -v "$PIPE_DIR:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-doris:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:$API_PORT/api/v2/health"

echo "==> Wait batch pipelines complete"
i=0
body=""
completed=0
while [ "$i" -lt 90 ]; do
  body="$(curl -fsS "http://127.0.0.1:$API_PORT/api/v2/pipelines")"
  completed="$(echo "$body" | grep -o '"status":"completed"' | wc -l | tr -d ' ')"
  if [ "$completed" -ge 3 ]; then break; fi
  i=$((i + 1)); sleep 2
done
echo "$body"
test "$completed" -ge 3
if echo "$body" | grep -q '"records_failed":[1-9]'; then
  echo "Doris e2e pipeline completed with failed records" >&2
  exit 1
fi

echo "==> Verify Doris rows"
wait_doris_count "customers_json" "id=9701" "1"
wait_doris_count "customers_csv" "id=9702" "1"
wait_doris_count "customers_insert" "id=9703" "1"

echo "==> Verify schema drift/typing"
amount_type="$(doris_sql -N "$MYSQL_DB" -e "SHOW CREATE TABLE customers_json;" | tr '\n' ' ')"
echo "$amount_type" | grep "UNIQUE KEY"
echo "$amount_type" | grep -i "decimal"

echo "==> Verify Doris BE outage routes Stream Load records to DLQ and replay recovers"
OUTAGE_PIPELINE="doris-be-outage-dlq"
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
rm -rf "$PIPE_DIR"
rm -f data-doris/etl.db data-doris/etl.db-*
rm -rf data-doris/checkpoint data-doris/dlq
mkdir -p "$PIPE_DIR"
cat >"$ROOT_DIR/data-doris/outage-input.jsonl" <<'EOF'
{"id":9721,"name":"Doris Outage Ada","email":"doris-outage-ada@example.com","phone":"13900009721","status":"active","amount":121.10}
{"id":9722,"name":"Doris Outage Grace","email":"doris-outage-grace@example.com","phone":"13900009722","status":"active","amount":122.20}
{"id":9723,"name":"Doris Outage Katherine","email":"doris-outage-katherine@example.com","phone":"13900009723","status":"active","amount":123.30}
{"id":9724,"name":"Doris Outage Dorothy","email":"doris-outage-dorothy@example.com","phone":"13900009724","status":"active","amount":124.40}
{"id":9725,"name":"Doris Outage Mary","email":"doris-outage-mary@example.com","phone":"13900009725","status":"active","amount":125.50}
EOF
cat >"$PIPE_DIR/$OUTAGE_PIPELINE.yaml" <<EOF
name: "$OUTAGE_PIPELINE"
source:
  type: file
  config:
    path: "/app/data/outage-input.jsonl"
    format: "json"

transforms:
  - type: rate_limiter
    config:
      rps: 1
      burst: 1

sink:
  type: doris
  config:
    host: "host.docker.internal"
    port: 9030
    http_port: 8030
    user: "root"
    database: "$MYSQL_DB"
    table: "customers_outage"
    write_mode: "stream_load"
    stream_load_format: "json"
    stream_load_timeout_sec: 1
    batch_mode: "upsert"
    pk_columns: ["id"]
    auto_create: false
    schema_drift: "add_columns"
    ddl_policy: "reject"

batch_size: 5
checkpoint_interval_sec: 1
backpressure_buffer: 10
retry:
  max_attempts: 1
  initial_interval_ms: 50
  max_interval_ms: 50
dlq:
  enable: true
EOF
doris_sql "$MYSQL_DB" -e "
DROP TABLE IF EXISTS customers_outage;
CREATE TABLE customers_outage (
  id BIGINT NOT NULL,
  amount DECIMAL(18,2) NULL,
  email VARCHAR(255) NULL,
  name VARCHAR(255) NULL,
  phone VARCHAR(255) NULL,
  status VARCHAR(255) NULL
) ENGINE=OLAP
UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
  \"replication_allocation\" = \"tag.location.default: 1\",
  \"enable_unique_key_merge_on_write\" = \"true\"
);
"
"$CONTAINER_CLI" stop -t 0 "$DORIS_BE_CONTAINER" >/dev/null

"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  --network "$DORIS_NETWORK" \
  --ip 172.31.90.4 \
  -p "$API_PORT:8001" \
  -v "$PIPE_DIR:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-doris:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE" >/dev/null

wait_http "http://127.0.0.1:$API_PORT/api/v2/pipelines"
wait_pipeline_dlq "$OUTAGE_PIPELINE" 5
dlq_body="$(curl -fsS "http://127.0.0.1:$API_PORT/api/v2/dlq/$OUTAGE_PIPELINE?contains=9721&limit=10")"
echo "$dlq_body" | grep '"error_class":"transient"'
echo "$dlq_body" | grep -E 'connection refused|stream load|backend|Backend|503|context deadline exceeded'

"$CONTAINER_CLI" start "$DORIS_BE_CONTAINER" >/dev/null
wait_doris_backend_alive
"$CONTAINER_CLI" restart "$DORIS_FE_CONTAINER" >/dev/null
wait_doris_sql
wait_doris_backend_alive
replay_body="$(curl -fsS -X POST "http://127.0.0.1:$API_PORT/api/v2/dlq/$OUTAGE_PIPELINE/replay")"
echo "$replay_body"
echo "$replay_body" | grep '"replayed":5'

outage_count="$(doris_sql -N "$MYSQL_DB" -e "SELECT COUNT(*) FROM customers_outage WHERE id BETWEEN 9721 AND 9725;" | tr -d '[:space:]')"
test "$outage_count" = "5"
dlq_after="$(curl -fsS "http://127.0.0.1:$API_PORT/api/v2/dlq/$OUTAGE_PIPELINE?limit=10")"
if ! echo "$dlq_after" | grep '"items":\[\]' >/dev/null; then
  echo "Doris outage DLQ records were not deleted after replay" >&2
  exit 1
fi

"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" rm -f "$DORIS_FE_CONTAINER" "$DORIS_BE_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" network rm "$DORIS_NETWORK" >/dev/null 2>&1 || true

echo "Doris batch Stream Load JSON/CSV and insert fallback E2E passed"
