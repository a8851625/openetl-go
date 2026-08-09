#!/bin/sh

# E2E: MySQL snapshot_cdc -> Kafka(envelope, single topic) -> Doris(
# table_template + pk_columns_from_metadata)
#
# Validates the fix that lets mysql_snapshot_cdc emit a JSON-object
# Metadata.Key (e.g. {"id":123}) so the doris sink with
# pk_columns_from_metadata can auto-detect the primary key column from the
# relayed Kafka envelope, without a static pk_columns config. Before the fix
# the doris sink failed with: "pk_columns_from_metadata requires Metadata.Key
# to be a non-empty JSON object".
#
# Topology:
#   - Leg 1 (producer): mysql_snapshot_cdc(customers) -> kafka(single topic,
#     envelope). The snapshot phase dumps the 5 seeded rows; the key written
#     into each envelope must be {"id":N}.
#   - Leg 2 (consumer): kafka(envelope) -> doris(table_template=ods_{table},
#     pk_columns_from_metadata=true, auto_create=true). Doris derives the
#     UNIQUE KEY column "id" from the envelope key and auto-creates
#     ods_customers.
#
# Coverage:
#   - mysql_snapshot_cdc writes JSON-object Metadata.Key in the snapshot phase
#   - kafka sink preserves the key in the envelope
#   - kafka source format=envelope restores Metadata.Key
#   - doris sink pk_columns_from_metadata derives the PK column name from it
#   - doris sink auto_create builds a UNIQUE KEY model from the detected PK
#
# SKIP (exit 77) when Doris FE/BE or Redpanda or MySQL source is not
# available; this is a skip, not a pass.

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
MYSQL_CONTAINER="etl-mysql-source"
REDPANDA_CONTAINER="etl-redpanda"
DORIS_FE_CONTAINER="etl-doris-fe"
DORIS_BE_CONTAINER="etl-doris-be"
DORIS_NETWORK="etl-doris-net"
DORIS_FE_IMAGE="docker.io/apache/doris:fe-2.1.11"
DORIS_BE_IMAGE="docker.io/apache/doris:be-2.1.11"
PRODUCER_APP="etl-openetl-go-snapshot-cdc-kafka-doris-prod"
CONSUMER_APP="etl-openetl-go-snapshot-cdc-kafka-doris-cons"
SOURCE_DB="dzh3136_go"
DORIS_DB="ods_snapshot_doris"
TOPIC="snapshot-cdc-doris-relay"
PRODUCER_API_PORT="${SNAPSHOT_DORIS_PRODUCER_PORT:-8031}"
CONSUMER_API_PORT="${SNAPSHOT_DORIS_CONSUMER_PORT:-8032}"
RUN_TAG="$(date +%s)"

wait_http() {
  url="$1"; i=0
  while [ "$i" -lt 90 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 2
  done
  return 1
}

wait_mysql_healthy() {
  i=0
  while [ "$i" -lt 60 ]; do
    status="$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
    [ "$status" = "healthy" ] && return 0
    i=$((i + 1)); sleep 2
  done
  return 1
}

wait_redpanda() {
  i=0
  while [ "$i" -lt 60 ]; do
    if "$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk cluster info --brokers localhost:9092 >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1)); sleep 2
  done
  return 1
}

doris_sql() {
  "$CONTAINER_CLI" exec "$DORIS_FE_CONTAINER" mysql -h127.0.0.1 -P9030 -uroot "$@"
}

wait_doris_sql() {
  i=0
  while [ "$i" -lt 90 ]; do
    if doris_sql -e "SELECT 1" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 2
  done
  return 1
}

wait_doris_backend_alive() {
  i=0
  alive=0
  while [ "$i" -lt 90 ]; do
    alive="$(doris_sql -N -e "SHOW BACKENDS;" 2>/dev/null | grep -c 'true' || true)"
    [ "${alive:-0}" -ge 1 ] && return 0
    i=$((i + 1)); sleep 3
  done
  doris_sql -e "SHOW BACKENDS;" || true
  return 1
}

ensure_doris() {
  # Reuse a running Doris FE/BE if present; otherwise start the canonical
  # etl-doris-net pair (same recipe as hack/e2e-doris.sh).
  if "$CONTAINER_CLI" exec "$DORIS_FE_CONTAINER" mysql -h127.0.0.1 -P9030 -uroot -e "SELECT 1" >/dev/null 2>&1; then
    echo "==> Doris FE already running"
    return 0
  fi
  echo "==> Start Doris FE/BE (etl-doris-net)"
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
}

wait_doris_count() {
  table="$1"; expected="$2"
  i=0
  while [ "$i" -lt 90 ]; do
    count="$(doris_sql -N "$DORIS_DB" -e "SELECT COUNT(*) FROM ${table};" 2>/dev/null | tr -d '[:space:]' || true)"
    [ "$count" = "$expected" ] && return 0
    i=$((i + 1)); sleep 2
  done
  echo "wait_doris_count: $table expected $expected, last got '$count'"; return 1
}

wait_producer_written() {
  expected="$1"
  i=0
  body=""
  while [ "$i" -lt 120 ]; do
    body="$(curl -fsS "http://127.0.0.1:${PRODUCER_API_PORT}/api/v2/pipelines" || true)"
    echo "$body" | grep '"name":"mysql-snapshot-cdc-to-kafka"' | grep "\"records_written\":$expected" >/dev/null 2>&1 && return 0
    i=$((i + 1)); sleep 1
  done
  echo "$body"; return 1
}

wait_consumer_written() {
  expected="$1"
  i=0
  body=""
  while [ "$i" -lt 120 ]; do
    body="$(curl -fsS "http://127.0.0.1:${CONSUMER_API_PORT}/api/v2/pipelines" || true)"
    echo "$body" | grep '"name":"kafka-envelope-to-doris-metadata-pk"' | grep "\"records_written\":$expected" >/dev/null 2>&1 && return 0
    i=$((i + 1)); sleep 1
  done
  echo "$body"; return 1
}

cleanup() {
  "$CONTAINER_CLI" rm -f "$PRODUCER_APP" "$CONSUMER_APP" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# --- Dependencies: MySQL + Redpanda required, Doris started on demand ----

"$CONTAINER_CLI" inspect "$MYSQL_CONTAINER" >/dev/null 2>&1 || {
  echo "SKIP: MySQL source container $MYSQL_CONTAINER not running (run docker-compose.dev.yml first); this is a skip, not a pass." >&2
  exit 77
}
"$CONTAINER_CLI" inspect "$REDPANDA_CONTAINER" >/dev/null 2>&1 || {
  echo "SKIP: Redpanda container $REDPANDA_CONTAINER not running; this is a skip, not a pass." >&2
  exit 77
}

ensure_doris

if [ "${E2E_SKIP_BUILD:-0}" = "1" ]; then
  echo "==> Skip image build (E2E_SKIP_BUILD=1, using $IMAGE)"
else
  echo "==> Build image"
  "$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile .
fi

echo "==> Start MySQL source + Redpanda"
compose -f docker-compose.dev.yml up -d mysql-source redpanda

echo "==> Wait MySQL healthy"
wait_mysql_healthy

echo "==> Wait Redpanda"
wait_redpanda

echo "==> Prepare source rows (deterministic 5-row customers fixture)"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e \
  "CREATE DATABASE IF NOT EXISTS ${SOURCE_DB}; \
   GRANT ALL PRIVILEGES ON ${SOURCE_DB}.* TO 'sync_user'@'%'; FLUSH PRIVILEGES;" 2>/dev/null || true
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 "$SOURCE_DB" -e \
  "DROP TABLE IF EXISTS customers; \
   CREATE TABLE customers (id INT NOT NULL PRIMARY KEY, name VARCHAR(100), email VARCHAR(100), phone VARCHAR(32), status VARCHAR(20), amount DECIMAL(10,2)); \
   INSERT INTO customers (id, name, email, phone, status, amount) VALUES \
   (1, 'Snapshot Doris Alice', 'sd-alice@example.com', '13900009101', 'active', 111.11), \
   (2, 'Snapshot Doris Bob',   'sd-bob@example.com',   '13900009102', 'active', 222.22), \
   (3, 'Snapshot Doris Carol', 'sd-carol@example.com', '13900009103', 'inactive', 33.33), \
   (4, 'Snapshot Doris Dave',  'sd-dave@example.com',  '13900009104', 'active', 444.44), \
   (5, 'Snapshot Doris Eve',   'sd-eve@example.com',   '13900009105', 'active', 55.55);"

echo "==> Reset Doris target + Kafka state"
doris_sql -e "CREATE DATABASE IF NOT EXISTS ${DORIS_DB};" >/dev/null
doris_sql "$DORIS_DB" -e "DROP TABLE IF EXISTS ods_customers;" 2>/dev/null || true
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk group delete "$TOPIC" --brokers localhost:9092 >/dev/null 2>&1 || true
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic delete "$TOPIC" --brokers localhost:9092 >/dev/null 2>&1 || true
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic create "$TOPIC" --partitions 1 >/dev/null 2>&1 || true

echo "==> Reset ETL data"
rm -rf data-snapshot-cdc-kafka-doris-producer data-snapshot-cdc-kafka-doris-consumer
mkdir -p data-snapshot-cdc-kafka-doris-producer/output data-snapshot-cdc-kafka-doris-producer/checkpoint data-snapshot-cdc-kafka-doris-producer/dlq \
         data-snapshot-cdc-kafka-doris-consumer/output data-snapshot-cdc-kafka-doris-consumer/checkpoint data-snapshot-cdc-kafka-doris-consumer/dlq logs
chmod -R a+rwX data-snapshot-cdc-kafka-doris-producer data-snapshot-cdc-kafka-doris-consumer logs

echo "==> Start consumer leg FIRST (kafka envelope -> doris) so it is subscribed"
cleanup
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --network "$DORIS_NETWORK" \
  --name "$CONSUMER_APP" \
  -p "${CONSUMER_API_PORT}:8001" \
  -v "$ROOT_DIR/testdata/pipes-kafka-doris-metadata-pk:/app/pipes:ro" \
  -v "$ROOT_DIR/data-snapshot-cdc-kafka-doris-consumer:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE" >/dev/null
wait_http "http://127.0.0.1:${CONSUMER_API_PORT}/api/v2/health"

echo "==> Start producer leg (mysql snapshot_cdc -> kafka)"
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --network "$DORIS_NETWORK" \
  --name "$PRODUCER_APP" \
  -p "${PRODUCER_API_PORT}:8001" \
  -v "$ROOT_DIR/testdata/pipes-snapshot-cdc-kafka:/app/pipes:ro" \
  -v "$ROOT_DIR/data-snapshot-cdc-kafka-doris-producer:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE" >/dev/null
wait_http "http://127.0.0.1:${PRODUCER_API_PORT}/api/v2/health"

echo "==> Wait both pipelines running"
i=0
while [ "$i" -lt 60 ]; do
  p="$(curl -fsS "http://127.0.0.1:${PRODUCER_API_PORT}/api/v2/pipelines")"
  c="$(curl -fsS "http://127.0.0.1:${CONSUMER_API_PORT}/api/v2/pipelines")"
  echo "$p" | grep '"name":"mysql-snapshot-cdc-to-kafka"' | grep '"status":"running"' >/dev/null 2>&1 \
    && echo "$c" | grep '"name":"kafka-envelope-to-doris-metadata-pk"' | grep '"status":"running"' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done

echo "==> Wait snapshot dump (5 rows) reach Doris via relay"
wait_producer_written 5
wait_consumer_written 5
wait_doris_count "ods_customers" 5

echo "==> Verify the envelope key is a JSON object {\"id\":N} (the regression)"
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic consume "$TOPIC" --brokers localhost:9092 --num 5 --group "snapshot-doris-e2e-inspect-$RUN_TAG" > data-snapshot-cdc-kafka-doris-producer/envelopes.jsonl
# The Kafka message partition key (set from Metadata.Key by the kafka sink)
# must be a JSON object {"id":N}, proving mysql_snapshot_cdc emitted the
# PK as a structured key. This is what pk_columns_from_metadata reads back.
grep '"key": "{\\"id\\"' data-snapshot-cdc-kafka-doris-producer/envelopes.jsonl >/dev/null

echo "==> Verify Doris auto-created ods_customers with a UNIQUE KEY on the detected 'id' column"
ddl="$(doris_sql "$DORIS_DB" -e "SHOW CREATE TABLE ods_customers;")"
echo "$ddl" | grep -i 'UNIQUE KEY' | grep -i '`id`' >/dev/null

echo "==> Verify a representative row round-tripped"
amt="$(doris_sql -N "$DORIS_DB" -e "SELECT amount FROM ods_customers WHERE id=1;" 2>/dev/null | tr -d '[:space:]' || true)"
test "$amt" = "111.11"

echo "==> Step 2/2: CDC UPDATE one row on source; verify upsert via relay (pk_columns_from_metadata UPDATE path)"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 "$SOURCE_DB" -e \
  "UPDATE customers SET amount=999.99, status='updated' WHERE id=1;"
wait_producer_written 6
wait_consumer_written 6
i=0
while [ "$i" -lt 60 ]; do
  amt="$(doris_sql -N "$DORIS_DB" -e "SELECT amount FROM ods_customers WHERE id=1;" 2>/dev/null | tr -d '[:space:]' || true)"
  [ "$amt" = "999.99" ] && break
  i=$((i + 1)); sleep 1
done
test "$amt" = "999.99"
status="$(doris_sql -N "$DORIS_DB" -e "SELECT status FROM ods_customers WHERE id=1;" 2>/dev/null | tr -d '[:space:]' || true)"
test "$status" = "updated"

echo "snapshot_cdc -> kafka -> doris (pk_columns_from_metadata) E2E passed"
