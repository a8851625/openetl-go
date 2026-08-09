#!/bin/sh

# E2E: Kafka(envelope, multi-table) -> ClickHouse(table_template fan-out)
#
# Validates the ClickHouse sink table_template + pk_columns_from_metadata
# feature: a single sink routing a mixed multi-table Kafka stream
# (format=envelope) into multiple ClickHouse tables, where each record's
# destination and ORDER BY key are derived from envelope metadata.
#
# Topology:
#   - envelope messages for two tables (orders, users) are produced into ONE
#     Kafka topic via rpk
#   - one pipeline: kafka source(topic, format=envelope) -> clickhouse sink(
#     table_template="ods_{table}", pk_columns_from_metadata=true,
#     auto_create=true, schema_drift=add_columns)
#
# Coverage:
#   - kafka source format=envelope restores Table metadata from envelope
#   - clickhouse sink table_template="ods_{table}" fans out to ods_orders /
#     ods_users
#   - heterogeneous ORDER BY keys (orders.order_id, users.user_no) drive
#     auto-created ReplacingMergeTree(_version) tables
#   - UPDATE on the same key collapses to the final row (FINAL view)
#   - DELETE routes to ALTER TABLE DELETE (mutation)
#   - schema_drift=add_columns adds a new column mid-stream
#   - checkpoint reset replay is absorbed: FINAL business state unchanged,
#     no duplicate inflation
#
# SKIP (exit 77) when ClickHouse or Redpanda is not available; this is a
# skip, not a pass. Core routing logic is covered by unit tests
# (internal/etl/sink/clickhouse_table_template_test.go).

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
REDPANDA_CONTAINER="etl-redpanda"
CH_CONTAINER="etl-clickhouse"
APP_CONTAINER="etl-openetl-ch-multitable"
CH_DB="dzh3136_go"
TOPIC="cdc-ch-multitable"
API_PORT="${CH_MT_API_PORT:-8028}"

wait_http() {
  url="$1"; i=0
  while [ "$i" -lt 90 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 2
  done
  return 1
}

ch_query() {
  "$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "$1" 2>/dev/null
}

wait_ch_value() {
  sql="$1"; expected="$2"
  i=0; got=""
  while [ "$i" -lt 120 ]; do
    got="$(ch_query "$sql" | tr -d '[:space:]' || true)"
    if [ "$got" = "$expected" ]; then return 0; fi
    i=$((i + 1)); sleep 2
  done
  echo "TIMEOUT waiting for '$sql' = $expected (last=$got)" >&2
  return 1
}

echo "==> Build image"
if [ "${E2E_SKIP_BUILD:-0}" = "1" ]; then
  echo "    (E2E_SKIP_BUILD=1, using $IMAGE)"
else
  "$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile .
fi

echo "==> Ensure ClickHouse up"
if ! "$CONTAINER_CLI" image inspect docker.io/clickhouse/clickhouse-server:24.3-alpine >/dev/null 2>&1; then
  echo "SKIP: clickhouse-server image not available locally; pull it first." >&2
  exit 77
fi
compose -f docker-compose.dev.yml up -d clickhouse >/dev/null
wait_http "http://127.0.0.1:8123/ping" || { echo "SKIP: ClickHouse not reachable"; exit 77; }

echo "==> Ensure Redpanda up"
compose -f docker-compose.dev.yml up -d redpanda >/dev/null
i=0
while [ "$i" -lt 90 ]; do
  "$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 2
done
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null 2>&1 || { echo "SKIP: Redpanda not reachable"; exit 77; }

echo "==> Prepare ClickHouse database and Kafka topic"
ch_query "CREATE DATABASE IF NOT EXISTS ${CH_DB}" >/dev/null || true
ch_query "DROP TABLE IF EXISTS ${CH_DB}.ods_orders" || true
ch_query "DROP TABLE IF EXISTS ${CH_DB}.ods_users" || true
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic delete "$TOPIC" >/dev/null 2>&1 || true
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic create "$TOPIC" --partitions 1 >/dev/null 2>&1 || true

echo "==> Produce envelope messages for two tables into the single topic"
# orders: BIGINT PK order_id; users: VARCHAR PK user_no. Both go to one topic.
# A DELETE event carries the key values in data (envelope has no before image).
RUN_TAG="run$(date +%s)"

produce_envelope() {
  msg="$1"
  "$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" sh -c "printf '%s\n' '$msg' | rpk topic produce $TOPIC"
}
produce_envelope "{\"event_id\":\"${RUN_TAG}-e1\",\"op\":\"INSERT\",\"table\":\"orders\",\"key\":\"{\\\"order_id\\\":5001}\",\"data\":{\"order_id\":5001,\"amount\":11.00,\"run\":\"$RUN_TAG\"}}"
produce_envelope "{\"event_id\":\"${RUN_TAG}-e2\",\"op\":\"UPDATE\",\"table\":\"orders\",\"key\":\"{\\\"order_id\\\":5001}\",\"data\":{\"order_id\":5001,\"amount\":15.00,\"run\":\"$RUN_TAG\"}}"
produce_envelope "{\"event_id\":\"${RUN_TAG}-e3\",\"op\":\"INSERT\",\"table\":\"orders\",\"key\":\"{\\\"order_id\\\":5002}\",\"data\":{\"order_id\":5002,\"amount\":22.00,\"run\":\"$RUN_TAG\"}}"
produce_envelope "{\"event_id\":\"${RUN_TAG}-e4\",\"op\":\"INSERT\",\"table\":\"users\",\"key\":\"{\\\"user_no\\\":\\\"MT_U1\\\"}\",\"data\":{\"user_no\":\"MT_U1\",\"name\":\"Alice\",\"run\":\"$RUN_TAG\"}}"
produce_envelope "{\"event_id\":\"${RUN_TAG}-e5\",\"op\":\"INSERT\",\"table\":\"users\",\"key\":\"{\\\"user_no\\\":\\\"MT_U2\\\"}\",\"data\":{\"user_no\":\"MT_U2\",\"name\":\"Bob\",\"run\":\"$RUN_TAG\"}}"

echo "==> Reset ETL data + pipes"
rm -rf data-ch-multitable
mkdir -p data-ch-multitable/pipes data-ch-multitable/output data-ch-multitable/checkpoint data-ch-multitable/dlq logs
cp testdata/pipes-kafka-ch-multitable/*.yaml data-ch-multitable/pipes/
# Unique consumer group per run: the topic accumulates messages across runs,
# and a fresh group_id re-consumes from oldest so each run is independent.
sed -i.bak "s/kafka-multitable-to-clickhouse/kafka-multitable-to-clickhouse-$RUN_TAG/" data-ch-multitable/pipes/*.yaml && rm -f data-ch-multitable/pipes/*.bak
chmod -R a+rwX data-ch-multitable logs
chmod a+rwX logs

echo "==> Run consumer pipeline (kafka envelope -> clickhouse table_template)"
# The app container must resolve etl-clickhouse and etl-redpanda, which live on
# different compose networks; attach to both dynamically.
NETWORKS="$(printf '%s %s' \
  "$("$CONTAINER_CLI" inspect "$CH_CONTAINER" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' 2>/dev/null || true)" \
  "$("$CONTAINER_CLI" inspect "$REDPANDA_CONTAINER" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' 2>/dev/null || true)")"
NETWORK_ARG="$(printf '%s\n' "$NETWORKS" | tr ' ' '\n' | sort -u | grep -v '^$' | paste -sd, -)"
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d --network "$NETWORK_ARG" --add-host host.docker.internal:host-gateway --name "$APP_CONTAINER" \
  -p ${API_PORT}:8001 \
  -v "$ROOT_DIR/data-ch-multitable:/app/data" \
  -v "$ROOT_DIR/data-ch-multitable/pipes:/app/pipes:ro" \
  "$IMAGE"
wait_http "http://127.0.0.1:${API_PORT}/api/v2/health"

echo "==> Wait table_template fan-out: ods_orders=2, ods_users=2"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_orders FINAL" "2"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_users FINAL" "2"

echo "==> Verify metadata-key upsert retained the final update"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_orders FINAL WHERE order_id = 5001 AND amount = 15.00" "1"

echo "==> Verify heterogeneous ORDER BY (auto-create DDL)"
orders_ddl="$(ch_query "SHOW CREATE TABLE ${CH_DB}.ods_orders")"
users_ddl="$(ch_query "SHOW CREATE TABLE ${CH_DB}.ods_users")"
printf '%s\n' "$orders_ddl" | grep "ORDER BY order_id" >/dev/null || { echo "orders DDL missing ORDER BY order_id:"; printf '%s\n' "$orders_ddl"; exit 1; }
printf '%s\n' "$users_ddl" | grep "ORDER BY user_no" >/dev/null || { echo "users DDL missing ORDER BY user_no:"; printf '%s\n' "$users_ddl"; exit 1; }
printf '%s\n' "$orders_ddl" | grep "ENGINE = ReplacingMergeTree" >/dev/null
printf '%s\n' "$users_ddl" | grep "ENGINE = ReplacingMergeTree" >/dev/null

echo "==> Verify schema drift add_columns mid-stream"
produce_envelope "{\"event_id\":\"${RUN_TAG}-e6\",\"op\":\"INSERT\",\"table\":\"users\",\"key\":\"{\\\"user_no\\\":\\\"MT_U3\\\"}\",\"data\":{\"user_no\":\"MT_U3\",\"name\":\"Cara\",\"city\":\"Shenzhen\",\"run\":\"$RUN_TAG\"}}"
wait_ch_value "SELECT count() FROM system.columns WHERE database = '${CH_DB}' AND table = 'ods_users' AND name = 'city'" "1"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_users FINAL WHERE user_no = 'MT_U3' AND city = 'Shenzhen'" "1"

echo "==> Verify DELETE routes to ALTER TABLE DELETE"
produce_envelope "{\"event_id\":\"${RUN_TAG}-e7\",\"op\":\"DELETE\",\"table\":\"orders\",\"key\":\"{\\\"order_id\\\":5001}\",\"data\":{\"order_id\":5001}}"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_orders FINAL WHERE order_id = 5001" "0"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_orders FINAL" "1"

echo "==> Verify checkpoint reset replay is absorbed by ReplacingMergeTree"
curl -fsS -X POST "http://127.0.0.1:${API_PORT}/api/v2/pipelines/kafka-multitable-to-clickhouse-$RUN_TAG/stop" >/dev/null || true
curl -fsS -X POST "http://127.0.0.1:${API_PORT}/api/v2/pipelines/kafka-multitable-to-clickhouse-$RUN_TAG/checkpoint/reset" >/dev/null
# Start may race the async teardown of the previous run; retry briefly.
i=0
while [ "$i" -lt 10 ]; do
  if curl -fsS -X POST "http://127.0.0.1:${API_PORT}/api/v2/pipelines/kafka-multitable-to-clickhouse-$RUN_TAG/start" >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1)); sleep 2
done
i=0
while [ "$i" -lt 120 ]; do
  body="$(curl -fsS "http://127.0.0.1:${API_PORT}/api/v2/pipelines")" || body=""
  printf '%s' "$body" | grep '"name":"kafka-multitable-to-clickhouse-' | grep '"status":"running"' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 2
done
# FINAL business-key state must remain correct (no silent loss / no inflated keys).
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_orders FINAL" "1"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_orders FINAL WHERE order_id = 5002 AND amount = 22.00" "1"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_orders FINAL WHERE order_id = 5001" "0"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_users FINAL" "3"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_users FINAL WHERE user_no = 'MT_U3' AND city = 'Shenzhen'" "1"
# Distinct business keys must not inflate after replay.
wait_ch_value "SELECT uniqExact(order_id) FROM ${CH_DB}.ods_orders FINAL" "1"
wait_ch_value "SELECT uniqExact(user_no) FROM ${CH_DB}.ods_users FINAL" "3"

echo ""
echo "===== PASS: kafka(envelope, multi-table) -> clickhouse table_template fan-out ====="
echo "  ods_orders  (ORDER BY order_id)  : update absorbed, delete applied, replay-safe"
echo "  ods_users   (ORDER BY user_no)   : schema drift added city, replay-safe"
echo ""
echo "Cleaning up app container"
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
