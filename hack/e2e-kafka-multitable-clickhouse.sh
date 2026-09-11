#!/bin/sh

# E2E: Kafka(envelope, multi-table) -> ClickHouse(table_template fan-out)
#
# Validates the ClickHouse sink table_template + pk_columns_from_metadata
# feature: a single sink routing a mixed multi-table Kafka stream
# (format=envelope) into multiple ClickHouse tables, where each record's
# destination and ORDER BY key are derived from envelope metadata.
#
# Topology:
#   - envelope messages for three tables (orders, users, order_items) are produced into ONE
#     Kafka topic via rpk
#   - one pipeline: kafka source(topic, format=envelope) -> clickhouse sink(
#     table_template="ods_{table}", pk_columns_from_metadata=true,
#     auto_create=true, schema_drift=add_columns, version_mode=source_order)
#
# Coverage:
#   - kafka source format=envelope restores Table metadata from envelope
#   - clickhouse sink table_template="ods_{table}" fans out to ods_orders /
#     ods_users / ods_order_items
#   - heterogeneous ORDER BY keys (orders.order_id, users.user_no,
#     order_items.(tenant_id,item_id)) drive
#     auto-created ReplacingMergeTree(_version, _is_deleted) tables
#   - UPDATE on the same key collapses to the final row (FINAL view)
#   - a composite-key UPDATE that changes item_id writes the new row and a
#     source-ordered old-key tombstone
#   - DELETE persists a source-ordered tombstone (no asynchronous mutation)
#   - schema_drift=add_columns adds a new column mid-stream
#   - ClickHouse outage routes one complete-identity event to DLQ; restart and
#     controlled replay writes it back
#   - checkpoint reset plus external consumer-group reset replays from oldest;
#     source offsets keep FINAL business state unchanged with no resurrection
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
PIPELINE_PREFIX="kafka-multitable-to-clickhouse"

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

cleanup() {
  "$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

consumer_group_exists() {
  "$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk group list --brokers localhost:9092 2>/dev/null |
    awk 'NR > 1 {print $NF}' | grep -Fx "$1" >/dev/null 2>&1
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
if "$CONTAINER_CLI" inspect "$CH_CONTAINER" >/dev/null 2>&1; then
  "$CONTAINER_CLI" start "$CH_CONTAINER" >/dev/null
else
  compose -f docker-compose.dev.yml up -d clickhouse >/dev/null
fi
wait_http "http://127.0.0.1:8123/ping" || { echo "SKIP: ClickHouse not reachable"; exit 77; }

echo "==> Ensure Redpanda up"
if "$CONTAINER_CLI" inspect "$REDPANDA_CONTAINER" >/dev/null 2>&1; then
  "$CONTAINER_CLI" start "$REDPANDA_CONTAINER" >/dev/null
else
  compose -f docker-compose.dev.yml up -d redpanda >/dev/null
fi
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
ch_query "DROP TABLE IF EXISTS ${CH_DB}.ods_order_items" || true
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic delete "$TOPIC" >/dev/null 2>&1 || true
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic create "$TOPIC" --partitions 1 >/dev/null 2>&1 || true

echo "==> Produce envelope messages for three tables into the single topic"
# orders: BIGINT PK order_id; users: VARCHAR PK user_no; order_items:
# composite (tenant_id,item_id). All go to one topic.
# Every envelope freezes its declared primary-key columns. UPDATE also carries
# a complete before image; missing declarations/before identity fail closed
# before the metadata-PK sink.
RUN_TAG="run$(date +%s)"
PIPELINE="${PIPELINE_PREFIX}-${RUN_TAG}"

produce_envelope() {
  msg="$1"
  "$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" sh -c "printf '%s\n' '$msg' | rpk topic produce $TOPIC"
}
produce_envelope "{\"event_id\":\"${RUN_TAG}-e1\",\"op\":\"INSERT\",\"table\":\"orders\",\"primary_key_columns\":[\"order_id\"],\"key\":\"{\\\"order_id\\\":5001}\",\"data\":{\"order_id\":5001,\"amount\":11.00,\"run\":\"$RUN_TAG\"}}"
produce_envelope "{\"event_id\":\"${RUN_TAG}-e2\",\"op\":\"UPDATE\",\"table\":\"orders\",\"primary_key_columns\":[\"order_id\"],\"key\":\"{\\\"order_id\\\":5001}\",\"before\":{\"order_id\":5001,\"amount\":11.00,\"run\":\"$RUN_TAG\"},\"data\":{\"order_id\":5001,\"amount\":15.00,\"run\":\"$RUN_TAG\"}}"
produce_envelope "{\"event_id\":\"${RUN_TAG}-e3\",\"op\":\"INSERT\",\"table\":\"orders\",\"primary_key_columns\":[\"order_id\"],\"key\":\"{\\\"order_id\\\":5002}\",\"data\":{\"order_id\":5002,\"amount\":22.00,\"run\":\"$RUN_TAG\"}}"
produce_envelope "{\"event_id\":\"${RUN_TAG}-e4\",\"op\":\"INSERT\",\"table\":\"users\",\"primary_key_columns\":[\"user_no\"],\"key\":\"{\\\"user_no\\\":\\\"MT_U1\\\"}\",\"data\":{\"user_no\":\"MT_U1\",\"name\":\"Alice\",\"run\":\"$RUN_TAG\"}}"
produce_envelope "{\"event_id\":\"${RUN_TAG}-e5\",\"op\":\"INSERT\",\"table\":\"users\",\"primary_key_columns\":[\"user_no\"],\"key\":\"{\\\"user_no\\\":\\\"MT_U2\\\"}\",\"data\":{\"user_no\":\"MT_U2\",\"name\":\"Bob\",\"run\":\"$RUN_TAG\"}}"
produce_envelope "{\"event_id\":\"${RUN_TAG}-e6\",\"op\":\"INSERT\",\"table\":\"order_items\",\"primary_key_columns\":[\"tenant_id\",\"item_id\"],\"key\":\"{\\\"tenant_id\\\":\\\"acme\\\",\\\"item_id\\\":1}\",\"data\":{\"tenant_id\":\"acme\",\"item_id\":1,\"qty\":2,\"run\":\"$RUN_TAG\"}}"
produce_envelope "{\"event_id\":\"${RUN_TAG}-e7\",\"op\":\"UPDATE\",\"table\":\"order_items\",\"primary_key_columns\":[\"tenant_id\",\"item_id\"],\"key\":\"{\\\"tenant_id\\\":\\\"acme\\\",\\\"item_id\\\":1}\",\"before\":{\"tenant_id\":\"acme\",\"item_id\":1,\"qty\":2,\"run\":\"$RUN_TAG\"},\"data\":{\"tenant_id\":\"acme\",\"item_id\":1,\"qty\":3,\"run\":\"$RUN_TAG\"}}"
produce_envelope "{\"event_id\":\"${RUN_TAG}-e8\",\"op\":\"UPDATE\",\"table\":\"order_items\",\"primary_key_columns\":[\"tenant_id\",\"item_id\"],\"key\":\"{\\\"tenant_id\\\":\\\"acme\\\",\\\"item_id\\\":1}\",\"before\":{\"tenant_id\":\"acme\",\"item_id\":1,\"qty\":3,\"run\":\"$RUN_TAG\"},\"data\":{\"tenant_id\":\"acme\",\"item_id\":2,\"qty\":4,\"run\":\"$RUN_TAG\"}}"

echo "==> Reset ETL data + pipes"
rm -rf data-ch-multitable
mkdir -p data-ch-multitable/pipes data-ch-multitable/output data-ch-multitable/checkpoint data-ch-multitable/dlq logs
cp testdata/pipes-kafka-ch-multitable/*.yaml data-ch-multitable/pipes/
# Unique consumer group per run: the topic accumulates messages across runs,
# and a fresh group_id re-consumes from oldest so each run is independent.
sed -i.bak "s/kafka-multitable-to-clickhouse/${PIPELINE}/" data-ch-multitable/pipes/*.yaml && rm -f data-ch-multitable/pipes/*.bak
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

echo "==> Wait table_template fan-out: ods_orders=2, ods_users=2, ods_order_items=1"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_orders FINAL" "2"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_users FINAL" "2"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_order_items FINAL" "1"

echo "==> Verify metadata-key upsert retained the final update"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_orders FINAL WHERE order_id = 5001 AND amount = 15.00" "1"

echo "==> Verify heterogeneous ORDER BY (auto-create DDL)"
orders_ddl="$(ch_query "SHOW CREATE TABLE ${CH_DB}.ods_orders")"
users_ddl="$(ch_query "SHOW CREATE TABLE ${CH_DB}.ods_users")"
items_ddl="$(ch_query "SHOW CREATE TABLE ${CH_DB}.ods_order_items")"
printf '%s\n' "$orders_ddl" | grep "ORDER BY order_id" >/dev/null || { echo "orders DDL missing ORDER BY order_id:"; printf '%s\n' "$orders_ddl"; exit 1; }
printf '%s\n' "$users_ddl" | grep "ORDER BY user_no" >/dev/null || { echo "users DDL missing ORDER BY user_no:"; printf '%s\n' "$users_ddl"; exit 1; }
printf '%s\n' "$items_ddl" | grep -E 'ORDER BY \((tenant_id, item_id|item_id, tenant_id)\)' >/dev/null || { echo "order_items DDL missing composite ORDER BY:"; printf '%s\n' "$items_ddl"; exit 1; }
printf '%s\n' "$orders_ddl" | grep "ENGINE = ReplacingMergeTree" >/dev/null
printf '%s\n' "$users_ddl" | grep "ENGINE = ReplacingMergeTree" >/dev/null
printf '%s\n' "$items_ddl" | grep "ENGINE = ReplacingMergeTree" >/dev/null

echo "==> Verify composite-key UPDATE and key-changing UPDATE tombstone"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_order_items FINAL WHERE tenant_id = 'acme' AND item_id = 2 AND qty = 4" "1"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_order_items FINAL WHERE tenant_id = 'acme' AND item_id = 1" "0"
wait_ch_value "SELECT uniqExact(tuple(tenant_id, item_id)) FROM ${CH_DB}.ods_order_items FINAL" "1"

echo "==> Verify schema drift add_columns mid-stream"
produce_envelope "{\"event_id\":\"${RUN_TAG}-e9\",\"op\":\"INSERT\",\"table\":\"users\",\"primary_key_columns\":[\"user_no\"],\"key\":\"{\\\"user_no\\\":\\\"MT_U3\\\"}\",\"data\":{\"user_no\":\"MT_U3\",\"name\":\"Cara\",\"city\":\"Shenzhen\",\"run\":\"$RUN_TAG\"}}"
wait_ch_value "SELECT count() FROM system.columns WHERE database = '${CH_DB}' AND table = 'ods_users' AND name = 'city'" "1"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_users FINAL WHERE user_no = 'MT_U3' AND city = 'Shenzhen'" "1"

echo "==> Verify DELETE persists a source-ordered tombstone"
produce_envelope "{\"event_id\":\"${RUN_TAG}-e10\",\"op\":\"DELETE\",\"table\":\"orders\",\"primary_key_columns\":[\"order_id\"],\"key\":\"{\\\"order_id\\\":5001}\",\"data\":{\"order_id\":5001}}"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_orders FINAL WHERE order_id = 5001" "0"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_orders FINAL" "1"

echo "==> Verify ClickHouse outage -> DLQ -> restart -> replay"
"$CONTAINER_CLI" stop "$CH_CONTAINER" >/dev/null
produce_envelope "{\"event_id\":\"${RUN_TAG}-outage\",\"op\":\"INSERT\",\"table\":\"users\",\"primary_key_columns\":[\"user_no\"],\"key\":\"{\\\"user_no\\\":\\\"MT_OUTAGE\\\"}\",\"data\":{\"user_no\":\"MT_OUTAGE\",\"name\":\"Outage\",\"run\":\"$RUN_TAG\"}}"
dlq_body=""
i=0
while [ "$i" -lt 90 ]; do
  dlq_body="$(curl -fsS "http://127.0.0.1:${API_PORT}/api/v2/dlq/${PIPELINE}?contains=MT_OUTAGE&limit=10" 2>/dev/null || true)"
  if printf '%s' "$dlq_body" | grep -q "MT_OUTAGE"; then break; fi
  i=$((i + 1)); sleep 1
done
printf '%s\n' "$dlq_body" | grep "MT_OUTAGE" >/dev/null || { echo "outage event did not reach DLQ" >&2; exit 1; }
dlq_id="$(printf '%s' "$dlq_body" | grep -o '"id":[0-9][0-9]*' | head -n1 | sed 's/[^0-9]//g')"
[ -n "$dlq_id" ] || { echo "could not identify outage DLQ id" >&2; exit 1; }
curl -fsS -X POST "http://127.0.0.1:${API_PORT}/api/v2/pipelines/${PIPELINE}/stop" >/dev/null || true
"$CONTAINER_CLI" stop "$APP_CONTAINER" >/dev/null || true
"$CONTAINER_CLI" start "$CH_CONTAINER" >/dev/null
wait_http "http://127.0.0.1:8123/ping" || { echo "ClickHouse did not recover" >&2; exit 1; }
"$CONTAINER_CLI" start "$APP_CONTAINER" >/dev/null || true
wait_http "http://127.0.0.1:${API_PORT}/api/v2/health"
replay_body="$(curl -fsS -X POST "http://127.0.0.1:${API_PORT}/api/v2/dlq/${PIPELINE}/${dlq_id}/replay")"
printf '%s\n' "$replay_body" | grep '"replayed":1' >/dev/null || { echo "outage DLQ replay failed: $replay_body" >&2; exit 1; }
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_users FINAL WHERE user_no = 'MT_OUTAGE' AND name = 'Outage'" "1"
dlq_after="$(curl -fsS "http://127.0.0.1:${API_PORT}/api/v2/dlq/${PIPELINE}?contains=MT_OUTAGE&limit=10")"
if printf '%s' "$dlq_after" | grep -q "\"id\":${dlq_id}"; then
  echo "replayed outage DLQ id ${dlq_id} was not deleted" >&2
  exit 1
fi

echo "==> Verify checkpoint reset replay is absorbed by ReplacingMergeTree"
curl -fsS -X POST "http://127.0.0.1:${API_PORT}/api/v2/pipelines/${PIPELINE}/stop" >/dev/null || true
curl -fsS -X POST "http://127.0.0.1:${API_PORT}/api/v2/pipelines/${PIPELINE}/checkpoint/reset" >/dev/null
# Resetting OpenETL's checkpoint alone deliberately does not rewind Kafka's
# external consumer-group offset. Reset the now-stopped group as the documented
# operator action so this section proves a real offset-0 replay.
i=0
while [ "$i" -lt 15 ]; do
  "$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk group delete "${PIPELINE}" --brokers localhost:9092 >/dev/null 2>&1 || true
  if ! consumer_group_exists "${PIPELINE}"; then
    break
  fi
  i=$((i + 1)); sleep 1
done
consumer_group_exists "${PIPELINE}" && {
  echo "Kafka consumer group still exists after reset" >&2
  exit 1
} || true
# Start may race the async teardown of the previous run; retry briefly.
i=0
while [ "$i" -lt 10 ]; do
  if curl -fsS -X POST "http://127.0.0.1:${API_PORT}/api/v2/pipelines/${PIPELINE}/start" >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1)); sleep 2
done
i=0
while [ "$i" -lt 120 ]; do
  body="$(curl -fsS "http://127.0.0.1:${API_PORT}/api/v2/pipelines")" || body=""
  printf '%s' "$body" | grep "\"name\":\"${PIPELINE}\"" | grep '"status":"running"' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 2
done
# FINAL business-key state must remain correct (no silent loss / no inflated keys).
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_orders FINAL" "1"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_orders FINAL WHERE order_id = 5002 AND amount = 22.00" "1"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_orders FINAL WHERE order_id = 5001" "0"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_users FINAL" "4"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_users FINAL WHERE user_no = 'MT_U3' AND city = 'Shenzhen'" "1"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_users FINAL WHERE user_no = 'MT_OUTAGE' AND name = 'Outage'" "1"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_order_items FINAL" "1"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_order_items FINAL WHERE tenant_id = 'acme' AND item_id = 2 AND qty = 4" "1"
wait_ch_value "SELECT count() FROM ${CH_DB}.ods_order_items FINAL WHERE tenant_id = 'acme' AND item_id = 1" "0"
# Distinct business keys must not inflate after replay.
wait_ch_value "SELECT uniqExact(order_id) FROM ${CH_DB}.ods_orders FINAL" "1"
wait_ch_value "SELECT uniqExact(user_no) FROM ${CH_DB}.ods_users FINAL" "4"

echo ""
echo "===== PASS: kafka(envelope, multi-table) -> clickhouse table_template fan-out ====="
echo "  ods_orders  (ORDER BY order_id)  : update absorbed, delete applied, replay-safe"
echo "  ods_users   (ORDER BY user_no)   : schema drift + outage DLQ replay, replay-safe"
echo "  ods_order_items (ORDER BY tenant_id,item_id): composite key + key-change tombstone"
echo ""
echo "Cleaning up app container"
