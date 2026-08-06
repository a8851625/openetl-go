#!/bin/sh

# E2E: MySQL CDC -> Kafka(topic_template relay) -> Kafka(envelope) -> MySQL
#
# Validates the three changes that make a Kafka-relayed CDC chain behave like a
# direct CDC consumer:
#   1. kafka sink topic_template routes per source table (relay-customers),
#      skipping static-topic validation and relying on broker auto-create;
#   2. kafka source format=envelope restores INSERT/UPDATE/DELETE semantics;
#   3. mysql sink applies upsert + delete from the restored operations.
#
# Insert/Update/Delete are emitted on the source; the target MySQL must end up
# reflecting the DELETE (row gone), proving the envelope carried the op through
# the Kafka relay.

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
MYSQL_CONTAINER="etl-mysql-source"
REDPANDA_CONTAINER="etl-redpanda"
RELAY_PRODUCER_APP="etl-openetl-go-cdc-relay-producer"
RELAY_CONSUMER_APP="etl-openetl-go-cdc-relay-consumer"
SOURCE_DB="dzh3136_go"
TARGET_DB="dzh3136_target"
RELAY_TOPIC="relay-customers"

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 60 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1)); sleep 1
  done
  return 1
}

wait_redpanda() {
  i=0
  while [ "$i" -lt 90 ]; do
    if "$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null 2>&1; then
      return 0
    fi
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

# Wait until consumer app's pipeline has written at least N records.
wait_consumer_written() {
  expected="$1"
  i=0
  while [ "$i" -lt 90 ]; do
    body="$(curl -fsS http://127.0.0.1:8011/api/v2/pipelines || true)"
    echo "$body" | grep '"name":"kafka-envelope-to-mysql"' | grep "\"records_written\":$expected" >/dev/null 2>&1 && return 0
    i=$((i + 1)); sleep 1
  done
  echo "$body"; return 1
}

# Wait until producer app's pipeline has written at least N records.
wait_producer_written() {
  expected="$1"
  i=0
  while [ "$i" -lt 90 ]; do
    body="$(curl -fsS http://127.0.0.1:8010/api/v2/pipelines || true)"
    echo "$body" | grep '"name":"mysql-cdc-to-kafka-relay"' | grep "\"records_written\":$expected" >/dev/null 2>&1 && return 0
    i=$((i + 1)); sleep 1
  done
  echo "$body"; return 1
}

cleanup() {
  "$CONTAINER_CLI" rm -f "$RELAY_PRODUCER_APP" "$RELAY_CONSUMER_APP" >/dev/null 2>&1 || true
}
trap cleanup EXIT

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

echo "==> Prepare source/target tables and consumer group"
# Target table mirrors source (reuse the same shape the CDC suite sets up).
# Create the target DB+table BEFORE the cleanup DELETE so the latter succeeds.
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e \
  "CREATE DATABASE IF NOT EXISTS ${TARGET_DB}; \
   CREATE TABLE IF NOT EXISTS ${TARGET_DB}.customers LIKE ${SOURCE_DB}.customers; \
   GRANT ALL PRIVILEGES ON ${TARGET_DB}.* TO 'sync_user'@'%'; FLUSH PRIVILEGES;"
# Clean prior rows with the test id range so the run is deterministic.
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e \
  "DELETE FROM ${SOURCE_DB}.customers WHERE id BETWEEN 9100 AND 9199; \
   DELETE FROM ${TARGET_DB}.customers WHERE id BETWEEN 9100 AND 9199;"

# Reset Kafka state: delete consumer group + topic so the run starts clean.
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk group delete envelope-relay-e2e --brokers localhost:9092 >/dev/null 2>&1 || true
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic delete "$RELAY_TOPIC" >/dev/null 2>&1 || true

echo "==> Reset ETL data"
rm -rf data-relay-producer data-relay-consumer
mkdir -p data-relay-producer/output data-relay-producer/checkpoint data-relay-producer/dlq \
         data-relay-consumer/output data-relay-consumer/checkpoint data-relay-consumer/dlq logs
chmod -R a+rwX data-relay-producer data-relay-consumer
chmod a+rwX logs

echo "==> Start consumer leg (kafka envelope -> mysql) FIRST so it is subscribed"
cleanup
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$RELAY_CONSUMER_APP" \
  -p 8011:8001 \
  -v "$ROOT_DIR/testdata/pipes-kafka-relay-mysql:/app/pipes:ro" \
  -v "$ROOT_DIR/data-relay-consumer:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE" >/dev/null
wait_http "http://127.0.0.1:8011/api/v2/health"

echo "==> Start producer leg (mysql cdc -> kafka relay)"
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$RELAY_PRODUCER_APP" \
  -p 8010:8001 \
  -v "$ROOT_DIR/testdata/pipes-cdc-kafka-relay:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-relay-producer:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE" >/dev/null
wait_http "http://127.0.0.1:8010/api/v2/health"

echo "==> Wait both pipelines running"
i=0
while [ "$i" -lt 60 ]; do
  p="$(curl -fsS http://127.0.0.1:8010/api/v2/pipelines)"
  c="$(curl -fsS http://127.0.0.1:8011/api/v2/pipelines)"
  echo "$p" | grep '"name":"mysql-cdc-to-kafka-relay"' | grep '"status":"running"' >/dev/null 2>&1 \
    && echo "$c" | grep '"name":"kafka-envelope-to-mysql"' | grep '"status":"running"' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done

echo "==> Step 1/3: INSERT two rows on source"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 "$SOURCE_DB" -e \
  "INSERT INTO customers (id, name, email, phone, status, amount) VALUES (9101, 'Relay Alice', 'relay-alice@example.com', '13900009101', 'active', 111.11); \
   INSERT INTO customers (id, name, email, phone, status, amount) VALUES (9102, 'Relay Bob',   'relay-bob@example.com',   '13900009102', 'active', 222.22);"
wait_producer_written 2
wait_consumer_written 2

echo "==> Verify relay topic was auto-created (topic_template path) and carries envelopes"
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic list --brokers localhost:9092 | grep "$RELAY_TOPIC"
# rpk default output escapes JSON quotes inside the value field (\"op\":\"INSERT\"),
# so grep for the escaped form. Each envelope is emitted by the OpenETL kafka sink.
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic consume "$RELAY_TOPIC" --brokers localhost:9092 --num 2 --group "envelope-relay-e2e-inspect-$$" > data-relay-producer/envelopes.jsonl
grep '\\"op\\":\\"INSERT\\"' data-relay-producer/envelopes.jsonl
grep 'Relay Alice' data-relay-producer/envelopes.jsonl
grep 'Relay Bob' data-relay-producer/envelopes.jsonl

echo "==> Verify target MySQL received both inserts via relay"
i=0
while [ "$i" -lt 60 ]; do
  cnt="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e \
    "SELECT COUNT(*) FROM ${TARGET_DB}.customers WHERE id IN (9101,9102);" 2>/dev/null | tr -d '[:space:]')"
  [ "$cnt" = "2" ] && break
  i=$((i + 1)); sleep 1
done
test "$cnt" = "2"

echo "==> Step 2/3: UPDATE one row on source"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 "$SOURCE_DB" -e \
  "UPDATE customers SET amount=999.99, status='updated' WHERE id=9101;"
wait_producer_written 3
wait_consumer_written 3

i=0
while [ "$i" -lt 60 ]; do
  amt="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e \
    "SELECT amount FROM ${TARGET_DB}.customers WHERE id=9101;" 2>/dev/null | tr -d '[:space:]')"
  [ "$amt" = "999.99" ] && break
  i=$((i + 1)); sleep 1
done
test "$amt" = "999.99"
status="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e \
  "SELECT status FROM ${TARGET_DB}.customers WHERE id=9101;" 2>/dev/null | tr -d '[:space:]')"
test "$status" = "updated"

echo "==> Step 3/3: DELETE one row on source (proves envelope carried DELETE op)"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 "$SOURCE_DB" -e \
  "DELETE FROM customers WHERE id=9102;"
wait_producer_written 4
wait_consumer_written 4

i=0
while [ "$i" -lt 60 ]; do
  cnt="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e \
    "SELECT COUNT(*) FROM ${TARGET_DB}.customers WHERE id=9102;" 2>/dev/null | tr -d '[:space:]')"
  [ "$cnt" = "0" ] && break
  i=$((i + 1)); sleep 1
done
test "$cnt" = "0"

echo "==> Final state: source and target agree on the surviving row"
src="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e \
  "SELECT id,amount,status FROM ${SOURCE_DB}.customers WHERE id IN (9101,9102) ORDER BY id;" 2>/dev/null)"
tgt="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e \
  "SELECT id,amount,status FROM ${TARGET_DB}.customers WHERE id IN (9101,9102) ORDER BY id;" 2>/dev/null)"
echo "source: $src"
echo "target: $tgt"
test "$src" = "$tgt"

echo "CDC -> Kafka relay (topic_template + envelope) -> MySQL E2E passed"
