#!/usr/bin/env bash
# e2e: GAP-3 — Kafka(envelope, multi-table) -> PostgreSQL(pk_columns_from_metadata)
#
# Closes ROADMAP GAP-3 residual: kafka multi-table fanout into PostgreSQL with
# per-table PK derived from the JSON-object Metadata.Key (no static pk_columns).
#
# Topology (mirrors e2e-kafka-multitable-clickhouse.sh):
#   - envelope messages for three tables (orders BIGINT pk order_id, users
#     VARCHAR pk user_no, order_items composite pk) are produced into ONE Kafka topic via rpk
#   - one pipeline: kafka source(topic, format=envelope) -> postgres sink(
#     pk_columns_from_metadata=true, auto_create=true)
#   - the postgres sink routes each record by Metadata.Table (ods_orders /
#     ods_users) and derives the per-table conflict target from the key
#
# Coverage:
#   - INSERT lands in the auto-created table
#   - UPDATE on the same key upserts (ON CONFLICT with the DERIVED pk column)
#   - a composite-key UPDATE that changes item_id removes the old key and keeps
#     the new key
#   - DELETE removes the row by the derived pk (deleteValues path)
#   - PostgreSQL outage routes a complete-identity event to DLQ; restart and
#     controlled replay writes it back
#   - checkpoint reset plus Kafka consumer-group reset replays the topic and
#     leaves the business-key state unchanged
#
# SKIP (exit 77) when Redpanda or the postgres image is unavailable.
set -euo pipefail
cd "$(dirname "$0")/.."

CONTAINER_CLI="${CONTAINER_CLI:-$(command -v docker || command -v podman)}"
[ -n "$CONTAINER_CLI" ] || { echo "no docker/podman"; exit 77; }

ROOT_DIR="$PWD"
IMAGE="openetl-go-etl:dev"
PG_IMAGE="docker.io/library/postgres:16-alpine"
RP="etl-redpanda"
PGC="etl-pg-fanout-e2e"
APP="etl-pg-fanout-app"
TOPIC="pg-fanout-relay"
PG_PORT="${PG_FANOUT_PORT:-15437}"
APP_PORT="${PG_FANOUT_APP_PORT:-8036}"
RUN_TAG="run$(date +%s)"
PIPELINE="pg-fanout-${RUN_TAG}"

cleanup() {
  "$CONTAINER_CLI" rm -f "$APP" "$PGC" >/dev/null 2>&1 || true
  rm -rf data-pg-fanout
}
trap cleanup EXIT  # restore: failure paths dump app logs before exiting

podman_redpanda() { "$CONTAINER_CLI" exec "$RP" rpk "$@"; }

echo "=== GAP-3: kafka multi-table fanout -> postgres pk_columns_from_metadata ==="
echo "=== certification: $(date -u +%Y-%m-%dT%H:%M:%SZ) image=$IMAGE pg_image=$PG_IMAGE ==="

"$CONTAINER_CLI" inspect "$RP" >/dev/null 2>&1 || { echo "SKIP: $RP not running"; exit 77; }
"$CONTAINER_CLI" image inspect "$PG_IMAGE" >/dev/null 2>&1 || { echo "SKIP: $PG_IMAGE not available locally"; exit 77; }

# Fresh PG for this run.
"$CONTAINER_CLI" rm -f "$PGC" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d --name "$PGC" -e POSTGRES_DB=analytics -e POSTGRES_USER=etl \
  -e POSTGRES_PASSWORD=etl123 -p ${PG_PORT}:5432 "$PG_IMAGE" >/dev/null
i=0
while [ $i -lt 60 ]; do
  "$CONTAINER_CLI" exec "$PGC" pg_isready -U etl -d analytics >/dev/null 2>&1 && break
  i=$((i+1)); sleep 2
done
"$CONTAINER_CLI" exec "$PGC" pg_isready -U etl -d analytics >/dev/null || { echo "FAIL: pg not ready"; exit 1; }

pg() { "$CONTAINER_CLI" exec "$PGC" psql -U etl -d analytics -tAc "$1" 2>/dev/null; }

podman_redpanda topic delete "$TOPIC" >/dev/null 2>&1 || true
podman_redpanda topic create "$TOPIC" --partitions 1 >/dev/null 2>&1 || true

produce() {
  "$CONTAINER_CLI" exec "$RP" sh -c "printf '%s\n' '$1' | rpk topic produce $TOPIC" >/dev/null
}

# Envelope stream: insert/update/delete across three tables in one topic.
produce "{\"event_id\":\"${RUN_TAG}-e1\",\"op\":\"INSERT\",\"table\":\"ods_orders\",\"primary_key_columns\":[\"order_id\"],\"key\":\"{\\\"order_id\\\":5001}\",\"data\":{\"order_id\":5001,\"amount\":11.00,\"run\":\"$RUN_TAG\"}}"
produce "{\"event_id\":\"${RUN_TAG}-e2\",\"op\":\"INSERT\",\"table\":\"ods_orders\",\"primary_key_columns\":[\"order_id\"],\"key\":\"{\\\"order_id\\\":5002}\",\"data\":{\"order_id\":5002,\"amount\":22.00,\"run\":\"$RUN_TAG\"}}"
produce "{\"event_id\":\"${RUN_TAG}-e3\",\"op\":\"INSERT\",\"table\":\"ods_users\",\"primary_key_columns\":[\"user_no\"],\"key\":\"{\\\"user_no\\\":\\\"U1\\\"}\",\"data\":{\"user_no\":\"U1\",\"name\":\"Alice\",\"run\":\"$RUN_TAG\"}}"
produce "{\"event_id\":\"${RUN_TAG}-e4\",\"op\":\"UPDATE\",\"table\":\"ods_orders\",\"primary_key_columns\":[\"order_id\"],\"key\":\"{\\\"order_id\\\":5001}\",\"before\":{\"order_id\":5001,\"amount\":11.00,\"run\":\"$RUN_TAG\"},\"data\":{\"order_id\":5001,\"amount\":15.00,\"run\":\"$RUN_TAG\"}}"
produce "{\"event_id\":\"${RUN_TAG}-e5\",\"op\":\"DELETE\",\"table\":\"ods_orders\",\"primary_key_columns\":[\"order_id\"],\"key\":\"{\\\"order_id\\\":5002}\",\"data\":{\"order_id\":5002,\"run\":\"$RUN_TAG\"}}"
produce "{\"event_id\":\"${RUN_TAG}-e6\",\"op\":\"INSERT\",\"table\":\"ods_order_items\",\"primary_key_columns\":[\"tenant_id\",\"item_id\"],\"key\":\"{\\\"tenant_id\\\":\\\"acme\\\",\\\"item_id\\\":1}\",\"data\":{\"tenant_id\":\"acme\",\"item_id\":1,\"qty\":2,\"run\":\"$RUN_TAG\"}}"
produce "{\"event_id\":\"${RUN_TAG}-e7\",\"op\":\"UPDATE\",\"table\":\"ods_order_items\",\"primary_key_columns\":[\"tenant_id\",\"item_id\"],\"key\":\"{\\\"tenant_id\\\":\\\"acme\\\",\\\"item_id\\\":1}\",\"before\":{\"tenant_id\":\"acme\",\"item_id\":1,\"qty\":2,\"run\":\"$RUN_TAG\"},\"data\":{\"tenant_id\":\"acme\",\"item_id\":1,\"qty\":3,\"run\":\"$RUN_TAG\"}}"
produce "{\"event_id\":\"${RUN_TAG}-e8\",\"op\":\"UPDATE\",\"table\":\"ods_order_items\",\"primary_key_columns\":[\"tenant_id\",\"item_id\"],\"key\":\"{\\\"tenant_id\\\":\\\"acme\\\",\\\"item_id\\\":1}\",\"before\":{\"tenant_id\":\"acme\",\"item_id\":1,\"qty\":3,\"run\":\"$RUN_TAG\"},\"data\":{\"tenant_id\":\"acme\",\"item_id\":2,\"qty\":4,\"run\":\"$RUN_TAG\"}}"

# Consumer pipeline: kafka envelope -> postgres (per-table PK from metadata).
rm -rf data-pg-fanout && mkdir -p data-pg-fanout/pipes data-pg-fanout/data
cat > data-pg-fanout/pipes/fanout.yaml <<YAML
name: $PIPELINE
source:
  type: kafka
  config:
    brokers: ["$RP:9092"]
    topic: "$TOPIC"
    group_id: "$PIPELINE"
    format: envelope
    initial_offset: oldest
sink:
  type: postgres
  config:
    host: $PGC
    port: 5432
    user: etl
    password: etl123
    database: analytics
    sslmode: disable
    schema: public
    table: ods_orders
    pk_columns_from_metadata: true
    auto_create: true
batch_size: 10
checkpoint_interval_sec: 3
retry:
  max_attempts: 3
  initial_interval_ms: 1000
  max_interval_ms: 5000
YAML
chmod -R a+rwX data-pg-fanout

NET="$("$CONTAINER_CLI" inspect "$PGC" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' | awk '{print $1}'),$("$CONTAINER_CLI" inspect "$RP" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' | awk '{print $1}')"
"$CONTAINER_CLI" rm -f "$APP" >/dev/null 2>&1 || true
# Pin container IPs via --add-host: podman aardvark DNS can forward unknown
# names to the host resolver, where a local VPN fake-ip pool hijacks container
# names (observed: name -> 198.20.0.x instead of the real 10.88.0.x). Pinning
# makes the run independent of host DNS pollution.
PG_IP="$("$CONTAINER_CLI" inspect "$PGC" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' | awk '{print $1}')"
RP_IP="$("$CONTAINER_CLI" inspect "$RP" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}' | awk '{print $1}')"
"$CONTAINER_CLI" run -d --network "$NET" --name "$APP" -p ${APP_PORT}:8001 \
  --add-host "${PGC}:${PG_IP}" --add-host "${RP}:${RP_IP}" \
  -v "$ROOT_DIR/data-pg-fanout/pipes:/app/pipes:ro" -v "$ROOT_DIR/data-pg-fanout/data:/app/data" \
  "$IMAGE" >/dev/null

wait_http() { local u="$1" i=0; while [ $i -lt 60 ]; do curl -fsS "$u" >/dev/null 2>&1 && return 0; i=$((i+1)); sleep 2; done; return 1; }
wait_http "http://127.0.0.1:${APP_PORT}/api/v2/health" || { echo "FAIL: app not up"; "$CONTAINER_CLI" logs "$APP" 2>&1 | tail -8; exit 1; }

wait_pg_count() { # table, expected
  local t="$1" want="$2" i=0 got=""
  while [ $i -lt 60 ]; do
    got="$(pg "SELECT count() FROM $t" 2>/dev/null || pg "SELECT count(*) FROM $t" 2>/dev/null || echo)" 
    [ "$got" = "$want" ] && return 0
    i=$((i+1)); sleep 2
  done
  echo "TIMEOUT: $t count=$got want=$want" >&2
  return 1
}

consumer_group_exists() {
  podman_redpanda group list --brokers localhost:9092 2>/dev/null |
    awk 'NR > 1 {print $NF}' | grep -Fx "$1" >/dev/null 2>&1
}

wait_app_running() {
  local i=0 body=""
  while [ "$i" -lt 90 ]; do
    body="$(curl -fsS "http://127.0.0.1:${APP_PORT}/api/v2/pipelines" 2>/dev/null || true)"
    if printf '%s' "$body" | grep -q "\"name\":\"${PIPELINE}\"" &&
       printf '%s' "$body" | grep "\"name\":\"${PIPELINE}\"" | grep -q '"status":"running"'; then
      return 0
    fi
    i=$((i+1)); sleep 1
  done
  echo "FAIL: pipeline ${PIPELINE} did not become running" >&2
  return 1
}

# ods_orders: 2 inserts, 1 update (same pk), 1 delete -> exactly 1 row (5001, 15.00).
wait_pg_count "ods_orders" "1"
v="$(pg "SELECT amount FROM ods_orders WHERE order_id=5001")"
[ "$v" = "15.00" ] || { echo "FAIL: ods_orders upsert amount=$v want 15.00 (derived PK ON CONFLICT)"; exit 1; }
echo "--- ods_orders OK: upsert by derived PK (order_id) applied, delete applied"

# ods_users: 1 row, pk derived from string key user_no.
wait_pg_count "ods_users" "1"
u="$(pg "SELECT name FROM ods_users WHERE user_no='U1'")"
[ "$u" = "Alice" ] || { echo "FAIL: ods_users row missing"; exit 1; }
echo "--- ods_users OK: varchar PK derived from Metadata.Key"

# Column-level sanity: the derived PK column is actually the primary key.
pkcol="$(pg "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) WHERE i.indrelid='ods_orders'::regclass AND i.indisprimary" | head -1)"
[ "$pkcol" = "order_id" ] || { echo "FAIL: ods_orders PK column = $pkcol, want order_id"; exit 1; }
echo "--- ods_orders PK constraint on order_id (derived, not static config)"

# Composite key table: the old key from the key-changing UPDATE must be gone,
# while the new key remains with the final value.
wait_pg_count "ods_order_items" "1"
item_qty="$(pg "SELECT qty FROM ods_order_items WHERE tenant_id='acme' AND item_id=2")"
[ "$item_qty" = "4" ] || { echo "FAIL: composite key update qty=$item_qty want 4"; exit 1; }
old_item_count="$(pg "SELECT count(*) FROM ods_order_items WHERE tenant_id='acme' AND item_id=1")"
[ "$old_item_count" = "0" ] || { echo "FAIL: old composite key still present ($old_item_count)"; exit 1; }
item_pk="$(pg "SELECT string_agg(a.attname, ',' ORDER BY array_position(i.indkey, a.attnum)) FROM pg_index i JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) WHERE i.indrelid='ods_order_items'::regclass AND i.indisprimary")"
[ "$item_pk" = "tenant_id,item_id" ] || [ "$item_pk" = "item_id,tenant_id" ] || { echo "FAIL: composite PK=$item_pk"; exit 1; }
echo "--- ods_order_items OK: composite key + key-changing UPDATE old-key delete"

echo "==> Verify PostgreSQL outage -> DLQ -> restart -> replay"
"$CONTAINER_CLI" stop "$PGC" >/dev/null
produce "{\"event_id\":\"${RUN_TAG}-outage\",\"op\":\"INSERT\",\"table\":\"ods_users\",\"primary_key_columns\":[\"user_no\"],\"key\":\"{\\\"user_no\\\":\\\"PG_OUTAGE\\\"}\",\"data\":{\"user_no\":\"PG_OUTAGE\",\"name\":\"Outage\",\"run\":\"$RUN_TAG\"}}"
dlq_body=""
i=0
while [ "$i" -lt 90 ]; do
  dlq_body="$(curl -fsS "http://127.0.0.1:${APP_PORT}/api/v2/dlq/${PIPELINE}?contains=PG_OUTAGE&limit=10" 2>/dev/null || true)"
  if printf '%s' "$dlq_body" | grep -q PG_OUTAGE; then break; fi
  i=$((i+1)); sleep 1
done
printf '%s\n' "$dlq_body" | grep PG_OUTAGE >/dev/null || { echo "outage event did not reach DLQ" >&2; exit 1; }
pg_dlq_id="$(printf '%s' "$dlq_body" | grep -o '"id":[0-9][0-9]*' | head -n1 | sed 's/[^0-9]//g')"
[ -n "$pg_dlq_id" ] || { echo "could not identify PostgreSQL outage DLQ id" >&2; exit 1; }
curl -fsS -X POST "http://127.0.0.1:${APP_PORT}/api/v2/pipelines/${PIPELINE}/stop" >/dev/null || true
"$CONTAINER_CLI" stop "$APP" >/dev/null || true
"$CONTAINER_CLI" start "$PGC" >/dev/null
i=0
while [ "$i" -lt 60 ]; do
  "$CONTAINER_CLI" exec "$PGC" pg_isready -U etl -d analytics >/dev/null 2>&1 && break
  i=$((i+1)); sleep 2
done
"$CONTAINER_CLI" exec "$PGC" pg_isready -U etl -d analytics >/dev/null || { echo "FAIL: postgres did not recover"; exit 1; }
# Recreate the app container: its --add-host pins the PG container IP, which
# changes when podman stops/starts the PG container. A plain `start` would
# keep resolving the old IP and replay would fail with "no route to host".
PG_IP="$("$CONTAINER_CLI" inspect "$PGC" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' | awk '{print $1}')"
"$CONTAINER_CLI" rm -f "$APP" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d --network "$NET" --name "$APP" -p ${APP_PORT}:8001 \
  --add-host "${PGC}:${PG_IP}" --add-host "${RP}:${RP_IP}" \
  -v "$ROOT_DIR/data-pg-fanout/pipes:/app/pipes:ro" -v "$ROOT_DIR/data-pg-fanout/data:/app/data" \
  "$IMAGE" >/dev/null
wait_http "http://127.0.0.1:${APP_PORT}/api/v2/health"
replay_body="$(curl -fsS -X POST "http://127.0.0.1:${APP_PORT}/api/v2/dlq/${PIPELINE}/${pg_dlq_id}/replay")"
printf '%s\n' "$replay_body" | grep '"replayed":1' >/dev/null || { echo "PostgreSQL outage replay failed: $replay_body" >&2; exit 1; }
wait_pg_count "ods_users" "2"
u="$(pg "SELECT name FROM ods_users WHERE user_no='PG_OUTAGE'")"
[ "$u" = "Outage" ] || { echo "FAIL: replayed PostgreSQL outage row=$u"; exit 1; }
dlq_after="$(curl -fsS "http://127.0.0.1:${APP_PORT}/api/v2/dlq/${PIPELINE}?contains=PG_OUTAGE&limit=10")"
if printf '%s' "$dlq_after" | grep -q "\"id\":${pg_dlq_id}"; then
  echo "replayed PostgreSQL DLQ id ${pg_dlq_id} was not deleted" >&2
  exit 1
fi

echo "==> Verify checkpoint + consumer-group reset replay"
curl -fsS -X POST "http://127.0.0.1:${APP_PORT}/api/v2/pipelines/${PIPELINE}/stop" >/dev/null || true
curl -fsS -X POST "http://127.0.0.1:${APP_PORT}/api/v2/pipelines/${PIPELINE}/checkpoint/reset" >/dev/null
i=0
while [ "$i" -lt 15 ]; do
  podman_redpanda group delete "$PIPELINE" --brokers localhost:9092 >/dev/null 2>&1 || true
  if ! consumer_group_exists "$PIPELINE"; then break; fi
  i=$((i+1)); sleep 1
done
consumer_group_exists "$PIPELINE" && { echo "Kafka consumer group still exists after reset" >&2; exit 1; } || true
curl -fsS -X POST "http://127.0.0.1:${APP_PORT}/api/v2/pipelines/${PIPELINE}/start" >/dev/null
wait_app_running
wait_pg_count "ods_orders" "1"
wait_pg_count "ods_users" "2"
wait_pg_count "ods_order_items" "1"
item_qty="$(pg "SELECT qty FROM ods_order_items WHERE tenant_id='acme' AND item_id=2")"
[ "$item_qty" = "4" ] || { echo "FAIL: reset replay changed composite row qty=$item_qty"; exit 1; }
old_item_count="$(pg "SELECT count(*) FROM ods_order_items WHERE tenant_id='acme' AND item_id=1")"
[ "$old_item_count" = "0" ] || { echo "FAIL: reset replay resurrected old composite key"; exit 1; }

echo "===== PASS: GAP-7.3 — kafka multi-table fanout -> postgres metadata-PK (composite/key-change/outage/replay) ====="
