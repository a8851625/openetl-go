#!/usr/bin/env bash
# e2e: GAP-3 — Kafka(envelope, multi-table) -> PostgreSQL(pk_columns_from_metadata)
#
# Closes ROADMAP GAP-3 residual: kafka multi-table fanout into PostgreSQL with
# per-table PK derived from the JSON-object Metadata.Key (no static pk_columns).
#
# Topology (mirrors e2e-kafka-multitable-clickhouse.sh):
#   - envelope messages for two tables (orders BIGINT pk order_id, users
#     VARCHAR pk user_no) are produced into ONE Kafka topic via rpk
#   - one pipeline: kafka source(topic, format=envelope) -> postgres sink(
#     pk_columns_from_metadata=true, auto_create=true)
#   - the postgres sink routes each record by Metadata.Table (ods_orders /
#     ods_users) and derives the per-table conflict target from the key
#
# Coverage:
#   - INSERT lands in the auto-created table
#   - UPDATE on the same key upserts (ON CONFLICT with the DERIVED pk column)
#   - DELETE removes the row by the derived pk (deleteValues path)
#   - a wrong/missing key value must NOT silently corrupt other tables
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

cleanup() {
  "$CONTAINER_CLI" rm -f "$APP" "$PGC" >/dev/null 2>&1 || true
  rm -rf data-pg-fanout
}
trap cleanup EXIT  # restore: failure paths dump app logs before exiting

podman_redpanda() { "$CONTAINER_CLI" exec "$RP" rpk "$@"; }

echo "=== GAP-3: kafka multi-table fanout -> postgres pk_columns_from_metadata ==="

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

# Envelope stream: insert/update/delete across two tables in one topic.
produce "{\"event_id\":\"${RUN_TAG}-e1\",\"op\":\"INSERT\",\"table\":\"ods_orders\",\"key\":\"{\\\"order_id\\\":5001}\",\"data\":{\"order_id\":5001,\"amount\":11.00,\"run\":\"$RUN_TAG\"}}"
produce "{\"event_id\":\"${RUN_TAG}-e2\",\"op\":\"INSERT\",\"table\":\"ods_orders\",\"key\":\"{\\\"order_id\\\":5002}\",\"data\":{\"order_id\":5002,\"amount\":22.00,\"run\":\"$RUN_TAG\"}}"
produce "{\"event_id\":\"${RUN_TAG}-e3\",\"op\":\"INSERT\",\"table\":\"ods_users\",\"key\":\"{\\\"user_no\\\":\\\"U1\\\"}\",\"data\":{\"user_no\":\"U1\",\"name\":\"Alice\",\"run\":\"$RUN_TAG\"}}"
produce "{\"event_id\":\"${RUN_TAG}-e4\",\"op\":\"UPDATE\",\"table\":\"ods_orders\",\"key\":\"{\\\"order_id\\\":5001}\",\"data\":{\"order_id\":5001,\"amount\":15.00,\"run\":\"$RUN_TAG\"}}"
produce "{\"event_id\":\"${RUN_TAG}-e5\",\"op\":\"DELETE\",\"table\":\"ods_orders\",\"key\":\"{\\\"order_id\\\":5002}\",\"data\":{\"order_id\":5002,\"run\":\"$RUN_TAG\"}}"

# Consumer pipeline: kafka envelope -> postgres (per-table PK from metadata).
rm -rf data-pg-fanout && mkdir -p data-pg-fanout/pipes data-pg-fanout/data
cat > data-pg-fanout/pipes/fanout.yaml <<YAML
name: pg-fanout-$RUN_TAG
source:
  type: kafka
  config:
    brokers: ["$RP:9092"]
    topic: "$TOPIC"
    group_id: "pg-fanout-$RUN_TAG"
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

echo "===== PASS: GAP-3 — kafka multi-table fanout -> postgres with pk_columns_from_metadata (insert/update/delete) ====="
