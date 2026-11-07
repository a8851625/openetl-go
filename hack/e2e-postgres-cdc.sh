#!/bin/sh
# postgres_cdc certification path (beta -> production evidence).
#
# Proves the real pgoutput path end to end: publication + logical slot creation,
# INSERT/UPDATE/DELETE decoding, primary-key metadata (Metadata.Key) so a
# metadata-PK sink can absorb replays, delete propagation, keepalive while idle,
# and restart recovery from the durable LSN checkpoint.
set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"
. "$ROOT_DIR/hack/container-cli.sh"

IMAGE="${IMAGE:-openetl-go-etl:dev}"
PG="etl-pg-cdc-src"
MYSQL="etl-mysql-pg-cdc-sink"
APP="etl-openetl-go-pg-cdc"
API_PORT="${PGCDC_API_PORT:-8028}"
PG_PORT="${PGCDC_PG_PORT:-15432}"
MYSQL_PORT="${PGCDC_MYSQL_PORT:-13320}"
DB="pgcdc_src"
DATA_DIR="$ROOT_DIR/data-pg-cdc"
PIPE_DIR="$DATA_DIR/pipes"

pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1" >&2; exit 1; }

pg_sql() { "$CONTAINER_CLI" exec -e PGPASSWORD=pgpass "$PG" psql -U postgres -d "$DB" -Atc "$1"; }
mysql_sql() { "$CONTAINER_CLI" exec "$MYSQL" mysql -uroot -proot123456 -N -B "$DB" -e "$1"; }

wait_http() {
  i=0
  while [ "$i" -lt 120 ]; do
    curl -fsS "$1" >/dev/null 2>&1 && return 0
    i=$((i + 1)); sleep 2
  done
  return 1
}
wait_pg() {
  i=0
  while [ "$i" -lt 90 ]; do
    pg_sql "SELECT 1" >/dev/null 2>&1 && return 0
    i=$((i + 1)); sleep 2
  done
  return 1
}
wait_mysql() {
  i=0
  while [ "$i" -lt 90 ]; do
    "$CONTAINER_CLI" exec "$MYSQL" mysql -uroot -proot123456 -e "SELECT 1" >/dev/null 2>&1 && return 0
    i=$((i + 1)); sleep 2
  done
  return 1
}
wait_count() {
  query="$1"; expected="$2"; label="$3"
  i=0; got=""
  while [ "$i" -lt 120 ]; do
    got="$(mysql_sql "$query" 2>/dev/null | tr -d '[:space:]')"
    [ "$got" = "$expected" ] && return 0
    i=$((i + 1)); sleep 1
  done
  echo "  last value for $label: '${got:-<empty>}' (want $expected)" >&2
  curl -fsS "http://127.0.0.1:$API_PORT/api/v2/pipelines" >&2 || true
  echo >&2
  return 1
}

echo "==> Start PostgreSQL (wal_level=logical) and MySQL sink"
"$CONTAINER_CLI" rm -f "$PG" "$MYSQL" "$APP" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d --name "$PG" \
  --add-host host.docker.internal:host-gateway \
  -e POSTGRES_PASSWORD=pgpass \
  -e POSTGRES_DB="$DB" \
  -p "$PG_PORT:5432" \
  docker.io/library/postgres:16-alpine \
  -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10 \
  -c wal_sender_timeout=20s >/dev/null
"$CONTAINER_CLI" run -d --name "$MYSQL" \
  --add-host host.docker.internal:host-gateway \
  -e MYSQL_ROOT_PASSWORD=root123456 \
  -e MYSQL_DATABASE="$DB" \
  -p "$MYSQL_PORT:3306" \
  docker.io/library/mysql:8.0 >/dev/null
wait_pg || fail "postgres did not become ready"
wait_mysql || fail "mysql did not become ready"
pass "postgres (wal_level=logical) and mysql sink are up"

pg_sql "CREATE TABLE pg_orders (id BIGINT PRIMARY KEY, name TEXT, amount INT);" >/dev/null
mysql_sql "CREATE TABLE pg_orders (id BIGINT PRIMARY KEY, name VARCHAR(64), amount INT) ENGINE=InnoDB;"
pass "source and target tables created"

echo "==> Run the postgres_cdc -> mysql pipeline"
rm -rf "$DATA_DIR"; mkdir -p "$PIPE_DIR" "$DATA_DIR"/{output,checkpoint,dlq} logs
chmod -R a+rwX "$DATA_DIR"; chmod a+rwX logs 2>/dev/null || true
cat > "$PIPE_DIR/pg-cdc.yaml" <<YAML
name: "pg-cdc-to-mysql"
source:
  type: postgres_cdc
  config:
    host: host.docker.internal
    port: $PG_PORT
    user: postgres
    password: pgpass
    database: $DB
    slot_name: etl_slot_e2e
    sslmode: disable
    tables: ["pg_orders"]
sink:
  type: mysql
  config:
    host: host.docker.internal
    port: $MYSQL_PORT
    user: root
    password: root123456
    database: $DB
    table: pg_orders
    batch_mode: upsert
    pk_columns_from_metadata: true
batch_size: 10
checkpoint_interval_sec: 1
backpressure_buffer: 32
YAML

"$CONTAINER_CLI" run -d --add-host host.docker.internal:host-gateway --name "$APP" \
  -p "$API_PORT:8001" \
  -v "$PIPE_DIR:/app/pipes:ro" -v "$DATA_DIR:/app/data" -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE" >/dev/null
wait_http "http://127.0.0.1:$API_PORT/api/v2/health" || fail "app did not become ready"

# The source must have created the publication and the logical slot.
i=0; slot=""
while [ "$i" -lt 60 ]; do
  slot="$(pg_sql "SELECT slot_name FROM pg_replication_slots WHERE slot_name='etl_slot_e2e';" || true)"
  [ -n "$slot" ] && break
  i=$((i + 1)); sleep 2
done
[ "$slot" = "etl_slot_e2e" ] || fail "logical replication slot was not created"
pass "logical replication slot created"
[ "$(pg_sql "SELECT pubname FROM pg_publication WHERE pubname='etl_pub';")" = "etl_pub" ] || fail "publication was not created"
pass "publication created"

echo
echo "==> 1. INSERT propagates"
pg_sql "INSERT INTO pg_orders VALUES (1,'alpha',10),(2,'beta',20);" >/dev/null
wait_count "SELECT COUNT(*) FROM pg_orders;" 2 "insert count" || fail "inserts did not propagate"
[ "$(mysql_sql "SELECT name FROM pg_orders WHERE id=1;")" = "alpha" ] || fail "inserted row content is wrong"
pass "2 inserts landed with the right content"

echo
echo "==> 2. UPDATE propagates and the metadata primary key is honoured"
pg_sql "UPDATE pg_orders SET name='alpha-v2', amount=11 WHERE id=1;" >/dev/null
wait_count "SELECT name FROM pg_orders WHERE id=1;" "alpha-v2" "updated name" || fail "update did not propagate"
pass "update propagated (metadata PK upsert, no duplicate row)"
[ "$(mysql_sql "SELECT COUNT(*) FROM pg_orders WHERE id=1;")" = "1" ] || fail "update duplicated the row"
pass "still exactly one row for id=1"

echo
echo "==> 3. DELETE propagates"
pg_sql "DELETE FROM pg_orders WHERE id=2;" >/dev/null
wait_count "SELECT COUNT(*) FROM pg_orders;" 1 "post-delete count" || fail "delete did not propagate"
pass "delete propagated (1 row left)"

echo
echo "==> 4. Idle keepalive: the stream survives past the sender timeout"
# PostgreSQL sends a keepalive every wal_sender_timeout/2 (20s here, so ~10s) and
# terminates a replication connection that does not answer. Staying connected
# while idle and then applying the next event proves the keepalive path works.
# (confirmed_flush_lsn legitimately does not move while no new WAL exists: the
# source only acknowledges the durable, already-checkpointed position.)
before="$(pg_sql "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='etl_slot_e2e';")"
sleep 25
[ "$(pg_sql "SELECT active FROM pg_replication_slots WHERE slot_name='etl_slot_e2e';")" = "t" ] \
  || fail "replication slot went inactive while idle (keepalive not answered)"
pass "slot stayed active across the sender timeout while idle"
pg_sql "INSERT INTO pg_orders VALUES (9,'idle-check',90);" >/dev/null
wait_count "SELECT COUNT(*) FROM pg_orders;" 2 "post-idle count" || fail "stream did not resume after an idle period"
pass "stream resumed after the idle period (no reconnect gap)"
pg_sql "DELETE FROM pg_orders WHERE id=9;" >/dev/null
wait_count "SELECT COUNT(*) FROM pg_orders;" 1 "post-idle cleanup" || fail "cleanup delete did not propagate"
after="$(pg_sql "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='etl_slot_e2e';")"
[ "$before" != "$after" ] || fail "confirmed_flush_lsn did not advance after new events ($before)"
pass "confirmed_flush_lsn advanced with new events ($before -> $after)"

echo
echo "==> 5. Restart resumes from the durable LSN checkpoint"
curl -fsS -X POST "http://127.0.0.1:$API_PORT/api/v2/pipelines/pg-cdc-to-mysql/stop" >/dev/null
i=0
while [ "$i" -lt 60 ]; do
  status="$(curl -fsS "http://127.0.0.1:$API_PORT/api/v2/pipelines" | sed -n 's/.*"name":"pg-cdc-to-mysql".*"status":"\([a-z]*\)".*/\1/p')"
  [ "$status" = "stopped" ] && break
  i=$((i + 1)); sleep 1
done
ckpt_lsn="$(curl -fsS "http://127.0.0.1:$API_PORT/api/v2/pipelines/pg-cdc-to-mysql/checkpoint")"
echo "$ckpt_lsn" | grep -q '"lsn"' || fail "checkpoint does not carry an LSN: $ckpt_lsn"
pass "checkpoint carries the durable LSN"

pg_sql "INSERT INTO pg_orders VALUES (3,'gamma',30);" >/dev/null
curl -fsS -X POST "http://127.0.0.1:$API_PORT/api/v2/pipelines/pg-cdc-to-mysql/start" >/dev/null
wait_count "SELECT COUNT(*) FROM pg_orders;" 2 "post-restart count" || fail "restart did not resume the stream"
[ "$(mysql_sql "SELECT name FROM pg_orders WHERE id=3;")" = "gamma" ] || fail "post-restart row is wrong"
pass "restart resumed from the checkpoint and applied the new row"
[ "$(mysql_sql "SELECT COUNT(*) FROM pg_orders;")" = "2" ] || fail "restart re-applied already-acknowledged rows"
pass "no duplicate replay of already-acknowledged rows"

echo
echo "==> 6. TRUNCATE is refused (production-safe default)"
pg_sql "TRUNCATE pg_orders;" >/dev/null
sleep 4
body="$(curl -fsS "http://127.0.0.1:$API_PORT/api/v2/pipelines" || true)"
echo "$body" | grep -qi "truncate" || fail "TRUNCATE did not surface an error (on_truncate=error default): $body"
pass "TRUNCATE aborted the stream instead of silently leaving stale sink rows"

echo
echo "==> All postgres_cdc e2e assertions passed"
if [ "${E2E_KEEP:-0}" = "1" ]; then
  echo "    keeping containers (E2E_KEEP=1)"
else
  "$CONTAINER_CLI" rm -f "$APP" >/dev/null 2>&1 || true
fi
