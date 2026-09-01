#!/usr/bin/env bash
# e2e: BUG-6 mysql_snapshot_cdc CDC-phase ColumnTypes -> Kafka -> ClickHouse
# auto-create with DECLARED types.
#
# Closes ROADMAP BUG-6 acceptance 2: CDC-phase INSERT/UPDATE/DELETE records
# carry Metadata.ColumnTypes; the kafka sink serializes column_types in the
# envelope; a clickhouse auto_create sink must build the table from those
# DECLARED types — not from sample-value + name-hint inference.
#
# Topology:
#   - Leg 1 (producer): mysql_snapshot_cdc(link_src) -> kafka(single topic,
#     envelope). Snapshot 2 rows, then CDC-phase UPDATE/INSERT events.
#   - Leg 2 (consumer): kafka(envelope) -> clickhouse(auto_create=true,
#     pk_columns_from_metadata=true, schema_drift=add_columns).
#
# Verifies (declared-type proof, the whole point of the bug):
#   - request_id (varchar as the MySQL source declares it) maps to CH String.
#     Before the fix, CDC-phase events carried NO column_types and the CH
#     auto-create fell back to sample-value + name-hint inference, turning
#     request_id (e.g. "R-0001") into an Int/UInt type when the sample looked
#     numeric or into a wrong fixed-width String.
#   - amount decimal(12,2) -> Decimal(12,2) (source precision passthrough).
#   - CDC-phase events (written AFTER the snapshot handoff) land in the
#     auto-created table using the declared schema.
#
# SKIP (exit 77) when MySQL/ClickHouse/Redpanda containers are unavailable.
set -euo pipefail
cd "$(dirname "$0")/.."

CONTAINER_CLI="${CONTAINER_CLI:-$(command -v docker || command -v podman)}"
[ -n "$CONTAINER_CLI" ] || { echo "no docker/podman"; exit 77; }

ROOT_DIR="$PWD"
IMAGE="openetl-go-etl:dev"
MYSQL="etl-mysql-source"
CH="etl-clickhouse"
RP="etl-redpanda"
CH_DB="dzh3136_go"
TOPIC="bug6-column-types-relay"
SRC_DB="bug6db"
PROD_API_PORT="${BUG6_PROD_PORT:-8033}"
CONS_API_PORT="${BUG6_CONS_PORT:-8034}"
RUN_TAG="$(date +%s)"

for c in "$MYSQL" "$CH" "$RP"; do
  "$CONTAINER_CLI" inspect "$c" >/dev/null 2>&1 || { echo "SKIP: $c not running"; exit 77; }
done

echo "=== BUG-6 ColumnTypes e2e: snapshot_cdc -> kafka envelope -> clickhouse declared types ==="

mysql() { "$CONTAINER_CLI" exec "$MYSQL" mysql -uroot -proot123456 "$@" 2>&1 | grep -v "Using a password" || true; }
chq()   { "$CONTAINER_CLI" exec "$CH" clickhouse-client -h 127.0.0.1 --password dzh123456 -q "$1" 2>/dev/null || true; }
rpk()   { "$CONTAINER_CLI" exec "$RP" rpk "$@"; }
wait_http() { local u="$1" i=0; while [ $i -lt 60 ]; do curl -fsS "$u" >/dev/null 2>&1 && return 0; i=$((i+1)); sleep 2; done; return 1; }
net_of() { "$CONTAINER_CLI" inspect "$1" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' | tr ' ' '\n' | sort -u | grep -v '^$' | paste -sd, -; }

# Source table: request_id varchar (numeric-looking samples -> before the fix
# inferred wrong), amount decimal with explicit precision.
mysql -e "DROP DATABASE IF EXISTS $SRC_DB; CREATE DATABASE $SRC_DB;"
mysql -e "CREATE TABLE $SRC_DB.link_src (id INT PRIMARY KEY, request_id VARCHAR(32), amount DECIMAL(12,2), name VARCHAR(64));"
mysql -e "INSERT INTO $SRC_DB.link_src VALUES (1,'R-0001',11.11,'a'),(2,'R-0002',22.22,'b');"

chq "DROP TABLE IF EXISTS ${CH_DB}.ods_link_src" || true
rpk topic delete "$TOPIC" >/dev/null 2>&1 || true
rpk topic create "$TOPIC" --partitions 1 >/dev/null 2>&1 || true

# Producer pipeline: mysql_snapshot_cdc -> kafka envelope.
rm -rf data-bug6 && mkdir -p data-bug6/prod-pipes data-bug6/cons-pipes data-bug6/prod-data data-bug6/cons-data
cat > data-bug6/prod-pipes/prod.yaml <<YAML
name: bug6-prod-$RUN_TAG
source:
  type: mysql_snapshot_cdc
  config:
    host: $MYSQL
    port: 3306
    user: root
    password: root123456
    database: $SRC_DB
    tables: ["link_src"]
    pk_columns: ["id"]
    server_id: 7006
sink:
  type: kafka
  config:
    brokers: ["$RP:9092"]
    topic: "$TOPIC"
    envelope: true
batch_size: 10
checkpoint_interval_sec: 3
retry:
  max_attempts: 3
  initial_interval_ms: 1000
  max_interval_ms: 5000
YAML
cat > data-bug6/cons-pipes/cons.yaml <<YAML
name: bug6-cons-$RUN_TAG
source:
  type: kafka
  config:
    brokers: ["$RP:9092"]
    topic: "$TOPIC"
    group_id: "bug6-cons-$RUN_TAG"
    format: envelope
sink:
  type: clickhouse
  config:
    host: $CH
    port: 9000
    user: default
    password: dzh123456
    database: $CH_DB
    table: ods_link_src
    pk_columns_from_metadata: true
    auto_create: true
    schema_drift: add_columns
batch_size: 10
checkpoint_interval_sec: 3
retry:
  max_attempts: 3
  initial_interval_ms: 1000
  max_interval_ms: 5000
YAML
chmod -R a+rwX data-bug6

NET="$(net_of "$MYSQL"),$(net_of "$CH"),$(net_of "$RP")"
"$CONTAINER_CLI" rm -f etl-bug6-prod etl-bug6-cons >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d --network "$NET" --name etl-bug6-prod -p $PROD_API_PORT:8001 \
  -v "$ROOT_DIR/data-bug6/prod-pipes:/app/pipes:ro" -v "$ROOT_DIR/data-bug6/prod-data:/app/data" "$IMAGE" >/dev/null
"$CONTAINER_CLI" run -d --network "$NET" --name etl-bug6-cons -p $CONS_API_PORT:8001 \
  -v "$ROOT_DIR/data-bug6/cons-pipes:/app/pipes:ro" -v "$ROOT_DIR/data-bug6/cons-data:/app/data" "$IMAGE" >/dev/null

wait_http "http://127.0.0.1:$PROD_API_PORT/api/v2/health" || { echo "FAIL: producer not up"; exit 1; }
wait_http "http://127.0.0.1:$CONS_API_PORT/api/v2/health" || { echo "FAIL: consumer not up"; exit 1; }

# Wait for snapshot rows (2) to land via the envelope relay.
i=0; ok=""
while [ $i -lt 90 ]; do
  n="$(chq "SELECT count() FROM ${CH_DB}.ods_link_src FINAL" | tr -d '[:space:]' || true)"
  [ "$n" = "2" ] && { ok=1; break; }
  i=$((i+1)); sleep 2
done
[ -n "$ok" ] || { echo "FAIL: snapshot rows not relayed to clickhouse"; "$CONTAINER_CLI" logs etl-bug6-prod 2>&1 | tail -10; exit 1; }
echo "--- snapshot relay OK: 2 rows in ods_link_src"

# CDC-phase events: UPDATE + INSERT. These must carry column_types.
mysql -e "UPDATE $SRC_DB.link_src SET amount=33.33, name='a2' WHERE id=1;"
mysql -e "INSERT INTO $SRC_DB.link_src VALUES (3,'R-0003',44.44,'c');"

i=0; ok=""
while [ $i -lt 90 ]; do
  n="$(chq "SELECT count() FROM ${CH_DB}.ods_link_src FINAL" | tr -d '[:space:]' || true)"
  [ "$n" = "3" ] && { ok=1; break; }
  i=$((i+1)); sleep 2
done
[ -n "$ok" ] || { echo "FAIL: CDC-phase events not relayed (FINAL count != 3)"; exit 1; }
echo "--- CDC-phase relay OK: 3 rows (update + insert landed)"

# THE assertion: the auto-created table uses DECLARED types.
# request_id came from source varchar(32) -> CH String, NOT an Int or
# fixed-width String from sample inference. amount decimal(12,2) -> Decimal(12,2).
reqtype="$(chq "SELECT type FROM system.columns WHERE database='$CH_DB' AND table='ods_link_src' AND name='request_id'" | tr -d '[:space:]' || true)"
amtype="$(chq "SELECT type FROM system.columns WHERE database='$CH_DB' AND table='ods_link_src' AND name='amount'" | tr -d '[:space:]' || true)"
echo "--- declared types: request_id=$reqtype amount=$amtype"
case "$reqtype" in
  String|Nullable\(String\)) ;;
  *) echo "FAIL: request_id type = $reqtype, want String (declared, not sample-inferred)"; exit 1;;
esac
case "$amtype" in
  Decimal\(12,2\)|Nullable\(Decimal\(12,2\)\)) ;;
  *) echo "FAIL: amount type = $amtype, want Decimal(12,2)"; exit 1;;
esac

# The CDC-phase row (id=3, written after the handoff) must be present with its declared value.
i=0; ok=""
while [ $i -lt 30 ]; do
  v="$(chq "SELECT amount FROM ${CH_DB}.ods_link_src WHERE id=3" | tr -d '[:space:]' || true)"
  [ "$v" = "44.44" ] && { ok=1; break; }
  i=$((i+1)); sleep 2
done
[ -n "$ok" ] || { echo "FAIL: id=3 amount not 44.44"; exit 1; }
echo "--- CDC-phase row id=3 amount=44.44 present"

echo "===== PASS: BUG-6 — CDC-phase ColumnTypes relay through kafka; clickhouse auto-create used declared types ====="

"$CONTAINER_CLI" rm -f etl-bug6-prod etl-bug6-cons >/dev/null 2>&1 || true
rm -rf data-bug6
chq "DROP TABLE IF EXISTS ${CH_DB}.ods_link_src" || true