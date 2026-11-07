#!/bin/sh
# Doris source_order (IT-7 T7.3/T7.4) end-to-end proof.
#
# Proves that the Doris sink actually injects the connector-owned source position
# into the sequence column of a real Doris 2.1.11 UNIQUE KEY table, that the
# value advances with each source event, and that a stale (lower) sequence
# arriving later does not regress the row.
set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"
. "$ROOT_DIR/hack/container-cli.sh"

IMAGE="${IMAGE:-openetl-go-etl:dev}"
DORIS_VERSION="${DORIS_VERSION:-2.1.11}"
FE_IMAGE="${DORIS_FE_IMAGE:-docker.io/apache/doris:fe-$DORIS_VERSION}"
BE_IMAGE="${DORIS_BE_IMAGE:-docker.io/apache/doris:be-$DORIS_VERSION}"
NETWORK="etl-doris-so-net"
SUBNET="172.31.91.0/24"
FE="etl-doris-so-fe"
BE="etl-doris-so-be"
MYSQL="etl-mysql-so"
APP="etl-openetl-go-doris-so"
API_PORT="${SO_API_PORT:-8027}"
MYSQL_PORT="${SO_MYSQL_PORT:-13319}"
DB="so_test"
DATA_DIR="$ROOT_DIR/data-doris-so"
PIPE_DIR="$DATA_DIR/pipes"

pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1" >&2; exit 1; }

doris_sql() { "$CONTAINER_CLI" exec "$FE" mysql -h127.0.0.1 -P9030 -uroot "$@"; }
mysql_sql() { "$CONTAINER_CLI" exec "$MYSQL" mysql -uroot -proot123456 "$DB" -e "$1"; }

wait_doris_sql() {
  i=0
  while [ "$i" -lt 120 ]; do
    doris_sql -e "SELECT 1" >/dev/null 2>&1 && return 0
    i=$((i + 1)); sleep 2
  done
  return 1
}
wait_backend() {
  i=0
  while [ "$i" -lt 120 ]; do
    alive="$(doris_sql -N -e "SHOW BACKENDS;" 2>/dev/null | grep -c 'true' || true)"
    [ "${alive:-0}" -ge 1 ] && return 0
    i=$((i + 1)); sleep 2
  done
  return 1
}
wait_http() {
  i=0
  while [ "$i" -lt 120 ]; do
    curl -fsS "$1" >/dev/null 2>&1 && return 0
    i=$((i + 1)); sleep 2
  done
  return 1
}
doris_value() { doris_sql -N "$DB" -e "$1" 2>/dev/null | tr -d '[:space:]'; }

echo "==> Start MySQL source"
"$CONTAINER_CLI" rm -f "$MYSQL" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d --name "$MYSQL" \
  --add-host host.docker.internal:host-gateway \
  -e MYSQL_ROOT_PASSWORD=root123456 \
  -e MYSQL_DATABASE="$DB" \
  -e MYSQL_USER=sync_user \
  -e MYSQL_PASSWORD=sync_password_123 \
  -p "$MYSQL_PORT:3306" \
  docker.io/library/mysql:8.0 \
  --server-id=99 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL \
  --default-authentication-plugin=mysql_native_password >/dev/null
i=0
while [ "$i" -lt 90 ]; do
  "$CONTAINER_CLI" exec "$MYSQL" mysql -uroot -proot123456 -e "SELECT 1" >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 2
done
"$CONTAINER_CLI" exec "$MYSQL" mysql -uroot -proot123456 -e "GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'sync_user'@'%'; FLUSH PRIVILEGES;"
mysql_sql "CREATE TABLE so_orders (id BIGINT PRIMARY KEY, name VARCHAR(64), amount INT) ENGINE=InnoDB;"
pass "source table created (rows are written after the CDC stream starts)"

echo "==> Start Doris FE/BE"
"$CONTAINER_CLI" rm -f "$APP" "$FE" "$BE" >/dev/null 2>&1 || true
"$CONTAINER_CLI" network rm "$NETWORK" >/dev/null 2>&1 || true
"$CONTAINER_CLI" network create --subnet="$SUBNET" "$NETWORK" >/dev/null
"$CONTAINER_CLI" run -d --name "$FE" --network "$NETWORK" --ip 172.31.91.2 \
  -e FE_SERVERS="fe1:172.31.91.2:9010" -e FE_ID=1 -p 8031:8030 -p 9031:9030 "$FE_IMAGE" >/dev/null
"$CONTAINER_CLI" run -d --name "$BE" --network "$NETWORK" --ip 172.31.91.3 \
  -e FE_SERVERS="fe1:172.31.91.2:9010" -e BE_ADDR="172.31.91.3:9050" "$BE_IMAGE" >/dev/null
wait_doris_sql || fail "Doris FE did not become ready"
wait_backend || fail "Doris BE did not become alive"
doris_sql -e "CREATE DATABASE IF NOT EXISTS $DB;"
# The source_order table contract: UNIQUE KEY + merge-on-write + LARGEINT
# sequence column declared through PROPERTIES.
doris_sql "$DB" -e "CREATE TABLE so_orders (
  id BIGINT NOT NULL,
  name VARCHAR(64) NULL,
  amount INT NULL,
  _version LARGEINT NOT NULL
) ENGINE=OLAP UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES('replication_allocation'='tag.location.default: 1',
           'enable_unique_key_merge_on_write'='true',
           'function_column.sequence_col'='_version');"
pass "Doris target table created with the sequence contract"

echo "==> Run the mysql_cdc -> doris source_order pipeline"
rm -rf "$DATA_DIR"; mkdir -p "$PIPE_DIR" "$DATA_DIR"/{output,checkpoint,dlq} logs
chmod -R a+rwX "$DATA_DIR"; chmod a+rwX logs 2>/dev/null || true
cat > "$PIPE_DIR/so.yaml" <<YAML
name: "so-cdc"
source:
  type: mysql_cdc
  config:
    host: host.docker.internal
    port: $MYSQL_PORT
    user: sync_user
    password: sync_password_123
    database: $DB
    server_id: 9911
    tables: ["so_orders"]
sink:
  type: doris
  config:
    host: 172.31.91.2
    port: 9030
    http_port: 8030
    user: root
    password: ""
    database: $DB
    table: so_orders
    write_mode: stream_load
    batch_mode: upsert
    pk_columns: ["id"]
    version_mode: source_order
    sequence_type: largeint
batch_size: 10
checkpoint_interval_sec: 1
backpressure_buffer: 32
YAML

"$CONTAINER_CLI" run -d --add-host host.docker.internal:host-gateway --name "$APP" \
  --network "$NETWORK" --ip 172.31.91.4 -p "$API_PORT:8001" \
  -v "$PIPE_DIR:/app/pipes:ro" -v "$DATA_DIR:/app/data" -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE" >/dev/null
wait_http "http://127.0.0.1:$API_PORT/api/v2/health" || fail "app did not become ready"

# mysql_cdc starts at the current binlog position, so the first row must be
# produced while the pipeline is live.
mysql_sql "INSERT INTO so_orders VALUES (1,'v1',10);"

echo
echo "==> 1. Sequence column carries a real source position"
i=0; version=""
while [ "$i" -lt 90 ]; do
  version="$(doris_value "SELECT _version FROM so_orders WHERE id=1;")"
  [ -n "$version" ] && [ "$version" != "NULL" ] && break
  i=$((i + 1)); sleep 2
done
[ -n "$version" ] || fail "row never reached Doris"
if [ "$version" = "0" ]; then
  fail "sequence column stayed 0: the sink did not inject the source position"
fi
pass "row landed with _version=$version (injected, not the column default)"

echo
echo "==> 2. A newer source event advances the sequence"
first_version="$version"
mysql_sql "UPDATE so_orders SET name='v2', amount=11 WHERE id=1;"
i=0; name=""; new_version=""
while [ "$i" -lt 90 ]; do
  name="$(doris_value "SELECT name FROM so_orders WHERE id=1;")"
  new_version="$(doris_value "SELECT _version FROM so_orders WHERE id=1;")"
  [ "$name" = "v2" ] && break
  i=$((i + 1)); sleep 2
done
[ "$name" = "v2" ] || fail "update never reached Doris (name=$name)"
python3 - "$first_version" "$new_version" <<'PY' || fail "sequence did not advance"
import sys
first, new = int(sys.argv[1]), int(sys.argv[2])
if new <= first:
    print(f"sequence did not advance: {first} -> {new}")
    sys.exit(1)
print(f"sequence advanced {first} -> {new}")
PY
pass "sequence advanced with the source event"

echo
echo "==> 3. A stale (lower) sequence cannot regress the row"
# Write a row carrying an OLD sequence directly through the Doris Stream Load
# path the sink uses (the same code path a late duplicate replay takes).
"$CONTAINER_CLI" exec "$FE" curl -sS -X PUT \
  "http://172.31.91.3:8040/api/$DB/so_orders/_stream_load" \
  -H 'format: json' -H 'read_json_by_line: true' -H "label: e2e_so_stale_$$" \
  -u root: -T /dev/stdin <<'JSON' >/tmp/so_stale_load.json
{"id":1,"name":"stale","amount":1,"_version":1}
JSON
grep -q '"Status": *"Success"' /tmp/so_stale_load.json || { cat /tmp/so_stale_load.json; fail "stale Stream Load did not succeed"; }
sleep 3
stale_name="$(doris_value "SELECT name FROM so_orders WHERE id=1;")"
[ "$stale_name" = "v2" ] || fail "a stale sequence overwrote the row (name=$stale_name)"
pass "stale write rejected by the sequence contract (row kept v2)"

echo
echo "==> All Doris source_order e2e assertions passed"
if [ "${E2E_KEEP:-0}" = "1" ]; then
  echo "    keeping containers for inspection (E2E_KEEP=1)"
else
  "$CONTAINER_CLI" rm -f "$APP" >/dev/null 2>&1 || true
fi
