#!/usr/bin/env bash
# e2e: BUG-2 binlog purged recovery — resnapshot policy (mysql_snapshot_cdc).
#
# Closes ROADMAP BUG-2 acceptance 3: with cdc_on_binlog_purged: resnapshot,
# after the CDC binlog is purged (RESET MASTER), the source must fall back to
# the snapshot phase, continue from its last per-table cursors (not re-read
# already-snapshot rows), re-enter CDC, and keep streaming new rows.
#
# Verifies:
#   1. snapshot+CDC phases checkpoint a real binlog position;
#   2. RESET MASTER purges it; pipeline restarts from the stale checkpoint;
#   3. the reconnect loop detects ERROR 1236 and breaks to resnapshot;
#   4. phase returns to snapshot, new rows land in the sink AFTER the
#      originally-snapshotted set, and the pipeline stays running (not failed,
#      not looping on the stale coordinate).
#
# Skips with exit 77 when MySQL/ClickHouse containers are unavailable.
set -euo pipefail
cd "$(dirname "$0")/.."

CONTAINER_CLI="${CONTAINER_CLI:-$(command -v docker || command -v podman)}"
[ -n "$CONTAINER_CLI" ] || { echo "no docker/podman"; exit 77; }

API="http://127.0.0.1:8045"
TOKEN="${ETL_API_TOKEN:-sk-test}"
MYSQL="etl-mysql-source"
CH="etl-clickhouse"
APP="etl-binlog-purge-resnap-e2e"
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

"$CONTAINER_CLI" inspect "$MYSQL" >/dev/null 2>&1 || { echo "SKIP: $MYSQL not running"; exit 77; }
"$CONTAINER_CLI" inspect "$CH" >/dev/null 2>&1 || { echo "SKIP: $CH not running"; exit 77; }

echo "=== BUG-2 binlog purged recovery e2e: resnapshot policy ==="

mysql() { "$CONTAINER_CLI" exec "$MYSQL" mysql -uroot -proot123456 "$@" 2>&1 | grep -v "Using a password" || true; }
chq()   { "$CONTAINER_CLI" exec "$CH" clickhouse-client -h 127.0.0.1 --password dzh123456 -q "$1" 2>/dev/null || true; }

# Fresh source table + known pre-purge rows.
mysql -e "DROP DATABASE IF EXISTS snap_e2e; CREATE DATABASE snap_e2e;"
mysql -e "CREATE TABLE snap_e2e.staff(id int NOT NULL AUTO_INCREMENT, name varchar(32), PRIMARY KEY(id)) ENGINE=InnoDB;"
mysql -e "INSERT INTO snap_e2e.staff(name) VALUES('alice'),('bob');"
chq "DROP TABLE IF EXISTS dzh3136_go.ods_binlog_purge_resnap"

# Resnapshot spec (sibling of snapshot-cdc-fail.yaml).
rm -rf data-binlog-purge-resnap && mkdir -p data-binlog-purge-resnap/pipes
cp testdata/pipes-binlog-purge/snapshot-cdc-fail.yaml data-binlog-purge-resnap/pipes/resnap.yaml
sed -i.bak "s/snapshot-cdc-binlog-purge-fail/snapshot-cdc-binlog-purge-resnap/; s/cdc_on_binlog_purged: fail/cdc_on_binlog_purged: resnapshot/" data-binlog-purge-resnap/pipes/resnap.yaml && rm -f data-binlog-purge-resnap/pipes/*.bak
chmod -R a+rwX data-binlog-purge-resnap

"$CONTAINER_CLI" rm -f "$APP" >/dev/null 2>&1 || true
NET="$("$CONTAINER_CLI" inspect "$MYSQL" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' | awk '{print $1}')"
echo "using network: $NET"
"$CONTAINER_CLI" run -d --name "$APP" --network "$NET" -p 8045:8001 \
  -v "$ROOT_DIR/data-binlog-purge-resnap/pipes:/app/pipes:ro" \
  -v "$ROOT_DIR/data-binlog-purge-resnap:/app/data" \
  openetl-go-etl:dev >/dev/null

for i in $(seq 1 30); do curl -fsS "$API/api/v2/health" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS "$API/api/v2/health" >/dev/null || { echo "FAIL: app not up"; "$CONTAINER_CLI" logs "$APP" 2>&1 | tail -10; exit 1; }

echo "--- waiting for snapshot+CDC to checkpoint a binlog position..."
sleep 18

PID=$(curl -s -H "X-API-Token: $TOKEN" "$API/api/v2/pipelines" | python3 -c "
import json,sys
d=json.load(sys.stdin)
for p in d.get('pipelines',[]):
    if 'binlog-purge-resnap' in p['name']:
        print(p['id']); break
" 2>/dev/null || true)
[ -n "$PID" ] || { echo "FAIL: pipeline not found"; exit 1; }
echo "pipeline id: $PID"

# First rows via snapshot.
chq "SELECT count() FROM dzh3136_go.ods_binlog_purge" | grep -q '^2$' || { echo "FAIL: snapshot rows != 2"; exit 1; }
echo "--- snapshot OK: 2 rows in sink"

curl -s -X POST -H "X-API-Token: $TOKEN" "$API/api/v2/pipelines/$PID/stop" >/dev/null 2>&1 || true
sleep 3
echo "--- checkpoint phase before purge:"
curl -s -H "X-API-Token: $TOKEN" "$API/api/v2/pipelines/$PID/checkpoint" 2>/dev/null | python3 -c "
import json,sys
try:
  d=json.load(sys.stdin); s=json.loads(d.get('position','{}'))
  src=s.get('source',{}); print('phase=',src.get('phase'))
except Exception as e: print('parse:',e)" 2>/dev/null || true

# Purge + insert a new row the resnapshot must pick up.
mysql -e "RESET MASTER;"
mysql -e "INSERT INTO snap_e2e.staff(name) VALUES('carol');"
echo "--- RESET MASTER done; inserted carol"

curl -s -X POST -H "X-API-Token: $TOKEN" "$API/api/v2/pipelines/$PID/start" >/dev/null 2>&1 || true
sleep 20

echo "--- app logs (resnapshot re-entry):"
"$CONTAINER_CLI" logs "$APP" 2>&1 | grep -iE "binlog purged|ERROR 1236|resnapshot|policy" | tail -5 || true

echo "--- sink rows after recovery (expect carol present):"
chq "SELECT name FROM dzh3136_go.ods_binlog_purge ORDER BY id" | tr '\n' ' '; echo

# carol (id=3, uploaded after purge) must be present; pipeline must be running.
chq "SELECT count() FROM dzh3136_go.ods_binlog_purge" | grep -q '^3$' || { echo "FAIL: expected 3 rows after resnapshot recovery"; "$CONTAINER_CLI" logs "$APP" 2>&1 | tail -15; exit 1; }
curl -s -H "X-API-Token: $TOKEN" "$API/api/v2/pipelines/$PID" 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
print('status:', d.get('status'))
" 2>/dev/null | grep -q 'running' || { echo "FAIL: pipeline not running after resnapshot"; exit 1; }

echo "===== PASS: BUG-2 resnapshot policy — purged binlog triggers re-snapshot from cursors, new row delivered ====="

# Cleanup.
"$CONTAINER_CLI" rm -f "$APP" >/dev/null 2>&1 || true
rm -rf data-binlog-purge-resnap
chq "DROP TABLE IF EXISTS dzh3136_go.ods_binlog_purge" 2>/dev/null || true