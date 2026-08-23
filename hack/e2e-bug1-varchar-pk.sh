#!/usr/bin/env bash
# BUG-1 container e2e: mysql_batch with a varchar/string pk_column must
# advance its cursor and complete instead of re-reading the table forever.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
CONTAINER_CLI="${CONTAINER_CLI:-$(command -v podman || command -v docker)}"
IMAGE="openetl-go-etl:dev"
APP=etl-bug1-varchar
API_PORT=8033
MYSQL=etl-mysql-source
RUN_TAG="$(date +%s)"

cleanup() { "$CONTAINER_CLI" rm -f "$APP" >/dev/null 2>&1 || true; }
trap cleanup EXIT

mysql_exec() { "$CONTAINER_CLI" exec -i "$MYSQL" mysql -uroot -proot123456 -e "$1"; }

wait_http() { for i in $(seq 1 60); do curl -fsS "$1" >/dev/null 2>&1 && return 0; sleep 1; done; return 1; }

# Prepare table with 6 varchar-PK rows
mysql_exec "DROP DATABASE IF EXISTS bug1; CREATE DATABASE bug1;"
mysql_exec "CREATE TABLE bug1.varchar_pk_t (code VARCHAR(32) PRIMARY KEY, name VARCHAR(64));"
mysql_exec "INSERT INTO bug1.varchar_pk_t VALUES ('b-1','n1'),('b-2','n2'),('b-3','n3'),('b-4','n4'),('b-5','n5'),('b-6','n6');"
"$CONTAINER_CLI" exec "$MYSQL" mysql -uroot -proot123456 -e "GRANT ALL ON bug1.* TO 'sync_user'@'%'; FLUSH PRIVILEGES;"

rm -rf data-bug1; mkdir -p data-bug1/pipes data-bug1/output data-bug1/checkpoint data-bug1/dlq
cp testdata/pipes-bug1-varchar-pk/*.yaml data-bug1/pipes/
sed -i.bak "s/bug1-varchar-pk-batch/bug1-varchar-pk-batch-$RUN_TAG/" data-bug1/pipes/*.yaml && rm -f data-bug1/pipes/*.bak
sed -i.bak "s/database: etl/database: bug1/" data-bug1/pipes/*.yaml && rm -f data-bug1/pipes/*.bak
chmod -R a+rwX data-bug1

NET="$("$CONTAINER_CLI" inspect "$MYSQL" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')"
"$CONTAINER_CLI" rm -f "$APP" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d --network "$NET" --name "$APP" -p ${API_PORT}:8001 \
  -v "$ROOT_DIR/data-bug1:/app/data" -v "$ROOT_DIR/data-bug1/pipes:/app/pipes:ro" "$IMAGE" >/dev/null
wait_http "http://127.0.0.1:${API_PORT}/api/v2/health" || { echo "FAIL: app not up"; exit 1; }

# The once-schedule pipeline must reach completed (not loop forever) and
# write exactly 6 rows — the pre-fix behavior re-read all rows every batch
# and never finished.
for i in $(seq 1 40); do
  st="$(curl -fsS "http://127.0.0.1:${API_PORT}/api/v2/pipelines" | python3 -c "import sys,json;d=json.load(sys.stdin);p=[x for x in d['pipelines'] if x['name'].startswith('bug1-varchar-pk-batch')];print(p[0]['status'],p[0]['stats']['records_written']) if p else print('missing',0)")"
  echo "  state: $st"
  set -- $st
  [ "$1" = "completed" ] && [ "$2" = "6" ] && break
  sleep 2
done
set -- $st
[ "$1" = "completed" ] || { echo "FAIL: status=$1 (expected completed)"; exit 1; }
[ "$2" = "6" ] || { echo "FAIL: written=$2 (expected 6)"; exit 1; }

echo "===== PASS: mysql_batch varchar pk_column cursor advances; once-pipeline completes with 6 rows ====="
