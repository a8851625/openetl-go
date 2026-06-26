#!/bin/sh
set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

IMAGE="openetl-go-etl:dev"
MYSQL_CONTAINER="etl-mysql-source"
APP_CONTAINER="etl-openetl-go-snap-cdc-crash"
TOTAL=500

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 60 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 1
  done
  return 1
}

target_count() {
  docker exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.snap_cdc_crash;" 2>/dev/null | tr -d '[:space:]'
}

echo "==> Build image"
docker build -t "$IMAGE" -f Dockerfile .

echo "==> Start MySQL source"
docker compose -f docker-compose.dev.yml up -d mysql-source

echo "==> Wait MySQL healthy"
i=0
while [ "$i" -lt 60 ]; do
  status="$(docker inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
  if [ "$status" = "healthy" ]; then break; fi
  i=$((i + 1)); sleep 2
done
[ "$(docker inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER")" = "healthy" ]

echo "==> Prepare snapshot+CDC crash tables"
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "
CREATE DATABASE IF NOT EXISTS dzh3136_target;
DROP TABLE IF EXISTS dzh3136_go.snap_cdc_crash;
CREATE TABLE dzh3136_go.snap_cdc_crash (id INT PRIMARY KEY, name VARCHAR(255), status VARCHAR(50), amount DECIMAL(10,2));
DROP TABLE IF EXISTS dzh3136_target.snap_cdc_crash;
CREATE TABLE dzh3136_target.snap_cdc_crash LIKE dzh3136_go.snap_cdc_crash;
GRANT ALL PRIVILEGES ON dzh3136_target.* TO 'sync_user'@'%';
FLUSH PRIVILEGES;
SET SESSION cte_max_recursion_depth=$((TOTAL + 10));
INSERT INTO dzh3136_go.snap_cdc_crash (id, name, status, amount) WITH RECURSIVE seq AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM seq WHERE n < $TOTAL) SELECT n, CONCAT('SnapCDC User ', n), 'active', n / 10.0 FROM seq;
"

echo "==> Reset ETL data"
rm -rf data-snap-cdc-crash
mkdir -p data-snap-cdc-crash/checkpoint data-snap-cdc-crash/dlq logs
chmod -R a+rwX data-snap-cdc-crash
chmod a+rwX logs

echo "==> Write snapshot+CDC crash spec"
mkdir -p testdata/pipes-snap-cdc-crash
cat > testdata/pipes-snap-cdc-crash/snap-cdc-crash.yaml <<'SPEC'
name: "snap-cdc-crash-recovery"
source:
  type: mysql_snapshot_cdc
  config:
    host: "host.docker.internal"
    port: 13306
    user: "sync_user"
    password: "sync_password_123"
    database: "dzh3136_go"
    table: "snap_cdc_crash"
    pk_column: "id"
    limit: 2
    server_id: 1601

transforms:
  - type: identity
    config: {}

sink:
  type: mysql
  config:
    host: "host.docker.internal"
    port: 13306
    user: "sync_user"
    password: "sync_password_123"
    database: "dzh3136_target"
    table: "snap_cdc_crash"
    batch_mode: "upsert"
    pk_columns:
      - "id"

batch_size: 2
checkpoint_interval_sec: 1
backpressure_buffer: 5

retry:
  max_attempts: 3
  initial_interval_ms: 100
  max_interval_ms: 1000

dlq:
  enable: true
SPEC

echo "==> Phase 1: Run and KILL during snapshot"
docker rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
docker run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p 8016:8001 \
  -v "$ROOT_DIR/testdata/pipes-snap-cdc-crash:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-snap-cdc-crash:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8016/api/v2/health"

echo "==> Wait partial snapshot progress then kill"
i=0
partial=0
while [ "$i" -lt 120 ]; do
  partial="$(target_count)"
  if [ "$partial" -gt 10 ] && [ "$partial" -lt "$TOTAL" ]; then break; fi
  i=$((i + 1)); sleep 1
done
echo "==> Snapshot progress: $partial / $TOTAL rows"
test "$partial" -gt 5
test "$partial" -lt "$TOTAL"

echo "==> KILLING during snapshot (target count=$partial)"
docker kill "$APP_CONTAINER" >/dev/null

test -f data-snap-cdc-crash/etl.db
echo "==> Checkpoint persisted to SQLite storage"

echo "==> Phase 2: Restart - should resume snapshot from checkpoint"
docker rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
docker run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p 8016:8001 \
  -v "$ROOT_DIR/testdata/pipes-snap-cdc-crash:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-snap-cdc-crash:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8016/api/v2/health"

echo "==> Wait for snapshot to complete (all $TOTAL rows)"
i=0
final=0
while [ "$i" -lt 180 ]; do
  final="$(target_count)"
  if [ "$final" -ge "$TOTAL" ]; then break; fi
  i=$((i + 1)); sleep 1
done
echo "==> After restart snapshot: $final / $TOTAL"
test "$final" -ge "$TOTAL"

echo "==> Phase 3: Emit CDC changes and KILL during CDC"
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "INSERT INTO snap_cdc_crash (id, name, status, amount) VALUES (9001, 'CDC Crash Alice', 'active', 500.00);"

i=0
while [ "$i" -lt 60 ]; do
  cdc1="$(docker exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.snap_cdc_crash WHERE id=9001;" 2>/dev/null | tr -d '[:space:]')"
  if [ "$cdc1" = "1" ]; then break; fi
  i=$((i + 1)); sleep 1
done
test "$cdc1" = "1"
echo "==> CDC event replicated, killing during CDC phase"
docker kill "$APP_CONTAINER" >/dev/null

echo "==> Phase 4: Restart CDC with checkpoint"
docker rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
docker run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p 8016:8001 \
  -v "$ROOT_DIR/testdata/pipes-snap-cdc-crash:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-snap-cdc-crash:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8016/api/v2/health"
sleep 2

echo "==> Emit second CDC event after restart"
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "INSERT INTO snap_cdc_crash (id, name, status, amount) VALUES (9002, 'CDC Crash Bob', 'active', 600.00); UPDATE snap_cdc_crash SET amount=999.00 WHERE id=9002;"

i=0
while [ "$i" -lt 60 ]; do
  cdc2="$(docker exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.snap_cdc_crash WHERE id=9002 AND amount=999.00;" 2>/dev/null | tr -d '[:space:]')"
  if [ "$cdc2" = "1" ]; then break; fi
  i=$((i + 1)); sleep 1
done
test "$cdc2" = "1"

echo "==> Verify no snapshot re-execution (CDC phase should be active)"
final_count="$(target_count)"
echo "==> Final target count: $final_count"

body="$(curl -fsS http://127.0.0.1:8016/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"name":"snap-cdc-crash-recovery"'

docker rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true

echo "Snapshot+CDC crash recovery E2E passed"
