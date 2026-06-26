#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

IMAGE="openetl-go-etl:dev"
MYSQL_CONTAINER="etl-mysql-source"
CH_CONTAINER="etl-clickhouse"
APP_CONTAINER="etl-openetl-go-snapshot-cdc-clickhouse"
APP_PORT="8024"
PIPELINE="mysql-snapshot-cdc-to-clickhouse"
TABLE="snap_cdc_clickhouse"

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 90 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

wait_mysql_healthy() {
  i=0
  while [ "$i" -lt 90 ]; do
    status="$(docker inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
    if [ "$status" = "healthy" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 2
  done
  return 1
}

wait_pipeline_running() {
  i=0
  while [ "$i" -lt 60 ]; do
    body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines" 2>/dev/null || true)"
    echo "$body" | grep "\"name\":\"$PIPELINE\"" | grep '"status":"running"' >/dev/null 2>&1 && return 0
    i=$((i + 1))
    sleep 1
  done
  echo "$body"
  return 1
}

wait_checkpoint_cdc() {
  i=0
  while [ "$i" -lt 60 ]; do
    body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/checkpoint" 2>/dev/null || true)"
    echo "$body" | grep '"phase":"cdc"' >/dev/null 2>&1 && return 0
    i=$((i + 1))
    sleep 1
  done
  echo "$body"
  return 1
}

run_app() {
  docker rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
  docker run -d \
    --add-host host.docker.internal:host-gateway \
    --name "$APP_CONTAINER" \
    -p "$APP_PORT:8001" \
    -v "$ROOT_DIR/testdata/pipes-snapshot-cdc-clickhouse:/app/pipes:ro" \
    -v "$ROOT_DIR/testdata:/app/testdata:ro" \
    -v "$ROOT_DIR/data-snapshot-cdc-clickhouse:/app/data" \
    -v "$ROOT_DIR/logs:/app/logs" \
    "$IMAGE" >/dev/null

  wait_http "http://127.0.0.1:$APP_PORT/api/v2/health"
  wait_pipeline_running
}

ch_query() {
  docker exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "$1" 2>/dev/null | tr -d '[:space:]'
}

wait_ch_value() {
  query="$1"
  want="$2"
  i=0
  got=""
  while [ "$i" -lt 90 ]; do
    got="$(ch_query "$query" || true)"
    if [ "$got" = "$want" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  echo "query did not reach expected value: $query (got=$got want=$want)" >&2
  return 1
}

if [ "${E2E_SKIP_BUILD:-0}" = "1" ]; then
  echo "==> Skip image build (E2E_SKIP_BUILD=1, using $IMAGE)"
else
  echo "==> Build image"
  docker build -t "$IMAGE" -f Dockerfile .
fi

echo "==> Start MySQL and ClickHouse"
docker compose -f docker-compose.dev.yml up -d mysql-source clickhouse

echo "==> Wait MySQL healthy"
wait_mysql_healthy
[ "$(docker inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER")" = "healthy" ]

echo "==> Wait ClickHouse HTTP"
wait_http "http://127.0.0.1:8123/ping"

echo "==> Prepare MySQL source table"
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "
DROP TABLE IF EXISTS dzh3136_go.$TABLE;
CREATE TABLE dzh3136_go.$TABLE (
  id INT PRIMARY KEY,
  name VARCHAR(128) NOT NULL,
  status VARCHAR(32),
  amount DECIMAL(12,2),
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO dzh3136_go.$TABLE (id, name, status, amount) VALUES
  (1, 'Snapshot CH 1', 'active', 10.10),
  (2, 'Snapshot CH 2', 'active', 20.20),
  (3, 'Snapshot CH 3', 'inactive', 30.30),
  (4, 'Snapshot CH 4', 'active', 40.40),
  (5, 'Snapshot CH 5', 'active', 50.50);
"

echo "==> Prepare ClickHouse target"
docker exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --multiquery "
CREATE DATABASE IF NOT EXISTS dzh3136_go;
DROP TABLE IF EXISTS dzh3136_go.$TABLE;
"

echo "==> Reset ETL data"
rm -rf data-snapshot-cdc-clickhouse
mkdir -p data-snapshot-cdc-clickhouse/output data-snapshot-cdc-clickhouse/checkpoint data-snapshot-cdc-clickhouse/dlq logs
chmod -R a+rwX data-snapshot-cdc-clickhouse
chmod a+rwX logs

echo "==> Run snapshot+CDC to ClickHouse pipeline"
run_app

echo "==> Verify initial snapshot copied"
wait_ch_value "SELECT count() FROM dzh3136_go.$TABLE FINAL" "5"
wait_checkpoint_cdc

echo "==> Verify CDC update/insert/delete"
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "
UPDATE $TABLE SET amount = 222.22 WHERE id = 2;
DELETE FROM $TABLE WHERE id = 3;
INSERT INTO $TABLE (id, name, status, amount) VALUES (6, 'CDC CH 6', 'active', 66.66);
"
wait_ch_value "SELECT count() FROM dzh3136_go.$TABLE FINAL WHERE id = 2 AND amount = 222.22" "1"
wait_ch_value "SELECT count() FROM dzh3136_go.$TABLE FINAL WHERE id = 3" "0"
wait_ch_value "SELECT count() FROM dzh3136_go.$TABLE FINAL WHERE id = 6 AND amount = 66.66" "1"
wait_checkpoint_cdc

echo "==> Verify schema drift add-column"
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "
ALTER TABLE $TABLE ADD COLUMN loyalty VARCHAR(32);
INSERT INTO $TABLE (id, name, status, amount, loyalty) VALUES (7, 'CDC CH 7', 'active', 77.77, 'gold');
"
wait_ch_value "SELECT count() FROM system.columns WHERE database = 'dzh3136_go' AND table = '$TABLE' AND name = 'loyalty'" "1"
wait_ch_value "SELECT count() FROM dzh3136_go.$TABLE FINAL WHERE id = 7 AND loyalty = 'gold'" "1"
wait_checkpoint_cdc

echo "==> Verify restart recovery from checkpoint"
docker kill "$APP_CONTAINER" >/dev/null
docker rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "
INSERT INTO $TABLE (id, name, status, amount, loyalty) VALUES (8, 'Restart CH 8', 'active', 88.88, 'silver');
"
run_app
wait_ch_value "SELECT count() FROM dzh3136_go.$TABLE FINAL WHERE id = 8 AND loyalty = 'silver'" "1"
wait_checkpoint_cdc

echo "==> Verify checkpoint reset replay is absorbed by ReplacingMergeTree"
curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/stop" >/dev/null
curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/checkpoint/reset" >/dev/null
curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/start" >/dev/null
wait_pipeline_running
wait_ch_value "SELECT count() FROM dzh3136_go.$TABLE FINAL" "7"
i=0
raw_id_1=0
while [ "$i" -lt 90 ]; do
  raw_id_1="$(ch_query "SELECT count() FROM dzh3136_go.$TABLE WHERE id = 1" || true)"
  if [ "$raw_id_1" != "" ] && [ "$raw_id_1" -ge 2 ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "${raw_id_1:-0}" -ge 2
wait_ch_value "SELECT count() FROM dzh3136_go.$TABLE FINAL WHERE id = 1" "1"
wait_checkpoint_cdc

echo "==> Verify ClickHouse outage routes to DLQ and replay succeeds"
docker compose -f docker-compose.dev.yml stop clickhouse >/dev/null
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 dzh3136_go -e "
INSERT INTO $TABLE (id, name, status, amount, loyalty) VALUES (9001, 'DLQ CH 9001', 'active', 900.10, 'replay');
"
i=0
dlq_body=""
while [ "$i" -lt 90 ]; do
  dlq_body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPELINE?contains=9001&limit=10" 2>/dev/null || true)"
  if echo "$dlq_body" | grep -q '9001'; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
echo "$dlq_body"
echo "$dlq_body" | grep '9001'
echo "$dlq_body" | grep -E 'clickhouse|connection refused|broken pipe|reset by peer|EOF'
dlq_id="$(echo "$dlq_body" | grep -o '"id":[0-9][0-9]*' | head -n1 | sed 's/[^0-9]//g')"
test "$dlq_id" != ""

curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/stop" >/dev/null || true
docker compose -f docker-compose.dev.yml up -d clickhouse
wait_http "http://127.0.0.1:8123/ping"
replay_body="$(curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPELINE/$dlq_id/replay")"
echo "$replay_body"
echo "$replay_body" | grep '"replayed":1'
wait_ch_value "SELECT count() FROM dzh3136_go.$TABLE FINAL WHERE id = 9001 AND loyalty = 'replay'" "1"
dlq_after="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPELINE?contains=9001&limit=10")"
echo "$dlq_after"
if echo "$dlq_after" | grep -q "\"id\":${dlq_id}"; then
  echo "replayed DLQ id ${dlq_id} was not deleted" >&2
  exit 1
fi

body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines")"
echo "$body"
echo "$body" | grep "\"name\":\"$PIPELINE\""

docker rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true

echo "Snapshot+CDC ClickHouse E2E passed"
