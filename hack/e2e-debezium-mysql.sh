#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

IMAGE="openetl-go-etl:dev"
REDPANDA_CONTAINER="etl-redpanda"
MYSQL_CONTAINER="etl-mysql-source"
APP_CONTAINER="etl-openetl-go-debezium-mysql"
DDL_REJECT_APP_CONTAINER="etl-openetl-go-debezium-ddl-reject"
TOPIC="debezium.dl_vls_dev.vehicle_charge"
DDL_REJECT_TOPIC="debezium.dl_vls_dev.vehicle_charge_ddl_reject"
TARGET_TABLE="ods_dl_vls_dev__vehicle_charge"
GROUP_ID="debezium-mysql-e2e-$(date +%s)"
DDL_REJECT_GROUP_ID="debezium-ddl-reject-e2e-$(date +%s)"

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 60 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 1
  done
  return 1
}

if [ "${E2E_SKIP_BUILD:-0}" = "1" ]; then
  echo "==> Skip image build (E2E_SKIP_BUILD=1, using $IMAGE)"
else
  echo "==> Build image"
  docker build -t "$IMAGE" -f Dockerfile .
fi

echo "==> Start MySQL and Redpanda"
docker compose -f docker-compose.dev.yml up -d mysql-source redpanda

echo "==> Wait MySQL healthy"
i=0
while [ "$i" -lt 60 ]; do
  status="$(docker inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
  if [ "$status" = "healthy" ]; then break; fi
  i=$((i + 1)); sleep 2
done
[ "$(docker inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER")" = "healthy" ]

echo "==> Wait Redpanda"
i=0
while [ "$i" -lt 90 ]; do
  if docker exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null 2>&1; then break; fi
  i=$((i + 1)); sleep 2
done
docker exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null

echo "==> Prepare Kafka topic"
docker exec "$REDPANDA_CONTAINER" rpk topic delete "$TOPIC" "$DDL_REJECT_TOPIC" --brokers localhost:9092 >/dev/null 2>&1 || true
docker exec "$REDPANDA_CONTAINER" rpk topic create "$TOPIC" "$DDL_REJECT_TOPIC" --brokers localhost:9092 >/dev/null

echo "==> Prepare MySQL target"
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "SET GLOBAL innodb_lock_wait_timeout=1; CREATE DATABASE IF NOT EXISTS dzh3136_target; DROP TABLE IF EXISTS dzh3136_target.$TARGET_TABLE; DROP TABLE IF EXISTS dzh3136_target.ods_dl_vls_dev__vehicle_charge_debug; GRANT ALL PRIVILEGES ON dzh3136_target.* TO 'sync_user'@'%'; FLUSH PRIVILEGES;"

echo "==> Reset ETL data"
rm -rf data-debezium-mysql data-debezium-ddl-reject
mkdir -p data-debezium-mysql/output data-debezium-mysql/checkpoint data-debezium-mysql/dlq logs
chmod -R a+rwX data-debezium-mysql
chmod a+rwX logs

echo "==> Run Debezium Kafka -> MySQL pipeline"
docker rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
docker run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p 8017:8001 \
  -e DEBEZIUM_MYSQL_GROUP_ID="$GROUP_ID" \
  -v "$ROOT_DIR/testdata/pipes-debezium-mysql:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-debezium-mysql:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8017/api/v2/health"

echo "==> Wait pipeline running"
i=0
while [ "$i" -lt 60 ]; do
  body="$(curl -fsS http://127.0.0.1:8017/api/v2/pipelines)"
  echo "$body" | grep '"name":"debezium-kafka-to-mysql"' | grep '"status":"running"' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done

echo "==> Produce Debezium CDC events"
cat <<'JSON' | docker exec -i "$REDPANDA_CONTAINER" rpk topic produce "$TOPIC" --brokers localhost:9092 >/dev/null
{"payload":{"op":"r","ts_ms":1710000000123,"snapshot":"true","source":{"db":"dl_vls_dev","table":"vehicle_charge"},"after":{"id":9002,"vin":"VIN-SNAPSHOT","soc":11}}}
{"payload":{"op":"c","ts_ms":1710000001123,"source":{"db":"dl_vls_dev","table":"vehicle_charge"},"after":{"id":9001,"vin":"VIN-9001","soc":68}}}
{"payload":{"op":"u","ts_ms":1710000002123,"source":{"db":"dl_vls_dev","table":"vehicle_charge"},"before":{"id":9001,"vin":"VIN-9001","soc":68},"after":{"id":9001,"vin":"VIN-9001","soc":72}}}
{"payload":{"op":"c","ts_ms":1710000003123,"source":{"db":"dl_vls_dev","table":"vehicle_charge_debug"},"after":{"id":9003,"vin":"VIN-DEBUG","soc":1}}}
{"payload":{"op":"d","ts_ms":1710000004123,"source":{"db":"dl_vls_dev","table":"vehicle_charge"},"before":{"id":9001,"vin":"VIN-9001","soc":72},"after":null}}
{"payload":{"ts_ms":1710000005123,"source":{"db":"dl_vls_dev","table":"vehicle_charge"},"ddl":"ALTER TABLE vehicle_charge DROP COLUMN soc"}}
JSON

echo "==> Verify MySQL target row"
i=0
while [ "$i" -lt 60 ]; do
  copied="$(docker exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.$TARGET_TABLE WHERE id=9001 AND soc=72;" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "$copied" = "1" ]; then break; fi
  i=$((i + 1)); sleep 1
done
test "$copied" = "1"

snapshot_count="$(docker exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.$TARGET_TABLE WHERE id=9002;" | tr -d '[:space:]')"
test "$snapshot_count" = "0"

debug_tables="$(docker exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='dzh3136_target' AND table_name='ods_dl_vls_dev__vehicle_charge_debug';" | tr -d '[:space:]')"
test "$debug_tables" = "0"

body="$(curl -fsS http://127.0.0.1:8017/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"name":"debezium-kafka-to-mysql"' | grep '"records_written":2'

echo "==> Replay from Kafka offset 1 through checkpoint set"
curl -fsS -X POST http://127.0.0.1:8017/api/v2/pipelines/debezium-kafka-to-mysql/stop >/dev/null
# Give the runner's graceful shutdown path time to finish its final checkpoint
# save before replacing it with the explicit replay checkpoint.
sleep 3
curl -fsS -X POST \
  -H 'Content-Type: application/json' \
  -d '{"source":"kafka","topic":"debezium.dl_vls_dev.vehicle_charge","partition":0,"offset":1}' \
  http://127.0.0.1:8017/api/v2/pipelines/debezium-kafka-to-mysql/checkpoint/set >/dev/null
curl -fsS -X POST http://127.0.0.1:8017/api/v2/pipelines/debezium-kafka-to-mysql/start >/dev/null

i=0
while [ "$i" -lt 60 ]; do
  body="$(curl -fsS http://127.0.0.1:8017/api/v2/pipelines)"
  echo "$body" | grep '"name":"debezium-kafka-to-mysql"' | grep '"records_written":2' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done
body="$(curl -fsS http://127.0.0.1:8017/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"name":"debezium-kafka-to-mysql"' | grep '"records_written":2'

final_rows="$(docker exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.$TARGET_TABLE WHERE id=9001 AND soc=72;" | tr -d '[:space:]')"
test "$final_rows" = "1"

echo "==> Restart Redpanda and verify Debezium consumer resumes"
docker restart "$REDPANDA_CONTAINER" >/dev/null
i=0
while [ "$i" -lt 90 ]; do
  if docker exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null 2>&1; then break; fi
  i=$((i + 1)); sleep 2
done
docker exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null

cat <<'JSON' | docker exec -i "$REDPANDA_CONTAINER" rpk topic produce "$TOPIC" --brokers localhost:9092 >/dev/null
{"payload":{"op":"c","ts_ms":1710000009123,"source":{"db":"dl_vls_dev","table":"vehicle_charge"},"after":{"id":9005,"vin":"VIN-BROKER-RESTART","soc":45}}}
JSON

i=0
while [ "$i" -lt 90 ]; do
  resumed_rows="$(docker exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.$TARGET_TABLE WHERE id=9005 AND vin='VIN-BROKER-RESTART' AND soc=45;" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "$resumed_rows" = "1" ]; then break; fi
  i=$((i + 1)); sleep 1
done
test "$resumed_rows" = "1"

echo "==> Trigger consumer group rebalance and verify Debezium consumer resumes"
docker exec "$REDPANDA_CONTAINER" sh -c "rpk topic consume '$TOPIC' --brokers localhost:9092 --group '$GROUP_ID' --offset end --num 1 >/tmp/openetl-rebalance-consumer.log 2>&1 & pid=\$!; sleep 5; kill \"\$pid\" >/dev/null 2>&1 || true; wait \"\$pid\" >/dev/null 2>&1 || true"

cat <<'JSON' | docker exec -i "$REDPANDA_CONTAINER" rpk topic produce "$TOPIC" --brokers localhost:9092 >/dev/null
{"payload":{"op":"c","ts_ms":1710000009223,"source":{"db":"dl_vls_dev","table":"vehicle_charge"},"after":{"id":9006,"vin":"VIN-REBALANCE","soc":46}}}
JSON

i=0
while [ "$i" -lt 90 ]; do
  rebalanced_rows="$(docker exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.$TARGET_TABLE WHERE id=9006 AND vin='VIN-REBALANCE' AND soc=46;" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "$rebalanced_rows" = "1" ]; then break; fi
  i=$((i + 1)); sleep 1
done
test "$rebalanced_rows" = "1"

echo "==> Inject transient MySQL lock wait and verify sink retry"
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "INSERT INTO dzh3136_target.$TARGET_TABLE (id, vin, soc) VALUES (9007, 'VIN-LOCK-RETRY', 1) ON DUPLICATE KEY UPDATE vin=VALUES(vin), soc=VALUES(soc);"
docker exec "$MYSQL_CONTAINER" sh -c "mysql -uroot -proot123456 dzh3136_target -e \"SET SESSION innodb_lock_wait_timeout=10; START TRANSACTION; SELECT id FROM $TARGET_TABLE WHERE id=9007 FOR UPDATE; DO SLEEP(3); COMMIT;\"" >/dev/null 2>&1 &
lock_pid="$!"
sleep 1
cat <<'JSON' | docker exec -i "$REDPANDA_CONTAINER" rpk topic produce "$TOPIC" --brokers localhost:9092 >/dev/null
{"payload":{"op":"u","ts_ms":1710000009323,"source":{"db":"dl_vls_dev","table":"vehicle_charge"},"before":{"id":9007,"vin":"VIN-LOCK-RETRY","soc":1},"after":{"id":9007,"vin":"VIN-LOCK-RETRY","soc":77}}}
JSON

i=0
while [ "$i" -lt 120 ]; do
  retried_rows="$(docker exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.$TARGET_TABLE WHERE id=9007 AND vin='VIN-LOCK-RETRY' AND soc=77;" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "$retried_rows" = "1" ]; then break; fi
  i=$((i + 1)); sleep 1
done
wait "$lock_pid" || true
test "$retried_rows" = "1"
body="$(curl -fsS http://127.0.0.1:8017/api/v2/dlq/debezium-kafka-to-mysql?contains=VIN-LOCK-RETRY)"
echo "$body"
echo "$body" | grep '"items":\[\]'

echo "==> Inject Debezium sink failure and replay from DLQ"
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "ALTER TABLE dzh3136_target.$TARGET_TABLE MODIFY COLUMN soc TINYINT UNSIGNED;"
cat <<'JSON' | docker exec -i "$REDPANDA_CONTAINER" rpk topic produce "$TOPIC" --brokers localhost:9092 >/dev/null
{"payload":{"op":"c","ts_ms":1710000010123,"source":{"db":"dl_vls_dev","table":"vehicle_charge"},"after":{"id":9004,"vin":"VIN-DLQ","soc":999}}}
JSON

i=0
while [ "$i" -lt 60 ]; do
  body="$(curl -fsS http://127.0.0.1:8017/api/v2/dlq/debezium-kafka-to-mysql?contains=VIN-DLQ)"
  echo "$body" | grep 'VIN-DLQ' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done
echo "$body"
echo "$body" | grep 'VIN-DLQ'
echo "$body" | grep 'Out of range'
echo "$body" | grep '"error_class":"data"'
curl -fsS -X POST http://127.0.0.1:8017/api/v2/pipelines/debezium-kafka-to-mysql/stop >/dev/null

docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "ALTER TABLE dzh3136_target.$TARGET_TABLE MODIFY COLUMN soc DOUBLE;"
replay="$(curl -fsS -X POST 'http://127.0.0.1:8017/api/v2/dlq/debezium-kafka-to-mysql/replay?contains=VIN-DLQ')"
echo "$replay"
echo "$replay" | grep '"replayed":1'

dlq_replayed_rows="$(docker exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.$TARGET_TABLE WHERE id=9004 AND vin='VIN-DLQ' AND soc=999;" | tr -d '[:space:]')"
test "$dlq_replayed_rows" = "1"
body="$(curl -fsS http://127.0.0.1:8017/api/v2/dlq/debezium-kafka-to-mysql?contains=VIN-DLQ)"
echo "$body"
echo "$body" | grep '"items":\[\]'

echo "==> Reject dangerous Debezium DDL into DLQ"
mkdir -p data-debezium-ddl-reject/output data-debezium-ddl-reject/checkpoint data-debezium-ddl-reject/dlq
chmod -R a+rwX data-debezium-ddl-reject
docker rm -f "$DDL_REJECT_APP_CONTAINER" >/dev/null 2>&1 || true
docker run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$DDL_REJECT_APP_CONTAINER" \
  -p 8019:8001 \
  -e DEBEZIUM_DDL_REJECT_GROUP_ID="$DDL_REJECT_GROUP_ID" \
  -v "$ROOT_DIR/testdata/pipes-debezium-ddl-reject:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-debezium-ddl-reject:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8019/api/v2/health"

i=0
while [ "$i" -lt 60 ]; do
  body="$(curl -fsS http://127.0.0.1:8019/api/v2/pipelines)"
  echo "$body" | grep '"name":"debezium-kafka-ddl-reject"' | grep '"status":"running"' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done

cat <<'JSON' | docker exec -i "$REDPANDA_CONTAINER" rpk topic produce "$DDL_REJECT_TOPIC" --brokers localhost:9092 >/dev/null
{"payload":{"ts_ms":1710000020123,"source":{"db":"dl_vls_dev","table":"vehicle_charge"},"ddl":"ALTER TABLE vehicle_charge DROP COLUMN soc"}}
JSON

i=0
while [ "$i" -lt 60 ]; do
  body="$(curl -fsS 'http://127.0.0.1:8019/api/v2/dlq/debezium-kafka-ddl-reject?error_contains=dangerous%20DDL%20rejected')"
  echo "$body" | grep 'dangerous DDL rejected' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done
echo "$body"
echo "$body" | grep 'ALTER TABLE vehicle_charge DROP COLUMN soc'
echo "$body" | grep '"error_class":"schema"'
curl -fsS -X POST http://127.0.0.1:8019/api/v2/pipelines/debezium-kafka-ddl-reject/stop >/dev/null

soc_columns="$(docker exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='dzh3136_target' AND table_name='$TARGET_TABLE' AND column_name='soc';" | tr -d '[:space:]')"
test "$soc_columns" = "1"

docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "SET GLOBAL innodb_lock_wait_timeout=50;"

echo "Debezium Kafka -> MySQL E2E passed"
