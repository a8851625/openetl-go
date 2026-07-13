#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="${IMAGE:-openetl-go-etl:dev}"
REDPANDA_CONTAINER="etl-redpanda"
MYSQL_CONTAINER="etl-mysql-source"
APP_CONTAINER="etl-openetl-go-debezium-mysql"
DDL_REJECT_APP_CONTAINER="etl-openetl-go-debezium-ddl-reject"
TOPIC="debezium.dl_vls_dev.vehicle_charge"
DDL_REJECT_TOPIC="debezium.dl_vls_dev.vehicle_charge_ddl_reject"
PK_TOPIC="debezium.dl_vls_dev.pk_metadata"
TARGET_TABLE="ods_dl_vls_dev__vehicle_charge"
PK_ORDER_TABLE="ods_pk_vehicle_order"
PK_TRIP_TABLE="ods_pk_vehicle_trip"
GROUP_ID="debezium-mysql-e2e-$(date +%s)"
DDL_REJECT_GROUP_ID="debezium-ddl-reject-e2e-$(date +%s)"
PK_METADATA_GROUP_ID="debezium-pk-metadata-e2e-$(date +%s)"

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
  "$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile .
fi

echo "==> Start MySQL and Redpanda"
compose -f docker-compose.dev.yml up -d mysql-source redpanda

echo "==> Wait MySQL healthy"
i=0
while [ "$i" -lt 60 ]; do
  status="$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
  if [ "$status" = "healthy" ]; then break; fi
  i=$((i + 1)); sleep 2
done
[ "$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER")" = "healthy" ]

echo "==> Wait Redpanda"
i=0
while [ "$i" -lt 90 ]; do
  if "$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null 2>&1; then break; fi
  i=$((i + 1)); sleep 2
done
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null

echo "==> Prepare Kafka topic"
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic delete "$TOPIC" "$DDL_REJECT_TOPIC" "$PK_TOPIC" --brokers localhost:9092 >/dev/null 2>&1 || true
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic create "$TOPIC" "$DDL_REJECT_TOPIC" "$PK_TOPIC" --brokers localhost:9092 >/dev/null

echo "==> Prepare MySQL target"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "
SET GLOBAL innodb_lock_wait_timeout=1;
CREATE DATABASE IF NOT EXISTS dzh3136_target;
DROP TABLE IF EXISTS dzh3136_target.$TARGET_TABLE;
DROP TABLE IF EXISTS dzh3136_target.ods_dl_vls_dev__vehicle_charge_debug;
DROP TABLE IF EXISTS dzh3136_target.$PK_ORDER_TABLE;
DROP TABLE IF EXISTS dzh3136_target.$PK_TRIP_TABLE;
CREATE TABLE dzh3136_target.$PK_ORDER_TABLE (
  order_id BIGINT PRIMARY KEY,
  vin VARCHAR(64) NOT NULL,
  amount INT NOT NULL
);
CREATE TABLE dzh3136_target.$PK_TRIP_TABLE (
  tenant_id VARCHAR(32) NOT NULL,
  seq INT NOT NULL,
  vin VARCHAR(64) NOT NULL,
  miles INT NOT NULL,
  PRIMARY KEY (tenant_id, seq)
);
INSERT INTO dzh3136_target.$PK_ORDER_TABLE (order_id, vin, amount) VALUES (8101, 'VIN-PK-ORDER', 10);
INSERT INTO dzh3136_target.$PK_TRIP_TABLE (tenant_id, seq, vin, miles) VALUES ('fleet-a', 7, 'VIN-PK-TRIP', 12);
GRANT ALL PRIVILEGES ON dzh3136_target.* TO 'sync_user'@'%';
FLUSH PRIVILEGES;
"

echo "==> Reset ETL data"
rm -rf data-debezium-mysql data-debezium-ddl-reject
mkdir -p data-debezium-mysql/output data-debezium-mysql/checkpoint data-debezium-mysql/dlq logs
chmod -R a+rwX data-debezium-mysql
chmod a+rwX logs

echo "==> Run Debezium Kafka -> MySQL pipeline"
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p 8017:8001 \
  -e DEBEZIUM_MYSQL_GROUP_ID="$GROUP_ID" \
  -e DEBEZIUM_PK_METADATA_GROUP_ID="$PK_METADATA_GROUP_ID" \
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
  echo "$body" | grep '"name":"debezium-kafka-to-mysql"' | grep '"status":"running"' >/dev/null 2>&1 && \
    echo "$body" | grep '"name":"debezium-pk-metadata-to-mysql"' | grep '"status":"running"' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done

echo "==> Produce Debezium CDC events"
cat <<'JSON' | "$CONTAINER_CLI" exec -i "$REDPANDA_CONTAINER" rpk topic produce "$TOPIC" --brokers localhost:9092 >/dev/null
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
  copied="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.$TARGET_TABLE WHERE id=9001 AND soc=72;" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "$copied" = "1" ]; then break; fi
  i=$((i + 1)); sleep 1
done
test "$copied" = "1"

snapshot_count="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.$TARGET_TABLE WHERE id=9002;" | tr -d '[:space:]')"
test "$snapshot_count" = "0"

debug_tables="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='dzh3136_target' AND table_name='ods_dl_vls_dev__vehicle_charge_debug';" | tr -d '[:space:]')"
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

final_rows="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.$TARGET_TABLE WHERE id=9001 AND soc=72;" | tr -d '[:space:]')"
test "$final_rows" = "1"

echo "==> Restart Redpanda and verify Debezium consumer resumes"
"$CONTAINER_CLI" restart "$REDPANDA_CONTAINER" >/dev/null
i=0
while [ "$i" -lt 90 ]; do
  if "$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null 2>&1; then break; fi
  i=$((i + 1)); sleep 2
done
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null

cat <<'JSON' | "$CONTAINER_CLI" exec -i "$REDPANDA_CONTAINER" rpk topic produce "$TOPIC" --brokers localhost:9092 >/dev/null
{"payload":{"op":"c","ts_ms":1710000009123,"source":{"db":"dl_vls_dev","table":"vehicle_charge"},"after":{"id":9005,"vin":"VIN-BROKER-RESTART","soc":45}}}
JSON

i=0
while [ "$i" -lt 90 ]; do
  resumed_rows="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.$TARGET_TABLE WHERE id=9005 AND vin='VIN-BROKER-RESTART' AND soc=45;" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "$resumed_rows" = "1" ]; then break; fi
  i=$((i + 1)); sleep 1
done
test "$resumed_rows" = "1"

echo "==> Trigger consumer group rebalance and verify Debezium consumer resumes"
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" sh -c "rpk topic consume '$TOPIC' --brokers localhost:9092 --group '$GROUP_ID' --offset end --num 1 >/tmp/openetl-rebalance-consumer.log 2>&1 & pid=\$!; sleep 5; kill \"\$pid\" >/dev/null 2>&1 || true; wait \"\$pid\" >/dev/null 2>&1 || true"

cat <<'JSON' | "$CONTAINER_CLI" exec -i "$REDPANDA_CONTAINER" rpk topic produce "$TOPIC" --brokers localhost:9092 >/dev/null
{"payload":{"op":"c","ts_ms":1710000009223,"source":{"db":"dl_vls_dev","table":"vehicle_charge"},"after":{"id":9006,"vin":"VIN-REBALANCE","soc":46}}}
JSON

i=0
while [ "$i" -lt 90 ]; do
  rebalanced_rows="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.$TARGET_TABLE WHERE id=9006 AND vin='VIN-REBALANCE' AND soc=46;" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "$rebalanced_rows" = "1" ]; then break; fi
  i=$((i + 1)); sleep 1
done
test "$rebalanced_rows" = "1"

echo "==> Inject transient MySQL lock wait and verify sink retry"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "INSERT INTO dzh3136_target.$TARGET_TABLE (id, vin, soc) VALUES (9007, 'VIN-LOCK-RETRY', 1) ON DUPLICATE KEY UPDATE vin=VALUES(vin), soc=VALUES(soc);"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" sh -c "mysql -uroot -proot123456 dzh3136_target -e \"SET SESSION innodb_lock_wait_timeout=10; START TRANSACTION; SELECT id FROM $TARGET_TABLE WHERE id=9007 FOR UPDATE; DO SLEEP(3); COMMIT;\"" >/dev/null 2>&1 &
lock_pid="$!"
sleep 1
cat <<'JSON' | "$CONTAINER_CLI" exec -i "$REDPANDA_CONTAINER" rpk topic produce "$TOPIC" --brokers localhost:9092 >/dev/null
{"payload":{"op":"u","ts_ms":1710000009323,"source":{"db":"dl_vls_dev","table":"vehicle_charge"},"before":{"id":9007,"vin":"VIN-LOCK-RETRY","soc":1},"after":{"id":9007,"vin":"VIN-LOCK-RETRY","soc":77}}}
JSON

i=0
while [ "$i" -lt 120 ]; do
  retried_rows="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.$TARGET_TABLE WHERE id=9007 AND vin='VIN-LOCK-RETRY' AND soc=77;" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "$retried_rows" = "1" ]; then break; fi
  i=$((i + 1)); sleep 1
done
wait "$lock_pid" || true
test "$retried_rows" = "1"
body="$(curl -fsS http://127.0.0.1:8017/api/v2/dlq/debezium-kafka-to-mysql?contains=VIN-LOCK-RETRY)"
echo "$body"
echo "$body" | grep '"items":\[\]'

echo "==> Inject Debezium sink failure and replay from DLQ"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "ALTER TABLE dzh3136_target.$TARGET_TABLE MODIFY COLUMN soc TINYINT UNSIGNED;"
cat <<'JSON' | "$CONTAINER_CLI" exec -i "$REDPANDA_CONTAINER" rpk topic produce "$TOPIC" --brokers localhost:9092 >/dev/null
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

"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "ALTER TABLE dzh3136_target.$TARGET_TABLE MODIFY COLUMN soc DOUBLE;"
replay="$(curl -fsS -X POST 'http://127.0.0.1:8017/api/v2/dlq/debezium-kafka-to-mysql/replay?contains=VIN-DLQ')"
echo "$replay"
echo "$replay" | grep '"replayed":1'

dlq_replayed_rows="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.$TARGET_TABLE WHERE id=9004 AND vin='VIN-DLQ' AND soc=999;" | tr -d '[:space:]')"
test "$dlq_replayed_rows" = "1"
body="$(curl -fsS http://127.0.0.1:8017/api/v2/dlq/debezium-kafka-to-mysql?contains=VIN-DLQ)"
echo "$body"
echo "$body" | grep '"items":\[\]'

echo "==> Validate multi-table Debezium key -> metadata PK delete inference"
cat <<'JSON' | "$CONTAINER_CLI" exec -i "$REDPANDA_CONTAINER" rpk topic produce "$PK_TOPIC" --brokers localhost:9092 -f '%k %v{json}\n' >/dev/null
{"order_id":8101} {"payload":{"op":"d","ts_ms":1710000011123,"source":{"db":"dl_vls_dev","table":"vehicle_order"},"before":{"order_id":8101,"vin":"VIN-PK-ORDER","amount":10},"after":null}}
{"tenant_id":"fleet-a","seq":7} {"payload":{"op":"d","ts_ms":1710000012123,"source":{"db":"dl_vls_dev","table":"vehicle_trip"},"before":{"tenant_id":"fleet-a","seq":7,"vin":"VIN-PK-TRIP","miles":12},"after":null}}
JSON

i=0
while [ "$i" -lt 90 ]; do
  pk_order_rows="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.$PK_ORDER_TABLE WHERE order_id=8101;" 2>/dev/null | tr -d '[:space:]' || true)"
  pk_trip_rows="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.$PK_TRIP_TABLE WHERE tenant_id='fleet-a' AND seq=7;" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "$pk_order_rows" = "0" ] && [ "$pk_trip_rows" = "0" ]; then break; fi
  i=$((i + 1)); sleep 1
done
test "$pk_order_rows" = "0"
test "$pk_trip_rows" = "0"
body="$(curl -fsS http://127.0.0.1:8017/api/v2/dlq/debezium-pk-metadata-to-mysql?limit=10)"
echo "$body"
echo "$body" | grep '"items":\[\]'

echo "==> Reject dangerous Debezium DDL into DLQ"
mkdir -p data-debezium-ddl-reject/output data-debezium-ddl-reject/checkpoint data-debezium-ddl-reject/dlq
chmod -R a+rwX data-debezium-ddl-reject
"$CONTAINER_CLI" rm -f "$DDL_REJECT_APP_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d \
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

cat <<'JSON' | "$CONTAINER_CLI" exec -i "$REDPANDA_CONTAINER" rpk topic produce "$DDL_REJECT_TOPIC" --brokers localhost:9092 >/dev/null
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

soc_columns="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='dzh3136_target' AND table_name='$TARGET_TABLE' AND column_name='soc';" | tr -d '[:space:]')"
test "$soc_columns" = "1"

"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "SET GLOBAL innodb_lock_wait_timeout=50;"

echo "Debezium Kafka -> MySQL E2E passed"
