#!/bin/sh

# IT-2/T2.4 source-order certification.
#
# Path 1 delegates to the MySQL snapshot+CDC -> ClickHouse native test, whose
# checkpoint-reset section changes source state while stopped and proves that
# a re-snapshot's binlog handoff version supersedes previously written CDC.
#
# Path 2 drives Kafka envelope -> ClickHouse HTTP. A target CHECK constraint
# sends an old UPDATE and INSERT to DLQ; newer UPDATE/DELETE events commit
# first, then replaying the old records must neither overwrite nor resurrect.
# Finally both the OpenETL checkpoint and the external Kafka group are reset so
# the entire topic is replayed and reconciled by Kafka partition/offset order.

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
REDPANDA_CONTAINER="etl-redpanda"
CH_CONTAINER="etl-clickhouse"
APP_CONTAINER="etl-openetl-clickhouse-replay-ordering"
APP_PORT="${CH_REPLAY_API_PORT:-8043}"
PIPELINE="kafka-clickhouse-replay-ordering"
GROUP="kafka-clickhouse-replay-ordering"
TOPIC="clickhouse-replay-ordering"
CH_DB="dzh3136_go"
CH_TABLE="replay_ordering_http"

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

wait_redpanda() {
  i=0
  while [ "$i" -lt 90 ]; do
    if "$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

ch_query() {
  "$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "$1" 2>/dev/null | tr -d '[:space:]'
}

wait_ch_value() {
  query="$1"
  want="$2"
  i=0
  got=""
  while [ "$i" -lt 120 ]; do
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

wait_pipeline_running() {
  i=0
  body=""
  while [ "$i" -lt 90 ]; do
    body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines" 2>/dev/null || true)"
    if echo "$body" | grep "\"name\":\"$PIPELINE\"" | grep '"status":"running"' >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  echo "$body" >&2
  return 1
}

wait_checkpoint_offset() {
  want="$1"
  i=0
  body=""
  while [ "$i" -lt 120 ]; do
    body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/checkpoint" 2>/dev/null || true)"
    if echo "$body" | grep "\"offsets\":{\"0\":$want}" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  echo "$body" >&2
  return 1
}

wait_dlq_id() {
  contains="$1"
  i=0
  body=""
  DLQ_ID=""
  while [ "$i" -lt 120 ]; do
    body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPELINE?contains=$contains&limit=20" 2>/dev/null || true)"
    DLQ_ID="$(echo "$body" | grep -o '"id":[0-9][0-9]*' | head -n1 | sed 's/[^0-9]//g' || true)"
    if [ -n "$DLQ_ID" ]; then
      echo "$body"
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  echo "$body" >&2
  return 1
}

produce_envelope() {
  printf '%s\n' "$1" | "$CONTAINER_CLI" exec -i "$REDPANDA_CONTAINER" \
    rpk topic produce "$TOPIC" --brokers localhost:9092 >/dev/null
}

delete_consumer_group() {
  i=0
  while [ "$i" -lt 20 ]; do
    if "$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk group delete "$GROUP" --brokers localhost:9092 >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  echo "could not delete stopped Kafka consumer group $GROUP" >&2
  return 1
}

if [ "${E2E_SKIP_BUILD:-0}" = "1" ]; then
  echo "==> Skip image build (E2E_SKIP_BUILD=1, using $IMAGE)"
else
  echo "==> Build image"
  "$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile .
fi

echo "==> Path 1/2: MySQL snapshot+CDC -> ClickHouse native"
E2E_SKIP_BUILD=1 "$ROOT_DIR/hack/e2e-snapshot-cdc-clickhouse.sh"

echo "==> Path 2/2: Kafka envelope -> ClickHouse HTTP"
if ! "$CONTAINER_CLI" image inspect docker.io/clickhouse/clickhouse-server:24.3-alpine >/dev/null 2>&1; then
  echo "SKIP: clickhouse-server:24.3-alpine is not available locally" >&2
  exit 77
fi
if ! "$CONTAINER_CLI" inspect "$CH_CONTAINER" >/dev/null 2>&1; then
  compose -f docker-compose.dev.yml up -d clickhouse >/dev/null
else
  "$CONTAINER_CLI" start "$CH_CONTAINER" >/dev/null
fi
if ! "$CONTAINER_CLI" inspect "$REDPANDA_CONTAINER" >/dev/null 2>&1; then
  compose -f docker-compose.dev.yml up -d redpanda >/dev/null
else
  "$CONTAINER_CLI" start "$REDPANDA_CONTAINER" >/dev/null
fi
wait_http "http://127.0.0.1:8123/ping"
wait_redpanda

echo "    ClickHouse version: $(ch_query 'SELECT version()')"
echo "    ClickHouse image: docker.io/clickhouse/clickhouse-server:24.3-alpine"

"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk group delete "$GROUP" --brokers localhost:9092 >/dev/null 2>&1 || true
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic delete "$TOPIC" --brokers localhost:9092 >/dev/null 2>&1 || true
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic create "$TOPIC" --brokers localhost:9092 --partitions 1 >/dev/null

"$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --multiquery --query "
CREATE DATABASE IF NOT EXISTS $CH_DB;
DROP TABLE IF EXISTS $CH_DB.$CH_TABLE;
CREATE TABLE $CH_DB.$CH_TABLE (
  id UInt64,
  value String DEFAULT '',
  _version UInt64,
  _is_deleted UInt8,
  CONSTRAINT reject_stale CHECK NOT (_is_deleted = 0 AND value IN ('old-update', 'old-insert'))
) ENGINE = ReplacingMergeTree(_version, _is_deleted)
ORDER BY id;
"

rm -rf data-clickhouse-replay-ordering
mkdir -p data-clickhouse-replay-ordering/output data-clickhouse-replay-ordering/checkpoint data-clickhouse-replay-ordering/dlq logs
chmod -R a+rwX data-clickhouse-replay-ordering logs

CH_NETWORK="$("$CONTAINER_CLI" inspect "$CH_CONTAINER" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{"\n"}}{{end}}' | head -n1)"
REDPANDA_NETWORK="$("$CONTAINER_CLI" inspect "$REDPANDA_CONTAINER" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{"\n"}}{{end}}' | head -n1)"
test -n "$CH_NETWORK"
test -n "$REDPANDA_NETWORK"

"$CONTAINER_CLI" create \
  --network "$CH_NETWORK" \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p "$APP_PORT:8001" \
  -v "$ROOT_DIR/testdata/pipes-clickhouse-replay-ordering:/app/pipes:ro" \
  -v "$ROOT_DIR/data-clickhouse-replay-ordering:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE" >/dev/null
if [ "$REDPANDA_NETWORK" != "$CH_NETWORK" ]; then
  "$CONTAINER_CLI" network connect "$REDPANDA_NETWORK" "$APP_CONTAINER"
fi
"$CONTAINER_CLI" start "$APP_CONTAINER" >/dev/null

wait_http "http://127.0.0.1:$APP_PORT/api/v2/health"
wait_pipeline_running

echo "==> Commit base row, then route an older UPDATE to DLQ"
produce_envelope '{"event_id":"base-7001","op":"INSERT","table":"replay_ordering_http","key":"{\"id\":7001}","data":{"id":7001,"value":"base"}}'
wait_ch_value "SELECT count() FROM $CH_DB.$CH_TABLE FINAL WHERE id = 7001 AND value = 'base'" "1"
produce_envelope '{"event_id":"old-update-7001","op":"UPDATE","table":"replay_ordering_http","key":"{\"id\":7001}","data":{"id":7001,"value":"old-update"}}'
wait_dlq_id "old-update"
OLD_UPDATE_DLQ_ID="$DLQ_ID"

echo "==> Commit newer UPDATE after the rejected old event"
produce_envelope '{"event_id":"new-update-7001","op":"UPDATE","table":"replay_ordering_http","key":"{\"id\":7001}","data":{"id":7001,"value":"new-update"}}'
wait_ch_value "SELECT count() FROM $CH_DB.$CH_TABLE FINAL WHERE id = 7001 AND value = 'new-update'" "1"

echo "==> Route old INSERT to DLQ, then commit a newer DELETE tombstone"
produce_envelope '{"event_id":"old-insert-7002","op":"INSERT","table":"replay_ordering_http","key":"{\"id\":7002}","data":{"id":7002,"value":"old-insert"}}'
wait_dlq_id "old-insert"
OLD_INSERT_DLQ_ID="$DLQ_ID"
produce_envelope '{"event_id":"delete-7002","op":"DELETE","table":"replay_ordering_http","key":"{\"id\":7002}","data":{"id":7002,"value":"old-insert"}}'
wait_ch_value "SELECT count() FROM $CH_DB.$CH_TABLE WHERE id = 7002 AND _is_deleted = 1" "1"
wait_ch_value "SELECT count() FROM $CH_DB.$CH_TABLE FINAL WHERE id = 7002" "0"
wait_checkpoint_offset 4

echo "==> Remove fault injection and replay old DLQ records after newer state"
curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/stop" >/dev/null
"$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 \
  --query "ALTER TABLE $CH_DB.$CH_TABLE DROP CONSTRAINT reject_stale"

replay="$(curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPELINE/$OLD_UPDATE_DLQ_ID/replay")"
echo "$replay" | grep '"replayed":1' >/dev/null
replay="$(curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPELINE/$OLD_INSERT_DLQ_ID/replay")"
echo "$replay" | grep '"replayed":1' >/dev/null

wait_ch_value "SELECT count() FROM $CH_DB.$CH_TABLE FINAL WHERE id = 7001 AND value = 'new-update'" "1"
wait_ch_value "SELECT count() FROM $CH_DB.$CH_TABLE FINAL WHERE id = 7001 AND value = 'old-update'" "0"
wait_ch_value "SELECT count() FROM $CH_DB.$CH_TABLE FINAL WHERE id = 7002" "0"
wait_ch_value "SELECT count() FROM $CH_DB.$CH_TABLE WHERE id = 7002 AND value = 'old-insert' AND _is_deleted = 0" "1"

echo "==> Reset checkpoint + Kafka group and replay the full offset range"
curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/stop" >/dev/null
curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/checkpoint/reset" >/dev/null
delete_consumer_group
curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/start" >/dev/null
wait_pipeline_running
wait_checkpoint_offset 4
wait_ch_value "SELECT count() FROM $CH_DB.$CH_TABLE FINAL WHERE id = 7001 AND value = 'new-update'" "1"
wait_ch_value "SELECT count() FROM $CH_DB.$CH_TABLE FINAL WHERE id = 7002" "0"

engine="$(ch_query "SELECT engine_full FROM system.tables WHERE database = '$CH_DB' AND name = '$CH_TABLE'")"
echo "$engine" | grep '^ReplacingMergeTree(_version,_is_deleted)' >/dev/null
test "$(ch_query "SELECT type FROM system.columns WHERE database = '$CH_DB' AND table = '$CH_TABLE' AND name = '_version'")" = "UInt64"
test "$(ch_query "SELECT type FROM system.columns WHERE database = '$CH_DB' AND table = '$CH_TABLE' AND name = '_is_deleted'")" = "UInt8"

"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true

echo "ClickHouse source-order replay E2E passed (snapshot+CDC/native + Kafka/HTTP)"
