#!/bin/sh

# E2E: Kafka canal_json -> ClickHouse metadata-PK identity and DLQ replay gate.
# Proves complete composite keys, key-changing UPDATE, DELETE, frozen DLQ
# identity context, controlled legacy repair, quarantine, and restart-safe replay.

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
REDPANDA_CONTAINER="etl-redpanda"
CH_CONTAINER="etl-clickhouse"
APP_CONTAINER="etl-openetl-canal-identity"
CH_DB="dzh3136_go"
TOPIC="canal-identity"
PIPELINE="kafka-canal-identity"
APP_PORT="${CANAL_IDENTITY_API_PORT:-8040}"
DATA_DIR="$ROOT_DIR/data-kafka-canal-identity"

cleanup() {
  "$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

wait_http() {
  url="$1"; i=0
  while [ "$i" -lt 90 ]; do
    curl -fsS "$url" >/dev/null 2>&1 && return 0
    i=$((i + 1)); sleep 2
  done
  return 1
}

ch_query() {
  "$CONTAINER_CLI" exec "$CH_CONTAINER" clickhouse-client --password dzh123456 --query "$1" 2>/dev/null
}

wait_ch_value() {
  sql="$1"; expected="$2"; i=0; got=""
  while [ "$i" -lt 120 ]; do
    got="$(ch_query "$sql" | tr -d '[:space:]' || true)"
    [ "$got" = "$expected" ] && return 0
    i=$((i + 1)); sleep 1
  done
  echo "TIMEOUT: $sql = $expected (last=$got)" >&2
  return 1
}

wait_checkpoint_offset() {
  expected="$1"; i=0; body=""
  while [ "$i" -lt 120 ]; do
    body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/checkpoint" 2>/dev/null || true)"
    echo "$body" | grep "\"offsets\":{\"0\":$expected}" >/dev/null 2>&1 && return 0
    i=$((i + 1)); sleep 1
  done
  echo "$body" >&2
  return 1
}

produce() {
  "$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" sh -c "printf '%s\n' '$1' | rpk topic produce $TOPIC" >/dev/null
}

if [ "${E2E_SKIP_BUILD:-0}" = "1" ]; then
  echo "==> Use existing $IMAGE"
else
  echo "==> Build image"
  "$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile .
fi

if ! "$CONTAINER_CLI" image inspect docker.io/clickhouse/clickhouse-server:24.3-alpine >/dev/null 2>&1; then
  echo "SKIP: ClickHouse 24.3 image is unavailable" >&2
  exit 77
fi

echo "==> Ensure ClickHouse and Redpanda are ready"
if "$CONTAINER_CLI" inspect "$CH_CONTAINER" >/dev/null 2>&1; then
  "$CONTAINER_CLI" start "$CH_CONTAINER" >/dev/null
else
  compose -f docker-compose.dev.yml up -d clickhouse >/dev/null
fi
if "$CONTAINER_CLI" inspect "$REDPANDA_CONTAINER" >/dev/null 2>&1; then
  "$CONTAINER_CLI" start "$REDPANDA_CONTAINER" >/dev/null
else
  compose -f docker-compose.dev.yml up -d redpanda >/dev/null
fi
wait_http "http://127.0.0.1:8123/ping" || { echo "SKIP: ClickHouse not reachable" >&2; exit 77; }
i=0
while [ "$i" -lt 90 ]; do
  "$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 2
done
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk cluster health >/dev/null 2>&1 || { echo "SKIP: Redpanda not reachable" >&2; exit 77; }

echo "==> Reset isolated target and topic"
ch_query "CREATE DATABASE IF NOT EXISTS ${CH_DB}" >/dev/null
ch_query "DROP TABLE IF EXISTS ${CH_DB}.canal_identity" >/dev/null
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic delete "$TOPIC" >/dev/null 2>&1 || true
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk topic create "$TOPIC" --partitions 1 >/dev/null
"$CONTAINER_CLI" exec "$REDPANDA_CONTAINER" rpk group delete "$PIPELINE" --brokers localhost:9092 >/dev/null 2>&1 || true

echo "==> Produce valid and identity-invalid Canal messages"
produce '{"type":"INSERT","database":"shop","table":"orders","pkNames":["tenant_id","id"],"mysqlType":{"tenant_id":"varchar(32)","id":"varchar(32)","amount":"int"},"data":[{"tenant_id":"t1","id":"old","amount":10}]}'
produce '{"type":"UPDATE","database":"shop","table":"orders","pkNames":["tenant_id","id"],"mysqlType":{"tenant_id":"varchar(32)","id":"varchar(32)","amount":"int"},"data":[{"tenant_id":"t1","id":"new","amount":20}],"old":[{"id":"old","amount":10}]}'
produce '{"type":"INSERT","database":"shop","table":"orders","pkNames":["tenant_id","id"],"data":[{"tenant_id":"broken"}]}'
produce '{"type":"UPDATE","database":"shop","table":"orders","pkNames":["tenant_id","id"],"data":[{"tenant_id":"t1","id":"new","amount":30}]}'
produce '{"type":"INSERT","database":"shop","table":"orders","pkNames":["tenant_id","id"],"data":[{"tenant_id":"t2","id":"keep","amount":40}]}'

echo "==> Start identity-aware pipeline"
mkdir -p "$DATA_DIR/pipes" "$DATA_DIR/data"
cp testdata/pipes-kafka-canal-identity/*.yaml "$DATA_DIR/pipes/"
chmod -R a+rwX "$DATA_DIR"
NETWORKS="$(printf '%s %s' \
  "$("$CONTAINER_CLI" inspect "$CH_CONTAINER" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' 2>/dev/null || true)" \
  "$("$CONTAINER_CLI" inspect "$REDPANDA_CONTAINER" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' 2>/dev/null || true)")"
NETWORK_ARG="$(printf '%s\n' "$NETWORKS" | tr ' ' '\n' | sort -u | grep -v '^$' | paste -sd, -)"
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d --network "$NETWORK_ARG" --name "$APP_CONTAINER" \
  -p "$APP_PORT:8001" \
  -v "$DATA_DIR/data:/app/data" \
  -v "$DATA_DIR/pipes:/app/pipes:ro" \
  "$IMAGE" >/dev/null
wait_http "http://127.0.0.1:$APP_PORT/api/v2/health"

echo "==> Verify valid key-changing UPDATE and post-error progress"
wait_checkpoint_offset 4
wait_ch_value "SELECT count() FROM ${CH_DB}.canal_identity FINAL WHERE tenant_id='t1' AND id='old'" 0
wait_ch_value "SELECT count() FROM ${CH_DB}.canal_identity FINAL WHERE tenant_id='t1' AND id='new' AND amount=20" 1
wait_ch_value "SELECT count() FROM ${CH_DB}.canal_identity FINAL WHERE tenant_id='t2' AND id='keep' AND amount=40" 1

echo "==> Verify incomplete key and missing old state are durable DLQ events"
i=0; dlq_body=""
while [ "$i" -lt 90 ]; do
  dlq_body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPELINE" 2>/dev/null || true)"
  echo "$dlq_body" | grep 'key_component_missing' >/dev/null 2>&1 && \
    echo "$dlq_body" | grep 'update_before_state_invalid' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done
echo "$dlq_body" | grep 'key_component_missing' >/dev/null
echo "$dlq_body" | grep 'update_before_state_invalid' >/dev/null
echo "$dlq_body" | grep '"error_class":"data"' >/dev/null
printf '%s' "$dlq_body" | python3 -c '
import json, sys
items = json.load(sys.stdin)["items"]
assert len(items) == 2, items
for item in items:
    ctx = item["identity_context"]
    assert ctx["raw_payload"].startswith("{"), ctx
    assert ctx["payload_encoding"] == "source_bytes", ctx
    assert ctx["primary_key_columns"] == ["tenant_id", "id"], ctx
    assert ctx["format_contract_id"] == "kafka.canal_json/v1", ctx
    assert ctx["source_database"] == "shop", ctx
    assert ctx["source_table"] == "orders", ctx
    assert ctx["target_database"] == "dzh3136_go", ctx
    assert ctx["target_table"] == "canal_identity", ctx
    assert ctx["replay_provenance"] == "normal_flow", ctx
'

echo "==> Verify complete composite DELETE"
produce '{"type":"DELETE","database":"shop","table":"orders","pkNames":["tenant_id","id"],"data":[{"tenant_id":"t1","id":"new","amount":20}]}'
wait_checkpoint_offset 5
wait_ch_value "SELECT count() FROM ${CH_DB}.canal_identity FINAL WHERE tenant_id='t1' AND id='new'" 0
wait_ch_value "SELECT count() FROM ${CH_DB}.canal_identity FINAL" 1

echo "==> Capture one complete identity event in DLQ during sink outage"
"$CONTAINER_CLI" stop "$CH_CONTAINER" >/dev/null
produce '{"type":"INSERT","database":"shop","table":"orders","pkNames":["tenant_id","id"],"mysqlType":{"tenant_id":"varchar(32)","id":"varchar(32)","amount":"int"},"data":[{"tenant_id":"t3","id":"legacy-ok","amount":50}]}'
i=0; dlq_body=""
while [ "$i" -lt 120 ]; do
  dlq_body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPELINE" 2>/dev/null || true)"
  echo "$dlq_body" | grep 'legacy-ok' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done
echo "$dlq_body" | grep 'legacy-ok' >/dev/null
printf '%s' "$dlq_body" | python3 -c '
import json, sys
item = next(x for x in json.load(sys.stdin)["items"] if x["record"]["data"].get("id") == "legacy-ok")
ctx = item["identity_context"]
assert ctx["raw_payload"].startswith("{"), ctx
assert ctx["primary_key_columns"] == ["tenant_id", "id"], ctx
assert ctx["format_contract_id"] == "kafka.canal_json/v1", ctx
assert ctx["replay_state"] == "pending", ctx
'
"$CONTAINER_CLI" stop "$APP_CONTAINER" >/dev/null
"$CONTAINER_CLI" start "$CH_CONTAINER" >/dev/null
wait_http "http://127.0.0.1:8123/ping"

echo "==> Convert isolated rows into legacy/unknown-contract fixtures while stopped"
python3 - "$DATA_DIR/data/etl.db" <<'PY'
import copy
import json
import sqlite3
import sys

db = sqlite3.connect(sys.argv[1])
rows = db.execute(
    "SELECT id, job_name, record_json, error, error_class, attempt, "
    "COALESCE(record_hash,''), COALESCE(pipeline_version,0), COALESCE(dag_node,''), created_at "
    "FROM dead_letters"
).fetchall()
selected = {}
for row in rows:
    record = json.loads(row[2])
    data = record.get("data") or {}
    if data.get("id") == "legacy-ok":
        selected["success"] = (row, record)
    elif data.get("tenant_id") == "broken":
        selected["partial"] = (row, record)
    elif data.get("amount") == 30:
        selected["keychange"] = (row, record)
assert set(selected) == {"success", "partial", "keychange"}, selected.keys()

for kind, (row, record) in selected.items():
    record.setdefault("metadata", {}).pop("key", None)
    if kind == "keychange":
        record["before"] = {"tenant_id": "t1", "id": "old", "amount": 20}
        record["metadata"]["before_image_state"] = "full"
    db.execute(
        "UPDATE dead_letters SET record_json=?, identity_context_json='{}' WHERE id=?",
        (json.dumps(record, separators=(",", ":")), row[0]),
    )

source_row, source_record = selected["success"]
unknown = copy.deepcopy(source_record)
unknown["data"]["id"] = "unknown-contract"
unknown.setdefault("metadata", {}).pop("key", None)
unknown["metadata"]["format_contract_id"] = "custom.unknown/v9"
unknown_context = {
    "raw_payload": json.dumps(unknown, separators=(",", ":")),
    "payload_encoding": "normalized_data_json",
    "primary_key_columns": ["tenant_id", "id"],
    "failure_reason": "key_missing",
    "format_contract_id": "custom.unknown/v9",
    "before_image_state": "absent",
    "source_database": "shop",
    "source_table": "orders",
    "target_database": "dzh3136_go",
    "target_table": "canal_identity",
    "replay_provenance": "normal_flow",
    "replay_state": "pending",
}
db.execute(
    "INSERT INTO dead_letters "
    "(job_name,record_json,error,error_class,identity_context_json,attempt,record_hash,pipeline_version,dag_node,created_at) "
    "VALUES (?,?,?,?,?,?,?,?,?,?)",
    (
        source_row[1], json.dumps(unknown, separators=(",", ":")), "unknown contract fixture",
        "data", json.dumps(unknown_context, separators=(",", ":")), 0, "", 0, "", source_row[9],
    ),
)
db.commit()
db.close()
PY
"$CONTAINER_CLI" start "$APP_CONTAINER" >/dev/null
wait_http "http://127.0.0.1:$APP_PORT/api/v2/pipelines"

echo "==> Verify legacy classification survives restart and API query"
dlq_body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPELINE?limit=20")"
set -- $(printf '%s' "$dlq_body" | python3 -c '
import json, sys
items = json.load(sys.stdin)["items"]
def find(pred): return next(x for x in items if pred(x["record"].get("data") or {}))
success = find(lambda d: d.get("id") == "legacy-ok")
partial = find(lambda d: d.get("tenant_id") == "broken")
keychange = find(lambda d: d.get("amount") == 30)
unknown = find(lambda d: d.get("id") == "unknown-contract")
for item in (success, partial, keychange):
    assert item["identity_context"]["replay_provenance"] == "legacy_unknown", item
    assert item["identity_context"]["replay_state"] == "repair_required", item
assert unknown["identity_context"]["format_contract_id"] == "custom.unknown/v9", unknown
print(success["id"], partial["id"], keychange["id"], unknown["id"])
')
legacy_ok_id="$1"
partial_id="$2"
keychange_id="$3"
unknown_id="$4"

echo "==> Repair exact legacy composite key and persist it across restart"
repair_file="$DATA_DIR/repair-response.json"
status="$(curl -sS -o "$repair_file" -w '%{http_code}' -X PUT \
  -H 'Content-Type: application/json' \
  --data '{"primary_key_columns":["tenant_id","id"]}' \
  "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPELINE/$legacy_ok_id/identity")"
test "$status" = "200"
grep '"replay_provenance":"legacy_verified"' "$repair_file" >/dev/null
grep '"key":"{\\"id\\":\\"legacy-ok\\",\\"tenant_id\\":\\"t3\\"}"' "$repair_file" >/dev/null
"$CONTAINER_CLI" restart "$APP_CONTAINER" >/dev/null
wait_http "http://127.0.0.1:$APP_PORT/api/v2/pipelines"
dlq_body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPELINE?limit=20")"
printf '%s' "$dlq_body" | python3 -c '
import json, sys
item = next(x for x in json.load(sys.stdin)["items"] if x["record"]["data"].get("id") == "legacy-ok")
assert item["identity_context"]["replay_provenance"] == "legacy_verified", item
assert json.loads(item["record"]["metadata"]["key"]) == {"tenant_id":"t3", "id":"legacy-ok"}, item
'
replay_file="$DATA_DIR/replay-response.json"
status="$(curl -sS -o "$replay_file" -w '%{http_code}' -X POST \
  "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPELINE/$legacy_ok_id/replay")"
test "$status" = "200"
grep '"replayed":1' "$replay_file" >/dev/null
wait_ch_value "SELECT count() FROM ${CH_DB}.canal_identity FINAL WHERE tenant_id='t3' AND id='legacy-ok' AND amount=50" 1

echo "==> Reject partial composite key, legacy key change, and unknown contract"
status="$(curl -sS -o "$repair_file" -w '%{http_code}' -X PUT \
  -H 'Content-Type: application/json' \
  --data '{"primary_key_columns":["tenant_id","id"]}' \
  "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPELINE/$partial_id/identity")"
test "$status" = "409"
grep 'data_key_component_missing' "$repair_file" >/dev/null
grep '"replay_state":"quarantined"' "$repair_file" >/dev/null

status="$(curl -sS -o "$repair_file" -w '%{http_code}' -X PUT \
  -H 'Content-Type: application/json' \
  --data '{"primary_key_columns":["tenant_id","id"]}' \
  "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPELINE/$keychange_id/identity")"
test "$status" = "409"
grep 'legacy_key_change_unsupported' "$repair_file" >/dev/null
grep '"replay_state":"quarantined"' "$repair_file" >/dev/null

status="$(curl -sS -o "$replay_file" -w '%{http_code}' -X POST \
  "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPELINE/$unknown_id/replay")"
test "$status" = "409"
grep 'format_contract_unknown' "$replay_file" >/dev/null
grep '"replay_state":"quarantined"' "$replay_file" >/dev/null

dlq_body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/dlq/$PIPELINE?limit=20")"
printf '%s' "$dlq_body" | python3 -c '
import json, sys
items = json.load(sys.stdin)["items"]
assert len(items) == 3, items
assert all(x["identity_context"]["replay_state"] == "quarantined" for x in items), items
assert not any((x["record"].get("data") or {}).get("id") == "legacy-ok" for x in items), items
'

echo "Kafka Canal identity E2E passed"
