#!/bin/sh
set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
ES_CONTAINER="etl-opensearch"
APP_CONTAINER="etl-openetl-go-es"
ES_PORT=9200
ES_IMAGE="docker.io/opensearchproject/opensearch:2.15.0"
PIPELINE="mysql-to-elasticsearch"
INDEX="customers"

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 120 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 2
  done
  return 1
}

if [ "${E2E_SKIP_BUILD:-0}" = "1" ]; then
  echo "==> Skip image build (E2E_SKIP_BUILD=1, using $IMAGE)"
else
  echo "==> Build image (or use cache)"
  "$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile . 2>&1 | tail -1
fi

echo "==> Start OpenSearch"
"$CONTAINER_CLI" rm -f "$ES_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$ES_CONTAINER" \
  -e "discovery.type=single-node" \
  -e "DISABLE_SECURITY_PLUGIN=true" \
  -e "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m" \
  -p "$ES_PORT:9200" \
  "$ES_IMAGE"

echo "==> Wait OpenSearch ready"
if ! wait_http "http://127.0.0.1:$ES_PORT/_cluster/health"; then
  echo "OpenSearch failed to start"
  "$CONTAINER_CLI" rm -f "$ES_CONTAINER" >/dev/null 2>&1 || true
  exit 1
fi
echo "==> OpenSearch is ready"

echo "==> Prepare OpenSearch mapping"
curl -fsS -X DELETE "http://127.0.0.1:$ES_PORT/$INDEX" >/dev/null 2>&1 || true
curl -fsS -X PUT "http://127.0.0.1:$ES_PORT/$INDEX" \
  -H 'Content-Type: application/json' \
  -d '{
    "mappings": {
      "properties": {
        "id": {"type": "long"},
        "name": {"type": "keyword"},
        "email": {"type": "keyword"},
        "phone": {"type": "long"},
        "status": {"type": "keyword"},
        "amount": {"type": "double"}
      }
    }
  }' >/dev/null

echo "==> Start MySQL source"
compose -f docker-compose.dev.yml up -d mysql-source

echo "==> Wait MySQL healthy"
i=0
while [ "$i" -lt 60 ]; do
  status="$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' etl-mysql-source 2>/dev/null || true)"
  if [ "$status" = "healthy" ]; then break; fi
  i=$((i + 1)); sleep 2
done
[ "$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' etl-mysql-source)" = "healthy" ]

echo "==> Prepare source data"
"$CONTAINER_CLI" exec etl-mysql-source mysql -uroot -proot123456 -e "
DELETE FROM dzh3136_go.customers WHERE id >= 9400;
INSERT INTO dzh3136_go.customers (id, name, email, phone, status, amount) VALUES
  (9401, 'ES Alice', 'es-alice@example.com', '13900009401', 'active', 100.00),
  (9402, 'ES Bob', 'es-bob@example.com', '13900009402', 'active', 200.00),
  (9403, 'ES Conflict', 'es-conflict@example.com', 'not-a-number', 'inactive', 300.00);
"

echo "==> Reset ETL data"
rm -rf data-es
mkdir -p data-es/output data-es/checkpoint data-es/dlq logs
chmod -R a+rwX data-es
chmod a+rwX logs

echo "==> Run ETL pipeline"
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p 8018:8001 \
  -v "$ROOT_DIR/testdata/pipes-es:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-es:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8018/api/v2/health"

echo "==> Wait for pipeline to complete"
i=0
while [ "$i" -lt 60 ]; do
  status="$(curl -fsS "http://127.0.0.1:8018/api/v2/pipelines/$PIPELINE" 2>/dev/null | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4 || true)"
  if [ "$status" = "completed" ] || [ "$status" = "stopped" ]; then break; fi
  i=$((i + 1)); sleep 2
done

echo "==> Verify successful bulk items in OpenSearch"
i=0
count=0
while [ "$i" -lt 30 ]; do
  count="$(curl -fsS "http://127.0.0.1:$ES_PORT/$INDEX/_count" 2>/dev/null | grep -o '"count":[0-9]*' | cut -d: -f2 || echo 0)"
  if [ "$count" = "2" ]; then break; fi
  i=$((i + 1)); sleep 2
done
echo "==> ES document count: $count"
test "$count" = "2"

echo "==> Verify specific document"
doc="$(curl -fsS "http://127.0.0.1:$ES_PORT/$INDEX/_doc/9401" 2>/dev/null)"
echo "$doc" | grep '"found":true'
echo "$doc" | grep 'ES Alice'
missing="$(curl -sS "http://127.0.0.1:$ES_PORT/$INDEX/_doc/9403" 2>/dev/null || true)"
echo "$missing" | grep '"found":false'

echo "==> Verify mapping conflict item is in DLQ"
i=0
dlq_body=""
while [ "$i" -lt 60 ]; do
  dlq_body="$(curl -fsS "http://127.0.0.1:8018/api/v2/dlq/$PIPELINE?contains=9403&limit=10" 2>/dev/null || true)"
  if echo "$dlq_body" | grep -q 'mapper_parsing_exception'; then
    break
  fi
  i=$((i + 1)); sleep 1
done
echo "$dlq_body"
echo "$dlq_body" | grep '9403'
echo "$dlq_body" | grep 'mapper_parsing_exception'
echo "$dlq_body" | grep '"error_class":"schema"'
dlq_id="$(echo "$dlq_body" | grep -o '"id":[0-9][0-9]*' | head -n1 | sed 's/[^0-9]//g')"
test "$dlq_id" != ""

echo "==> Repair mapping and replay DLQ"
curl -fsS -X DELETE "http://127.0.0.1:$ES_PORT/$INDEX" >/dev/null
curl -fsS -X PUT "http://127.0.0.1:$ES_PORT/$INDEX" \
  -H 'Content-Type: application/json' \
  -d '{
    "mappings": {
      "properties": {
        "id": {"type": "long"},
        "name": {"type": "keyword"},
        "email": {"type": "keyword"},
        "phone": {"type": "keyword"},
        "status": {"type": "keyword"},
        "amount": {"type": "double"}
      }
    }
  }' >/dev/null
replay_body="$(curl -fsS -X POST "http://127.0.0.1:8018/api/v2/dlq/$PIPELINE/$dlq_id/replay")"
echo "$replay_body"
echo "$replay_body" | grep '"replayed":1'

i=0
while [ "$i" -lt 30 ]; do
  replayed="$(curl -fsS "http://127.0.0.1:$ES_PORT/$INDEX/_doc/9403" 2>/dev/null || true)"
  if echo "$replayed" | grep -q '"found":true'; then
    break
  fi
  i=$((i + 1)); sleep 1
done
echo "$replayed" | grep '"found":true'
echo "$replayed" | grep 'ES Conflict'
dlq_after="$(curl -fsS "http://127.0.0.1:8018/api/v2/dlq/$PIPELINE?contains=9403&limit=10")"
echo "$dlq_after"
if echo "$dlq_after" | grep -q "\"id\":${dlq_id}"; then
  echo "replayed DLQ id ${dlq_id} was not deleted" >&2
  exit 1
fi

body="$(curl -fsS http://127.0.0.1:8018/api/v2/pipelines)"
echo "$body"
echo "$body" | grep "\"name\":\"$PIPELINE\""

"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" rm -f "$ES_CONTAINER" >/dev/null 2>&1 || true

echo "Elasticsearch mapping conflict E2E passed"
