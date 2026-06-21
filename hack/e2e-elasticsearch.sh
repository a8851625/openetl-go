#!/bin/sh
set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

IMAGE="openetl-go-etl:dev"
ES_CONTAINER="etl-opensearch"
APP_CONTAINER="etl-openetl-go-es"
ES_PORT=9200
ES_IMAGE="docker.io/opensearchproject/opensearch:2.15.0"

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 120 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 2
  done
  return 1
}

echo "==> Build image (or use cache)"
podman build -t "$IMAGE" -f Dockerfile . 2>&1 | tail -1

echo "==> Start OpenSearch"
podman rm -f "$ES_CONTAINER" >/dev/null 2>&1 || true
podman run -d \
  --name "$ES_CONTAINER" \
  -e "discovery.type=single-node" \
  -e "DISABLE_SECURITY_PLUGIN=true" \
  -e "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m" \
  -p "$ES_PORT:9200" \
  "$ES_IMAGE"

echo "==> Wait OpenSearch ready"
if ! wait_http "http://127.0.0.1:$ES_PORT/_cluster/health"; then
  echo "OpenSearch failed to start"
  podman rm -f "$ES_CONTAINER" >/dev/null 2>&1 || true
  exit 1
fi
echo "==> OpenSearch is ready"

echo "==> Start MySQL source"
podman-compose -f docker-compose.dev.yml up -d mysql-source

echo "==> Wait MySQL healthy"
i=0
while [ "$i" -lt 60 ]; do
  status="$(podman inspect -f '{{.State.Health.Status}}' etl-mysql-source 2>/dev/null || true)"
  if [ "$status" = "healthy" ]; then break; fi
  i=$((i + 1)); sleep 2
done
[ "$(podman inspect -f '{{.State.Health.Status}}' etl-mysql-source)" = "healthy" ]

echo "==> Prepare source data"
podman exec etl-mysql-source mysql -uroot -proot123456 -e "
DELETE FROM dzh3136_go.customers WHERE id >= 9400;
INSERT INTO dzh3136_go.customers (id, name, email, phone, status, amount) VALUES
  (9401, 'ES Alice', 'es-alice@example.com', '13900009401', 'active', 100.00),
  (9402, 'ES Bob', 'es-bob@example.com', '13900009402', 'active', 200.00),
  (9403, 'ES Charlie', 'es-charlie@example.com', '13900009403', 'inactive', 300.00);
"

echo "==> Reset ETL data"
rm -rf data-es
mkdir -p data-es/output data-es/checkpoint data-es/dlq logs

echo "==> Write ES pipeline spec"
mkdir -p testdata/pipes-es
cat > testdata/pipes-es/mysql-to-es.yaml <<'SPEC'
name: "mysql-to-elasticsearch"
source:
  type: mysql_batch
  config:
    host: "host.containers.internal"
    port: 13306
    user: "sync_user"
    password: "sync_password_123"
    database: "dzh3136_go"
    table: "customers"
    pk_column: "id"
    limit: 100

transforms:
  - type: identity
    config: {}

sink:
  type: elasticsearch
  config:
    hosts:
      - "http://host.containers.internal:9200"
    index: "customers"
    id_column: "id"

batch_size: 100
checkpoint_interval_sec: 5
backpressure_buffer: 100
SPEC

echo "==> Run ETL pipeline"
podman rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
podman run -d \
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
  status="$(curl -fsS http://127.0.0.1:8018/api/v2/pipelines/mysql-to-elasticsearch 2>/dev/null | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4 || true)"
  if [ "$status" = "completed" ] || [ "$status" = "stopped" ]; then break; fi
  i=$((i + 1)); sleep 2
done

echo "==> Verify data in OpenSearch"
i=0
count=0
while [ "$i" -lt 30 ]; do
  count="$(curl -fsS "http://127.0.0.1:$ES_PORT/customers/_count" 2>/dev/null | grep -o '"count":[0-9]*' | cut -d: -f2 || echo 0)"
  if [ "$count" -ge 3 ]; then break; fi
  i=$((i + 1)); sleep 2
done
echo "==> ES document count: $count"
test "$count" -ge 3

echo "==> Verify specific document"
doc="$(curl -fsS "http://127.0.0.1:$ES_PORT/customers/_doc/9401" 2>/dev/null)"
echo "$doc" | grep '"found":true'
echo "$doc" | grep 'ES Alice'

body="$(curl -fsS http://127.0.0.1:8018/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"name":"mysql-to-elasticsearch"'

podman rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
podman rm -f "$ES_CONTAINER" >/dev/null 2>&1 || true

echo "Elasticsearch E2E passed"
