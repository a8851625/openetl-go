#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

IMAGE="openetl-go-etl:dev"
APP_CONTAINER="etl-openetl-go-s3"

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 60 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 1
  done
  return 1
}

echo "==> Build image"
docker build -t "$IMAGE" -f Dockerfile .

echo "==> Start MinIO"
docker compose -f docker-compose.dev.yml up -d minio

echo "==> Wait MinIO"
wait_http "http://127.0.0.1:9001/minio/health/live"

echo "==> Reset MinIO bucket"
docker run --rm --network host --entrypoint /bin/sh quay.io/minio/mc:latest -c "mc alias set local http://127.0.0.1:9001 minio minio123 >/dev/null && mc rb --force local/etl-bucket >/dev/null 2>&1 || true"

echo "==> Reset ETL data"
rm -rf data-s3
mkdir -p data-s3/output data-s3/checkpoint data-s3/dlq logs
chmod -R a+rwX data-s3
chmod a+rwX logs

echo "==> Run S3 pipeline"
docker rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
docker run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p 8007:8001 \
  -v "$ROOT_DIR/testdata/pipes-s3:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-s3:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8007/api/v2/health"

echo "==> Wait pipeline complete"
i=0
while [ "$i" -lt 60 ]; do
  body="$(curl -fsS http://127.0.0.1:8007/api/v2/pipelines)"
  echo "$body" | grep '"name":"file-to-s3"' | grep '"records_written":3' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done
body="$(curl -fsS http://127.0.0.1:8007/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"records_written":3'

echo "==> Verify S3 object"
docker run --rm --network host --entrypoint /bin/sh quay.io/minio/mc:latest -c "mc alias set local http://127.0.0.1:9001 minio minio123 >/dev/null && object=\$(mc find local/etl-bucket/s3-e2e --name '*.jsonl' | head -n 1) && test -n \"\$object\" && mc cat \"\$object\"" > data-s3/object.jsonl
grep 'Ada' data-s3/object.jsonl
grep 'Grace' data-s3/object.jsonl
grep 'Hopper' data-s3/object.jsonl

echo "S3 MinIO E2E passed"
