#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
APP_CONTAINER="etl-openetl-go-s3"
PIPELINE="file-to-s3"

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 60 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 1
  done
  return 1
}

wait_http_down() {
  url="$1"
  i=0
  while [ "$i" -lt 30 ]; do
    if ! curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 1
  done
  return 1
}

wait_pipeline_dlq() {
  name="$1"
  expected="$2"
  i=0
  while [ "$i" -lt 90 ]; do
    body="$(curl -fsS http://127.0.0.1:8007/api/v2/pipelines)"
    echo "$body" | grep "\"name\":\"$name\"" | grep "\"records_dlq\":$expected" >/dev/null 2>&1 && return 0
    i=$((i + 1)); sleep 1
  done
  echo "$body"
  return 1
}

finished_count() {
  "$CONTAINER_CLI" logs "$APP_CONTAINER" 2>&1 | grep -c "Pipeline finished. written=3 read=3 failed=0" || true
}

if [ "${E2E_SKIP_BUILD:-0}" = "1" ]; then
  echo "==> Skip image build (E2E_SKIP_BUILD=1, using $IMAGE)"
else
  echo "==> Build image"
  "$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile .
fi

echo "==> Start MinIO"
compose -f docker-compose.dev.yml up -d minio

echo "==> Wait MinIO"
wait_http "http://127.0.0.1:9001/minio/health/live"

echo "==> Reset MinIO bucket"
"$CONTAINER_CLI" run --rm --network host --entrypoint /bin/sh quay.io/minio/mc:latest -c "mc alias set local http://127.0.0.1:9001 minio minio123 >/dev/null && mc rb --force local/etl-bucket >/dev/null 2>&1 || true"

echo "==> Reset ETL data"
rm -rf data-s3
mkdir -p data-s3/output data-s3/checkpoint data-s3/dlq logs
chmod -R a+rwX data-s3
chmod a+rwX logs

echo "==> Run S3 pipeline"
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d \
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
  echo "$body" | grep "\"name\":\"$PIPELINE\"" | grep '"records_written":3' >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done
body="$(curl -fsS http://127.0.0.1:8007/api/v2/pipelines)"
echo "$body"
echo "$body" | grep '"records_written":3'

echo "==> Verify S3 object"
first_object="$("$CONTAINER_CLI" run --rm --network host --entrypoint /bin/sh quay.io/minio/mc:latest -c "mc alias set local http://127.0.0.1:9001 minio minio123 >/dev/null && mc find local/etl-bucket/s3-e2e --name '*.jsonl' | sort | head -n 1")"
test "$first_object" != ""
object_count="$("$CONTAINER_CLI" run --rm --network host --entrypoint /bin/sh quay.io/minio/mc:latest -c "mc alias set local http://127.0.0.1:9001 minio minio123 >/dev/null && mc find local/etl-bucket/s3-e2e --name '*.jsonl' 2>/dev/null | wc -l" | tr -d '[:space:]')"
test "$object_count" = "1"
"$CONTAINER_CLI" run --rm --network host --entrypoint /bin/sh quay.io/minio/mc:latest -c "mc alias set local http://127.0.0.1:9001 minio minio123 >/dev/null && mc cat \"$first_object\"" > data-s3/object.jsonl
grep 'Ada' data-s3/object.jsonl
grep 'Grace' data-s3/object.jsonl
grep 'Hopper' data-s3/object.jsonl
first_run_count="$(finished_count)"
test "$first_run_count" -ge 1

echo "==> Verify checkpoint reset replay overwrites the same deterministic object"
curl -fsS -X POST "http://127.0.0.1:8007/api/v2/pipelines/$PIPELINE/checkpoint/reset" >/dev/null
curl -fsS -X POST "http://127.0.0.1:8007/api/v2/pipelines/$PIPELINE/start" >/dev/null

i=0
replay_run_count="$first_run_count"
while [ "$i" -lt 60 ]; do
  replay_run_count="$(finished_count)"
  [ "$replay_run_count" -gt "$first_run_count" ] && break
  i=$((i + 1)); sleep 1
done
test "$replay_run_count" -gt "$first_run_count"
body="$(curl -fsS http://127.0.0.1:8007/api/v2/pipelines)"
echo "$body"
echo "$body" | grep "\"name\":\"$PIPELINE\""

second_object="$("$CONTAINER_CLI" run --rm --network host --entrypoint /bin/sh quay.io/minio/mc:latest -c "mc alias set local http://127.0.0.1:9001 minio minio123 >/dev/null && mc find local/etl-bucket/s3-e2e --name '*.jsonl' | sort | head -n 1")"
test "$second_object" = "$first_object"
object_count="$("$CONTAINER_CLI" run --rm --network host --entrypoint /bin/sh quay.io/minio/mc:latest -c "mc alias set local http://127.0.0.1:9001 minio minio123 >/dev/null && mc find local/etl-bucket/s3-e2e --name '*.jsonl' 2>/dev/null | wc -l" | tr -d '[:space:]')"
test "$object_count" = "1"

echo "==> Verify MinIO outage routes records to DLQ and replay recovers"
OUTAGE_PIPELINE="file-to-s3-outage"
OUTAGE_DIR="$ROOT_DIR/data-s3-outage"
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run --rm --network host --entrypoint /bin/sh quay.io/minio/mc:latest -c "mc alias set local http://127.0.0.1:9001 minio minio123 >/dev/null && mc rb --force local/etl-bucket >/dev/null 2>&1 || true"
rm -rf "$OUTAGE_DIR"
mkdir -p "$OUTAGE_DIR/pipes" "$OUTAGE_DIR/checkpoint" "$OUTAGE_DIR/dlq" "$OUTAGE_DIR/output"
chmod -R a+rwX "$OUTAGE_DIR"
cat >"$OUTAGE_DIR/input.jsonl" <<'EOF'
{"id":1,"first":"Ada","last":"Lovelace","status":"active"}
{"id":2,"first":"Alan","last":"Turing","status":"active"}
{"id":3,"first":"Grace","last":"Hopper","status":"deleted"}
{"id":4,"first":"Katherine","last":"Johnson","status":"active"}
{"id":5,"first":"Dorothy","last":"Vaughan","status":"active"}
EOF
cat >"$OUTAGE_DIR/pipes/$OUTAGE_PIPELINE.yaml" <<EOF
name: "$OUTAGE_PIPELINE"
source:
  type: file
  config:
    path: "/app/data/input.jsonl"
    format: "json"

transforms:
  - type: rate_limiter
    config:
      rps: 1
      burst: 1

sink:
  type: s3
  config:
    endpoint: "http://host.docker.internal:9001"
    region: "us-east-1"
    bucket: "etl-bucket"
    access_key: "minio"
    secret_key: "minio123"
    format: "jsonl"
    prefix: "s3-outage/"
    max_retries: 0
    retry_base_ms: 50

batch_size: 5
checkpoint_interval_sec: 1
backpressure_buffer: 10
retry:
  max_attempts: 1
  initial_interval_ms: 50
  max_interval_ms: 50
dlq:
  enable: true
EOF

"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p 8007:8001 \
  -v "$OUTAGE_DIR/pipes:/app/pipes:ro" \
  -v "$OUTAGE_DIR:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE" >/dev/null

wait_http "http://127.0.0.1:8007/api/v2/health"
compose -f docker-compose.dev.yml stop -t 0 minio >/dev/null
wait_http_down "http://127.0.0.1:9001/minio/health/live"
wait_pipeline_dlq "$OUTAGE_PIPELINE" 5
dlq_body="$(curl -fsS "http://127.0.0.1:8007/api/v2/dlq/$OUTAGE_PIPELINE?contains=Ada&limit=10")"
echo "$dlq_body" | grep '"error_class":"transient"'
echo "$dlq_body" | grep -E 'connection refused|s3 upload'

compose -f docker-compose.dev.yml up -d minio >/dev/null
wait_http "http://127.0.0.1:9001/minio/health/live"
replay_body="$(curl -fsS -X POST "http://127.0.0.1:8007/api/v2/dlq/$OUTAGE_PIPELINE/replay")"
echo "$replay_body"
echo "$replay_body" | grep '"replayed":5'

object_count="$("$CONTAINER_CLI" run --rm --network host --entrypoint /bin/sh quay.io/minio/mc:latest -c "mc alias set local http://127.0.0.1:9001 minio minio123 >/dev/null && mc find local/etl-bucket/s3-e2e --name '*.jsonl' 2>/dev/null | wc -l" | tr -d '[:space:]')"
test "$object_count" = "0"
object_count="$("$CONTAINER_CLI" run --rm --network host --entrypoint /bin/sh quay.io/minio/mc:latest -c "mc alias set local http://127.0.0.1:9001 minio minio123 >/dev/null && mc find local/etl-bucket/s3-outage --name '*.jsonl' | wc -l" | tr -d '[:space:]')"
test "$object_count" = "5"
"$CONTAINER_CLI" run --rm --network host --entrypoint /bin/sh quay.io/minio/mc:latest -c "mc alias set local http://127.0.0.1:9001 minio minio123 >/dev/null && for obj in \$(mc find local/etl-bucket/s3-outage --name '*.jsonl' | sort); do mc cat \"\$obj\"; done" > "$OUTAGE_DIR/outage-replay.jsonl"
grep 'Ada' "$OUTAGE_DIR/outage-replay.jsonl"
grep 'Hopper' "$OUTAGE_DIR/outage-replay.jsonl"
grep 'Vaughan' "$OUTAGE_DIR/outage-replay.jsonl"
dlq_after="$(curl -fsS "http://127.0.0.1:8007/api/v2/dlq/$OUTAGE_PIPELINE?limit=10")"
if ! echo "$dlq_after" | grep '"items":\[\]' >/dev/null; then
	echo "S3 DLQ records were not deleted after replay" >&2
	exit 1
fi

echo "S3 MinIO E2E passed"
