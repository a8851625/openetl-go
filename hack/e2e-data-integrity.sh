#!/bin/sh
# End-to-end verification for the data-integrity features:
#
#   1. data reconciliation   POST /api/v2/pipelines/{id}/reconcile
#   2. object commit manifest (s3 / file_sink `manifest: true`)
#   3. targeted single-table resync  POST /api/v2/pipelines/{id}/resync
#   4. keyless-table snapshot        source.config.no_pk_snapshot: hash
#
# Run with:  CONTAINER_CLI=podman sh hack/e2e-data-integrity.sh
# Skip the image build with E2E_SKIP_BUILD=1.

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="${IMAGE:-openetl-go-etl:dev}"
MYSQL_CONTAINER="etl-mysql-source"
APP_CONTAINER="etl-openetl-go-integrity"
APP_PORT="${APP_PORT:-8005}"
BASE_URL="http://127.0.0.1:$APP_PORT"
DATA_DIR="$ROOT_DIR/data-dataintegrity"
PIPES_DIR="$ROOT_DIR/testdata/pipes-data-integrity"

pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1"; exit 1; }
check() { if [ "$1" = "0" ]; then pass "$2"; else fail "$2"; fi; }

mysql_exec() {
  "$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -N -B -e "$1"
}

wait_http() {
  i=0
  while [ "$i" -lt 90 ]; do
    if curl -fsS "$1" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

# wait_for_sql <expected> <sql> <label>
wait_for_sql() {
  expected="$1"
  sql="$2"
  label="$3"
  i=0
  while [ "$i" -lt 90 ]; do
    got="$(mysql_exec "$sql" 2>/dev/null || echo "ERR")"
    if [ "$got" = "$expected" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  echo "  last value for $label: ${got:-<empty>} (want $expected)"
  return 1
}

cleanup() {
  if [ "${E2E_KEEP:-0}" = "1" ]; then
    echo "==> Keeping $APP_CONTAINER for debugging (E2E_KEEP=1); inspect with: $CONTAINER_CLI logs $APP_CONTAINER"
    return 0
  fi
  "$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [ "${E2E_SKIP_BUILD:-0}" = "1" ]; then
  echo "==> Skip image build (E2E_SKIP_BUILD=1, using $IMAGE)"
else
  echo "==> Build image"
  "$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile .
fi

echo "==> Start MySQL source and MinIO"
compose -f docker-compose.dev.yml up -d mysql-source minio
i=0
while [ "$i" -lt 90 ]; do
  status="$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' etl-minio 2>/dev/null || true)"
  [ "$status" = "healthy" ] && break
  i=$((i + 1))
  sleep 2
done
i=0
while [ "$i" -lt 90 ]; do
  status="$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
  [ "$status" = "healthy" ] && break
  i=$((i + 1))
  sleep 2
done
[ "$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER")" = "healthy" ]

echo "==> Prepare source and target databases"
mysql_exec "DROP DATABASE IF EXISTS integrity_src; DROP DATABASE IF EXISTS integrity_dw;"
mysql_exec "CREATE DATABASE integrity_src; CREATE DATABASE integrity_dw;
GRANT ALL PRIVILEGES ON integrity_src.* TO 'sync_user'@'%';
GRANT ALL PRIVILEGES ON integrity_dw.* TO 'sync_user'@'%';
FLUSH PRIVILEGES;"
# orders has a primary key; audit_log deliberately has none and contains one
# duplicated row so the row-hash collapse is observable.
mysql_exec "CREATE TABLE integrity_src.orders (id BIGINT PRIMARY KEY, amount DECIMAL(10,2) NOT NULL, note VARCHAR(64));
INSERT INTO integrity_src.orders VALUES (1,10.50,'a'),(2,20.00,'b'),(3,30.25,'c'),(4,40.00,'d'),(5,50.75,'e');
CREATE TABLE integrity_src.audit_log (actor VARCHAR(32), action VARCHAR(32), at_ts VARCHAR(32));
INSERT INTO integrity_src.audit_log VALUES ('alice','login','t1'),('bob','logout','t2'),('alice','login','t1'),('carol','login','t3');"

echo "==> Reset the S3 verification prefix"
mc_run() {
  "$CONTAINER_CLI" run --rm --network host --entrypoint /bin/sh quay.io/minio/mc:latest \
    -c "mc alias set local http://127.0.0.1:9001 minio minio123 >/dev/null && $1"
}
mc_run "mc rb --force local/etl-bucket/s3-integrity >/dev/null 2>&1 || true"

# Dimension table for the incremental lookup scenario. updated_at starts in the
# past so the first load establishes a watermark and later updates are "newer".
mysql_exec "CREATE TABLE integrity_src.dim_users (id BIGINT PRIMARY KEY, name VARCHAR(32), region VARCHAR(32), updated_at TIMESTAMP(6) NOT NULL, is_deleted TINYINT NOT NULL DEFAULT 0);
INSERT INTO integrity_src.dim_users VALUES
  (0,'alice-old','east','2026-01-01 00:00:00.000000',0),
  (1,'bob','east','2026-01-01 00:00:00.000000',0),
  (2,'carol','west','2026-01-01 00:00:00.000000',0);"

echo "==> Reset ETL data dirs"
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR/output" "$DATA_DIR/checkpoint" "$DATA_DIR/dlq"
chmod -R a+rwX "$DATA_DIR"

echo "==> Start OpenETL with the data-integrity pipelines"
cleanup
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p "$APP_PORT:8001" \
  -v "$PIPES_DIR:/app/pipes:ro" \
  -v "$DATA_DIR:/app/data" \
  "$IMAGE" >/dev/null
wait_http "$BASE_URL/api/v2/health" || fail "OpenETL did not become healthy"

echo
echo "==> 4. Keyless-table snapshot (no_pk_snapshot: hash)"
wait_for_sql "5" "SELECT COUNT(*) FROM integrity_dw.orders" "dw.orders snapshot" \
  || fail "snapshot of keyed table did not land"
pass "keyed table snapshotted (5 rows)"
# 4 source rows, one duplicated -> 3 distinct _row_hash keys.
wait_for_sql "3" "SELECT COUNT(*) FROM integrity_dw.audit_log" "dw.audit_log row-hash snapshot" \
  || fail "keyless table was not snapshotted with row hashes"
pass "keyless table snapshotted by content hash (4 rows -> 3 distinct keys)"
hash_col="$(mysql_exec "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='integrity_dw' AND table_name='audit_log' AND column_name='_row_hash'")"
[ "$hash_col" = "1" ] || fail "target table is missing the synthetic _row_hash column"
pass "synthetic _row_hash column created on the target"
row_hash_len="$(mysql_exec "SELECT MIN(CHAR_LENGTH(_row_hash)) FROM integrity_dw.audit_log")"
[ "$row_hash_len" = "64" ] || fail "row hash is not a 64-char sha256 (got $row_hash_len)"
pass "_row_hash holds a 64-char sha256"

echo
echo "==> 1. Data reconciliation"
rec="$(curl -fsS -X POST -H 'Content-Type: application/json' \
  -d '{"tables":["orders","audit_log"],"buckets":4}' \
  "$BASE_URL/api/v2/pipelines/integrity-mysql/reconcile")"
echo "$rec"
echo "$rec" | grep '"tables":2' >/dev/null || fail "reconcile did not compare both tables"
echo "$rec" | grep '"limits"' >/dev/null || fail "reconcile report must carry its limits"
# orders must match exactly...
echo "$rec" | grep '"source_table":"orders","target_table":"orders","verdict":"match"' >/dev/null \
  || fail "reconcile did not report match for the keyed table"
# ...while the row-hash table legitimately shows the duplicate-collapse delta and
# says so instead of pretending the data is missing.
echo "$rec" | grep '"source_table":"audit_log"' | grep '"verdict":"count_mismatch"' >/dev/null \
  || fail "row-hash table must be reported as count_mismatch (source 4 / target 3)"
echo "$rec" | grep '重复行折叠' >/dev/null || fail "row-hash collapse must be explained in the report detail"
pass "reconcile: keyed table matches; row-hash table explains the duplicate collapse"

caps="$(curl -fsS "$BASE_URL/api/v2/pipelines/integrity-mysql/reconcile")"
echo "$caps" | grep '"sinks"' >/dev/null || fail "GET reconcile must document the supported matrix"
pass "GET reconcile documents the supported matrix"

echo "==> 1b. Reconciliation detects target drift"
mysql_exec "DELETE FROM integrity_dw.orders WHERE id = 3;"
drift="$(curl -fsS -X POST -H 'Content-Type: application/json' -d '{"tables":["orders"]}' \
  "$BASE_URL/api/v2/pipelines/integrity-mysql/reconcile")"
echo "$drift"
echo "$drift" | grep '"verdict":"count_mismatch"' >/dev/null || fail "drift was not detected as count_mismatch"
echo "$drift" | grep '"row_delta":1' >/dev/null || fail "row_delta must be 1 (source ahead of target)"
echo "$drift" | grep '"bucket_mismatches"' >/dev/null || fail "bucket localisation did not run for a numeric key"
pass "drift detected (count_mismatch, row_delta=1, bucket localised)"

echo
echo "==> 3. Targeted single-table resync"
status="$(curl -fsS "$BASE_URL/api/v2/pipelines/integrity-mysql" | sed -n 's/.*"status":"\([a-z_]*\)".*/\1/p')"
[ "$status" = "running" ] || fail "pipeline must be running before the resync checks (status=$status); see $CONTAINER_CLI logs $APP_CONTAINER"
pass "pipeline is running with a healthy checkpoint (no blocked-checkpoint errors)"
# Running pipeline must refuse the rewrite.
code="$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
  -d '{"tables":["orders"]}' "$BASE_URL/api/v2/pipelines/integrity-mysql/resync")"
[ "$code" = "409" ] || fail "resync on a running pipeline must return 409 (got $code)"
pass "resync refuses a running pipeline (409)"

# Unknown table names are rejected, not silently ignored.
code="$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
  -d '{"tables":["not_a_table"]}' "$BASE_URL/api/v2/pipelines/integrity-mysql/resync")"
[ "$code" = "400" ] || fail "resync of an unknown table must return 400 (got $code)"
pass "resync rejects unknown table names (400)"

curl -fsS -X POST "$BASE_URL/api/v2/pipelines/integrity-mysql/stop" >/dev/null
i=0
while [ "$i" -lt 60 ]; do
  status="$(curl -fsS "$BASE_URL/api/v2/pipelines/integrity-mysql" | sed -n 's/.*"status":"\([a-z_]*\)".*/\1/p')"
  [ "$status" = "stopped" ] && break
  i=$((i + 1))
  sleep 1
done
resync="$(curl -fsS -X POST -H 'Content-Type: application/json' \
  -d '{"tables":["orders"]}' "$BASE_URL/api/v2/pipelines/integrity-mysql/resync")"
echo "$resync"
echo "$resync" | grep '"status":"resync_scheduled"' >/dev/null || fail "resync did not schedule"
echo "$resync" | grep '"phase":"snapshot"' >/dev/null || fail "resync details must show the snapshot phase"
echo "$resync" | grep '"sink_requirement"' >/dev/null || fail "resync must explain the sink requirement"
pass "resync scheduled for the quiescent pipeline (phase=snapshot)"

curl -fsS -X POST "$BASE_URL/api/v2/pipelines/integrity-mysql/start" >/dev/null
# The re-snapshot must restore the deleted row without touching the other table.
wait_for_sql "5" "SELECT COUNT(*) FROM integrity_dw.orders" "dw.orders after resync" \
  || fail "targeted resync did not restore the drifted table"
pass "targeted resync restored the drifted table (5 rows)"
wait_for_sql "3" "SELECT COUNT(*) FROM integrity_dw.audit_log" "dw.audit_log after resync" \
  || fail "keyless table gained duplicate rows during resync"
pass "keyless table stayed idempotent across the resync (still 3 keys)"

after="$(curl -fsS -X POST -H 'Content-Type: application/json' -d '{"tables":["orders"]}' \
  "$BASE_URL/api/v2/pipelines/integrity-mysql/reconcile")"
echo "$after"
echo "$after" | grep '"verdict":"match"' >/dev/null || fail "reconcile still reports drift after the resync"
echo "$after" | grep '"row_delta":0' >/dev/null || fail "row_delta must return to 0 after the resync"
pass "reconcile returns to match (row_delta=0) after the resync"

echo
echo "==> 2. Object commit manifest (file_sink manifest: true)"
i=0
while [ "$i" -lt 60 ]; do
  n="$(find "$DATA_DIR/output/manifest-demo" -name '*.manifest.json' 2>/dev/null | wc -l | tr -d ' ')"
  [ "$n" != "0" ] && break
  i=$((i + 1))
  sleep 1
done
manifest="$(find "$DATA_DIR/output/manifest-demo" -name '*.manifest.json' 2>/dev/null | head -1)"
[ -n "$manifest" ] || fail "no manifest was written for the manifest-enabled file sink"
pass "manifest written: $(basename "$manifest")"
grep '"version": 1' "$manifest" >/dev/null || fail "manifest is missing its version"
grep '"objects"' "$manifest" >/dev/null || fail "manifest does not list data objects"
grep '"sha256"' "$manifest" >/dev/null || fail "manifest does not record the object checksum"
grep '"batch_seq"' "$manifest" >/dev/null || fail "manifest does not record the batch sequence"
pass "manifest records version, objects, checksum and batch sequence"

# Every committed object must exist on disk, and the manifest must not reference
# anything else.
committed_key="$(sed -n 's/.*"key": "\([^"]*\)".*/\1/p' "$manifest" | head -1)"
[ -n "$committed_key" ] || fail "manifest object key could not be parsed"
[ -f "$DATA_DIR/output/manifest-demo/$committed_key" ] || fail "committed object $committed_key is missing on disk"
pass "committed object exists on disk ($committed_key)"

# Crash-window leftover: an object written without a manifest is not committed.
orphan_dir="$DATA_DIR/output/manifest-demo/1999/01/01"
mkdir -p "$orphan_dir"
echo '{"id":999}' > "$orphan_dir/orphan.jsonl"
if grep -r 'orphan.jsonl' "$DATA_DIR/output/manifest-demo" --include='*.manifest.json' >/dev/null 2>&1; then
  fail "an unmanifested object must not appear in any manifest"
fi
pass "unmanifested object stays uncommitted (crash-window orphan)"

echo "==> Checkpoint records the manifest commit token"
ckpt="$(curl -fsS "$BASE_URL/api/v2/pipelines/manifest-file/checkpoint")"
echo "$ckpt" | grep 'manifest_key' >/dev/null || fail "checkpoint does not record the manifest key"
pass "checkpoint carries sink_commit.native.manifest_key"

echo
echo "==> 5. Incremental dimension refresh (lookup incremental_column)"
# Wait until the first full load has happened and records are flowing.
i=0
while [ "$i" -lt 60 ]; do
  produced="$(find "$DATA_DIR/output/lookup-incremental" -name '*.jsonl' 2>/dev/null | wc -l | tr -d ' ')"
  [ "$produced" != "0" ] && break
  i=$((i + 1))
  sleep 1
done
[ "${produced:-0}" != "0" ] || fail "lookup pipeline produced no output"
pass "lookup pipeline is enriching records (full load done)"

# Change one dimension row; the incremental refresh must pick it up without
# reloading the whole table.
mysql_exec "UPDATE integrity_src.dim_users SET name='alice-renamed', updated_at=NOW(6) WHERE id=0;"

# Wait for the demo source to finish (40 records * 250ms).
i=0
while [ "$i" -lt 90 ]; do
  status="$(curl -fsS "$BASE_URL/api/v2/pipelines/lookup-incremental" | sed -n 's/.*"status":"\([a-z_]*\)".*/\1/p')"
  [ "$status" = "completed" ] && break
  i=$((i + 1))
  sleep 1
done
all_out="$(find "$DATA_DIR/output/lookup-incremental" -name '*.jsonl' -exec cat {} \; 2>/dev/null)"
echo "$all_out" | grep 'alice-old' >/dev/null || fail "records emitted before the update must carry the original dimension value"
pass "pre-update records carry the original dimension value"
echo "$all_out" | grep 'alice-renamed' >/dev/null || fail "the incremental refresh did not merge the updated dimension row"
pass "post-update records carry the updated dimension value"

metrics="$(curl -fsS "$BASE_URL/api/v2/metrics")"
echo "$metrics" | grep '"incremental_merge"' >/dev/null || fail "lookup metrics do not expose incremental_merge"
merge_count="$(echo "$metrics" | tr ',' '\n' | sed -n 's/.*"incremental_merge":\([0-9]*\).*/\1/p' | head -1)"
[ "${merge_count:-0}" -ge 1 ] || fail "incremental refresh never ran (incremental_merge=$merge_count)"
pass "incremental refresh ran $merge_count time(s) instead of a full reload"

echo
echo "==> 2c. Compaction rewrites the committed view without deleting data"
compact_resp="$(curl -fsS -X POST -H 'Content-Type: application/json' -d '{"min_objects":2}' \
  "$BASE_URL/api/v2/pipelines/manifest-file/output/compact")"
echo "$compact_resp" | grep -q '"groups"' || fail "compaction response has no groups: $compact_resp"
merged_key="$(echo "$compact_resp" | sed -n 's/.*"merged_key":"\([^"]*\)".*/\1/p' | head -1)"
[ -n "$merged_key" ] || fail "compaction did not report a merged key: $compact_resp"
[ -f "$DATA_DIR/output/manifest-demo/$merged_key" ] || fail "merged object $merged_key is missing on disk"
pass "compaction merged a day of objects into $merged_key"

# Readers must now resolve exactly one committed copy of the data: the merged
# object, with the replaced keys retired but still present on disk.
python3 - "$DATA_DIR/output/manifest-demo" <<'PYEOF' || fail "compaction broke the committed view"
import json, os, sys, glob
root = sys.argv[1]
manifests = []
for path in glob.glob(os.path.join(root, "**", "*.manifest.json"), recursive=True):
    with open(path) as fh:
        manifests.append(json.load(fh))
manifests.sort(key=lambda m: m.get("created_at", ""))
active = {}
for m in manifests:
    for obj in m.get("objects", []):
        active[obj["key"]] = True
    for key in m.get("replaces", []) or []:
        active[key] = False
committed = [k for k, keep in active.items() if keep]
if len(committed) != 1:
    print(f"expected exactly 1 committed object after compaction, got {len(committed)}: {committed}")
    sys.exit(1)
for m in manifests:
    for key in m.get("replaces", []) or []:
        if not os.path.exists(os.path.join(root, key)):
            print(f"compaction deleted {key}; it must stay on disk")
            sys.exit(1)
print(committed[0])
PYEOF
pass "committed view is exactly one merged object; replaced objects kept on disk"

echo
echo "==> 2b. Object commit manifest on S3 (MinIO)"
i=0
while [ "$i" -lt 60 ]; do
  s3_manifest="$(mc_run "mc find local/etl-bucket/s3-integrity --name '*.manifest.json' | sort | head -n 1" 2>/dev/null || true)"
  [ -n "$s3_manifest" ] && break
  i=$((i + 1))
  sleep 1
done
[ -n "${s3_manifest:-}" ] || fail "no manifest object was written for the s3 sink"
pass "S3 manifest object written: $s3_manifest"
mc_run "mc cat '$s3_manifest'" > /tmp/s3-manifest.json
grep '"version": 1' /tmp/s3-manifest.json >/dev/null || fail "S3 manifest is missing its version"
grep '"sha256"' /tmp/s3-manifest.json >/dev/null || fail "S3 manifest does not record the object checksum"
grep '"batch_seq"' /tmp/s3-manifest.json >/dev/null || fail "S3 manifest does not record the batch sequence"
pass "S3 manifest records version, checksum and batch sequence"

s3_key="$(sed -n 's/.*"key": "\([^"]*\)".*/\1/p' /tmp/s3-manifest.json | head -1)"
[ -n "$s3_key" ] || fail "S3 manifest object key could not be parsed"
mc_run "mc stat 'local/etl-bucket/$s3_key' >/dev/null" || fail "committed S3 object $s3_key is missing"
pass "committed S3 data object exists in the bucket ($s3_key)"

# An object uploaded without a manifest must not become committed data.
"$CONTAINER_CLI" run --rm --network host --entrypoint /bin/sh quay.io/minio/mc:latest \
  -c 'mc alias set local http://127.0.0.1:9001 minio minio123 >/dev/null && printf "{\"id\":999}\n" | mc pipe local/etl-bucket/s3-integrity/1999/01/01/orphan.jsonl' >/dev/null
all_manifests="$(mc_run "for f in \$(mc find local/etl-bucket/s3-integrity --name '*.manifest.json'); do mc cat \"\$f\"; done" 2>/dev/null || true)"
if echo "$all_manifests" | grep 'orphan.jsonl' >/dev/null 2>&1; then
  fail "an unmanifested S3 object must not appear in any manifest"
fi
pass "unmanifested S3 object stays uncommitted"

s3_ckpt="$(curl -fsS "$BASE_URL/api/v2/pipelines/manifest-s3/checkpoint")"
echo "$s3_ckpt" | grep 's3-integrity/' >/dev/null || fail "S3 checkpoint does not record the manifest key"
pass "S3 checkpoint carries the manifest key"

echo
mc_run "mc rb --force local/etl-bucket/s3-integrity >/dev/null 2>&1 || true" 2>/dev/null || true

echo
echo "==> All data-integrity e2e assertions passed"
