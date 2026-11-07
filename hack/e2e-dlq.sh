#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
MYSQL_CONTAINER="etl-mysql-source"
APP_CONTAINER="etl-openetl-go-dlq"

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 60 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

echo "==> Build image"
"$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile .

echo "==> Start MySQL source"
compose -f docker-compose.dev.yml up -d mysql-source

echo "==> Wait MySQL healthy"
i=0
while [ "$i" -lt 60 ]; do
  status="$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
  if [ "$status" = "healthy" ]; then break; fi
  i=$((i + 1))
  sleep 2
done
[ "$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER")" = "healthy" ]

echo "==> Prepare missing sink table"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "CREATE DATABASE IF NOT EXISTS dzh3136_target; DROP TABLE IF EXISTS dzh3136_target.dlq_customers; GRANT ALL PRIVILEGES ON dzh3136_target.* TO 'sync_user'@'%'; FLUSH PRIVILEGES;"

echo "==> Reset ETL data"
rm -rf data-dlq
mkdir -p data-dlq/output data-dlq/checkpoint data-dlq/dlq logs
chmod -R a+rwX data-dlq
chmod a+rwX logs

echo "==> Run failing pipeline"
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p 8004:8001 \
  -v "$ROOT_DIR/testdata/pipes-dlq:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-dlq:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE"

wait_http "http://127.0.0.1:8004/api/v2/health"

echo "==> Wait DLQ record"
i=0
while [ "$i" -lt 60 ]; do
  body="$(curl -fsS http://127.0.0.1:8004/api/v2/pipelines)"
  echo "$body" | grep '"name":"file-to-missing-mysql"' | grep '"records_dlq":2' >/dev/null 2>&1 && break
  i=$((i + 1))
  sleep 1
done
body="$(curl -fsS http://127.0.0.1:8004/api/v2/dlq/file-to-missing-mysql)"
echo "$body"
echo "$body" | grep 'DLQ Alice'
echo "$body" | grep 'DLQ Bob'

echo "==> Current backlog is exposed separately from the cumulative counter"
pipelines="$(curl -fsS http://127.0.0.1:8004/api/v2/pipelines)"
echo "$pipelines"
# dlq_backlog is the authoritative backlog; stats.records_dlq is cumulative and
# must not be used as one.
echo "$pipelines" | grep '"dlq_backlog":2'
echo "$pipelines" | grep '"dlq_backlog_known":true'

echo "==> Paging reaches the second record"
page1="$(curl -fsS 'http://127.0.0.1:8004/api/v2/dlq/file-to-missing-mysql?limit=1')"
page2="$(curl -fsS 'http://127.0.0.1:8004/api/v2/dlq/file-to-missing-mysql?limit=1&offset=1')"
test "$(printf '%s' "$page1" | grep -o '"job_name"' | wc -l | tr -d '[:space:]')" = "1"
test "$(printf '%s' "$page2" | grep -o '"job_name"' | wc -l | tr -d '[:space:]')" = "1"
test "$page1" != "$page2"

echo "==> Dry run reports the replay plan without writing"
dry="$(curl -fsS -X POST 'http://127.0.0.1:8004/api/v2/dlq/file-to-missing-mysql/replay?dry_run=1')"
echo "$dry"
echo "$dry" | grep '"dry_run":true'
echo "$dry" | grep '"replayable":2'
echo "$dry" | grep '"blocked":0'
# The target table still does not exist, so any real write would have failed.
# Rows and backlog must be untouched by the preview.
body="$(curl -fsS http://127.0.0.1:8004/api/v2/dlq/file-to-missing-mysql)"
test "$(printf '%s' "$body" | grep -o '"job_name"' | wc -l | tr -d '[:space:]')" = "2"

echo "==> Repair sink and selectively replay"
"$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "CREATE TABLE dzh3136_target.dlq_customers LIKE dzh3136_go.customers;"
replay="$(curl -fsS -X POST 'http://127.0.0.1:8004/api/v2/dlq/file-to-missing-mysql/replay?contains=9901')"
echo "$replay"
echo "$replay" | grep '"replayed":1'

copied="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.dlq_customers WHERE id=9901;" 2>/dev/null | tr -d '[:space:]')"
test "$copied" = "1"
not_copied="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.dlq_customers WHERE id=9902;" 2>/dev/null | tr -d '[:space:]')"
test "$not_copied" = "0"

body="$(curl -fsS http://127.0.0.1:8004/api/v2/dlq/file-to-missing-mysql)"
echo "$body"
echo "$body" | grep 'DLQ Bob'

echo "==> Replay remaining DLQ"
replay="$(curl -fsS -X POST http://127.0.0.1:8004/api/v2/dlq/file-to-missing-mysql/replay)"
echo "$replay"
echo "$replay" | grep '"replayed":1'
copied="$("$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -usync_user -psync_password_123 -e "SELECT COUNT(*) FROM dzh3136_target.dlq_customers WHERE id IN (9901,9902);" 2>/dev/null | tr -d '[:space:]')"
test "$copied" = "2"
body="$(curl -fsS http://127.0.0.1:8004/api/v2/dlq/file-to-missing-mysql)"
echo "$body"
echo "$body" | grep '"items":\[\]'

echo "==> Backlog returns to zero after replay"
pipelines="$(curl -fsS http://127.0.0.1:8004/api/v2/pipelines)"
echo "$pipelines" | grep '"dlq_backlog":0'

echo "==> Delete-all reports the real remaining count (never the -1 sentinel)"
deleted="$(curl -fsS -X DELETE http://127.0.0.1:8004/api/v2/dlq/file-to-missing-mysql)"
echo "$deleted"
echo "$deleted" | grep '"deleted":0'
if printf '%s' "$deleted" | grep -q '"deleted":-1'; then
  echo "delete-all still returns the -1 sentinel" >&2
  exit 1
fi

echo "DLQ E2E passed"
