#!/bin/sh

# IT-2/T2.2 lifecycle gate: a durable stop survives a process restart without
# source/sink I/O, preserves the checkpoint, and resumes from that boundary
# only after an explicit start.
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="openetl-go-etl:dev"
MYSQL_CONTAINER="etl-mysql-source"
APP_CONTAINER="etl-openetl-go-lifecycle"
APP_PORT="8024"
PIPELINE="mysql-cdc-crash-to-mysql"
DATA_DIR="$ROOT_DIR/data-lifecycle-desired-state"

mysql_root() {
  "$CONTAINER_CLI" exec "$MYSQL_CONTAINER" mysql -N -uroot -proot123456 -e "$1" 2>/dev/null
}

target_count() {
  mysql_root "SELECT COUNT(*) FROM dzh3136_target.customers WHERE id IN (9421,9422,9423);" | tr -d '[:space:]'
}

cleanup() {
  "$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
  mysql_root "DELETE FROM dzh3136_go.customers WHERE id IN (9421,9422,9423); DELETE FROM dzh3136_target.customers WHERE id IN (9421,9422,9423);" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

wait_mysql() {
  i=0
  while [ "$i" -lt 60 ]; do
    status="$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
    if [ "$status" = "healthy" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 2
  done
  return 1
}

wait_api() {
  i=0
  while [ "$i" -lt 60 ]; do
    body="$(curl -sS "http://127.0.0.1:$APP_PORT/api/v2/pipelines" 2>/dev/null || true)"
    if echo "$body" | grep -q '"pipelines"'; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

wait_target_count() {
  want="$1"
  i=0
  while [ "$i" -lt 60 ]; do
    got="$(target_count)"
    if [ "$got" = "$want" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  echo "target count=$(target_count), want $want" >&2
  return 1
}

run_app() {
  "$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
  "$CONTAINER_CLI" run -d \
    --add-host host.docker.internal:host-gateway \
    --name "$APP_CONTAINER" \
    -p "$APP_PORT:8001" \
    -v "$ROOT_DIR/testdata/pipes-cdc-crash:/app/pipes:ro" \
    -v "$ROOT_DIR/testdata:/app/testdata:ro" \
    -v "$DATA_DIR:/app/data" \
    -v "$ROOT_DIR/logs:/app/logs" \
    "$IMAGE" >/dev/null
  wait_api
}

if [ "${E2E_SKIP_BUILD:-0}" = "1" ]; then
  echo "==> Skip image build (E2E_SKIP_BUILD=1, using $IMAGE)"
else
  echo "==> Build image"
  "$CONTAINER_CLI" build -t "$IMAGE" -f Dockerfile .
fi

echo "==> Start and prepare MySQL source/target"
compose -f docker-compose.dev.yml up -d mysql-source
wait_mysql
mysql_root "CREATE DATABASE IF NOT EXISTS dzh3136_target; CREATE TABLE IF NOT EXISTS dzh3136_target.customers LIKE dzh3136_go.customers; GRANT ALL PRIVILEGES ON dzh3136_target.* TO 'sync_user'@'%'; FLUSH PRIVILEGES; DELETE FROM dzh3136_go.customers WHERE id IN (9421,9422,9423); DELETE FROM dzh3136_target.customers WHERE id IN (9421,9422,9423);" >/dev/null

echo "==> Start a fresh CDC pipeline and establish a checkpoint"
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR" "$ROOT_DIR/logs"
chmod -R a+rwX "$DATA_DIR" "$ROOT_DIR/logs"
run_app
mysql_root "INSERT INTO dzh3136_go.customers (id, name, email, phone, status, amount) VALUES (9421, 'Lifecycle One', 'lifecycle-1@example.com', '13900009421', 'active', 101.00);" >/dev/null
wait_target_count 1
sleep 2

echo "==> Persist desired_state=stopped and emit an event while stopped"
stop_body="$(curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/stop")"
echo "$stop_body" | grep '"desired_state":"stopped"' >/dev/null
sleep 2
checkpoint_before="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/checkpoint")"
echo "$checkpoint_before" | grep '"checkpoint"' >/dev/null
mysql_root "INSERT INTO dzh3136_go.customers (id, name, email, phone, status, amount) VALUES (9422, 'Lifecycle Two', 'lifecycle-2@example.com', '13900009422', 'active', 202.00);" >/dev/null
sleep 3
test "$(target_count)" = "1"

echo "==> Restart the process; stopped pipeline must perform zero I/O"
"$CONTAINER_CLI" restart "$APP_CONTAINER" >/dev/null
wait_api
pipeline_body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines")"
echo "$pipeline_body" | grep "\"name\":\"$PIPELINE\"" | grep '"desired_state":"stopped"' | grep '"observed_state":"stopped"' >/dev/null
checkpoint_after="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/checkpoint")"
test "$checkpoint_after" = "$checkpoint_before"
mysql_root "INSERT INTO dzh3136_go.customers (id, name, email, phone, status, amount) VALUES (9423, 'Lifecycle Three', 'lifecycle-3@example.com', '13900009423', 'active', 303.00);" >/dev/null
sleep 3
test "$(target_count)" = "1"

echo "==> Explicit start resumes from the preserved checkpoint"
start_body="$(curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/start")"
echo "$start_body" | grep '"desired_state":"running"' >/dev/null
echo "$start_body" | grep '"generation"' >/dev/null
wait_target_count 3
test "$(mysql_root "SELECT COUNT(*) FROM dzh3136_target.customers WHERE id=9422 AND amount=202.00;" | tr -d '[:space:]')" = "1"
test "$(mysql_root "SELECT COUNT(*) FROM dzh3136_target.customers WHERE id=9423 AND amount=303.00;" | tr -d '[:space:]')" = "1"

echo "Lifecycle desired-state restart E2E passed"
