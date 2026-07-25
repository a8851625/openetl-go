#!/bin/sh
# PR-1.3: PostgreSQL upgrade + backup/restore smoke.
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

CONTAINER="etl-upgrade-test-pg"
DB="openetl_upgrade"
USER="etl"
PASS="etl123"
HOST_PORT="15434"

cleanup() {
  "$CONTAINER_CLI" rm -f "$CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

echo "==> Start throwaway PostgreSQL 16 (port $HOST_PORT)"
cleanup
"$CONTAINER_CLI" run -d --name "$CONTAINER" \
  --add-host host.docker.internal:host-gateway \
  -e POSTGRES_DB="$DB" \
  -e POSTGRES_USER="$USER" \
  -e POSTGRES_PASSWORD="$PASS" \
  -p "$HOST_PORT:5432" \
  docker.io/library/postgres:16-alpine >/dev/null

echo "==> Wait for PostgreSQL"
i=0
while [ "$i" -lt 60 ]; do
  if "$CONTAINER_CLI" exec "$CONTAINER" pg_isready -U "$USER" -d "$DB" >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1)); sleep 2
done
if [ "$i" -ge 60 ]; then
  echo "!! PostgreSQL did not become ready"; exit 1
fi

DSN="postgres://${USER}:${PASS}@127.0.0.1:${HOST_PORT}/${DB}?sslmode=disable"

echo "==> PostgreSQL upgrade + backup/restore path"
if "$CONTAINER_CLI" ps --format '{{.Names}}' 2>/dev/null | grep -q '^etl-go-dev$'; then
  DEV_DSN="postgres://${USER}:${PASS}@host.docker.internal:${HOST_PORT}/${DB}?sslmode=disable"
  "$CONTAINER_CLI" exec -e POSTGRES_DSN="$DEV_DSN" -w /workspace etl-go-dev \
    go test -count=1 -v -run 'TestBackupRestoreUpgradePath/postgres' ./internal/etl/storage/
else
  echo "   (host Go toolchain)"
  POSTGRES_DSN="$DSN" go test -count=1 -v -run 'TestBackupRestoreUpgradePath/postgres' ./internal/etl/storage/
fi

echo "==> PostgreSQL storage upgrade: PASS"
