#!/bin/sh
# E2E: storage conformance suite against MySQL (scalable mode backend).
# Brings up a throwaway MySQL 8 container, runs the full storage.Storage
# conformance matrix + migration-parity test, tears down.
#
# Usage: ./hack/e2e-storage-mysql.sh
# Exit: 0 on success, non-zero on failure.

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

CONTAINER="etl-storage-test-mysql"
DB="openetl_conf"
ROOT_PASS="root123456"
HOST_PORT="13398"

cleanup() {
  podman rm -f "$CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

echo "==> Start throwaway MySQL 8 container (port $HOST_PORT)"
cleanup
podman run -d --name "$CONTAINER" \
  -e MYSQL_ROOT_PASSWORD="$ROOT_PASS" \
  -e MYSQL_DATABASE="$DB" \
  -p "$HOST_PORT:3306" \
  docker.io/library/mysql:8.0 >/dev/null

echo "==> Wait for MySQL to accept connections"
i=0
while [ "$i" -lt 60 ]; do
  if podman exec "$CONTAINER" mysqladmin ping -h localhost -u root -p"$ROOT_PASS" >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1)); sleep 2
done
if [ "$i" -ge 60 ]; then
  echo "!! MySQL did not become ready in time"; exit 1
fi

# Ensure the target DB exists (the MYSQL_DATABASE env creates it, but be explicit).
podman exec "$CONTAINER" mysql -u root -p"$ROOT_PASS" -e "CREATE DATABASE IF NOT EXISTS $DB;" >/dev/null 2>&1 || true

DSN="root:${ROOT_PASS}@tcp(127.0.0.1:${HOST_PORT})/${DB}?parseTime=true&multiStatements=true"

echo "==> Run storage conformance suite (SQLite always + MySQL + migration parity)"
# Run inside the go-dev container so the test process can reach 127.0.0.1:HOST_PORT
# via host networking; fall back to local `go test` if go-dev is absent.
if podman ps --format '{{.Names}}' | grep -q '^etl-go-dev$'; then
  # The go-dev container reaches the host's mapped port via host.containers.internal.
  HOST_GATEWAY="host.containers.internal"
  DEV_DSN="root:${ROOT_PASS}@tcp(${HOST_GATEWAY}:${HOST_PORT})/${DB}?parseTime=true&multiStatements=true"
  podman exec -e MYSQL_DSN="$DEV_DSN" -w /workspace etl-go-dev \
    go test -race -count=1 -v -run 'TestSQLiteConformance|TestMySQLConformance|TestMigrationParity' ./internal/etl/storage/
else
  echo "   (etl-go-dev container not found — running tests on host Go toolchain)"
  MYSQL_DSN="$DSN" go test -race -count=1 -v \
    -run 'TestSQLiteConformance|TestMySQLConformance|TestMigrationParity' ./internal/etl/storage/
fi

echo "==> Storage conformance against MySQL: PASS"
