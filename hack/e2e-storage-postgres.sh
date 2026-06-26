#!/bin/sh
# E2E: storage conformance suite against PostgreSQL (scalable mode backend).
# Brings up a throwaway PostgreSQL 16 container, runs the full storage.Storage
# conformance matrix, tears down.
#
# Usage: ./hack/e2e-storage-postgres.sh
# Exit: 0 on success, non-zero on failure.

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

CONTAINER="etl-storage-test-pg"
DB="openetl_conf"
USER="etl"
PASS="etl123"
HOST_PORT="15432"

cleanup() {
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

echo "==> Start throwaway PostgreSQL 16 container (port $HOST_PORT)"
cleanup
docker run -d --name "$CONTAINER" \
  --add-host host.docker.internal:host-gateway \
  -e POSTGRES_DB="$DB" \
  -e POSTGRES_USER="$USER" \
  -e POSTGRES_PASSWORD="$PASS" \
  -p "$HOST_PORT:5432" \
  docker.io/library/postgres:16-alpine >/dev/null

echo "==> Wait for PostgreSQL to accept connections"
i=0
while [ "$i" -lt 60 ]; do
  if docker exec "$CONTAINER" pg_isready -U "$USER" -d "$DB" >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1)); sleep 2
done
if [ "$i" -ge 60 ]; then
  echo "!! PostgreSQL did not become ready in time"; exit 1
fi

DSN="postgres://${USER}:${PASS}@127.0.0.1:${HOST_PORT}/${DB}?sslmode=disable"

echo "==> Run storage conformance suite (SQLite always + PostgreSQL)"
if docker ps --format '{{.Names}}' | grep -q '^etl-go-dev$'; then
  HOST_GATEWAY="host.docker.internal"
  DEV_DSN="postgres://${USER}:${PASS}@${HOST_GATEWAY}:${HOST_PORT}/${DB}?sslmode=disable"
  docker exec -e POSTGRES_DSN="$DEV_DSN" -w /workspace etl-go-dev \
    go test -race -count=1 -v -run 'TestSQLiteConformance|TestPostgresConformance' ./internal/etl/storage/
else
  echo "   (etl-go-dev container not found — running tests on host Go toolchain)"
  POSTGRES_DSN="$DSN" go test -race -count=1 -v \
    -run 'TestSQLiteConformance|TestPostgresConformance' ./internal/etl/storage/
fi

echo "==> Storage conformance against PostgreSQL: PASS"
