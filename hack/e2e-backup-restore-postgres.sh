#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
. "$ROOT/hack/container-cli.sh"
detect_container_cli

CONTAINER="etl-backup-test-pg"
HOST_PORT="15433"
PASS="pgpass123"

cleanup() { "$CONTAINER_CLI" rm -f "$CONTAINER" >/dev/null 2>&1 || true; }
trap cleanup EXIT INT TERM

echo "==> Start throwaway Postgres"
cleanup
"$CONTAINER_CLI" run -d --name "$CONTAINER" \
  -e POSTGRES_PASSWORD="$PASS" \
  -e POSTGRES_DB=openetl_backup \
  -p "$HOST_PORT:5432" \
  docker.io/library/postgres:16-alpine >/dev/null

i=0
while [ "$i" -lt 60 ]; do
  if "$CONTAINER_CLI" exec "$CONTAINER" pg_isready -U postgres >/dev/null 2>&1; then break; fi
  i=$((i+1)); sleep 2
done
[ "$i" -lt 60 ] || { echo "pg not ready"; exit 1; }

export POSTGRES_DSN="postgres://postgres:${PASS}@127.0.0.1:${HOST_PORT}/openetl_backup?sslmode=disable"
go test ./internal/etl/storage/ -count=1 -run 'Backup|Retention'
OUT="$(mktemp -d)"
go run ./hack/cmd/postgres-backup-smoke "$OUT"
echo "PR-1.3 Postgres backup e2e PASS"
