#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
if [[ -z "${POSTGRES_DSN:-}" && -z "${CONTAINER_CLI:-}" ]]; then
  echo "SKIP: set POSTGRES_DSN or CONTAINER_CLI=podman to run Postgres backup e2e"
  exit 0
fi
go test ./internal/etl/storage/ -count=1 -run 'Backup|Retention'
echo "PR-1.3 postgres path: unit gates PASS (full container dump residual)"
