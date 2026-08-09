#!/bin/sh

# PR-0.3: fail-closed production profile, compose interpolation, and HTTP
# security-boundary smoke.
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"
. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

echo "==> runtime profile unit gate"
go test ./internal/etl/server ./internal/cmd -run 'Test(ProductionProfile|DevelopmentProfile|RuntimeHelpDocumentsPriorityAndCoreFlags|ParseRuntimeFlags|ValidateRuntimeFlagsRejectsInvalidValues)' -count=1

echo "==> HTTP security boundary unit gate"
go test ./internal/etl/server -run 'Test(ParseCORS|CORS|ClientIP|AuthFailure)' -count=1

echo "==> TLS topology unit gate"
go test ./internal/logic/app -run 'Test(NormalizeETLTarget|ETLProxyTransport|ConfigureHTTPSTopology)' -count=1

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT INT TERM
mkdir -p "$TMP_DIR/certs"

echo "==> compose must reject missing production secrets"
if env -u OPENETL_IMAGE -u OPENETL_CERTS_DIR -u ETL_API_TOKEN -u ETL_SPEC_ENCRYPTION_KEY -u ETL_TLS_SERVER_NAME \
  -u MYSQL_ROOT_PASSWORD -u MYSQL_PASSWORD -u REDIS_PASSWORD \
  "$CONTAINER_CLI" compose -f docker-compose.yml config >"$TMP_DIR/missing.out" 2>&1; then
  echo "FAIL: compose accepted missing production secrets" >&2
  cat "$TMP_DIR/missing.out" >&2
  exit 1
fi

echo "==> compose accepts complete pinned production inputs"
ETL_API_TOKEN="production-token-0123456789" \
ETL_SPEC_ENCRYPTION_KEY="MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=" \
ETL_SPEC_ENCRYPTION_KEY_ID=primary \
ETL_TLS_SERVER_NAME=localhost \
MYSQL_ROOT_PASSWORD="root-password-0123456789" \
MYSQL_PASSWORD="metadata-password-0123456789" \
REDIS_PASSWORD="redis-password-0123456789" \
OPENETL_IMAGE="ghcr.io/a8851625/openetl-go:v0.2.12-beta.8" \
OPENETL_CERTS_DIR="$TMP_DIR/certs" \
  "$CONTAINER_CLI" compose -f docker-compose.yml config >"$TMP_DIR/complete.out"

grep -q 'ETL_PROFILE: production' "$TMP_DIR/complete.out"
grep -q 'ETL_TLS_SERVER_NAME: localhost' "$TMP_DIR/complete.out"
grep -q 'https://localhost:8000/api/v2/health' "$TMP_DIR/complete.out"
if grep -E 'change-me|:latest' "$TMP_DIR/complete.out" >/dev/null 2>&1; then
  echo "FAIL: production compose rendered a placeholder or floating latest image" >&2
  exit 1
fi

echo "Production profile smoke passed"
