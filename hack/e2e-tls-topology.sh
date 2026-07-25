#!/usr/bin/env bash
set -euo pipefail

# PR-0.3.3: one certificate pair terminates TLS on both the embedded UI and
# ETL API; the UI reverse proxy verifies the API certificate by name.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

UI_PORT="${TLS_UI_PORT:-18443}"
API_PORT="${TLS_API_PORT:-18444}"
TOKEN="tls-smoke-token-0123456789"
TMP_DIR="$(mktemp -d)"
APP_PID=""

cleanup() {
  if [[ -n "$APP_PID" ]]; then
    kill "$APP_PID" >/dev/null 2>&1 || true
    wait "$APP_PID" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT INT TERM

command -v openssl >/dev/null 2>&1 || { echo "openssl is required" >&2; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "curl is required" >&2; exit 1; }

mkdir -p "$TMP_DIR/pipes" "$TMP_DIR/data" "$TMP_DIR/logs"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -subj '/CN=localhost' \
  -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -keyout "$TMP_DIR/tls.key" \
  -out "$TMP_DIR/tls.crt" >/dev/null 2>&1

BIN="${OPENETL_TLS_SMOKE_BINARY:-}"
if [[ -z "$BIN" ]]; then
  echo "==> build TLS smoke binary"
  if command -v go >/dev/null 2>&1; then
    go build -o "$TMP_DIR/openetl-go" .
  else
    . "$ROOT_DIR/hack/container-cli.sh"
    detect_container_cli
    "$CONTAINER_CLI" run --rm \
      -v "$PWD:/workspace" \
      -v "$TMP_DIR:/out" \
      -v openetl-go_go-cache:/go \
      -v openetl-go_go-build-cache:/root/.cache/go-build \
      -w /workspace etl-go-dev:latest \
      sh -c 'go build -o /out/openetl-go .'
  fi
  BIN="$TMP_DIR/openetl-go"
fi

echo "==> start HTTPS-only UI/API runtime"
ETL_PROFILE=development \
ETL_API_TOKEN="$TOKEN" \
ETL_AUDIT_ENABLED=true \
LOGGER_FORMAT=text \
  "$BIN" \
    --host 127.0.0.1 \
    --port "$UI_PORT" \
    --etl-api-host 127.0.0.1 \
    --etl-api-port "$API_PORT" \
    --data-dir "$TMP_DIR/data" \
    --log-dir "$TMP_DIR/logs" \
    --specs-dir "$TMP_DIR/pipes" \
    --tls-cert "$TMP_DIR/tls.crt" \
    --tls-key "$TMP_DIR/tls.key" \
    --tls-server-name localhost >"$TMP_DIR/app.log" 2>&1 &
APP_PID=$!

for _ in $(seq 1 80); do
  if curl -ksSf "https://127.0.0.1:${UI_PORT}/api/v2/health" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$APP_PID" >/dev/null 2>&1; then
    cat "$TMP_DIR/app.log" >&2
    exit 1
  fi
  sleep 0.25
done

echo "==> verify UI, proxied API, and direct API over TLS"
curl -ksSf "https://127.0.0.1:${UI_PORT}/" | grep -q 'OpenETL'
curl -ksSf "https://127.0.0.1:${UI_PORT}/api/v2/health" | grep -q '"status":"ok"'
curl -ksSf "https://127.0.0.1:${API_PORT}/api/v2/health" | grep -q '"status":"ok"'

if curl -fsS "http://127.0.0.1:${UI_PORT}/api/v2/health" >/dev/null 2>&1; then
  echo "FAIL: UI port still accepts clear-text HTTP" >&2
  exit 1
fi
if curl -fsS "http://127.0.0.1:${API_PORT}/api/v2/health" >/dev/null 2>&1; then
  echo "FAIL: API port still accepts clear-text HTTP" >&2
  exit 1
fi

echo "==> verify headers and authenticated proxy forwarding"
curl -ksS -D "$TMP_DIR/ui.headers" -o /dev/null "https://127.0.0.1:${UI_PORT}/"
grep -qi '^Strict-Transport-Security: max-age=31536000; includeSubDomains' "$TMP_DIR/ui.headers"
grep -qi '^X-Content-Type-Options: nosniff' "$TMP_DIR/ui.headers"
grep -qi '^X-Frame-Options: DENY' "$TMP_DIR/ui.headers"

status="$(curl -ksS -D "$TMP_DIR/auth.headers" -o "$TMP_DIR/auth.body" -w '%{http_code}' "https://127.0.0.1:${UI_PORT}/api/v2/pipelines")"
[[ "$status" == "401" ]]
grep -qi '^WWW-Authenticate: Bearer realm="openetl-etl-api"' "$TMP_DIR/auth.headers"
curl -ksSf -H "X-API-Token: $TOKEN" "https://127.0.0.1:${UI_PORT}/api/v2/pipelines" | grep -q '"pipelines"'

echo "TLS topology smoke passed"
