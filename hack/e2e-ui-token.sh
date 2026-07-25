#!/usr/bin/env bash
set -euo pipefail

# PR-0.3.2 focused browser gate: the shared API token is usable by the UI for
# the current page lifetime, is never persisted to localStorage, and is lost on
# a full reload. This intentionally avoids unrelated P4 wizard/layout checks.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

IMAGE="${IMAGE:-openetl-go-etl:ui-token-e2e}"
APP="etl-ui-token-e2e"
DATA_DIR="$ROOT_DIR/data-ui-token-e2e"
LOG_DIR="$ROOT_DIR/logs"
BASE_URL="http://127.0.0.1:${UI_TOKEN_E2E_PORT:-8078}"
API_PORT="${UI_TOKEN_E2E_API_PORT:-8079}"
TOKEN="ui-memory-token-0123456789"

command -v playwright-cli >/dev/null 2>&1 || {
  echo "playwright-cli is required" >&2
  exit 1
}

cleanup() {
  "$CONTAINER_CLI" rm -f "$APP" >/dev/null 2>&1 || true
  rm -rf "$DATA_DIR"
  playwright-cli close >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

mkdir -p "$DATA_DIR/output" "$DATA_DIR/checkpoint" "$DATA_DIR/dlq" "$DATA_DIR/input" "$LOG_DIR"
chmod -R a+rwX "$DATA_DIR" "$LOG_DIR"

if [[ "${E2E_SKIP_BUILD:-0}" == "1" ]]; then
  echo "==> Skip image build (E2E_SKIP_BUILD=1, using $IMAGE)"
else
  echo "==> Build current UI image"
  "$CONTAINER_CLI" build --quiet -t "$IMAGE" -f "$ROOT_DIR/Dockerfile" "$ROOT_DIR"
fi

"$CONTAINER_CLI" rm -f "$APP" >/dev/null 2>&1 || true
echo "==> Start token-protected app container"
"$CONTAINER_CLI" run -d --name "$APP" \
  --add-host host.docker.internal:host-gateway \
  -p "${BASE_URL##*:}:8000" -p "$API_PORT:8001" \
  -e ETL_API_TOKEN="$TOKEN" \
  -e ETL_AUDIT_ENABLED=true \
  -v "$ROOT_DIR/testdata/pipes-auth:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$DATA_DIR:/app/data" \
  -v "$LOG_DIR:/app/logs" \
  "$IMAGE" >/dev/null

echo "==> Wait for UI/API health"
for _ in $(seq 1 60); do
  if curl -fsS "$BASE_URL/api/v2/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS "$BASE_URL/api/v2/health" >/dev/null

evaljs() {
  playwright-cli --raw eval "$1"
}

open_settings() {
  evaljs "(() => { const n=document.querySelector('[data-nav=settings]'); if (n) { n.click(); return true; } return false; })()" >/dev/null
  for _ in $(seq 1 20); do
    if [[ "$(evaljs "document.querySelector('input[placeholder*=API]') !== null")" == "true" ]]; then
      return 0
    fi
    sleep 0.25
  done
  echo "FAIL: settings token input did not open" >&2
  return 1
}

echo "==> Open browser and prove unauthenticated API is rejected"
playwright-cli open "$BASE_URL/?e2e=$(date +%s)" >/dev/null
sleep 2
playwright-cli --raw eval "(() => { localStorage.setItem('etl_lang','en'); localStorage.setItem('etl_e2e','1'); return true; })()" >/dev/null
unauthorized="$(evaljs "fetch('/api/v2/pipelines').then(r => r.status)")"
[[ "$unauthorized" == "401" ]]

echo "==> Save token in page memory and exercise authenticated UI request"
open_settings
playwright-cli fill "input[placeholder*='API']" "$TOKEN" >/dev/null
evaljs "(() => { const b=Array.from(document.querySelectorAll('button')).find(x => (x.textContent || '').includes('Save Token')); if (!b) return false; b.click(); return true; })()" >/dev/null
sleep 0.5
memory_only="$(evaljs "document.querySelector('input[placeholder*=API]')?.value === '$TOKEN' && localStorage.getItem('etl_api_token') === null")"
[[ "$memory_only" == "true" ]]

evaljs "(() => { document.querySelector('[data-testid=reload-specs-anchor]')?.click() || document.querySelector('[data-testid=reload-specs]')?.click() || Array.from(document.querySelectorAll('button')).find(b => (b.textContent || '').includes('Reload Specs'))?.click(); return true; })()" >/dev/null
authenticated="false"
for _ in $(seq 1 12); do
  authenticated="$(evaljs "document.body.innerText.includes('Reload specs') || document.body.innerText.includes('Success')")"
  if [[ "$authenticated" == "true" ]]; then
    break
  fi
  sleep 0.5
done
[[ "$authenticated" == "true" ]]

echo "==> Reload page and prove token was not persisted"
evaljs "location.reload(); true" >/dev/null
sleep 2
persisted="$(evaljs "localStorage.getItem('etl_api_token')")"
[[ "$persisted" == "null" ]]
open_settings
empty_after_reload="$(evaljs "document.querySelector('input[placeholder*=API]')?.value === ''")"
[[ "$empty_after_reload" == "true" ]]
unauthorized_after_reload="$(evaljs "fetch('/api/v2/pipelines').then(r => r.status)")"
[[ "$unauthorized_after_reload" == "401" ]]

echo "UI token memory-only smoke passed"
