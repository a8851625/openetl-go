#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

COMPILER_IMAGE="openetl-extism-js:1.6.0"
APP_IMAGE="openetl-go-etl:extism-dev"
APP_CONTAINER="etl-openetl-go-wasm-plugin"
APP_PORT="8026"
PLUGIN_NAME="replay-matrix-transform"
DATA_DIR="$ROOT_DIR/data-wasm-plugin"
EXAMPLE_DIR="$ROOT_DIR/web/plugin-sdk/examples/replay-matrix-transform"
BUILD_DIR="$DATA_DIR/build"

cleanup() {
  "$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 90 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 1
  done
  return 1
}

wait_pipeline_stats() {
  written="$1"
  dlq="$2"
  i=0
  while [ "$i" -lt 90 ]; do
    body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines")"
    echo "$body" | grep '"name":"wasm-replay-matrix"' | grep "\"records_written\":$written" | grep "\"records_dlq\":$dlq" >/dev/null 2>&1 && return 0
    i=$((i + 1)); sleep 1
  done
  echo "$body"
  return 1
}

run_app() {
  "$CONTAINER_CLI" run -d \
    --add-host host.docker.internal:host-gateway \
    --name "$APP_CONTAINER" \
    -p "$APP_PORT:8001" \
    -v "$DATA_DIR/pipes:/app/pipes:ro" \
    -v "$DATA_DIR:/app/data" \
    -v "$ROOT_DIR/logs:/app/logs" \
    "$APP_IMAGE" >/dev/null
  wait_http "http://127.0.0.1:$APP_PORT/api/v2/health"
}

install_plugin() {
  version="$1"
  wasm="$2"
  manifest="$3"
  response="$(curl -fsS -X POST \
    -F "wasm=@$wasm" \
    -F "name=$PLUGIN_NAME" \
    -F "kind=transform" \
    -F "version=$version" \
    -F "manifest=<$manifest" \
    "http://127.0.0.1:$APP_PORT/api/v2/plugins/install")"
  echo "$response"
  echo "$response" | grep '"status":"installed"'
  echo "$response" | grep '"manifest_validated":true'
  echo "$response" | grep '"abi":"openetl.plugin.abi/v1"'
}

if [ "${E2E_SKIP_BUILD:-0}" != "1" ]; then
  echo "==> Build pinned extism-js compiler image"
  "$CONTAINER_CLI" build -t "$COMPILER_IMAGE" -f hack/wasm-compiler.Dockerfile .

  echo "==> Build OpenETL-Go image with extism runtime"
  "$CONTAINER_CLI" build --build-arg GO_BUILD_TAGS=extism -t "$APP_IMAGE" -f Dockerfile .
else
  echo "==> Skip image builds (E2E_SKIP_BUILD=1)"
fi

echo "==> Reset WASM E2E data"
cleanup
rm -rf "$DATA_DIR"
mkdir -p "$BUILD_DIR" "$DATA_DIR/input" "$DATA_DIR/output" "$DATA_DIR/pipes" "$DATA_DIR/checkpoint" "$DATA_DIR/dlq" logs
chmod -R a+rwX "$DATA_DIR" logs

echo "==> Compile real TypeScript plugins to WASM outside the server request path"
"$CONTAINER_CLI" run --rm \
  -v "$ROOT_DIR:/workspace" \
  -w /workspace/web/plugin-sdk \
  "$COMPILER_IMAGE" sh -c '
    set -eu
    esbuild examples/replay-matrix-transform/replay-matrix-v1.ts --bundle --platform=neutral --format=cjs --target=es2020 --outfile=/workspace/data-wasm-plugin/build/replay-matrix-v1.js
    extism-js /workspace/data-wasm-plugin/build/replay-matrix-v1.js -i examples/replay-matrix-transform/plugin.d.ts -o /workspace/data-wasm-plugin/build/replay-matrix-v1.wasm
    esbuild examples/replay-matrix-transform/replay-matrix-v2.ts --bundle --platform=neutral --format=cjs --target=es2020 --outfile=/workspace/data-wasm-plugin/build/replay-matrix-v2.js
    extism-js /workspace/data-wasm-plugin/build/replay-matrix-v2.js -i examples/replay-matrix-transform/plugin.d.ts -o /workspace/data-wasm-plugin/build/replay-matrix-v2.wasm
  '
test -s "$BUILD_DIR/replay-matrix-v1.wasm"
test -s "$BUILD_DIR/replay-matrix-v2.wasm"

cat > "$DATA_DIR/input/records.jsonl" <<'JSON'
{"id":"drop-1","mode":"drop"}
{"id":"single-1","mode":"single"}
{"id":"split-1","mode":"split"}
{"id":"fail-1","mode":"fail"}
JSON

echo "==> Start extism-enabled OpenETL-Go"
run_app

echo "==> Install ABI v1 plugin with typed secret config"
install_plugin "1.0.0" "$BUILD_DIR/replay-matrix-v1.wasm" "$EXAMPLE_DIR/manifest-v1.json"
plugin_json="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/plugins/$PLUGIN_NAME")"
echo "$plugin_json" | grep '"manifest_validated":true'
echo "$plugin_json" | grep '"secret":true'

echo "==> Create ordinary pipeline using plugin_$PLUGIN_NAME"
curl -fsS -X POST \
  -H 'Content-Type: application/json' \
  -d '{"spec":{"name":"wasm-replay-matrix","source":{"type":"file","config":{"path":"/app/data/input/records.jsonl","format":"json"}},"transforms":[{"type":"plugin_replay-matrix-transform","config":{"label":"certified","api_token":"cert-secret"}}],"sink":{"type":"file_sink","config":{"output_dir":"/app/data/output/wasm","format":"jsonl","prefix":"wasm_"}},"batch_size":4,"checkpoint_interval_sec":1,"backpressure_buffer":10,"retry":{"max_attempts":1,"initial_interval_ms":50,"max_interval_ms":50},"dlq":{"enable":true}}}' \
  "http://127.0.0.1:$APP_PORT/api/v2/pipelines" >/dev/null
curl -fsS -X POST \
  "http://127.0.0.1:$APP_PORT/api/v2/pipelines/wasm-replay-matrix/start" >/dev/null

echo "==> Verify real WASM zero/one/many outputs and record-level DLQ"
wait_pipeline_stats 3 1
grep -R 'single-1' "$DATA_DIR/output/wasm"
grep -R 'split-1-a' "$DATA_DIR/output/wasm"
grep -R 'split-1-b' "$DATA_DIR/output/wasm"
if grep -R 'drop-1' "$DATA_DIR/output/wasm"; then
  echo "drop output unexpectedly reached sink" >&2
  exit 1
fi
grep -R '"plugin_label":"certified"' "$DATA_DIR/output/wasm"
grep -R '"secret_was_configured":true' "$DATA_DIR/output/wasm"
if grep -R 'cert-secret' "$DATA_DIR/output/wasm"; then
  echo "secret config leaked into output" >&2
  exit 1
fi
dlq_json="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/dlq/wasm-replay-matrix")"
echo "$dlq_json" | grep 'fail-1'
echo "$dlq_json" | grep 'injected wasm transform failure'

echo "==> Upgrade same plugin name and replay the failed record"
install_plugin "1.1.0" "$BUILD_DIR/replay-matrix-v2.wasm" "$EXAMPLE_DIR/manifest-v2.json"
replay_json="$(curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/dlq/wasm-replay-matrix/replay")"
echo "$replay_json" | grep '"replayed":1'
grep -R 'fail-1' "$DATA_DIR/output/wasm"
grep -R '"recovered":true' "$DATA_DIR/output/wasm"
remaining_dlq="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/dlq/wasm-replay-matrix")"
echo "$remaining_dlq" | grep '"items":\[\]'

echo "==> Restart app and verify plugin reloads from storage"
printf '%s\n' '{"id":"after-restart","mode":"single"}' >> "$DATA_DIR/input/records.jsonl"
"$CONTAINER_CLI" kill "$APP_CONTAINER" >/dev/null
"$CONTAINER_CLI" rm -f "$APP_CONTAINER" >/dev/null 2>&1 || true
run_app
wait_pipeline_stats 1 0
grep -R 'after-restart' "$DATA_DIR/output/wasm"
grep -R '"plugin_version":"1.1.0"' "$DATA_DIR/output/wasm"
plugin_json="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/plugins/$PLUGIN_NAME")"
echo "$plugin_json" | grep '"version":"1.1.0"'
echo "$plugin_json" | grep '"manifest_validated":true'

echo "WASM plugin E2E passed"
