#!/usr/bin/env bash
# Lightweight runtime smoke: CLI help, invalid role, optional binary/container health.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

pass=0
fail=0
check() {
  local name="$1"
  shift
  if "$@"; then
    echo "PASS: $name"
    pass=$((pass + 1))
  else
    echo "FAIL: $name"
    fail=$((fail + 1))
  fi
}

echo "== OpenETL-Go runtime smoke =="

# 1) Unit tests already document --help and invalid role.
if command -v go >/dev/null 2>&1; then
  check "cmd package runtime flags" go test ./internal/cmd -count=1
else
  CONTAINER_CLI="${CONTAINER_CLI:-$(command -v docker || command -v podman || true)}"
  if [[ -n "${CONTAINER_CLI}" ]] && "$CONTAINER_CLI" image inspect etl-go-dev:latest >/dev/null 2>&1; then
    check "cmd package runtime flags (container)" \
      "$CONTAINER_CLI" run --rm \
        -v "$PWD:/workspace" \
        -v openetl-go_go-cache:/go \
        -v openetl-go_go-build-cache:/root/.cache/go-build \
        -w /workspace etl-go-dev:latest \
        sh -c "go test ./internal/cmd -count=1"
  else
    echo "SKIP: go toolchain and etl-go-dev image unavailable for unit smoke"
  fi
fi

# 2) If a built binary exists, exercise --help and illegal role without starting servers.
BIN=""
for candidate in ./openetl-go ./bin/openetl-go ./temp/openetl-go; do
  if [[ -x "$candidate" ]]; then
    BIN="$candidate"
    break
  fi
done

if [[ -n "$BIN" ]]; then
  check "binary --help exits 0" bash -c "$BIN --help >/tmp/openetl-help.txt"
  check "binary --help documents priority" grep -q "CLI flags > environment variables" /tmp/openetl-help.txt
  check "binary rejects invalid --role" bash -c "! $BIN --role sidecar >/tmp/openetl-bad-role.txt 2>&1"
  check "invalid role message" grep -qi "standalone, master, or worker" /tmp/openetl-bad-role.txt || grep -qi "invalid --role" /tmp/openetl-bad-role.txt
else
  echo "SKIP: no local openetl-go binary (unit tests still cover parse/validate)"
fi

# 3) Compose files for three deploy forms exist.
check "standalone compose present" test -f docker-compose.yml
check "distributed compose present" test -f docker-compose.distributed.yml
check "runtime modes doc present" test -f docs/runtime-modes.md
check "runbook mentions backup" grep -q "Backup / restore" docs/runtime-modes.md
check "runbook mentions worker scale" grep -q "Worker scale-out" docs/runtime-modes.md

# 4) Optional container CLI health if CONTAINER_CLI and image are available.
CONTAINER_CLI="${CONTAINER_CLI:-$(command -v docker || command -v podman || true)}"
if [[ -n "${CONTAINER_CLI}" ]] && "$CONTAINER_CLI" image inspect openetl-go-etl:dev >/dev/null 2>&1; then
  # Image CMD is ["./main"]; extra args replace CMD entirely when entrypoint is empty,
  # so invoke the binary explicitly.
  check "container --help" bash -c "$CONTAINER_CLI run --rm --entrypoint ./main openetl-go-etl:dev --help >/tmp/openetl-ctr-help.txt"
  check "container rejects invalid role" bash -c "! $CONTAINER_CLI run --rm --entrypoint ./main openetl-go-etl:dev --role sidecar >/tmp/openetl-ctr-bad.txt 2>&1"
else
  echo "SKIP: container image openetl-go-etl:dev not present"
fi

echo "== summary: $pass passed, $fail failed =="
if [[ "$fail" -gt 0 ]]; then
  exit 1
fi
