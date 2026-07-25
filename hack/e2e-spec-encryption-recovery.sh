#!/bin/sh

# Storage-backed encrypted-spec recovery gate for PR-0.1.
# The Go integration tests create encrypted linear and DAG specs, close and
# reopen the SQL store twice, exercise versions/diff/rollback, and execute an
# encrypted worker task. They are intentionally kept in the repository's hack
# namespace so release evidence can invoke one stable command.
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

TEST_REGEX='TestEncryptedSpecRecoveryFlowLinearAndDAG|TestLegacyPlaintextSpecRecoveryFlow|TestLegacySpecCiphertextRemainsReadable|TestExecuteShardReadsEncryptedPipelineSpec|TestExecuteShardFailsWhenEncryptedSpecKeyIsMissing'

if command -v go >/dev/null 2>&1; then
  echo "==> encrypted spec recovery (local Go)"
  go test ./internal/etl/server ./internal/etl/worker -run "$TEST_REGEX" -count=1
  echo "Encrypted spec recovery E2E passed"
  exit 0
fi

# Keep the documented container fallback when the host has no Go toolchain.
. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli
if ! "$CONTAINER_CLI" image inspect etl-go-dev:latest >/dev/null 2>&1; then
  echo "BLOCKED: Go toolchain and etl-go-dev:latest are unavailable" >&2
  exit 2
fi

echo "==> encrypted spec recovery (container: $CONTAINER_CLI)"
"$CONTAINER_CLI" run --rm \
  -v "$ROOT_DIR:/workspace" \
  -v openetl-go_go-cache:/go \
  -v openetl-go_go-build-cache:/root/.cache/go-build \
  -w /workspace etl-go-dev:latest \
  sh -c "go test ./internal/etl/server ./internal/etl/worker -run '$TEST_REGEX' -count=1"
echo "Encrypted spec recovery E2E passed"
