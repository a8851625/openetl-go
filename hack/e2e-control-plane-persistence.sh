#!/bin/sh

# PR-0.2 + IT-2/T2.1 control-plane persistence gate. Covers atomic current/
# version/checkpoint/delete transactions, API fault rollback, and durable
# restore_failed visibility/repair without checkpoint or encrypted-spec drift.
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

TEST_REGEX='TestPipeline(CreateStorageFailureLeavesNoRuntimeOrRows|UpdateStorageFailureKeepsLastSuccessfulRuntimeAndDB|UpdateCheckpointFailureRollsBackSpecAndCheckpoint|RollbackStorageFailureKeepsCurrentVersion|DeleteStorageFailureKeepsRuntimeRowsVersionsAndCheckpoint)|TestCheckpointResetFailureReturnsNon2xxAndKeepsCheckpoint|TestSpecImportStorageFailureKeepsExistingRuntimeAndDB|TestScheduleStorageFailureKeepsInMemoryScheduleUnchanged|TestPipelineSpecStoreAtomic|TestRestore(FailuresPersistAndRemainVisible|FailureRepairPreservesCheckpointAndEncryptedSpec|StrictProfileDefaultsAndOverrides)'

if command -v go >/dev/null 2>&1; then
  echo "==> control-plane persistence (local Go)"
  go test ./internal/etl/server ./internal/etl/storage -run "$TEST_REGEX" -count=1
  echo "Control-plane persistence E2E passed"
  exit 0
fi

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli
if ! "$CONTAINER_CLI" image inspect etl-go-dev:latest >/dev/null 2>&1; then
  echo "BLOCKED: Go toolchain and etl-go-dev:latest are unavailable" >&2
  exit 2
fi

echo "==> control-plane persistence (container: $CONTAINER_CLI)"
"$CONTAINER_CLI" run --rm \
  -v "$ROOT_DIR:/workspace" \
  -v openetl-go_go-cache:/go \
  -v openetl-go_go-build-cache:/root/.cache/go-build \
  -w /workspace etl-go-dev:latest \
  sh -c "go test ./internal/etl/server ./internal/etl/storage -run '$TEST_REGEX' -count=1"
echo "Control-plane persistence E2E passed"
