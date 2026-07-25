#!/usr/bin/env bash
# P5 CI gate: unit/vet/race smoke + production profile + release asset scan +
# storage backend matrix (skip when external deps missing).
set -euo pipefail

ROOT="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PASS=0
FAIL=0
SKIP=0
RESULTS=()

run_step() {
  local name="$1"
  shift
  printf '▸ %s ... ' "$name"
  local log
  log="$(mktemp)"
  if "$@" >"$log" 2>&1; then
    echo "PASS"
    PASS=$((PASS + 1))
    RESULTS+=("PASS|$name")
  else
    # Treat explicit SKIP markers from child scripts.
    if grep -Eiq 'SKIP|skipped|not available|no .* environment' "$log"; then
      echo "SKIP"
      SKIP=$((SKIP + 1))
      RESULTS+=("SKIP|$name")
    else
      echo "FAIL (see $log)"
      FAIL=$((FAIL + 1))
      RESULTS+=("FAIL|$name|$log")
      # Keep going so the matrix report is complete.
    fi
  fi
}

echo "╔══════════════════════════════════════════════════╗"
echo "║   OpenETL-Go P5 production / CI gate             ║"
echo "╚══════════════════════════════════════════════════╝"

run_step "go vet" go vet ./internal/etl/... ./internal/logic/... ./internal/cmd/...
run_step "unit race (telemetry/alert/storage/server core)" \
  go test -race -count=1 \
    ./internal/etl/telemetry/... \
    ./internal/etl/alert/... \
    ./internal/etl/storage/... \
    ./internal/etl/server -run 'Test(ProductionProfile|DevelopmentProfile|ParseCORS|ClientIP|AuthFailure|SecretEnvelope|ConnectionCatalogCRUD)'
run_step "release asset scan" bash ./hack/check-release-assets.sh
run_step "production profile smoke" bash ./hack/e2e-production-profile.sh
run_step "runtime smoke" bash ./hack/e2e-runtime-smoke.sh
run_step "sqlite backup/retention" bash ./hack/e2e-backup-restore-sqlite.sh

HAVE_CONTAINER=false
if command -v docker >/dev/null 2>&1 || command -v podman >/dev/null 2>&1; then
  HAVE_CONTAINER=true
fi

# External backends: self-contained scripts spin up throwaway containers when a
# runtime is available. Missing runtime → explicit SKIP (never silent green).
if [[ "$HAVE_CONTAINER" == true ]] || [[ -n "${MYSQL_HOST:-}" ]] || [[ -n "${MYSQL_DSN:-}" ]]; then
  run_step "mysql storage e2e" bash ./hack/e2e-storage-mysql.sh
else
  echo "▸ mysql storage e2e ... SKIP (no container runtime / MYSQL_HOST)"
  SKIP=$((SKIP + 1))
  RESULTS+=("SKIP|mysql storage e2e")
fi

if [[ "$HAVE_CONTAINER" == true ]] || [[ -n "${POSTGRES_HOST:-}" ]] || [[ -n "${PGHOST:-}" ]] || [[ -n "${POSTGRES_DSN:-}" ]]; then
  run_step "postgres storage e2e" bash ./hack/e2e-storage-postgres.sh
else
  echo "▸ postgres storage e2e ... SKIP (no container runtime / POSTGRES_HOST)"
  SKIP=$((SKIP + 1))
  RESULTS+=("SKIP|postgres storage e2e")
fi

# Optional path smokes — skip when containers unavailable.
if [[ "$HAVE_CONTAINER" == true ]]; then
  run_step "path contract smoke" bash ./hack/e2e-path-contract-smoke.sh
else
  echo "▸ path contract smoke ... SKIP (no container runtime)"
  SKIP=$((SKIP + 1))
  RESULTS+=("SKIP|path contract smoke")
fi

echo ""
echo "═══════════════════════════════════════"
echo "  Results: $PASS passed, $FAIL failed, $SKIP skipped"
echo "═══════════════════════════════════════"
for r in "${RESULTS[@]}"; do
  status="${r%%|*}"
  rest="${r#*|}"
  name="${rest%%|*}"
  echo "  $status  $name"
done

if [[ "$FAIL" -gt 0 ]]; then
  exit 1
fi
echo "P5 production gate passed (skips are explicit, not silent green)"
