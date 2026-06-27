ROOT_DIR    = $(shell pwd)
NAMESPACE   = "default"
DEPLOY_NAME = "openetl-go"
DOCKER_NAME = "openetl-go"

include ./hack/hack-cli.mk
include ./hack/hack.mk

# ── Test targets ────────────────────────────────────────────────────────

# Run unit tests with race detector.
.PHONY: test
test:
	go test -race -count=1 ./internal/etl/... ./internal/logic/... ./internal/controller/...

# Run integration/E2E tests (requires docker or podman with docker-compose.dev.yml).
.PHONY: test-integration
test-integration:
	@echo "Starting dev services..."
	@. ./hack/container-cli.sh; compose -f docker-compose.dev.yml up -d mysql-source clickhouse
	@sleep 10
	@echo "Running E2E tests..."
	go test -race -count=1 -tags=integration ./internal/etl/...
	@echo "Stopping dev services..."
	@. ./hack/container-cli.sh; compose -f docker-compose.dev.yml down
	@echo "Done."

# Run all tests.
.PHONY: test-all
test-all: test test-integration

# Quick test — run unit tests without race detector (faster for dev loops).
.PHONY: test-quick
test-quick:
	go test -count=1 ./internal/etl/... ./internal/logic/...

# Run tests for a specific package (usage: make test-pkg PKG=pipeline).
.PHONY: test-pkg
test-pkg:
	go test -race -count=1 -v ./internal/etl/$(PKG)/... 2>&1 | head -200
