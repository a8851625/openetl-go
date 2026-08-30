// Package e2e contains container-backed end-to-end tests for the core
// Source -> Transform -> Sink paths (IT-1, roadmap RA-7).
//
// Everything in this directory is isolated behind the `e2e` build tag so that
// testcontainers-go (and its Docker client dependency tree) never enters the
// main binary or the default dependency graph:
//
//	go list -deps ./... | grep testcontainers   # must print nothing
//
// The harness pure-logic helpers (namespace derivation, skip recording) live
// in internal/etl/e2e/harness without the tag so their unit tests run in the
// regular `go test ./internal/etl/...` gate; only the testcontainers-backed
// files carry the tag.
//
// Run locally (podman rootless example — resolve the machine API socket via
// `podman machine inspect` -> ConnectionInfo.PodmanSocket):
//
//	export DOCKER_HOST=unix:///var/folders/.../podman/podman-machine-default-api.sock
//	export TESTCONTAINERS_RYUK_DISABLED=true
//	go test -tags=e2e -count=1 ./internal/etl/e2e/...
//
// CI gate mode (skip is never pass):
//
//	go test -tags=e2e -count=1 -timeout 40m ./internal/etl/e2e/... -e2e.strict
//
// The tests assert the exact case sets of their origin shell scripts
// (hack/e2e-path-mysql-cdc-mysql.sh, hack/e2e-snapshot-cdc-clickhouse.sh);
// the mapping table is recorded in docs/iterations/IT-1-verification-substrate/
// tasks.md. Assertions must not be relaxed during migration.
package e2e
