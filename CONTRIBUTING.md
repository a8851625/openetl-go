# Contributing

## Environment

- Go 1.24+, pure Go default build (no CGO, no external runtimes)
- Optional dev container: `podman exec -it etl-go-dev bash` (Go 1.24 + integration test containers pre-configured)

## Build & Test

```bash
go build ./...                              # Default build (pure Go + Lua)
go build -tags=extism ./...                 # + WASM plugins
go build -tags=nolua ./...                  # Strip Lua

make test                                   # Unit tests + -race
make test-quick                             # Fast tests (./internal/etl/... only)
make test-integration                       # Integration tests (requires MySQL + ClickHouse containers)
```

## Code Style

Match surrounding code. Key conventions (see `SPEC.md` §3-§5):

- **Zero data loss**: Every write path must route to DLQ after retry exhaustion; DLQ write failures must alert and trip circuit breaker — never silently `return` or `continue`
- **Concurrency**: Shared mutable state protected by mutex or atomics; `-race` is always on in tests
- **Error messages**: Every connector error must carry WHERE context (host:port / db / table / brokers)
- **Interface-first**: Use typed optional interfaces (`SchemaValidator`, `SinkMetricsProvider`) rather than `map[string]any` capability flags
- **Backward compatibility**: Unknown keys in YAML specs are ignored and logged at debug level (load-first, warn-later)

## Adding a Source / Sink / Transform

Each connector is a self-contained file that self-registers in `init()`:

**Source** — implements `core.Source` → registers via `registry.RegisterSource("name", factory)`
- File location: `internal/etl/source/`

**Sink** — implements `core.Sink` → registers via `registry.RegisterSink("name", factory)`
- File location: `internal/etl/sink/`

**Transform** — implements `core.Transform` → registers via `registry.RegisterTransform("name", factory)`
- File location: `internal/etl/transform/`

### Optional Interfaces

Implement these where applicable (recommended but not required):

| Interface | Purpose |
|-----------|---------|
| `core.CheckpointPositioner` | Persist checkpoint position after each write |
| `core.SinkMetricsProvider` | Expose Prometheus metrics per sink |
| `core.SchemaValidator` | Validate schema compatibility at startup |
| `core.PreflightChecker` | Pre-flight verification (connectivity, grants, binlog format, etc.) |
| `core.SchemaDescriptor` | Describe output schema for auto-create |

### Connector Checklist

- [ ] Constructor → `Open()` (connect/validate) → `Read()`/`Write()` → `Close()` lifecycle
- [ ] All connect/ping errors carry WHERE context (host:port, db, table, brokers)
- [ ] Secrets (passwords, tokens) read from config, never logged
- [ ] `init()` registration with a unique, descriptive name
- [ ] Config uses `mapstructure` tags matching the YAML schema
- [ ] Test covers at minimum: constructor, Open, happy-path Read/Write, Close

## Pull Request Checklist

- [ ] `go build ./...`, `go build -tags=extism ./...`, `go build -tags=nolua ./...` all pass
- [ ] `go test -race -count=1 ./...` all pass
- [ ] `go vet ./...` is clean
- [ ] New connectors have tests (at minimum constructor + Open + Ping happy-path)
- [ ] Connector error messages include host/port/db/table WHERE context
- [ ] No silent data-loss paths — every failed write routes to DLQ or alerts + breaker trip
- [ ] New Go module dependencies added to `.goreleaser.yml` or corresponding file tree (if any)
- [ ] E2E test results attached to PR description (or explanation of why not needed)

## Documentation

Feature changes must update:
- `SPEC.md` — remove completed Phase 5 table rows, update Tier summaries
- `CHANGELOG.md` — add entry under `[Unreleased]`
- `docs/etl-config-schema.md` — if new or changed config fields
- `docs/etl-api.md` — if new or changed API endpoints
- Both language versions (`.zh.md`) for all changed docs

## Build Tags

| Tag | Effect | Default? |
|-----|--------|----------|
| *(none)* | Pure Go core + all sinks/sources + Lua (gopher-lua) | ✅ |
| `-tags=extism` | + WASM plugin runtime (wazero, pure Go) | — |
| `-tags=nolua` | Strip Lua runtime for smaller binary | — |
| `CGO_ENABLED=1` | + JavaScript/TypeScript transform (QuickJS, CGO) | — |

## Release Process

1. Update `CHANGELOG.md` — move `[Unreleased]` to version number and date
2. Merge to `main` (`internal/consts/consts.go` has `var Version = "0.0.0-dev"`; goreleaser injects the tag via ldflags)
3. Push tag: `git tag vX.Y.Z && git push origin vX.Y.Z`
4. GitHub Actions triggers `goreleaser`, builds multi-platform archives + Docker image to GHCR, and creates a GitHub Release
