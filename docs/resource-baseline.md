# Resource Baseline (P5)

Record measured baselines for release notes. Update when packaging, connectors,
or default resource limits change by more than the regression thresholds below.

## How to measure

```bash
# Binary size (default pure-Go build)
go build -trimpath -ldflags="-s -w" -o /tmp/openetl-go .
ls -lh /tmp/openetl-go

# Container image (pinned tag)
docker image inspect ghcr.io/a8851625/openetl-go:<version> --format '{{.Size}}'

# Startup (standalone, sqlite, no pipelines)
time ./openetl-go --help >/dev/null
# cold process start to /api/v2/health (local):
#   /usr/bin/time -l ./openetl-go ... &  # or time curl until health ok

# Idle RSS after health ok, zero pipelines (sample)
ps -o rss= -p $(pgrep -n openetl-go)
```

Throughput / checkpoint delay should be measured on the declared production path
(see `docs/path-contract.md`), not on synthetic no-op pipes alone.

## Current baseline snapshot

> Fill numbers from the machine that cuts the release. Values below are the
> **targets / last known engineering baselines** for standalone production profile
> (MySQL metadata + Redis state + 2 CPU / 2Gi limit).

| Metric | Baseline | Regression threshold | Notes |
| --- | --- | --- | --- |
| Linux amd64 binary (`-s -w`) | ~45–70 MiB | +20% | Pure Go; extism variant larger |
| GHCR image (goreleaser default) | ~80–120 MiB | +25% | Alpine/runtime base + binary |
| Cold start to HTTP listen | < 3 s | > 5 s | Empty specs, local sqlite |
| Idle RSS (0 pipelines) | < 150 MiB | > 250 MiB | Excludes MySQL/Redis sidecars |
| Compose default limits | 2 CPU / 2 GiB | n/a | `OPENETL_CPU_LIMIT` / `OPENETL_MEM_LIMIT` |
| Checkpoint interval (typical CDC) | 30 s | path-specific | Spec `checkpoint_interval_sec` |
| Health checkpoint stale threshold | 300 s | n/a | `ETL_HEALTH_CHECKPOINT_STALE_SEC` |
| Health CDC lag threshold | 60 s | n/a | `ETL_HEALTH_CDC_LAG_MS` |

## Frontend bundle

```bash
cd web && npm ci && npm run build
du -sh ../resource/public
```

Record total `resource/public` size. Fail release candidate review if size grows
>20% without an explicit exemption in the PR / release notes.

## Storage backend cost notes

| Backend | Idle overhead | Notes |
| --- | --- | --- |
| SQLite | lowest | Single node only; file backup |
| MySQL 8 | sidecar ~300–500 MiB | Production default in compose |
| PostgreSQL | sidecar similar | Prefer when already standardized |

## Regression policy

1. Measure on release candidate, same architecture as production (linux/amd64).
2. If binary/image exceeds threshold, either optimize or document exemption with owner.
3. If idle RSS exceeds threshold under empty load, investigate goroutine/leak before GA.
4. Path throughput regressions belong in reliability notes, not only this table.

## Release notes template snippet

```text
Resource baseline (<commit>):
- binary: <size>
- image: <size> (<tag/digest>)
- start_to_health: <sec>
- idle_rss: <MiB>
- frontend_public: <size>
- storage matrix: sqlite=passed mysql=... postgres=...
```
