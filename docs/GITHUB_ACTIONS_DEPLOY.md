# GitHub Actions Release and Deployment Guide

This guide documents the GitHub Actions setup that is currently present in this repository. It covers CI, release artifacts, GHCR images, and a practical server deployment path for OpenETL-Go.

Chinese version: [`GITHUB_ACTIONS_DEPLOY.zh.md`](./GITHUB_ACTIONS_DEPLOY.zh.md).

## Workflow Overview

| Workflow | File | Trigger | What it does |
| --- | --- | --- | --- |
| Test | `.github/workflows/test.yml` | Push or pull request to `main` | Runs `go vet`, unit tests with `-race`, and integration tests with MySQL + ClickHouse services. |
| Release | `.github/workflows/release.yml` | Push tag matching `v*` | Runs GoReleaser, publishes multi-platform archives to GitHub Releases, and pushes the default Docker image to GHCR. |

There is no repository workflow that SSHs into a server. Server rollout is intentionally a separate operator step: pull a released image from GHCR, mount production config and data directories, then restart the container or service manager.

## What Release Publishes

GoReleaser is configured by [`.goreleaser.yml`](../.goreleaser.yml).

| Artifact | Contents | Notes |
| --- | --- | --- |
| `openetl-go_<version>_<os>_<arch>` | Default pure-Go binary, `manifest/`, `README.md`, `LICENSE` | Linux, macOS, Windows for amd64/arm64, excluding Windows arm64. |
| `openetl-go-extism_<version>_<os>_<arch>` | WASM-enabled binary plus the same release files | Built with `-tags=extism`. |
| `ghcr.io/a8851625/openetl-go:<version>` | Runtime Docker image | Contains binary, `manifest/`, and `resource/`. Pipeline specs are not baked in — mount them to `/app/pipes`. Test fixtures are not shipped. |
| `ghcr.io/a8851625/openetl-go:latest` | Latest release image | Updated by tagged releases. |

Release images do **not** include `testdata/`. Test fixtures remain in the repository for automated e2e scripts, but production containers should receive input files, pipeline specs, and writable state through explicit mounts.

## Required Repository Settings

The current workflows use GitHub-provided credentials only:

| Permission | Why it is needed |
| --- | --- |
| `contents: write` | GoReleaser creates the GitHub Release and uploads archives/checksums. |
| `packages: write` | GoReleaser pushes the Docker image to GitHub Container Registry. |

No Alibaba Cloud registry secrets, SSH private key, or server host secret is required by the checked-in workflows.

If a future repository policy restricts `GITHUB_TOKEN`, enable package write permission under **Settings -> Actions -> General -> Workflow permissions**, or replace it with a scoped token that can publish GHCR packages.

## Release Procedure

1. Ensure `main` is clean and CI is green.

   ```sh
   git status --short
   git fetch origin
   ```

2. Create and push a version tag.

   ```sh
   git tag v0.1.0
   git push origin v0.1.0
   ```

3. Watch the release workflow.

   ```sh
   gh run list --workflow Release --limit 5
   ```

4. Verify the published artifacts.

   ```sh
   gh release view v0.1.0
   CONTAINER_CLI="${CONTAINER_CLI:-$(command -v podman || command -v docker)}"
   "$CONTAINER_CLI" pull ghcr.io/a8851625/openetl-go:v0.1.0
   ```

Use semantic tags (`vMAJOR.MINOR.PATCH`) for stable releases. GoReleaser marks prereleases automatically when the tag matches its prerelease rules.

## Local Release Dry Run

Use a snapshot build before pushing a tag:

```sh
goreleaser build --snapshot --clean
```

This verifies cross-platform binary builds without creating GitHub releases or pushing images. For a full local release simulation that does not publish:

```sh
goreleaser release --snapshot --clean --skip=publish
```

The default build is pure Go (`CGO_ENABLED=0`). JavaScript/TypeScript transforms that require QuickJS are intentionally not part of the cross-compiled release matrix; build them locally with CGO when needed.

## Server Deployment

### Directory Layout

Create one deployment directory per environment:

```sh
sudo mkdir -p /opt/openetl-go/{config,pipes,data,input,logs}
sudo chown -R "$USER":"$USER" /opt/openetl-go
```

Recommended responsibilities:

| Path | Mounted to container | Purpose |
| --- | --- | --- |
| `/opt/openetl-go/config/config.yaml` | `/app/manifest/config/config.yaml:ro` | Runtime config. |
| `/opt/openetl-go/pipes` | `/app/pipes:ro` | Environment-specific pipeline specs. |
| `/opt/openetl-go/data` | `/app/data` | SQLite metastore, checkpoints, DLQ, output files. |
| `/opt/openetl-go/input` | `/app/data/input:ro` | Optional file-source input data. |
| `/opt/openetl-go/logs` | `/app/logs` | Rolling log files when file logging is enabled. |

### Minimal Standalone Config

Use SQLite for a single-node deployment:

```yaml
server:
  address: ":8000"
  serverRoot: "resource/public"

etl:
  enabled: true
  address: ":8001"
  role: "standalone"
  specsDir: "./pipes"
  checkpointDir: "./data/checkpoint"
  dlqDir: "./data/dlq"
  storage:
    type: "sqlite"
    sqlite:
      path: "./data/etl.db"

logger:
  level: "info"
  stdout: true
  path: "./logs"
  file: "openetl-{Y-m-d}.log"
  rotate: "daily"
  backups: 30
```

Set production secrets through environment variables rather than writing them into Git:

```sh
export ETL_API_TOKEN="$(openssl rand -hex 32)"
export ETL_SPEC_ENCRYPTION_KEY="$(openssl rand -base64 32)"
```

### Run the Released Image

```sh
CONTAINER_CLI="${CONTAINER_CLI:-$(command -v podman || command -v docker)}"
"$CONTAINER_CLI" pull ghcr.io/a8851625/openetl-go:v0.1.0

"$CONTAINER_CLI" run -d \
  --name openetl-go \
  --restart unless-stopped \
  -p 8000:8000 \
  -p 8001:8001 \
  -e ETL_API_TOKEN="$ETL_API_TOKEN" \
  -e ETL_SPEC_ENCRYPTION_KEY="$ETL_SPEC_ENCRYPTION_KEY" \
  -v /opt/openetl-go/config/config.yaml:/app/manifest/config/config.yaml:ro \
  -v /opt/openetl-go/pipes:/app/pipes:ro \
  -v /opt/openetl-go/data:/app/data \
  -v /opt/openetl-go/input:/app/data/input:ro \
  -v /opt/openetl-go/logs:/app/logs \
  ghcr.io/a8851625/openetl-go:v0.1.0
```

Verify the service:

```sh
curl -fsS http://127.0.0.1:8000/api/v2/health
curl -fsS -H "X-API-Token: $ETL_API_TOKEN" http://127.0.0.1:8000/api/v2/pipelines
```

Open the UI at `http://<server>:8000`.

### Upgrade

```sh
CONTAINER_CLI="${CONTAINER_CLI:-$(command -v podman || command -v docker)}"
"$CONTAINER_CLI" pull ghcr.io/a8851625/openetl-go:v0.1.1
"$CONTAINER_CLI" stop openetl-go
"$CONTAINER_CLI" rm openetl-go
# run the same container run command with the new tag
```

For scripted deployments, keep the container `run` command in a checked deployment script or use Compose/systemd. Do not store runtime state inside the container filesystem; only mounted `/app/data` should persist.

### Rollback

```sh
CONTAINER_CLI="${CONTAINER_CLI:-$(command -v podman || command -v docker)}"
"$CONTAINER_CLI" pull ghcr.io/a8851625/openetl-go:v0.1.0
"$CONTAINER_CLI" stop openetl-go
"$CONTAINER_CLI" rm openetl-go
# run the previous tag with the same mounted config/data directories
```

If a release changes the storage schema, inspect release notes before rolling back. The SQLite file or SQL metastore may contain migrations that older binaries do not understand.

## Distributed Deployment Notes

Use `etl.storage.type: mysql` or `postgresql` before running more than one process. SQLite is a local single-file store and is not suitable for multi-node master-worker operation.

Typical layout:

| Process | Required config |
| --- | --- |
| Master | `etl.role: master`, SQL storage DSN, public API address. |
| Worker | `etl.role: worker`, same SQL storage DSN, `etl.masterURL` pointing to the master API. |

Workers can also be configured with environment variables:

```sh
ETL_ROLE=worker
ETL_MASTER_URL=http://openetl-master:8001
ETL_WORKER_ID=worker-a
```

Mount the same pipeline specs to the master. Workers execute assigned shards and report heartbeats through the master API.

## Troubleshooting

### Release workflow cannot push GHCR

Check workflow permissions and package visibility. The release workflow needs `packages: write`; the package may also need to be linked to the repository under GitHub Packages settings.

### `goreleaser` fails after `go mod tidy`

GoReleaser runs `go mod tidy` as a pre-hook. Run it locally and review the diff:

```sh
go mod tidy
git diff -- go.mod go.sum
```

Commit intentional dependency changes before tagging.

### Container starts but API is unavailable

Check logs and the mounted config:

```sh
CONTAINER_CLI="${CONTAINER_CLI:-$(command -v podman || command -v docker)}"
"$CONTAINER_CLI" logs --tail 200 openetl-go
"$CONTAINER_CLI" exec openetl-go ls -la /app/manifest/config /app/pipes /app/data
```

Common causes are an invalid config path, missing SQL credentials, a port already in use, or a pipeline spec that references an unreachable source/sink.

### File source cannot find input data

Release images do not include test fixtures. Mount input files explicitly and reference the mounted container path:

```yaml
source:
  type: file
  config:
    path: /app/data/input/customers.jsonl
    format: json
```

### Health check returns non-200

Use the proxied health endpoint first:

```sh
curl -i http://127.0.0.1:8000/api/v2/health
```

Then inspect `/metrics` and pipeline status with an API token. A `503` means the process is reachable but the ETL server reports an unhealthy state.

## Related Documents

- [`quickstart.md`](./quickstart.md) — local MySQL to ClickHouse walkthrough.
- [`etl-config-schema.md`](./etl-config-schema.md) — pipeline YAML fields.
- [`etl-api.md`](./etl-api.md) — runtime API reference.
- [`parallelism-and-batching.md`](./parallelism-and-batching.md) — sharding and batching behavior.
