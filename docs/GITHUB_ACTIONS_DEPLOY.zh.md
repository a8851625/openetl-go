# GitHub Actions 发布与部署指南

本文档说明当前仓库实际存在的 GitHub Actions 配置，覆盖 CI、Release 产物、GHCR 镜像以及推荐的服务器部署方式。

English version: [`GITHUB_ACTIONS_DEPLOY.md`](./GITHUB_ACTIONS_DEPLOY.md).

## 工作流概览

| 工作流 | 文件 | 触发条件 | 作用 |
| --- | --- | --- | --- |
| Test | `.github/workflows/test.yml` | push 或 pull request 到 `main` | 执行 `go vet`、带 `-race` 的单元测试，以及带 MySQL + ClickHouse 服务的集成测试。 |
| Release | `.github/workflows/release.yml` | 推送匹配 `v*` 的 tag | 运行 GoReleaser，发布多平台压缩包到 GitHub Releases，并推送默认 Docker 镜像到 GHCR。 |

当前仓库没有通过 SSH 登录服务器的部署工作流。服务器发布是单独的运维动作：从 GHCR 拉取已发布镜像，挂载生产配置和数据目录，然后重启容器或服务管理器。

## Release 发布内容

GoReleaser 配置位于 [`.goreleaser.yml`](../.goreleaser.yml)。

| 产物 | 内容 | 说明 |
| --- | --- | --- |
| `openetl-go_<version>_<os>_<arch>` | 默认纯 Go 二进制、`manifest/`、`README.md`、`LICENSE` | 支持 Linux、macOS、Windows 的 amd64/arm64，不包含 Windows arm64。 |
| `openetl-go-extism_<version>_<os>_<arch>` | 启用 WASM 的二进制及相同发布文件 | 使用 `-tags=extism` 构建。 |
| `ghcr.io/a8851625/openetl-go:<version>` | 运行时 Docker 镜像 | 包含二进制、`manifest/`、`resource/` 和 `pipes/`。不携带测试夹具。 |
| `ghcr.io/a8851625/openetl-go:latest` | 最新发布镜像 | 由 tag 发布流程更新。 |

发布镜像不包含 `testdata/`。测试夹具保留在仓库中供自动化 e2e 脚本使用；生产容器应通过显式挂载传入输入文件、pipeline spec 和可写状态目录。

## 仓库权限要求

当前工作流只使用 GitHub 提供的凭证：

| 权限 | 用途 |
| --- | --- |
| `contents: write` | GoReleaser 创建 GitHub Release 并上传压缩包/checksum。 |
| `packages: write` | GoReleaser 推送 Docker 镜像到 GitHub Container Registry。 |

已提交的工作流不需要阿里云镜像仓库密钥、SSH 私钥或服务器地址密钥。

如果仓库策略限制了 `GITHUB_TOKEN`，请在 **Settings -> Actions -> General -> Workflow permissions** 中开启 package 写权限，或改用有 GHCR 发布权限的受限 token。

## 发布流程

1. 确认 `main` 干净且 CI 通过。

   ```sh
   git status --short
   git fetch origin
   ```

2. 创建并推送版本 tag。

   ```sh
   git tag v0.1.0
   git push origin v0.1.0
   ```

3. 查看 Release 工作流。

   ```sh
   gh run list --workflow Release --limit 5
   ```

4. 校验发布产物。

   ```sh
   gh release view v0.1.0
   docker pull ghcr.io/a8851625/openetl-go:v0.1.0
   ```

稳定版本建议使用语义化 tag（`vMAJOR.MINOR.PATCH`）。是否标记为 prerelease 由 GoReleaser 的规则自动决定。

## 本地发布演练

推送 tag 前可以先做 snapshot 构建：

```sh
goreleaser build --snapshot --clean
```

该命令只验证多平台二进制构建，不创建 GitHub Release，也不推送镜像。需要更接近完整发布但不真正发布时：

```sh
goreleaser release --snapshot --clean --skip=publish
```

默认构建是纯 Go（`CGO_ENABLED=0`）。依赖 QuickJS 的 JavaScript/TypeScript transform 不进入跨平台发布矩阵；需要时请在目标环境启用 CGO 单独构建。

## 服务器部署

### 目录结构

每个环境建议使用独立部署目录：

```sh
sudo mkdir -p /opt/openetl-go/{config,pipes,data,input,logs}
sudo chown -R "$USER":"$USER" /opt/openetl-go
```

推荐职责划分：

| 路径 | 容器挂载点 | 用途 |
| --- | --- | --- |
| `/opt/openetl-go/config/config.yaml` | `/app/manifest/config/config.yaml:ro` | 运行配置。 |
| `/opt/openetl-go/pipes` | `/app/pipes:ro` | 当前环境的 pipeline spec。 |
| `/opt/openetl-go/data` | `/app/data` | SQLite 元存储、checkpoint、DLQ、输出文件。 |
| `/opt/openetl-go/input` | `/app/data/input:ro` | 可选的 file source 输入数据。 |
| `/opt/openetl-go/logs` | `/app/logs` | 启用文件日志时的滚动日志。 |

### 最小单机配置

单节点部署使用 SQLite：

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

生产密钥通过环境变量传入，不写入 Git：

```sh
export ETL_API_TOKEN="$(openssl rand -hex 32)"
export ETL_SPEC_ENCRYPTION_KEY="$(openssl rand -base64 32)"
```

### 运行发布镜像

```sh
docker pull ghcr.io/a8851625/openetl-go:v0.1.0

docker run -d \
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

验证服务：

```sh
curl -fsS http://127.0.0.1:8000/api/v2/health
curl -fsS -H "X-API-Token: $ETL_API_TOKEN" http://127.0.0.1:8000/api/v2/pipelines
```

管理界面地址为 `http://<server>:8000`。

### 升级

```sh
docker pull ghcr.io/a8851625/openetl-go:v0.1.1
docker stop openetl-go
docker rm openetl-go
# 使用同一份 docker run 命令，仅替换新 tag
```

如果要自动化部署，请把 `docker run` 固化在部署脚本、Compose 或 systemd 中。不要把运行状态写在容器文件系统里；只有挂载的 `/app/data` 应持久化。

### 回滚

```sh
docker pull ghcr.io/a8851625/openetl-go:v0.1.0
docker stop openetl-go
docker rm openetl-go
# 使用相同配置和数据目录运行旧 tag
```

如果某个 release 修改了存储 schema，回滚前需要先阅读 release notes。SQLite 文件或 SQL 元存储可能包含旧二进制无法识别的迁移。

## 分布式部署说明

多进程运行前必须把 `etl.storage.type` 切换为 `mysql` 或 `postgresql`。SQLite 是本地单文件存储，不适合 master-worker 多节点共享。

典型配置：

| 进程 | 必要配置 |
| --- | --- |
| Master | `etl.role: master`、SQL storage DSN、对外 API 地址。 |
| Worker | `etl.role: worker`、同一 SQL storage DSN、`etl.masterURL` 指向 master API。 |

Worker 也可以用环境变量配置：

```sh
ETL_ROLE=worker
ETL_MASTER_URL=http://openetl-master:8001
ETL_WORKER_ID=worker-a
```

Pipeline spec 挂载到 master 即可。Worker 执行分配到的分片，并通过 master API 上报 heartbeat。

## 常见问题

### Release 工作流无法推送 GHCR

检查 workflow permissions 和 package 可见性。Release 工作流需要 `packages: write`；GitHub Packages 中的包也可能需要关联到当前仓库。

### `goreleaser` 在 `go mod tidy` 后失败

GoReleaser 会在 pre-hook 中运行 `go mod tidy`。先本地运行并检查差异：

```sh
go mod tidy
git diff -- go.mod go.sum
```

有意的依赖变更需要先提交，再推送 tag。

### 容器启动但 API 不可用

检查日志和挂载配置：

```sh
docker logs --tail 200 openetl-go
docker exec openetl-go ls -la /app/manifest/config /app/pipes /app/data
```

常见原因包括配置路径错误、SQL 凭据缺失、端口被占用，或 pipeline spec 指向了不可达的 source/sink。

### File source 找不到输入文件

发布镜像不包含测试夹具。请显式挂载输入文件，并在 spec 中引用容器内挂载路径：

```yaml
source:
  type: file
  config:
    path: /app/data/input/customers.jsonl
    format: json
```

### Health check 返回非 200

先检查统一端口上的代理 health endpoint：

```sh
curl -i http://127.0.0.1:8000/api/v2/health
```

然后使用 API token 查看 `/metrics` 和 pipeline 状态。`503` 表示进程可达，但 ETL server 报告非健康状态。

## 相关文档

- [`quickstart.zh.md`](./quickstart.zh.md) — 本地 MySQL 到 ClickHouse 入门。
- [`etl-config-schema.zh.md`](./etl-config-schema.zh.md) — pipeline YAML 字段。
- [`etl-api.zh.md`](./etl-api.zh.md) — 运行时 API 参考。
- [`parallelism-and-batching.zh.md`](./parallelism-and-batching.zh.md) — 分片与批处理行为。
