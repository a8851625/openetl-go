# testdata/ — E2E 测试夹具（非 user-facing 示例）

本目录是 `hack/e2e-*.sh`（26+ 个端到端测试脚本）依赖的**测试夹具**，不是面向用户的
example 配置。

## 不要照抄这里的 spec

`pipes-*/` 下的 YAML 写死了 `dev` harness 专用的服务名、端口和凭据，例如：

- `host.docker.internal:13306` / `:9001` / `:19092`（指向 `docker-compose.dev.yml` 起的服务）
- `sync_user` / `sync_password_123` / `dzh123456`（dev harness 凭据）
- `/app/testdata/files/*.jsonl`（e2e 专用输入数据路径）

这些 spec 在 `docker-compose.quickstart.yml` 或生产环境里**无法直接跑通**。

## 用户面向示例

请用 [`../pipes-quickstart/`](../pipes-quickstart) —— 这些 spec 与
`docker-compose.quickstart.yml` 的服务名（`quickstart-mysql` / `quickstart-clickhouse`）
和凭据对齐，可以一键启动。

每个连接器的字段、配置语义和示例 YAML 见 [`../docs/components/`](../docs/components)。

## testdata 目录结构

```
testdata/
├── files/                  # e2e 输入数据（jsonl）
├── mysql/init/             # MySQL source 初始化 SQL
├── clickhouse/init/        # ClickHouse sink 初始化 SQL
├── gb32960/                # Kafka raw protocol parser fixture
├── http-fixture/           # HTTP source mock server
└── pipes-*/                # 各 e2e 场景的 pipeline spec（30+ 目录）
```

## 镜像构建

`testdata/` 已被 `.dockerignore` 排除，**不会进入生产镜像**。提交到 git 是因为
e2e 脚本需要 clone 仓库后直接使用。
