# 贡献指南

## 环境准备

- Go 1.24+，纯 Go 默认构建（无 CGO，无外部运行时）
- 可选开发容器：`docker exec -it etl-go-dev bash`（已配置 Go 1.24 + 集成测试容器）

## 构建与测试

```bash
go build ./...                              # 默认构建（纯 Go + Lua）
go build -tags=extism ./...                 # + WASM 插件
go build -tags=nolua ./...                  # 剥离 Lua

make test                                   # 单元测试 + -race
make test-quick                             # 快速测试（仅 ./internal/etl/...）
make test-integration                       # 集成测试（需要 MySQL + ClickHouse 容器）
```

## 代码风格

匹配周围代码。关键约定（详见 `SPEC.md` §3-§5）：

- **零数据丢失**：每条写路径必须在重试耗尽后路由到 DLQ；DLQ 写入失败必须告警并触发断路器，绝不静默 `return` 或 `continue`
- **并发安全**：共享可变状态使用 mutex 或 atomics 保护；`-race` 默认打开
- **错误消息**：每个连接器错误都必须包含 WHERE 上下文（host:port / db / table / brokers）
- **接口优先**：使用类型化可选接口（`SchemaValidator`、`SinkMetricsProvider`），而非 `map[string]any` 能力标志
- **向后兼容**：YAML spec 中的未知 key 忽略并记录 debug 日志（先加载后告警）

## 添加 Sink / Source / Transform

每个连接器都是一个自包含的文件，在 `init()` 中自行注册：

**Source** — 实现 `core.Source` → 注册 `registry.RegisterSource("name", factory)`
- 文件位置：`internal/etl/source/`

**Sink** — 实现 `core.Sink` → 注册 `registry.RegisterSink("name", factory)`
- 文件位置：`internal/etl/sink/`

**Transform** — 实现 `core.Transform` → 注册 `registry.RegisterTransform("name", factory)`
- 文件位置：`internal/etl/transform/`

### 可选接口

建议实现但不强制：

| 接口 | 用途 |
|------|------|
| `core.CheckpointPositioner` | 每次写入后持久化检查点位置 |
| `core.SinkMetricsProvider` | 暴露 Prometheus 指标 |
| `core.SchemaValidator` | 启动时校验 schema 兼容性 |
| `core.PreflightChecker` | 启动前验证（连接、权限、binlog 格式等） |
| `core.SchemaDescriptor` | 描述输出 schema 用于自动建表 |

### 连接器开发清单

- [ ] 构造 → `Open()`（连接/验证）→ `Read()`/`Write()` → `Close()` 生命周期完整
- [ ] 所有 connect/ping 错误包含 WHERE 上下文（host:port、db、table、brokers）
- [ ] 密钥（密码、token）从 config 读取，绝不记录到日志
- [ ] `init()` 注册使用唯一、描述性的名称
- [ ] Config 使用与 YAML schema 匹配的 `mapstructure` 标签
- [ ] 测试至少覆盖：构造、Open、happy-path Read/Write、Close

## Pull Request 清单

- [ ] `go build ./...`、`go build -tags=extism ./...`、`go build -tags=nolua ./...` 全部通过
- [ ] `go test -race -count=1 ./...` 全部通过
- [ ] `go vet ./...` 干净
- [ ] 新增连接器有测试（至少构造+Open+Ping 的 happy-path）
- [ ] 连接器错误消息包含 host/port/db/table WHERE 上下文
- [ ] 没有静默数据丢失路径 —— 每条失败的 write 都路由到 DLQ 或告警+断路器触发
- [ ] 在 `.goreleaser.yml` 或相应的文件树中添加了新的 go module 依赖（如有）
- [ ] PR 描述中附上 E2E 测试结果（或说明为什么不需要）

## 文档

功能变更需更新：
- `SPEC.md` — 删除已修复项的 Phase 5 表格行，更新 Tier 摘要
- `CHANGELOG.md` — 在 `[Unreleased]` 下添加条目（同时更新 `CHANGELOG.zh.md`）
- `docs/etl-config-schema.md` — 如有新增或修改的配置字段（同时更新 `.zh.md` 中文版）
- `docs/etl-api.md` — 如有新增或修改的 API 端点（同时更新 `.zh.md` 中文版）
- 所有变更文档的双语版本（`.zh.md`）

## 构建标签

| 标签 | 效果 | 默认？ |
|------|------|--------|
| *(无)* | 纯 Go 核心 + 全部 sink/source + Lua（gopher-lua） | ✅ |
| `-tags=extism` | + WASM 插件运行时（wazero，纯 Go） | — |
| `-tags=nolua` | 剥离 Lua 运行时，更小的二进制 | — |
| `CGO_ENABLED=1` | + JavaScript/TypeScript transform（QuickJS，CGO） | — |

## 发布流程

1. 更新 `CHANGELOG.md` 和 `CHANGELOG.zh.md` — 将 `[Unreleased]` 改为版本号和日期
2. 合并到 `main`（`internal/consts/consts.go` 中 `var Version = "0.0.0-dev"`，goreleaser 通过 ldflags 注入标签版本）
3. 推送标签：`git tag vX.Y.Z && git push origin vX.Y.Z`
4. GitHub Actions 触发 `goreleaser`，构建多平台归档 + Docker 镜像至 GHCR，并创建 GitHub Release
