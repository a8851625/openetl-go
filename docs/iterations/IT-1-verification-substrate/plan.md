# IT-1 技术方案

> 本文是实现输入，不是验收标准。验收以 [spec.md](./spec.md) 为准。

## 方案总览

三条技术路线，按依赖顺序：

1. **把测试门禁做成 reusable workflow**，让 release 在 tag 所指 commit 上**同步**执行门禁，
   而不是异步等待另一个 workflow 的结果。
2. **把核心路径 e2e 从 shell + 镜像构建改为 Testcontainers + 子进程二进制**。这一步直接消除了
   使 BUG-1/2/6 停摆的那个阻塞（镜像构建依赖 `go mod download`），因为子进程模式只需
   `go build`，不需要打镜像。
3. **让证据成为测试的输出而不是人工誊写的输入**，并由门禁校验其与 commit 的绑定关系。

存量欠账（BUG-2/6、GAP-1/3/4、P4）不单独设计方案，而是作为路线 2 的**首批用户**落地 ——
它们既是需要闭合的债，也是框架能力的验证样本。

## 分项设计

### A. CI 门禁拓扑

**现状**：

- `.github/workflows/test.yml` 七个 job（`lint`、`connector-evidence`、`unit-test`、
  `production-gate`、`storage-mysql`、`storage-postgres`、`integration-test`）彼此无 `needs:`。
- `.github/workflows/release.yml` 与 `release-beta-container.yml` 只跑
  `hack/check-connector-evidence.sh -strict`，不引用上述任何 job。

**目标**：新增 `.github/workflows/_gate.yml`（`on: workflow_call`）承载全部门禁 job；
`test.yml`、`release.yml`、`release-beta-container.yml` 三处均以 `uses:` 调用它。

**设计权衡**：

| 备选 | 结论 | 理由 |
| --- | --- | --- |
| `workflow_run` 触发 | **否决** | 异步、按分支关联，tag 事件下难以保证门禁结果对应 tag 所指 commit（违反验收 3），且存在竞态 |
| 全部合并为单一 workflow + 条件 job | 否决 | tag 与 PR 的 job 集合差异需要大量 `if:`，可读性和可维护性差 |
| **reusable workflow (`workflow_call`)** | **采纳** | 同步执行、天然运行在调用方的 commit 上、三处复用同一份定义、单点维护 |

**关键机制 —— skip 不得计为 pass**：GitHub Actions 中被 skip 的 job 不会阻塞下游 `needs`，
因此聚合 job 必须显式断言每个前置 job 的 `result`，不能只靠 `needs`：

```yaml
gate-passed:
  needs: [lint, connector-evidence, unit-test, production-gate,
          storage-mysql, storage-postgres, integration-test, connector-e2e]
  if: always()
  runs-on: ubuntu-latest
  steps:
    - name: Assert every gate job succeeded
      run: |
        echo '${{ toJSON(needs) }}' \
          | jq -e 'to_entries | all(.value.result == "success")' \
          || { echo '${{ toJSON(needs) }}'; exit 1; }
```

豁免清单（若确有必须跳过的 job）以显式 `allowlist` 变量表达，并要求
`release-checklist.md` 中逐条记录理由，不允许沉默跳过。

**关键文件**：

| 文件 | 改动性质 |
| --- | --- |
| `.github/workflows/_gate.yml` | 新增，`on: workflow_call`，承载全部门禁 job + `gate-passed` 聚合 |
| `.github/workflows/test.yml` | 改为 `uses: ./.github/workflows/_gate.yml`（push / pull_request） |
| `.github/workflows/release.yml` | 新增 `gate` job（`uses:`），`release` job 加 `needs: gate` |
| `.github/workflows/release-beta-container.yml` | 同上 |
| `docs/release-checklist.md` | 人工检查项改写为流水线强制项 + 豁免清单说明 |

**注意**：reusable workflow 需要 `secrets: inherit`（GHCR 登录、storage e2e 的凭据）。

### B. e2e 框架（Testcontainers + 子进程二进制）

**现状**：69 个 `hack/*.sh`，CI 仅覆盖 `storage-mysql`、`storage-postgres`、
`integration-test` 三类；其余依赖人工在本机执行，且多数需要先 `make image`。

**目标**：`internal/etl/e2e/` 包，`//go:build e2e` 隔离，用 testcontainers-go 管理依赖服务，
用**子进程**运行被测二进制。

**为什么是子进程而不是容器镜像**：

- 崩溃恢复类验收（SIGKILL → 重启 → checkpoint 续跑）需要真实进程生命周期，in-process 无法覆盖。
- 镜像构建依赖 `go mod download`，而这正是使 BUG-1/2/6 停摆四个月的那个阻塞点。子进程模式只需
  `go build`，去掉了该依赖。
- 镜像形态的验证仍然需要，但它属于**发布产物验证**（`hack/check-release-assets.sh`、
  `e2e-production-profile.sh`），不应成为每条数据路径 e2e 的前置。

**包结构**：

```text
internal/etl/e2e/
├── harness/
│   ├── containers.go     # MySQL/PG/Redpanda/ClickHouse/Redis/MinIO 的懒启动 + 生命周期
│   ├── server.go         # 被测二进制的 build / start / SIGKILL / restart / 端口探活
│   ├── namespace.go      # 按 t.Name() 派生 database/schema/topic 前缀，隔离共享容器
│   └── evidence.go       # 结构化证据记录器（见 C）
├── path_mysql_cdc_mysql_test.go
├── path_mysql_snapcdc_clickhouse_test.go
├── path_kafka_clickhouse_test.go
├── ...
└── doc.go
```

**容器复用策略**：包级 `sync.Once` 懒启动，同一 suite 内共享；隔离靠命名空间而非独立实例。
理由是完全隔离会让 8-10 条路径的容器启动开销超出 CI 时间预算。

**skip 语义**：容器运行时不可用时本地 `t.Skip`，但同时写出 `result: "skipped"` 的证据记录；
CI 的 `connector-e2e` job 通过 `-e2e.strict` 标志把任何 skip 变成失败，满足交付约束 1。

**podman 兼容**：需要 `DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock` 与
`TESTCONTAINERS_RYUK_DISABLED=true`；沿用 `hack/container-cli.sh` 的检测结果注入环境变量。

**首批迁移路径**（对应验收 4 的「≥8 条」）：

| # | 路径 | 覆盖的欠账 |
| --- | --- | --- |
| 1 | MySQL CDC → MySQL upsert | 主推荐链路，crash/restart |
| 2 | MySQL snapshot+CDC → ClickHouse | 主推荐链路，checkpoint reset replay |
| 3 | MySQL batch（varchar PK）→ 任意 sink | BUG-1 验收 4 |
| 4 | MySQL CDC binlog purged 三策略 | BUG-2 验收 1/2/3/4 |
| 5 | snapshot_cdc → Kafka → ClickHouse autocreate | BUG-6 |
| 6 | PostgreSQL CDC → sink（metadata 契约） | GAP-1 |
| 7 | Kafka → PostgreSQL sink（metadata PK） | GAP-3 |
| 8 | Kafka → Elasticsearch（mapping conflict） | GAP-4 |
| 9 | DLQ → replay → sink | 通用 replay 契约 |

**数据语义**：本迭代**不改变**任何运行时语义。每条路径的 source position、checkpoint 边界、
sink acknowledgement、重复策略、DLQ 行为、restart/reset 预期，一律沿用
[reliability-certification.md](../../reliability-certification.md) 与
[path-contract.md](../../path-contract.md) 的既有定义。e2e 是这些定义的**可执行表达**，
断言与文档不一致时以文档为准并修 e2e，不得反向放宽文档。

### C. 证据产出与 commit 绑定

**现状**：证据是人工写进 markdown 的文本；`PathContract.LastCertified` 未填充；
`hack/check-connector-evidence.sh -strict -commit <sha>` 已有 commit 参数但校验对象是静态清单。

**目标**：证据成为测试的结构化输出。

```json
{
  "path_id": "mysql_cdc__mysql_upsert",
  "commit": "<git rev-parse HEAD>",
  "run_started_at": "2026-08-29T10:00:00Z",
  "runner": "github-actions|local",
  "deps": {"mysql": "8.0.36", "clickhouse": "24.3", "redpanda": "v23.3.5"},
  "checks": [
    {"name": "happy_path", "result": "passed"},
    {"name": "crash_after_ack_before_checkpoint", "result": "passed"},
    {"name": "dlq_replay", "result": "skipped", "reason": "minio unavailable"}
  ],
  "result": "failed"
}
```

**双重角色**（避免鸡生蛋）：

- **CI 内**：门禁不读已提交的证据文件，而是**在 tag commit 上实跑**并按 live 结果判定。
- **仓库内**：提交的证据 JSON 供文档、`LastCertified` 与人工审阅使用；
  `check-connector-evidence.sh` 校验其 `commit` 字段是当前 HEAD 或其祖先，且不早于
  最近一次相关源码改动 —— 这是验收 5「人工改证据而未重跑测试则失败」的实现点。

**`LastCertified`**：由证据 JSON 派生填充，不再手工维护。

**关键文件**：

| 文件 | 改动性质 |
| --- | --- |
| `internal/etl/e2e/harness/evidence.go` | 新增，结构化记录器 |
| `docs/evidence/<path_id>.json` | 新增目录，测试产物 |
| `hack/check-connector-evidence.sh` | 改造，增加新鲜度与 commit 绑定校验 |
| `internal/etl/server/path_contract*.go` | `LastCertified` 由证据派生 |
| `docs/connector-certification.md` | 记录新的证据生产流程 |

### D. 存量欠账落地

每项**沿用其在 ROADMAP 中的原验收标准**，只是执行载体换成 B 的框架：

| 条目 | 动作 | 特别说明 |
| --- | --- | --- |
| BUG-1 | 先核对：标题写「2026-08-23 delivered」，状态却是 `active`，而证据段落记录容器 e2e 已通过（镜像 `9b887beb`）。三者矛盾。核对证据是否绑定当前 HEAD，一致则置 `delivered`，否则在新框架内重跑 | **先核对再动手**，不假设已通过 |
| BUG-2 | `fail` 策略证据补入验收矩阵；`resume_from_current` 已有真机验证需重新绑定；`resnapshot` 端到端首次执行 | 通过 `RESET MASTER` 模拟 binlog purge |
| BUG-6 | CDC 相位 `ColumnTypes` 经 Kafka → ClickHouse 自动建表 | 断言使用声明类型而非样本推断 |
| GAP-1 | PostgreSQL 实例上验证三个 Metadata 契约 | 容器需 `wal_level=logical` |
| GAP-3 | `postgres` sink `pk_columns_from_metadata` | 与 GAP-1 共用 PG 容器 |
| GAP-4 | ES mapping-conflict 策略 | schema_drift 已收敛为该策略，验收按收敛后的定义 |
| P4 | Doris/Kafka 事实核验 | **风险**：Doris 容器较重，若超出 CI 时间预算则保留 `hack/e2e-doris.sh` 人工路径并显式记为 `blocked`，不得记为 pass |

## 架构约束

- 不引入 Kubernetes、etcd、Zookeeper 或任何新的运行时基础设施依赖。
- testcontainers-go 及其传递依赖必须被 `//go:build e2e` 隔离，**不得进入主二进制**。
  收口时用 `go list -deps ./... | grep -c testcontainers` 断言为 0。
- 不新增并行的执行模型；e2e 驱动的是既有 `Source -> Transform -> Sink` 契约。
- 不删除 `hack/*.sh`。

## 风险与回滚

| 风险 | 影响 | 缓解 | 回滚路径 |
| --- | --- | --- | --- |
| CI 全量时长超 30 分钟 | 开发反馈变慢，倾向于绕过门禁 | 分层：PR 跑快速集，main/tag 跑全量；容器共享 + 命名空间隔离 | 保留 `test.yml` 原七 job 结构，仅回退 release 的 `needs` |
| 共享容器跨测试污染 | 偶发失败被误判为产品缺陷 | 按 `t.Name()` 派生 database/schema/topic 前缀；失败时 dump 命名空间状态 | 退化为每测试独立容器（慢但正确） |
| podman rootless 下 testcontainers 不可用 | 本地无法执行 e2e | 显式 `DOCKER_HOST` + `RYUK_DISABLED` 配置并写入文档 | 本地回退 `hack/*.sh`（已保留） |
| Doris 容器过重 | P4 核验无法进 CI | 显式记为 `blocked` + 保留人工脚本 | 不阻塞迭代其余部分 |
| 门禁上线后既有 tag 流程中断 | 无法发版 | 先在非发布分支验证一次完整 tag 流程 | 单 commit 回退 release workflow |
| 迁移中放宽断言以求通过 | 门禁形同虚设 | 迁移前后逐条比对断言集合，差异需说明 | 代码评审 + 交付约束 5 |

## 测试策略

| 层级 | 范围 | 命令 |
| --- | --- | --- |
| 1 静态与单测 | 格式、`go vet`、harness 自身单测 | `go vet ./...`；`go test ./internal/etl/e2e/harness/` |
| 2 包级 / `-race` | 不适用（本迭代不改运行时） | — |
| 3 后端矩阵 | SQLite / MySQL / PostgreSQL storage 门禁沿用 | `hack/e2e-storage-{mysql,postgres}.sh` |
| 4 容器 e2e | 首批 9 条路径 | `go test -tags=e2e ./internal/etl/e2e/ -e2e.strict` |
| 5 外部环境认证 | 不适用（属 PT-A） | — |

门禁自身的验证是本迭代的特殊项：需要**故意构造失败**来证明门禁有效（验收 1、2、5），
这三次构造必须留下 run URL，不能只声明「已配置」。
