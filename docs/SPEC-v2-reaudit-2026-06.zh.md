# openetl-go 生产就绪 SPEC · v2 复审中文版

> **状态**：Stable v2（已复审）· 2026-06-21
> **范围**：定义 openetl-go 达到生产就绪所需满足的架构、命令、测试、边界和缺口清单。
> **英文版**：[`SPEC-v2-reaudit-2026-06.md`](./SPEC-v2-reaudit-2026-06.md)。

本文是英文复审 SPEC 的中文对应版本。编号、严重级别和结论与英文版保持一致；实现、评审和修复时应以相同 ID 追踪同一问题。

## 1. 目标

openetl-go 是一个**单二进制、插件化 ETL/CDC 引擎**，核心抽象为：

```text
Source -> Transform -> Sink
```

它从数据库、消息流、文件和 HTTP 中读取数据，经过可组合的 transform 链，写入分析型或在线存储。`internal/etl/` 是当前主产品面；`internal/logic/sync/` 中旧 Canal 路径仅保留兼容能力，不再作为新功能入口。

### 1.1 两种运行模式

| 模式 | 存储 | 分布式能力 | 适用场景 |
| --- | --- | --- | --- |
| Demo / 单机 | `sqlite`（默认） | 降级为进程内执行 | 本地评估、单机同步、CI。 |
| 可扩展 | `mysql` 或 `postgresql` | master-worker 分片执行 | 生产横向扩展。 |

复审时发现原先的“可扩展模式已分布式执行”说法被夸大。2026-06-22 的 A11-redo 已补齐 `etl.role` / `ETL_ROLE`，支持 `standalone`、`master`、`worker` 三种角色，master 通过 `task_assignments` 分配 shard，worker 轮询执行并上报 heartbeat。该能力已有 3 个 MySQL 集成测试证明 4 个 shard 可在 2 个 worker 上无重叠执行，并能在 worker 崩溃后重分配。

### 1.2 三个平衡目标

1. **数据同步可靠**：at-least-once 投递、幂等 sink、无静默数据丢失。
2. **易用**：自动建表、schema 校验、启动前检查、清晰错误信息。
3. **轻量**：单二进制、默认纯 Go、低资源占用、最少外部依赖。

### 1.3 生产就绪定义

openetl-go 被称为生产就绪前，必须在单机和可扩展两种模式下满足：

- 写入路径不静默丢数据；失败记录进入 DLQ，DLQ 自身失败时必须升级告警并停止推进。
- 崩溃或 SIGTERM 后不丢失已提交数据，重复投递不超过 sink 幂等容忍范围。
- schema 变化要么自动应用，要么明确拒绝，不能静默丢字段。
- `/api/v2/health`、`/metrics` 和运行状态能真实反映健康与能力。
- MySQL/PostgreSQL 存储模式下能真实跨 worker 分配 shard，并基于 heartbeat 重分配。
- 新用户可以在 5 分钟内跑通 SQLite 模式下的 MySQL -> ClickHouse 快速开始。

## 2. 命令

| 命令 | 用途 |
| --- | --- |
| `make build` | 使用 GoFrame (`gf build -ew`) 构建单二进制。 |
| `go build -o bin/openetl-go .` | 不依赖 GoFrame CLI 的普通 Go 构建。 |
| `./openetl-go` | 使用 `manifest/config/config.yaml` 启动。 |
| `make test` | 带 `-race` 的单元测试。 |
| `make test-quick` | 快速测试 `./internal/etl/...`。 |
| `make test-integration` | 依赖 MySQL + ClickHouse 的集成测试。 |

集成测试使用 `integration` build tag。修改 source/sink/storage/dispatch 时，需要按影响面补相应集成或 e2e 证明。

## 3. 架构边界

### 3.1 项目结构

```text
openetl-go/
├── main.go, internal/cmd/                 # 入口与启动
├── internal/etl/                          # 主 ETL 产品面
│   ├── core/                              # Source/Sink/Transform/Record 接口
│   ├── pipeline/                          # Runner、ParallelRunner、breaker、metrics
│   ├── orchestrator/                      # DAG executor
│   ├── source/, sink/, transform/         # 插件实现
│   ├── storage/{sqlite,mysql,postgres}/   # 元数据存储
│   ├── server/                            # HTTP API、reconcile、hot reload
│   └── master/, worker/                   # 分布式分片执行
├── internal/logic/{app,sync,monitor}/     # app bootstrap + 旧 Canal + monitor
├── pipes/                                 # 示例 pipeline spec
├── manifest/config/config.yaml            # 默认配置
├── hack/                                  # e2e、发布和辅助脚本
└── docs/                                  # 用户与审计文档
```

### 3.2 插件契约

每个 source/sink/transform 实现 `internal/etl/core/core.go` 中的小接口，并通过 registry 注册。能力应由 typed optional interface 声明，例如：

- `core.SchemaDescriptor`：source 暴露输出 schema。
- `core.SchemaValidator`：sink 在启动前校验 schema。
- `core.SinkMetricsProvider`：sink 暴露写入指标。
- `core.RecordCheckpointer`：reader 提供逐记录 checkpoint。

规则：能力以接口实现为准，不以 `server.go` 中的 metadata 字符串为准。metadata 只能作为 UI 和文档提示。

### 3.3 存储边界

`storage/factory.NewStore` 按配置选择 SQLite/MySQL/PostgreSQL。生产路径中的 pipeline spec、checkpoint、DLQ、audit、run history、worker registry 和 plugin state 都必须通过 `storage.Storage` 访问。旧的文件型 checkpoint/DLQ writer 只能用于迁移或兼容，不应被新生产路径调用。

## 4. 代码风格与错误处理

- 默认 Go 1.24、`gofmt`、`goimports`。
- I/O 函数必须接收并尊重 `context.Context`。
- 错误需要用 `%w` 包装，并通过错误类别区分 transient/data/schema/auth/config/programming。
- 用户可见错误必须说明“什么失败、在哪里失败、为什么失败、如何修复”。
- 并发状态必须由 mutex 或 atomic 保护；pipeline 通道必须有背压。
- Transform 不应在未复制的情况下跨链路共享和修改 `Record.Data` map。

## 5. 测试策略

| 层级 | 范围 | 要求 |
| --- | --- | --- |
| 单元测试 | 纯函数、接口、DDL、路由、retry、breaker | PR 必须通过，默认 `-race`。 |
| 集成测试 | 真实数据库 source/sink、checkpoint、auto-create | 修改插件时必须覆盖对应插件。 |
| E2E | 完整 pipeline、崩溃恢复、DLQ replay、分布式 | 修改核心链路或运行模式时必须覆盖。 |

核心不变量：

- checkpoint 只在 sink 写成功后推进。
- 同一批次重放到幂等 sink 不产生重复业务结果。
- SIGTERM 或 crash 后可安全恢复。
- 强制 sink 失败时记录进入 DLQ，不能消失。
- `go test -race` 不应报告数据竞争。

## 6. 不可破坏的边界

### 6.1 必须做到

- 默认构建为单静态二进制、纯 Go；CGO/Extism/QuickJS 等能力必须是 opt-in。
- 所有写路径零静默丢失，包括 DAG、shutdown、DLQ 写失败场景。
- Lua/QuickJS 等脚本运行时必须有 CPU/时间/内存预算。
- YAML spec 保持向后兼容；新增字段需要合理默认值。
- SQLite 与 MySQL/PostgreSQL 在核心能力上保持一致，分布式执行除外。
- “已完成”必须同时满足代码实现、测试证明、文档/metadata 与实现一致。

### 6.2 需要先确认

- 新增重量级依赖。
- 修改 `core.Source`、`core.Sink`、`core.Transform` 等插件 ABI。
- 默认自动应用 DDL。
- 引入 etcd/Zookeeper/Kubernetes operator 等额外必需服务。

### 6.3 禁止事项

- 不为核心功能引入外部 orchestrator。
- 不为让测试通过而静默丢弃数据。
- 不破坏 unit/integration 分层。
- 不继续分叉 `internal/etl/` 与旧 `internal/logic/sync/` 两条产品路径。
- 不宣称 SQLite 模式具备分布式保证。
- 不在请求路径动态拉取未固定版本的工具。
- 不在 transform 链中无防御复制地原地修改共享 `Record.Data`。

## 7. 生产就绪缺口清单

### Tier A：生产就绪声明前必须完成

| ID | 缺口 | 状态 |
| --- | --- | --- |
| A1 | LogBuffer 格式化参数丢失 | 已完成 |
| A2 | DAG 条件操作符 Gt/Lt/Ge/Le/Regex 缺失 | 已完成 |
| A3 | Sink auto-create 曾经 all-TEXT | 部分完成：MySQL/PG/JDBC/Doris 已修复；其余 connector 证据独立跟踪 |
| A4 | Redis source 使用阻塞 `KEYS` | 已完成，改为 SCAN |
| A5 | Prometheus counter/gauge 类型错误 | 已完成 |
| A6 | ClickHouse `_version` 并发下非单调 | 已完成 |
| A7 | DDL apply 将源 DDL 原样下发到目标 | 部分完成：ClickHouse 已接 translator；Doris 默认 reject，apply 仅允许安全 ADD COLUMN；MySQL/PG 仍需单独复核 |
| A8 | Schema mismatch 运行时静默失败 | 部分完成：Doris 已实现 `SchemaValidator` 和 Unique Key/model preflight；全量 connector 覆盖仍需继续 |
| A9 | per-sink metrics 只有 ClickHouse | 已完成：内置 sink 通过 `sinkCounters` 暴露指标 |
| A10 | MySQL/PG storage backend 未验证 | 已完成 |
| A11 | Master-worker 分布式执行 | 已完成 A11-redo，仍建议补三进程 e2e |
| A12 | `make test`/CI 脚手架 | 已完成 |

### Tier A.1：复审新增 P0

| ID | 严重级别 | 问题 | 修复方向 |
| --- | --- | --- | --- |
| P4-1 | P0 | DAG executor 忽略 DLQ 写失败并增加 `RecordsDLQ` | 对齐 linear Runner：DLQ 写失败时告警、停止推进 checkpoint、触发 breaker。 |
| P4-2 | P0 | `Stop()` 在约一半情况下用已取消 context flush in-flight batch | EOF/shutdown flush 使用新的 background timeout context，或 Stop 先等待 done 再 cancel。 |
| P4-3 | P0 | Lua transform 无内存/CPU/时间预算 | 增加 instruction hook、context 中断和内存限制。 |
| P4-4 | P0 | QuickJS transform 只有内存限制，无 CPU/时间预算 | 使用 interrupt handler 并尊重 context。 |
| P4-5 | P0 | `npx --yes extism-js` 动态拉取且插件名未校验 | 校验 `^[A-Za-z0-9_.-]+$`，禁止路径穿越，固定工具版本或使用预装工具。 |

### Tier A.2：复审新增 P1

| ID | 问题 | 修复方向 |
| --- | --- | --- |
| P4-6 | `Runner.lastRecordAt` 数据竞争 | 加锁读取或改 atomic unix nano。 |
| P4-7 | `retry` 在 `InitialInterval == 0` 时 panic | 对 interval 做下限保护和配置校验。 |
| P4-8 | 文件 DLQ writer 不 `fsync` | 写入后 sync，或增加可配置批量 sync。 |
| P4-9 | DAG executor 忽略 checkpoint 保存错误 | 对齐 linear Runner：记录错误、告警、停止推进。 |
| P4-10 | DLQ replay 按秒级时间窗口删除，可能误删/重复 | 使用稳定 ID，逐条成功后删除。 |
| P4-11 | preflight sink reachability 是 no-op | 短超时调用 `Open`/`Ping` 或复用 connection test。 |
| P4-12 | deduplicator transform 无 mutex | 增加互斥保护缓存与游标。 |
| P4-13 | join inner miss 用 `ErrRecordFiltered` 静默丢记录 | 增加 `on_miss: drop|dlq|error`，inner join 默认进入 DLQ 或报错。 |
| P4-14 | window 无 watermark，sliding/session 不应在 pipeline spec 中宣称 | 增加 watermark/allowed lateness，并拒绝未支持的 sliding/session 配置。 |
| P4-15 | transform/route 缺逐记录 panic recovery | 每条记录的 Apply/route 周围 recover，失败进入 DLQ 或标记 pipeline failed。 |
| P4-16 | enricher 吞掉错误且 cache 无界增长 | 返回错误并进入重试/DLQ，增加 cache 清理。 |
| P4-17 | `mysql_batch` 提前 done 的指控复核为误报 | 无需修改。 |
| P4-18 | `ListTasks` 固定 50 行导致旧 pending task 不可见 | 增加 pending/running 专用查询或清理 finished task。 |

### Tier B：开源发布强烈建议

| ID | 缺口 | 状态 |
| --- | --- | --- |
| B1 | 5 分钟 quickstart | 已完成 |
| B2 | 启动前检查 | 部分完成，sink reachability 仍无效，错误被降为 warning |
| B3 | JSON 结构化日志 | 已完成：`LOGGER_FORMAT=json` 在启动早期安装 GoFrame JSON stdout handler |
| B4 | `postgres_cdc` TRUNCATE/DDL 完整语义 | 部分完成，TRUNCATE 不再中断但未同步目标清空语义 |
| B5 | S3 multipart/retry、ES cluster/429 retry | 已完成，S3 Parquet 类型丢失仍是 SK-5 |

### Tier B.1：复审新增 P2

| ID | 问题 | 修复方向 |
| --- | --- | --- |
| P4-19 | `SchemaValidator`/`SchemaDescriptor` 无实现者 | 在关系型 source/sink 实现，或移除接口与声明。 |
| P4-20 | 非 ClickHouse sink 无 `SinkMetricsProvider` | 已完成：内置 sink 通过共享 `sinkCounters` 暴露指标并在成功写入后记录。 |
| P4-21 | S3 Parquet 所有列写成 String | 根据样本值推断 Parquet 类型。 |
| P4-22 | WASM runtime 默认链接 | 拆分 build-tagged extism 包。 |
| P4-23 | router 覆盖 `Metadata.Source` | 新增 route 字段，保留 provenance。 |
| P4-24 | transform chain 共享 `Record.Data` map | 链路入口 deep copy 或制定不可变契约并强制执行。 |
| P4-25 | Lua `os` 仅删字段未置 nil，且 type assert 可能 panic | `L.SetGlobal("os", lua.LNil)`，使用 comma-ok assert。 |
| P4-26 | filter 类型不匹配时静默过滤 | 增加 `strict_types`，不匹配时报错/DLQ。 |
| P4-27 | `MarkOffline` 删除 worker 行 | 改为状态置 `offline`，显式退出才 deregister。 |
| P4-28 | `SavePipelineVersion` read-then-write 竞态 | 使用原子 insert-select 或 duplicate retry。 |
| P4-29 | failed task 不重试/不重分配 | 增加 retry counter 和 requeue 策略，或暴露终态失败。 |

## 8. 变更控制

- 本 SPEC 为 v2。修改目标、插件 ABI 或硬边界时，需要版本升级和用户确认。
- 进度可在 TODO/ROADMAP 中跟踪，但生产就绪判断以本 SPEC 为准。
- “已完成”可以被复审撤回；一旦证据反驳原声明，必须在同一 release 中记录。

## 9. 复审发现登记

本节保留英文版 §9 的问题证据结构，便于按 ID 修复。更完整的原始证据、文件行号和英文表述见英文版。

### P0：数据丢失 / 安全

- **PC-1 / TF-9**：DAG executor 在 `orchestrator/executor.go` 中忽略 `WriteDLQ` 错误，却增加 DLQ 计数。修复：DLQ 写失败必须阻止 checkpoint 推进并触发告警。
- **PC-2**：`pipeline.go` 的 Stop/flush 分支可能用已取消 context 写最后一批。修复：flush 使用新的 timeout context。
- **TF-1**：`transform/lua.go` 的 Lua `PCall` 不受 context 控制。修复：instruction/time/memory budget。
- **TF-2**：`transform/ts.go` QuickJS 无 CPU/time interrupt。修复：interrupt handler + context。
- **TF-3**：`server.go` 和 plugin manager 中动态 `npx --yes` 与未校验插件名组合导致供应链和路径穿越风险。修复：固定工具、校验名称、禁止 `/` 和 `..`。
- **ST-2**：复审时 master-worker 只是架构声明，非真实分布式执行。A11-redo 已补实现，但仍建议补三进程自动化 e2e。

### P1：正确性 / 安全网

- **PC-3**：`Runner.lastRecordAt` 写入加锁、读取未加锁；`-race` 可报。修复：锁或 atomic。
- **PC-4**：retry 初始间隔为 0 时 `rand.Int63n(0)` panic。修复：clamp interval。
- **PC-5**：文件 DLQ 不 fsync，崩溃可能丢 DLQ。修复：写后 sync。
- **PC-6**：DAG `routeAndWrite` 取消时 flush 使用已取消 context。修复：新 timeout context。
- **PC-7**：DAG checkpoint 保存错误被 `_ =` 忽略。修复：对齐 linear Runner。
- **SV-1**：DLQ replay 按秒级 timestamp 删除。修复：使用稳定 ID。
- **SV-2**：preflight 只构造 sink，不打开连接。修复：短超时 Open/Ping。
- **TF-6**：deduplicator cache/map 无锁。修复：互斥保护。
- **TF-7**：join inner miss 被当成正常 filter。修复：可配置 miss 策略。
- **TF-8**：window 无 watermark，滑动/会话能力不应在 spec 中宣称。修复：保留 tumbling-only，并拒绝未支持的 sliding/session 配置。
- **TF-10**：transform/route 缺逐记录 panic recovery。修复：每条记录 recover 并转入失败处理。
- **TF-13**：enricher 吞错误并缓存无界。修复：返回错误、清理过期缓存。
- **SRC-5**：`mysql_batch` 提前结束是误报，已撤回。
- **ST-1**：task 查询固定 LIMIT 50。修复：按状态查询活跃任务或增加清理器。

### P2：宣称缺失 / 打磨项

- **SK-1**：ClickHouse auto-create 绕过 typing。修复：接入 `typing.InferFromValue`。
- **SK-2（已完成）**：Doris auto-create 已传入代表性 field values，并有 Doris DDL/类型推断单测覆盖。
- **SK-3**：schema optional interfaces 无实现者。修复：实现或删除。
- **SK-4（已完成）**：内置 sink 通过共享 `sinkCounters` 暴露指标。
- **SK-5**：S3 Parquet 全部 string。修复：按值类型映射。
- **SV-3**：preflight error 被降级为 warning。修复：`valid:false` 并返回 errors。
- **SV-4**：AI generation handler 未挂路由且输出未 validate。修复：注册路由并接 ValidateSpec/RunPreflight。
- **SV-6（已完成）**：`internal/logic/app/logging.go` 在 `LOGGER_FORMAT=json` 时安装 JSON stdout handler。
- **TF-4**：WASM 默认链接。修复：build tag 隔离。
- **TF-5**：router 覆盖 provenance。修复：新增 route 字段。
- **TF-11**：transform chain 共享 Data map。修复：deep copy。
- **TF-12**：Lua `os` sandbox 不彻底。修复：全局置 nil。
- **TF-14**：filter 类型不匹配静默 drop。修复：strict mode。
- **ST-3**：MarkOffline 删除 worker 历史。修复：状态置 offline。
- **ST-4**：pipeline version 保存竞态。修复：原子分配版本。
- **ST-5**：failed task 不重试。修复：retry/requeue 策略。
- **SRC-2**：postgres TRUNCATE 不同步目标语义。修复：合成 truncate/delete record 或明确限制。

### P3：非阻塞打磨

P3 包括 alert queue overflow 时绕过结构化日志、冗余 `contains` 分支、JDBC/Doris 死代码或 label 冲突、MySQL `VALUES(col)` 兼容性、SQLite DLQ LIMIT 拼接、单实例 rate limiter 文档、若干 source close/context/checkpoint 细节，以及 registry 缺能力 metadata。它们不阻塞生产就绪主线，但应在相关模块变更时顺手清理。

## 10. 已确认稳固的能力

复审确认以下能力已有实际代码和测试基础：

- linear Runner 的 at-least-once checkpoint 顺序。
- linear DLQ 写失败升级处理。
- 三态 circuit breaker。
- retry backoff、jitter、context 和错误分类。
- DAG 条件操作符完整。
- readLoop/writeLoop panic recovery。
- Kafka 写成功后推进 offset。
- HTTP source checkpoint 恢复与 429/5xx backoff。
- file CSV/JSON checkpoint。
- Redis SCAN streaming。
- MySQL CDC server_id、时区处理。
- snapshot+CDC 两阶段一致性。
- PostgreSQL CDC late ACK LSN 和 reconnect backoff。
- ClickHouse `_version` 原子单调。
- MySQL/PG/JDBC 事务批量写。
- Kafka idempotent producer。
- ES round-robin + Retry-After。
- S3 5xx retry。
- ClickHouse DDL translator 接线。
- SQLite/MySQL/PostgreSQL storage conformance。
- health 503、secrets masking、auth constant-time compare、Prometheus metric 类型、AES-256-GCM spec encryption、hot reload graceful degradation。
