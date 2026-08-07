# OpenETL-Go Production Readiness 代码审计报告

> 审计日期：2026-07-26
>
> 审计基线：[`0fea5fd908f368413828ef99b2239532521798a7`](https://github.com/a8851625/openetl-go/commit/0fea5fd908f368413828ef99b2239532521798a7)
>
> 基线版本：`v0.2.11-beta.3-28-g0fea5fd`
>
> 审计类型：只读代码与证据核对，不包含代码修改或 Roadmap 状态变更

## 重要说明

本文记录的是上述提交的 production readiness 审计快照。后续提交可能已经修复、替换或重构本文提到的实现；在将任一结论用于当前版本的发布认证前，必须重新执行对应代码核对、故障测试和外部环境认证。

## 结论

该审计基线不能移除 beta，也不能做项目级 “production ready” 声明。主要差距不是 connector 数量，而是仍存在可能导致静默丢数据、错误恢复、凭据泄露和发布误认证的边界。

| 范围 | 审计判断 |
| --- | --- |
| 项目整体 | 尚未 production ready |
| Standalone | 只能称限定路径的 production-candidate；必须明确 source/sink、storage backend、write mode、business key/upsert 和 at-least-once 边界 |
| Distributed | 继续保持 beta/candidate |
| MaxCompute | 继续保持 `experimental + blocked_external`，不应阻塞 standalone GA |

## 审计范围与方法

本次核对覆盖：

- checkpoint、source position、sink acknowledgement 和 crash/restart 边界；
- DLQ 保存、查询、删除与 replay 语义；
- SQLite/MySQL/PostgreSQL storage、迁移、backup/restore；
- API token、TLS、secret encryption、audit persistence；
- health、metrics、deployment profile 和 release gate；
- standalone/distributed 生命周期与 fencing；
- MySQL、ClickHouse 主要 sink 语义；
- Roadmap、生产执行计划、connector/path maturity 和现有测试证据的一致性。

目标单元测试在审计时通过：

```text
go test ./internal/etl/source ./internal/etl/sink ./internal/etl/pipeline ./internal/etl/server ./internal/etl/storage/... -count=1
```

这些单元测试不能替代 checkpoint storage 故障、外部位点推进、真实 crash、跨版本升级、乱序 replay 和远端 connector 认证。

## P0：主路径 production 声明前必须关闭

### 1. Checkpoint load/corruption fail-open

线性 Runner 只有在 `Load` 无错误时才使用 checkpoint；错误被忽略，随后以 `nil` checkpoint 打开 source：

- [`internal/etl/pipeline/pipeline.go:566`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/pipeline/pipeline.go#L566)

DAG 只记录 warning，并同样返回 `nil` checkpoint：

- [`internal/etl/orchestrator/executor.go:791`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/orchestrator/executor.go#L791)

MySQL CDC 在没有 checkpoint 时从当前 master position 开始：

- [`internal/etl/source/mysql_cdc.go:338`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/source/mysql_cdc.go#L338)

因此 checkpoint storage 短暂读取失败或 checkpoint JSON 损坏可能被误判为首次启动，并跳过尚未处理的 binlog。生产要求应为：无法读取或验证 checkpoint 时 pipeline fail-closed，不得启动 source。

最低验收：

1. checkpoint backend 返回错误时 pipeline 进入 failed/blocking 状态；
2. checkpoint envelope、source position JSON 损坏时不得回退到空位点；
3. API、health、run history 显示明确错误；
4. 覆盖 linear、DAG 和主要 CDC/Kafka source 的 fault test。

### 2. PostgreSQL WAL/Kafka external ack 早于 durable checkpoint

PostgreSQL `CheckpointForRecord` 在生成 checkpoint 时先更新 `committedLsn`：

- [`internal/etl/source/postgres_cdc.go:1133`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/source/postgres_cdc.go#L1133)

keepalive 使用该值发送 flush/apply acknowledgement：

- [`internal/etl/source/postgres_cdc.go:1051`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/source/postgres_cdc.go#L1051)

Kafka 同样在 `CheckpointForRecord` 内调用 consumer session commit：

- [`internal/etl/source/kafka.go:445`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/source/kafka.go#L445)

Runner 在这些操作之后才保存内部 checkpoint：

- [`internal/etl/pipeline/pipeline.go:1484`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/pipeline/pipeline.go#L1484)

如果进程在 external ack 成功后、checkpoint store Save 前崩溃，broker 或 replication slot 已前进，而内部恢复点仍旧，形成数据丢失窗口。

建议拆分为：

```text
生成 checkpoint（无副作用）
    -> durable checkpoint Save
    -> source external ack/commit
```

并增加精确的 sink-ack → checkpoint-save → external-ack crash 注入测试。

### 3. RestoreFromDB 静默跳过 pipeline

`RestoreFromDB` 对单条 pipeline 的 YAML 解析、connection resolution、spec validation 和 runner 创建错误多处使用 `continue`：

- [`internal/etl/server/server.go:474`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/server/server.go#L474)

结果是数据库中存在的 pipeline 可以在重启后完全不进入内存、API 和 health，但服务整体仍成功启动。

最低验收应二选一：

- 严格模式下 startup fail；或
- 将该 pipeline 持久化为 `restore_failed`/`failed`，保留在 API 与 health 中，并提供可操作错误。

### 4. Portable backup 存在明文 secret 导出路径

`SecretFieldStore.ListConnections/ListSettings` 返回解密后的明文：

- [`internal/etl/storage/adapters.go:590`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/storage/adapters.go#L590)

portable backup 直接通过 storage interface 读取连接和设置：

- [`internal/etl/storage/backup/backup.go:80`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/storage/backup/backup.go#L80)

当调用方传入 `Server.Storage()` 等 wrapped store 时，backup JSON 可能写入明文密码或 API key，而现有 backup 测试主要使用 raw SQLite store，未覆盖 wrapped-store security contract。

最低验收：backup 必须读取 raw encrypted rows，或在 export 层重新加密；增加 wrapped storage 导出测试，并扫描产物确保 plaintext secret 不存在。

### 5. Connection catalog 的 DSN secret 识别不完整

字段级加密依赖字段名模式，列表中没有 `dsn`：

- [`internal/etl/storage/secret_fields.go:8`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/storage/secret_fields.go#L8)

JDBC connection descriptor 的 `dsn` 也没有 `Secret: true`：

- [`internal/etl/server/schema.go:402`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/server/schema.go#L402)

含用户名和密码的 JDBC DSN 可能在 storage row、API masking、SQL dump 或 portable backup 中作为普通字符串出现。应统一使用 descriptor `Secret` metadata 驱动保存、加密、mask、导出和轮换，避免仅靠字段名猜测。

### 6. ClickHouse ReplacingMergeTree 版本不代表源事件顺序

native 与 HTTP insert 都为 `_version` 调用 process-local `nextVersion()`：

- [`internal/etl/sink/clickhouse.go:735`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/sink/clickhouse.go#L735)
- [`internal/etl/sink/clickhouse.go:770`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/sink/clickhouse.go#L770)

较旧事件如果稍后通过 DLQ replay 或 checkpoint reset 重放，会获得更大的 `_version`，可能覆盖更新后的状态。DELETE 使用物理 mutation 时，旧 INSERT replay 还可能重新生成已删除数据。

最低验收：

1. 使用稳定、可比较的 source ordering/version；或明确阻断不安全的乱序 replay；
2. 覆盖旧 UPDATE 晚于新 UPDATE、旧 INSERT 晚于 DELETE 的测试；
3. 在文档中明确无法建立全序时的 duplicate/reconciliation 边界。

### 7. ClickHouse async insert durability 默认值不一致

runtime 构造函数中 `asyncInsertWait` 零值为 `false`，descriptor 却声明默认 `true`：

- [`internal/etl/sink/clickhouse.go:93`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/sink/clickhouse.go#L93)
- [`internal/etl/server/schema.go:330`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/server/schema.go#L330)

当调用方只设置 `async_insert=true` 且没有物化 descriptor 默认值时，runtime 会发送 `wait_for_async_insert=0`。sink 可能在 ClickHouse durable 接收前返回成功，破坏 checkpoint 的 sink acknowledgement 假设。

生产 profile 应强制等待，或阻断 `async_insert=true && async_insert_wait=false`。

## P1：Standalone GA 与发布门禁阻断项

### 生命周期 desired state 与 checkpoint reset 缺少 fencing

- start/stop 没有可靠持久化 `running/stopped` desired state；pause/resume 的 storage error 被忽略；
- startup `StartAll` 无视 persisted row status，用户停止或暂停的 pipeline 可能在重启后再次运行；
- reset/set 可对正在运行的 pipeline 调用，in-flight checkpoint 可能马上覆盖 reset；
- generic checkpoint reset 只删除内部 checkpoint：[`internal/etl/server/server.go:3206`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/server/server.go#L3206)，并不等价于 OpenAPI 声称的“从头开始”。

各 source 实际语义不同：Kafka 仍受 broker consumer-group offset 控制，MySQL CDC 通常从当前 master position 开始，PostgreSQL 又受 replication slot 已确认位置约束。API 必须提供 source-specific reset 语义，并要求 pipeline stopped/paused 或使用 generation fencing。

### Linear DLQ replay contract 不完整

linear replay 总是使用当前 spec 和完整 transform chain：

- [`internal/etl/server/server.go:4322`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/server/server.go#L4322)

主要缺口：

- 运行时未可靠填充 `PipelineVersion`；
- DLQ row 不包含失败 stage/node；
- sink failure 保存的是已 transform record，replay 却再次运行完整 transform；
- `flat_map` 多输出依赖 `ApplyBatch`，linear replay 调用单条 `Apply`：[`internal/etl/transform/flat_map.go:101`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/transform/flat_map.go#L101)；
- 未注入与生产运行一致的 state namespace/defaults；
- transform chain 未可靠 close，可能泄漏 Redis/SQL/goroutine 资源。

需要定义并持久化：pipeline version、失败 node/stage、record 处于 raw/processed 哪个阶段、state namespace、replay 目标和兼容策略。

### DAG source 启动失败未传播为 pipeline failed

source `Open` 失败只记录日志并让 goroutine 返回：

- [`internal/etl/orchestrator/executor.go:339`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/orchestrator/executor.go#L339)

多 source DAG 可能只执行剩余 source，形成部分执行，最终状态还可能表现为 stopped 而非 failed。生产要求应默认 fail pipeline，除非 DAG spec 明确声明允许 partial source execution。

### DLQ、audit、settings 与 health 存在 fail-open/false-green

- DLQ list/delete storage error 多处返回 JSON error body 但 HTTP 仍可能是 200；
- 全量删除使用未知计数时可能将 `-1` 写入 delete counter；
- SQL DLQ 遇损坏 JSON row 直接跳过，坏记录对用户不可见；
- audit write error 被丢弃：[`internal/etl/server/server.go:5002`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/server/server.go#L5002)；
- checkpoint Load error 被当作 age=0，DLQ Count error 被当作 count=0；
- running pipeline 从未产生 checkpoint 时也可能显示非 stale；
- source/sink latency 已采集，但 health 没有用于 stuck 判定。

生产 profile 强制 audit 或 storage health 时，相应持久化失败必须影响 API 结果和 overall health，不能只写日志或返回默认零值。

### Backup/restore 完整性不足

portable backup 对 DLQ、audit、run history 使用固定 `100000` limit：

- [`internal/etl/storage/backup/backup.go:105`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/storage/backup/backup.go#L105)

restore 先 clear，再逐表逐行写入，没有全局事务：

- [`internal/etl/storage/backup/backup.go:299`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/storage/backup/backup.go#L299)

其他缺口包括：

- pipeline version/ID 未原样保留；
- run history 通过重新 RecordStart/End 重建，时间、ID、状态可能改变；
- reconciliation 主要比较计数，不验证完整内容；
- plugin backup 只保留 metadata/path，不复制 WASM artifact；
- Redis transform state 不在统一 backup/restore drill 内；
- 普通维护者缺少正式 backup/restore CLI 或 API。

### Legacy file migration 静默忽略失败

legacy migration 对目录读取、文件读取、JSON decode、Save 和 scanner error 存在多处忽略：

- [`internal/etl/storage/factory/migrate.go:17`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/storage/factory/migrate.go#L17)

应返回成功、失败、跳过清单，保留未迁移文件标记，并在不可安全继续时阻断启动。

### Distributed fencing 与 persistence 仍有 fail-open

worker CAS 失败后存在无条件 `UpdateTask` fallback：

- [`internal/etl/worker/poll.go:101`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/worker/poll.go#L101)

master stale task 扫描对 List/Update error 处理不足：

- [`internal/etl/master/dispatch.go:240`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/master/dispatch.go#L240)

同时 production profile 明确拒绝 worker：

- [`internal/etl/server/runtime_profile.go:58`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/server/runtime_profile.go#L58)

因此 distributed profile 在该审计基线不能宣称 production ready。

### Release gate、deployment profile 与证据治理不一致

tag release workflow 只执行 checkout、Go setup 和 GoReleaser，没有依赖 production test gate：

- [`.github/workflows/release.yml`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/.github/workflows/release.yml)

其他证据缺口：

- `hack/e2e-path-contract-smoke.sh` 默认主要检查文档/脚本存在，完整主路径需要显式 full mode；
- production gate 中 skip 不必然阻断发布；
- 根 `docker-compose.yml` 强制 production profile/token/spec key/TLS/audit，而 `deploy/production` 的 profile/TLS 语义不同；
- distributed compose 默认 HTTP/development 风格，与 production worker restriction 冲突；
- `PathContract.LastCertified` 未填充，maturity 主要依赖静态字符串和脚本路径；
- production plan 的证据 commit 与实际 HEAD 漂移；
- UI 文档记录 `108 passed / 0 failed`，而审计时当前证据为 `91 passed / 17 failed`；
- resource baseline 多为估算或目标值，不是当前 release 的实测记录。

## 已确认的正面基础

### MySQL sink

MySQL sink 的 batch write 使用 transaction，pre-write 也在同一事务边界内：

- [`internal/etl/sink/mysql.go:330`](https://github.com/a8851625/openetl-go/blob/0fea5fd908f368413828ef99b2239532521798a7/internal/etl/sink/mysql.go#L330)

MySQL upsert 使用 `ON DUPLICATE KEY UPDATE`，在稳定 primary/business key 下能够吸收 at-least-once replay。该事务不包含 source acknowledgement 和 checkpoint store，因此仍不能宣称三方 exactly-once。

### ClickHouse 与现有 E2E

ClickHouse native batch、HTTP protocol、auto-create、schema drift、ReplacingMergeTree 和 DLQ/replay 已有实际实现及 e2e 基础。仓库也已经覆盖多种 snapshot/CDC、crash/restart、storage backend、DLQ、Kafka 和 distributed 场景。

这些基础说明项目已经超过 demo 阶段，但现有 e2e 尚未覆盖本报告指出的 checkpoint load failure、external ack 顺序、乱序 replay、backup plaintext、audit/storage fail-open 和上一稳定版本升级边界。

### 产品语义

继续明确声明 at-least-once 是正确的。生产指导应依赖 business key、版本、upsert、ReplacingMergeTree-style sink 或显式 deduplication 来吸收 replay，而不是升级为 exactly-once 宣称。

## 验收矩阵

| Criterion | Evidence | Result | Residual / blocker |
| --- | --- | --- | --- |
| checkpoint load/corrupt fail-closed | linear/DAG load 代码与缺失 fault test | fail | 实现阻断启动、结构校验和 fault injection |
| external ack 晚于 durable checkpoint | PG/Kafka checkpointer 与 Runner Save 顺序 | fail | 重构 ack 生命周期并增加精确 crash test |
| MySQL transaction/upsert | MySQL sink transaction/upsert 实现及现有测试 | partial pass | 无 source/sink/checkpoint 三方原子性 |
| ClickHouse replay ordering | `_version=nextVersion()` 与现有顺序 replay e2e | fail | source ordering/version 与乱序 update/delete 测试 |
| portable backup secret safety | wrapped store decrypt + backup interface export | fail | raw encrypted export 或重新加密，补 security test |
| backup/restore integrity | backup package、restore 与 reconcile 实现 | partial | 100k 截断、非原子、ID/version/history/artifact/state |
| lifecycle/reset/recovery | start/stop/StartAll/reset 代码 | fail | desired state、generation fencing、source-specific reset |
| release production gate | tag release workflow | fail | tag 必须依赖 mandatory production gate，skip 不得算 pass |
| UI first-run | 审计时记录 `91 passed / 17 failed` | fail | 修复失败并同步文档证据 |
| MaxCompute remote certification | 无真实 SDK writer/credential e2e | blocked_external | 外部凭据、权限/表检查和远端认证 |

## 最小 GA 前置顺序

1. 修复 checkpoint fail-closed、PostgreSQL/Kafka external ack 顺序和 DB restore 可见性。
2. 修复 DSN/secret metadata、portable backup 明文风险和原子 restore。
3. 修复 ClickHouse source-order version，并强制 async insert 等待 durable acknowledgement。
4. 补齐 lifecycle/reset fencing、linear DLQ replay、audit/health/storage persistence error contract。
5. 对当前声明的主要 standalone paths 执行完整 crash、reset、outage、DLQ、duplicate absorption 和乱序 replay 矩阵。
6. 将 production gate 接入 tag release，统一根 compose、`deploy/production` 和 distributed profile/TLS 语义。
7. 完成 SQLite/MySQL/PostgreSQL 从上一稳定版本的真实升级与 backup/restore drill。
8. 更新 `LastCertified`、证据 commit、UI 结果和实测 resource baseline，再评估 standalone 移除 beta。
9. Distributed 单独完成 fencing、worker production profile、crash reassignment 和多 backend 认证，不与 standalone GA 捆绑宣称。

## 最终判断

该审计基线具备较完整的 CDC/ETL runtime 骨架，若限定在经过验证的 source → transform → sink 路径、稳定业务键/upsert 和明确的 at-least-once 运维约束下，可以作为 production-candidate 继续试运行。

但在 checkpoint fail-open、external ack 顺序、secret/backup、ClickHouse ordering、restore/lifecycle 和 release evidence 门禁关闭前，不应移除 beta，也不应做项目级或 distributed production-ready 声明。
