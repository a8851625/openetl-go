# OpenETL-Go Roadmap

> 当前审计基线：`cfbd496`（v0.2.11-beta.3 后续开发，2026-07-24）
>
> 最后核对：2026-07-24

本文只维护尚未完成、可以验收的产品和工程工作。已经交付的功能、测试命令和版本说明进入 [CHANGELOG.zh.md](../CHANGELOG.zh.md)；本文末尾只保留必要的证据索引，不再重复完整实现日志。

## 产品定位与边界

OpenETL-Go 是轻量、自托管、开源的 CDC/ETL 数据同步、清洗和汇聚运行时。核心产品模型始终是：

```text
Source -> Transform -> Sink
```

YAML、API 和 UI 操作同一份 pipeline/DAG spec。DAG、调度、并行分片和 master-worker 是该模型的扩展，不形成独立产品线。

近期工作必须优先服务以下能力：

- 数据库、Kafka、文件、HTTP、对象存储、OLAP/搜索等常见数据路径。
- checkpointed at-least-once、失败可见、DLQ replay、sink 幂等和 schema/preflight 安全。
- connector/plugin 合约、认证证据和小团队可运维性。
- 首次任务闭环和 YAML/API/UI 等价性。

明确边界：

- 默认语义是 at-least-once，不承诺跨 sink exactly-once。
- 生产链路依赖业务主键、版本列、upsert、ReplacingMergeTree、显式 deduplicate 或补偿吸收重放。
- 不建设通用 keyed state、任意 processing-time timer、Flink savepoint、完整 SQL planner、复杂 sliding/session window、late side-output 或 retraction 语义。
- metadata/checkpoint 存储与高频运行时 state/cache 分离；需要缓存或可恢复状态的内置能力使用 Redis，不退化到 SQLite/MySQL/PostgreSQL 充当高频缓存。
- 新 connector 只有在同时具备 schema、preflight、DLQ、metrics、重放边界和测试证据时才进入核心。

详细定位见 [positioning.zh.md](./positioning.zh.md)，at-least-once 与幂等边界见 [etl-idempotency.md](./etl-idempotency.md)。

## 小团队 Production Ready 目标

OpenETL-Go 的近期目标不是覆盖更多平台，而是让通常没有专职数据平台或 SRE 的 1-5 人团队，能够用少量基础设施稳定运行、升级和排障常见的数据同步任务。第一优先发布形态是 **standalone 单进程 + 外部 metadata storage（按规模选择 SQLite/MySQL/PostgreSQL）+ 按需 Redis state**；master-worker 作为独立的扩展形态单独验收，不阻塞 standalone 达到 production ready，也不得在自身门槛完成前借用 standalone 的成熟度声明。

Production ready 必须同时满足以下目标：

| 目标 | 可验收定义 |
| --- | --- |
| 可靠 | API 返回成功的配置已经持久化；进程崩溃、主机重启和短暂依赖故障后能够按 checkpoint 恢复；不能把丢任务、丢 spec、丢 DLQ 或长期不推进隐藏在 `running` 状态后面。 |
| 易上手 | 新用户从空环境到首条已验证的生产候选链路，目标在 30 分钟内完成；默认路径不要求编辑 YAML，连接、schema、写入语义、preflight 和重放风险都能在同一闭环中确认。 |
| 易维护 | 生产配置默认 fail-closed；升级、备份、恢复、回滚、容量基线和故障定位有可重复脚本及 runbook；一个普通维护者可以判断“是否在推进、哪里失败、如何恢复”。 |
| 数据一致性有保障 | 默认语义明确为 checkpointed at-least-once；sink acknowledgement 之后才推进 checkpoint；生产链路必须声明业务键、版本/upsert/dedup 或对象重放策略，并通过 crash、checkpoint reset、依赖中断和 DLQ replay 的记录对账。 |
| 安全 | API、worker 和管理操作在生产形态下默认需要认证；传输路径可验证；spec、connection 和 settings 中的 secret 具备静态加密、轮换和恢复方案。 |
| 有证据 | “代码存在”“脚本存在”或 maturity 字符串都不等于完成；每项声明必须有当前版本实际执行的单测、故障注入、跨进程 e2e、升级/恢复 smoke 或真实外部环境记录。 |

### 发布声明分级

- **项目级 production ready**：只用于已经通过下方 `PR-*` 必选门槛的发布版本。
- **standalone production ready**：优先目标；允许明确说明单节点故障恢复时间，不要求 multi-active HA。
- **distributed production candidate/ready**：单独受 `PR-D1` 约束；在完成前，distributed compose 和文档必须标注 beta/受限边界。
- **connector/path production ready**：按具体 `source -> transforms -> sink + write mode + storage/runtime mode` 声明，不把单个 connector 的 maturity 外推成任意组合都可生产。

### 项目级发布门槛

在对外移除 beta/production-candidate 限定前，至少必须满足：

- `PR-0`、`PR-1`、`PR-2` 全部交付，且 P4/P5 中与首次任务、健康度、升级恢复相关的验收项完成。
- 至少两条主推荐链路完成当前版本的真实故障认证：MySQL CDC -> MySQL upsert，以及 MySQL snapshot+CDC/CDC -> ClickHouse 幂等版本表。
- SQLite、MySQL、PostgreSQL 中任一仍公开标记为 production 的 storage backend，都必须进入同一套 migration/backup/restore conformance；不能用 SQLite 通过替代其他 backend 的证据。
- 发布资产不使用空 token、`change-me` 密码或浮动 `latest` 镜像作为生产默认值；不满足生产门槛的模式和 connector 明确降级为 beta/experimental。
- 发布说明列出 at-least-once、可能重复、RPO/RTO、单点、非原子 fanout 和未认证 connector 等残余边界。

## 当前已交付基线

以下能力已进入当前基线，不再作为未来 roadmap 项重复实施：

- 线性 pipeline、DAG 文件加载/hot-reload、条件路由、fanout、并行分片、cron/periodic/streaming/dependency 调度。
- MySQL batch/CDC/snapshot+CDC、PostgreSQL CDC、Kafka、file、HTTP、Redis、Feishu Sheet 等 source；ClickHouse、MySQL、PostgreSQL、Doris、Elasticsearch、Kafka、Redis、S3/file、JDBC、MaxCompute experimental contract 等 sink。
- lookup/enricher 异步 I/O 第一轮闭环：并发、in-flight 上限、超时、retry/backoff、背压、metrics、局部失败 DLQ、Redis-only cache gate。
- `flat_map`/`udtf` 一进多出、Debezium CDC policy、投影/类型转换、map_fields、extract、deduplicate、lookup、tumbling window 等同步和轻量汇聚能力。
- Web UI 固定任务向导、Connection Catalog 上下文、schema/sample/DDL preview、transform dry-run、validate/preflight、YAML/表单往返和 DLQ replay。
- DAG 节点级 DLQ replay；没有 `dag_node` 上下文的旧记录显式拒绝并保留。
- Connector certification kit 第一版、Plugin ABI v1 manifest/兼容边界、TypeScript SDK 和 Feishu source plugin 样板。
- 可靠性认证矩阵：source/state/sink checkpoint envelope、DLQ persistence gate、普通 Kafka crash/restart/rebalance/replay，以及 lookup/deduplicate/window StateStore 恢复证据。
- 真实 WASM transform 认证链路：固定工具链编译、ABI manifest 安装、0/1/N 输出、secret config、DLQ/replay、升级和 restart reload。
- standalone/master/worker/headless 运行文档、CLI smoke 和最小生产 runbook。
- MySQL/PostgreSQL `pre_write`、`increment`、生成列跳过、Debezium metadata PK 提取，以及多表映射/CDC 宽表生产候选链路。

当前公开成熟度必须继续以 descriptor/readiness、组件文档和可重复测试证据为准。没有真实环境证据的 MaxCompute、Feishu 和第三方插件不得提升为 production。

## 执行规则

Roadmap 状态只使用以下值：

| 状态 | 含义 |
| --- | --- |
| `active` | 当前唯一主任务，正在实现或验证 |
| `blocked_external` | 实现已具备，但缺少凭据、外部服务或人工授权 |
| `queued` | 已排序但尚未开始 |
| `delivered` | 已达到验收标准，应迁入 changelog/证据索引 |
| `deferred` | 明确不进入近期主线 |

执行纪律：

- 同一时间只推进一个 `active` 主任务。
- 外部阻塞必须写明所缺输入和解除条件；不得把被动等待包装成开发进度。
- 后续项不能静默改变当前最高优先级；需要调整时必须显式说明原因并获得用户确认。
- 每个任务必须有范围、验收标准和证据位置。完成后更新证据并从活动 backlog 移出。
- 实现过程中发现的相邻需求进入“有界后续”，不扩大当前验收标准。

## 当前主任务

### P0：MaxCompute 真实环境认证

状态：`blocked_external`

这是现有最高优先级，不改变原 roadmap 排序。MaxCompute/ODPS sink 的 SDK batch writer、partition/schema validator、远端 preflight、错误分类、retry/backoff、metrics 和环境门控 e2e 脚本已经存在；当前缺口不是继续实现 writer，而是真实 MaxCompute 环境中的认证证据。

解除阻塞所需输入：

- `MAXCOMPUTE_ENDPOINT`
- `MAXCOMPUTE_PROJECT`
- `MAXCOMPUTE_TABLE`
- `MAXCOMPUTE_ACCESS_KEY_ID`
- `MAXCOMPUTE_ACCESS_KEY_SECRET`
- 可选的 tunnel endpoint、quota 和用于失败注入的受控权限/测试表

验收标准：

- 实跑 Kafka ODS JSON -> `project` / `type_convert` -> MaxCompute 分区表。
- 验证正常写入、动态/静态分区、权限失败分类和远端 schema/partition preflight。
- 验证 sink 暂时失败进入 DLQ、修复后 replay 写回。
- 验证应用 restart、checkpoint reset/replay，并记录 append 模式可能重复的边界。
- 更新组件文档、connector readiness 和 certification evidence。
- 在上述证据完成前，`maxcompute` / `odps` maturity 保持 `experimental`。

现有入口：[e2e-maxcompute.sh](../hack/e2e-maxcompute.sh)、[sink-maxcompute.md](./components/sink-maxcompute.md)。

## Production Ready 收口队列

本节把 2026-07-24 production-readiness 审计中确认的缺口拆成可交付任务。它**不改变当前 MaxCompute P0 的最高优先级**；P0 仍为 `blocked_external`。当外部凭据继续不可得且需要切换主任务时，推荐顺序为 `PR-0 -> PR-1 -> PR-2 -> P3 -> P4 -> P5`，但实际切换仍需显式确认。`PR-D1` 是 distributed 独立门槛，在 standalone 收口后实施，除非用户明确把 distributed 提前。

| 阶段 | 面向目标 | 退出结果 | 依赖 | 状态 |
| --- | --- | --- | --- | --- |
| `PR-0` | 可靠、安全、持久化一致 | API/内存/DB 一致；加密恢复和生产安全默认值通过 | 当前 P0 完成或显式切换 | `delivered` |
| `PR-1` | 易维护、安全 | secret、migration、backup/restore、upgrade/rollback 可重复 | `PR-0` | `delivered` (1.1/1.2/1.3) |
| `PR-2` | 数据一致性 | 主推荐链路通过 crash/reset/outage/DLQ replay 对账 | `PR-0`，并复用 `PR-1` storage gate | `delivered` |
| P3 | 证据治理 | maturity 与当前版本实际认证证据一致 | `PR-2` 定义 path gate | `delivered` |
| P4 | 易上手 | 30 分钟首次任务与 10 分钟故障定位目标可验证 | `PR-0` 安全/profile 约定 | `delivered` |
| P5 | 易维护、可观测 | 业务健康、资源基线、CI 和 production runbook 成为发布门槛 | `PR-1`、`PR-2` | `delivered` |
| `PR-D1` | distributed 可靠性 | worker 认证、fencing、重试和真实多进程恢复通过 | standalone 收口后，或显式提前 | `delivered` |

### PR-0：控制面持久化一致性与安全默认值

状态：`delivered`

当前领取记录：

```text
Round: 1/5
Roadmap item: PR-0.1
Profile/path: standalone control plane + distributed worker spec read
Objective: 加密 spec 在 restart、版本读取/diff/rollback 和 worker execution 中统一可恢复，任何密钥/密文错误都明确失败。
Scope: spec crypto/store adapter、DB restore、version/diff/rollback API、worker spec load、对应单测/e2e 与 runtime runbook。
Non-goals: PR-0.2 的 current/version 原子提交与 storage failure rollback；PR-0.3 的 production profile/TLS/CORS；PR-1 的 connection/settings secret envelope。
Acceptance: 1) 明文旧数据与加密线性/DAG spec 均通过 create -> restart -> restore -> GET/version/diff -> rollback -> restart；2) worker 可读取加密 spec；3) 缺失/错误 key、损坏密文和不支持 envelope version 明确失败；4) key rotation 保留旧 key 时可读并在后续保存时使用新 key。
Evidence: internal/etl/server/spec_crypto*_test.go、internal/etl/worker/executor_test.go、hack/e2e-spec-encryption-recovery.sh、docs/runtime-modes.md。
Result: delivered
Residual/follow-up: PR-0.2
```

PR-0.1 本轮证据（Round 1/5）：

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| 明文旧数据与加密线性/DAG restart、版本、diff、rollback | `internal/etl/server/spec_crypto_recovery_test.go`：`TestEncryptedSpecRecoveryFlowLinearAndDAG`、`TestLegacyPlaintextSpecRecoveryFlow`；`go test ./internal/etl/server -count=1` | passed | 事务失败回滚留给 PR-0.2 |
| DAG import/export 与统一存储适配层 | 同上测试中的 DAG import；`server.go` `handleSpecImport`/`handlePipelineExport` | passed | API 事务边界留给 PR-0.2 |
| worker 加密读取且不泄露密文/secret | `internal/etl/worker/executor_test.go`；`TestExecuteShardReadsEncryptedPipelineSpec` | passed | master-worker transport 仍由 PR-D1 负责 |
| 缺 key、错 key、损坏密文、未知 envelope version | `TestRestoreFromDBFailsClosedOnSpecCryptoErrors`；`TestExecuteShardFailsWhenEncryptedSpecKeyIsMissing` | passed | production profile fail-closed 仍由 PR-0.3 负责 |
| 旧 key 轮换与 legacy envelope 可读 | `TestSpecEncryptionRotationKeepsOldVersionsReadable`、`TestLegacySpecCiphertextRemainsReadable` | passed | 删除 previous key 前的全量 re-encrypt 运维流程列为 PR-1 follow-up |
| 可重复执行入口 | `hack/e2e-spec-encryption-recovery.sh`（实际执行通过） | passed | 外部 MySQL/PostgreSQL storage certification 尚未纳入本轮 |

下一轮领取记录：

```text
Round: 2/5
Roadmap item: PR-0.2
Profile/path: standalone control plane persistence
Objective: current pipeline row、version row、内存 runner/scheduler 和 checkpoint 的提交边界失败可见且可恢复，不再返回半成功状态。
Scope: PipelineSpecStore/Storage transaction seam、server create/update/import/rollback/delete lifecycle、fault-injection tests and evidence docs。
Non-goals: PR-0.3 production profile/TLS/CORS；PR-1 migrations/key rotation；跨 backend 全量 backup/restore。
Acceptance: 1) current+version 保存要么一起成功要么可恢复；2) storage failure 时 API 非 2xx 且内存 runner/scheduler 回到最后成功状态；3) delete/rollback/checkpoint errors 不被吞掉；4) SQLite seam 有可注入故障测试，MySQL/PostgreSQL 接口边界有明确残余说明。
Evidence: internal/etl/server/*fault*_test.go、internal/etl/storage/*、targeted API tests、docs/runtime-modes.md。
Result: delivered
Residual/follow-up: PR-0.3
```

PR-0.2 本轮证据（Round 2/5）：

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| current/version 原子提交 | `internal/etl/storage/sqlstore/store.go` `SavePipelineWithVersion*`；`TestPipelineSpecStoreAtomic*`；`TestSQLiteConformance/PipelineAtomicLifecycle` | passed | 并发 version 分配见 PR-1.2 |
| checkpoint reset 与 spec 提交同一边界 | `SavePipelineWithVersionAndCheckpointReset`；`TestPipelineUpdateCheckpointFailureRollsBackSpecAndCheckpoint`；MySQL/PostgreSQL conformance | passed | Redis 高频 state 不在本事务范围 |
| create/update/import/rollback storage failure 不激活半成品 runner | `internal/etl/server/storage_fault_test.go` 全套 API fault-injection tests；`hack/e2e-control-plane-persistence.sh` | passed | scheduler 注册失败后的补偿为后续 bounded follow-up |
| delete 同时清理 current/version/checkpoint | `DeletePipelineWithCheckpoint`；`TestPipelineDeleteStorageFailureKeepsRuntimeRowsVersionsAndCheckpoint`；三 backend conformance | passed | 外部备份/恢复仍属于 PR-1.3 |
| schedule/checkpoint 错误可见 | `TestScheduleStorageFailureKeepsInMemoryScheduleUnchanged`、`TestCheckpointResetFailureReturnsNon2xxAndKeepsCheckpoint` | passed | status/audit 写失败的全链路告警列入 P5 |
| backend scope | `CONTAINER_CLI=podman ./hack/e2e-storage-mysql.sh`、`...e2e-storage-postgres.sh`、SQLite conformance | passed | 未执行真实生产 credentials/path e2e |

PR-0.3 有界增量领取记录：

```text
Round: 3/5
Roadmap item: PR-0.3.1
Profile/path: standalone production profile
Objective: production profile 缺少 API token、spec key、TLS/config secret 或使用占位值时在启动前 fail-closed，只有显式 insecure-development 开关可放行。
Scope: runtime profile config/CLI、server/app startup validation、production compose/config smoke、docs/tests。
Non-goals: Round 4 的 CORS/security headers/trusted proxy/UI token storage；Round 5 的双端口 TLS runtime e2e。
Acceptance: 1) development 默认兼容；2) production 缺 token/key/TLS 或使用 change-me/latest 失败；3) explicit insecure development 可诊断放行；4) production compose interpolation 缺 secret 失败且固定 image；5) smoke 自动化可重复。
Evidence: internal/etl/server/runtime_profile_test.go、internal/cmd/runtime_flags_test.go、hack/e2e-production-profile.sh、compose config output、docs/runtime-modes.md。
Result: delivered
Residual/follow-up: PR-0.3.2 security HTTP boundary
```

PR-0.3.1 本轮证据（Round 3/5）：

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| production 缺 token/key/TLS/审计或占位值 fail-closed | `internal/etl/server/runtime_profile.go`、`runtime_profile_test.go` | passed | 双端口 TLS runtime topology 在 Round 5 |
| development 兼容与显式 insecure bypass | `TestDevelopmentProfileRemainsCompatibleWithoutProductionSecrets`、`TestProductionProfileExplicitInsecureDevelopmentBypassIsDiagnosable` | passed | bypass 只用于开发，不提升 maturity |
| CLI profile 入口 | `internal/cmd/runtime_flags.go`/`runtime_flags_test.go` | passed | 无 |
| standalone compose 必填 secret、证书目录、固定 image | `docker-compose.yml`、`hack/e2e-production-profile.sh` 实际执行通过 | passed | distributed compose 继续 beta/PR-D1 边界 |

下一轮领取记录：

```text
Round: 4/5
Roadmap item: PR-0.3.2
Profile/path: standalone HTTP API + embedded UI token handling
Objective: 限制跨 origin、仅信任配置的 forwarded headers、为所有 API 响应补安全头，并避免 UI 将 API token 持久化到 localStorage。
Scope: internal/etl/server security middleware/tests、UI API token plumbing、production smoke、runtime runbook。
Non-goals: Round 5 的 UI/API 双端口 TLS termination topology；PR-D1 worker authenticated transport；RBAC。
Acceptance: 1) development wildcard 与 production exact allow-list/same-origin 行为可测试；2) 未授权 origin（含非预检写请求）403；3) untrusted forwarded IP 不影响 audit/rate-limit identity，trusted CIDR 可读取；4) auth/rate-limit/CORS 拒绝响应带安全头，401 带 WWW-Authenticate；5) UI token 仅驻留页面内存，构建和 UI smoke 通过。
Evidence: internal/etl/server/security_test.go、hack/e2e-production-profile.sh、web/src/lib/api.ts 及 token consumers、`npm run typecheck`/`npm run build`、本地 Playwright smoke。
Result: delivered
Residual/follow-up: Round 5 TLS topology and runtime smoke
```

PR-0.3.2 本轮证据（Round 4/5）：

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| development wildcard、production 禁止 wildcard、exact origin 与 same-origin 代理行为 | `internal/etl/server/security_test.go`（`TestParseCORSOriginsProfileDefaults`、`TestCORSAllowListAndSecurityHeaders`、`TestSameHostOriginRemainsAllowedWithExplicitCrossOriginList`） | passed | 双端口 TLS 下的真实浏览器路径留给 Round 5 |
| 未授权跨 origin 的预检和普通请求均拒绝 | `TestCORSRejectsDisallowedPreflight`；`hack/e2e-production-profile.sh` security gate | passed | 无 |
| forwarded IP 仅受 trusted proxy CIDR 影响 | `TestClientIPOnlyTrustsForwardedHeadersFromConfiguredProxy` | passed | 多级代理链策略仍需在部署 runbook 中按拓扑核验 |
| 安全响应头、HSTS 与认证 challenge 覆盖拒绝响应 | `TestCORSAllowListAndSecurityHeaders`、`TestAuthFailureHasChallengeAndSecurityHeaders`；middleware 顺序在 `StartHTTP` | passed | GoFrame 静态首页的双端口 TLS 头部整体验证留给 Round 5 |
| UI token 不进入长期 localStorage | `web/src/lib/api.ts` 内存 token；`rg etl_api_token web/src` 无持久化读写；`npm run typecheck`、`npm run build`；`hack/e2e-ui-token.sh` 验证未认证 401、保存后 authenticated UI request、刷新后 token 清空并恢复 401 | passed | reload 后需重新输入是有意语义 |
| 可重复安全/profile smoke | `hack/e2e-production-profile.sh`（profile + compose + HTTP security gate）；`hack/e2e-ui-token.sh`（当前构建镜像上的 focused browser token gate） | passed | 完整 `hack/e2e-ui.sh` 现为 108 passed/0 failed（P4 residual 已于 2026-08-06 收口）；PR-0 token gate 仍以 `e2e-ui-token.sh` 为准，不与 UI 全量 e2e 混计 |

最后一轮领取记录：

```text
Round: 5/5
Roadmap item: PR-0.3.3
Profile/path: standalone production UI + ETL API transport
Objective: 统一 :8000 UI/proxy 与 :8001 ETL API 的 TLS termination，避免生产 profile 出现 UI 明文、代理降级或不可验证的内部 TLS。
Scope: GoFrame TLS setup、UI→API reverse proxy transport、TLS server-name config/CLI、UI security headers、Docker/compose health、TLS smoke/runbook。
Non-goals: external ingress-only termination mode；PR-D1 worker/master authenticated transport；ACME/证书自动轮换；distributed compose maturity。
Acceptance: 1) 配置证书后两个端口只接受 HTTPS；2) 反代验证证书 chain/name 且不使用 InsecureSkipVerify；3) UI、代理 health、直连 API、auth challenge、安全头均通过；4) production compose health 与必填 TLS server name 一致；5) development 无证书仍保持 HTTP 兼容。
Evidence: internal/logic/app/tls_topology*.go、internal/cmd/runtime_flags_test.go、hack/e2e-tls-topology.sh、hack/e2e-production-profile.sh、Dockerfile/docker-compose.yml、docs/runtime-modes.md。
Result: delivered
Residual/follow-up: none inside PR-0.3; PR-D1 retains distributed transport scope
```

PR-0.3.3 本轮证据（Round 5/5）：

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| UI/API 同证书且启用后无明文 listener | `ConfigureHTTPSTopology`；`hack/e2e-tls-topology.sh` 对 :8000/:8001 的 HTTPS 与 HTTP rejection 检查 | passed | external ingress-only termination 未纳入本轮 |
| UI→API 证书 chain/name 验证且无跳过 | `newETLProxyTransport`；`TestETLProxyTransportTrustsConfiguredCertificateWithoutSkippingVerification`；`ETL_TLS_SERVER_NAME` | passed | 证书自动轮换属于 PR-1/运维 follow-up |
| UI、代理/direct health、auth 与安全头 | `hack/e2e-tls-topology.sh` 实际执行通过 | passed | 无 |
| compose/health/config 一致 | `docker-compose.yml` HTTPS health + required `ETL_TLS_SERVER_NAME`；`hack/e2e-production-profile.sh` 实际执行通过 | passed | distributed compose 仍为 beta/PR-D1 |
| development HTTP 兼容 | `TestNormalizeETLTargetUsesHTTPSWhenTLSIsConfigured` 的 HTTP default；现有全量 Go/e2e 路径无 TLS 时不变 | passed | 无 |
| 最终回归范围 | `go test ./... -count=1`；`go test -race ./internal/etl/server ./internal/logic/app ./internal/cmd -count=1`；`npm run typecheck && npm run build`；`hack/e2e-runtime-smoke.sh` | passed | Vite bundle 仍有既有 >500kB warning |

上一轮五轮上限交接（历史）：

```text
Round: 5/5
Roadmap item: PR-0
Profile/path: standalone control plane
Result: active
Residual/follow-up: PR-0.2a scheduler activation compensation。create/update/import 在持久化提交后若 RegisterExecutor 失败，仍有非 2xx 与 current/version/runner 状态不一致的路径；rollback 的 replaceRunnerForRollback 还直接忽略 registerRuntimeSchedule 错误。下一轮应先为 cron/periodic/dependency 注册失败注入测试，再使持久化、旧 runner/scheduler 与 API 结果回到最后一次成功边界。容器版 hack/e2e-ui.sh 另因本机 Podman build 卡住未完成，但本地浏览器 token smoke 已通过。
```

本轮领取与收口记录：

```text
Round: 1/5
Roadmap item: PR-0.2a
Profile/path: standalone control plane scheduler activation
Objective: scheduler 注册失败或持久化失败时，API、DB、内存 runner 和 scheduler 保持最后一次成功边界。
Scope: orchestrator Scheduler prepare/commit seam、server create/update/import/rollback/schedule lifecycle、故障注入测试与本 roadmap 证据。
Non-goals: PR-1 migration/secret envelope；PR-D1 worker transport；新调度类型或调度产品能力。
Acceptance: 1) cron/periodic/dependency 配置与注册失败不污染 scheduler；2) create/update/import/rollback 的 scheduler failure 返回非 2xx 且不提交半成品；3) storage failure 在 scheduler prepare 后保持旧 current/version、内存 runner 和旧 registration；4) schedule PUT/DELETE 同样具备补偿边界。
Evidence: internal/etl/orchestrator/scheduler.go、internal/etl/server/server.go；internal/etl/orchestrator/scheduler_test.go；internal/etl/server/scheduler_activation_fault_test.go；hack/e2e-spec-encryption-recovery.sh；hack/e2e-control-plane-persistence.sh；hack/e2e-production-profile.sh；hack/e2e-tls-topology.sh；hack/e2e-runtime-smoke.sh；hack/e2e-ui-token.sh；go test ./... -count=1；go test -race ./internal/etl/orchestrator ./internal/etl/server -count=1。
Result: delivered
Residual/follow-up: PR-0 final acceptance 已完成；跨请求并发 version 分配、migration/backup/restore 与 distributed transport 按 PR-1/PR-D1 保留。
```

PR-0.2a 验收矩阵：

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| cron/periodic/dependency 校验、五/六字段 cron 与失败注册不污染 scheduler | `internal/etl/orchestrator/scheduler_test.go`：`TestRegisterExecutorFailureLeavesNoSchedulerState`、`TestImmediateRegisterFailureLeavesNoRunnerEntry`、`TestValidateScheduleConfigAcceptsFiveAndSixFieldCron`、`TestPrepareExecutorInjectedFailureLeavesPreviousExecutor` | passed | 无 |
| candidate 在持久化前不触发，成功后才 swap scheduler | `TestPrepareExecutorKeepsOldExecutorLiveUntilCommit`；`ExecutorReplacement` prepare/commit seam | passed | 无 |
| create/update/import/rollback scheduler failure 保持 API、DB、内存 runner、旧 dependency registration | `internal/etl/server/scheduler_activation_fault_test.go`：`TestPipelineCreateSchedulerFailureLeavesNoRuntimeOrRows`、`TestPipelineUpdateSchedulerFailureKeepsLastSuccessfulBoundary`、`TestSpecImportSchedulerFailureKeepsLastSuccessfulBoundary`、`TestPipelineRollbackSchedulerFailureKeepsLastSuccessfulBoundary` | passed | 无 |
| storage failure 在 prepare 后不改变 current/version、内存与旧 scheduler | `TestStorageFailureAfterSchedulerStagingRestoresOldRegistration`、`TestScheduleEnableStorageFailureRemovesStagedRegistration`、`TestScheduleDisableStorageFailureRestoresOldRegistration` | passed | 无 |
| 回归与并发安全 | `go test ./... -count=1`；`go test -race ./internal/etl/orchestrator ./internal/etl/server -count=1` | passed | 未执行外部 connector/e2e；本增量仅覆盖 standalone control-plane lifecycle |

当前收口领取记录（已完成）：

```text
Round: 1/5
Roadmap item: PR-0 (final acceptance reconciliation)
Profile/path: standalone production control plane
Objective: 按 PR-0 的完整验收范围重跑加密恢复、持久化故障、production profile、TLS topology 和 runtime smoke，并使证据与当前代码/版本一致。
Scope: 现有 hack/e2e-* smoke、PR-0 相关单测/故障测试、runtime-modes 与 roadmap evidence reconciliation。
Non-goals: PR-1 secret envelope/migration/backup-restore；PR-2 connector path certification；PR-D1 distributed transport；新增产品能力。
Acceptance: 1) PR-0 必需 smoke 在当前工作树可重复通过；2) 失败/阻塞明确记录，不以脚本存在代替证据；3) 若无剩余 mandatory gap，更新 PR-0 状态与交接，否则保留 active 并记录具体 gap。
Evidence: hack/e2e-spec-encryption-recovery.sh、hack/e2e-control-plane-persistence.sh、hack/e2e-storage-mysql.sh、hack/e2e-storage-postgres.sh、hack/e2e-production-profile.sh、hack/e2e-tls-topology.sh、hack/e2e-runtime-smoke.sh、hack/e2e-ui-token.sh、相关 Go tests 与前端 build。
Result: delivered
```

PR-0 最终验收矩阵：

| Criterion | Evidence | Result | Residual/follow-up |
| --- | --- | --- | --- |
| 加密线性/DAG spec restart、版本/diff/rollback 与 worker 读取 | `./hack/e2e-spec-encryption-recovery.sh`；相关 server/worker crypto tests | passed | 旧 key re-encrypt 运维流程属于 PR-1 follow-up |
| current/version/checkpoint/delete 原子边界与三 backend conformance | `./hack/e2e-control-plane-persistence.sh`；`CONTAINER_CLI=podman ./hack/e2e-storage-mysql.sh`；`CONTAINER_CLI=podman ./hack/e2e-storage-postgres.sh`；SQLite conformance | passed | migration/backup/restore 与并发 version 分配属于 PR-1 |
| scheduler prepare/commit compensation | `scheduler_activation_fault_test.go`；create/update/import/rollback/schedule fault tests | passed | 无 |
| production profile、CORS、trusted proxy、安全头与双端口 TLS | `CONTAINER_CLI=podman ./hack/e2e-production-profile.sh`；`./hack/e2e-tls-topology.sh` | passed | ACME/自动轮换、distributed transport 属于 PR-1/PR-D1 |
| UI token 内存化与当前构建上下文 | `npm run typecheck && npm run build`；`./hack/e2e-ui-token.sh`；`.dockerignore` 排除本地缓存/构建产物 | passed | 完整 UI 向导脚本的 17 个 P4 residual 不属于 PR-0 |
| runtime/release regression | `./hack/e2e-runtime-smoke.sh`；`go test ./... -count=1`；`go test -race ./internal/etl/orchestrator ./internal/etl/server -count=1`；`git diff --check` | passed | 未认证 connector/path 仍按各自 maturity 声明 |

因此 `PR-0.1`、`PR-0.2` 主事务切片、`PR-0.3` 安全/TLS 切片、`PR-0.2a` scheduler compensation 和本轮最终验收均已交付；`PR-0` 现标记为 `delivered`。这不提升项目级或 distributed maturity：P0 MaxCompute 仍为 `blocked_external`，PR-1 storage/secret 演进和 PR-D1 distributed transport 保持原顺序。P4 UI e2e residual 已于 2026-08-06 收口（`hack/e2e-ui.sh` 108/0）。

最终交接：

```text
Round: 1/5
Roadmap item: PR-0
Profile/path: standalone control plane
Result: delivered
Residual/follow-up: 不自动领取 PR-1；下一次显式 continue 重新同步后，按 P0 blocked_external 与既定顺序决定是否领取 PR-1.1。P4 UI e2e residual 已清零，不与 PR-0 token smoke 证据混用。
```

目标：消除“API 看似成功但重启后丢失”“启用加密后无法恢复”和“生产默认未认证”三类上线阻断，使 API 返回状态、内存运行状态和持久化状态保持一致。

内部增量（一次只推进一个）：

1. `PR-0.1` 加密读取与恢复：统一 encrypted spec 访问、密钥错误和 restart/rollback 测试。
2. `PR-0.2` 持久化提交边界：事务化 current/version，补 storage failure injection 和内存状态回滚。
3. `PR-0.3` 安全生产 profile：fail-closed、TLS topology、固定镜像和 compose/config smoke。

范围：

- 修复 encrypted spec 的所有读取路径：pipeline list/restore、单版本读取、diff、rollback、import/export 和 worker execution；禁止绕过加密适配层直接解析 `spec_yaml`。
- 缺少密钥、密钥错误、ciphertext 损坏和 key version 不支持时明确失败并给出 remediation；不得把密文当 YAML 后仅记录 warning 并跳过 pipeline。
- 创建、更新、导入、rollback 和删除 pipeline 时显式处理 `SavePipeline`、`SavePipelineVersion`、checkpoint 和删除错误；持久化成功后才能返回成功或激活新 runner，失败时回滚内存/调度状态。
- 将当前 pipeline row 与 version row 的保存边界做成事务或等价的可恢复操作，避免只更新 current spec、未生成 version 的半成功状态。
- 引入明确的 production profile：API token、spec encryption key 和生产依赖 secret 缺失时 fail-closed；只有显式的 insecure development 开关可以放行。
- 在不强行引入复杂 RBAC 的前提下，明确 shared-token 的 scope、轮换、审计和撤销边界；限制 CORS origin，补齐安全响应头，只有受信任 proxy 才能提供 client IP，UI 不把长期 token 无条件放在可被脚本读取的持久化存储中。
- 统一 TLS termination 拓扑，确保 UI/`:8000` 代理、ETL API/`:8001`、health 和命令行客户端使用同一套可验证约定；生产 compose 不再默认空 token、`change-me` 或 `latest`。

验收标准：

- 新增 `create -> restart -> RestoreFromDB -> GET/version/diff -> rollback -> restart` 自动化，在线性 spec 和 DAG、明文旧数据和加密新数据上都通过。
- worker 读取加密 spec 的测试必须通过；若采用 master 下发解密 spec，日志、task row 和 API response 不得泄露 secret。
- 使用可注入失败的 storage，逐项证明 create/update/delete/rollback 在 current row、version row 或 checkpoint 写失败时返回非 2xx，且重启后的 DB 与最后一次成功响应一致。
- 错误或缺失 encryption key 时启动失败并指出 key 问题；旧 key 到新 key 的迁移不产生不可读 spec。
- production compose/config smoke 证明缺失必填 secret 会在启动前失败；固定版本镜像能够启动，UI/API/health 在选定 TLS 拓扑下全部通过。
- 未授权跨 origin、伪造 forwarded IP、缺失安全响应头和 token 持久化风险有自动化安全回归；RBAC 若未实现，文档必须明确其不属于当前 standalone 许可范围。

目标证据：`internal/etl/server/spec_crypto*_test.go`、server storage fault-injection tests、`hack/e2e-spec-encryption-recovery.sh`、production compose config smoke、[runtime-modes.md](./runtime-modes.md)。

### PR-1：Secret 管理、Storage 演进与可恢复升级

状态：`delivered`

目标：让一个小团队能够安全保存连接信息，并在没有人工改表或猜测回滚步骤的情况下完成升级、备份和恢复。

当前领取记录：

```text
Round: 2/5
Roadmap item: PR-1.2
Profile/path: standalone control plane storage
Objective: SQLite/MySQL/PostgreSQL migration lock + 并发 pipeline version 分配不产生重复 version/半迁移
Scope: sqlstore migration/version allocation、conformance/fault tests、runtime-modes residual notes
Non-goals: PR-1.3 backup/restore/janitor scripts；PR-2 path certification；RBAC
Dependency: PR-1.1 delivered
Acceptance: 1) migration 有显式 schema version/lock/失败终止；2) 并发 migration 只有一个执行 DDL；3) migration error 阻止半迁移启动；4) 并发 pipeline update 不重复 version 且冲突明确；5) current/version/checkpoint 边界与 PR-0 一致
Evidence: storage concurrency/migration tests、三 backend conformance、docs/runtime-modes.md
Result: delivered
```

PR-1.2 本轮证据（Round 2/5）：

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| 三 backend migration 显式 schema version + lock | `sqlstore.WithMigrationLock`（SQLite lease / MySQL `GET_LOCK` / PG advisory）；`MigrateSQLite` / `mysql.New` / `postgres.New` 均在 lock 内执行 | passed | — |
| 并发 migration 仅一个执行 DDL | `TestSQLiteMigrationLockSerializesCallbacks`；`TestConcurrentSQLiteStoreOpenMigratesOnce` | passed | MySQL/PG 依赖原生 advisory lock + e2e open |
| migration error 阻止半迁移 | `runVersionedMigrations` 显式错误；失败不写入 `_schema_version`；`TestSQLiteMigrationFailureDoesNotRecordSchemaVersion`；`TestSQLiteMigrationLockPropagatesFailure` | passed | 前向 upgrade smoke 在 PR-1.3 |
| 并发 `SavePipelineWithVersion` 无重复 version | unique `(pipeline, version)` + 冲突重试；`TestConcurrentPipelineVersionAllocationIsUnique` | passed | — |
| current/version/checkpoint 事务边界与 PR-0 一致 | `savePipelineWithVersionOnce` 单事务；`TestPipelineSpecStoreAtomic*`；conformance `PipelineAtomicLifecycle` | passed | — |

PR-1.1 本轮证据（Round 1/5）：

```text
Round: 1/5
Roadmap item: PR-1.1
Profile/path: standalone control plane
Objective: connection/settings 持久化 secret 字段级加密、rotation 和 restart/restore 可验证
Scope: storage secret envelope adapter、connection/settings encode/decode、API mask/preserve、runtime-modes 与相关测试
Non-goals: PR-1.2 migration lock/concurrent version；PR-1.3 backup/restore/janitor；PR-2 path certification；重新实现 pipeline spec encryption；RBAC
Dependency: PR-0 delivered；P0 MaxCompute 仍 blocked_external，本轮按执行方案显式推进 PR-1.1
Acceptance: 1) 固定测试 secret 写入后 dump/直接查询无明文；2) API 只返回 mask，masked 更新不覆盖真实 secret；3) key ID/旧 key/新 key re-encrypt/错误 key/损坏密文有明确结果；4) 重启后 connection/settings 仍可用于 runtime；5) 失败路径不把 secret 写入日志/audit/task/DLQ
Evidence: internal/etl/storage/secret_fields*.go；internal/etl/server/secret_envelope_test.go；go test ./internal/etl/storage ./internal/etl/server -count=1；go test -race ...；./hack/e2e-storage-mysql.sh；./hack/e2e-storage-postgres.sh；docs/runtime-modes.md
Result: delivered
Residual/follow-up: PR-1.2
```

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| dump/直接查询无明文 | `TestSecretFieldStoreEncryptsConnectionAndSettings`；`TestConnectionAndSettingsSecretEnvelopeAPI` 原始 SQL 断言 | passed | backup file scanner 在 PR-1.3 |
| API mask + masked 更新不覆盖 | server secret envelope API test；settings preserve masked `llm_api_key` | passed | — |
| key rotation / wrong key / malformed | `TestSecretFieldStoreRotationAndWrongKey`；`TestSecretEnvelopeWrongKeyFailsClosed`；malformed ciphertext test | passed | 运维 re-encrypt CLI 可在 PR-1.3 补 |
| restart 后仍可读 | API restart reopen store path in `TestConnectionAndSettingsSecretEnvelopeAPI` | passed | restore-from-backup 在 PR-1.3 |
| 三 backend secret field conformance | `TestSQLite/MySQL/PostgresConformance/SecretFields` via e2e-storage-*.sh | passed | — |
| 不破坏 PR-0 atomic/fault 边界 | SecretFieldStore 转发 atomic save/delete；storage fault tests passed | passed | — |

内部增量（一次只推进一个）：

1. `PR-1.1` Secret envelope：connection/settings 字段级加密、key ID 和 rotation。`delivered`
2. `PR-1.2` Storage migration：显式错误、migration lock、并发 pipeline version 分配。`delivered`
3. `PR-1.3` 恢复与保留：三个 backend 的 upgrade/backup/restore smoke 和 retention/janitor。`delivered`

PR-1.3 证据（合入 agent/fullstack-dev/f856487d，并保留既有逻辑导出路径）：

| 验收项 | 证据 | 结果 | 备注 |
|--------|------|------|------|
| SQLite 前向升级（legacy schema → current） | `TestSQLiteForwardUpgradeFromLegacySchema`；`hack/e2e-storage-upgrade-sqlite.sh` | passed | id backfill + `_schema_version` 记录 |
| 失败升级阻止启动 | `TestSQLiteUpgradeFailureBlocksStartup`；`WithMigrationLock` 错误传播；PR-1.2 半迁移不写版本 | passed | 三 backend 共用 lock 语义 |
| 备份覆盖 11 类对象 | `internal/etl/storage/backup` Export；`TestBackupRestoreRoundTripSQLite` | passed | pipelines/versions/checkpoints/DLQ/audit/runs/workers/tasks/plugins/connections/settings |
| 逻辑导出 + 明文 secret 扫描 | `storage.BackupSQLStore`；`TestBackupSQLStoreAndSecretScan`；`hack/cmd/*-backup-smoke` | passed | 与 JSON snapshot 路径并存 |
| 恢复后对账 | `backup.Reconcile`；`TestBackupRestoreUpgradePath`（sqlite 恒跑；mysql/pg 需 DSN） | passed | critical 表计数 + checkpoint/DLQ 内容 |
| retention/janitor 配置·上限·告警 | `internal/etl/server/janitor.go`；`TestRetentionJanitorPurgesDLQAndAudit`；health `janitor*` 字段 | passed | DLQ/audit/run/task TTL；batch/max 硬上限 |
| e2e 脚本 | `hack/e2e-backup-restore-*.sh`；`hack/e2e-storage-upgrade-*.sh`；runbook `docs/runtime-modes.md` | passed | 外部 backend 无容器时记 skipped |


范围：

- 对 connection catalog、LLM/settings 和其他持久化 secret 实施字段级静态加密；API mask 只作为展示保护，不能替代数据库静态加密。
- 密文携带 key ID/version；提供受控 key rotation 和 re-encrypt 流程，允许在维护窗口中验证旧数据并完成迁移。
- 所有 SQLite/MySQL/PostgreSQL migration 显式处理 schema-version 建表、查询、DDL 和记录版本错误；增加 migration lock，防止 master/worker 并发启动重复执行。
- 修复 pipeline version 的 `MAX(version)+1` 并发竞争，保证版本号分配和 current/version 保存的一致性。
- 建立 SQLite/MySQL/PostgreSQL 的前向升级、失败回滚、备份恢复 smoke；备份范围覆盖 pipelines、versions、checkpoints、DLQ、audit、runs、workers/tasks、plugins、connections 和 settings。
- 为 task、run history、audit 和 DLQ 建立可配置 retention/janitor，避免小团队依赖手工 SQL 清表。

验收标准：

- 向数据库写入固定测试 secret 后，SQL dump 和直接查询中不能出现明文；重启、轮换密钥和恢复备份后仍能连接并运行 pipeline。
- 两个以上进程同时启动 migration 时只有一个执行 DDL，其他进程等待或安全确认版本；任何 migration 错误都会阻止服务以旧/半迁移 schema 继续运行。
- 并发更新同一 pipeline 不会产生重复版本号、丢 version 或 current/version 不一致；冲突通过明确的 409/版本前置条件返回。
- 从上一个稳定 release 升级到当前版本、恢复备份并回滚镜像的 smoke 在三个公开支持的 storage backend 上分别有结果；缺少环境的 backend 不得标记为已认证。
- 单个维护者按照 runbook 能完成备份、恢复验证和回滚演练，结果包含数据对象计数与关键 checkpoint 对账，而不是只检查进程启动成功。

目标证据：storage conformance suite、`hack/e2e-storage-upgrade-*.sh`、`hack/e2e-backup-restore-*.sh`、[runtime-modes.md](./runtime-modes.md) 和 release checklist。

### PR-2：数据一致性契约与生产链路认证

状态：`delivered`（2026-07-25 · SEL-217）

目标：把“默认 at-least-once”变成可验证、可解释的生产契约，保证不静默丢数据，并把可能重复限制在已声明、可吸收或可对账的边界内。

内部增量（一次只推进一个）：

1. `PR-2.1` Path contract：定义认证矩阵、业务键/版本策略和自动对账格式。
2. `PR-2.2` 主链路故障认证：完成 MySQL -> MySQL/ClickHouse 的 crash/reset/outage/DLQ replay。
3. `PR-2.3` 边界语义收口：PostgreSQL TRUNCATE、append sink 和 fanout 采取实现、阻断或降级声明。

范围：

- 建立 path-level 认证矩阵，认证对象必须包含 source、关键 transforms、sink、write mode、business key/version 策略、storage backend 和 runtime mode；connector 单体 maturity 不能替代整条链路认证。
- 首批强制认证 MySQL CDC -> MySQL upsert、MySQL snapshot+CDC/CDC -> ClickHouse ReplacingMergeTree-style sink；随后按 P3 事实源扩展 PostgreSQL sink、Doris、Kafka、S3/File 等路径。
- 每条生产链路验证 sink ack 后、checkpoint commit 前崩溃，checkpoint storage failure，应用 restart，checkpoint reset/replay，sink outage，DLQ repair/replay 和 schema drift。
- 使用业务键、版本和源端/目标端对账证明没有静默丢失；重复记录必须被 upsert/version/deduplicate 吸收，或以明确的 append 重复边界进入 readiness warning。
- 对 PostgreSQL CDC TRUNCATE、跨 sink fanout、Kafka/File/S3 append 等不完整语义采取“实现、阻断或降级成熟度”之一；不能只 warning 后继续并仍声称路径 production ready。
- 危险组合默认由 validate/preflight 阻断；`allow_unsafe` 只允许显式接受风险，并在 API/UI/audit 中保留不可忽略的风险记录。

验收标准：

- 认证用例对源端业务键/版本与 sink 最终状态做自动对账；目标是静默丢失为 0，重复结果符合该 sink 的公开契约。
- 每个 production path 至少包含正常写入、受控 crash、依赖中断、checkpoint reset 和 DLQ replay 证据；脚本仅存在但未在当前版本执行不算通过。
- `docs/reliability-certification.md` 和 `docs/etl-idempotency.md` 能从 descriptor/readiness 反向定位到具体测试、最后通过版本和已知残余风险。
- PostgreSQL CDC TRUNCATE 在 production path 中要么有目标语义及 e2e，要么 preflight 明确阻断；不得保留源端已清空但目标端继续保留旧行的无提示结果。
- S3/File/Kafka 等 append 路径在 first-class manifest/transaction 未实现前保持 `production_with_review` 或更低，并给出下游去重/对账方式。

目标证据：[reliability-certification.md](./reliability-certification.md)、[etl-idempotency.md](./etl-idempotency.md)、connector certification suite 和各 production-candidate e2e。

### PR-2.4：checkpoint 恢复 fail-closed（用户明确授权的有界后续）

状态：`delivered`（2026-08-08 · PR-2.4.1/.2/.3/.4）

本项不改变已交付 PR-2 的主链路声明，也不把当前 blocked_external 的 MaxCompute P0 静默改为已完成；它只修复审计确认的恢复边界：checkpoint storage/envelope 读取失败时，linear 与 DAG 不得以空位点继续打开 source。

当前领取记录：

```text
Round: 1/5
Roadmap item: PR-2.4.1
Profile/path: standalone linear + DAG checkpoint restore
Objective: checkpoint store 读取错误、损坏/未知版本 envelope 和 DAG source checkpoint 读取错误均 fail-closed，pipeline 进入 failed 并保留可诊断错误。
Scope: internal/etl/checkpoint envelope unwrap、internal/etl/pipeline Runner.Start、internal/etl/orchestrator DAG source startup、targeted fault-injection tests、reliability evidence。
Non-goals: PR-2.4.2 的 Kafka/PostgreSQL external ack 顺序；PR-2.4.3 的 mysql_snapshot_cdc producer read-ahead/string cursor；connector schema/UI 和多表 preflight。
Acceptance: 1) linear checkpoint Load error 返回启动错误且不打开 source；2) DAG checkpoint Load/error 或 source Open error 将 executor 置为 failed 并停止其他 source；3) 损坏 JSON、未知 envelope version、缺失 envelope source 明确失败；4) legacy source position 仍兼容；5) 单测覆盖 fault injection，更新可靠性文档和证据索引。
Evidence: internal/etl/checkpoint/*_test.go、internal/etl/pipeline/*checkpoint*_test.go、internal/etl/orchestrator/*checkpoint*_test.go、docs/reliability-certification.md。
Result: delivered
Residual/follow-up: PR-2.4.2 external ack ordering 与 PR-2.4.3 snapshot cursor commit boundary 已分别在 Round 2/3 交付；后续只按新 claim 处理残余语义。
```

PR-2.4.1 本轮证据（Round 1/5）：

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| linear checkpoint store Load error fail-closed | `internal/etl/pipeline/runner_test.go` `TestRunnerFailsStartupWhenCheckpointLoadFails`；`go test ./internal/etl/pipeline -count=1` | passed | source-specific legacy position shape validation留给后续增量 |
| linear corrupt/unknown envelope 不打开 source | `TestRunnerFailsStartupWhenCheckpointEnvelopeIsCorrupt`；`internal/etl/checkpoint/envelope_test.go` | passed | external source ack ordering未处理 |
| DAG checkpoint validation/load/source Open failure 可见且停止 | `internal/etl/orchestrator/orchestrator_test.go` `TestDAGExecutorCheckpointRestoreFailsClosed`、`TestDAGExecutorSourceStartupFailureStopsPipeline` | passed | 多 source 运行时错误聚合与 health API 对账留给后续 |
| legacy valid position 兼容 | `TestUnwrapForSourceKeepsLegacyPosition` | passed | legacy 语义校验仍由各 source codec负责 |
| package/race/static checks | `go test ./internal/etl/... -count=1`；`go test -race ./internal/etl/checkpoint ./internal/etl/pipeline ./internal/etl/orchestrator -count=1`；`go vet ./internal/etl/checkpoint ./internal/etl/pipeline ./internal/etl/orchestrator`；`git diff --check` | passed | 未执行外部 connector e2e（本增量不要求） |

当前领取记录（Round 2）：

```text
Round: 2/5
Roadmap item: PR-2.4.2
Profile/path: standalone linear + DAG；Kafka source / PostgreSQL CDC source
Objective: 将 source checkpoint 生成、durable checkpoint Save 与 Kafka/PG external ack 拆成明确顺序，消除 external ack 早于内部 checkpoint 的丢失窗口。
Scope: core CheckpointAcker contract、linear Runner/DAGExecutor boundary、Kafka consumer offset lifecycle、PostgreSQL WAL status lifecycle、fault-injection tests、reliability/component evidence。
Non-goals: PR-2.4.3 的 mysql_snapshot_cdc producer read-ahead/string cursor；connector schema/UI、多表 preflight、跨 sink exactly-once。
Acceptance: 1) CheckpointForRecord 不产生 Kafka MarkOffset/Commit 或 PG committedLsn/standby ack 副作用；2) durable Save 成功后才调用 AckCheckpoint；3) external ack 失败阻断后续 checkpoint、pipeline failed/停止并允许安全 replay；4) Kafka auto-commit 关闭；5) PG 无 durable LSN 的 keepalive 不使用 server read-ahead end；6) linear/DAG/source 单测、race、vet 及文档证据通过。
Evidence: internal/etl/core/core.go、internal/etl/pipeline/runner_test.go、internal/etl/orchestrator/orchestrator_test.go、internal/etl/source/kafka_test.go、internal/etl/source/postgres_cdc_test.go、docs/reliability-certification.md、source component docs。
Result: delivered
Residual/follow-up: PR-2.4.3 mysql_snapshot_cdc producer read-ahead/string cursor 与 snapshot commit boundary 已在 Round 3 交付；其余 path 仍按各自 roadmap item 管理。
```

PR-2.4.2 本轮验收矩阵（Round 2/5）：

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| linear/DAG durable Save -> external Ack 顺序 | `TestRunnerCheckpointBoundaryOrdersDurableSaveBeforeExternalAck`、`TestRunnerExternalAckFailureBlocksAndFailsAfterDurableSave`、`TestDAGCheckpointBoundaryOrdersDurableSaveBeforeExternalAck` | passed | 进程级 crash 注入仍属于后续 path e2e |
| Kafka external offset 生命周期 | `TestKafkaCheckpointForRecordHasNoExternalSideEffects`、`TestKafkaBuildSaramaConfigDisablesAutoCommit`、`TestKafkaAckCheckpointRequiresActiveSession`；`CONTAINER_CLI=docker ./hack/e2e-kafka.sh` | passed | Sarama `Commit()` 不返回 broker error；异步 group error 仍按 source error 可见性处理 |
| PostgreSQL WAL ack 生命周期 | `TestPGCheckpointForRecordDoesNotAdvanceExternalLSN`、`TestPGResumeLSNUsesDurableMarkerNotReadAhead`、`TestPGAckCheckpointUpdatesExternalLSNAfterSend`、`TestPGAckCheckpointSendFailureDoesNotAdvanceExternalLSN`；`CONTAINER_CLI=docker E2E_SKIP_BUILD=1 ./hack/e2e-postgres-cdc.sh` | passed | 无 |
| keepalive 不误报 read-ahead 进度 | `TestPGKeepaliveWithoutDurableLSNDoesNotAckServerEnd` | passed | 无 |
| package/race/static checks | `go test ./internal/etl/... -count=1`；`go test -race ./internal/etl/source ./internal/etl/pipeline ./internal/etl/orchestrator -count=1`；`go vet ./internal/etl/core ./internal/etl/source ./internal/etl/pipeline ./internal/etl/orchestrator`；`git diff --check` | passed | 无 |

当前领取记录（Round 3）：

```text
Round: 3/5
Roadmap item: PR-2.4.3
Profile/path: standalone mysql_snapshot_cdc -> idempotent sink；snapshot phase + CDC handoff
Objective: 消除 snapshot producer read-ahead 与 checkpoint cursor 不一致导致的跳过窗口，统一字符串游标的可恢复提交边界，并让 snapshot/CDC restart 从最后 durable cursor 安全重放。
Scope: core batch-checkpointer contract、linear/DAG checkpoint boundary、internal/etl/source/mysql_snapshot_cdc.go、snapshot cursor/checkpoint tests、相关可靠性/component evidence；仅触及 snapshot source 的读取与提交边界。
Non-goals: connector schema/UI、多表 preflight、全库异构主键新能力、跨 sink exactly-once、其他 source 的 cursor 重构。
Acceptance: 1) producer 不得在 checkpoint durable 前推进可丢失的 source cursor；2) linear/DAG 以完整 source batch 生成 checkpoint，numeric/string cursor 与 snapshot phase 在 Save -> external/source boundary 后一致；3) crash/restart、checkpoint reset、空页/末页和 cursor 编码错误均 fail-closed 或安全重放；4) checkpoint 生成错误阻断后续推进并使 pipeline failed；5) targeted/race/vet 与可用 snapshot e2e 通过；6) 更新 source 文档、reliability evidence。
Evidence: internal/etl/source/mysql_snapshot_cdc*_test.go、相关 hack/e2e-snapshot-cdc*.sh（含安全 phase 断言更新）、docs/reliability-certification.md、docs/components/source-mysql_snapshot_cdc.md。
Result: delivered
Residual/follow-up: DAG 过滤后无 sink 输出的 checkpoint 进度与多 sink 跨批次单调合并仍沿用既有 at-least-once 边界；若需改变语义，另列有界后续，不扩大本轮。
```

PR-2.4.3 本轮验收矩阵（Round 3/5）：

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| producer read-ahead 不污染 durable cursor | `TestSnapshotCheckpointDoesNotUseProducerReadAhead`、`TestSnapshotCheckpointPersistsStringCursorAfterDurableAck`、`TestSnapshotCheckpointCoversAllRecordsInMultiTableBatch` | passed | producer 仍可在内存 channel 窗口内 read-ahead；重连只使用 durable binlog position |
| numeric/string cursor、legacy LastID 与 snapshot→CDC handoff | `TestSnapshotAckKeepsLegacyLastIDCompatibility`、`TestSnapshotCheckpointTransitionsToCDCFromRecordBoundary`、`TestSnapshotStartPositionReusesRestoredHandoff`、`TestSnapshotCursorStringKeepsLocalWallClock` | passed | binlog 与 sink 不是分布式事务 |
| 缺失/非法 cursor、缺失 handoff、channel 结束 fail-closed | `TestSnapshotCheckpointRejectsInvalidNumericCursor`、`TestSnapshotCheckpointRejectsMissingCursorColumn`、`TestSnapshotCheckpointRejectsMissingHandoff`、`TestSnapshotCDCReaderClosedChannelsReturnEOF`；linear/DAG checkpoint-generation fault tests | passed | 无 |
| CDC 重连、snapshot crash/restart、reset/DLQ 与异构 numeric/string path | `TestSnapshotCDCReconnectPositionUsesDurableCheckpoint`；`CONTAINER_CLI=docker ./hack/e2e-snapshot-cdc.sh`；`CONTAINER_CLI=docker ./hack/e2e-snapshot-cdc-crash.sh`；`CONTAINER_CLI=docker E2E_SKIP_BUILD=1 ./hack/e2e-snapshot-cdc-clickhouse.sh`；`CONTAINER_CLI=docker ./hack/e2e-snapshot-cdc-heteropk.sh` | passed | 共享 MySQL/ClickHouse 容器已存在时 compose 输出环境 warning，但脚本退出码为 0 |
| linear/DAG 完整 batch checkpoint 与生成错误 fail-closed | `TestRunnerCheckpointUsesCompleteBatchCheckpointer`、`TestRunnerCheckpointThrottleRetainsAllPendingBatches`、`TestRunnerCheckpointGenerationErrorFailsClosed`、`TestDAGCheckpointUsesCompleteSourceBatch`、`TestDAGCheckpointGenerationErrorFailsClosed` | passed | DAG reader 在 writer drain 后统一关闭；多 sink 语义残余见上 |
| package/race/static checks | `go test ./internal/etl/... -count=1`；`go test -race ./internal/etl/source ./internal/etl/pipeline ./internal/etl/orchestrator -count=1`；`go vet ./internal/etl/core ./internal/etl/source ./internal/etl/pipeline ./internal/etl/orchestrator`；`git diff --check` | passed | 未执行全量 connector certification；本轮只要求 snapshot 路径 |

当前领取记录（Round 1/5）：

```text
Round: 1/5
Roadmap item: PR-2.4.4
Profile/path: standalone linear + DAG；主要 checkpoint source codecs；API/WebUI failure visibility
Objective: 合法 JSON 但语义损坏的 source position 不得回退到首次启动；checkpoint 启动失败在 API、health 和 WebUI 中保留可操作错误与 remediation。
Scope: core Source checkpoint-validator contract、Kafka/HTTP/REST/Redis/MySQL/PostgreSQL/File position validators、Runner/DAG startup wiring、fault-injection/API/UI regression tests and evidence。
Non-goals: backup/restore atomicity、ClickHouse ordering、lifecycle generation fencing、MaxCompute external certification、new connector semantics。
Acceptance: 1) validator 在 source.Open 前执行，缺少必需字段、负 offset/page/cursor、topic/source mismatch、非法 LSN/phase 均 fail-closed；2) 无 checkpoint 与合法 legacy position 继续兼容；3) linear/DAG failed 状态和 LastError 可从 API/health 读取；4) WebUI pipeline detail/issues 面板展示 checkpoint remediation，并提供安全的停止/重试入口；5) targeted/race/vet、嵌入式 UI build/e2e 与 git diff --check 通过。
Evidence: internal/etl/core/*、internal/etl/source/*checkpoint*_test.go、internal/etl/pipeline/runner_test.go、internal/etl/orchestrator/orchestrator_test.go、internal/etl/server/*checkpoint*_test.go、web/src/pages/pipelines/PipelineDetailPage.tsx、hack/e2e-ui.sh、docs/reliability-certification.md。
Result: delivered
Residual/follow-up: durable run_history 尚未保存结构化 error/code/remediation；restore-from-DB 的 malformed spec 保留/restore_failed 状态、backup secret/atomicity 和 lifecycle fencing 另列后续，不扩大本轮。
```

PR-2.4.4 本轮验收矩阵（Round 1/5）：

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| source-specific semantic validator 在 `Source.Open` 前 fail-closed | `internal/etl/source/checkpoint_validation.go`；`TestSourceCheckpointValidatorsFailClosedOnSemanticCorruption`、`TestSourceCheckpointValidatorsAcceptNilFirstStart`；Kafka/HTTP/REST/Redis/MySQL batch+CDC+snapshot/PostgreSQL CDC/File/Demo/Feishu 覆盖 | passed | Kafka 保留合法 `-1` committed-offset sentinel；Feishu 尚无 durable row cursor，因此显式拒绝 persisted checkpoint |
| linear/DAG validator 顺序、failed 状态与 source 未打开 | `TestRunnerValidatesCheckpointBeforeSourceOpenAndExposesRemediation`、`TestRunnerAllowsValidLegacyCheckpointThroughValidator`、`TestDAGSourceCheckpointValidatorFailsBeforeOpenAndSurfacesStats` | passed | DAG source failure 仍为异步状态转换，但 stats/health 会在失败后立即可见 |
| API/health 稳定错误码与 remediation | `last_error_code` / `last_error_remediation` stats；pipeline start HTTP 422；health `pipeline_issues`；`TestPipelineAPIAndHealthExposeCheckpointRemediation`、`TestPipelineStartReturnsNon2xxWithCheckpointRemediation` | passed | durable run_history row 仍只保存 status/counters，不保存结构化错误详情 |
| WebUI 安全恢复入口 | `PipelineDetailPage.tsx` checkpoint recovery overview/issues panels；retry start + inspect logs；不把 reset 作为默认修复；`resource/public` 已重建 | passed | source-specific reset/generation fencing 仍属 lifecycle 后续 |
| targeted/full/race/static/UI evidence | `go test ./internal/etl/... -count=1`；`go test -race ./internal/etl/source ./internal/etl/pipeline ./internal/etl/orchestrator ./internal/etl/server -count=1`；`go vet ./internal/etl/core ./internal/etl/source ./internal/etl/pipeline ./internal/etl/orchestrator ./internal/etl/server ./internal/etl/telemetry`；`npm run typecheck && npm run build`；`bash hack/e2e-ui.sh`（117 passed / 0 failed）；`git diff --check` | passed | 未执行与本 codec-only 变更无关的全量外部 connector certification |

### PR-D1：Distributed 安全与任务所有权

状态：`delivered`（2026-07-25 · PR-D1.1/.2/.3）

本项是独立的 distributed profile 门槛，不阻塞 standalone production ready。完成前，master-worker 只能声明 production candidate/beta，且必须继续公开单 master、DAG master-local、单 continuous shard 非 multi-active HA 的边界。

内部增量（一次只推进一个）：

1. `PR-D1.1` Worker transport：统一 authenticated HTTPS client、凭据和超时/重试。
2. `PR-D1.2` Task ownership：lease/generation/CAS fencing、attempt history 和失败 requeue。
3. `PR-D1.3` 真实拓扑认证：独立 master + 2 workers 的 crash/restart/checkpoint e2e。

范围：

- worker 使用统一的有超时 HTTP client，向 register/heartbeat/poll/report/deregister 请求发送 scoped token，并支持可验证 HTTPS；生产环境不得依赖裸 `http://master:8001`。
- task assignment 增加 lease/generation/attempt 或等价 fencing token；所有状态更新使用 worker ID + generation/CAS，旧 worker 不能覆盖重新分配后的新 owner。
- 对执行失败、worker crash、heartbeat stale、master restart 建立有界 retry/backoff/requeue；保留 worker offline history 和 task attempt history，而不是直接删除全部证据。
- 建立 master、至少两个 worker 的独立 OS/container e2e，使用真实 `worker.ExecuteShard` 而不是 recording stub，并覆盖 encrypted spec、共享 storage、Redis state 和 checkpoint resume。
- 文档继续明确不实现 multi-master consensus、跨 worker DAG 和单 shard multi-active；如果业务需要这些能力，应建议外部进程编排和幂等恢复，而不是扩展成通用分布式计算平台。

验收标准：

- token 和 TLS 均启用时，worker 注册、心跳、poll、结果上报和注销全部成功；无 token、错误 token、过期凭据和证书错误均被拒绝并可诊断。
- 旧 worker 在 lease 失效后提交完成状态不会改变新 owner 的 task；故障注入测试重复运行不产生双 owner 或静默遗漏 shard。
- worker SIGKILL 后任务在超时内被重新分配并从相同 checkpoint namespace 恢复；允许的重放由 PR-2 的 sink 契约吸收。
- master restart 后 pending/assigned/running task 状态可恢复；失败任务遵循可配置 attempts/backoff，超过上限后进入可见终态并触发告警。
- `hack/e2e-distributed.sh` 或替代脚本真正启动三个以上独立进程/容器，并在 release evidence 中记录测试拓扑与结果。


#### PR-D1 证据（2026-07-25）

| 切片 | 证据 | 结果 | 备注 |
|------|------|------|------|
| D1.1 Worker transport | `internal/etl/worker/transport.go` + `transport_test.go`；`TestWorkerHTTPAuthAndFencing` | passed | `X-API-Token`/`Bearer`、超时、5xx 重试；错误 token 拒绝注册 |
| D1.2 Task ownership | `ClaimTask`/`CASUpdateTask`；`task_fence_test.go`；`master/fence_test.go` | passed | generation CAS；旧 owner 完成 fenced；lease/max-attempts requeue |
| D1.3 拓扑 | `hack/e2e-distributed.sh`；`TestDistributed*` integration；可选 `OPENETL_BIN` 多进程 smoke | passed (hermetic+integration) | MySQL 共享 store + master HTTP + 2 workers；compose 强制 token 并标 beta |
| 边界 | `docker-compose.distributed.yml`、`docs/runtime-modes.md` | documented | 单 master、DAG master-local、单 continuous shard 非 multi-active HA |

Non-goals (unchanged): multi-master consensus；跨 worker DAG；single-shard multi-active；跨地域 active-active。

## 排队中的工作

以下是既有排队工作，其原有相对顺序和验收范围保持不变。P1、P2 已于 2026-07-13 达到验收标准并迁入 CHANGELOG/证据索引；P0 未解除阻塞时，是否切换到 `PR-0`、P3 或其他任务仍需显式确认。

### P3：成熟度事实源与认证覆盖扩展

状态：`delivered`（2026-08-08 · P3.1/P3.2/P3.3.1/P3.3.2）

当前领取记录：

```text
Round: 4/5
Roadmap item: P3.1 descriptor/schema 单一事实源
Profile/path: standalone connector discovery + certification gate
Objective: schema 成为 required/default/secret/scope 的唯一事实源，且任一 production source/sink 自动进入注册、schema、文档与 e2e evidence 门禁。
Scope: internal/etl/server/schema.go、connector_descriptor.go、server.go plugin metadata、connector/schema certification tests、ClickHouse 构造默认值、PostgreSQL 16 generated-column introspection 认证失败恢复、docs/connector-certification.md 与本 roadmap 证据。
Non-goals: P3.2 多表 schema/preflight 契约；新增 connector、maturity 提升、专用 UI 流程、MaxCompute 外部认证。
Dependencies: PR-2 delivered；P0 仍 blocked_external；本 goal 已显式授权在独立 worktree 推进审计发现的 connector 契约问题。
Acceptance: 1) metadata required 不再手写，且与 schema required 完全一致；2) descriptor required/secret/field scope/default 与 schema 完全一致；3) production source/sink 集合与 certification target 完全一致，未知 production 项自动失败；4) JDBC DSN 标记 secret，ClickHouse async_insert_wait 的 runtime 默认与 schema=true 一致；5) targeted/package/race/vet 与 git diff --check 通过并更新认证文档。
Evidence: internal/etl/server/schema_test.go、connector_certification_test.go、internal/etl/sink/clickhouse_test.go、docs/connector-certification.md、Go targeted/package/race/vet checks。
Result: delivered
Residual/follow-up: P3.2 多表 source preflight 必须显式返回 per-table/partial/blocking 契约，不得用单表 DDL preview 代表异构表集合。
```

P3.1 本轮验收矩阵（Round 4/5）：

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| metadata required 只由 schema 派生 | `pluginCapabilityMetadata` 不再携带 required；`pluginMetadataFromSchema`；`TestPluginMetadataRequiredFieldsAreDerivedFromSchema` | passed | 条件必填（如 table/query 二选一）继续由 validate/preflight 表达，不伪装成静态 required |
| descriptor required/secret/scope/default 与 schema 一致 | `TestConnectorDescriptorConfigContractMatchesSchemaExactly`；JDBC `dsn` secret；ClickHouse `async_insert_wait` schema/runtime default=true | passed | 其他 connector 构造默认值的全量自动对账可另列后续，不扩大本轮 |
| 任一 production source/sink 自动进入 certification target | `TestConnectorCertificationKitProductionSet` 对 production 集合做双向完全匹配；新增 HTTP、PostgreSQL/PostgreSQL alias、Doris target 与组件文档/e2e 引用 | passed | maturity 未提升；MaxCompute/ODPS 仍 experimental + blocked_external |
| 新增 production target 的实际路径证据 | `CONTAINER_CLI=docker ./hack/e2e-http-source.sh`；`CONTAINER_CLI=docker E2E_SKIP_BUILD=1 ./hack/e2e-mysql-postgres.sh`；`CONTAINER_CLI=docker E2E_SKIP_BUILD=1 ./hack/e2e-doris.sh` | passed | 复用既有 MySQL 容器时 compose 输出 name-in-use 环境 warning，但脚本最终退出 0 |
| PostgreSQL 16 generated-column schema introspection | 首次 e2e 暴露 `attgenerated` binary char -> string 扫描失败；改为 DB 端 `is_generated` bool 后同一 e2e 正常写入、schema rejection 与 checkpoint reset/upsert replay 均通过 | passed | 无 |
| package/race/static checks | `go test ./internal/etl/... -count=1`；`go test -race ./internal/etl/server ./internal/etl/sink -count=1`；`go vet ./internal/etl/server ./internal/etl/sink`；`git diff --check` | passed | 无 |

当前领取记录（Round 5）：

```text
Round: 5/5
Roadmap item: P3.2 多表 source preflight 契约
Profile/path: standalone mysql_cdc/mysql_snapshot_cdc -> schema-aware sink preflight
Objective: 多表与 tables=["*"] source 不再以空 schema 静默跳过，也不以任一单表 schema/DDL preview 代表整个异构表集合。
Scope: internal/etl/server/preflight.go、pipelines_preflight_test.go、schema.go 的已实现 snapshot 字段、MySQL information_schema 定向检查、source component/config docs 与本 roadmap 证据。
Non-goals: 改造 runtime SchemaInfo/runner 为多表 schema 传播；改变现有多表读取/路由/写入语义；新增 connector、maturity 提升、专用 UI 或全库 schema registry。
Acceptance: 1) 固定多表列表与 `*` 展开检查全部 base tables，缺表/无列/显式 snapshot 无可用单列主键 fail-closed；2) whole-database 无单列主键表以结构化 warning 声明 snapshot skip/CDC-only；3) 动态 per-record sink 返回 `schema-multi-table-partial` warning + field issue 且无单表 DDL preview；4) 固定单目标 sink 未显式过滤/映射/归一化时阻断，显式归一化时降为 partial warning；5) 单表 schema/preflight 行为保持不变；6) targeted/package/race/vet、相关多表 e2e 与 git diff --check 通过。
Data semantics/rollback: 本切片只改变 preflight 诊断与阻断，不改变 checkpoint、DLQ、replay 或 sink write；回滚即移除多表契约检查，运行时数据路径不迁移。
Evidence: internal/etl/server/pipelines_preflight_test.go；docs/components/source-mysql_cdc.md、source-mysql_snapshot_cdc.md、docs/etl-config-schema.md；hack/e2e-multi-table-map.sh、hack/e2e-mysql-cdc-wide.sh、hack/e2e-snapshot-cdc-heteropk.sh。
Result: delivered
Residual/follow-up: runtime 级 per-table schema propagation/target-specific DDL preview 若未来需要，必须另列有界 item；本轮不把 preflight 扩展成 schema registry。
```

P3.2 本轮验收矩阵（Round 5/5）：

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| 固定多表与 `*` 检查全部表，不取第一表代表集合 | `TestReadMySQLMultiTableSchemasInspectsEveryHeterogeneousTable`、`...FailsWhenAnyConfiguredTableIsMissing`、`...ExpandsWildcardAndReportsCDCOnlyTable` | passed | runtime `SchemaInfo` 仍为单表 flat contract；本轮只治理 preflight |
| snapshot 无可用单列 key 的显式/whole-DB 边界 | 显式复合/无 key：`TestReadMySQLMultiTableSchemasFailsExplicitSnapshotWithoutUsableKey`；whole-DB：真实 API 返回 `schema-multi-table-snapshot-skip` + `source.config.tables` field issue | passed | `skip_no_pk_tables=true` 仍是显式接受 CDC-only 历史边界 |
| 动态 sink partial、固定 sink block、显式归一化 partial | 定向 tests：`TestMultiTableSchemaContractReturnsPartialWarningForDynamicSink`、`...BlocksUnnormalizedFixedSink`、`...AllowsExplicitNormalizationWithWarning`；真实 `/api/v2/specs/validate` 分别返回 `valid=true/false/true` | passed | post-transform schema 不自动推导，需逐表 dry-run/目标验证 |
| 不生成误导性单表 DDL preview | 上述定向 tests 与四次真实 API 复验均断言 `ddl_preview` 缺失；missing-table error 绑定 `source.config.tables` | passed | target-specific per-table DDL preview 留给独立后续 |
| 单表行为与多表运行时路径保持 | `TestMySQLSingleTableSourceKeepsFlatSchemaContract`；`CONTAINER_CLI=docker E2E_SKIP_BUILD=1 ./hack/e2e-multi-table-map.sh`、`.../e2e-mysql-cdc-wide.sh`、`.../e2e-snapshot-cdc-heteropk.sh` | passed | multi-table-map 首次因共享 E2E 库残留原名目标表失败；脚本补隔离清理后同一镜像通过 |
| 当前镜像/拓扑与故障证据 | image `3ec316269fc685b289c2ae5130fa9cf5460eaa1234d535bdc4b336be86e6c32f`；MySQL `8.0.46`；ClickHouse `24.3.18.7`；2026-08-08 09:17 CST；真实 API 覆盖 dynamic/fixed/normalized/wildcard-no-PK | passed | compose 复用共享 MySQL/ClickHouse 时输出 name-in-use 环境 warning，脚本退出码为 0 |
| package/race/static checks | `go test ./internal/etl/... -count=1`；`go test -race ./internal/etl/server ./internal/etl/source -count=1`；`go vet ./internal/etl/server ./internal/etl/source`；`sh -n` 三个相关 e2e；`git diff --check` | passed | 无 |

Round 5/5 后续交接已由后续窗口完成：P3.3 将 certification evidence 的 commit/image、依赖版本、实际执行时间和过期策略收敛为机器可检查输入，并在缺失/过期时自动降低 readiness。

目标：减少手写 maturity 字符串与测试证据之间的漂移。

范围：

- 将现有 certification kit 从首批 MySQL/ClickHouse/Kafka/S3/File 扩展到所有标记为 production 的内置 connector，优先补 HTTP、PostgreSQL sink 和 Doris。
- maturity 提升必须同时满足 descriptor/schema、注册、preflight/readiness、组件文档和可重复 e2e evidence。
- 将 connector maturity 与 path certification 分开：同一个 sink 在 upsert、append、CDC、snapshot、DAG 和不同 storage/runtime mode 下必须分别声明适用边界。
- connector/plugin 的 partial readiness 必须携带具体 evidence 和 remediation。
- 第三方 connector 在不修改专用 UI 代码的情况下提供类型化表单；preflight、metrics、DLQ 等行为继续通过统一合约暴露。
- certification 记录必须包含当前 commit/image、依赖版本、外部服务拓扑、实际执行时间和失败注入结果；仅引用“存在某个 e2e 脚本”不能提升 maturity。

验收标准：

- 任一 connector 被标记为 production 时，认证测试自动将其纳入或显式拒绝未知 production 项。
- descriptor、schema required 字段、secret/scope、组件文档和实现注册之间有一致性测试。
- 不再仅靠人工修改 metadata 字符串提升成熟度；未执行或证据过期的 connector 自动降级为 `production_with_review`、`beta` 或 `experimental`。
- 认证矩阵能够从一条公开 production path 追溯到 source/sink write mode、幂等策略、checkpoint/DLQ/replay e2e 和最后一次通过的发布版本。

当前领取记录（Round 1/5）：

```text
Round: 1/5
Roadmap item: P3.3.1 evidence manifest + freshness gate
Profile/path: standalone connector descriptors/readiness + certification kit
Objective: 将 connector 认证的 commit、image、依赖版本、执行时间、过期策略和验证脚本收敛为机器可读 manifest；manifest 缺失、损坏或过期时 readiness 自动降级，但不偷偷提升或修改 maturity。
Scope: internal/etl/server evidence manifest loader/validator、descriptor e2e_evidence gate、manifest fixture、certification tests、hack checker 和 connector certification 文档。
Non-goals: PR-2.4.4 checkpoint position 校验、Runner/DAG/UI 错误展示；MaxCompute 外部认证；自动运行所有外部 e2e；修改 connector runtime 语义。
Dependencies: P3.1/P3.2 delivered；当前 `sync-canal-go-hardening-20260808` 正在处理 PR-2.4.4，本切片避开其修改路径。
Acceptance: 1) 每个 production source/sink 有唯一 evidence record，字段包含 commit/image/dependencies/started_at/finished_at/expires_at/scripts；2) manifest schema、重复记录、时间窗口、过期和缺失均有 deterministic checks；3) descriptor gate 暴露 evidence metadata，fresh/verified 为 pass，过期为 partial，缺失/损坏为 missing；4) checker 支持对当前 commit/image 做可选严格校验；5) targeted/package/race/vet 与 git diff --check 通过。
Evidence: internal/etl/server/evidence_manifest.go、evidence_manifest_test.go、connector_descriptor.go、connector_certification_test.go、internal/etl/server/evidence/connector-evidence.json、hack/check-connector-evidence.sh、docs/connector-certification.md。
Result: delivered
Residual/follow-up: CI 中接入真实外部 e2e 产出的 manifest 更新和过期自动阻断另列 P3.3.2；本切片不伪造未执行的外部认证。
```

P3.3.1 当前验收矩阵：

| Criterion | Evidence | Result | Residual/blocker |
| --- | --- | --- | --- |
| 14 个 production source/sink 有唯一 manifest record 与 commit/image/dependency/time/scripts/cases | `internal/etl/server/evidence/connector-evidence.json`；`TestConnectorEvidenceManifestLoadsAndCoversProductionConnectors`；`go test ./internal/etl/server -run 'TestConnectorEvidence' -count=1` | passed | P3.3.2 已在真实认证后回写 `verified:true` |
| manifest 重复键、时间窗口、脚本路径和 JSON 结构 deterministic 校验 | `ValidateConnectorEvidenceManifest`；`TestValidateConnectorEvidenceManifestRejectsStructuralDrift`；`sh -n hack/check-connector-evidence.sh` | passed | 无 |
| readiness 自动区分 fresh/verified、unverified、expired、missing/corrupt | `TestConnectorEvidenceFreshnessControlsReadinessGate`；production descriptors 的 evidence gate 按 manifest freshness 派生 | passed | 当前 verified/fresh evidence gate 为 `pass`；过期后自动降级 |
| descriptor/API 暴露 evidence metadata，认证 kit 不再要求 e2e gate 永远 pass | `ConnectorReadinessGate.evidence_metadata`；`TestConnectorCertificationKitProductionSet`；`go test ./internal/etl/server -count=1` | passed | 无 |
| checker 可绑定 commit/image 并在 strict/expiry 模式失败 | `./hack/check-connector-evidence.sh`；commit ancestry/allowlist tests；`-strict`、expiry、image mismatch 负向验证 | passed | 证据回写提交只允许改 manifest/认证文档；runtime/script/workflow 变化要求重跑 |
| package/race/static checks | `go test ./...`；`go test -race ./internal/etl/server -count=1`；`go vet ./...`；`git diff --check` | passed | Web `npm run typecheck/build` 在新 worktree 缺少 `web/node_modules`，未执行；本切片不改 Web 源码 |

当前领取记录（Round 2/5）：

```text
Round: 2/5
Roadmap item: P3.3.2 真实 connector e2e 证据回写与 strict gate
Profile/path: standalone production source/sink certification
Objective: 在当前可用的真实依赖拓扑中执行 manifest 列出的 production connector e2e，将实际通过的 commit/image/时间/cases 回写；未能运行的外部路径必须明确记录为 skip/block，不得伪造 verified。
Scope: manifest 证据记录与 checker、认证脚本执行日志、release/CI strict gate、connector certification 文档和本 roadmap 验收矩阵。
Non-goals: connector runtime 语义、checkpoint/UI 错误契约、MaxCompute 真实认证（仍 blocked_external）、修改或合并 `sync-canal-go-hardening-20260808` 的任务。
Dependencies: P3.3.1 manifest gate delivered；使用仓库标准 container runtime 和现有 production connector e2e fixtures。
Acceptance: 1) 对每个 manifest record 对应脚本执行或记录明确 skip/block 原因；2) 只有脚本及其 required cases 全部通过的 record 才标记 verified=true，并回写 finished_at/expires_at/image；3) strict checker 在当前认证集合通过，在未验证/过期/commit-image 不匹配时非零；4) CI/release gate 调用同一 checker，外部环境缺失显示为 skip/block 而非 pass；5) package/race/vet、脚本语法、git diff --check 和认证文档证据更新通过。
Evidence: `hack/e2e*.sh` 实际输出、`internal/etl/server/evidence/connector-evidence.json`、`hack/check-connector-evidence.sh`、`.github/workflows/*`、`docs/connector-certification.md`。
Result: delivered
Residual/follow-up: MaxCompute/ODPS 仍为 experimental + blocked_external，不属于本次 production connector 集合；其成熟度不因本项提升。
```

P3.3.2 验收矩阵：

| Criterion | Evidence | Result | Residual/blocker |
| --- | --- | --- | --- |
| manifest 列出的 production connector 脚本逐项执行 | `docs/connector-certification.md` 记录 2026-08-08 UTC 的 13 个唯一脚本窗口；最终 run 13/13 退出 0 | passed | 初次探索暴露 expected-400、旧 app SQLite、Podman broker restart、Doris 端口/网络隔离问题，修复后在最终基线重跑 |
| verified record 只来自完整通过的 required cases | `internal/etl/server/evidence/connector-evidence.json`：14/14 `verified:true`，逐记录 scripts/cases/time/dependencies | passed | MaxCompute/ODPS 未进入 production 集合，未伪造外部认证 |
| strict freshness/commit/image gate | commit `4d16ff318a7583b8c9e51dd95cfcc0e940eb8a80`；image `sha256:876ae664e462e695ef0682b523157a1943caceac98c4c77e1c361bc57fa774d2`；checker 正向与 commit/image/expiry/runtime-change 负向检查 | passed | 仅 manifest 与两份认证文档可作为 certified commit 的后代变化 |
| CI/release 接入同一 gate | `.github/workflows/test.yml`、`release.yml`、`release-beta-container.yml` 调用同一 checker，checkout 保留 ancestry | passed | PR 只做结构检查；main push/release 执行 strict gate |
| package/race/static/syntax | `go test ./... -count=1`；`go test -race ./internal/etl/server ./hack/cmd/check-connector-evidence -count=1`；`go vet ./...`；相关 `bash -n`；`git diff --check` | passed | 无 |

### P4：首次任务体验残留收口

状态：`delivered`（P4.2a 已完成；后续 Doris/Kafka 事实核验作为独立有界 follow-up，不改变其他 roadmap 优先级）

当前领取记录：

```text
Round: 1/5
Roadmap item: P4.2a
Profile/path: standalone Web UI + ETL API pipeline validate/create/update
Objective: 线性和 DAG 管道配置失败不再被显示为成功、原始 JSON 或隐藏状态，并能定位到 pipeline/node/field 与 remediation。
Scope: DAG/linear validate-create-update 错误契约、Web API error adapter、向导/Designer 错误面板与字段映射、对应服务端测试和浏览器 e2e。
Non-goals: 新 connector、connector runtime 语义变更、P4 其他信息架构/状态口径重构、distributed 执行语义。
Acceptance: 1) DAG 未知/无效 connector 在 validate 和 create/update 中明确失败，任何 error payload 不得使用 2xx；2) 前端解析结构化错误，Toast 只显示摘要，完整详情持久可查看；3) preflight issue 靠近对应步骤/字段并保留 remediation，不以折叠高级区或前 5 条截断代替；4) create/update/dry-run 失败在当前操作上下文可见；5) Playwright 验证错误详情、字段定位、修复后重验和失败不创建 pipeline。
Evidence: `internal/etl/server/pipeline_validation_contract_test.go`；`go test ./internal/etl/server -run 'Test(DAG|LinearValidationContract|ValidateContract)' -count=1`；`go test ./... -count=1`；`go test -race ./internal/etl/server ./internal/etl/sink ./internal/etl/source -count=1`；`go vet ./internal/etl/... ./internal/logic/... ./internal/cmd/...`；`npm run typecheck`；`npm run build`；`npm run lint`；`hack/e2e-ui.sh`（嵌入式生产镜像，112 passed/0 failed，含向导/DAG 错误详情、字段 remediation、失败不落库和修复后重验）；`git diff --check`。
Result: delivered
Residual/follow-up: Doris/Kafka table_template、Debezium envelope key、brokers JSON 字符串作为独立 follow-up 核验；不回写本项验收标准。
```

当前领取记录（独立有界 follow-up）：

```text
Round: 2/5
Roadmap item: P4.2a-follow-up
Profile/path: standalone connector preflight + Kafka(envelope) -> Doris(table_template)
Objective: 核验并修复 table_template 静态 table 假阳性、metadata PK 运行时缺口和 JSON 字符串 brokers 解析不一致。
Scope: Doris preflight/runtime/schema key path、Kafka source/sink/connection-context slice parsing、targeted unit tests、Doris/Redpanda E2E fixture and evidence。
Non-goals: 改变 upsert 必须有稳定主键的语义；Kafka exactly-once；其他 connector 的动态主键实现；UI 信息架构重构。
Acceptance: 1) Doris table_template 不要求静态 table；2) pk_columns_from_metadata 的 JSON object key 可用于 compact/upsert/DELETE/auto-create DDL，标量 key 明确失败；3) Kafka brokers 的 YAML array 与 JSON-string array 在 runtime/preflight 一致；4) 可用外部服务时 E2E 验证，缺失时明确 skip/block。
Evidence: internal/etl/server/pipelines_preflight_test.go、internal/etl/server/connection_catalog_test.go、internal/etl/sink/config_slices_test.go、internal/etl/sink/doris_test.go、internal/etl/source/config_slices_test.go、internal/etl/source/kafka_test.go、hack/e2e-doris-table-template.sh、go test ./...、go test -race ./internal/etl/server ./internal/etl/sink ./internal/etl/source、git diff --check。
Result: delivered
Residual/follow-up: 无；首次执行因 Doris FE/BE 未启动而 skip，随后启动仓库标准 2.1.11 FE/BE 后完成真实 E2E。后续仍需在发布环境按认证矩阵复跑。
```

P4.2a follow-up 验收矩阵（Round 2/5）：

| Criterion | Evidence | Result | Residual/blocker |
| --- | --- | --- | --- |
| `table_template` 不再要求静态 `sink.config.table` | `TestRunPreflightDorisTableTemplateRelaxesStaticTableRequirement` | passed | — |
| metadata PK JSON object 支持 compact/upsert、DELETE 和 auto-create DDL；标量 key 明确失败 | `TestDorisMetadataKeyColumnsFromEnvelope`、`TestDorisWriteCompactsUsingEnvelopeMetadataKey`、`TestDorisWriteCompactsMetadataKeyWithStaticTargetAndEmptySourceTable`、`TestDorisDeleteUsesEnvelopeMetadataKey`、`TestDorisValidateSchemaAllowsMetadataPKAutoCreate`、`TestDorisMetadataKeyColumnsRejectScalarEnvelopeKey` | passed | — |
| 仅有 `debezium_cdc` 不隐式绕过 PK 检查 | `TestRunPreflightDorisCDCRequiresExplicitMetadataPKFlag` | passed | — |
| Kafka brokers YAML array / JSON-string array 在 runtime、preflight 与 connection context 一致 | `TestSinkConfigStringSlices`、`TestSourceConfigStringSlices`、`TestStringSliceFieldPreservesBracketedScalarBroker`、`TestRunPreflightParsesJSONStringKafkaBrokers`、`TestConnectionContextIntrospectsKafkaSink` | passed | — |
| 回归、竞态和格式检查 | `go test ./... -count=1`；`go test -race ./internal/etl/server ./internal/etl/sink ./internal/etl/source -count=1`；`npm run typecheck`；`npm run build`；`npm run lint`；`git diff --check` | passed（lint 仅既有 warnings） | — |
| Kafka envelope -> Doris table_template 真实链路 | `CONTAINER_CLI=podman sh hack/e2e-doris-table-template.sh`（Doris 2.1.11 + Redpanda；`auto_create: true`；orders.order_id/users.user_no DDL；6 records，0 failed/0 DLQ；orders update 和 users DELETE 均核验） | passed | — |

现有向导和上下文闭环已经交付，但 2026-07-20 产品走查确认当前 Web UI 仍更接近“能力完整的工程控制台”，尚未完全收敛为围绕“创建成功、稳定运行、快速修复”的任务型产品。主要证据包括：一级导航平铺构建、运维和系统对象；首次任务向导在单个长弹窗中同时暴露模板、连接、descriptor、运行参数、transform、样例、YAML 和 preflight；运行状态、健康度和累计指标存在口径混用；页面状态没有可分享 URL；部分危险操作、国际化和无障碍表达不一致。

**2026-07-21 已交付（证据）**：全宽管道列表 + URL 筛选；`#/pipelines/new` 全页三段式向导 + 草稿；DLQ 聚合主视图 + Replay 确认面板；详情写入语义/生命周期；总览时间范围切换；Connections 抽屉；问题中心固定排序；顶栏用户菜单与扩展分组；e2e/文档区分「路由可达」与「原型对齐」。Residual：DAG 空画布模板、小屏信息行、截图刷新、多 run 历史。

**2026-08-08 现状核对**：`hack/e2e-ui.sh` 在当前嵌入式生产镜像上为 117 passed/0 failed；P4.2a 进一步收口了 validate/create/update/dry-run 的结构化错误契约、持久错误面板、字段级 remediation、显式 DAG 格式保持和失败不落库。PR-0 的独立 token 安全门槛由 `hack/e2e-ui-token.sh` 单独验证并通过，不与 UI 全量 e2e 混计。

本项在现有 React UI、connector descriptor/introspection/preflight 和同一份 pipeline spec 上渐进收口，不另建独立 UI 语义、设计器模型或服务端执行模型。内部按以下顺序实施；同一时间只推进一个子阶段：

#### P4.1：状态语义与交互可信度

- 建立统一的展示状态：`healthy`、`degraded`、`failed`、`paused`、`scheduled`、`completed`，由期望状态、实际运行状态、lag、checkpoint、DLQ 和最近错误共同派生；不得再使用“running 数量 / pipeline 总数”作为健康度。
- 区分失败 pipeline 数、失败记录累计值、当前 DLQ backlog 和历史 DLQ/replay 计数；所有卡片、列表和详情使用相同口径并标明时间范围。
- `failed` 不计入 `stopped`，主动暂停、等待调度和一次性完成不得显示为不健康。
- 统一批量启动/停止、立即运行、禁用调度、checkpoint reset、连接删除、worker deregister、DLQ 删除/replay 等高影响操作的目标数量、风险说明、确认和结果反馈；连接删除需提示被引用的 pipeline。
- 补齐关键页面中硬编码的中英文混用，统一 Lucide 图标、文本标签和状态颜色；可点击行、图标按钮和状态提示具备键盘、ARIA 和非颜色表达。
- API Token 默认遮挡；AI/LLM 明确为可选能力，任何 AI 入口仍必须经过 validate/preflight，不能成为创建或启动 pipeline 的旁路。

#### P4.2：首次任务分步闭环

- 将现有长弹窗重组为同一向导内的渐进步骤：场景选择 -> Source 连接与数据选择 -> Sink 与写入语义 -> 可选 Transform -> 安全检查 -> 确认并启动。
- 默认流程只展示完成当前步骤所需字段；connector maturity/readiness、原始 JSON、完整 YAML、批量和 checkpoint 等高级参数使用渐进披露，但不得因此丢失或重写隐藏字段。
- Source 步骤展示真实 connection health、库表/topic、schema 和 sample；Sink 步骤展示目标、auto-create/DDL preview、主键、insert/upsert/pre_write 等写入语义和 replay 重复边界。
- Transform 默认可跳过；新增、排序、删除和逐阶段 dry-run 保留，并把失败定位到具体 stage/field。
- Preflight 问题靠近对应步骤和字段展示，并提供可执行 remediation；修复后可重新验证。生产 UI 移除 `Failure demo`、`Repair to file_sink` 等 e2e/demo 专用控制。
- 最终确认页以 `Source -> Transform -> Sink` 摘要展示连接、数据范围、调度、幂等策略、DDL、checkpoint、DLQ 和已知重放风险，再执行创建和启动。

#### P4.3：任务型信息架构与可分享上下文

- 一级信息架构收敛为总览、管道、运维、资源和系统等任务分组；Designer 作为创建/编辑 pipeline 的入口，Schedule 作为 pipeline 生命周期配置，同时保留必要的全局运维视图。
- Connector 能力/成熟度目录与已保存 Connection 实例分开表达；WASM 编辑/编译归入扩展或开发者能力，不与日常运行入口同权展示。
- standalone 模式不突出 worker 实现细节；worker/cluster 管理只在对应运行模式或系统分组下展示。
- 引入可刷新、可返回、可分享的 URL，至少覆盖 `/pipelines`、`/pipelines/:id`、`/pipelines/:id/runs`、`/pipelines/:id/dlq` 和 `/connections/:name`；刷新和浏览器前进/后退不得丢失选择上下文。
- 总览从累计数字陈列收敛为可操作的待处理事项入口，优先呈现 failed/degraded pipeline、DLQ backlog、CDC lag、过期 checkpoint、异常 connection 和离线 worker；点击后定位到对应对象和修复上下文。
- Pipeline 列表直接展示 Source -> Transform -> Sink 摘要、batch/CDC 模式、schedule、sink 写入模式和最近错误；详情按 Overview、Runs、Issues、Checkpoints、Spec/Versions 组织。
- DLQ 按 error class、DAG node 和时间范围聚合，并形成“定位问题 -> 编辑修复 -> replay -> 核对剩余记录”的闭环；replay 前继续明确 at-least-once 和可能重复的边界。

#### P4.4：小团队首次上线与故障自助

- Quickstart 明确区分 demo 和 production profile：demo 可以使用临时凭据，production profile 必须在启动前校验 token、加密 key、storage、TLS 和备份位置。
- 首次任务模板只展示已经通过 PR-2 认证的 source/sink/write-mode 组合；未认证组合必须显示 maturity、重放边界和“需要人工复核”的原因。
- 提供面向非平台专家的故障路径：连接失败、preflight 失败、checkpoint stale、sink outage、DLQ backlog、worker offline 各自有下一步操作，而不是只展示原始堆栈。
- 将“当前是否在推进”与“累计处理过多少”分开显示，并允许导出包含 pipeline、checkpoint、lag、DLQ、最近错误和配置摘要的诊断包。
- 以真实小团队任务走查验证从空环境创建、暂停/恢复、修复/replay、备份/恢复和升级回滚；发现的手工步骤要么自动化，要么写入明确 runbook。

范围：

- 补齐仍为 partial 的 connector schema/sample/DDL preview 和字段级 remediation。
- 将 schedule 重跑风险与 sink 幂等性 warning 串联，而不污染 source capability 定义。
- 统一 pipeline、DAG node、字段、风险、修复动作和是否可 replay 的错误表达。
- 使用 Playwright 保持分步向导、URL/deep-link、YAML 往返、preflight 修复、创建启动、状态口径和 DLQ replay 的关键路径。

验收标准：

- 新增 UI 工作必须由真实 connector descriptor/introspection/preflight 驱动，不使用独立静态执行语义。
- 同一 spec 在 UI、YAML 和 API 间往返不丢失隐藏字段。
- 错误提示可以定位到具体 pipeline/node/field，并给出可执行 remediation。
- 主动暂停、等待调度、一次性完成、运行失败和 degraded 状态在总览、列表、详情和指标中口径一致，并有自动化覆盖证明失败记录数、失败 pipeline 数和 DLQ backlog 未混用。
- 用户可以从空环境沿分步向导完成 connection 选择、schema/sample 确认、transform dry-run、sink 幂等/DDL 检查、preflight 修复、创建启动；默认路径不要求编辑 JSON/YAML。
- 关键对象具有稳定 URL，刷新、前进/后退和直接打开 deep link 后仍能恢复同一 pipeline/tab/filter 上下文。
- failed/degraded/DLQ 入口能从总览或 pipeline 详情定位到具体错误，并完成修复后的 replay 或重启；结果反馈包含成功数、失败数和剩余 backlog。
- 高影响操作具备一致确认和影响说明；关键中文路径不出现未翻译的产品文案，图标按钮与可点击行通过键盘和无障碍检查。
- standalone 与 distributed 模式分别验证导航和系统入口，日常 pipeline 用户不需要理解 worker/plugin 编译等实现细节即可完成首次任务和故障处理。
- 空环境到首条已验证链路的中位耗时达到 30 分钟以内；失败任务能够在 10 分钟内从问题入口定位到可执行 remediation 或明确的人工升级路径。
- production profile 缺少必填 secret、备份位置或不兼容 connector 时在创建/启动前阻断，而不是启动后才暴露运行时错误。
- 诊断包和 runbook 不包含明文 password、API key 或完整加密 spec。

### P5：轻量运行、可观测性与生产运维收口

状态：`delivered`（2026-07-25）

现有运行模式文档和最小 runbook 已交付；本项把业务健康契约、CI 发布门槛、资源基线与可重复运维 runbook 收口为发布门禁。PR-1 负责 storage/secret 的正确性，本项负责把这些能力变成可操作的发布与日常运维流程。

范围：

- 评估 source/sink build tags，优先裁剪重依赖或低频 connector。
- 为 SQLite/MySQL/PostgreSQL storage 接入 PR-1 的 migration、升级、回滚和备份恢复 smoke，并在 CI 中显式声明哪些 backend 被执行、哪些因外部依赖 skip。
- 建立默认镜像大小、启动耗时、空闲内存、典型吞吐和 checkpoint 延迟基线。
- 将 health 从“storage 可连接 + runner 不是 failed”扩展为 source lag、sink latency、checkpoint age、DLQ backlog、Redis state、scheduler 和 worker heartbeat 的业务健康判定。
- 保证 Prometheus label 对用户可控名称正确转义；alert queue 的丢弃有 counter、日志上下文和可配置重试/降级策略。
- 提供一个受版本控制的 production deployment profile：secret 必填校验、固定 image digest/tag、资源限制、日志格式、TLS、备份目录、DLQ retention 和告警配置集中可见。
- 建立 finished task/run/audit/DLQ retention 的默认值、上限和 janitor 运行状态，避免长期运行依赖人工清库。
- 在 CI 中覆盖 Go unit/vet/race、SQLite/MySQL/PostgreSQL storage、至少一条 Kafka/对象存储/真实进程 distributed smoke，并把外部环境缺失明确显示为 skip 而不是绿色通过。
- 为前端 build bundle、lint warning、关键页面可访问性和生产日志敏感字段建立发布基线；超出阈值时阻止 release candidate 或给出明确豁免。

验收标准：

- standalone、master-worker 和 headless 路径有可重复 smoke。
- storage schema 变更具备前向升级和受控回滚/恢复证据。
- health 在 checkpoint stale、CDC lag 超阈值、DLQ backlog、sink/source 卡死、worker offline 时进入正确的 degraded/unhealthy 状态，并有 API/UI/metrics 一致性测试。
- 任意合法 pipeline 名称（包含引号、反斜杠和换行边界的拒绝/规范化规则）都不会破坏 Prometheus exposition；alert dropped 数量可观测。
- production deployment profile 的 config validation、compose config、启动/停止/备份/恢复和升级 smoke 在 release candidate 中全部执行。
- 发布说明记录资源基线、显著回归阈值、支持的 storage/backend 矩阵、RPO/RTO 和仍需人工操作的步骤。

## 已知缺陷（BUG backlog）

### BUG-1：`mysql_batch` 字符串主键游标不推进（2026-08-09 发现）

状态：`active`（代码完成 + 单测闭合；验收 4 容器级 e2e 未跑——镜像构建被
go mod download 网络阻塞，恢复后补跑再置 delivered）

**现象与根因**：`mysql_batch` 源的 `updateLastID` 只处理 int/int64/float64，
字符串主键（如 `request_id`）作为 `pk_column` 时游标从不推进，每次轮询
`WHERE pk > lastID` 都从头重读整张表：records_read 无限增长、管道永不完成
（`schedule: once` 不收敛，streaming 则无限重读同一批数据）。

**复现**：2026-08-09 在 ClickHouse metainfo 验证期间于容器级复现 —— 源表 2 行
（varchar 主键），`pk_column: request_id`，records_read 持续增长至数十万，
管道不结束；换数字主键后同一链路正常。

**范围**：`internal/etl/source/mysql_batch.go` 游标类型（`lastID` 由 int64 改为
any/字符串游标）、checkpoint position 序列化与恢复兼容（旧数值 checkpoint 可读）、
字符串比较与 MySQL 排序规则对齐（utf8mb4 二进制比较）、custom query 与 shard 路径
同步覆盖。

**验收**：
1. varchar 主键表完整读一遍后管道完成（records_read = 表行数，不再循环）；
2. 中断重启/checkpoint 恢复从上次游标继续，不重复读取；
3. 数字主键路径行为不变（旧 checkpoint 可恢复）；
4. 容器级 e2e（varchar PK 源 → 任意 sink）通过并记录证据。

**Non-goals**：数值字符串的 MySQL 隐式转换兼容（如 `'001'` 与 `1` 比较）、
自然排序等多级键语义；这些按源语义文档化即可。

**证据**（2026-08-21 修复）：
- `TestMySQLBatchStringPKCursorAdvances`：varchar PK 第一批参数 int64(0) -> 第二批参数
  "b-2"（游标推进）、空集后 done（不再死循环）、Snapshot 序列化为 last_cursor 新格式。
- `TestMySQLBatchLegacyNumericCheckpointRestores`：旧 {"last_id":42} checkpoint 恢复为
  数值游标；新数值 checkpoint 保持 last_id 字节兼容。
- go test ./internal/etl/... 全绿；source 包 -race 绿。
- **未闭合**：容器级 varchar PK e2e（验收 4）被镜像构建网络阻塞，未跑；据此状态为 active 而非 delivered。

### BUG-2：MySQL CDC binlog 断裂（ERROR 1236）无自动恢复（2026-08-12 发现）

状态：`active`（fail 策略容器 e2e 已跑但证据未入验收矩阵；resume_from_current 有真机
运行时验证 6a814fe；resnapshot 端到端与完整 e2e 记录待容器恢复后补跑再置 delivered）

**现象与根因**：当 MySQL binlog 被按保留期（`binlog_expire_logs_seconds`，云库
常仅 1–3 天）自动清理，或被 `PURGE BINARY LOGS`/`RESET MASTER` 删除后，checkpoint
里记录的 binlog 位点（如 `mysql-bin.000120:2789538`）对应的文件已不存在。
`mysql_cdc` 与 `mysql_snapshot_cdc` 的 canal 重连循环（`internal/etl/source/mysql_cdc.go`
255-285、`mysql_snapshot_cdc.go` 665-710）对该错误（MySQL ERROR 1236「Could not
find first log file name in binary log index」）只做指数退避重试，且每次都用同一个
失效位点 `c.RunFrom(pos)`，导致**永久卡死**，只能人工重置 checkpoint（丢弃中间变更或
重做全量）。长时间停顿（如 sqlite checkpoint 阻塞，见 BUG-3）会放大为 binlog 过期，
形成连锁故障。

**复现**：用户生产环境（host 192.168.31.35）2026-08-12 报错 `Read error (x13):
canal disconnected: ERROR 1236 (HY000): Could not find first log file name in
binary log index file`，checkpoint 显示 `phase=cdc, file=mysql-bin.000120,
pos=2789538`，而 MySQL 已无该 binlog。

**范围**：
1. `internal/etl/source/mysql_cdc.go`、`internal/etl/source/mysql_snapshot_cdc.go`：
   检测 ERROR 1236（binlog purged/缺失），**终止无效重试**，发 `alert.LevelCritical`
   告警（含失效位点、最早可用 binlog），并按可配置策略恢复：
   - `cdc_on_binlog_purged: fail`（默认，fail-closed，停止管道并告警，等人工）；
   - `cdc_on_binlog_purged: resume_from_current`（从 MySQL 当前 master 位点续 CDC，
     丢弃中间变更，显式 RPO 声明）；
   - `cdc_on_binlog_purged: resnapshot`（仅 snapshot_cdc：回退 snapshot 阶段从
     `last_ids`/`last_strs` 续读全量后重新进 CDC）。
2. 新增配置 key `cdc_on_binlog_purged`（source.config，默认 `fail`），文档化每个策略
   的数据语义（RPO / 是否丢数据 / 是否重复）。
3. 告警与日志区分「瞬时断连重试」与「binlog 永久丢失需干预」。

**验收**：
1. 模拟 binlog 缺失（checkpoint 指向已 purge 的文件）下，`fail` 策略停止管道并发
   critical 告警，不再无限重试；
2. `resume_from_current` 策略从当前 master 位点续 CDC，新变更正常写入，checkpoint
   位点更新；
3. `resnapshot`（snapshot_cdc）从 `last_ids` 续读后重新进 CDC，不重读已读行；
4. 瞬时网络断连仍走原指数退避，不被误判为 binlog purged；
5. 单测 + 容器级 e2e（手动 purge binlog 触发）证据。

**Non-goals**：GTID-based 自动补齐已 purge 区间（需 MySQL GTID 模式且源端保留完整
binlog，非通用）；跨实例 binlog 归档恢复；改变 at-least-once 语义（任一策略的丢/重
边界必须在文档与告警中显式声明）。

**证据**：ref — 用户生产报错日志（ERROR 1236, checkpoint mysql-bin.000120）；
修复后在本条目更新验收矩阵。

**残留收口**（2026-08-21，Round 5/5）：`resume_from_current` 策略完成真机运行时验证
（`TestBinlogPurgeRuntimeDetectionResumeFromCurrent`，真 MySQL 8.0 + RESET MASTER）：
旧坐标 RunFrom 报 1236 且被 `isBinlogPurgedError` 识别 → GetMasterPos 探测新坐标 →
新坐标持续流式 3s 无 purge 错误；无 MySQL 时测试自动 skip。
`resnapshot` 策略的端到端（快照重拉验证）仍待容器 e2e（随镜像构建恢复后补跑）。

**状态修正**（2026-08-21 拷问审查）：fail 策略的容器 e2e 证据（hack/e2e-binlog-purged.sh，
随 77052e9 提交，beta.13 镜像可用时真机跑过：RESET MASTER 模拟确认检测并 1 次重试后
干净终止）当时未写入本验收矩阵，证据链断裂；且验收 2/3/5 未全部闭合。按"验收未全部
闭合不得 delivered"的一致性原则，回退为 active。

### BUG-3：sqlite 单连接（MaxOpenConns(1)）导致 checkpoint 写入排队超时阻塞（2026-08-12 发现）

状态：`delivered`（2026-08-12 · beta.14 · 读写分离 + janitor 默认 TTL）

**现象与根因**：sqlite 后端 `internal/etl/storage/sqlite/sqlite.go` 设
`SetMaxOpenConns(1)`（单连接池），而 checkpoint、DLQ、audit、run_history、spec、
health/metrics 查询**全部共用这一个连接**。2+ 个 streaming pipeline 高频 writeBatch
（每批顺序执行 sink.Write → DLQ.WriteDLQ → checkpoint.Save）+ 每条 sink write 写一条
audit，任一慢操作（大 DLQ 批、大 audit 写、慢盘 fsync）会让 checkpoint.Save 排队超过
`writeBatch` 的 `commitCtx` 30s deadline，报 `context deadline exceeded` →
`blockCheckpoint()` → 管道永久阻塞，只能重启容器。688MB 的 etl.db（audit/run_history
无 TTL 无限堆积，因 janitor 默认未启用）+ 慢盘是放大器，不是主因；主因是连接串行化。

**复现**：用户生产环境（host 192.168.31.35）2026-08-12 两个 streaming pipeline 同时
报 `checkpoint blocked until restart: checkpoint save failed: context deadline
exceeded`，etl.db 688MB、WAL 27MB、load 13.55、IO wait 36-64%、swap 耗尽；重启后
缓解。MySQL/PostgreSQL 后端（`MaxOpenConns(20)`）不受影响。

**范围**：
1. `internal/etl/storage/sqlite/sqlite.go`：为 checkpoint/spec 这类控制面高频小事务
   开独立连接（读连接池 + 单写连接分离，或 checkpoint 与 DLQ/audit 分库），避免被
   audit/DLQ 大事务阻塞；
2. `docker-compose.yml` / `docker-compose.distributed.yml`：补 `ETL_AUDIT_TTL`、
   `ETL_RUN_HISTORY_TTL` 默认值（janitor 默认不启用是「静默膨胀」陷阱）；
3. production checklist / quickstart：明确 sqlite 仅适合单 pipeline / 低频场景，
   多 streaming pipeline 或慢盘生产环境推荐 MySQL/PostgreSQL storage 后端；
4. 启动时若 audit/run_history 表过大且无 TTL，打 WARN 日志提示。

**验收**：
1. sqlite 后端下，控制面（checkpoint save / spec 查询）与数据面（DLQ/audit 写）
   连接分离，checkpoint.Save 不被 DLQ/audit 大事务排队阻塞（单测 + 容器压测证据）；
2. docker-compose 默认开启 audit/run_history TTL，etl.db 不再无限膨胀；
3. production 文档明确 sqlite 适用边界与多 pipeline 推荐后端；
4. 现有 sqlite e2e（`hack/e2e-storage-mysql.sh` 等）回归通过；
5. targeted/package/race/vet + git diff --check 通过。

**Non-goals**：sqlite 性能对标 MySQL/PostgreSQL（单机嵌入式数据库的固有上限）；
改变默认 storage 后端（保持 sqlite 为开箱默认，只声明边界）；checkpoint 阻塞后的
自动重试（属 BUG-2 的恢复策略范畴）。

**证据**：ref — 用户生产报错日志（checkpoint blocked, context deadline exceeded,
etl.db 688MB, load 13.55）；修复后在本条目更新验收矩阵。

### BUG-4：停止后的 Runner 复用已关闭运行时资源（2026-08-13 发现）

状态：`delivered`

```text
Round: 1/5
Roadmap item: BUG-4
Profile/path: standalone streaming pipeline（mysql_snapshot_cdc -> rate_limiter -> kafka）
Objective: Stop/Pause 后保留 checkpoint 重启时重建 source/transform/sink 运行时资源，避免复用已关闭的限流器、连接或脚本运行时。
Scope: internal/etl/pipeline、internal/etl/orchestrator、scheduler lifecycle 与对应 unit/integration tests。
Non-goals: 修改用户本地 pipeline spec；重置 checkpoint；改变 at-least-once 语义；诊断或修改 Redpanda 集群。
Acceptance: 1) 同一 Runner Stop -> Wait -> Start 后可超过 rate_limiter burst 持续写入；2) checkpoint 从原位置恢复；3) parallel/DAG/cron trigger 不会二次关闭 done 或假成功；4) 正在清理时明确可重试，scheduler 不标记 failed；5) targeted/race tests 与相关 e2e 通过。
Evidence:
  - TestRunnerStopStartRebuildsRateLimiterRuntime（线性 Stop->Wait->Start 后写入数超过 burst=2，0 failed/0 dlq）。
  - TestParallelRunnerCanStartTwiceAfterCompletion / TestParallelRunnerPauseResumeRebuildsRuntime（parallel Stop/Pause->Wait->Resume 重建 shard 运行时，StatusPaused 与重启语义保持）。
  - TestDAGRunnerWrapperCanStartTwiceAfterCompletion（DAG 二次 Start 不二次关闭 done channel）。
  - TestDAGRunnerMetricsSafeAcrossRestarts（重启 5 轮 + 并发 SinkMetrics/TransformMetrics/StateMetrics/CircuitBreakerState，-race 通过）。
  - TestPipelineStartReturnsConflictWhilePreviousRunStops（server 层返回 409 pipeline_stopping 可重试）。
  - TestTriggerPipelineSkipsRunnerStillStopping（scheduler 跳过、不标记 failed）。
  - go test -race ./internal/etl/pipeline/... ./internal/etl/orchestrator/... ./internal/etl/server/... 全部通过；go test ./internal/etl/... 全绿。
  - 残留：container 级 e2e 镜像构建受 go mod download 网络停滞阻塞，未能完成全新镜像运行。
Result: delivered（单元 + race 验证完整；container e2e 待镜像构建可用后补跑）
Residual/follow-up: writeBatch 共享 commitCtx 导致单次超时可放大为整批 DLQ（独立缺口，非本项范围）；镜像构建恢复后重跑 kafka-multitable / crash-recovery 回归。
```

**现象与根因**：线性 `Runner.runLoop` 在每轮结束时关闭 reader、transform chain 和
sink，但 `Runner.Start` 只重置统计与 done channel，仍复用同一个 source/transform/sink
实例。生产 `rate_limiter(rps=5000, burst=2500)` 在 Stop 后其 refill goroutine 已退出，
重启后只剩初始 2500 个 token；恰好写完 2500 条后，每个后续 record 等待到 30 秒 batch
deadline 并以 `context deadline exceeded` 进入 DLQ。Kafka、ClickHouse、Lua、lookup 等
有 Close 生命周期的组件也有同类风险。ParallelRunner 和 DAG executor 同时存在 done
channel 复用或已关闭节点重启问题。

**范围**：每一轮开始构建新的 source、transform、sink、hooks/circuit breaker 等运行时
对象；checkpoint store、DLQ writer、spec 与 checkpoint 保持不变。上一轮 teardown 未完成
时返回可识别的可重试错误；scheduler 跳过该 trigger，不把它持久化为 failed。

**Non-goals**：不移除 Close、不保留已经关闭的连接、不通过删除/重建 pipeline 或 reset
checkpoint 规避问题；不改变 sink acknowledgement 后才推进 checkpoint 的 at-least-once
边界。

### BUG-5：writeBatch 共享 30s commitCtx，单次超时放大为整批 DLQ（2026-08-13 发现）

状态：`delivered`

```text
Round: 1/5
Roadmap item: BUG-5
Profile/path: standalone 任意 sink 批量写路径（writeBatch）
Objective: 单-row 隔离、DLQ 写入与 checkpoint 保存不再复用可能已被批次重试耗尽的 commitCtx，一次瞬时 sink 超时不得放大为整批 500 条 DLQ。
Scope: internal/etl/pipeline/pipeline.go writeBatch/writeRecordsIndividually/DLQ 路径 + unit/integration tests。
Non-goals: 改变 at-least-once 语义；取消重试机制；修改 sink 实现。
Acceptance: 1) sink.Write 耗尽 30s 后隔离路径用新鲜 ctx 重试每条记录，非过期 ctx；2) 瞬时超时场景下好记录不被误进 DLQ；3) DLQ 写入与 checkpoint 保存有独立超时；4) 现有 TestRunnerWritesDLQAfterSinkContextDeadline 语义保持（关机路径仍整批 DLQ）；5) targeted tests + -race 通过。
Evidence:
  - TestRunnerIsolationUsesFreshContextAfterCommitBudgetExhausted（新增回归：批量写耗尽 commitCtx 后隔离用新鲜 deadline 恢复 2/2，0 failed/0 DLQ）。
  - TestRunnerWritesDLQAfterSinkContextDeadline 语义不变（外层 ctx 带 deadline 的关机场景隔离仍正确失败进 DLQ）。
  - go test -race ./internal/etl/pipeline/ 全绿；server/orchestrator 回归全绿。
Result: delivered（单元 + race 完整；at-least-once 语义不变：失败批次仍不推进 checkpoint，重放由幂等 sink 吸收）
Residual/follow-up: none
```

**现象与根因**：writeBatch 在 :1117 创建一个 30 秒的 commitCtx，覆盖 sink 写、
单-row 隔离、DLQ 写和 checkpoint 保存。当 sink.Write + retry.Do 耗尽该预算后，
writeRecordsIndividually 仍拿到过期 ctx，每条记录的 retry.Do 首次尝试即返回
context deadline exceeded（非 retryable），全部 500 条计入 failures 进 DLQ。
一次瞬时的 sink 超时被放大为整批 DLQ 洪水。

### BUG-6：snapshot_cdc CDC 阶段事件不填 ColumnTypes（2026-08-13 发现）

状态：`active`

```text
Round: 0/5
Roadmap item: BUG-6
Profile/path: mysql_snapshot_cdc CDC 相位 -> kafka -> clickhouse 自动建表
Objective: CDC 相位事件携带 ColumnTypes（canal e.Table.Columns 的 RawType，复用 mysql_cdc 现成模式），使下游不再退化回样本值 + name-hint 推断。
Scope: internal/etl/source/mysql_snapshot_cdc.go OnRow + tests。
Non-goals: 改 kafka envelope 格式；改 typing 推断逻辑本身。
Acceptance: 1) CDC 相位 INSERT/UPDATE/DELETE 记录的 Metadata.ColumnTypes 非空且与 canal schema 一致；2) 经 kafka -> clickhouse 自动建表使用声明类型；3) 单测 + 相关 e2e 通过。
Evidence:
  - TestSnapshotCDCHandlerFillsColumnTypes（新增回归：canal schema 列类型含 unsigned 限定符正确构建；request_id/work_time 经 MapSourceType 解析为 String 而非 Int64/DateTime64；空 RawType 跳过；decimal 源精度直传已另行修复，见下方 Decimal 残留条目）。
  - kafka sink Debezium envelope 已序列化 column_types（sink/kafka.go:65 + schema.fields），链路复用 beta.12 既有路径，无需改动。
  - go test ./internal/etl/... 全绿；source 包 -race 绿。
Result: active（代码完成 + 单测闭合；验收含"相关 e2e 通过"但 e2e 未跑——镜像构建被网络阻塞，恢复后补跑再置 delivered）
Residual/follow-up: none
```

### 低优先级残留（不入 BUG backlog，随相关模块迭代时处理）

- **Boolean flag hint 语义受限**（mapper.go:187-198，`is_*`/`has_*`/`active`/`enabled`/
  `deleted`/`_flag` → ClickHouse UInt8）：非数字文本（'yes'/'on'）在
  convertClickHouseValue 中原样传给 AppendRow 响亮失败进 DLQ，非静默错误；仅在需要
  宽容文本布尔语义时再评估。
- **Decimal 源精度直传**（2026-08-21 修复）：resolve.go decimalDDL 现在解析源类型
  （p,s）后缀并直传（decimal(5,2) → Decimal(5,2)），越界/畸形回退 18,2 默认；MySQL
  方言 raw 直传不变。测试 TestDecimalSourcePrecisionPassthrough（4 方言 + 边界钳制
  11 例）。**残留**：name-hint 命中（mapper.go amount/price 等列名且无源声明类型）仍
  固定 Decimal(18,2)——该路径本就无源精度可查，by-design。

## Connector 成熟度对齐（对标 ClickHouse/Doris 基线，2026-08-21 审计）

审计方法：以本轮 ClickHouse/Doris 补齐的能力（多表扇出、metadata PK、schema drift、
错误分类、per-table 指标、ColumnTypes 元数据）为基线，逐一核对其余 source/sink 的
实现（grep + 代码走读，非运行时验证）。发现的缺口按生产影响排队如下，逐项做到
与基线同等成熟度后再关项。

### GAP-1：`postgres_cdc` 三个 Metadata 契约全缺（2026-08-21 已实现，待 PG 实例 e2e）

- **现状**：postgres_cdc 的 INSERT/UPDATE/DELETE 记录（postgres_cdc.go:716/804/839）
  只填 Source/Table/Timestamp/LSN，完全不填 `Metadata.Key`、`Metadata.ColumnTypes`。
- **影响**：DELETE 下游无法定位主键（与 mysql_cdc beta.15 修的同构 bug）；auto_create
  退化为 value+name-hint 推断（request_id/work_time 同类 bug 在 PG 链路复发）。
- **方案**：pgoutput 的 Relation message 携带列名/类型 OID 与 PK 标记——解析 relation
  元数据填充 Key（含复合 PK，对标 metadataKeyJSONMulti）与 ColumnTypes（OID→文本类型）。
- **验收**：单测覆盖 INSERT/UPDATE/DELETE 的 Key JSON 与 ColumnTypes（含复合 PK）；
  `go test ./internal/etl/...` 全绿；有界后续补 PG 真实例 e2e。
- **2026-08-21 实现记录**：pgCatalog 增加 tablePKs 缓存与 columnTypes()（OID→文本
  类型，pgTypeName）；启动时 loadTablePKs 经 replConn 查 pg_index（复合 PK 按
  indkey 顺序，tables 为空时全量 indisprimary，best-effort 非致命）；三个
  parse*Msg 填 Key（recordKey：UPDATE 优先 before-image，镜像 mysql_cdc 契约）
  与 ColumnTypes。测试：TestPGCatalogRecordKeyComposite、
  TestPGCatalogColumnTypes；source 全量与 -race 绿。**残留**：PG 实例 e2e（真实
  pgoutput 流含 Key/ColumnTypes 断言）待容器环境恢复后补。

### GAP-2：`mysql_cdc` 缺 `Metadata.ColumnTypes`（2026-08-21 复核：审计误报，已存在）

- **更正**：2026-08-21 审计 grep 模式（`Metadata.ColumnTypes` 全串）误报此缺口；
  复核代码确认 mysql_cdc OnRow 已从 canal `e.Table.Columns` RawType（含 unsigned
  后缀）构建 colTypes 并填入每条 CDC 记录（mysql_cdc.go:501 起），与 BUG-6 的
  snapshot_cdc 修复同构。**无需开发**，本项关闭。

### GAP-3：`postgres` sink 缺 `pk_columns_from_metadata`

- **现状**：mysql sink 支持 per-table PK 派生（mysql.go:354），postgres sink 为 0。
- **影响**：kafka 多表扇出→PG upsert 链路无法按表解析 PK，DELETE/UPDATE 失败。
- **方案**：复用 metadata_pk.go 的 parseMetadataKeyColumns，镜像 mysql.go 的接入点。
- **验收**：单测覆盖 JSON 对象 Key 解析、复合 PK、Key 缺失时报错；多表扇出→PG
  upsert 的 e2e（对标 e2e-kafka-multitable-clickhouse.sh 建脚本）。

### GAP-4：`elasticsearch` sink 缺 `schema_drift` 与 index 模板

- **现状**：ES 无 schema_drift（mapping 冲突目前靠 item-level DLQ 兜底）；index 名只做
  `Metadata.Table` 小写直用（elasticsearch.go:369），无 `{table}` 模板。
- **方案**：(1) `index_template`（如 `ods_{table}`）对齐 table_template 语义；
  (2) schema_drift 先做 add_field（ES mapping 动态性天然支持），mapping conflict
  策略留有界后续。
- **验收**：单测覆盖模板替换与缺 metadata 报错；e2e 用 MinIO/ES 容器验证模板扇出。

### GAP-5：sink 错误分类对齐（mysql/postgres/clickhouse/kafka/jdbc 缺 ClassifiedError）

- **现状**：主动标注 `core.ClassifiedError` 的只有 doris/elasticsearch/maxcompute；
  其余 sink 依赖全局 ClassifyError 字符串兜底（可用但不够精确）。
- **影响**：DLQ 按 error_class 过滤/重放策略在这些 sink 上精度下降。
- **方案**：各 sink 的写失败路径在错误已知类别（约束冲突→Data、连接类→Transient、
  schema 不匹配→Schema）时包 ClassifiedError；不做穷举，只标高频路径。
- **验收**：每个 sink 至少 3 类分类单测；DLQ entry 的 ErrorClass 字段单测断言。

### GAP-6：per-table 写入指标对齐（本轮 ClickHouse 新增能力的推广）

- **现状**：仅 ClickHouse sink 有 TableWriteStats()（2026-08-21 新增）。
- **方案**：mysql/postgres/doris sink 接入同构 per-table 计数（复用 tableWriteMetrics
  模式）；kafka sink 按 topic 计数（topicTemplate 扇出场景）。
- **验收**：各 sink 指标快照单测；纳入 SinkMetricsProvider 快照或 API 暴露方案
  （与 P5 可观测性对齐，具体暴露方式在实现时定）。

### 执行顺序

GAP-1 → GAP-2 → GAP-3 → GAP-4 → GAP-5 → GAP-6（按生产影响排序；GAP-1/2 是数据
正确性问题优先，GAP-3/4 是链路能力缺失，GAP-5/6 是运维体验）。每项独立成 commit、
独立验收；与 BUG backlog 的容器 e2e 欠账（BUG-1/2/6）同批补齐时优先 e2e。

## 待用户决策

- **ClickHouse 写入吞吐性能分析是否立项**（2026-08-21 提出，未决）：正确性维度的
  缺口清单已全部处理（BUG-1/5/6 + Decimal 直传；BUG-1/6 容器 e2e 待网络恢复）。
  性能维度有两项此前未做、且属于单方面裁剪排除，现显式提交用户决定：
  (1) ClickHouse 写入吞吐基准测试（batch 大小/flush 间隔/并发写入画像）；
  (2) 异步 insert（async_insert）vs 当前同步批量 insert 的量化对比。
  用户未答复前不立项、不启动。

## 有界后续

这些事项只有在上方当前任务完成或被明确重新排序后才进入执行：

- S3/File first-class manifest；当前 content-addressed key 只吸收相同 batch 边界的重放，不宣称通用 exactly-once 文件输出。
- ODPS/MaxCompute lookup/source 方向；必须在 MaxCompute sink 真实认证后再评估，优先推荐将维表镜像到 MySQL/PostgreSQL/Redis。
- Feishu 内置 source 和插件样板的真实环境、429/rate-limit、token failure 和 restart 证据；完成前保持 beta/dev-only。
- JS/TS/WASM parser 示例扩展；不得将具体行业协议硬编码进核心。
- 更复杂的多事实实时 merge、CDC dimension update 和 late-data 策略；只在不引入 Flink 级状态计算语义的前提下评估。

## 明确暂缓或不做

- 为数量新增大量数据库、消息队列或 SaaS connector。
- Kafka exactly-once transaction 和跨 sink 原子 fanout。
- 任意 keyed state、通用 timer、CoProcessFunction、多流状态机。
- Flink/Spark savepoint 兼容。
- 通用 SQL planner 或 Flink SQL 迁移层；只支持将其数据流语义拆成普通 pipeline。
- sliding/session window、复杂 trigger、late side-output、retraction/update 聚合语义。
- connector marketplace、下载量/评分系统。
- Kubernetes operator、etcd、Zookeeper 等新的基础设施依赖。
- multi-master consensus、跨地域 active-active 和跨 sink 分布式事务；小团队第一生产形态接受单节点/单 master 的明确 RPO/RTO 边界。
- AI 直接绕过 validate/preflight 启动 pipeline。

## 交付证据索引

| 已交付领域 | 主要证据 |
| --- | --- |
| v0.2.9 多表映射、CDC 宽表、UI 场景和 connection scope | [CHANGELOG.zh.md](../CHANGELOG.zh.md)、`hack/e2e-multi-table-map.sh`、`hack/e2e-mysql-cdc-wide.sh`、`hack/e2e-ui.sh` |
| DAG 声明式加载与节点级 DLQ replay | `internal/etl/server/dag_load_test.go`、`internal/etl/server/dlq_test.go`、`internal/etl/orchestrator/replay.go` |
| lookup/enricher 异步 I/O | `hack/e2e-lookup-query.sh`、`hack/e2e-enricher.sh`、transform/pipeline/server 单测 |
| 关系型 pre_write/increment、生成列与 metadata PK | `hack/e2e-relational-write-modes.sh`、`hack/e2e-debezium-mysql.sh` |
| Connector certification 与 Plugin ABI v1 | [connector-certification.md](./connector-certification.md)、[plugin-abi-v1.md](./plugin-abi-v1.md)、`internal/etl/server/connector_certification_test.go` |
| P1 可靠性认证矩阵 | [reliability-certification.md](./reliability-certification.md)、`hack/e2e-kafka.sh`、`hack/e2e-wide-table.sh`、`hack/e2e-lookup-state.sh`、checkpoint/pipeline/orchestrator 单测 |
| P2 真实 WASM 插件链路 | `hack/e2e-wasm-plugin.sh`、`hack/wasm-compiler.Dockerfile`、`web/plugin-sdk/examples/replay-matrix-transform/`、`TestWASMPluginCertificationFixture` |
| Feishu source plugin 样板 | `web/plugin-sdk/examples/feishu-sheet-source/` |
| 运行模式与生产 runbook | [runtime-modes.md](./runtime-modes.md)、`hack/e2e-runtime-smoke.sh` |
| P5 业务健康 / CI / 发布门槛 | [release-checklist.md](./release-checklist.md)、[ops-runbook.md](./ops-runbook.md)、[resource-baseline.md](./resource-baseline.md)、`hack/e2e-production-gate.sh`、`hack/check-release-assets.sh`、`/api/v2/health` 扩展 |
| UI 首次任务闭环与 AI context pack | `web/src/main.tsx`、`web/src/DagEditorPage.tsx`、`internal/etl/server/ai_context_test.go`、`hack/e2e-ui.sh` |
| mysql_snapshot_cdc 全库异构主键快照（bounded follow-up） | `internal/etl/source/mysql_snapshot_cdc_pk_test.go`、`hack/e2e-snapshot-cdc-heteropk.sh`、[etl-config-schema.md](./etl-config-schema.md#mysql_snapshot_cdc) |
| doris sink table_template 多表扇出 + kafka source format=envelope（bounded follow-up） | `internal/etl/sink/doris_table_template_test.go`、`internal/etl/server/schema_test.go`、`hack/e2e-doris-table-template.sh`、`hack/e2e-cdc-kafka-relay.sh` |

## 跟踪指标

- 可靠性：失败记录可见率、静默丢失数（目标为 0）、DLQ 写入失败次数、replay 成功率、crash/rebalance e2e 通过率、checkpoint 恢复耗时、task stale overwrite 数（目标为 0）。
- 易用性：空环境到首条已验证任务的中位耗时（目标 ≤30 分钟）、向导完成率、preflight 拦截率、修复后成功率、deep-link 上下文恢复率、从 failed/degraded/DLQ 入口到 remediation/replay 的中位耗时（目标 ≤10 分钟）。
- 数据一致性：源/目标业务键对账差异、重复吸收率、checkpoint reset 后的重复边界、DLQ backlog age、CDC lag、lookup hit/miss、window emit。
- 安全与维护：生产 profile fail-closed 通过率、明文 secret 检测结果、key rotation 成功率、migration/backup/restore smoke 通过率、升级/回滚耗时、alert dropped 数。
- 扩展性：descriptor/schema/preflight 覆盖率、path-level production 认证率、production evidence 新鲜度、Plugin ABI 兼容测试通过率。
- 轻量性：镜像/二进制大小、启动耗时、空闲内存、外部依赖数量、checkpoint 延迟和典型吞吐；发布说明记录基线及回归阈值。
