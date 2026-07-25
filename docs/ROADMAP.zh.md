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
| `PR-1` | 易维护、安全 | secret、migration、backup/restore、upgrade/rollback 可重复 | `PR-0` | `active` |
| `PR-2` | 数据一致性 | 主推荐链路通过 crash/reset/outage/DLQ replay 对账 | `PR-0`，并复用 `PR-1` storage gate | `queued` |
| P3 | 证据治理 | maturity 与当前版本实际认证证据一致 | `PR-2` 定义 path gate | `queued` |
| P4 | 易上手 | 30 分钟首次任务与 10 分钟故障定位目标可验证 | `PR-0` 安全/profile 约定 | `queued` |
| P5 | 易维护、可观测 | 业务健康、资源基线、CI 和 production runbook 成为发布门槛 | `PR-1`、`PR-2` | `queued` |
| `PR-D1` | distributed 可靠性 | worker 认证、fencing、重试和真实多进程恢复通过 | standalone 收口后，或显式提前 | `queued` |

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
| current/version 原子提交 | `internal/etl/storage/sqlstore/store.go` `SavePipelineWithVersion*`；`TestPipelineSpecStoreAtomic*`；`TestSQLiteConformance/PipelineAtomicLifecycle` | passed | 并发 version 分配仍属于 PR-1.2 |
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
| 可重复安全/profile smoke | `hack/e2e-production-profile.sh`（profile + compose + HTTP security gate）；`hack/e2e-ui-token.sh`（当前构建镜像上的 focused browser token gate） | passed | 完整 `hack/e2e-ui.sh` 当前为 91 passed/17 failed，失败集中在 P4 wizard/filter 选择器与流程 residual，不把它计作 PR-0 token gate |

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

因此 `PR-0.1`、`PR-0.2` 主事务切片、`PR-0.3` 安全/TLS 切片、`PR-0.2a` scheduler compensation 和本轮最终验收均已交付；`PR-0` 现标记为 `delivered`。这不提升项目级或 distributed maturity：P0 MaxCompute 仍为 `blocked_external`，P4 UI residual、PR-1 storage/secret 演进和 PR-D1 distributed transport 保持原顺序。

最终交接：

```text
Round: 1/5
Roadmap item: PR-0
Profile/path: standalone control plane
Result: delivered
Residual/follow-up: 不自动领取 PR-1；下一次显式 continue 重新同步后，按 P0 blocked_external 与既定顺序决定是否领取 PR-1.1。P4 UI e2e 17 个失败项继续留在 P4，不借用 PR-0 证据。
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

状态：`active`

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
Result: active
```

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
2. `PR-1.2` Storage migration：显式错误、migration lock、并发 pipeline version 分配。`active`
3. `PR-1.3` 恢复与保留：三个 backend 的 upgrade/backup/restore smoke 和 retention/janitor。

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

状态：`queued`

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

### PR-D1：Distributed 安全与任务所有权

状态：`queued`

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

## 排队中的工作

以下是既有排队工作，其原有相对顺序和验收范围保持不变。P1、P2 已于 2026-07-13 达到验收标准并迁入 CHANGELOG/证据索引；P0 未解除阻塞时，是否切换到 `PR-0`、P3 或其他任务仍需显式确认。

### P3：成熟度事实源与认证覆盖扩展

状态：`queued`

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

### P4：首次任务体验残留收口

状态：`queued`（2026-07-21 原型对齐批次已落地主路径；剩余 residual 见 `docs/UI-REDESIGN-TODO.zh.md`）

现有向导和上下文闭环已经交付，但 2026-07-20 产品走查确认当前 Web UI 仍更接近“能力完整的工程控制台”，尚未完全收敛为围绕“创建成功、稳定运行、快速修复”的任务型产品。主要证据包括：一级导航平铺构建、运维和系统对象；首次任务向导在单个长弹窗中同时暴露模板、连接、descriptor、运行参数、transform、样例、YAML 和 preflight；运行状态、健康度和累计指标存在口径混用；页面状态没有可分享 URL；部分危险操作、国际化和无障碍表达不一致。

**2026-07-21 已交付（证据）**：全宽管道列表 + URL 筛选；`#/pipelines/new` 全页三段式向导 + 草稿；DLQ 聚合主视图 + Replay 确认面板；详情写入语义/生命周期；总览时间范围切换；Connections 抽屉；问题中心固定排序；顶栏用户菜单与扩展分组；e2e/文档区分「路由可达」与「原型对齐」。Residual：DAG 空画布模板、小屏信息行、截图刷新、多 run 历史。

**2026-07-25 现状核对**：`hack/e2e-ui.sh` 在当前构建镜像上为 91 passed/17 failed；失败集中在首次任务向导的 schema-driven form、transform/saved-connection 交互和 DLQ filter 选择器，属于本节 P4 residual。PR-0 的独立 token 安全门槛由 `hack/e2e-ui-token.sh` 单独验证并通过，不将 P4 失败误报为 PR-0 通过或失败。

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

状态：`queued`

现有运行模式文档和最小 runbook 已交付；剩余工作聚焦小团队可维护性、业务健康契约、自动化恢复和资源基线。PR-1 负责 storage/secret 的正确性，本项负责把这些能力变成可操作的发布与日常运维流程。

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
| UI 首次任务闭环与 AI context pack | `web/src/main.tsx`、`web/src/DagEditorPage.tsx`、`internal/etl/server/ai_context_test.go`、`hack/e2e-ui.sh` |

## 跟踪指标

- 可靠性：失败记录可见率、静默丢失数（目标为 0）、DLQ 写入失败次数、replay 成功率、crash/rebalance e2e 通过率、checkpoint 恢复耗时、task stale overwrite 数（目标为 0）。
- 易用性：空环境到首条已验证任务的中位耗时（目标 ≤30 分钟）、向导完成率、preflight 拦截率、修复后成功率、deep-link 上下文恢复率、从 failed/degraded/DLQ 入口到 remediation/replay 的中位耗时（目标 ≤10 分钟）。
- 数据一致性：源/目标业务键对账差异、重复吸收率、checkpoint reset 后的重复边界、DLQ backlog age、CDC lag、lookup hit/miss、window emit。
- 安全与维护：生产 profile fail-closed 通过率、明文 secret 检测结果、key rotation 成功率、migration/backup/restore smoke 通过率、升级/回滚耗时、alert dropped 数。
- 扩展性：descriptor/schema/preflight 覆盖率、path-level production 认证率、production evidence 新鲜度、Plugin ABI 兼容测试通过率。
- 轻量性：镜像/二进制大小、启动耗时、空闲内存、外部依赖数量、checkpoint 延迟和典型吞吐；发布说明记录基线及回归阈值。
