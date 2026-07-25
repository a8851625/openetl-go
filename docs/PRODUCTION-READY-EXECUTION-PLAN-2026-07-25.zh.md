# OpenETL-Go Production Ready 开发执行方案

> 日期：2026-07-25  
> 用途：给后续开发 agent 的交接与排期，不直接改变 `docs/ROADMAP.zh.md` 中的状态或优先级。  
> 路线图来源：[docs/ROADMAP.zh.md](./ROADMAP.zh.md)  
> 当前工作树基线：`cfbd496`（`v0.2.11-beta.3` 后续，工作树有未提交改动）

## 结论先行

完成 `PR-1`、`P4` 和 `PR-D1` 仍不足以对外笼统宣称“OpenETL-Go production ready”。在此之前还必须完成或明确处置：

- `PR-2`：主推荐数据链路的 crash/reset/outage/DLQ replay 对账与 at-least-once 契约；
- `P3`：connector/path maturity 与当前版本实际认证证据的一致性；
- `P5`：业务健康、资源基线、CI、发布 profile、升级/备份/恢复和生产 runbook；
- 至少两条主推荐链路的当前版本真实故障认证；
- SQLite、MySQL、PostgreSQL 公开 storage backend 的升级、备份、恢复证据；
- MaxCompute 等没有真实外部环境证据的 connector 必须继续降级为 `experimental` 或 `production_with_review`。

最终声明必须限定 profile、path、write mode、storage backend 和已知边界，不能把单个 connector 或单条 e2e 的结果外推为所有组合都生产就绪。

## 1. 当前状态（2026-07-25）

| 项目 | 当前状态 | 说明 |
| --- | --- | --- |
| `PR-0` | `delivered` | 控制面持久化一致性、spec 加密恢复、production profile、TLS 和 scheduler compensation 已完成 |
| `P0 MaxCompute` | `blocked_external` | 缺真实 endpoint/project/table/AccessKey 和受控失败注入环境；不能用本地 mock 代替认证 |
| `PR-1` | `queued` | Secret envelope、migration、backup/restore、retention |
| `PR-2` | `delivered` | path contract、主链路故障认证、边界语义 |
| `P3` | `queued` | maturity/readiness/certification 事实源 |
| `P4` | `queued` | 首次任务向导、状态语义、deep link、故障自助；当前 UI e2e 为 91 passed/17 failed |
| `P5` | `queued` | 业务健康、运维、CI、资源基线、发布门槛 |
| `PR-D1` | `delivered` | distributed worker 认证、fencing、真实三进程恢复 |

P0 仍是路线图最高优先级。若凭据不可得而要开发其他项目，必须由产品/用户显式确认 priority switch；本文件不自动领取下一项。

## 2. Production Ready 的分层定义

### 2.1 Standalone production ready

至少需要：

1. `PR-0`、`PR-1`、`PR-2` 交付；
2. P4/P5 中与首次任务、健康度、升级恢复相关的验收完成；
3. MySQL CDC -> MySQL upsert 和 MySQL snapshot+CDC/CDC -> ClickHouse 幂等版本表完成真实故障认证；
4. 声明支持的 SQLite/MySQL/PostgreSQL backend 均有 migration/backup/restore 证据；
5. 发布资产无空 token、`change-me` 密码或浮动 `latest`；
6. 发布说明列出 at-least-once、可能重复、RPO/RTO、单点、非原子 fanout 和未认证 connector。

`PR-D1` 不阻塞 standalone 达到这一层，但 distributed 能力必须单独标注 beta/production candidate。

### 2.2 Distributed production ready/candidate

除了 standalone 门槛，还需要完整 `PR-D1`：worker token/TLS、lease/generation/CAS fencing、attempt/requeue、master/worker restart 和真实多进程 e2e。完成前不得宣称 distributed production ready，并继续公开以下边界：单 master、DAG master-local、单 continuous shard、非 multi-active HA。

### 2.3 Connector/path production ready

声明格式必须类似：

> `standalone + MySQL CDC -> MySQL upsert + PostgreSQL metadata storage + business-key/version dedup`，在版本 `<commit/image>` 上通过指定 crash/reset/outage/DLQ replay 认证。

不能写成“mysql sink 已 production，所以所有 source、append、DAG、storage 组合都 production”。

## 3. 依赖和交付顺序

```text
PR-0 delivered
    |
    v
PR-1 (secret / migration / backup-restore)
    |
    v
PR-2 (path contract / data-consistency certification)
    |
    +--> P3 (maturity and evidence source)
    |
    +--> P4 (first-task UX; may prepare after PR-0, claim only when priority allows)
    |
    v
P5 (health / CI / release operations) --> standalone release gate

PR-D1 (distributed gate; after standalone close, unless explicitly advanced)

P0 MaxCompute (external credential gate; never replace with a lower-fidelity mock)
```

建议在共享工作树上一次只保留一个 `active` 主任务。多个 agent 可以在同一个主任务内承担互不重叠的 bounded increment；后续项目只能做只读预检或在独立分支准备，不能静默改变路线图状态。

## 4. 开发任务包

下面的任务包是对路线图现有项目的执行拆分，不是新增产品范围。每个包完成时都要更新对应路线图证据、文档和 acceptance matrix。

### PR-1：Secret 管理、Storage 演进与可恢复升级

依赖：`PR-0`。建议作为 standalone 下一条主开发线。

#### PR-1.1：Secret envelope

**可观察结果**：connection catalog、settings/LLM 等持久化 secret 在 SQL dump、直接查询和备份文件中均不可读，且重启与 key rotation 后仍可用。

**允许触及的范围**：connection/settings storage、secret encode/decode adapter、API mask/restore 逻辑、key 配置与 rotation 工具、相关文档和测试。

**非目标**：重新实现已经交付的 pipeline spec encryption；不引入 RBAC；不把 token 只做 UI mask 就视为完成。

**验收**：

1. 固定测试 secret 写入 SQLite/MySQL/PostgreSQL 后，dump 和直接查询没有明文；
2. API 读取只返回 mask，更新 masked payload 不会覆盖真实 secret；
3. key ID/version、旧 key 读取、新 key re-encrypt、错误 key 和损坏密文均有明确结果；
4. 重启、备份恢复后 connection/settings 仍可被 pipeline 使用；
5. 失败恢复不会把 secret 写入日志、audit、task row、DLQ 或诊断包。

**证据**：storage/server 单测、三 backend conformance、secret dump scanner、rotation/restart/restore smoke、`docs/runtime-modes.md` 更新。

#### PR-1.2：Storage migration 与并发 version

**可观察结果**：两个以上进程并发启动或更新同一 pipeline 时，不会重复迁移、产生重复 version 或形成 current/version 半成功状态。

**验收**：

1. SQLite/MySQL/PostgreSQL migration 有显式 schema version、migration lock、失败终止语义；
2. 并发 migration 只有一个执行 DDL，其他进程等待或安全确认版本；
3. migration error 阻止服务以旧/半迁移 schema 继续运行；
4. 并发 pipeline update 不产生重复 version，不丢 version，冲突返回明确 409/版本前置条件；
5. current/version/checkpoint 的提交边界与 `PR-0` 保持一致。

**证据**：migration fault-injection、双进程/多进程测试、storage conformance、三 backend 真实环境结果、升级日志。

#### PR-1.3：Upgrade、backup/restore、retention/janitor

**可观察结果**：维护者可以按 runbook 完成升级、备份、恢复、回滚和过期数据清理，不需要手工猜 SQL。

**验收**：

1. 上一稳定 release -> 当前版本的前向升级在 SQLite/MySQL/PostgreSQL 分别有结果；
2. 失败升级可恢复或安全阻止启动，回滚镜像后数据可读；
3. 备份覆盖 pipelines、versions、checkpoints、DLQ、audit、runs、workers/tasks、plugins、connections、settings；
4. 恢复后对象计数、关键 checkpoint、DLQ 和 audit 对账；
5. task/run/audit/DLQ retention 有配置、上限、janitor 状态和失败告警。

**证据**：目标脚本 `hack/e2e-storage-upgrade-*.sh`、`hack/e2e-backup-restore-*.sh`、runbook、release checklist；外部 backend 缺失必须记为 `skipped/blocked`。

### PR-2：数据一致性契约与生产链路认证

依赖：`PR-1` 的 storage gate。不能用单元测试代替真实 path 认证。

#### PR-2.1：Path contract 与自动对账

**结果**：每条公开 production path 都能从 descriptor/readiness 追溯到 source、transform、sink、write mode、business key/version、storage/runtime mode、最后通过版本和已知重复边界。

**验收**：

- 建立认证矩阵和固定 fixture；
- 定义源端/目标端业务键、版本和最终状态对账格式；
- 明确 sink ack、checkpoint commit、crash、reset/replay 的边界；
- `docs/reliability-certification.md` 和 `docs/etl-idempotency.md` 能反向定位测试证据。

#### PR-2.2：两条主推荐链路故障认证

强制路径：

1. MySQL CDC -> MySQL upsert；
2. MySQL snapshot+CDC/CDC -> ClickHouse ReplacingMergeTree-style/版本幂等表。

每条路径必须覆盖正常写入、sink ack 后 crash、checkpoint storage failure、应用 restart、checkpoint reset/replay、sink outage、DLQ repair/replay、schema drift，并以业务键/版本自动对账证明静默丢失为 0。

#### PR-2.3：边界语义收口

- PostgreSQL CDC TRUNCATE：实现目标语义或 preflight 阻断；
- Kafka/File/S3 append：在 first-class manifest/transaction 未完成前保持 `production_with_review` 或更低，并记录下游去重方式；
- 跨 sink fanout 和其他危险组合：默认阻断或要求显式 `allow_unsafe`，并在 API/UI/audit 留风险记录；
- 任何 warning 不能掩盖仍然不适合 production 的路径。

### P3：成熟度事实源与认证覆盖扩展

**结果**：maturity 不再靠人工修改字符串提升，过期或缺证据的 connector 自动降级。

**任务**：

- 将 certification kit 覆盖所有公开标记为 production 的内置 connector，优先 HTTP、PostgreSQL sink、Doris；
- 对 descriptor/schema required、secret/scope、注册、preflight/readiness、组件文档做一致性测试；
- 将 connector maturity 与具体 path certification 分离；
- certification 记录 commit/image、依赖版本、外部拓扑、执行时间、失败注入和残余风险；
- 未执行/证据过期的 connector 自动变为 `production_with_review`、`beta` 或 `experimental`。

**验收**：未知 production connector 被测试自动发现并拒绝；每条公开 production path 能追溯到完整 e2e 和最后通过版本。

### P4：首次任务体验残留收口

当前基线：`hack/e2e-ui.sh` 为 91 passed/17 failed；这些失败集中在 schema-driven wizard、transform/saved-connection 和 DLQ filter，不能通过修改测试选择器伪造通过。

#### P4.1：状态语义与交互可信度

- 统一 `healthy/degraded/failed/paused/scheduled/completed` 派生规则；
- 分离失败 pipeline、失败记录、DLQ backlog、历史 replay 计数；
- 统一批量启动/停止、checkpoint reset、connection delete、worker deregister、DLQ replay/delete 的确认与结果；
- 补齐键盘、ARIA、非颜色状态、中文文案和 token 遮挡；AI 入口不能绕过 validate/preflight。

#### P4.2：首次任务分步闭环

- 场景 -> Source -> Sink/写入语义 -> Transform -> 安全检查 -> 确认启动；
- 由真实 descriptor/introspection/preflight 驱动，不建立静态 UI 执行语义；
- Source 展示 health/schema/sample，Sink 展示 DDL、主键、insert/upsert/pre_write 和 replay 边界；
- Transform 支持逐阶段 dry-run；preflight 错误定位到字段并提供 remediation；
- 移除 `Failure demo`、`Repair to file_sink` 等 e2e 专用控件。

#### P4.3：任务型信息架构与 deep link

- 总览、管道、运维、资源、系统分组；
- `/pipelines`、`/pipelines/:id`、runs、DLQ、connections 等 URL 可刷新、返回、分享；
- 总览优先呈现 failed/degraded、DLQ、CDC lag、stale checkpoint、异常 connection、offline worker；
- DLQ 形成定位 -> 修复 -> replay -> 对账闭环，并明确 at-least-once/可能重复。

#### P4.4：小团队首次上线与故障自助

- demo 与 production profile 分开；
- 首次任务模板只展示 PR-2 已认证组合；
- connection/preflight/checkpoint/sink outage/DLQ/worker offline 提供下一步 remediation；
- 诊断包不含 password、API key 或完整加密 spec；
- 真实走查达到空环境首条已验证链路 <=30 分钟、故障定位 <=10 分钟。

**P4 总验收**：UI/YAML/API 往返不丢隐藏字段；状态口径一致；稳定 URL 恢复上下文；关键 Playwright 路径通过；高影响操作有确认和影响说明。

### P5：轻量运行、可观测性与生产运维收口

建议拆成三个 bounded increment（不改变路线图项目名）：

#### P5.1：业务健康与可观测性

- health 同时反映 source lag、sink latency、checkpoint age、DLQ backlog、Redis state、scheduler、worker heartbeat；
- checkpoint stale、CDC lag、DLQ backlog、source/sink 卡死、worker offline 时 API/UI/metrics 一致地进入 degraded/unhealthy；
- Prometheus label 正确转义任意合法 pipeline 名称；alert dropped 有 counter、日志上下文和可配置降级。

#### P5.2：生产 deployment profile 与 CI gate

- 固定 image tag/digest、secret 校验、TLS、资源限制、日志格式、备份目录、DLQ retention、告警配置；
- CI 显式覆盖 Go unit/vet/race、SQLite/MySQL/PostgreSQL、至少一条 Kafka/对象存储/真实 distributed smoke；
- 外部环境缺失只能标 `skip/blocked`，不能变成绿色通过；
- frontend build、可访问性、敏感日志和 bundle warning 有阈值和豁免记录。

#### P5.3：资源基线、升级演练与 runbook

- 记录镜像/二进制大小、启动耗时、空闲内存、典型吞吐、checkpoint 延迟；
- 将 PR-1 的升级/备份/恢复接入 standalone、master-worker、headless smoke；
- release checklist 记录支持 backend 矩阵、RPO/RTO、回滚步骤、仍需人工操作的步骤；
- retention/janitor 运行状态可观测。

### PR-D1：Distributed 安全与任务所有权

依赖：standalone 主路径收口；若要提前开发，必须显式把 distributed 提前，并保留独立 maturity 声明。

#### PR-D1.1：Worker transport

- register/heartbeat/poll/report/deregister 使用统一有超时 HTTP client；
- scoped token、可验证 HTTPS、超时/重试/backoff；
- 缺 token、错误 token、过期凭据、证书错误均拒绝且可诊断；
- 生产不依赖裸 `http://master:8001`。

#### PR-D1.2：Task ownership/fencing

- lease/generation/attempt 或等价 fencing token；
- worker ID + generation/CAS 更新所有 task 状态；
- 旧 worker lease 失效后提交完成不能覆盖新 owner；
- worker crash、heartbeat stale、执行失败支持有界 requeue/backoff；
- 保留 worker offline history 和 task attempt history。

#### PR-D1.3：真实拓扑认证

- 独立 master + 至少 2 workers，三个以上独立进程/容器；
- 使用真实 `worker.ExecuteShard`，不是 recording stub；
- 覆盖 encrypted spec、共享 storage、Redis state、checkpoint resume、worker SIGKILL、master restart、pending/assigned/running 恢复；
- 任务超过 attempts 上限进入可见终态并告警。

**非目标**：multi-master consensus、跨 worker DAG、single-shard multi-active、跨地域 active-active、跨 sink 分布式事务。

### P0：MaxCompute 外部认证门

这一项不是继续扩展 writer 的开发包，而是凭据到位后的认证包。解除条件：

- `MAXCOMPUTE_ENDPOINT`
- `MAXCOMPUTE_PROJECT`
- `MAXCOMPUTE_TABLE`
- `MAXCOMPUTE_ACCESS_KEY_ID`
- `MAXCOMPUTE_ACCESS_KEY_SECRET`
- 可选 tunnel/quota/受控失败注入权限

凭据到位后执行 Kafka ODS JSON -> `project`/`type_convert` -> MaxCompute 分区表，覆盖动态/静态分区、权限失败、远端 schema/partition preflight、DLQ/replay、restart、checkpoint reset/replay，并更新 [sink-maxcompute.md](./components/sink-maxcompute.md)、readiness 和 certification evidence。凭据不到位时只能 `blocked_external`，不能让 agent 在本地 mock 上宣称通过。

## 5. Agent 分工和协作规则

### 推荐角色

| 角色 | 首选任务 | 交付物 |
| --- | --- | --- |
| Storage/Security agent | PR-1.1、PR-1.2 | secret envelope、migration lock、并发 version 测试 |
| Recovery/Backend agent | PR-1.3、P5.3 | 三 backend upgrade/backup/restore、retention/runbook |
| Reliability/Certification agent | PR-2.1、PR-2.2、PR-2.3 | path matrix、对账 fixture、主链路 e2e、边界声明 |
| Evidence/Maturity agent | P3 | readiness 一致性测试、证据索引、maturity 降级规则 |
| UI/Product agent | P4.1-P4.4 | 状态口径、向导、deep link、故障自助和 Playwright |
| Distributed agent | PR-D1.1-D1.3 | authenticated transport、fencing、真实多进程 e2e |
| Ops/Release agent | P5.1-P5.3 | health/metrics、CI、profile、资源基线、release checklist |
| External certification owner | P0 | MaxCompute 凭据、外部环境、认证记录 |

### 必须遵守的协作约束

1. 共享工作树同一时间只保留一个 `active` 主路线图项目；agent 可以并行做该项目内互不重叠的 increments。
2. 后续项目在未领取前只能做只读审计、fixture 设计或独立分支预检；不能把 `queued` 改成 `active`。
3. 每个 increment 先写 claim：目标、范围、非目标、依赖、验收、证据和 rollback；完成后写 acceptance matrix。
4. 不把脚本存在、单元测试通过或 maturity 字符串修改当作认证通过；缺服务/凭据必须记为 `skip/blocked`。
5. 所有 agent 保留现有用户改动，禁止 reset/checkout/clean；编辑文件使用 `apply_patch`。
6. 代码、API response、DB rows、checkpoint/DLQ、metrics、docs 和 maturity metadata 必须在交付前互相一致。

## 6. 统一验收和证据模板

每个 agent 交付时附上：

```text
Round: <n>/5
Roadmap item: <PR-1.1 / PR-2.2 / ...>
Profile/path: <standalone|distributed|specific connector path>
Objective: <一个可观察结果>
Scope: <允许修改的文件/组件>
Non-goals: <明确排除>
Acceptance: <编号列表>
Evidence: <精确命令、脚本、文档、commit/image>
Result: <delivered|active|blocked_external>
Residual/follow-up: <有界后续>
```

验收矩阵至少使用以下列：

| Criterion | Evidence（精确命令/文件/运行） | Result | Residual 或 blocker |
| --- | --- | --- | --- |
| stated acceptance criterion | command、e2e、dump、对账、截图或文档 | passed/failed/skipped/blocked | 具体下一步 |

最低收口检查：

- targeted unit/static/format；
- 相关 package integration/conformance，涉及并发时加 `-race`；
- 每个公开 storage/connector backend 的对应测试；
- crash/restart、checkpoint reset、outage、DLQ replay、duplicate absorption；
- `git diff --check`，敏感字段扫描，确认无 token/password/generated debug artifact 泄露；
- `go test ./... -count=1` 或按 AGENTS.md 使用指定容器；
- UI 变更执行 `npm run typecheck && npm run build` 和相关 Playwright；
- 外部环境不足时记录 `skipped/blocked` 及解除条件。

## 7. 对外发布前的最终决策门

### 可以宣称 standalone production ready 的条件

- PR-0/1/2 已 `delivered`；
- 相关 P4/P5 验收通过；
- 两条主推荐链路有当前版本真实故障认证和自动对账；
- 公开 storage backend 有 upgrade/backup/restore 证据；
- P3 能从 maturity 反查具体 path evidence；
- 发布 profile、镜像、TLS、secret、runbook、RPO/RTO 和已知 residual 全部记录；
- 未认证 connector（包括 MaxCompute，如 P0 未完成）明确降级并从生产模板中排除。

### 可以宣称 distributed production ready 的额外条件

- standalone 门槛全部满足；
- PR-D1.1/.2/.3 全部通过真实 token/TLS、fencing、crash/requeue/master restart e2e；
- 发布说明明确不是 multi-master、multi-active HA 或跨 sink exactly-once。

### 推荐发布措辞

在全部门槛完成前，使用：

> `OpenETL-Go standalone production candidate for <列出的认证路径>，默认 at-least-once；其他 connector/path 仍为 beta/experimental/production_with_review。`

完成对应门槛后，使用：

> `OpenETL-Go standalone production ready for <精确 source -> transform -> sink、write mode、storage/runtime mode>，证据版本为 <commit/image>；不覆盖未列出的 connector/path。`

## 8. 下一步交接

当前不自动领取任务。若用户明确切换优先级，首个建议 claim 是：

```text
Roadmap item: PR-1.1
Profile/path: standalone control plane
Objective: connection/settings 持久化 secret 字段级加密、rotation 和 restart/restore 可验证
Dependency: PR-0 delivered
External blocker: none for SQLite; MySQL/PostgreSQL 认证需对应服务/DSN
Non-goals: PR-1.2 migration lock/concurrent version；PR-2 path certification；PR-D1 transport
```

如果 MaxCompute 凭据先到，则保留 PR-1 queued，优先完成 P0 的真实认证；如果要提前做 distributed，必须显式记录 priority switch，并将 standalone 与 distributed evidence 分开。

