# UI-E.3 服务端事实契约 preflight（只读）

> 后续状态（2026-11-26）：用户已授权先完成技术修复。连接引用契约由 PR-0-C1
> 交付；恢复事实及深链由 UI-E-R1 交付，见
> [技术验收记录](../P1-technical-closure-20261126/r1-evidence.md)。
> 下文保留 2026-11-09 的只读基线，不代表当前实现。外部用户研究仍缺参与者。

> 只读预检，2026-11-09 执行，基线 commit `e28a740`。范围仅限阅读 `internal/etl/**`、`web/src/**`、`docs/etl-api.md`、`docs/openapi.yaml`；未修改任何 Go/TS/API/存储代码，未运行测试与构建。本文**不替代** UI-E.1 的用户研究（`research-protocol.md`），只回答"服务端现在能否支撑一个可靠的 sink outage recovery task"。结论是**不足**，最小提案在第 5 节，**未实现，待用户批准**。

## 事实清单

判定口径：
- **重启后仍稳定** = 落库或由 UUID 主键推导，进程重启后值不变。
- **请求时计算** = 由内存 runner 计数或现场 SQL 聚合得到，重启/重放后可变或归零。
- **不存在** = 无该字段/端点，附返回空的 grep。

| 事实 | 当前 API/存储来源（file:line） | 稳定性 | 备注 |
| --- | --- | --- | --- |
| pipeline 稳定 ID | `internal/etl/storage/storage.go:133-138`（`uuid.NewString()`）、`internal/etl/server/server.go:97-101` | 重启后仍稳定 | UUID，落 `pipelines.id`（`sqlstore/store.go:214-225`）。是当前唯一可作为 URL 身份的稳定 ID |
| pipeline 显示名 | `internal/etl/server/server.go:103-111`、`server.go:239-248` | 重启后仍稳定 | **非唯一**：`resolvePipelineRefLocked` 在重名时返回 `matches multiple instances; use pipeline id`（`server.go:248`）。不可作唯一键 |
| pipeline 期望/观测状态 | `sqlstore/store.go:219-220`、`server.go:1905-1909` | 重启后仍稳定 | `desired_state` / `observed_state` / `generation`；`server.go:71-95` 写入终态 |
| pipeline 当前 status | `server.go:1892`（`runner.Status()`） | 请求时计算 | 内存 runner 状态；重启后由 lifecycle 恢复流程重建，不来自该字段 |
| pipeline `generation`（生命周期代） | `sqlstore/store.go:221`、`server.go:1908` | 重启后仍稳定 | 每次 start 递增（`lifecycle.go:47-51`），用于 checkpoint fence，**不是 issue ID** |
| `stats.last_error` | `pipeline/pipeline.go:86`、写入点 `pipeline.go:526/546/1366/1980/2050` | 请求时计算 | 内存字符串，**永不自动清空**（全仓无 `setLastError("")`，`grep -rn 'setLastError("")' internal/` 返回空）；无发生时间、无 ID |
| `stats.last_error_code` | `pipeline/pipeline.go:87`、`pipeline.go:534-548` | 请求时计算 | 仅少数路径赋值（`checkpoint_invalid`、`checkpoint_generation_fenced`、`startup_failed`）；大部分 sink 写失败路径（如 `pipeline.go:1366`）**只写 last_error，不写 code** |
| `stats.last_error_remediation` | `pipeline/pipeline.go:88-90`、`pipeline.go:548` | 请求时计算 | 同上，稀疏 |
| 最近一次 checkpoint 时间（内存） | `pipeline/pipeline.go:91`、`pipeline.go:2016`（`LastCheckpoint = time.Now()`） | 请求时计算 | **进程内时间**，重启后为 zero；不是持久化的"最近成功写入时间" |
| 持久化 checkpoint 时间 | `sqlstore/store.go:235-242`（`checkpoints.timestamp`）、`sqlstore/store.go:1079-1104` | 重启后仍稳定 | 真实落库时间；`server.go:5192-5194` 用它算 `checkpoint_age_seconds`。但语义是"最近一次 checkpoint 提交"，**不等于 sink 写入成功时刻** |
| checkpoint position | `sqlstore/store.go:238`、`storage.go:19-24` | 重启后仍稳定 | 各 source 自有 JSON；无统一"消费位点语义"字段 |
| `dlq_backlog` / `dlq_backlog_known` | `server.go:1896-1904`、`server.go:1761-1785` | 请求时计算 | 现场 `COUNT(*)`（`server.go:1774`）；查询失败置 `known=false`（`server.go:1777-1779`） |
| `stats.records_dlq` | `pipeline/pipeline.go:81` | 请求时计算 | 进程内累计计数，重启归零、replay 后不减。`web/src/lib/dlq.ts:3-17` 已明确禁止当 backlog 用 |
| `stats.records_failed` | `pipeline/pipeline.go:80` | 请求时计算 | 同上 |
| `stats.dlq_replay_count` / `dlq_delete_count` | `pipeline/pipeline.go:84-85` | 请求时计算 | 进程内累计 |
| DLQ 行主键 `id` | `sqlstore/store.go:244`、`storage.go:28` | 重启后仍稳定 | 自增整数，**按 `job_name` 作用域**（`store.go:1194`：`WHERE job_name=? AND id=?`）；不是全局 ID |
| DLQ `job_name` | `storage.go:29`、写入 `storage/adapters.go:143-154` | 重启后仍稳定 | 实为 pipeline UUID（`server.go:113-119` `runtimeSpec` 把 `spec.Name` 改为 `id`），但字段名叫 "job_name"，无类型约束 |
| DLQ `dag_node` | `storage.go:37`、`orchestrator/executor.go:1237` | 重启后仍稳定 | **仅 DAG 路径写入**；linear 路径 `pipeline.go:1733-1740` 不设该字段（`grep -rn "DAGNode" internal/etl/pipeline/pipeline.go` 只有 struct 定义 `pipeline.go:43`） |
| DLQ `pipeline_version` | `storage.go:36`、`pipeline/pipeline.go:38-41`（注释）、`docs/etl-api.md:52` | 不存在（恒 0） | 文档明写 `pipeline_version` is always 0 today（`docs/etl-api.md:52`）；不可用于版本关联 |
| DLQ `record_hash` | `storage.go:35`、`storage/dlq_hash.go:9-22` | 重启后仍稳定 | 对 `record_json` 做 SHA-256（`dlq_hash.go:17-22`）；相同记录重复捕获命中 `(job_name, record_hash)` 唯一键（`sqlstore/dialect.go:62-76`） |
| DLQ `attempt` | `storage.go:34`、`sqlstore/dialect.go:72`（`attempt = dead_letters.attempt + 1`） | 重启后仍稳定 | 重复捕获累加，**但 replay 成功后整行被删除**（见下），计数随之消失 |
| DLQ `error` / `error_class` | `storage.go:31-32`、分类 `core/errors.go:135-164` | 重启后仍稳定 | `error_class` ∈ `transient/data/schema/auth/config/programming/unknown`（`core/errors.go:67-73`）；**class 由 `strings.Contains` 文本匹配推导**（`core/errors.go:147-163`），不是结构化错误类型 |
| DLQ `identity_context` | `core/dlq_identity.go:42-59`、`sqlstore/store.go:249` | 重启后仍稳定 | 含 `failure_reason`/`source_database`/`source_table`/`target_database`/`target_table`/`replay_state`/`replay_provenance`/`sink_acknowledged_at`；**无 pipeline/node/connection 引用** |
| DLQ replay 生命周期状态 | `core/dlq_identity.go:25-30`（`pending`/`repair_required`/`quarantined`/`sink_acked`）、`server/dlq_replay_identity.go:156-171` | 重启后仍稳定 | 只在行存在期间有效；`sink_acked` 后该行被删除（`server.go:5085-5088`、`server.go:5141-5155`） |
| DLQ replay 结果 | `server.go:4695-4737`（`{"replayed": N}`） | 请求时计算 | 单次响应；审计里有 `dlq.replay` 事件（`server.go:4710`），但无逐条结果 |
| 审计事件 | `sqlstore/store.go:255-263`、`server.go:1570-1605` | 重启后仍稳定 | 字段仅 `action/method/path/target/remote/created_at`（`storage.go:58-66`）；`target` 为自由文本，如 `name` 或 `name:id`（`server.go:4687`），**无结构化 pipeline/node/issue 引用** |
| 运行历史 `run_history` | `sqlstore/store.go:265-276`、`storage.go:150-161`、写入 `server/lifecycle.go:62-91` | 重启后仍稳定（可能不完整） | 有 `started_at`/`finished_at`/`records_*`/`duration_ms`。**只暴露为 `GET /api/v2/pipelines/{id}/history`**（`server.go:3720-3728`），且 UI 仅 `SchedulesPage` 消费（`web/src/SchedulesPage.tsx:153`） |
| 运行历史悬挂行 | `server/lifecycle.go:62-65`、`lifecycle.go:71-95` | 请求时计算（有缺陷） | 进程在 `observeManagedPipeline` 前崩溃时，行永久停留 `status='running'`（`lifecycle.go:80` 才更新终态）；全仓无 stale-run 收敛逻辑（`grep -rn "run_history" internal/etl/server/*.go` 只命中 janitor 的 TTL 清理） |
| 派生 health | `telemetry/health.go:89-124`、`server.go:5202-5213`、`server.go:5413-5424` | 请求时计算 | 由 `status` + `DLQFileCount` + `cdc_lag_ms` + `checkpoint_age_seconds` + `circuit_breaker_state` 现场推导；**无 ID、无窗口、无历史** |
| `circuit_breaker_state` | `pipeline/pipeline.go:467-472`、`pipeline/runner_iface.go:142-150` | 请求时计算 | `0/1/2`；per-pipeline（parallel 取 worst）。重启归零 |
| Health 聚合 `pipeline_issues` | `server.go:5425-5439`、`server.go:5460-5464` | 请求时计算 | 把 `error_code`/`remediation` 塞进 `map[string]string` 再 `json.Marshal` 成字符串（`server.go:5461-5463`）；**是转义后的 JSON 字符串，不是数组** |
| connection 记录 | `sqlstore/store.go:310-320`、`storage.go:236-246` | 重启后仍稳定 | `name` 为主键（TEXT）。字段：`kind/type/config_json/last_status/last_error/last_tested_at` |
| connection 连接测试结果 | `sqlstore/store.go:315-317` | 重启后仍稳定 | `last_status`/`last_error`/`last_tested_at`；**是"最近一次手动 test"的结果，不是运行时 sink 健康** |
| connection context | `server/connection_context.go:31-58`、路由分发 `server/connection_catalog.go:76-79` | 请求时计算 | `GET /api/v2/connections/{name}/context`。返回 descriptor/recommendations/introspection；**没有"哪些 pipeline 用了它"** |
| connection 引用解析（正向） | `server/connection_refs.go:27-49`、`connection_refs.go:115-154` | 请求时计算 | spec → connection 的**正向**解析（启动时 merge config）。**不是反向引用索引** |
| 反向 connection 引用（谁用 X） | `grep -rn "used_by\|UsedBy\|usedBy\|referenced_by\|ReferencedBy\|connection_references\|ConnectionReferences" internal/ web/src/ docs/etl-api.md docs/openapi.yaml` → **0 命中** | 不存在 | Connections 删除确认文案自己承认：`Pipeline reference relationships cannot be confirmed here`（`web/src/i18n.ts:555-558`）；删除前无任何检查（`server/connection_catalog.go:99-108` 直接 `DeleteConnection`） |
| 稳定 incident/issue ID | `grep -rn "issue_id\|IssueID\|incident_id\|IncidentID" internal/ web/src/` → **0 命中**；`grep -rn "correlation" internal/ web/src/ docs/etl-api.md docs/openapi.yaml` → **0 命中** | 不存在 | 前端 issue `id` 是模板拼接：`` `${key}:failed` ``/`${key}:dlq`/`${key}:lag`/`${key}:cp`/`${key}:lasterr`（`web/src/lib/pipeline-health.ts:182/198/213/228/241/255`），服务端不认识这些值 |
| `/api/v2/issues` / `/api/v2/incidents` 端点 | `grep -rn "api/v2/issues\|api/v2/incidents" internal/ web/src/ docs/openapi.yaml` → **仅 1 条无关命中**（`internal/etl/source/rest_templates_test.go:76` 的 GitHub URL 字面量） | 不存在 | 路由表 `server.go:1203-1234` 无 issue/incident 端点 |
| `PipelineIssue`（结构化校验错误） | `server/pipeline_errors.go:19-30` | 请求时计算 | 有 `code/scope/field/node_id/node_kind/plugin/message/remediation`，**形似契约但仅用于 validate/create/preflight 的即时响应**；不持久化、不进 DLQ、不与运行时故障关联 |
| `/api/v2/workers` | `internal/etl/master/master.go:172-176`、注册于 `server.go:1236` | 重启后仍稳定 | 落在 master 节点；standalone 也注册，但 worker 表仅分布式有意义（`sqlstore/store.go:277-286`） |
| reconcile 报告 | `server/reconcile.go:88-175`、路由 `server.go`（`/reconcile`） | 请求时计算 | 源/目标对账结果，**不落库**；回答"数据是否一致"，与 outage 生命周期无关 |
| preflight 结果 | `server/preflight.go:93-102`、`preflight.go:114-125` | 请求时计算 | `RunPreflight` 不写存储：`grep -n "s\.store\." internal/etl/server/preflight.go` → **0 命中** |
| alert 事件 | `internal/etl/alert/alert.go:26-33` | 请求时计算 | 有 `JobName`/`Timestamp`，但**无持久化**（`grep -rn "store\.\|Insert" internal/etl/alert/*.go` 无存储写入）；异步 256 缓冲，溢出即丢 |

## 同一 sink outage + DLQ 生命周期可关联性

以 `orders`（MySQL CDC → ClickHouse）遇到 ClickHouse 停机为例，按代码实际行为逐步走查。时序与判据全部来自第 1 节的 file:line。

### 阶段 1：sink 开始失败，尚未产生 DLQ 行

1. 批量写失败 → `pipeline.go:1361-1367`：`circuitBreaker.RecordFailure` + `stats.LastError = err.Error()`。此时 **`last_error_code` 可能为空**（该路径不赋值，见 `pipeline.go:1365-1367` 对比 `pipeline.go:546-548`）。
2. 单行隔离重试（`pipeline.go:1376-1389`）把真正的坏行挑出来。
3. 此时可观察信号：`stats.last_error`、`circuit_breaker_state`、`sink_write_latency_ms`、`sink_metrics[].errors`。

**关联性**：这一阶段没有任何"事件"对象。三个信号同源于同一个内存 runner，因此**隐式同 pipeline**——但只能靠"同一个 `id`"推断，不靠任何 ID。若 UI 想把它们显示为"一个故障"，唯一可用的 join key 是 pipeline UUID，而这会把该 pipeline 历史上所有不相干故障合并成一个。

### 阶段 2：失败行写入 DLQ

`pipeline.go:1732-1774` → `handleFailedRecord` 构造 `DLQEntry{JobName: r.spec.Name, Record, Error, ErrorClass: ClassifyError(err), IdentityContext, Attempt: 1}`，**不设 `DAGNode`、不设 `PipelineVersion`**（`pipeline.go:1733-1740`）。

落库走 `storage/adapters.go:138-158` → `sqlstore/store.go:1154-1184`，按 `(job_name, record_hash)` upsert（`dialect.go:62-76`）。

**此时 DLQ 行携带的 join key**：

| 候选 join key | 是否可用 | 证据 |
| --- | --- | --- |
| `job_name` = pipeline UUID | 可用，但粒度=pipeline | `server.go:113-119` 把 `spec.Name` 换成 id；`storage.go:29` |
| `dag_node` | **linear 路径恒空** | `pipeline.go:1733-1740` 无该字段；仅 `orchestrator/executor.go:1237` 写入 |
| `pipeline_version` | **恒 0，不可用** | `docs/etl-api.md:52`、`pipeline.go:38-41` |
| connection 名 | **不存在** | `DLQRecord`（`storage.go:26-39`）与 `DLQIdentityContext`（`core/dlq_identity.go:42-59`）均无该字段 |
| incident/issue ID | **不存在** | 见第 1 节 grep（0 命中） |
| `error_class` | 可用，但只到类别级 | `core/errors.go:67-73`；一个 ClickHouse 停机所有行都是 `transient`，与"MySQL 连接超时"无法区分 |
| `record_hash` | 可用，但只标识单条记录 | `storage/dlq_hash.go:9-22`；无法表达"这是同一次故障" |

**关键断点**：DLQ 行与阶段 1 的 `last_error` 之间**没有共享标识**。两者的唯一联系是"都在同一个 pipeline 上"。DLQ 行也不记录"捕获时对应的那次 sink 错误"，只记录该行自己的 `error`（`pipeline.go:1736`）——对批量失败+单行隔离，这个 error 通常是隔离重试时的错误，**可能与首次批量错误不同文本**。

### 阶段 3：`deriveIssues` 生成多条"issue"

`web/src/lib/pipeline-health.ts:169-267`。同一 outage 会同时产出：

- `${key}:dlq`（`pipeline-health.ts:196-210`，条件 `backlog > 0`）
- `${key}:rec-failed`（`pipeline-health.ts:211-225`，条件 `records_failed > 0 && backlog === 0`）
- `${key}:lag` 或 `${key}:cp`（`pipeline-health.ts:226-252`，`else if` 分支：lag 与 cp **互斥**）
- `${key}:lasterr`（`pipeline-health.ts:253-265`，仅当前面分支都没命中时）

注意 `pipeline-health.ts:194` 的 `else if (health === 'degraded')`：**只有 `degraded`/`failed` 才生成 issue**。而 `derivePipelineHealth`（`pipeline-health.ts:52-68`）对 `running` 只看 backlog/lag/checkpoint age，**不看 `last_error`**——所以一个 `last_error` 非空但 backlog=0、lag 正常、checkpoint 新鲜的 pipeline 会显示 `healthy`，其 `${key}:lasterr` 永远不会出现。

这些 `id` **不是服务端 ID**，是前端模板字符串（`pipeline-health.ts:182/198/213/228/241/255`）。服务端从未见过它们，`GET /api/v2/health` 也不返回它们。

**关联性判定**：这些信号**彼此之间没有服务端 join key**。它们能被并列显示，纯粹因为 `deriveIssues` 在同一个 `for (const p of pipelines)` 循环里遍历到同一个 `p`（`pipeline-health.ts:174`）。这是**客户端按 pipeline 分组**，正是 spec.md 非目标禁止的"在客户端聚合事故"（`spec.md:40`、`tasks.md:68`）。它们的数量、顺序（`pipeline-health.ts:269-284`）和存在与否都随请求时刻的计数变化。

### 阶段 4：运维修复并 replay

`POST /api/v2/dlq/{pipeline}/replay`（`server.go:4695-4717`）→ 逐条：`checkpointDLQReplay` 置 `sink_acked`（`dlq_replay_identity.go:156-171`）→ `deleteReplayedDLQItem` 按主键删除（`server.go:5141-5155`）。

**关联性断点（重要）**：replay 成功后**整行被删除**。因此：

1. 该 DLQ 行的 `id`、`record_hash`、`identity_context.sink_acknowledged_at`、`replay_state` **全部消失**。
2. 唯一残留是审计行 `action='dlq.replay'`、`target=name` 或 `name:id`（`server.go:4710`、`server.go:4730`、`server.go:4687`）。`target` 是自由文本，且 `name:id` 里的 `id` 已无法再解析回任何行。
3. 响应 `{"replayed": N}`（`server.go:4717`）是**一次性**的，不落库。UI 刷新后无法得知"刚才那条 replay 成功了还是失败了"。
4. `stats.dlq_replay_count` 是进程内累计（`pipeline.go:84`），重启归零。

所以 **"故障 → 修复 → 验证"的闭环在服务端没有持久化证据**：replay 之前之后唯一稳定可查的是 `audit_logs` 里一条没有对象语义的文本行。

### 阶段 5：恢复判定

`dlq_backlog` 归零（`server.go:1901-1903`）+ checkpoint age 正常 → `derivePipelineHealth` 回到 `healthy`。此时：

- `stats.last_error` **仍然是旧的错误文本**（永不自动清空，见第 1 节）——但它不参与 health 判定（`telemetry/health.go:114-120` 只读 DLQFileCount/CDCLag/cpAge/CB），所以不会再让 pipeline 显示 degraded。
- 但 `web/src/lib/pipeline-health.ts:253-265` 仍会在 `degraded` 时把这条陈旧 `last_error` 当 `${key}:lasterr` 展示。
- 没有任何字段记录"这次 outage 何时开始、何时结束、影响了多少记录、replay 了多少"。

### 小结表

| 信号 | 携带到下一个信号的 join key | 结论 |
| --- | --- | --- |
| `last_error` / `last_error_code` | 仅 pipeline UUID（隐式）；无时间、无 ID | 独立信号 |
| `circuit_breaker_state` | 仅 pipeline UUID | 独立信号 |
| DLQ 行 | `job_name` + 可选 `dag_node`（linear 恒空）+ `record_hash` | 可关联到 pipeline，**不可关联到"哪次故障"** |
| `dlq_backlog` | 仅 pipeline UUID | 独立聚合数 |
| `checkpoint_age_seconds` | 仅 pipeline UUID | 独立信号 |
| `cdc_lag_ms` | 仅 pipeline UUID | 独立信号 |
| health | 由上述现场推导 | 派生值，非事实源 |
| replay 结果 | **无**（行已删除） | 一次性响应 |
| connection 引用 | **不存在** | — |

**可稳定关联的**：同一 pipeline UUID 下的所有信号；同一 DLQ 行在 `id`/`record_hash` 不变期间的身份与 replay 前置状态。

**仅为独立信号的**：`last_error`、`circuit_breaker_state`、`dlq_backlog`、`checkpoint_age_seconds`、`cdc_lag_ms`、`stats.records_*`、`deriveIssues` 产出的所有 `${key}:*`。它们无法回答"这两条是不是同一次故障"。

## 结论

**判定：否——不足以支撑可靠的 recovery task。**

具体缺口（每条对应第 1/2 节的证据）：

1. **无稳定故障/事件身份**。服务端没有任何 incident/issue 表、ID、或 correlation 概念（`grep -rn "incident_id\|IssueID\|correlation" internal/ web/src/` → 0 命中）。前端 `DerivedIssue.id` 是模板字符串（`pipeline-health.ts:182-255`），随计数和 `else if` 分支变化，刷新即可变。没有它，`#/issues?issue=...` 无稳定对象可指（`routing.ts` 的 `AppRoute` 亦无 issue 字段）。
2. **无 node 级故障归属（linear）**。`dag_node` 只在 DAG 路径写入（`orchestrator/executor.go:1237`），linear 路径 `handleFailedRecord` 不设（`pipeline.go:1733-1740`）。而 UI-E 的主场景 `orders → ClickHouse` 恰是 linear。
3. **无 spec 版本关联**。`pipeline_version` 恒 0（`docs/etl-api.md:52`、`pipeline.go:38-41`）。无法回答"这次失败属于哪个 spec 版本"。
4. **无 connection 引用（任一方向）**。没有"哪些 pipeline 用了 connection X"的 API 或索引（grep 0 命中）。Connections 删除前不做检查（`connection_catalog.go:99-108`），文案自认不可确认（`i18n.ts:555-558`）。UI-E.5 的"删除影响"无事实来源。
5. **无"最近成功写入"事实**。`stats.last_checkpoint` 是进程内 `time.Now()`（`pipeline.go:2016`），重启为零。持久化的 `checkpoints.timestamp`（`store.go:241`）语义是 checkpoint 提交时刻，不等于 sink 成功写入。二者都不能直接回答"最近一次成功写入是什么时候"。`plan.md:109` 已把 `last_successful_write_at` 列为待核对项——核对结果是**不存在**。
6. **恢复过程不留痕**。replay 成功后行被删除（`server.go:5141-5155`），只剩无结构语义的审计文本（`storage.go:58-66`）。"故障→修复→验证"无法服务端回放。
7. **`run_history` 不可用于 outage 归因**。它按 run 记录、只有起点终点与总量（`storage.go:150-161`），无错误信息、无 DLQ 关联；且崩溃时可能永久停留 `running`（`lifecycle.go:62-95` 无收敛）。

**但不等于"现有信号毫无价值"**：pipeline UUID、DLQ 行 `id`+`record_hash`、`error_class`、`checkpoint_age_seconds`、`dlq_backlog` 都是可靠且重启稳定的。问题不在单个字段，而在**它们之间缺少一条被服务端承认的关联**。

**因此 UI-E.4 的验收 1（"仅当 UI-E.3 有可靠契约时…否则保留多信号列表但改善动作命名"）当前走"否则"分支**：可以改善动作命名与上下文连续性，**不得**呈现单一 recovery task 的根因判定。

## 若不足：最小 server-backed contract 提案（待用户批准，未实现）

> **状态：PROPOSAL，未实现，未改任何代码。** 需用户明确批准后另立 claim。目标不是"做一套事件系统"，而是让**一次 sink outage 的 recovery task 可靠**——即：能生成、能查询、能表达状态迁移、能在 replay 后核对。
>
> **设计原则**：只加"身份与状态"，不加"推断"。服务端不猜根因，只记录调用方（runner）当时确实知道的事实。

### 最小字段集

新增一张表 `recovery_events`，与既有 `pipelines` / `dead_letters` 通过主键关联，不复制它们的字段。

| 字段 | 类型 | 稳定 ID 语义 | 写入时机 | 存储 backend 兼容 | API 版本兼容策略 |
| --- | --- | --- | --- | --- | --- |
| `id` | TEXT PRIMARY KEY (UUID) | 全局唯一、永不复用；即 URL 里的 `issue` 值 | 事件创建时（首次失败），由服务端生成 | 三 backend 均为 TEXT PRIMARY KEY，无自增依赖（对齐 `pipelines.id`，`store.go:215`） | 新字段，`/api/v2` 仅增不改；旧客户端忽略 |
| `pipeline_id` | TEXT NOT NULL | 引用 `pipelines.id`（UUID） | 创建时 | 已有列类型；加索引 `(pipeline_id, opened_at DESC)` | 复用现有 pipeline UUID，无需新 ID 体系 |
| `node_id` | TEXT NOT NULL DEFAULT '' | DAG node id；linear 路径写空串 | 创建时由 runner 传（linear 显式写 `''`，不猜） | `ALTER TABLE ... ADD COLUMN dag_node TEXT` 已有先例（`store.go:377`）；`NOT NULL DEFAULT ''` 略强于先例，三 backend 均支持 | 空串=无 node（linear），非 nullable 避免三值逻辑 |
| `connection_name` | TEXT NOT NULL DEFAULT '' | 失败时实际使用的 connection 名；内联配置写空串 | 创建时（`resolvePipelineConfig` 已知该值，`connection_refs.go:115-154`） | TEXT 默认空 | 新字段；空串=内联/未知，UI 必须如实表达 |
| `error_class` | TEXT NOT NULL | 复用 `core.ErrorClass`（`errors.go:67-73`） | 创建时 | 已有枚举值，`store.go:248` 同类型 | 复用既有分类，不新增枚举 |
| `opened_at` | DATETIME NOT NULL | 事件首次观察时间 | 创建时 | 三 backend 均支持（`store.go:251` 同型） | — |
| `closed_at` | DATETIME NULL | 关闭时间；NULL 表示仍打开 | 关闭时 | nullable DATETIME 已在 `run_history.finished_at` 使用（`store.go:270`） | — |
| `state` | TEXT NOT NULL | `open` / `replaying` / `resolved` / `abandoned` | 状态迁移时 | TEXT 枚举，同 `status` 列 | 未知值客户端降级为 unknown，不报错 |
| `spec_version` | INTEGER NOT NULL DEFAULT 0 | 事件发生时的 `pipeline_versions.version`；未知写 0 | 创建时，若可得 | INTEGER 已有（`store.go:376`） | 0 表示未知，**不得**显示为"版本 0" |
| `attempt_count` | INTEGER NOT NULL DEFAULT 0 | 归入该事件的失败次数（服务端累加） | 每次捕获失败行时 +1 | INTEGER，同 `attempt` 列 | — |
| `dlq_row_ids` | 不落此表 | **反向**：`dead_letters` 加一列 `recovery_event_id TEXT DEFAULT NULL` | 写 DLQ 行时 | `ALTER TABLE ... ADD COLUMN`，与迁移 4/5/6 同模式（`store.go:375-377`） | nullable 列，旧行 NULL=未关联，不伪造 |

**为什么 `dlq_row_ids` 放在 `dead_letters` 而不是事件表**：一次 outage 可对应上万行，事件表存数组不可查询且会超长。反向单列 + 索引 `(recovery_event_id)` 是最小可行方案。

### 生命周期（服务端状态机，不允许客户端推进）

```
open ──(用户点 replay)──> replaying ──(全部行 replay 成功/删除)──> resolved
  │                            │
  └──(用户放弃)──> abandoned    └──(replay 失败)──> open (attempt_count+1)
```

- `open → replaying`：由 `POST /api/v2/dlq/{pipeline}/replay` 在服务端迁移，不由 UI 调用。
- `replaying → resolved`：当该事件关联的 DLQ 行为 0 时由服务端迁移。**不因"UI 刷新"迁移。**
- 迁移全部落库（`closed_at` 同时写），重启后状态不变。

### 最小 API（仅 2 个只读端点 + 1 个既有端点扩展）

| 端点 | 变更 |
| --- | --- |
| `GET /api/v2/recovery-events?pipeline=&state=&limit=&offset=` | 新增。返回事件列表，字段同上表 + `dlq_open_rows`（现场 COUNT，与 `dlq_backlog` 同语义，`server.go:1761-1785` 可复用） |
| `GET /api/v2/recovery-events/{id}` | 新增。单事件 + 关联 DLQ 行摘要（不返回 `raw_payload`） |
| `GET /api/v2/dlq/{pipeline}` | **仅增字段** `recovery_event_id`；既有字段与语义完全不变 |
| `GET /api/v2/pipelines` | **仅增字段** `open_recovery_event_id`（无则 null） |

**明确不做**：不新增 `POST/PUT /recovery-events`（状态只由服务端迁移）；不改 DLQ/replay 语义；不做事件合并、去重启发式、根因推断；不加 `/api/v2/issues`。

### 存储 backend 兼容

| backend | 兼容方式 | 证据/风险 |
| --- | --- | --- |
| SQLite | `CREATE TABLE IF NOT EXISTS` + `ALTER TABLE ADD COLUMN`，走既有 migrations 数组 | `store.go:212-352`、`store.go:369-412` 已有 24 个迁移的先例 |
| MySQL | 同上；`dialect.Bind` 的 `?`→`$` 改写只影响 Postgres | `dialect.go:151-163`（`MySQLDialect.DeadLetterUpsert`）证明同一关系型语义可在双 backend 表达 |
| PostgreSQL | 同上；注意 `store.go:1281-1285` 提到的 `DELETE ... LIMIT` 不可移植——**本提案不含限长 DELETE，无此风险** | `dialect.go:165-176` |
| Redis 状态后端 | **不涉及**。`recovery_events` 是控制面元数据，与 `state_redis_*` 无关 | — |
| 备份/恢复 | 需在 `storage/backup.go:75` 附近登记新表；`sqlstore/backup.go:130` 的截断清单需同步 | 遗漏会导致备份静默丢事件——**这是最需要测试的点** |

### 测试策略

| 层级 | 覆盖 | 落点 |
| --- | --- | --- |
| 单元 | 状态机迁移合法性；`spec_version=0` 不被渲染为版本 | 新 `recovery_events_test.go` |
| 存储契约 | SQLite/MySQL/PostgreSQL 三 backend 同一 conformance 用例建表+写入+查询+迁移 | 扩展 `storage/conformance_test.go`、`conformance_backends_test.go` |
| 迁移 | 旧库（无新表）升级后：新表存在、旧 `dead_letters` 行 `recovery_event_id` 为 NULL 且**不被回填猜测** | 对齐 `storage/upgrade_test.go` 既有模式 |
| 备份 | 含事件的备份 restore 后事件与关联完整 | `storage/backup_test.go` |
| 故障路径 | sink 停机 → 事件 open → 修复 → replay → 事件 resolved；**崩溃在 replay 中途**后事件仍为 `replaying` 且未误判 resolved | 对齐 `dlq_semantics_test.go:175-212`（并发 replay）、`dlq_replay_identity_test.go:368-424`（崩溃后可重试） |
| 容器 E2E | `hack/e2e-dlq.sh` 或 `hack/e2e-clickhouse.sh` 注入 ClickHouse 停机，断言事件生命周期与 `GET /api/v2/recovery-events` | 复用既有 outage 路径 |
| 契约文档 | `docs/openapi.yaml` 新 path + `docs/etl-api.md` 新节 | — |

### Rollback 故事

1. **纯增量**：新表 + nullable 新列 + 新只读端点。旧端点零改动 ⇒ 旧客户端不受影响。
2. **回滚步骤**：删端点 → 停写新表（runner 侧 feature flag 关闭）→ 保留表与列（**不 drop**，避免回滚丢历史）→ UI 回到多信号列表。
3. **数据安全**：`recovery_events` 与 `dead_letters.recovery_event_id` 不被任何 replay/checkpoint 逻辑读取 ⇒ 回滚不影响 at-least-once 与幂等边界。
4. **失败注入开关**：写入失败必须降级为"事件缺失"而非阻塞 DLQ 写入。**DLQ 写入路径不能因事件写入失败而失败**（`pipeline.go:1750-1773` 的现有降级逻辑必须保持）。
5. 若采用后发现关联错误：关闭 UI 消费即可退回第 2 节的多信号列表，无需迁移回退。

### 明确不在本提案内

- 不做根因推断、错误聚类、跨 pipeline 事件合并。
- 不改 `error_class` 的文本匹配实现（`core/errors.go:147-163`）——它是既有语义，改动会动 DLQ/replay 行为。
- 不实现 connection 反向引用索引。它属于 UI-E.5，且需要独立设计（是否随 spec 变更增量维护 vs 请求时扫 `pipeline_versions`）。**建议单独立项**，不塞进本提案。

## UI 不得推断的边界

以下为冻结的消费规则。**在获得批准的 contract 之前，前端必须按此执行**；即使 contract 落地，1–6 条也仍然成立。

1. **不得由 `last_error` 文本生成身份或根因。** `stats.last_error` 是无 ID 的陈旧字符串（`pipeline.go:86`，永不自动清空），且多数 sink 失败路径不写 `last_error_code`（`pipeline.go:1365-1367`）。禁止字符串匹配、相似度、关键词归类。
2. **不得按时间接近把多个信号合并成一个事故。** `dlq_backlog`（现场 COUNT）、`checkpoint_age_seconds`（真实落库时间）、`last_error`（进程内时间）三者时钟来源不同：前两者来自存储/`time.Since`，后者是进程内赋值。时间接近不构成关联证据。
3. **不得把 pipeline 名字当作唯一关联键。** 名称可重名，`resolvePipelineRefLocked` 会拒绝有歧义的名称（`server.go:239-248`）。URL 与内部关联一律用 UUID（`storage.go:133-138`）。
4. **不得把 `stats.records_dlq` / `stats.records_failed` / `stats.dlq_replay_count` 当作积压或当前状态。** 它们是进程内累计、重启归零、replay 后不减（`pipeline.go:80-85`）。积压只有 `dlq_backlog` 一个来源，且必须尊重 `dlq_backlog_known=false`（`server.go:1901-1903`、`dlq.ts:19-22`）。
5. **不得把 `pipeline_version` 当作 spec 版本展示。** 恒 0（`docs/etl-api.md:52`）。
6. **不得把 `dag_node` 缺失解释为"node 未知但可推断"。** linear 路径恒空（`pipeline.go:1733-1740`）。显示 `unavailable`，不得回退到 "sink" 字面量猜测。
7. **不得把 `last_checkpoint` 当作"最近成功写入"。** 它是进程内 `time.Now()`（`pipeline.go:2016`）；重启后为零。展示时必须区分"checkpoint age（服务端计算）"与"本次进程启动时长"。
8. **不得把 health 当作历史事实。** `health` 是请求时推导（`telemetry/health.go:89-124`），无窗口无历史。可用于"当前是否异常"，不可用于"何时开始异常"。
9. **不得在 replay 成功后声称"已恢复"而没有服务端证据。** replay 成功后 DLQ 行被删除（`server.go:5141-5155`），响应 `{"replayed": N}` 是一次性的（`server.go:4717`）。在没有 event contract 时，UI 只能说"本次请求返回 N 条已重放"，并主动提示刷新后不可复核。
10. **不得展示 connection 引用影响。** 无反向引用 API（grep 0 命中）。删除确认必须保留"引用关系不可确认"的明确表述（现有文案见 `i18n.ts:555-558`），不得改为"已检查，无影响"。
11. **不得自行拼接 issue ID 作为 URL 参数。** `deriveIssues` 的 `${key}:*` 是前端产物（`pipeline-health.ts:182-255`），服务端不认识。当前 `IssuesPage` 写 `?issue=`（`IssuesPage.tsx:39-45`）但 `routing.ts` 的 `AppRoute` 无该字段——该参数目前是死数据，不得在此基础上构建深链。
12. **不得把 `pipeline_issues` 当结构化对象解析。** 它是 `map[string]string` 再 `json.Marshal` 的**转义字符串**（`server.go:5460-5464`），需二次解析，且键为显示名（`server.go:5409-5412`）而非 ID。
13. **保留多信号列表时的措辞约束。** 在无 contract 期间，UI-E.4 验收 1 的"否则"分支要求：可以把 `DLQ backlog`、`checkpoint stalled`、`last error` 并列为**独立观察**，可以改善动作命名（"查看失败原因"/"测试连接"），但**不得**出现"根因"/"影响范围"/"恢复阶段"这类暗示已建立关联的表述。

## 证据

### 执行的命令（全部只读）

```bash
# 基线与范围
git log --oneline -1                                   # e28a740
ls internal/etl/server/ internal/etl/storage/ internal/etl/storage/sqlstore/

# 路由表
grep -n "mux\.\(HandleFunc\|Handle\)\(" internal/etl/server/server.go
# → server.go:1203-1234，无 issues/incidents；1236 挂 master 的 /workers

# 不存在性证明（全部返回 0 命中）
grep -rn "incident_id\|IssueID\|incident_id\|IncidentID" internal/ web/src/
grep -rn "correlation" internal/ web/src/ docs/etl-api.md docs/openapi.yaml
grep -rni "incident" internal/ web/src/ docs/etl-api.md docs/openapi.yaml
grep -rn "used_by\|UsedBy\|usedBy\|referenced_by\|ReferencedBy" internal/ web/src/ docs/etl-api.md docs/openapi.yaml
grep -rn "connection_references\|ConnectionReferences" internal/ web/src/
grep -rn "api/v2/issues\|api/v2/incidents" internal/ web/src/ docs/openapi.yaml
grep -rn "LastSuccess\|last_success\|last_successful" --include=*.go --include=*.ts --include=*.tsx internal/ web/src/
# → 仅测试函数名（scheduler_activation_fault_test.go 等）与 plan.md:109 的待核对项，无 API 字段

# DLQ 写入/读取/upsert
grep -n "func (s \*Store) WriteDeadLetter" -A 40 internal/etl/storage/sqlstore/store.go
grep -n "DeadLetterUpsert" -A 25 internal/etl/storage/sqlstore/dialect.go
grep -n "DLQEntry{" -r internal/etl/ --include=*.go
sed -n '20,60p' internal/etl/pipeline/pipeline.go     # DLQEntry + PipelineVersion 注释

# 存储 schema
sed -n '210,340p' internal/etl/storage/sqlstore/store.go   # 建表 + migrations 1..24

# run history 与悬挂行
grep -rn "RecordRunStart\|RecordRunEnd\|ListRunHistory" --include=*.go internal/etl/
grep -rn "run_history" internal/etl/server/*.go        # 仅 janitor TTL，无 stale 收敛

# connection 引用
grep -n "func (s \*Server) connectionContext" -A 30 internal/etl/server/connection_context.go
grep -rn "DeleteConnection" internal/etl/server/*.go   # connection_catalog.go:103 直接删，无检查

# 前端消费
sed -n '169,285p' web/src/lib/pipeline-health.ts      # deriveIssues + 排序
sed -n '52,68p' web/src/lib/pipeline-health.ts        # running 只认 backlog/lag/cpAge
cat web/src/lib/dlq.ts
sed -n '30,50p' web/src/pages/IssuesPage.tsx          # ?issue= 死参数
sed -n '1,60p' web/src/lib/routing.ts                 # AppRoute 无 issue 字段
```

### 关键 file:line 索引

| 主题 | 锚点 |
| --- | --- |
| 路由表 | `internal/etl/server/server.go:1203-1236` |
| pipeline UUID 生成 | `internal/etl/storage/storage.go:133-138`；`server.go:97-101` |
| 名称歧义拒绝 | `internal/etl/server/server.go:239-248` |
| 运行时 spec 改名 | `internal/etl/server/server.go:113-129` |
| DLQ 行结构 | `internal/etl/storage/storage.go:26-39` |
| DLQ identity context | `internal/etl/core/dlq_identity.go:42-59` |
| DLQ replay 状态枚举 | `internal/etl/core/dlq_identity.go:23-30` |
| DLQ 写入（linear，无 DAGNode） | `internal/etl/pipeline/pipeline.go:1732-1740` |
| DLQ 写入（DAG，有 DAGNode） | `internal/etl/orchestrator/executor.go:1232-1241` |
| DLQ upsert `(job_name, record_hash)` | `internal/etl/storage/sqlstore/dialect.go:62-76` |
| `record_hash` = SHA-256(record_json) | `internal/etl/storage/dlq_hash.go:9-22` |
| `pipeline_version` 恒 0 的文档 | `docs/etl-api.md:52` |
| `dead_letters` DDL | `internal/etl/storage/sqlstore/store.go:243-254` |
| `IdentityContext` 列 | `internal/etl/storage/sqlstore/store.go:249`、迁移 22 `:394` |
| `pipelines` DDL（含 generation） | `internal/etl/storage/sqlstore/store.go:214-225` |
| `checkpoints` DDL | `internal/etl/storage/sqlstore/store.go:235-242` |
| `run_history` DDL | `internal/etl/storage/sqlstore/store.go:265-276` |
| `audit_logs` DDL | `internal/etl/storage/sqlstore/store.go:255-263` |
| `connections` DDL | `internal/etl/storage/sqlstore/store.go:310-320` |
| `workers` / `task_assignments` DDL | `internal/etl/storage/sqlstore/store.go:277-297` |
| checkpoint 落库（fence） | `internal/etl/storage/sqlstore/store.go:1079-1104` |
| 进程内 LastCheckpoint | `internal/etl/pipeline/pipeline.go:2016` |
| `Stats` 结构 | `internal/etl/pipeline/pipeline.go:77-94` |
| `last_error` 永不自动清空 | 全仓无 `setLastError("")`；写入点 `pipeline.go:526/546/1366/1980/2050` |
| sink 写失败只写 last_error | `internal/etl/pipeline/pipeline.go:1361-1367` |
| 结构化 code 仅少数路径 | `internal/etl/pipeline/pipeline.go:530-550` |
| error 分类（文本匹配） | `internal/etl/core/errors.go:135-164`；枚举 `:67-73` |
| health 派生 | `internal/etl/telemetry/health.go:89-124` |
| health 输入装配 | `internal/etl/server/server.go:5202-5213`、`:5413-5424` |
| `pipeline_issues` 转义字符串 | `internal/etl/server/server.go:5425-5439`、`:5460-5464` |
| `dlq_backlog` 语义 | `internal/etl/server/server.go:1896-1904`；计算 `:1761-1785` |
| replay 成功后删行 | `internal/etl/server/server.go:5085-5088`、`:5141-5155` |
| replay 前打 `sink_acked` | `internal/etl/server/dlq_replay_identity.go:156-171` |
| replay 一次性响应 | `internal/etl/server/server.go:4695-4737` |
| audit 字段（无结构引用） | `internal/etl/storage/storage.go:58-66`；写入 `server.go:5832-5837` |
| audit target 自由文本 | `internal/etl/server/server.go:4687`、`:4730` |
| run history 悬挂行 | `internal/etl/server/lifecycle.go:62-95` |
| run history 唯一消费点 | `internal/etl/server/server.go:3720-3728`；UI `web/src/SchedulesPage.tsx:153` |
| connection 结构 | `internal/etl/storage/storage.go:236-246` |
| connection 正向解析 | `internal/etl/server/connection_refs.go:27-49`、`:115-154` |
| connection context 响应 | `internal/etl/server/connection_context.go:31-58`、`:95-125` |
| connection 删除无引用检查 | `internal/etl/server/connection_catalog.go:99-108` |
| 删除确认自认不可确认 | `web/src/i18n.ts:555-558` |
| 前端 issue 模板 ID | `web/src/lib/pipeline-health.ts:182-255` |
| 只有 degraded/failed 出 issue | `web/src/lib/pipeline-health.ts:194` |
| `?issue=` 死参数 | `web/src/pages/IssuesPage.tsx:39-45`；`web/src/lib/routing.ts:4-18` |
| 前端 health 不读 last_error | `web/src/lib/pipeline-health.ts:52-68` |
| backlog 唯一来源 | `web/src/lib/dlq.ts:12-22` |
| preflight 不落库 | `internal/etl/server/preflight.go:93-102`、`:114-125`（`grep -n "s\.store\." internal/etl/server/preflight.go` → 0 命中） |
| alert 不持久化 | `internal/etl/alert/alert.go:26-33`；`internal/etl/alert/*.go` 无 store 写入 |
| `PipelineIssue`（即时响应） | `internal/etl/server/pipeline_errors.go:19-30` |

### 未执行的检查（如实记录）

- **未运行任何测试或构建**（任务要求只读；`go test`、`npm run build`、`hack/e2e-*.sh` 均未执行）。
- **未做 tasks.md:62 要求的"最小容器/fixture 验证"**。本次以代码路径推演替代，理由是：推演出的断点（`dag_node` 恒空、replay 后删行、`pipeline_version` 恒 0）在单元测试中已可确认，且容器验证属于会改变运行状态的执行动作。**这项验证仍应在实现前补做**，作为第 5 节提案的输入确认。
- 未核对 ClickHouse/MySQL connector 各自是否额外暴露 sink 级错误计数（`sink_metrics[].errors` 来自 `internal/etl/sink/sink_metrics.go:39`，未逐 connector 确认赋值完整性）。
