# 领取与验收记录

| Round | Increment | Status | Evidence |
| --- | --- | --- | --- |
| 1/5 | EV-2 | delivered | Go 1.24.13 + 隔离 MySQL 8：3 个 targeted -race tests passed；1000 行字段对账通过；PR run 45652762125 / integration job 139341305228 success。 |
| 2/5 | P5-H1 | delivered | API/Go race、9 个前端 contracts、真实容器 14/14：见 h1-evidence.md。 |
| 3/5 | PR-0-C1 | delivered | 三 backend race/conformance/backup、完整 server -race、真实容器 9/9；见 c1-evidence.md。 |
| 4/5 | UI-E-R1 | delivered | 三 backend migration/recovery/backup、完整 affected Go race、真实 UI 21/21；见 r1-evidence.md。 |
| 5/5 | GA-C1 | delivered | 898af75 候选 23/23、七个 tagged path（28 checks）、44 UI checks、3 readiness checks；保存资产本地 registry 同字节晋升与 run 45711062461 的 36/36 CI jobs 全部 passed。 |

## Round 1 Claim

Round: 1/5
Roadmap item: EV-2
Profile/path: standalone / mysql sink integration CI
Objective: 1000-row smoke 验证提交后的记录与字段，新的 integration CI job 通过
Scope: internal/etl/sink/mysql_test.go、对应 workflow 验证和 roadmap/迭代证据
Non-goals: 性能预算升级、sink 写入语义变化、发布新版本
Dependencies: 隔离 MySQL、项目 Go 1.24 工具链、GitHub PR CI
Acceptance: 1) 1000 行完整对账；2) 真实 MySQL targeted test；3) CI integration job success
Evidence: etl-go-dev:latest（Go 1.24.13 linux/arm64）；隔离 openetl-p1-mysql；go test -race -count=1 -run '^TestMySQL(BenchmarkRealDB|BuildBatchInsertSQL|BuildBatchDeleteSQL)$' -v ./internal/etl/sink → PASS，exit 0；PR #13 run 45652762125 / job 139341305228 success
Result: delivered
Residual/follow-up: 原验收要求仅为该 integration job 转绿；本窗口首次拆分曾额外加入 aggregated gate，现纠正其归属至 GA-C1。旧路径证据造成的聚合失败继续有界处理。

## Round 2 Claim

Round: 2/5
Roadmap item: P5-H1（P5/UI-F 有界后续）
Profile/path: standalone API / production Web UI
Objective: open breaker、restore_failed、DLQ unknown 从四个现有 UI 入口一致可见
Scope: telemetry/health.go 与测试、UI types/health consumers、i18n、UI regression、相关 API/发布说明
Non-goals: 新 incident 模型、连接删除保护、全局 UI 重做、P2 基准/历史修复
Dependencies: EV-2 delivered；现有 metrics 已提供服务端 health 与 circuit_breaker_state
Acceptance: 1) 三个失败/未知场景的 API/UI 对齐；2) 恢复后回归 healthy；3) server -race 与 UI unit/typecheck/lint/build；4) 四入口真实容器 UI 回归
Evidence: [h1-evidence.md](./h1-evidence.md)；Go 1.24.13 container server/telemetry/pipeline -race exit 0；npm test 9/9、typecheck/lint/build exit 0；image 03c991b9a2cf… real UI 14/14 exit 0。
Result: delivered
Residual/follow-up: PR-0-C1；用户研究不在本增量。

## 证据规则

每轮记录 pass/fail/skip/block 与 exact command/run。
没有目标用户研究记录时，UI-E 整体不置 delivered。
到 Round 5/5 后只完成当轮安全收口，后续需要新的用户指令。

## Round 3 Claim

Round: 3/5
Roadmap item: PR-0-C1
Profile/path: standalone + shared SQL metadata (SQLite/MySQL/PostgreSQL)
Objective: 删除连接不会破坏当前持久化管道引用，UI 可列出真实影响
Eligibility: EV-2/P5-H1 delivered；用户已授权全部 P1 技术修复；无外部依赖缺口
Scope: storage reference extraction/cipher policy/SQL transaction guard/migrations、spec adapters、connection API/UI、三 backend tests 与对应 API/runbook evidence
Non-goals: 连接版本系统、自动重写管道、历史版本引用保护、用户研究、checkpoint/DLQ 语义改造
Data semantics: 共享 SQL coordination row 序列化 current-spec write、connection mutation/delete；cipher 解密仅在进程内；当前 spec 为事实，历史版本不算引用。引用解除后才可删；未知/不可解密的 legacy spec 阻断删除并定位。备份 fidelity 原样恢复 legacy rows，不制造新版本。
Rollback: additive guard table 保留；不删除 pipeline/checkpoint；独立回退 API/UI 后可撤回 guard。
Acceptance: 1) source/transform/sink/DLQ/DAG current 引用可查询；2) 删除/类型修改冲突 409 包含稳定 pipeline/node IDs；3) API/YAML/current-spec 与删除并发不留可运行的悬空引用；4) 三 backend race/conformance、加密/restart/backup 和真实 UI evidence
Evidence: [c1-evidence.md](./c1-evidence.md)；三 backend storage/sqlstore/backup -race 与完整 server -race exit 0；真实 API/YAML/UI 9/9 exit 0；typecheck/lint/build、OpenAPI YAML check、diff check exit 0。
Result: delivered
Residual/follow-up: UI-E-R1

## Round 4 Claim

Round: 4/5
Roadmap item: UI-E-R1
Profile/path: standalone recovery + three SQL metadata backends; DAG node capture only, no distributed maturity claim
Objective: sink outage 的失败关联、dry-run/replay 结果在刷新、重启与 DLQ 删除后仍可复核
Eligibility: PR-0-C1 delivered；用户明确授权先实现技术契约，研究不参与技术验收
Scope: core/pipeline/DAG known capture context、storage/adapters/SQL recovery/migrations/backup、server recovery list/detail/replay/delete/dry-run、Issues/DLQ/Connections/detail routing、i18n、对应测试/API/runbook/evidence
Non-goals: 根因推断、错误文本/时间聚类、跨 pipeline 合并、任意用户推进事件状态、用户研究、运行历史 P2、exactly-once 或 connector maturity 升级
Data semantics: durable DLQ 捕获建立服务端 UUID，按明确的 pipeline/endpoint/node/connection scope 复用未关闭记录；context 未知不猜关联。捕获 metadata 事务失败回退到 durable DLQ 并持久化/暴露 tracking error，不影响 checkpoint 安全门。sink_acked 后的 DLQ 删除和 retained replay outcome 同事务；失败保留 DLQ。主动删除、TTL、pipeline delete 记 abandoned，混合丢弃不记 resolved。启动不按超时推断成功。
Backup/rollback: v3 增加 recovery tables/关联/操作，保留 v1/v2 读取与 ID/计数/时间 fidelity；回退保留新表，不 drop。URL 仅接受 typed identity/filter allowlist，敏感参数/无效 ID 明确拒绝。
Acceptance: 1) stable persisted failure/replay identity + known associations；2) failures/restart/replay/sink_ack/delete/TTL 三 backend；3) capture bookkeeping failure 不破坏 DLQ durability，result persistence failure 不误删；4) v1/v2 migration + v3 backup/restore fidelity；5) real outage/dry-run/replay + refresh/deep-link tests；6) UI-E parent research保持 blocked_external
Evidence: r1-evidence.md；三 backend recovery/migration/backup 与 affected packages -race exit 0；server final -race exit 0；frontend 15/15 + typecheck/lint/build；最终 container UI 21/21 exit 0。
Result: delivered
Residual/follow-up: GA-C1


## Round 5 Claim

Round: 5/5
Roadmap item: GA-C1
Profile/path: release assets + standalone primary paths / connector certification; no distributed maturity upgrade
Objective: certifier and publisher consume the exact candidate image/binaries/archives, with retained execution and promotion receipts
Eligibility: UI-E-R1 delivered at 9924d50; all five P1 technical repairs explicitly authorized; pinned GoReleaser 1.26.2 CLI available, Go/container/GitHub CI available
Scope: release/candidate workflows and helpers, .goreleaser.yml, Dockerfile.goreleaser, necessary IMAGE/binary selectors in certification scripts, tagged harness/evidence, evidence validator, build-source identity and externally bound readiness evidence, docs and generated evidence
Bounded prerequisite: embedded certification currently requires an image rebuild to update its digest. Add a read-only external report bound to the actual executable SHA and compiled source identity; do not rebuild the candidate to change certification facts.
Non-goals: public release/tag/merge, connector or distributed maturity upgrade, EV-3 benchmark changes, user research, checkpoint/DLQ execution changes
Data semantics: candidates immutable after build; reports external and fail closed on missing/failed/skipped/stale/mismatched evidence. Publication loads/uploads the preserved assets, then checks registry digest/config ID and executable hash. Report failure changes readiness evidence only, not delivery state or secrets.
Rollback: keep original assets and reports; reverting the workflow cannot certify rebuilt bytes using a previous report. Legacy manifests remain readable as historical evidence.
Acceptance: 1) build once and hash all advertised assets; 2) two primary paths, seven tagged path files and full 16-script matrix use actual candidate selectors; 3) P1 regressions, freshness/provenance, incomplete/mutated artifact rejection; 4) same-byte load/promotion receipt using an isolated registry and future release workflows; 5) final PR aggregated gate success; 6) no public release, external research remains blocked
Evidence: ga-evidence.md；run 45711062461 source + exact-candidate + Delivery Passed success（36/36 jobs）；23-case certificate、十个 archive/checksum hashes、config sha256:5ce9794ad47c434b…/binary 992c4214…、local push/pull digest af6c9f35…；12 Python rejection tests + checker -race，strict 7 paths / 16 records exit 0。实际 source/date/hash/receipt 原样保存。
Result: delivered
Residual/follow-up: no sixth increment in this window; external user research only
