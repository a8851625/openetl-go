# 领取与验收记录

| Round | Increment | Status | Evidence |
| --- | --- | --- | --- |
| 1/5 | EV-2 | done | Go 1.24.13 + 隔离 MySQL 8：3 个 targeted -race tests passed；1000 行字段对账通过；PR run 45652762125 / integration job 139341305228 success。 |
| 2/5 | P5-H1 | active | 已核对 telemetry、metrics、UI health 和四个 UI 入口；开始实现。 |
| 3/5 | PR-0-C1 | todo | 待领取。 |
| 4/5 | UI-E-R1 | todo | 用户已授权技术修复；外部研究继续缺参与者。 |
| 5/5 | GA-C1 | todo | 依赖最终运行时代码冻结。 |

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
Evidence: 待执行
Result: active
Residual/follow-up: PR-0-C1

## 证据规则

每轮记录 pass/fail/skip/block 与 exact command/run。
没有目标用户研究记录时，UI-E 整体不置 delivered。
到 Round 5/5 后只完成当轮安全收口，后续需要新的用户指令。
