# 领取与验收记录

| Round | Increment | Status | Evidence |
| --- | --- | --- | --- |
| 1/5 | EV-2 | active | Go 1.24.13 容器 + 隔离 MySQL 8：3 个 targeted -race tests passed；1000 行字段对账通过，写入 21.160856ms。CI 待执行。 |
| 2/5 | P5-H1 | todo | 待领取。 |
| 3/5 | PR-0-C1 | todo | 待领取。 |
| 4/5 | UI-E-R1 | todo | 用户已授权技术修复；外部研究继续缺参与者。 |
| 5/5 | GA-C1 | todo | 依赖最终运行时代码冻结。 |

## Round 1 Claim

Round: 1/5
Roadmap item: EV-2
Profile/path: standalone / mysql sink integration CI
Objective: 1000-row smoke 验证提交后的记录与字段，新的 integration/aggregated CI 门禁通过
Scope: internal/etl/sink/mysql_test.go、对应 workflow 验证和 roadmap/迭代证据
Non-goals: 性能预算升级、sink 写入语义变化、发布新版本
Dependencies: 隔离 MySQL、项目 Go 1.24 工具链、GitHub PR CI
Acceptance: 1) 1000 行完整对账；2) 真实 MySQL targeted test；3) CI integration 与 aggregated gate success
Evidence: etl-go-dev:latest（Go 1.24.13 linux/arm64）；隔离 openetl-p1-mysql；go test -race -count=1 -run '^TestMySQL(BenchmarkRealDB|BuildBatchInsertSQL|BuildBatchDeleteSQL)$' -v ./internal/etl/sink → PASS，exit 0；CI 待执行
Result: active
Residual/follow-up: 验证后领取 P5-H1

## 证据规则

每轮记录 pass/fail/skip/block 与 exact command/run。
没有目标用户研究记录时，UI-E 整体不置 delivered。
到 Round 5/5 后只完成当轮安全收口，后续需要新的用户指令。
