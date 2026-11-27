# IT-9 / EV-3 验收记录

Round: 4/5；EV-3.1–.3 `delivered`，EV-3.4 `active`。
当前尚未取得修正后的完整 CI；下表明确保留未闭合项，不是迭代交付声明。

| Criterion | Evidence (command/file/run) | Result | Residual or blocker |
| --- | --- | --- | --- |
| A1 v3 所选层规范化字节、config/archive SHA、原始 engine 诊断 | `archive-fixtures.md`、固定 Docker-save/OCI raw/gzip CLI；实际 R2 archive 重算 | passed | 原始归档在 bounded artifact/owned roots，未提交二进制 |
| A2 四 variant、default native production/SQLite/TLS/UID1001/audit/2CPU/1GiB/GOMAXPROCS2、3 trials/16×100 和恢复对账 | `calibration-v3-amd64.json`、`baseline-v3-arm64.json`、两份 source inventories 与 actual check logs | passed at calibration scope | 当前 R4 CI 的完整新 writer/policy baseline 待回填 |
| A3 同一镜像在真实 Docker/Podman 的配置/diff IDs/展开层字节一致 | `r2-conformance.json`、两个实际 provider reports；comparison CLI exit 0 | passed | 修正后的 native daemonless Podman CI probe 待回填 |
| A4 v2 只读、v1拒绝、v2/dirty/missing method/policy 禁止 enforce | `r3-cli-evidence.json`、38 local/Python3.12 tests | passed | 历史原文件 SHA 运行前后相同 |
| A5 warning 0/enforce 1；缺失/失败/错误测量任何模式失败 | `r3-budget-*.json`、12 actual CLI cases、38 tests | passed | 正常/受控低预算的当前 CI 结果待回填 |
| A6 按方法/单位/架构/default 的 versioned policy、两个稳定体积项；time/RSS warning | `hack/bench-budgets.json`、`budget-decision.md`、两个 native real positive CLI verdicts | passed | 当前 CI canary 待验证 |
| A7 source/三 storage/frontend/exact-candidate/Delivery Passed 全部通过，无新 GATE_ALLOWLIST | 初始 `r2-ci.json` run 46017295606 有真实失败；已按 plan 修正 native provider 与 MySQL fixture TCP readiness | pending | 修正后完整 PR run 必须全绿，不能以旧局部 pass 代替 |
| A8 Env/私钥/payload 不进报告；任务资源 cleanup 可见 | 38 tests 的秘密哨兵/异常归档/cleanup/输出目录负向；两个 native calibrations cleanup passed | passed at local/calibration scope | 当前 CI artifact metadata/secret/cleanup 检查待回填 |
| A9 文档/policy/source/harness/原始证据一致；回滚仍校验测量 | `r3-cli-evidence.json` 的 rollback-retains-validity；policy/calibration SHA 对账；资源文档选择性 index | pending | 当前 raw CI/candidate 与 final diff/strict descendant 检查待回填 |

变更清单和回归面按 Round 1–3 的验收文档及 `tasks.md` claim 记录；当前 R4 仅触及
resource job、严格 checker 的指定 bookkeeping paths、Doris fixture startup readiness 和相关文档。
原始 at-least-once/checkpoint/DLQ、connector maturity、distributed、CGO、发布配方和 public release scope 不变。

VERDICT: PARTIAL
