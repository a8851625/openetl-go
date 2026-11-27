# IT-9 / EV-3 验收记录

Round: 4/5；EV-3.1–.4 `delivered`；第五轮缓冲未使用。
冻结实现：`191683e4a2ddcd65a7a5c7463d9c6708892ef3c8`；实际完整 CI：
[46024237806](https://github.com/a8851625/openetl-go/actions/runs/46024237806)，**36/36 jobs success**，含 source、exact-candidate 和 Delivery Passed。
后续只回填指定 evidence/bookkeeping；PR #14 的最新 HEAD 检查由同一完整 gate 再执行。

| Criterion | Evidence (command/file/run) | Result | Residual or blocker |
| --- | --- | --- | --- |
| A1 v3 配置/归档 SHA、大小、所选层展开字节、方法/单位与 engine 诊断 | `archive-fixtures.md`；`baseline-v3.json`；下载四份原始 archive 后当前 `--check --budgets --enforce-budgets` 重算 exit 0 | passed | 定义为层 tar 流字节之和；legacy engine.Size 不作预算 |
| A2 四变体、default native production/SQLite/TLS/UID1001/audit/2CPU/1GiB/GOMAXPROCS2、3 trials/16×100 恢复对账 | 当前 `baseline-v3.json`：四变体、3 cold/3 restore、15 RSS samples、1600 records、16 checkpoints/identity；真实源码 inventory 和 process executable SHA | passed | native Linux/amd64 packaging/startup/restore；独立 ARM64 校准另存，不是 throughput/distributed/全发布资产容量认证 |
| A3 同一镜像在真实 Docker/Podman 的 config/diff IDs/展开字节一致 | `docker-image-metrics.json`、`podman-image-metrics.json`、`conformance.json`；当前 CLI 重新读取两份原始 archive，exit 0 | passed | Docker 29.4.2 / Podman 5.7.0 daemonless，所选 linux/amd64；image-only，probe 不启动应用 |
| A4 v2 只读/v1拒绝，v2/dirty/missing method/policy 禁止 enforce | `r3-cli-evidence.json` 的实际历史 v2 与负向；最终 `r4-python-tests.log` / `r4-python312-tests.log` 各 38 tests | passed | 原始 v2 SHA 不变；v3 未适用或未指定 policy 不伪称预算通过 |
| A5 warning 0/enforce 1；错误或缺失测量任何模式失败 | CI `budget-low-enforce.json`：真实 147265024 bytes 对 147265023 bytes，data_valid true、failed、exit 1；同一 `budget-low-observe.json` warning、exit 0；38 tests 的两种模式错误输入 | passed | 受控低预算复制的是 policy，不修改 baseline；未知错误不能满足 negative assertion |
| A6 versioned/method/unit/arch/default policy，两个稳定项灰度；time/RSS warning | `hack/bench-budgets.json` version 2026-11-27.1、SHA `42871889…`；`budget-enforce.json`；`budget-decision.md` 的两个 clean native 校准与 25% 余量 | passed | default AMD64 81/176 MiB；ARM64 77/169 MiB；其他变体完整观测，时间/RSS仍 warning |
| A7 source/三 storage/frontend/exact-candidate/Delivery Passed 全部通过，无新 allowlist | `ci.json`：36 jobs success；`candidate.json` / `certification.json`：10 binaries、23/23 mandatory cases；7 tagged paths / 28 checks；current source/candidate gate dependencies 与 P1 原版本逐字对账 | passed | 冻结 source/candidate 各有真实 ref；静态检查与真实各 backend 证据不互相替代 |
| A8 Env/私钥/payload 不进入报告，cleanup 失败可见 | `r4-secret-check.json`：454 份实际 metadata/log clean，四个 image config 无临时 runtime token/key；baseline/两 probe cleanup passed；38 tests 的秘密哨兵、malformed archive、cleanup 和目录负向 | passed | 原始 archive 仅保留 immutable image 内容；临时 env、TLS私钥、backup/output fixture payload 在 private dir；raw archive CI retention 14d |
| A9 文档/policy/source/harness/原始证据一致，回滚仍验证测量 | `source-merge.json` 的真实 merge `1f5b1d6e…` parent 含 `191683e…`；harness/source inventory/policy SHA 实际对账；当前 strict source/config + 7 paths exit 0；`r4-user-changes.json` / final diff 检查 | passed | 后续只允许列明 bookkeeping；policy/代码/脚本/workflow/calibration 变更仍需重新认证；停止 enforce 后 wrong method 仍 exit 1 |

实际验证输出：

- Python 最终两份日志：`Ran 38 tests ... OK`，`[exit code: 0]`，本机和 Linux Python 3.12。
- Go 1.24.13：`ok github.com/a8851625/openetl-go/hack/cmd/check-connector-evidence 5.541s`，`[exit code: 0]`；actionlint v1.7.7 exit 0；shell syntax 与 diff checks exit 0。
- 当前 raw baseline recheck：`Measurements valid; size budgets passed; observation warnings: 0`，exit 0。
- 当前 raw archive comparison：`Image-only conformance passed`，exit 0。
- 候选完整 bytes/result recheck：`Candidate assets and mandatory results verified`，exit 0。
- strict：`path evidence OK: 7 file(s)`、`connector evidence manifest OK: 16 record(s)`，冻结 source/config 绑定，exit 0。

## 原始证据位置与身份

- Resource artifact：run 46024237806 的 `resource-baseline-46024237806`，原始四变体和两份 engine archive、source/fixture/command logs；本机 `/private/tmp/openetl-it9-r4-ci-baseline/`。
- Exact candidate artifact：同 run 的 `candidate-certified-46024237806-1`（artifact 15211723900），十个归档/原始 binary、校验文件、全部 case logs、七个 tagged JSON 和 immutable bundle；本机 `/private/tmp/openetl-it9-r4-candidate/`。
- Native baseline source：真实 PR merge `1f5b1d6e8187a789c70823c2f6dc45c1d7d81de8`；build-input SHA `1d8d2668…`，未改写为 branch SHA。
- Candidate source：`191683e4a2ddcd65a7a5c7463d9c6708892ef3c8`；config `sha256:1102254a…`；executable `b4fed8a6…`；全部完整 SHA 在原始 ledger/certificate。
- 本轮 default Dockerfile baseline：67,285,176 binary bytes；147,265,024 uncompressed layer bytes；config `c84fa8a1…`。它和 GoReleaser candidate 是分别声明的测量/认证 profile。

## 变更清单与回归面

| Files | Purpose / criterion |
| --- | --- |
| `hack/bench_baseline.py`、`hack/test_bench_baseline.py` | archive parser、v3 writer/reader、source/profile/整数检查、policy/audit、退出码、安全/cleanup；A1–A6/A8/A9 |
| `hack/e2e-bench-image-metrics.sh`、`hack/bench-budgets.json` | 实际单 provider probe 与 versioned size policy；A3/A6 |
| `.github/workflows/_gate.yml` | 完整 v3、实际双引擎、显式 size enforcement、受控低预算/rollback 与 bounded archives；A2/A3/A5–A9 |
| `hack/cmd/check-connector-evidence/{main.go,main_test.go}` | 只允许指定 IT-9 bookkeeping descendant，policy/runtime/script/workflow/calibration 仍拒绝；A7/A9 |
| `hack/e2e-doris-source-order.sh` | 初始化临时 mysqld 不可满足 TCP readiness；原 mandatory path 真正通过；A7 |
| `internal/etl/server/evidence/connector-evidence.json`、既有七份 `docs/evidence/*.json` | 拷贝同一真实候选的 fresh canonical evidence；A7/A9；无 maturity upgrade |
| `docs/resource-baseline.md`、`docs/release-checklist.md`、IT-9/ROADMAP/index | profile、方法、预算、失败/回滚、claim/close 和完整证据；A9 |

回归检查覆盖 archive format/selection/hash/limits/links、v2历史读取、四 tag variants、native runtime/restore、两种预算模式、metadata/lifecycle cleanup、strict source binding、source backend/frontend 和 exact candidate。
本轮不修改 Source→Transform→Sink、checkpoint、DLQ/replay、sink ack、schema migration 或 connector maturity。
没有 merge、public release/tag、用户研究或 CH-C5 实现；不存在本迭代 mandatory blocker。
六份用户其他文件 byte-identical，资源文档原有增删行逐字保持并仍留在 working tree。

失败记录保留：run 46017295606 的 native Podman version shape / MySQL init readiness 及其 aggregate failure；run 46023663634 在类型补查后被替换/取消，不计 final pass。

VERDICT: DELIVERABLE
