# IT-9 / EV-3 最终验收

Round: **5/5**；EV-3.1–.4 与有界 MySQL fixture health 补验收全部 `delivered`。
冻结实现：`8dfbf8438dafd8568a215464fd8c96acb5618b2e`；完整 CI：
[46032636182](https://github.com/a8851625/openetl-go/actions/runs/46032636182)，**36/36 jobs success**，包含 source、exact-candidate 和 Delivery Passed。
证据收口后仅更新列明 bookkeeping，PR #14 最新 HEAD 由同一完整 gate 再执行。

| Criterion | Evidence (command/file/run) | Result | Residual or blocker |
| --- | --- | --- | --- |
| A1 v3 config/archive hashes、归档大小、所选层展开 bytes 与 engine 原始诊断 | `archive-fixtures.md`、当前 `baseline-v3.json`；当前 CI 实际重算四份原始 archive；初始完整本地 raw archive recheck 记录在 git e72f9fe | passed | `uncompressed-layer-bytes/v1`，engine.Size 只诊断 |
| A2 四 tag variants、default native production/SQLite/TLS/UID1001/audit/2CPU/1GiB/GOMAXPROCS2、3 trials/16×100 和恢复对账 | 当前 `baseline-v3.json`：4 variants、3 cold/3 restore、15 RSS samples、1600 records、16 checkpoint/identity/executable SHA 对账 | passed | native Linux/amd64 packaging/startup/restore；独立 ARM64 校准另存；不认证吞吐/distributed/全部发布资产容量 |
| A3 相同保存镜像在真实两个 provider 的 config/diff IDs/展开层 bytes 一致 | `docker-image-metrics.json`、`podman-image-metrics.json`、`conformance.json`；当前 CI actual comparison exit0 | passed | Docker29.4.2 / native Podman5.7.0；image-only probe 不启动 app |
| A4 v2只读、v1拒绝；v2/dirty/missing method/policy 不能 enforce | `r3-cli-evidence.json` actual historical v2；最终38 tests的compatibility/dirty/selector/method cases | passed | 原历史 SHA 不变；not_applicable/not_evaluated 不伪称 budget pass |
| A5 观测超限0、size enforce超限1；测量错误任何模式失败 | `budget-low-enforce.json`、`budget-low-observe.json`：同一真实147265024 bytes对limit147265023；data_valid true；当前CI明确检查1/0；38 tests两模式负向 | passed | 受控复制policy，不修改测量；未知错误不能满足negative assertion |
| A6 versioned/method/unit/arch/default policy、两个稳定体积项；time/RSS warning | `hack/bench-budgets.json` version2026-11-27.1、SHA42871889...；`budget-enforce.json`与budget-decision.md的两个clean native校准/25%余量 | passed | AMD64 binary/image 81/176MiB；ARM64 77/169MiB；其他variants完整观测 |
| A7 当前source/三storage/frontend/exact-candidate/Delivery Passed全绿、无新allowlist | 当前 `ci.json` 36success；`candidate.json`/`certification.json` 10binaries、23/23cases；7tagged/28checks；source/candidate聚合依赖逐字对账 | passed | 当前CDC-MySQL真实通过；没有skip/cancel-as-pass |
| A8 Env/私钥/payload不进入报告、自有cleanup失败可见 | `r4-secret-check.json`：当前23份原始verification members CRC/SHA/noEnv/key；历史191683e的454份full metadata/log扫描与四image-env checks；当前38 sentinel/lifecycle tests；全部cleanup passed | passed | 当前小成员校验范围明确；private fixture env/key/backup不上传；raw archive retention14d |
| A9 doc/policy/source/harness/原始证据一致；停止enforce仍验证measurement | source-merge.json的c3c4d8b2... parent含8dfbf84；raw source/harness/policy对账；当前strict source/config与7paths exit0；rollback负向、r4-user-changes.json/final diff | passed | policy/runtime/script/workflow/calibration变更仍需重新认证；仅具体bookkeeping允许descendant |

## 实际验证证据

- 最终 Python 日志：本机与 Linux Python3.12 均 `Ran 38 tests ... OK`，`[exit code: 0]`；当前 CI 同一 suite passed。
- Go1.24.13 checker `-race`：`ok .../hack/cmd/check-connector-evidence 5.541s`，exit0；actionlint1.7.7、shell syntax和YAML parse exit0。
- 当前 CI actual v3：normal selected2 enforce0、one-byte-low enforce1、same observation0，method/source/archive完整校验仍必过。
- 当前候选 certified job 在原始保存 bytes 上实际执行 collect/verify，23cases、7tagged/28checks、16connector records通过。
- 从同一immutable artifact ZIP获取verification members，成员CRC与ledger/certificate/manifest摘要实际相等；初始full download与range读的12成员逐字相等已经实测。
- 当前本地 strict：`path evidence OK: 7 file(s)`、`connector evidence manifest OK: 16 record(s)`，source8dfbf84/config d890d019...，exit0。
- `r5-mysql-health-probe.json`：真实临时 mysqld socket SELECT1 exit0、TCP SQL/health非零且not healthy；最终TCP SQL/health exit0/healthy；自有container/volume cleanup passed。

## 原始资产与身份

- 当前 resource artifact：run46032636182 的 `resource-baseline-46032636182`（artifact15215048784），原始4-variant和2-provider archive、完整日志/fixture/source；metadata receipt记录同一ZIP字节和成员SHA。
- 当前 exact candidate artifact：同 run 的 `candidate-certified-46032636182-1`（artifact15215294387），10个raw binaries/归档/checksum、所有case logs、7tagged JSON及immutable bundle；完整asset verification在CI实际执行。
- baseline真实source merge：`c3c4d8b2cdc845785b8320573fe67b99659111c9`，parent含branch8dfbf84；inventory SHA `7a6dab1d...`；未改写为candidate branch。
- candidate真实source：`8dfbf8438dafd8568a215464fd8c96acb5618b2e`；config `sha256:d890d019...`；executable `d8ebad8021...`；所有完整SHA在ledger/certificate。
- default Dockerfile baseline仍为67,285,176 binary bytes、147,265,024 expanded layer bytes，config `ff850b98...`；它和GoReleaser候选分别声明profile。
- 原始191683e的本地full archive/asset verification、454-log扫描及相关JSON在git **e72f9fe** 和run46024237806原样保留；当前metadata替换为新版，不改写原历史或两个policy校准文件。

## 变更清单与回归面

| Files | Why / criterion |
| --- | --- |
| hack/bench_baseline.py、hack/test_bench_baseline.py | streaming archive/v3/compatibility/source/profile/integer checks、policy/audit、退出码、安全/cleanup；A1–A6/A8/A9 |
| hack/e2e-bench-image-metrics.sh、hack/bench-budgets.json | actual provider probe和versioned size policy；A3/A6 |
| _gate.yml resource job | 四variants/default restore、actual conformance、explicit size canary、受控negative/rollback、bounded artifacts；A2/A3/A5–A9 |
| checker main.go/main_test.go | 仅指定IT-9 bookkeeping descendants；policy/runtime/script/workflow/calibration仍拒绝；A7/A9 |
| e2e-doris-source-order.sh、docker-compose.dev.yml mysql-source health | 暂时socket server不可满足真实TCP readiness；修正mandatory candidate初始化竞态；A7/A9 |
| canonical connector manifest与既有7份docs/evidence JSON | 从同一实际新候选回填fresh source/image/window/digests；A7/A9；不升maturity |
| resource-baseline/release-checklist、IT-9三件套/evidence、ROADMAP/index | 方法/profile/预算、claim/close、失败/回滚与current evidence；A9 |

回归覆盖 formats/selection/digests/limits/links、v2历史、4tags、native runtime/restore、两个budget modes、cleanup、strict binding、source backends/frontend和exact candidate。
Source→Transform→Sink、at-least-once/checkpoint/DLQ/sink ack、migration与connector maturity不变。
未merge、public release/tag、用户研究、CH-C5或第六个增量；本迭代没有mandatory blocker。
六份用户其他文件byte-identical，资源文档原有增删行逐字保持并仍留working tree。

失败历史：46017295606的native Podman shape/Doris初始化；46023663634被类型修正替代/取消；46029555087真实CDC-MySQL初始化socket失败及aggregate failure。均保留且不计pass。当前46032636182全部通过。

VERDICT: DELIVERABLE
