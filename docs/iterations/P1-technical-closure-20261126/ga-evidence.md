# GA-C1 候选资产与认证证据

Round: 5/5
Roadmap item: GA-C1
Profile/path: release assets / standalone connector paths
Result: active

当前冻结的实现提交：`1388b5d892c818bb31b9d0dfc30d128990916222`。
最新实际候选 run：[45702765271](https://github.com/a8851625/openetl-go/actions/runs/45702765271)。
1388b5d 的候选认证已通过（23/23），本地 registry receipt 已保存。独立 checker 的绝对路径/零文件拒绝及 push 摘要回执修正仍需在更新后的 PR aggregate 复验，GA-C1 保持 active。

## 验收矩阵

| Criterion | Evidence | Result | Residual |
| --- | --- | --- | --- |
| 构建一次，保留 default/extism 十个归档、镜像配置/executable/checksums | ga-candidate.json；CI build 139516859118 success | passed | 5 OS/arch × 2 variants；无发布后重建 |
| 16-script matrix、两条 primary、七个 tagged JSON 消费候选 | ga-certification.json、ga-case-*.json、docs/evidence/*.json | passed | 完整 23-case report（含 probes） |
| 三个 P1 UI probes、external-readiness binding 与拒绝边界 | 14+9+21 UI checks、3 readiness checks；12 Python rejection tests + checker race | passed | strict 现在明确读取绝对路径七个文件，零文件阻断 |
| 同字节 load/tag/push/immutable pull，registry digest 对账 | ga-promotion.json / ga-promotion.txt | passed | config + executable + rootfs diff IDs；实际 push digest 单独保存 |
| 最终 PR source + exact-candidate aggregate success | PR #13；新的 evidence/validator commit 待 run | active | 原 run 45702765271 仅旧 checked-in evidence source gate failed；候选认证 success |
| 技术与研究验收分离 | ROADMAP / 本迭代 spec | passed | UI-E 外部研究继续 blocked_external；无 distributed/maturity 升级 |

## 候选与晋升身份

- Source: `1388b5d892c818bb31b9d0dfc30d128990916222`；builder Go 1.24.13 / GoReleaser 1.26.2。
- Image configuration: `sha256:708cacb9b1244842793ee8fd0cdd9c65fa74f9524ef560bc5128225e2045fa31`。
- Docker execution index: `sha256:ce4ac50d4171c9a571f3d9528f383dd580b85601796e4de1fa2c29d3b2680c33`。
- Executable: `98c36fd7580a6e2e5d2b33285c615dc6ca81040bd30d69b2146bdb6c90e5a7f2`。
- Actual pushed registry reference: `127.0.0.1:41625/openetl-p1@sha256:a22ea6c19e88069827ab32683da348ed7747c22d759a35b85edcafba2aee2105`。
- Test-only promotion: 2026-11-25T21:44:15Z，配置与 executable SHA 均一致，exit 0。
- 原始 candidate ledger SHA、十个 archive/checksum SHA、每项实际窗口与 raw artifact SHA 见 ga-candidate.json / ga-certification.json。

## 已取得的技术验证

- `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s hack -p test_release_candidate.py -v`：
  `Ran 12 tests ... OK`，exit 0。覆盖归档/校验文件/原始路径及 probe 产物变更、错误 binary、
  不完整/失败/跳过 case、执行窗口过期、错误 release source 和不安全归档路径。
- `go test -race -count=1 ./internal/etl/storage/backup/... ./internal/etl/storage/sqlstore/...`：
  `ok .../storage/backup 32.345s`、`ok .../storage/sqlstore 4.277s`，exit 0，Go 1.24.13。
- SQLite container targeted conformance：`ok .../storage/backup 1.964s`，exit 0。
  MySQL/PostgreSQL 的隔离 `e2e-backup-restore-*.sh` 各自 exit 0，包含新增 NULL/JSON shape
  fidelity、v1/v2 compatibility、known recovery state、故障原子回滚、加密引用与独立 CLI。
- 三 backend `hack/e2e-backup-volume.sh` 各自 exit 0：每个 backend 的 DLQ/audit/run 各
  100037 行，2003 条 terminal/orphan task，逐字段校验与独立 raw SQL before/after hash 相同。
- actionlint v1.7.7 对五个修改的 workflows exit 0；GoReleaser 1.26.2 config check exit 0。
- 保存归档里的配置后，`8c43c8c` 的既有 release-style Darwin/arm64 executable 在隔离目录
  启动返回 HTTP 200。该项仅验证 extraction/startup，不替代最新候选的完整 path certification。

## 失败记录与修正范围

| Actual run | Result | Finding / follow-through |
| --- | --- | --- |
| 45691291665 | failed | job-level env 不接受 runner.temp；11c63e9 修正 context，actionlint passed |
| 45692007631 | failed | 旧路径 JSON stale；恢复新增 nullable recovery columns 在 backup 中被归一化；冷 runner 缺本地 ClickHouse/MinIO fixture；Docker 重启改变临时端口；tagged extraction 漏归档 config |
| 45694597383 | cancelled after replacement | 修改后的三 backend source gates、Go race/integration、前端和三个真实 P1 UI probes 已 success；旧 candidate tagged extraction 不再继续冒充最终候选 |
| 45696570080 | failed | 同一候选 17 个 shell/primary、七个 tagged path 与三个 P1 UI probes success；S3 的固定公共服务端镜像也返回 401。固定 MinIO/mc 源码 fallback 已在本机构建，配置身份规范化解决 Docker 29 index / Podman config 差异。 |
| 45695202813 / 45695689947 | cancelled after replacement | 实现变化后停止重复候选；保留其已上传结果，不计为新候选验收 |

原始输出在对应 GitHub run artifacts；本机调试输出位于 `/private/tmp/openetl-p1-r5-*`。
修复原始 SQL fidelity 时保留旧 fingerprint 检查；修复测试配置时从同一 hashed archive 提取，
没有重建或替换 executable。Readiness probe 只调用真实 API，不引入浏览器或数据库 fixture 前置。

Docker 29→Podman 的实际载入已按保存文件中的配置摘要和 executable SHA 对账通过。
原始 dcd ledger 未被改写，该次是 load integration，不是完整 certification。ARM VM 下
AMD64 Go 程序触发 QEMU lfstack 运行时错误；全路径认证由原生 AMD64 CI 执行。
本机只将跨引擎载入/registry/config+binary 验证计入该范围。

## 数据语义与回归面

候选输出 immutable；测试结果在镜像外绑定 source、binary/image、artifact hashes 与实际窗口。
外部证据读取失败、错 binary/source 或过期时 readiness fail closed。Embedded history 不能认证另一个
BuildCommit。此链路不改变 checkpoint/DLQ 的 at-least-once delivery，不提高 connector maturity。

补齐 SQL NULL/JSON backup fidelity 仅影响维护导出/恢复；runtime DLQ 读写不填充 supplemental
backup fields。冲突 metadata 使 restore 原子失败；原数据、已知 association 与 sink acknowledgement
均由三 backend regression 保持。Rollback 保留新表/数据；早期 v1/v2/v3 备份仍可读。

## 变更清单（本增量）

| File | Why / criterion |
| --- | --- |
| `.github/workflows/_candidate.yml` | build/certify/preserve/promote 与 skip/cancel 聚合阻断（#1–5） |
| `.github/workflows/_gate.yml` | build/certify/preserve/promote 与 skip/cancel 聚合阻断（#1–5） |
| `.github/workflows/release-beta-container.yml` | build/certify/preserve/promote 与 skip/cancel 聚合阻断（#1–5） |
| `.github/workflows/release.yml` | build/certify/preserve/promote 与 skip/cancel 聚合阻断（#1–5） |
| `.github/workflows/test.yml` | build/certify/preserve/promote 与 skip/cancel 聚合阻断（#1–5） |
| `.gitignore` | 隔离真实验证输出，避免输入自污染（#1/#5） |
| `.goreleaser.yml` | 只构建一次，保留平台/variant，stamp source（#1） |
| `docs/ROADMAP.zh.md` | 交付契约、运行要求、claim/close 与残余证据（#1–6） |
| `docs/candidate-release.md` | 交付契约、运行要求、claim/close 与残余证据（#1–6） |
| `docs/iterations/P1-technical-closure-20261126/plan.md` | 交付契约、运行要求、claim/close 与残余证据（#1–6） |
| `docs/iterations/P1-technical-closure-20261126/tasks.md` | 交付契约、运行要求、claim/close 与残余证据（#1–6） |
| `docs/ops-runbook.md` | 交付契约、运行要求、claim/close 与残余证据（#1–6） |
| `docs/release-checklist.md` | 交付契约、运行要求、claim/close 与残余证据（#1–6） |
| `hack/Dockerfile.minio-fixture` | 固定源码 S3 fixture，真实执行既有脚本（#2） |
| `hack/candidate_readiness_e2e.py` | 实际 executable-bound 外部证据与错误证据 fail closed（#3） |
| `hack/cmd/backup-volume-fixture/main.go` | NULL 与原始 SQL shape 的独立输入/逐字段对账（#3/#5） |
| `hack/cmd/check-connector-evidence/main.go` | 真实 source/provenance 与 evidence freshness（#3/#5） |
| `hack/e2e-cdc-mysql.sh` | 消费明确候选 IMAGE，修正 acknowledgement 等待边界（#2） |
| `hack/e2e-cdc-postgres.sh` | 消费明确候选 IMAGE，修正 acknowledgement 等待边界（#2） |
| `hack/e2e-clickhouse-replay-ordering.sh` | 消费明确候选 IMAGE，修正 acknowledgement 等待边界（#2） |
| `hack/e2e-clickhouse.sh` | 消费明确候选 IMAGE，修正 acknowledgement 等待边界（#2） |
| `hack/e2e-doris.sh` | 消费明确候选 IMAGE，修正 acknowledgement 等待边界（#2） |
| `hack/e2e-http-source.sh` | 消费明确候选 IMAGE，修正 acknowledgement 等待边界（#2） |
| `hack/e2e-kafka-multitable-clickhouse.sh` | 消费明确候选 IMAGE，修正 acknowledgement 等待边界（#2） |
| `hack/e2e-kafka-raw-ods.sh` | 消费明确候选 IMAGE，修正 acknowledgement 等待边界（#2） |
| `hack/e2e-kafka.sh` | 消费明确候选 IMAGE，修正 acknowledgement 等待边界（#2） |
| `hack/e2e-mysql-postgres.sh` | 消费明确候选 IMAGE，修正 acknowledgement 等待边界（#2） |
| `hack/e2e-s3-minio.sh` | 消费明确候选 IMAGE，修正 acknowledgement 等待边界（#2） |
| `hack/e2e-snapshot-cdc-clickhouse.sh` | 消费明确候选 IMAGE，修正 acknowledgement 等待边界（#2） |
| `hack/e2e-snapshot-cdc.sh` | 消费明确候选 IMAGE，修正 acknowledgement 等待边界（#2） |
| `hack/e2e.sh` | 消费明确候选 IMAGE，修正 acknowledgement 等待边界（#2） |
| `hack/ensure-minio-fixture.sh` | 固定源码 S3 fixture，真实执行既有脚本（#2） |
| `hack/health_ui_e2e.py` | 真实 Docker restart 后重新获取 endpoint（#3） |
| `hack/release_candidate.py` | 保存/验收 config、binary、archive、case 与 registry identities（#1–5） |
| `hack/test_release_candidate.py` | 缺失/失败/过期/篡改/错误 source/config 拒绝（#3/#4） |
| `internal/consts/consts.go` | 编译 source identity（#1/#3） |
| `internal/etl/e2e/harness/candidate.go` | 拒绝替代编译，绑定实际 executable 与原始 evidence（#2/#3） |
| `internal/etl/e2e/harness/candidate_test.go` | 拒绝替代编译，绑定实际 executable 与原始 evidence（#2/#3） |
| `internal/etl/e2e/harness/evidence.go` | 拒绝替代编译，绑定实际 executable 与原始 evidence（#2/#3） |
| `internal/etl/e2e/harness/server.go` | 拒绝替代编译，绑定实际 executable 与原始 evidence（#2/#3） |
| `internal/etl/server/evidence_binding_test.go` | runtime report/source/executable binding 与历史降级（#3） |
| `internal/etl/server/evidence_manifest.go` | runtime report/source/executable binding 与历史降级（#3） |
| `internal/etl/storage/backup/conformance_test.go` | backup-only NULL/JSON fidelity，三 backend 与原子失败验证（#3/#5） |
| `internal/etl/storage/sqlstore/backup.go` | backup-only NULL/JSON fidelity，三 backend 与原子失败验证（#3/#5） |
| `internal/etl/storage/sqlstore/backup_reader.go` | backup-only NULL/JSON fidelity，三 backend 与原子失败验证（#3/#5） |
| `internal/etl/storage/storage.go` | backup-only NULL/JSON fidelity，三 backend 与原子失败验证（#3/#5） |

`hack/cmd/check-connector-evidence/main_test.go` 补充绝对路径实际读取、失败用例拒绝与 strict 零文件测试（#3/#5）。

后续 `ga-*` 证据只保存真实 command/run/ledger/hash/receipt；七个 path JSON 与 connector manifest 用实际运行结果更新（#2–5）。

## 交接边界

本轮不能领取第六个 increment。外部研究的缺失输入仍为 3–5 名目标用户参与者；技术证据不能
替代研究。EV-3 与运行历史 P2 不在本窗口。公共 tag/release/merge 不属于本次验证范围。

VERDICT: REWORK — 所有候选与晋升验收已通过；最后的 validator/evidence 更新需要新的 PR aggregate success。
