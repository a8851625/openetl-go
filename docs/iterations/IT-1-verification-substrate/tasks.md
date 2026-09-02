# IT-1 任务分解

> 状态取值：`todo` / `active` / `done` / `blocked`。一次只允许一个 `active`。
>
> 领取时同步把 ROADMAP 对应条目置 `active`，收口时回填其验收矩阵。

## Round 分组

| Round | 包含 task | 目标 | 可独立发版 |
| --- | --- | --- | --- |
| 1/5 | T1.1、T1.2 | 发布门禁生效：未过门禁的 tag 不能出产物 | 是 |
| 2/5 | T1.3、T1.4 | e2e 框架可用，两条主推荐路径在 CI 内实跑 | 是 |
| 3/5 | T1.5 | 证据由测试产出并与 commit 绑定，`LastCertified` 自动填充 | 是 |
| 4/5 | T1.6、T1.7、T1.8 | BUG backlog 清零 | 是 |
| 5/5 | T1.9、T1.10、T1.11、T1.12 | GAP e2e 欠账闭合 + 迭代收口 | 是（T1.1/2/4/5 阻塞待 push） |

## 任务表

| ID | 任务 | 依赖 | 状态 | 证据落点 |
| --- | --- | --- | --- | --- |
| T1.1 | 抽出 `_gate.yml` reusable workflow + `gate-passed` 聚合断言 | — | `blocked` | workflow 文件、故意失败的 run URL |
| T1.2 | release / release-beta-container 接入门禁 | T1.1 | `blocked` | 失败 tag 与正常 tag 两次 run URL |
| T1.3 | e2e harness 骨架（容器、子进程、命名空间、skip 语义） | — | `done` | `internal/etl/e2e/harness/` + harness 单测 |
| T1.4 | 迁移两条主推荐路径并接入 CI | T1.3 | `blocked` | `connector-e2e` job run URL |
| T1.5 | 结构化证据产出 + commit 绑定校验 + `LastCertified` | T1.4 | `blocked` | `docs/evidence/*.json`、篡改证据的失败 run |
| T1.6 | BUG-1 状态与证据一致性核对与订正 | T1.3 | `done` | ROADMAP BUG-1 验收矩阵 |
| T1.7 | BUG-2 三策略容器级闭合 | T1.3 | `done` | ROADMAP BUG-2 验收矩阵 |
| T1.8 | BUG-6 `ColumnTypes` e2e 闭合 | T1.3 | `done` | ROADMAP BUG-6 验收矩阵 |
| T1.9 | GAP-1 / GAP-3 PostgreSQL 实例 e2e | T1.3 | `done` | ROADMAP GAP-1/GAP-3 条目 |
| T1.10 | GAP-4 ES mapping-conflict e2e | T1.3 | `done` | ROADMAP GAP-4 条目 |
| T1.11 | P4 Doris/Kafka 事实核验 | T1.3 | `done` | P4 follow-up 记录（Kafka PASS；Doris 镜像拉取 3 次 EOF 记 blocked） |
| T1.12 | CI 时长调优 + 迭代收口 | T1.1..T1.11 | `todo` | 时长记录、`README.md` 看板、迭代 DoD |

## 任务明细

### T1.1 抽出 `_gate.yml` reusable workflow

**验收**：

1. `.github/workflows/_gate.yml` 存在，`on: workflow_call`，承载原 `test.yml` 全部 job。
2. `gate-passed` 聚合 job 用 `toJSON(needs)` + `jq` 显式断言每个前置 job `result == "success"`。
3. 构造一个 job 被 skip 的场景，`gate-passed` **失败**（验收 2 的实现证明）。
4. `test.yml` 改为 `uses:` 调用，push / pull_request 行为与改造前等价。

**证据落点**：workflow 文件 diff；skip 场景的失败 run URL。

### T1.2 release 接入门禁

**验收**：

1. `release.yml` 与 `release-beta-container.yml` 均新增 `gate` job（`uses: ./.github/workflows/_gate.yml`，
   `secrets: inherit`），发布 job 加 `needs: gate`。
2. 构造单测失败的 tag，release 失败且**无** Release / GHCR 镜像产出。
3. 正常 tag 全流程通过，产物与改造前一致。
4. 门禁结果对应 tag 所指 commit，不受分支后续提交影响。
5. `docs/release-checklist.md` 中人工项改为流水线强制项，豁免清单有逐条理由。

**证据落点**：两次 run URL（失败 tag / 正常 tag）；`release-checklist.md` diff。

### T1.3 e2e harness 骨架

**验收**：

1. `internal/etl/e2e/harness/` 提供：依赖容器懒启动与复用、被测二进制 build/start/SIGKILL/restart、
   按 `t.Name()` 派生的命名空间隔离、结构化 skip 记录。
2. `-e2e.strict` 标志下任何 skip 视为失败；不带该标志时本地可正常 skip。
3. podman rootless 下可运行（`DOCKER_HOST` + `TESTCONTAINERS_RYUK_DISABLED` 已文档化）。
4. `go list -deps ./...` 中不含 testcontainers（`e2e` build tag 隔离生效）。
5. harness 自身有单测覆盖命名空间派生与 skip 语义。

**证据落点**：`internal/etl/e2e/harness/*_test.go`；`go list -deps` 输出。

### T1.4 迁移两条主推荐路径

**验收**：

1. MySQL CDC → MySQL upsert：happy path、sink 故障 → DLQ → replay、
   SIGKILL 后按 checkpoint 续跑，三项断言通过。
2. MySQL snapshot+CDC → ClickHouse：happy path、checkpoint reset replay 被
   ReplacingMergeTree 吸收、snapshot/CDC 交接点崩溃恢复，三项断言通过。
3. 断言集合与 `hack/e2e-path-mysql-cdc-mysql.sh`、`hack/e2e-snapshot-cdc-clickhouse.sh`
   逐条比对，**无放宽**；差异需在 PR 说明。
4. `connector-e2e` job 接入 `_gate.yml`，在 CI 内实跑通过。

**证据落点**：CI run URL；断言比对表（迁移前 shell 断言 → 迁移后 Go 断言）。

### T1.5 证据产出与 commit 绑定

**验收**：

1. 每条路径运行后产出 `docs/evidence/<path_id>.json`，含 `path_id`、`commit`、
   `run_started_at`、`runner`、依赖版本、逐项 `checks[]` 与总 `result`。
2. `hack/check-connector-evidence.sh` 校验证据 `commit` 为 HEAD 或其祖先，且不早于该路径
   相关源码的最近改动；手工篡改证据而不重跑 → 校验失败。
3. `PathContract.LastCertified` 由证据派生，CI 运行后自动变化且与 run 的 commit 一致。
4. CI 门禁按 **live 运行结果**判定，不读已提交的证据文件。

**证据落点**：`docs/evidence/*.json`；篡改证据的失败 run URL；`LastCertified` 前后对比。

### T1.6 BUG-1 状态与证据一致性核对

**先核对，不假设**：该条目标题写「2026-08-23 delivered」，`状态` 却是 `active`，
证据段落又记录容器 e2e 已通过（镜像 `9b887beb`）。三者矛盾，需先判定事实。

**验收**：

1. 核对 `hack/e2e-bug1-varchar-pk.sh` 的证据是否绑定当前 HEAD 的 `mysql_batch` 实现。
2. 一致 → ROADMAP BUG-1 置 `delivered` 并说明依据；不一致 → 在 T1.3 框架内重跑后再置。
3. varchar PK 路径纳入 T1.4 的常驻 e2e 集合，防止再次回归。

**证据落点**：ROADMAP BUG-1 验收矩阵；e2e run 记录。

### T1.7 BUG-2 三策略容器级闭合

**验收**（沿用 ROADMAP BUG-2 原验收 1-5，不改写）：

1. `fail` 策略：binlog 缺失时停止管道 + critical 告警，不无限重试；证据**写入验收矩阵**
   （此前证据存在但未入矩阵，是该条目回退为 `active` 的直接原因）。
2. `resume_from_current`：从当前 master 位点续 CDC，checkpoint 更新，RPO 边界在文档声明。
3. `resnapshot`：从 `last_ids` / `last_strs` 续读后重入 CDC，不重读已读行（**首次端到端验证**）。
4. 瞬时网络断连仍走指数退避，不被误判为 binlog purged。
5. 单测 + 容器级 e2e（`RESET MASTER` 触发）证据齐备。

**证据落点**：ROADMAP BUG-2 验收矩阵；`internal/etl/e2e/path_mysql_cdc_binlog_purged_test.go`。

### T1.8 BUG-6 `ColumnTypes` e2e 闭合

**验收**（沿用原验收 1-3）：

1. CDC 相位 INSERT/UPDATE/DELETE 的 `Metadata.ColumnTypes` 非空且与 canal schema 一致。
2. 经 Kafka → ClickHouse 自动建表使用**声明类型**，不退化为样本值 + name-hint 推断。
3. 单测 + 该路径 e2e 通过。

**证据落点**：ROADMAP BUG-6 验收矩阵；建表 DDL 断言输出。

### T1.9 GAP-1 / GAP-3 PostgreSQL 实例 e2e

**验收**：

1. PostgreSQL 容器配置 `wal_level=logical`，`postgres_cdc` 三个 Metadata 契约在真实实例上验证。
2. `postgres` sink 的 `pk_columns_from_metadata` 覆盖单键、复合键、UPDATE、DELETE。
3. 两项均记录实际命令、镜像版本、执行日期与 passed/failed。

**证据落点**：ROADMAP GAP-1、GAP-3 条目；CI run URL。

### T1.10 GAP-4 ES mapping-conflict e2e

**验收**：

1. index_template 已实现部分回归通过。
2. mapping 冲突按收敛后的策略进入 DLQ 或拒绝，行为与文档一致。
3. ES 的 fan-out 认证按其 index/mapping 语义单独记录，**不复用关系型 sink 的证据**。

**证据落点**：ROADMAP GAP-4 条目。

### T1.11 P4 Doris/Kafka 事实核验

**验收**：

1. 核验 Doris/Kafka 相关文档声明与实际运行行为是否一致，差异订正。
2. 若 Doris 容器超出 CI 时间预算 → 保留 `hack/e2e-doris.sh` 人工路径，显式记为 `blocked`
   并写明 blocker，**不得记为 pass**。

**证据落点**：P4 follow-up 记录；若 blocked 则记录具体资源约束。

### T1.12 CI 时长调优与迭代收口

**验收**：

1. 记录单次全量门禁运行时长；超过 30 分钟则实施分层（PR 快速集 / main-tag 全量），
   分层规则写入 `release-checklist.md`。
2. 连续 3 次 CI 运行无 flaky 失败。
3. `spec.md` 的 12 项验收标准逐条核对为 `passed`。
4. ROADMAP 中 RA-4、RA-7、BUG-1/2/6 置 `delivered`；GAP-1/3/4、P4 的 e2e 欠账标注闭合。
5. `docs/iterations/README.md` 状态看板更新。

**证据落点**：时长记录；ROADMAP diff；README 看板。

## 领取记录

### Round 1/5 —— RA-4（T1.1 + T1.2），2026-08-29 领取

```text
Round: 1/5
Roadmap item: RA-4 (IT-1/T1.1 + T1.2)
Profile/path: standalone（CI 发布流水线）
Objective: 未通过 _gate.yml 聚合门禁的 commit/tag 不能产出 Release 或 GHCR 镜像；skip 不计为 pass
Scope: .github/workflows/_gate.yml(新增)、test.yml、release.yml、release-beta-container.yml、docs/release-checklist.md
Non-goals: 不迁移 e2e 路径（T1.3/T1.4 属 Round 2）；不删改 hack/*.sh；不改 goreleaser 矩阵；不改测试断言语义
Acceptance: T1.1 验收 1-4；T1.2 验收 1-5（见上方任务明细）
Evidence: workflow 文件 diff；CI run URL（skip 场景 / 失败 tag / 正常 tag）——实现与本地校验在本 round 完成，run URL 证据需 push 后构造，记 pending
Result: active
Residual/follow-up: push 后补齐 3 次 run URL（构造方法见下）
```

### Round 1/5 实施证据（2026-08-29，本地阶段）

**T1.1**：

- 验收 1 ✅ `.github/workflows/_gate.yml` 新增，`on: workflow_call`，承载原 `test.yml`
  全部 7 个 job + `gate-passed` 聚合 job；支持可选 `ref` 输入（tag 绑定用）。
- 验收 2 ✅ `gate-passed`（`if: always()`）用 `toJSON(needs)` + `jq -e` 显式断言
  每个 job `result == "success"`；豁免经 `GATE_ALLOWLIST` 显式表达（默认空），
  豁免必须记入 `release-checklist.md` §1b。
- 验收 3 ◐ 逻辑已验证：gojq（jq 兼容实现，宿主 go1.26.5 + goproxy.cn）实跑 6 场景
  6/6 —— all success→true、unit-test skipped→false、failure→false、cancelled→false、
  storage-mysql skipped + allowlist→true、allowlist 带空格→trim 后豁免生效。
  **run URL pending**（构造方法见下）。`jq -e` 表达式已在 CI runner 的 jq 1.7 上复核语义
  （`. as $entry` 绑定避开 `index(.key)` 的数组索引错误）。
- 验收 4 ✅ `test.yml` 改为 `uses: ./.github/workflows/_gate.yml` 薄调用；
  结构等价校验（ruby/YAML）7/7 job 与原 `test.yml` 深比对通过，唯一差异为
  checkout 增加 `ref: ${{ inputs.ref }}`（空值时行为与原 checkout 相同）；
  push/PR 实际 run 待下次推送观察（行为等价的最终证据）。

**T1.2**：

- 验收 1 ✅ `release.yml` / `release-beta-container.yml` 均新增 `gate` job
  （`uses: ./.github/workflows/_gate.yml`，`secrets: inherit`），发布 job 加
  `needs: gate`；beta 的 `workflow_dispatch` 路径通过 `with.ref = inputs.tag || github.ref_name`
  把门禁钉到目标 tag（否则 dispatch 时门禁会落在默认分支 commit 上）。
- 验收 2 ◐ pending：失败 tag run URL（构造方法 2）。
- 验收 3 ◐ pending：正常 tag run URL（构造方法 3）。
- 验收 4 ✅(机制) 门禁为同步 `workflow_call`，运行在调用方事件 ref（= tag commit）上，
  不受分支后续提交影响；beta dispatch 由 `ref` 输入显式钉住。run URL 复验 pending。
- 验收 5 ✅ `docs/release-checklist.md` §1 改写为「流水线强制项」，新增 §1a 聚合判定
  （skip≠pass）与 §1b 豁免清单（默认空，逐条理由 + re-review date）。

**本地校验环境记录**：无 `jq`/`actionlint`/pyyaml，apk 网络不通；以宿主 go1.26.5 +
gojq v0.12.16（goproxy.cn）替代 jq 实测断言逻辑，ruby/YAML 做结构与等价校验。
CI run（ubuntu-latest 的 jq 1.7）为最终证据。

### Round 2/5 实施证据（2026-08-29/30，本地阶段）

**T1.3 —— 全部验收闭合，置 `done`**：

- 验收 1 ✅ `harness/`：`containers.go`（MySQL/ClickHouse `sync.Once` 懒启动 + 跨测试复用、
  TCP + sync_user 身份就绪探活、`stdcopy` 解帧 exec 输出）；`server.go`（被测二进制
  一次性 `go build`、Start/Stop/Kill/Restart、`/api/v2/health` 探活、日志尾部诊断、
  clean cwd 防 repo 配置泄漏）；`namespace.go`（按 `t.Name()` 派生库/前缀名，
  截断 + FNV 消歧）；`skip.go`（结构化 skip 记录 + strict 语义）。
- 验收 2 ✅ `-e2e.strict`（`skipFatal` 决策单测覆盖）；不带该标志时本地正常 skip。
- 验收 3 ✅ podman rootless 已文档化（`doc.go` + `PodmanHint` + `runtimeHint`），
  且**实际以 podman machine API socket 跑通全流程**（testcontainers "Connected to
  docker: Server Version 5.8.4"）。
- 验收 4 ✅ `go list -deps ./... | grep -c testcontainers` = **0**；
  `-tags=e2e` 时 = 7（隔离生效）。
- 验收 5 ✅ `namespace_test.go`（5 组：净化/截断消歧/确定性/subtest/DB 前缀）+
  `skip_test.go`（3 组：记录、skip 结束测试、strict 决策与默认值）；
  `go test -race ./internal/etl/e2e/harness/` 绿。

**T1.4 —— 验收 1-3 本地闭合，验收 4 待 CI run URL，置 `blocked`**：

- 验收 1 ✅ `path_mysql_cdc_mysql_test.go`：happy（3 行 + update 落库）、
  crash_restart（SIGKILL → 续跑 → 无丢失）、checkpoint_reset（upsert 吸收回放）、
  sink_outage_dlq_replay（DLQ error_class=schema → replay:1 → 条目删除 → 5 行对账）
  全部通过（本地 29.6s）。
- 验收 2 ✅ `path_mysql_snapcdc_clickhouse_test.go`：snapshot 5 行 FINAL、
  CDC update/insert/delete、schema_drift add-column（system.columns + FINAL 断言）、
  restart_recovery（停机期写入行恢复）、checkpoint_reset（FINAL=7 且 RMT 吸收，
  raw 重复仅 note，同脚本语义）、CH outage DLQ（error_class=transient, connection
  refused）→ replay:1 → 条目删除（本地 17.2s）。双测试共宿主 MySQL 容器全量跑 59.0s。
- 验收 3 ✅ 断言比对（无放宽）：

  | # | shell 断言（脚本行） | Go 断言 | 一致 |
  | --- | --- | --- | --- |
  | P1-1 | `wait_mysql_value COUNT IN(9101..9103)=3`（L216） | 同 SQL + `WaitValue` | ✓ |
  | P1-2 | update 后 `amount=11.11 AND status='vip'`（L218） | 同 | ✓ |
  | P1-3 | kill → INSERT 9104 → COUNT=1 + 总 4 行（L227-229） | `srv.Kill/Restart` + 同 | ✓ |
  | P1-4 | stop/reset/start → COUNT=4 + 9101=11.11（L234-240） | 同（start 见差异 3） | ✓ |
  | P1-5 | RENAME 目标表 → DLQ contains 9201（L246-262） | RENAME + `pollDLQ("9201")` | ✓ |
  | P1-6 | replay → `"replayed":1` → 行在 → DLQ id 删除 → 总 5 行（L267-277） | 同 | ✓ |
  | P2-1 | 快照 FINAL=5（L158） | 同 | ✓ |
  | P2-2 | UPDATE/DELETE/INSERT 三断言 + phase=cdc（L166-173） | 同 | ✓ |
  | P2-3 | add loyalty → system.columns=1 → FINAL id7 gold（L176-182） | 同 | ✓ |
  | P2-4 | kill → INSERT id8 → 续跑 → FINAL + phase=cdc（L184-192） | 同 | ✓ |
  | P2-5 | reset → FINAL=7 / id=1 唯一 / id8 在；raw 重复 note-only（L194-220） | 同（数值比较） | ✓ |
  | P2-6 | UPDATE id1 → FINAL=111.11 → phase=cdc（L223-227） | 同 | ✓ |
  | P2-7 | 停 CH → DLQ contains 9001 + 错误关键词（L229-248） | 同关键词集合 | ✓ |
  | P2-8 | 起 CH → replay:1 → FINAL 9001 → DLQ 删除（L250-262） | 同 | ✓ |

  **差异说明（非放宽）**：① 被测形态容器镜像 → 子进程二进制（plan.md 路线 B，
  崩溃恢复需真实进程生命周期）；② 依赖服务 compose → testcontainers（同一镜像、
  相同 mysqld flags：ROW/GTID/FULL/native_password）；③ `POST /start` 对
  409 `pipeline_stopping` 做有界重试 —— stop 为异步、API remediation 明示重试，
  shell 版靠进程间延迟偶然规避，非断言变更。

- 验收 4 ◐ `connector-e2e` job 已接入 `_gate.yml`（job + `gate-passed` needs 共 8 项），
  **CI 内实跑 run URL pending push**。

**实现调试记录**（供后续迁移参考）：testcontainers `Exec` 返回 docker attach
多路复用流，须 `stdcopy.StdCopy` 解帧否则帧头混入查询结果；MySQL entrypoint
初始化期有临时 server（skip-networking），socket 探活会误判 ready，须 TCP +
sync_user 探活后再授权。

**run URL 证据构造方法**（push 后执行，逐条回填）：

1. **T1.1 验收 3（skip → 门禁失败）**：临时分支在 `unit-test` job 加 `if: false()`
   （其余 job 正常），触发 push → 预期 `gate-passed` 失败，`needs` JSON 中
   `unit-test.result == "skipped"` 被显式断言捕获。留存 run URL 后删除分支。
2. **T1.2 验收 2（失败 tag）**：在临时 commit 上故意破坏一个单测 → 打
   `v0.0.0-gate-proof-fail` tag → 预期 release workflow 在 `gate` job 失败，
   无 Release/GHCR 产出。留存 run URL 后删除 tag。
3. **T1.2 验收 3（正常 tag）**：正常打下一个 beta tag → release / release-beta-container
   全流程通过，产物与改造前一致。

### Round 2/5 —— RA-7（T1.3 + T1.4），2026-08-29 领取

```text
Round: 1/5（新窗口，对应 IT-1 Round 2/5）
Roadmap item: RA-7 (IT-1/T1.3 + T1.4)；承接 RA-4 blocked_external 释放的 active 名额
Profile/path: standalone（两条主推荐路径：mysql_cdc→mysql upsert、mysql_snapshot_cdc→clickhouse）
Objective: e2e harness 骨架可用（容器懒启动复用、子进程被测二进制、命名空间隔离、skip≠pass），
  两条主推荐路径迁移为 Go e2e 并接入 _gate.yml 的 connector-e2e job
Scope: internal/etl/e2e/**、go.mod/go.sum（testcontainers 仅进 e2e tag）、.github/workflows/_gate.yml 新增一个 job
Non-goals: 不迁移其余路径（T1.5/T1.9-T1.11）；不改运行时语义；不放宽既有 shell 断言；不删 hack/*.sh
Acceptance: T1.3 验收 1-5；T1.4 验收 1-4（见任务明细）
Evidence: harness 单测、go vet、go list -deps 无 testcontainers、断言比对表、本地容器实跑输出（podman 可用时）、CI run URL pending push
Result: active
Residual/follow-up: connector-e2e 在 CI 内实跑的 run URL 证据 pending push；T1.5 证据产出属下一 round
```

## 领取记录模板

```text
Round: <n>/5
Roadmap item: <RA-4 | RA-7 | BUG-2 | ...> (IT-1/T1.<n>)
Profile/path: <standalone | connector path>
Objective: <one observable outcome>
Scope: <files/components allowed>
Non-goals: <explicit exclusions>
Acceptance: <numbered checks，直接引用本文件对应 task 的验收>
Evidence: <commands, run URL, e2e, docs>
Result: <delivered|active|blocked_external>
Residual/follow-up: <bounded next item or none>
```

### Round 3/5 —— RA-7（T1.5），2026-08-31 领取

```text
Round: 3/5（同一窗口第三轮，对应 IT-1 Round 3/5）
Roadmap item: RA-7 (IT-1/T1.5)
Profile/path: standalone（两条主推荐路径的证据产出）
Objective: 证据成为测试的结构化输出：每条路径运行产出 docs/evidence/<path_id>.json；
          校验器对已提交证据做 commit 祖先绑定 + 相对相关源码的新鲜度 + 一致性校验；
          PathContract.LastCertified 由证据派生，不再手工维护
Scope: internal/etl/e2e/harness/evidence.go（+单测）、docs/evidence/（新增目录）、
       hack/cmd/check-connector-evidence/{main,main_test}.go、
       internal/etl/server/path_contract{,_test}.go、docs/connector-certification.md
Non-goals: 不改 _gate.yml；不迁移其余路径；不动 connector-evidence.json；不重认证 manifest
Acceptance: T1.5 验收 1-4（见任务明细）
Evidence: 本地容器实跑产出两份 JSON（检查项全 passed）；校验器单测 10 项（含 6 种篡改）；
          篡改实证：翻转 check 与伪 commit 均被拒；server 单测（last_certified 派生）；
          CI run URL pending push
Result: blocked（验收 1/2/3 本地闭合；验收 4 与篡改失败 run URL 待 push）
Residual/follow-up: connector-e2e CI run URL + 篡改失败 run URL 待 push；
      新增迭代级发现：connector-evidence.json 的 CertifiedCommit=d75600be 早于本迭代
      全部 workflow 改动（_gate.yml/test.yml/release*.yml，commit 932370f/3c615b6），
      `-strict -commit` 在 main push / release 上必然失败 —— 需一次完整 certification
      run 重绑 manifest，或按用户对认证策略的裁决调整（记入 T1.12 迭代收口）
```

**Round 3/5 实施证据（2026-08-31，本地阶段）**：

- 验收 1 ✅ 两条路径实跑（podman socket + testcontainers，runner=local，2/2 PASS）后产出：
  - `docs/evidence/mysql_cdc__mysql_upsert.json`：4 个 checks（happy_path /
    crash_restart_recovery / checkpoint_reset_absorption / sink_outage_dlq_replay）全
    passed，`result:"passed"`，`commit` = 运行时的 HEAD；
  - `docs/evidence/mysql_snap_cdc__ch_rmt.json`：6 个 checks（snapshot_initial /
    cdc_update_insert_delete / schema_drift_add_column / process_restart_recovery /
    checkpoint_reset_rmt_absorption / sink_outage_dlq_replay）全 passed；
  - 字段齐备：`path_id/commit/run_started_at/runner/deps/checks[]/result`。
- 验收 2 ✅ 校验器改造 + 实证：
  - `check-connector-evidence` 新增路径证据校验（`-path-evidence docs/evidence` 默认）：
    commit 可解析且为当前 HEAD/`-commit` 的祖先或相等；不早于相关源码最近改动
    （`internal/etl/source|sink|core|transform|checkpoint|pipeline|server|e2e`、
    `internal/logic`、`manifest/config` 等，snapshot→CH 另含 `internal/etl/ddl`）；
    checks 非空且全 passed；`result` 与 checks 一致；`path_id` 与文件名一致；
  - 单测 10 项全绿（既有 3 项 commit 绑定 + 新增 4 项路径证据 + 6 种篡改变体）；
  - 本地篡改实证：① 将 happy_path 翻成 failed → `check "happy_path" result failed
    (reason: ); evidence not certified`；② 把 commit 改成不存在的哈希 →
    `does not resolve ... (tampered or rerun needed)`；恢复后校验通过；
  - 新鲜度（不早于相关源码改动）由单测 `TestCheckPathEvidenceRejectsStaleAfterSourceChange`
    覆盖（sink 改动后证据 → `stale` 拒绝）。
- 验收 3 ✅ `PathContract.LastCertified` 由 `docs/evidence/<path_id>.json` 派生：
  - `applyPathEvidenceLastCertified` 只在对应证据 `result=passed` 时填充
    （`run_started_at @ <8位commit>`），缺失/失败/不可解析证据一律留空；
  - 单测：hermetic 临时目录 passed→填充、failed→空；`TestHandlePathContracts` 扩为
    真实仓库级断言（证据已提交时两条主路径 `last_certified` 非空且含 ` @`）。
- 验收 4 ✅（本地确认，CI 待 push）`connector-e2e` job 按 `go test -tags=e2e -e2e.strict`
  的 **live 结果**判定，job 不读任何已提交证据文件；证据文件只是测试的写产物。

**发现并记录（迭代级阻塞，非 T1.5 缺陷）**：如上文 `Residual`，manifest 静态绑定
（CertifiedCommit=d75600be）与本迭代全部 workflow 改动冲突，`-strict -commit` 步骤
在 main push / release 上必失败。T1.5 的路径证据校验本身已绿（
`path evidence OK: 2 file(s) under docs/evidence, bound to <HEAD>`）；
manifest 重绑需要一次完整 certification run，属 T1.12 迭代收口范围，需用户裁决。

### Round 4/5 —— RA-7（T1.6 + T1.7），2026-09-01 领取

```text
Round: 4/5（同一窗口第四轮，对应 IT-1 Round 4/5）
Roadmap item: RA-7 (IT-1/T1.6 + T1.7)
Profile/path: standalone（BUG-1 mysql_batch varchar PK；BUG-2 binlog purged 三策略）
Objective: BUG-1 状态与证据一致性核对订正；BUG-2 fail/resume_from_current/resnapshot
          三策略容器级闭合，证据入验收矩阵
Scope: hack/e2e-binlog-purged*.sh、internal/etl/source/binlog_purge_test.go、
       mysql_binlog_purge_integration_test.go（addr env 化）、ROADMAP BUG-1/BUG-2 矩阵
Non-goals: 不改 mysql_batch/mysql_snapshot_cdc 运行时代码（代码已完成，本轮只补证据）；
          不动 connector-evidence.json
Acceptance: T1.6 核对 BUG-1 标题「delivered」vs 状态矛盾；T1.7 BUG-2 验收 1-5 全闭合
Evidence: e2e-bug1-varchar-pk.sh / e2e-binlog-purged.sh / e2e-binlog-purged-resnapshot.sh
          实跑输出（openetl-go-etl:dev + 本机 compose）、集成测试真机 PASS、单测
Result: active（T1.6/T1.7 证据闭合并置 done；process 继续 T1.8）
Residual/follow-up: T1.8 BUG-6 ColumnTypes e2e 下一轮
```

**Round 4/5 实施证据（2026-09-01）**：

- T1.6 ✅ BUG-1 核对订正：`mysql_batch.go` 自认证 commit d75600be 起**零改动**；
  `hack/e2e-bug1-varchar-pk.sh` 实跑 `openetl-go-etl:dev`（08-23 构建）→
  `state: completed, written=6`（验收 4 容器级证据补全）。ROADMAP BUG-1 置
  `delivered`，验收矩阵在标题行注明 2026-09-01 复核。
- T1.7 ✅ BUG-2 三策略容器级闭合：
  1. **fail**（`e2e-binlog-purged.sh`，修复脚本 docker 硬编码 + set -e pipefail
     grep 无匹配即退出的两个脚本 bug）：RESET MASTER → `binlog purged (ERROR
     1236) ... policy=fail` → `[ALERT] {"level":"error","title":"Pipeline read
     error"}` critical 告警 → 管道 status `completed`（终止态，非无限重试）；
  2. **resume_from_current**（集成测试 env 化后真机跑
     `TestBinlogPurgeRuntimeDetectionResumeFromCurrent`，OPENETL_TEST_MYSQL_ADDR=
     127.0.0.1:13306，compose mysql-source）：陈旧坐标 RunFrom → 1236 识别 →
     GetMasterPos 探测 → 续流无 1236；
  3. **resnapshot**（新脚本 `e2e-binlog-purged-resnapshot.sh`）：快照 2 行 →
     RESET MASTER → checkpoint `mysql-bin.000004:4460` 失效 → `falling back to
     snapshot phase from last cursors (RPO gap ...)` → 续读不重读 + 新行 carol
     送达（sink `alice bob carol`）→ 管道 running；
  4. 单测补齐：`TestBinlogPurgedRecoveryResnapshotOnPlainCDCFailsClosed`（纯
     mysql_cdc 的 resnapshot 请求 fail-closed 返回 ErrBinlogPurged 哨兵）；
  5. ROADMAP BUG-2 验收矩阵 5 项全部 pass 入档。

**T1.8 实施证据（2026-09-01，Round  追加）**：

- BUG-6 验收 2/3 闭合：新脚本 `hack/e2e-bug6-column-types.sh` 容器级实跑 PASS：
  snapshot_cdc（MySQL 源表 `link_src`，`request_id varchar(32)`、`amount decimal(12,2)`）→
  kafka envelope（`column_types` 序列化确认于 sink/kafka.go kafkaEnvelope.ColumnTypes）→
  clickhouse auto_create：CH `system.columns` 断言 `request_id=String`（源 varchar →
  声明类型，而非数值样本推断成 Int）、`amount=Decimal(12,2)`（源精度直传）；
  CDC 相位 UPDATE+INSERT 经信封中继后 FINAL count=3、id=3 行 `amount=44.44` 落地。
- 脚本自身两处修正（非产品缺陷）：① kafka source 的 topic 配置项为单数
  `topic:`（`topics:` 数组配置无效 → invalid topic）；② ReplacingMergeTree 下
  count 断言必须 `FINAL`（raw count 含未折叠副本导致 4≠3）。
- ROADMAP BUG-6 置 `delivered`，README 看板同步。

**T1.9/T1.10/T1.11 实施证据（2026-09-01/02，Round 5/5）**：

- T1.9 ✅ GAP-1：`e2e-postgres-cdc.sh` 实跑（重建镜像=当前 HEAD 代码）PASS——真实
  pgoutput 流 INSERT/UPDATE/DELETE → MySQL sink，含 checkpoint stop/restart 停止期
  事件补收。GAP-3：新脚本 `e2e-kafka-postgres-fanout.sh` PASS——单 topic 两表
  envelope 扇出 → PG `pk_columns_from_metadata`（INSERT/upsert 11.00→15.00/DELETE、
  `pg_index` 断言派生 PK 约束）。**e2e 暴露并修复真实缺陷**（commit 1966a03）：
  auto-create 建表缺派生 PK 约束 → 后续 `ON CONFLICT(pk)` SQLSTATE 42P10 进 DLQ；
  修复为 pkByTable 快照提前 + `buildPgCreateTableDDL` 输出真 PRIMARY KEY。
- T1.10 ✅ GAP-4：`e2e-elasticsearch.sh` PASS——bulk 2 成功 + `mapper_parsing_exception`
  单条 item-level DLQ + 修 mapping 后 replay `replayed:1`。**语义修正**（同 commit）：
  `esTypeCompatible` 放行 string→数值/日期（bulk 自解析、值冲突落单条 DLQ，与 GAP-4
  契约一致），bool→数值仍拒绝；preflight 测试拆分两契约各自断言。
- T1.11 部分：**Kafka 事实核验 ✅** `e2e-kafka.sh` PASS（rebalance 恢复 + offset
  replay 去重吸收）。**Doris `blocked`**：`apache/doris:be-2.1.11` 镜像拉取两次
  于大 blob 处 `unexpected EOF`/TLS 握手超时（registry 不稳；FE 镜像拉取成功，
  BE 始终失败），本机 5CPU/16GB 可运行但镜像不可得；`hack/e2e-doris.sh` 保留为
  手动路径，不记 pass（P4 规则：镜像不可得记 blocked，绝不虚报）。

### 迭代收口状态（2026-09-02，T1.12）

**本地可完成工作已全部完成**。剩余任务全部阻塞于同一外部输入：**push `main` 触发 CI**：

- T1.1/T1.2：`_gate.yml` reusable workflow 与 release 门禁接线**代码已就绪**（此前轮次
  已提交），验收要求的三类 run URL（skip-fail、fail tag、normal tag）只能由真实
  push 产生 → `blocked`（待用户授权 push）；
- T1.4/T1.5：两条主路径迁移、`connector-e2e` job、结构化证据与校验器**全部完成且
  本地绿**（证据绑定 3d6c8ad，checker 全绿，篡改拒绝已本地实证），验收要求的
  `connector-e2e` run URL 与篡改失败 run URL 同样待 push → `blocked`（同因）；
- **manifest 重绑决策（需用户裁决）**：`check-connector-evidence -strict -commit HEAD`
  失败——manifest `CertifiedCommit=d75600be` 早于本迭代全部 workflow 改动
  （932370f/3c615b6 等），main push/release 上 strict 模式必失败。重绑需要一次完整
  certification run（或用户决定放宽 strict 绑定策略）。不在本迭代擅自重绑。
- 卫生清理（T1.12）：根目录 `data-*`×20、`logs/`、`.tmp-go-cache/`(839M)、
  `openetl-go`(86M) 测试残留已清除；`.pi/`、`.pi-subagents/`、`pipes-clickhouse-prod/`
  按 AGENTS 规则未触碰。

**全量回归（2026-09-02）**：`go vet` OK；`go test ./... -count=1` 无 FAIL；
`-race`（server/sink/source/checker）全 ok。
