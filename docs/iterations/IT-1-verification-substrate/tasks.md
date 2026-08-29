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
| 5/5 | T1.9、T1.10、T1.11、T1.12 | GAP e2e 欠账闭合 + 迭代收口 | 是 |

## 任务表

| ID | 任务 | 依赖 | 状态 | 证据落点 |
| --- | --- | --- | --- | --- |
| T1.1 | 抽出 `_gate.yml` reusable workflow + `gate-passed` 聚合断言 | — | `active` | workflow 文件、故意失败的 run URL |
| T1.2 | release / release-beta-container 接入门禁 | T1.1 | `active` | 失败 tag 与正常 tag 两次 run URL |
| T1.3 | e2e harness 骨架（容器、子进程、命名空间、skip 语义） | — | `todo` | `internal/etl/e2e/harness/` + harness 单测 |
| T1.4 | 迁移两条主推荐路径并接入 CI | T1.3 | `todo` | `connector-e2e` job run URL |
| T1.5 | 结构化证据产出 + commit 绑定校验 + `LastCertified` | T1.4 | `todo` | `docs/evidence/*.json`、篡改证据的失败 run |
| T1.6 | BUG-1 状态与证据一致性核对与订正 | T1.3 | `todo` | ROADMAP BUG-1 验收矩阵 |
| T1.7 | BUG-2 三策略容器级闭合 | T1.3 | `todo` | ROADMAP BUG-2 验收矩阵 |
| T1.8 | BUG-6 `ColumnTypes` e2e 闭合 | T1.3 | `todo` | ROADMAP BUG-6 验收矩阵 |
| T1.9 | GAP-1 / GAP-3 PostgreSQL 实例 e2e | T1.3 | `todo` | ROADMAP GAP-1/GAP-3 条目 |
| T1.10 | GAP-4 ES mapping-conflict e2e | T1.3 | `todo` | ROADMAP GAP-4 条目 |
| T1.11 | P4 Doris/Kafka 事实核验 | T1.3 | `todo` | P4 follow-up 记录 |
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

**run URL 证据构造方法**（push 后执行，逐条回填）：

1. **T1.1 验收 3（skip → 门禁失败）**：临时分支在 `unit-test` job 加 `if: false()`
   （其余 job 正常），触发 push → 预期 `gate-passed` 失败，`needs` JSON 中
   `unit-test.result == "skipped"` 被显式断言捕获。留存 run URL 后删除分支。
2. **T1.2 验收 2（失败 tag）**：在临时 commit 上故意破坏一个单测 → 打
   `v0.0.0-gate-proof-fail` tag → 预期 release workflow 在 `gate` job 失败，
   无 Release/GHCR 产出。留存 run URL 后删除 tag。
3. **T1.2 验收 3（正常 tag）**：正常打下一个 beta tag → release / release-beta-container
   全流程通过，产物与改造前一致。

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
