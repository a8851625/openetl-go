# GA 收口评估（IT-4）— 2026-09-16

> 评估基线：commit `b1deefa`（本地 main，含 UI-A/UI-B 全部增量与 IT-3/T3.6-B 容量闭合）。
> 方法：按 [ROADMAP](./ROADMAP.zh.md)「项目级发布门槛」五条 + [IT-4 spec](./iterations/IT-4-ga-closeout/spec.md)
> 十项验收逐条核验；证据只认当前版本实际执行结果。本判定保守：证据不完整即 fail。

## 结论（显式判定）

| 声明分级 | 判定 | 一句话理由 |
| --- | --- | --- |
| **项目级 production ready** | ❌ **不可移除 beta** | 门槛 2 的两条主链路证据刚在本评估内重绑当前 commit，尚未在一个「候选发布 commit + 发布镜像」上整体冻结；且 distributed 证据与 standalone 未分离重验 |
| **standalone production ready** | ✅ **可声明**（v0.2.12-beta.20 发布时生效） | 五条门槛中 1/3/4/5 全部 pass；门槛 2 已在 `b1deefa` 通过（4+6 项检查矩阵），发布流程需在新 tag 上重跑并重绑（这是发布纪律，不是缺口） |
| **distributed production ready** | ❌ 保持 beta | PR-D1 证据链（worker 认证/fencing/多进程恢复）绑定旧 commit/镜像，未随当前版本重验；按「不得用一个形态的证据宣称另一个形态」保持降级 |
| **connector-path production ready** | ✅ 按 path 声明有效 | 15 条 connector 证据 verified、expires 2026-10-11；两条 forced primary path 在 `b1deefa` 全矩阵通过 |

**若要移除项目级 beta，剩余阻断项**（写入 ROADMAP 排期）：

1. 在发布候选 commit（changelog 冻结后）重跑两条 forced primary path + 14 个 shell 认证脚本，重绑 connector-evidence manifest（certified_commit/certified_image 指向发布 commit 与发布镜像 digest）。
2. distributed 形态在当前版本重新执行 PR-D1 多进程恢复/fencing 认证，或明确文档只声明 standalone。
3. 完成一个版本周期的 CI 观测预算噪声量化（resource-baseline warning→blocking 决策）。

## 五条项目级发布门槛核验

| # | 门槛 | 判定 | 证据 |
| --- | --- | --- | --- |
| 1 | PR-0/PR-1/PR-2 全部交付 + P4/P5 首次任务/健康度/升级恢复验收完成 | ✅ pass | ROADMAP 状态：PR-0（5 轮）、PR-1（1.1/1.2/1.3）、PR-2（含 PR-2.4 checkpoint fail-closed）、PR-D1 delivered；P3/P4/P5 delivered；UI-A/UI-B 迭代 complete（e2e 158 passed / 0 failed） |
| 2 | 两条主推荐链路当前版本真实故障认证 | ✅ pass（本评估内完成） | `go test -tags=e2e -e2e.strict`：mysql_cdc→mysql upsert（happy/crash_restart/checkpoint_reset/sink_outage_dlq_replay 4 项）与 mysql snapshot+CDC→ClickHouse RMT（6 项，含乱序 replay）在 `b1deefa` 全部 passed；`hack/check-connector-evidence.sh` 全绿（2 path + 15 connector 绑定校验通过） |
| 3 | production 标记的 storage backend 同套 migration/backup/restore conformance | ✅ pass | 三 backend 升级 drill 本次实跑通过：`hack/e2e-storage-upgrade-{sqlite,mysql,postgres}.sh`（legacy→current 前向升级、失败阻断启动、backup/restore 跨版本路径）；IT-3 T3.3/T3.4 已有 100k 行三 backend 逐字段对账证据 |
| 4 | 发布资产无空 token/change-me/浮动 latest 生产默认 | ✅ pass | `hack/check-release-assets.sh` 通过（compose 必填 secret、固定 image、TLS server name 必填；`:latest` 仅作为 goreleaser 次 tag 且带 NOTE） |
| 5 | 发布说明列出全部残余边界 | ✅ pass（模式已建立） | CHANGELOG beta.19 已列 at-least-once/重复/RPO/RTO/单点/非原子 fanout/未认证 connector；beta.20 changelog 沿用并更新（见下节模板） |

## 十项验收标准核验（IT-4 spec）

| # | 验收 | 判定 | 证据 |
| --- | --- | --- | --- |
| 1 | 五条门槛逐条核验 | ✅ | 上表 |
| 2 | 两条主链路当前 commit 全矩阵 | ✅ | 同门槛 2 |
| 3 | 三 backend 同套 conformance | ✅ | 同门槛 3 |
| 4 | 升级 drill 完成 | ✅ | 同门槛 3（前向升级 + 失败可见；回滚窗口依赖 backup/restore 路径，T3.4 原子 restore 已验） |
| 5 | LastCertified 全量新鲜 | ✅ | path 证据 `b1deefa`；connector manifest 15 条 verified、expires 2026-10-11、certified_commit `81ab3d8`（binding 检查通过——manifest 更新后的窄路径规则） |
| 6 | maturity 与证据一致 | ✅ | `check-connector-evidence.sh` 全绿；maxcompute/odps 保持 experimental（writer-disabled）；distributed 保持 beta 边界；无声明高于证据项 |
| 7 | 发布资产安全默认 | ✅ | `check-release-assets.sh` |
| 8 | 残余边界完整列出 | ✅ | 见下节 |
| 9 | 声明分级分别判定 | ✅ | 见结论表 |
| 10 | GA 判定显式产出 | ✅ | 本文档 |

## 残余边界（发布说明必须保留）

- 默认语义 checkpointed **at-least-once**；checkpoint reset / crash replay 可能产生重复，生产链路靠业务键/upsert/版本列/显式 dedup 吸收。
- standalone 单进程是单点：RPO = 最后持久化 checkpoint，RTO = 进程重启 + checkpoint 恢复（实测基线见 resource-baseline.md：启动到健康 ~0.4s；容量曲线按 backend/并发档位）。
- fanout 非原子：多 sink 部分成功时其余 sink 可能重放（DLQ 可见）。
- 未认证/降级 connector：MaxCompute（experimental，writer 禁用）、Feishu（插件样板）、第三方插件；distributed compose 保持 beta。
- SQLite metadata 存储在 >8 并发 streaming pipeline 下 checkpoint 尾延迟劣化（p99 247ms@32），推荐 MySQL/PostgreSQL。
- linux/amd64 发布镜像的路径认证：当前证据为 linux/arm64 本机 + CI（GitHub runner amd64）；发布 tag 上由 CI 全量重跑覆盖。

## IT-4 任务状态

| ID | 任务 | 状态 | 证据 |
| --- | --- | --- | --- |
| T4.1 | 证据全量复核对照表 | done | `check-connector-evidence.sh`（工具化）本次输出全绿 |
| T4.2 | maturity 对齐 | done | 无降级项触发：全部声明 ≤ 证据；不需要改动四类文档 |
| T4.3 | 三 backend 升级 drill | done | 三个 `e2e-storage-upgrade-*.sh` 本次实跑 PASS |
| T4.4 | 两条主链路当前版本认证 | done | `-tags=e2e -e2e.strict` 在 `b1deefa` 全矩阵 PASS；证据 JSON 已重绑 |
| T4.5 | GA 判定 + 发布说明 + 收口 | done | 本文档 |

## 对发布流程的要求（非缺口，纪律）

beta.20（或 GA 候选）发布时：候选 commit 上重跑两条 forced path + 14 shell 认证脚本 → 重绑 manifest → tag → 三 workflow 全绿。在该循环完成前，项目级 beta 标识保留。
