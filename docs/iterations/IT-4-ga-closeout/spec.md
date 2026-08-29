# IT-4：GA 收口评估

> 状态：`queued` | 依赖：IT-1 + IT-2 + IT-3 | 归属 roadmap 条目：项目级发布门槛、发布声明分级、证据治理

## 问题陈述

前三个迭代关闭具体缺口，但**「是否可以移除 beta」是一个独立动作**，不是前三者完成后的
自动结果。ROADMAP 的「项目级发布门槛」列了五条硬性条件，需要逐条核验并留下判定记录：

1. `PR-0` / `PR-1` / `PR-2` 全部交付，且 P4/P5 中与首次任务、健康度、升级恢复相关的验收项完成。
2. 至少两条主推荐链路完成**当前版本**的真实故障认证：MySQL CDC → MySQL upsert，
   以及 MySQL snapshot+CDC/CDC → ClickHouse 幂等版本表。
3. SQLite、MySQL、PostgreSQL 中任一仍公开标记为 production 的 storage backend，都必须进入
   同一套 migration / backup / restore conformance；不能用 SQLite 通过替代其他 backend 的证据。
4. 发布资产不使用空 token、`change-me` 密码或浮动 `latest` 镜像作为生产默认值；
   不满足生产门槛的模式和 connector 明确降级为 beta/experimental。
5. 发布说明列出 at-least-once、可能重复、RPO/RTO、单点、非原子 fanout 和未认证 connector
   等残余边界。

同时存在两个跨迭代的治理问题：

- **证据新鲜度**：`PathContract.LastCertified` 在 IT-1 后由测试自动填充，但历史 maturity
  字符串（connector-certification、path-contract、README、positioning）是否与当前证据一致，
  需要一次全量复核。审计已记录过「证据 commit 与实际 HEAD 漂移」。
- **升级路径未验证**：从上一稳定版本到当前版本的真实升级 + backup/restore drill 在
  SQLite / MySQL / PostgreSQL 三 backend 上尚未完成。

**判定必须是显式产出**。本迭代允许的结论包括「可移除 beta」「standalone 可移除 beta 但
distributed 保持 beta」「仍不可移除，缺口为 X」—— 但不允许没有结论。

## 可观察结果

1. 每条公开的 maturity 声明都能追溯到当前版本实际执行的证据。
2. 从上一稳定版本升级到当前版本，三个 storage backend 均有可重复的升级与恢复记录。
3. 发布说明明确列出全部残余边界，用户能据此判断适用性。
4. 产出一份**显式的 GA 判定**，说明哪些形态可以移除 beta、哪些不能、理由是什么。

## 验收标准

| # | 验收标准 | 判定方式 |
| --- | --- | --- |
| 1 | 五条项目级发布门槛逐条核验 | 每条给出 pass/fail + 证据位置，fail 项列出具体缺口 |
| 2 | 两条主推荐链路当前版本认证 | MySQL CDC → MySQL upsert、MySQL snapshot+CDC → ClickHouse 的 crash / reset / outage / DLQ replay / 重复吸收 / 乱序 replay 矩阵在当前 commit 通过 |
| 3 | 三 backend 同套 conformance | SQLite / MySQL / PostgreSQL 均通过 migration + backup/restore；无「用 SQLite 替代」的情况 |
| 4 | 升级 drill 完成 | 从上一稳定版本升级到当前版本，三 backend 各一次，含回滚验证 |
| 5 | `LastCertified` 全量新鲜 | 所有 path contract 的该字段绑定当前版本，无空值、无过期 |
| 6 | maturity 与证据一致 | `connector-certification.md`、`path-contract.md`、`README(.zh).md`、`positioning(.zh).md` 中每条 maturity 声明有对应当前证据；不一致者降级 |
| 7 | 发布资产无不安全默认值 | 无空 token、`change-me`、浮动 `latest`；`hack/check-release-assets.sh` 通过 |
| 8 | 残余边界完整列出 | 发布说明含 at-least-once、可能重复、RPO/RTO、单点、非原子 fanout、未认证 connector |
| 9 | 声明分级明确 | 按 ROADMAP「发布声明分级」分别判定：项目级 / standalone / distributed / connector-path |
| 10 | GA 判定显式产出 | 产出判定文档，含结论、理由、若不可移除则列出剩余阻断项与预估 |

## 非目标

- 不在本迭代实现新功能或修复新缺口。发现的缺口记入 ROADMAP 并进入后续迭代，不就地修复。
- 不为达成 GA 而放宽任一门槛或降低证据要求。
- 不把 distributed 与 standalone 捆绑宣称；两者分别判定。
- 不评估 MaxCompute（属 PT-A，且其 `experimental` 状态不阻塞 standalone GA）。

## 交付约束

1. **判定必须保守**。证据不完整时结论是「不可移除 beta」，而不是「基本满足」。
   ROADMAP 已明确：「代码存在」「脚本存在」「maturity 字符串」都不等于完成。
2. **不得用一个形态的证据宣称另一个形态**。standalone 的证据不能用于 distributed；
   单个 connector 的 maturity 不能外推为任意组合可生产。
3. **发现缺口时不就地修复**。本迭代是评估，不是实现。就地修复会使评估失去独立性。
4. **降级优先于解释**。maturity 与证据不一致时，第一动作是降级声明，而不是补充说明文字。
5. **判定文档需含否定结论的表达能力**。模板必须能表达「不可移除 beta」并列出阻断项，
   否则评估会退化为走过场。

## 依赖与前置

| 类型 | 内容 | 状态 |
| --- | --- | --- |
| 迭代依赖 | IT-1（证据自动化与门禁） | `queued` |
| 迭代依赖 | IT-2（正确性缺口关闭） | `queued` |
| 迭代依赖 | IT-3（完整性与容量基线） | `queued` |
| 外部输入 | 上一稳定版本的可用发布产物（用于升级 drill） | 已具备（GHCR 历史 tag） |
| 待决策 | 无（判定本身是本迭代的产出） | — |

## 完成定义（DoD）

1. `tasks.md` 中全部 task 状态为 `done`。
2. 上方 10 项验收标准全部 `passed`（验收 10 的「产出判定」即使结论是否定的也算 passed）。
3. 判定文档产出并链接进 ROADMAP 与 `release-checklist.md`。
4. 若判定为可移除 beta：README、positioning、connector-certification、path-contract、
   发布说明同步更新；若判定为不可移除：剩余阻断项已作为新条目写入 ROADMAP 并排期。
5. `docs/iterations/README.md` 状态看板更新。
