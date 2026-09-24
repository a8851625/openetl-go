# IT-6 任务分解：mysql_snapshot_cdc 复合主键快照

> 状态：`queued` | 窗口：2–3 轮 | spec：[spec.md](./spec.md) | 方案：[plan.md](./plan.md)

## 任务表

| ID | 任务 | 状态 | 证据位置 |
| --- | --- | --- | --- |
| T6.1 | 复合 PK 解析（KEY_COLUMN_USAGE 探测 + pkKindComposite + 可排序约束）与行值比较分页 | todo | internal/etl/source/mysql_snapshot_cdc.go + 新单测 |
| T6.2 | checkpoint 复合游标持久化（last_composite_cursors 可选字段 + 旧格式兼容 + 断点续跑） | todo | 同上 + 恢复单测 |
| T6.3 | 快照记录复合键元数据（PrimaryKeyColumns 全列 + metadataKeyJSONMulti） | todo | mysql_snapshot_cdc.go:1045 区域 + 单测 |
| T6.4 | e2e（复合 PK 表快照→CH 建表/合并/重启恢复 + -e2e.strict 回归）与可见性（WARN 文案 + preflight snapshot_coverage） | todo | internal/etl/e2e/ + preflight + docs |
| T6.5 | 回归收口（三 backend drill 确认无影响 + 认证脚本回归 + 文档 + ROADMAP/看板更新） | todo | drill 记录 + docs |

## 轮次规划

| 轮次 | 内容 |
| --- | --- |
| Round 1 | T6.1 + T6.2（核心：解析与游标分页 + checkpoint 恢复；单测 + MySQL 8 集成分页测试全绿） |
| Round 2 | T6.3 + T6.4（元数据 + e2e + 可见性） |
| Round 3 | T6.5（收口；若 Round 2 顺利可并入） |

## Claim 记录

（领取时按 AGENTS.md 最小记录格式追加）
