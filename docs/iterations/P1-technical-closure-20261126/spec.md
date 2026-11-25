# P1 技术修复闭环

基线为 e28a740 / v0.2.12-beta.21。2026-11-26 用户授权完成五项 P1，
并明确暂时没有外部参与者，先完成技术修复。
执行 backlog 与验收标准仍以 docs/ROADMAP.zh.md 为准。

## 结果与验收

| Increment | 可观察结果 | 验收 |
| --- | --- | --- |
| EV-2 | MySQL integration smoke 校验已提交数据，CI 对共享 runner 抖动稳定。 | 1000 行业务字段对账通过；真实 MySQL 执行；新的 CI integration 和 aggregated gate 为 success。 |
| P5-H1 | 故障与未知状态能从现有 UI 入口发现。 | open breaker 为 degraded；restore_failed 可行动；DLQ 未知不显示为已确认 healthy；API/UI 阈值同源；四个入口自动化覆盖。 |
| PR-0-C1 | 连接删除保持持久化管道引用有效。 | source/transform/sink 与 DAG 引用来自当前 persisted spec；引用未解除时删除返回冲突并列出影响；并发写入与删除不会留下悬空引用；三 storage backend 和重启验证。 |
| UI-E-R1 | 一次 sink outage 及其 replay 可通过持久化事实核对。 | 稳定 identity；已知 pipeline/node/connection 关联；失败/重放/结果持久化；重启可查；删除/驱逐具有准确结论；三 backend 与 backup/restore；真实 outage/dry-run/replay 和深链回归。 |
| GA-C1 | 被发布的候选资产与被测试的资产相同。 | 构建一次；推荐路径与认证矩阵使用该候选；commit、image/binary digest、执行窗口和结果在证据中绑定；发布步骤消费已通过的资产；失败/跳过阻断；当前候选实际执行。 |

## 范围

延伸现有 Source → Transform → Sink、SQL metadata、API v2、hash router 和发布 workflow。
每轮只领取一个上表增量，最多 5 rounds。

本窗口不承担 EV-3、运行历史 P2、connector 扩展或全局向导/信息架构重做。
UI-E 外部任务走查保留现有 3–5 人研究验收，缺输入时如实记录。

## 数据与交付约束

- source/checkpoint/DLQ 的 at-least-once 边界以既有持久化确认契约为准。
- 恢复事实以实际写入/replay/delete 结果驱动，事件写入错误须可见，DLQ 持久化失败仍阻断 checkpoint。
- 连接引用以当前持久化定义为准，删除与定义写入必须在同一一致性边界内终审。
- 新存储数据需要 SQLite/MySQL/PostgreSQL 迁移、备份、恢复与旧数据兼容证据。
- URL 承载稳定对象 identity 与已允许的 filter，拒绝敏感参数。
- 当前用户变更保留，测试中的数据重置运行于隔离工作目录。
