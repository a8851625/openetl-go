# 技术方案与回滚

## EV-2

允许修改 mysql_test.go、对应 CI 验证及证据文档。
现有 smoke 在真实 MySQL 中读取已提交行数与字段值，写入耗时保留为测量输出。
批量 SQL/事务的既有测试继续覆盖实现。
运行真实 MySQL 的 targeted test，再在 PR 的 integration job 复验。
整体聚合门禁在最终 GA-C1 的候选认证后核验。
该增量只有测试与证据变化，回滚恢复前一个测试版本。

## P5-H1

允许修改 telemetry/health、server metrics/health、UI types/health consumer、i18n 与 UI 测试。
以服务端当前 health、阈值和未知标记作为事实，保留旧 API 客户端与旧 payload 的兼容 fallback。
restore_failed 进入现有可行动入口，open breaker 与 unreadable backlog 有可解释状态。
通过函数/契约测试和四个 UI 入口验证，恢复后健康状态应能回到正常。
回滚 UI 消费和新增响应字段，既有持久化格式不变。

## PR-0-C1

允许修改 server connection/spec lifecycle、sqlstore/specstore/storage contracts、Connections UI 和三 backend 测试。
解析 persisted current spec 的 connection/connection_ref，包含 transforms 与 DAG nodes。
影响查询返回稳定 pipeline ID、显示名与 endpoint/node 定位。
删除和 current-spec 写入在存储一致性边界内校验，覆盖 API、YAML 导入/hot reload、restore 和多进程写入。
无法读取/解析引用时阻断删除并给出可定位错误。
兼容原有未引用连接删除与已有 inline overrides。
回滚只读影响 API/UI，保护逻辑按独立增量撤回；当前定义与 checkpoint 不删除。

## UI-E-R1

允许修改 DLQ/恢复事实存储、adapters、pipeline/DAG 失败记录与 server replay/delete、backup/migrations、
Issues/DLQ/routing 与 API/runbook 文档。
依据已授权的 contract-preflight.md 提案，用已知字段记录稳定故障 identity。
持久化状态由捕获、sink acknowledgement、DLQ row 删除与恢复操作驱动，区分成功重放和主动丢弃。
失败路径同时保留 DLQ 的 durable gate；事件记录失败应暴露缺失事实。
旧 DLQ 不猜测关联，旧 API payload 继续可读。
备份包含全部新 metadata 与关联；恢复不重写已知 ID、计数与时间。
回滚保留新增表/列和历史事实，UI 可退回既有信号入口。

## GA-C1

允许修改 release workflows、artifact/certification helper、必要的 E2E image/binary 选择、
evidence validator 与认证/发布文档。
资产构建与发布解耦，认证读取实际待发布镜像及已打包二进制。
认证报告独立于镜像内容，避免把镜像自己的 digest 写回镜像造成循环重建。
报告绑定候选源码 commit、镜像 identity、二进制/资产 hash、每项实际执行结果与窗口。
所有 mandatory cases success 后才能消费该资产发布，skip/failure/cancel 阻断。
发布后的 immutable digest 与测试资产 identity 核对并与报告一同保存。
回滚 workflow 到旧构建流程时仍保留原有同步 gate；认证状态如实降级。
