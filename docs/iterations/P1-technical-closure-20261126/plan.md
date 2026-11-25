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
容器恢复验收发现必要前置：默认 30s breaker cooldown 消耗 30s batch commit deadline，恢复后仍触发 checkpoint timeout。允许在 pipeline/circuit_breaker.go、pipeline.go 和最近 runner tests 中把冷却等待移到提交期限之前；停止等待不推进 checkpoint，已有在途提交的 Stop 语义保持测试覆盖。
回滚 UI 消费和新增响应字段，既有持久化格式不变。

## PR-0-C1

允许修改 server connection/spec lifecycle、sqlstore/specstore/storage contracts、Connections UI 和三 backend 测试。
解析 persisted current spec 的 connection/connection_ref，包含 transforms 与 DAG nodes。
影响查询返回稳定 pipeline ID、显示名与 endpoint/node 定位。
删除和 current-spec 写入在存储一致性边界内校验，覆盖 API、YAML 导入/hot reload、restore 和多进程写入。
无法读取/解析引用时阻断删除并给出可定位错误。
兼容原有未引用连接删除与已有 inline overrides。
UI 删除确认读取 references API；允许 shared ConfirmDialog 增加可选 details 与 confirmDisabled，默认调用行为不变。加载、查询失败、未知或仍被引用时不提供可执行删除；最终 DELETE 仍原子重验。
回滚只读影响 API/UI，保护逻辑按独立增量撤回；当前定义与 checkpoint 不删除。

## UI-E-R1

允许修改 DLQ/恢复事实存储、adapters、pipeline/DAG 失败记录与 server replay/delete、backup/migrations、
Issues/DLQ/Connections/detail/routing、useApi 的显式禁用能力与 API/runbook 文档。
依据已授权的 contract-preflight.md 提案，用已知字段记录稳定故障 identity。
Preflight 确认 multi-shard checkpoint 仅改变 checkpoint namespace，runner 的 pipeline UUID 保持不变；但 transform clone 丢弃 connection/ref 值字段。允许在 shard_builder.go 保留这些字段并保留独立的顶层 config map，用于准确捕获连接身份；不改 shard/checkpoint 键或执行语义。
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


### GA-C1 bounded delivery split

1. Candidate capture: GoReleaser builds and archives once with publishing disabled; source, executable, archive and image identities are written after build. Allowed output is a task-owned directory; inputs are frozen before generation. Preserve default/extism platform matrix and existing image entry point.
2. Candidate certification: existing shell and tagged tests accept an explicit immutable image/binary; report collectors retain real per-case results and windows. Missing services/credentials are recorded and block promotion. Current-source/checksum checks reject a wrong executable rather than building a substitute.
3. Candidate promotion: a reusable build → certify → aggregate workflow exposes the same artifacts to publishing. Beta retry reads an already certified bundle. Upload/load/tag/push consume preserved bytes; an isolated registry proves content identity without publishing externally.
4. Evidence closure: all mandatory results, strict provenance, API readiness and docs reconcile. Embedded history stays historical; a read-only external manifest binds source plus executable SHA after certification, avoiding digest self-reference. No runtime pipeline semantics change.

### GA-C1 aggregate-gate prerequisite (2026-11-26)

The actual PR run 45692007631 found raw DLQ SQL fidelity failures in SQLite and
PostgreSQL volume backup/restore. R4's new nullable recovery context/tracking
columns were coalesced during backup and restored as an empty context/string.
Within Round 5/5, preserve their SQL NULL and original JSON representation in
backup-only fields; do not relax the independent raw SQL fingerprint check.
Allowed files: storage/storage.go, sqlstore/backup_reader.go and backup.go,
backup/conformance_test.go, the existing backup-volume fixture, and backup docs.
Acceptance: mixed legacy NULL/empty/known recovery context round-trips on all
three backends; existing >100k-per-table volume checks pass; no runtime DLQ
capture/replay or checkpoint behavior changes. Legacy v1/v2 backups remain
readable. Rollback retains schema/data and only removes supplemental backup
metadata support.

The same candidate run also exposed cold-run fixture assumptions (ClickHouse and
MinIO must be pulled before their scripts), a PostgreSQL assertion that observed
source completion before sink acknowledgements, and cached ephemeral Docker host
ports in all three browser probes. Correct these certification harness boundaries
in _candidate.yml, e2e-mysql-postgres.sh and the shared health_ui_e2e.py probe, then
rerun the actual candidate cases. Preserve their failed reports; do not reuse a
success from another image or infer a pass from a harness edit.

The completed failed tagged case in run 45692007631 showed that extracting only
the release executable omitted the distribution's runtime config (all seven
server-based paths failed startup). Extract that config from the same hashed
platform archive next to the executable; never substitute checkout config or
compile a different binary. Acceptance remains the full seven-path candidate run.

The dcd5089 candidate run passed both primary paths, all seven tagged files and
the three P1 browser probes. Its S3 job was blocked before execution: Quay returned
401 for the pinned MinIO server image and Docker Hub also denied the same tag.
The public source commits are available and match the cached fixture versions:
MinIO 3535197f993dea840dcb96302ba4d883dc619097 and mc
11034f9de1e9f993c36fbad961f76f876a753328. Add a test-only source-built fixture
fallback in hack/ensure-minio-fixture.sh and hack/Dockerfile.minio-fixture,
with pinned inputs, retained licenses and source-build labels; _candidate.yml
uses it only for the existing S3 case. It does not build or retag the application
candidate. Acceptance: forced fallback builds from those exact inputs, the full
S3 failure/replay script executes on the preserved candidate, and the final
complete candidate/PR gates pass. No new connector or certification claim.

Loading the dcd5089 Docker 29 archive on Podman exposed another concrete
identity prerequisite: Docker's inspect Id is the OCI index digest 5d12f718…,
while the saved configuration digest is 8a2afbac… (the Podman image Id).
Capture the actual configuration hash from the saved archive as the portable
image identity, retain the provider execution ID separately, preserve the saved
reference for load, and compare configuration plus executable after registry
pull. Config contains the rootfs diff IDs, so it binds the filesystem as well.
Allowed files remain the candidate helper/tests, workflow receipt consumers and
candidate evidence docs. Do not change EV-3 resource metric semantics. Acceptance:
real Docker→Podman load/promotion, wrong configuration rejection, and full current
candidate/PR gates; no rewritten legacy source/date/hash claims.

The local post-run strict check found that filepath.Join incorrectly re-rooted
an absolute -path-evidence directory and the checker accepted zero files.
Fix absolute-directory resolution and require nonempty path evidence in strict
mode, with targeted checker tests. Scope: hack/cmd/check-connector-evidence
main.go/main_test.go and evidence docs. The runner already verified seven actual
candidate path files; the independent freshness/provenance gate must also read
them. Preserve optional structural-only legacy checking and rerun the full current
candidate/aggregate after this validator correction.

The real local-registry promotion kept config/executable bytes but Podman
RepoDigests included cached source aliases alongside the pushed digest. Bind the
receipt to the digest returned by push (Podman digestfile / Docker push output),
pull and verify that immutable reference, and rerun local promotion. Scope stays
in release_candidate.py/tests and this evidence record; no registry publication
outside the task-owned loopback service.
