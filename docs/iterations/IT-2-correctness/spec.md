# IT-2：正确性（控制面真值 + 数据面身份与顺序）

> 状态：`queued` | 依赖：IT-1 | 归属 roadmap 条目：RA-1、RA-2、RA-3、GAP-7.1/.2/.3

## 问题陈述

本迭代的四组缺口共享一个根因：**运行时行为与持久化真值不一致**，且不一致不产生任何信号。

**控制面（配置与生命周期的真值）**：

- `internal/etl/server/server.go:474-572` `RestoreFromDB` 有 11 处 `continue` 搭配
  `g.Log().Warningf`。YAML 解析、connection 解析、`ValidateSpec`、`newRunner` 任一失败，
  该 pipeline 就不进入内存、API、health 与 scheduler，而函数仍返回 `nil`、服务启动成功。
  DB 里有这行，运行时对用户完全不可见。用户看到的是「任务没了」，不是「任务失败了」。
- `internal/etl/server/server.go:734-795` `StartAll` 对所有非 deferred pipeline 无条件
  `runner.Start(ctx)`，全程**只写不读**状态（`UpdatePipelineStatus` 出现 4 次，无一处读取
  `row.Status` 作为门禁）；`RestoreFromDB` 也不按状态过滤。用户显式 stop / pause 的 pipeline
  在进程重启后会自行运行 —— 对 CDC 链路这是「以为停了、其实在持续写目标库」。
  同时 checkpoint reset/set 可对运行中 pipeline 调用，in-flight checkpoint 可能立即覆盖 reset。

**数据面（行身份与事件顺序的真值）**：

- `internal/etl/sink/clickhouse.go:1210` `nextVersion()` 返回
  `(time.Now().UnixMilli() << 20) | (counter & 0xFFFFF)`，调用于 `:879`（native）与 `:911`（HTTP）。
  版本表达的是「何时被写入 ClickHouse」，不是「源库何时发生」。DLQ replay 或 checkpoint reset
  重放一条较旧的 UPDATE 时，它取得高于现存新行的 `_version`，ReplacingMergeTree 合并后**旧值胜出**。
- GAP-7 记录的身份缺口同源：`kafka.go` 的 `tryCanalJSON` 可产生**部分复合 Key**；
  ClickHouse `pkColumnsByTable` 对空 Key 做静态回退，可能误定位行。

**为什么合并为一个迭代**：RA-1 需要从 `core.Record.Metadata` 派生稳定源序，GAP-7 需要从同一
`Metadata` 保证身份完整 —— 两者要改的是**同一个契约层**。分开做会导致该层被改两次，且第一次
的验收会在第二次被推翻。

RA-1 是当前全部已知缺口中**唯一产生静默错值**的一项：丢数据和重复数据可由行数/主键对账发现，
被旧值覆盖的行在计数和主键维度上都表现正常，只有逐字段比对才能发现。

## 可观察结果

1. 重启后 API 与 health 显示 DB 中**全部** pipeline，恢复失败的显示为 `restore_failed`
   并带可操作错误，不会静默消失。
2. 显式 stop / pause 的 pipeline 在进程重启后**保持停止**，不产生任何 source 读取或 sink 写入。
3. 对运行中 pipeline 调用 checkpoint reset 返回明确冲突错误，不再出现 reset 被 in-flight
   checkpoint 立即覆盖。
4. 乱序 replay（DLQ replay、checkpoint reset）不再让旧值覆盖新值；无法建立全序的组合在
   preflight 阶段被拒绝，而不是静默回退到时钟序。
5. 身份不完整（空 Key、部分复合 Key）的正常 DML 不会流向 identity-aware sink，而是成为
   带原因的 DLQ 事件。

## 验收标准

| # | 验收标准 | 判定方式 |
| --- | --- | --- |
| 1 | 六类 restore 失败均可见 | DAG yaml / 线性 yaml / DAG connection / 线性 connection / validate / newRunner 各有注入测试，重启后仍在 API 与 health 中，状态 `restore_failed` |
| 2 | strict 模式 fail-closed | production profile 下 `restore_failed` 非零时启动失败并打印完整清单；非 strict 启动成功但 health `degraded` |
| 3 | 修复后可恢复 | 补齐 connection / 修正 YAML 后重启，pipeline 恢复为原 desired state |
| 4 | desired state 持久化生效 | stop 后重启进程，pipeline 保持 stopped 且无 source 读取与 sink 写入；pause 同理 |
| 5 | 生命周期错误可见 | desired state 持久化失败时 API 返回非 2xx，内存状态回到最后成功值 |
| 6 | reset fencing 生效 | 对运行中 pipeline reset 返回冲突错误；旧 generation 的 in-flight checkpoint 写入被拒绝且可观测 |
| 7 | source-specific reset 语义准确 | 每类 source 的 reset 行为有测试与文档条目，与 OpenAPI 描述一致 |
| 8 | 乱序 replay 不产生错值 | 旧 UPDATE 晚于新 UPDATE（DLQ replay 与 checkpoint reset 两条路径）时 `FINAL` 查询保持新值；旧 INSERT 晚于 DELETE 不重生成已删除行 |
| 9 | 源序派生一致 | 复合主键、主键变更 UPDATE、多表 fan-out 下版本派生一致；时钟回拨与并发写入无版本倒挂（`-race`） |
| 10 | 无源序能力的组合被拒 | validate/preflight 阶段拒绝，错误指明缺失的 metadata 字段 |
| 11 | 完整 Key 契约生效 | 单列与复合 `pkNames` 的 INSERT/UPDATE/DELETE 均产出完整 Key；缺列时不再因「至少一列存在」生成部分 Key |
| 12 | 主键变更 UPDATE 正确 | 使用完整 Before Key；仅已登记的缺列语义允许以 `Data` 补齐，未登记 / `old` 缺失 / 冲突输入进入身份错误或 DLQ |
| 13 | DLQ 身份上下文完整 | 新 DLQ 条目持久化原始 payload、声明 `pkNames`、缺键原因、冻结的 format-contract ID、原始 `old` 状态、目标表/库与 replay provenance；重启、备份恢复、API 查询后不丢失 |
| 14 | 受控 replay 门禁 | 可重建的记录先补全 Key 再走普通 sink 路径；不可证明身份的保持 repair/quarantine；静态 PK 回退仅接受带 legacy provenance 且 `pkNames` 与静态键集合精确一致的记录 |
| 15 | metadata-PK sink 语义一致 | 共享 conformance 覆盖单键/复合键、INSERT/UPDATE/DELETE、主键变更、跨表 fan-out、空/部分 Key 拒绝、合格 legacy replay；每个声明支持 metadata PK 的 sink 均通过 |
| 16 | 跨路径容器认证 | Kafka → ClickHouse 与 Kafka → PostgreSQL 多表路径完成容器 e2e，含 checkpoint reset/replay、sink outage → DLQ → replay、复合键、key-changing UPDATE |

## 非目标

- 不实现跨 sink exactly-once；不承诺三方原子性。
- 不为 append-only source（file/HTTP 等）虚构主键或版本；对它们只做 preflight 阻断。
- 不把 ClickHouse 的 ReplacingMergeTree、mutation、native/HTTP 协议或 `async_insert`
  推广为通用实现。
- 不批量自动修复历史 DLQ；不允许 replay provenance 成为绕过完整 Key 的通道。
- 不实现多活 HA 的 leader election；distributed fencing 属 PR-D1，不在本迭代。
- 不在本迭代做 ClickHouse 吞吐基准（属 IT-3）。

## 交付约束

1. **不得把可能重复变成可能丢失**。源序版本、身份契约、fencing 三处改动都触及 replay 路径，
   任何一处不得以「减少重复」为由跳过未确认的记录。
2. **上线顺序有强制要求**：普通流切换为 fail-closed 之前，必须先识别现存的空 Key DLQ 记录。
   切换后仍保留原始事件与 checkpoint；回滚不得删除 DLQ 或把已拒绝事件静默确认。
3. **静态 PK 兼容必须可观测、可撤销**：需有日志、指标与配置开关；但关闭兼容不能使正常流
   重新接受无身份 DML。
4. **共享契约层先于消费方交付**。源序派生与身份完整性的公共实现必须先落地并通过 conformance，
   再接入各 sink，避免每个 sink 各自实现一套。
5. **不得用单个 sink 的证据宣称全连接器成熟**。每个声明支持 metadata PK 的 sink 逐一通过
   共享 conformance。
6. **RA-1 的容器 e2e 必须在 IT-1 的框架内执行**。若因故提前于 IT-1 领取，则需以
   `hack/*.sh` 一次性人工闭合，并在证据中显式标注该例外及后续由 IT-1 接管。

## 依赖与前置

| 类型 | 内容 | 状态 |
| --- | --- | --- |
| 迭代依赖 | IT-1（乱序 replay 矩阵与跨路径 e2e 需要其框架） | `queued` |
| 迭代依赖 | IT-1/T1.9（GAP-1/GAP-3 的 PG metadata 已在真实实例验证） | `queued` |
| 外部输入 | 无 | — |
| 待决策 | RA-1 是否提升为当前最高优先级（ROADMAP「待用户决策」）。未答复前按 (a) 执行：RA-1 排在 IT-1 之后 | 未决 |

## 完成定义（DoD）

1. `tasks.md` 中全部 task 状态为 `done`，每项有实际执行的证据记录。
2. 上方 16 项验收标准全部 `passed`。
3. ROADMAP 中 RA-1、RA-2、RA-3、GAP-7.1/.2/.3 置 `delivered`；GAP-7 父条目标注收口。
4. [reliability-certification.md](../../reliability-certification.md)、
   [path-contract.md](../../path-contract.md)、[etl-idempotency.md](../../etl-idempotency.md)、
   [etl-api.md](../../etl-api.md)、`docs/openapi.yaml`、`docs/components/sink-clickhouse.md`
   已在同一交付内更新。
5. 每条受影响路径的证据 JSON 已由 IT-1 的框架重新产出并绑定当前 commit。
