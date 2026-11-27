# IT-9：可信资源度量与预算控制

> 状态：`active`；归属：EV-3。规划日期：2026-11-27；代码基线：`ee3797c`。
> 用户选择：1 位实现者、两周参考窗口。用户已授权实施；Round: 5/5，EV-3.4 当前 HEAD 的 dev MySQL fixture health 补验收 active。
> 正式验收以 [ROADMAP](../../ROADMAP.zh.md) EV-3 为准；技术方案见 [plan.md](./plan.md)，领取顺序见 [tasks.md](./tasks.md)。

## 问题与用户结果

让小团队维护者能解释 OpenETL-Go 的部署体积、启动与恢复开销，并明确区分观测警告和必须阻断的预算超限。
可信度量是后续容量治理的输入，本轮只关闭 EV-3，不晋级或实现 CH-C5。

当前可核对的事实：

- `hack/bench_baseline.py:image_identity` 将引擎 `inspect.Size` 写入 `bytes`；不同容器引擎／版本的语义不能直接作为同一预算。
- `hardware.runtime_version` 已记录服务端版本，`baseline-run.log` 的自污染已修正；本轮保留这些能力，不重复实现。
- 当前 writer／validator 使用 schema v2；预算超限只 warning，缺失／失败测量会失败。没有 `--enforce-budgets`。
- 构建矩阵是 default、extism、nolua、extism-nolua 四种 `CGO_ENABLED=0` 变体。
- 启动、RSS、离线恢复只测 default 的 native Linux standalone production profile：SQLite、TLS、UID 1001、audit、2 CPU／1 GiB、`GOMAXPROCS=2`。
- 既有恢复数据为 16 个管道 × 100 条记录；3 次启动／恢复，15 个空闲 RSS 样本。它不是持续吞吐或多后端容量认证。

EV-3 原先标为 `blocked_external`，主要原因实际是内部度量缺陷。此次规划将其纠正为 `queued`；
首项可用固定归档与 Python 测试开始。真实引擎、镜像、网络缺失时，只对相应必需验收记录 blocker，不能计为 pass。

## 可观察结果

1. 维护者读取四种构建的报告，可以知道体积如何计算，以及引擎自报值、归档大小和展开层大小各自的含义。
2. 同一个镜像经真实 Docker／Podman 读取时，规范化层体积和内容身份可以对账，CLI 名称或不同 inspect ID 不会制造假差异。
3. 默认模式保留观测警告；显式强制模式下，预算内成功、选定预算超限失败，测量缺失／失败始终失败。
4. schema v2 历史记录仍可读取，不被改写成新的可信度量；schema v3 的方法、原始证据、policy 和源码可追溯。

## 范围与非目标

范围：已有 packaging-startup-restore 测量链路、归档体积计算、报告兼容、预算判定、相关 CI 与证据文档。

非目标：生产运行时准入／内存控制、吞吐／并发扩测、ClickHouse async/native/HTTP 专项、竞品基准、
新 connector、UI 改版、用户研究、distributed 认证、CGO/QuickJS、重做 GA-C1 或自动合并／发布。
不以新测量结论移除项目 beta，不改变任何 connector maturity。

## 验收矩阵

所有项均为实施验收；分项实际结果在 evidence/ 记录，不能用计划或 fixture 代替必需真实环境证据。

| # | 必需验收 | 测试／证据 |
| --- | --- | --- |
| A1 | schema v3 明确定义配置摘要、归档摘要／大小、展开层体积、方法／单位及引擎诊断值；引擎 `Size` 不作为规范化预算的替代值 | 固定 Docker-save／OCI 归档 fixtures；逐层字节和 diff-id 对账；`EV-3.1` |
| A2 | 四个构建变体完整；default 的启动／RSS／恢复 profile、数据和身份／checkpoint 对账保留 | 真实 native Linux 完整 baseline，3 trials、16×100 数据；`EV-3.4` |
| A3 | 至少 default 同一已保存镜像在两个真实不同引擎的规范化结果一致；明确 image-only conformance 与应用运行证据的不同范围 | 配置摘要、diff IDs、层字节、archive hashes、真实 provider/version；`EV-3.2` |
| A4 | v2 历史只读兼容；v1 的旧计时错误仍拒绝；v2、dirty 或缺方法／缺适用 policy 的报告不能通过 v3 强制预算判定 | 兼容与负向 CLI tests；历史原始文件保持不变；`EV-3.1`／`EV-3.3` |
| A5 | 默认预算超限 warning 且 exit 0；强制模式选定预算超限 exit 1；缺失／错误测量两种模式都 exit 1 | 正常、超限、缺字段、错误单位、错误选择器／policy 的 CLI tests；`EV-3.3` |
| A6 | policy 可追溯、按方法／单位／架构／变体限定；至少 default binary 与规范化 image 两个稳定体积项可灰度强制；时间／RSS 保留 warning | 当前 v3 实测依据与余量说明；正常 policy、受控低预算；`EV-3.3`／`EV-3.4` |
| A7 | CI 使用真实新报告与明确 mode/policy；所选体积预算进入灰度强制，方法错误仍失败；不增加 allowlist，不放宽原 source／candidate 聚合门禁 | 当前提交 source + exact-candidate + Delivery Passed run；真实 baseline 与受控超限证据；`EV-3.4` |
| A8 | 不持久化临时凭据、TLS 私钥、config.Env 或完整业务 payload；只清理任务自己的资源，cleanup 失败可见 | 秘密哨兵、异常归档、资源 cleanup 负向验证；`EV-3.1`／`EV-3.2`／`EV-3.4` |
| A9 | 文档、policy、原始报告、实际引擎／源码／构建信息一致；停止强制后仍保留测量有效性校验 | `evidence/` 接受矩阵、预算决策、回滚演练与 diff 检查；`EV-3.4` |

规范化体积采用“所选镜像按 manifest 顺序引用的各层，解压后的 tar 流字节数之和”。
它不等于最终合并 rootfs 占用、容器 writable layer、传输压缩量或实际进程内存。
压缩值小于二进制展开大小不能单独作为错误判据；必须先对齐方法与单位。

## 依赖与 Definition of Ready

| 条件 | 当前判断 | 负责人／解除方式 |
| --- | --- | --- |
| 已有测量 harness、12 个边界测试、四变体构建、default 恢复流程 | 已定位 | 实现者按当前文件 preflight；不改旧数据语义 |
| EV-3.1 固定归档 fixtures、Python CLI | 可准备，首项不依赖远端服务 | 实现者生成小型确定性 fixtures，先核对期望字节／hash |
| 完整 baseline 的构建镜像、网络、容器服务 | 当前 CI native baseline passed；实际 raw archive 与 metadata 已复核 | 实现者／CI 维护者提供标准 runtime 与固定输入；缺失记录具体镜像／错误 |
| 双引擎 image-only 验证 | 实际 Docker 29.4.2／Podman 5.7.0 CI 对账 passed；本地 Podman 5.8.4 另有真实证据 | CI 维护者提供两个 provider 与同一归档；以 `EV-3.2` probe 解除 |
| 预算数值与噪声范围 | 两个 clean native v3 校准与 versioned policy 已通过；默认体积已进入 CI 灰度 | `EV-3.2`／完整 v3 报告后记录基线与余量，`EV-3.4` 回填灰度决定 |
| 用户研究参与者 | UI-E 仍 blocked_external | 不属于本轮依赖或交付 |

真实应用测量必须 native；image-only 对账只证明同一镜像／架构的内容与度量，可以不执行镜像内应用。
Docker 28 样式的 fixture 只证明解析兼容，不宣称完成该引擎版本的运行认证。

## 容量与完成定义

两周参考窗口，实施开始时确定起止。用户选择 1 位实现者；按每天 4 小时专注实现、10 个工作日估算
40 理想小时，承诺 32 小时，预留 8 小时（20%）。这是初始估算，无历史 velocity 支撑；首轮结束校准剩余工时。
承诺四个有序增量，第五轮仅用于本项的补验收／修正／收口，不加入第二个主任务。

EV-3／IT-9 置 `delivered` 必须：全部必需验收 passed；四个增量 delivered；真实双引擎与 native baseline 证据齐全；
预算灰度与时间／RSS warning 边界已记录；文档和 Roadmap 已回填；当前 PR 聚合通过。
缺服务／镜像／网络时保留 active 或记录精确 blocked_external，不以 fixture 或 unit test 替代真实验证。
两周耗尽或 Round 5/5 后做安全交接；不扩大验收范围，不因时间不足宣布交付。

## 交付记录 — 2026-11-27

冻结实现 `191683e`；run 46024237806 的 36/36 jobs success，source＋exact-candidate＋Delivery Passed 全部通过。
A1–A9 全部证据见 [acceptance.md](./evidence/acceptance.md)；四个增量按序 delivered，Round 4/5 收口，未领取第五轮或第二个主任务。
实施参考窗口起点 2026-11-27、两周参考终点 2026-12-11；实际技术验收闭合日 2026-11-27。
没有将参考窗口或估算工时当作完成条件。当前原始归档、source/harness/policy、CLI 负向与 strict binding 都已实际核对。
