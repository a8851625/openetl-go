# IT-9 领取任务与迭代容量

> 主任务：EV-3，状态 `active`；1 位实现者、两周参考窗口。实施 Round: 3/5。
> 用户已明确授权开发直至本迭代交付；EV-3.1–.2 delivered；EV-3.3 已领取，EV-3.4 queued。状态沿用 `queued / active / blocked_external / delivered / deferred`。
> 同一时间只有一个 EV-3 增量 active。验收见 [spec.md](./spec.md)，技术与允许文件见 [plan.md](./plan.md)。

## 目标、容量与关键路径

Sprint Goal：维护者拿到引擎无关、可追溯的资源度量，并能通过明确 CLI 模式区分观测警告与必须失败的体积超预算。

- Duration：两周参考窗口，实际起止在实施开始时确定。
- Team：1 位实现者；评审／CI 输入由维护者协调，不假定存在第二位实现者。
- Capacity：10 工作日 × 4 专注小时 = 40 理想小时（假设），无历史 velocity 数据。
- Committed：四项初始估算合计 32 小时；Buffer：8 小时／20%，第五轮只作本项安全收口。
- 第 1 轮后核对实际投入；超出容量时缩小当期承诺或交接，既有必需验收不改成可选。

关键路径：EV-3.1 → EV-3.2 → EV-3.3 → EV-3.4；任何当前必需 blocker 未解除时不跳过认证去领更低优先级主任务。

| Round | Increment | 可观察结果 | 初始估算 | 依赖／状态 | 证据落点（实施时生成） |
| --- | --- | --- | --- | --- | --- |
| 1/5 | EV-3.1 | 归档进入 CLI 后输出可复核的规范化体积，完整 harness 支持 v3 与 v2 历史读取 | 8 h | 验收已通过；`delivered` | `evidence/archive-fixtures.md`、parser/CLI logs |
| 2/5 | EV-3.2 | 同一个 default 镜像在真实不同引擎上结果可对账 | 8 h | .1 已交付；真实 Docker/Podman 归档已对账；`delivered` | `evidence/*-image-metrics.json`、`conformance.json` |
| 3/5 | EV-3.3 | 用户选择 warning／enforce，预算与错误输入产生明确退出码 | 6 h | .2 与 AMD64/ARM64 原生 v3 校准已通过；`active` | policy／budget-decision、positive/negative CLI logs |
| 4/5 | EV-3.4 | 当前 CI 执行完整测量和体积灰度，报告、文档、Roadmap 一致 | 10 h | .1–.3；`queued` | baseline-v3、acceptance.md、当前 source＋candidate CI 链接 |
| 5/5 | 本项缓冲／收口 | 只修当前验收暴露的有界问题或补必需 evidence | 8 h buffer | 有具体残余才使用；未领取 | 实际补验收与安全 handoff |

## EV-3.1：从归档到可信报告

Outcome：维护者可通过明确 CLI 检查一个所选镜像的展开层字节，读到方法／单位与身份；不会拿 inspect.Size 充当统一体积。

Allowed：`hack/bench_baseline.py`、`hack/test_bench_baseline.py`、必要的 wrapper；本迭代证据与领取记录。
Non-goals：实际双引擎认证、强制阈值、生产资源限制、新构建变体或修改发布配方。

Acceptance（A1/A4/A8）：

1. 固定 Docker-save／OCI fixtures 的已知 config、层字节和 diff ID 与纯归档 CLI 输出一致；所选镜像以外的平台不计入。
2. 支持的方法／编码清楚，错误 config／缺 blob／hash 错／路径越界／未知编码显式失败；不能 fallback raw.Size 或返回伪造 0。
3. 四种完整构建报告支持 v3 新字段；default runtime 测量的现有输入和结果验证保留；v2 历史默认只读、v1 继续拒绝。
4. 输出和错误路径不泄露 config.Env／秘密哨兵；unit 与 CLI fixture 证据明确为 fixture，尚不计真实 runtime pass。

Data/Rollback：只读归档与生成新 report，无生产状态变化；失败报告不伪装 passed，历史文件不覆写。
回退 parser/writer 时保留新 evidence 和 schema 版本，不能补造 v3 内容给 v2 consumer。

Ready：Python、现有脚本与确定性小型 fixtures；不需要 API 凭据或远端服务。

## EV-3.2：真实双引擎结果对账

Outcome：Docker 与 Podman 实际读取同一镜像后，规范化层体积／内容身份一致；两条 CLI 若连接同一引擎无法冒充通过。

Allowed：新 `hack/e2e-bench-image-metrics.sh`、必要的 baseline metadata／tests、`_gate.yml` 的 probe／工具准备、迭代证据。
Non-goals：跨硬件性能排名、运行 AMD64 Go/QEMU 来代替 native 应用验证、全连接器 e2e 改造或新增生产依赖。

Acceptance（A3/A8）：

1. 以任务拥有的同一 default 保存归档，两个实际 provider 的 config／architecture／diff IDs／展开层字节对账一致。
2. provider IDs、archive 大小与 engine.Size 原样记录，允许不同；真实 server versions 和 provider 必须不同且可辨识。
3. wrong config、缺第二引擎、CLI 别名伪双引擎和传输内容变化都能失败，且只清理自己的资源。
4. 保存 actual command／时间窗口／原始 JSON／归档 hashes；Docker 28 fixture 不被写成真实 Docker 28 certification。

Data/Rollback：只有任务拥有镜像的 load/save/inspect，不启动业务程序，不改 SQL／checkpoint。
失败保留原始归档／expected digest；禁止改期望值接受错误传输。撤 probe 不撤测量有效性检查。

Blocker：CI／环境维护者提供实际 Docker Engine、Podman Engine 与同一原始归档。
Unblock：重跑同一 probe，取得两个 provider 的完整 conformance；没有结果时记 blocked_external，不能记 skipped-as-pass。

## EV-3.3：明确的预算模式与可信 policy

Outcome：用户运行同一 checker，明确知道 warning／enforce 的退出码、适用指标和预算依据。

Allowed：baseline CLI／tests、新 `hack/bench-budgets.json`、迭代预算决策和证据。
Non-goals：所有时间指标转 blocking、全架构套同一阈值、通过低估体积消除告警、生产 admission/backpressure。

Acceptance（A4/A5/A6）：

1. 新 `--budgets`／`--enforce-budgets` 的默认 warning、强制超限、测量错误退出码满足 spec；positive、低预算、缺字段／错误单位负向都可复验。
2. v2、dirty、缺适用 policy／错误方法和异常 budget 值不能通过强制模式；历史观测读取仍兼容。
3. 至少 default binary 与规范化 image 两个稳定项具有明确 policy；基于当前可信 v3 数据记录上限／余量、架构／variant 和 hash，不直接复用旧 image 常量。
4. 时间／RSS 继续 warning，其他构建变体继续完整观测；报告区分数据有效、超预算和不适用，不能把未知标为预算通过。

Data/Rollback：budget 为只读判定，失败不会改业务数据；只撤 enforce flag 即恢复观测模式，测量错误仍失败。
政策回滚留版本记录，不改历史 v2/v3 数值。

Ready：.2 方法验证与可信 v3 基线；没有有效数据时记录精确数据缺口，不先猜阈值。

## EV-3.4：CI、灰度与原 EV-3 收口

Outcome：当前实现提交的 CI 和用户文档使用相同度量／policy，选定体积灰度强制可验证，原 EV-3 必需证据闭合。

Allowed：`_gate.yml` 测量／预算步骤与注释、必要 harness/tests、资源／release checklist、迭代证据与正式 Roadmap／索引。
Non-goals：新增 allowlist／静默 skip、减少四变体或恢复 trials、使用另一镜像冒充发布候选、自动合并／发 tag／移除 beta。

Acceptance（A2/A6/A7/A8/A9）：

1. native Linux 完整 baseline：四变体；default production、SQLite、TLS、UID 1001、audit、2 CPU／1 GiB／GOMAXPROCS=2；3 trials、16×100，恢复身份和 checkpoint 对账通过。
2. 真实两引擎 conformance 与完整 v3 报告、source/harness/policy／归档 fingerprints 对齐；真实 source ref 不改写。
3. 选定体积项在 CI 灰度 enforce，正常结果 0；受控低预算验证确实 1；缺失测量仍 fail；时间／RSS warning 的保留范围与作业说明一致。
4. 原 source、三 storage／frontend、exact-candidate 与 Delivery Passed 聚合全部通过，不以新的 probe success 替代任何旧门禁。
5. 文档与用户已有修改正确融合；`acceptance.md` A1–A9 全部 passed、残余明确；diff／secret／自有资源检查通过，回滚 mode 可复验。

Data/Rollback：仅修改自己的 benchmark fixture／输出；沿用既有维护 CLI，不改生产数据语义。
移除 enforce flag 可回退灰度；不移除有效性校验或通过 allowlist 放过真实失败。

## 领取与交接

领取时先同步当前源码／工作树、原 EV-3 与上一增量证据，再记录一项 active。下面是实施时填写的模板，当前并非领取记录。

```text
Round: <n>/5
Roadmap item: EV-3 (IT-9 / <EV-3.1|EV-3.2|EV-3.3|EV-3.4>)
Profile/path: standalone packaging-startup-restore; image-only conformance 单独标注
Objective: <复制对应 Outcome>
Eligibility: <依赖与所需环境已满足的实际依据>
Scope: <复制 Allowed，限定本增量文件>
Non-goals: <复制 Non-goals>
Acceptance: <本增量编号项>
Evidence: <实际命令、输入数据／版本／hash、CI run；不是计划>
Result: <active|delivered|blocked_external>
Residual/follow-up: <具体残余；没有则 none>
```

受外部条件阻塞的一轮同样计数，写明 missing input、owner 和 unblock command 后交接；不在剩余轮中反复尝试同一 blocker。
第 5 轮仅安全完成当前项／回填 evidence；不得自动领取第六个增量、CH-C5 或 UI-E。
只有四个承诺增量 delivered、A1–A9 全部必需 evidence passed，才更新原 EV-3／IT-9 为 delivered。

## Round 1 Claim — 2026-11-27

```text
Round: 1/5
Roadmap item: EV-3 (IT-9 / EV-3.1)
Profile/path: standalone packaging-startup-restore + fixture-only image-archive metrics
Objective: 同一已知归档经 CLI 产生有方法、字节、配置/层摘要的报告，完整 harness 可写 v3并读取 v2 历史
Eligibility: 用户已明确授权迭代完整实施；P1已交付；当前Python、源脚本与固定fixture可用，不依赖远端服务
Scope: hack/bench_baseline.py, hack/test_bench_baseline.py, 必要wrapper, IT-9证据/领取与Roadmap/索引
Non-goals: 真双引擎认证、强制预算、生产资源限制、发布配方、connector/UI/存储语义改造
Acceptance: A1/A4/A8，见 EV-3.1 的四项验收；CLI和内容/路径/单位/摘要负向，四variant v3与v2历史兼容
Evidence: 待实际 parser/CLI tests 与原始日志；fixture结果不宣称真实runtime认证
Result: active
Residual/follow-up: EV-3.2；EV-3.4 strict bookkeeping 前置已在plan记录
```

## Round 1 Close / Round 2 Claim — 2026-11-27

```text
Round: 1/5
Roadmap item: EV-3 (IT-9 / EV-3.1)
Evidence: evidence/archive-fixtures.md, archive-fixtures.json, r1-parser-tests.log, r1-python312-tests.log, r1-real-archive-metrics.json
Result: delivered (25 tests on local Python and Linux Python 3.12; 4 deterministic CLI cases; actual preserved archive parser exit 0)
Residual/follow-up: EV-3.2 actual provider conformance; EV-3.4 native full baseline. No runtime certification claimed by fixtures.
```

```text
Round: 2/5
Roadmap item: EV-3 (IT-9 / EV-3.2)
Profile/path: image-only Docker/Podman conformance; native baseline is a separate input
Objective: 同一 default 保存归档经过两个真实 provider 后，config、architecture、diff IDs 与展开层字节一致
Eligibility: EV-3.1 passed; GitHub Actions Docker Engine and installable Podman available; local docker is Podman alias and is not a second engine
Scope: hack/e2e-bench-image-metrics.sh, necessary baseline identity/tests, _gate.yml provider tools/probe/retention, IT-9 evidence and status
Non-goals: 启动跨架构程序、不同主机性能排名、生产依赖、预算阈值或 connector changes
Acceptance: EV-3.2 checks 1–4 / A3+A8; wrong config, CLI alias, content mutation and cleanup negative cases
Evidence: Pending actual two-provider JSON, archives and conformance; source baseline may also supply native v3 calibration for EV-3.3
Result: active
Residual/follow-up: EV-3.3 after actual conformance and trusted native v3 baseline
```

### Round 2 local preflight

- Linux Python 3.12: 30 tests, OK, exit 0 (`evidence/r2-python312-tests.log`).
- Podman 5.8.4 on ARM host loads/saves AMD64 image without starting it: passed (`r2-local-podman.json`); six layers, original config and 147,790,336 expanded bytes retained; owned tag cleanup passed.
- Actual CLI alias comparison and wrong-config probes returned the required exit 1 (`r2-alias-negative.json`, `r2-wrong-config.json`).
- Two earlier real format failures are retained; the exact prerequisite and safe legacy alias rules are in plan.md.
- Actual Docker+Podman CI and current native full v3 calibration are still pending; EV-3.2 remains active.

## Round 2 Close / Round 3 Claim — 2026-11-27

```text
Round: 2/5
Roadmap item: EV-3 (IT-9 / EV-3.2)
Evidence: evidence/image-conformance.md, r2-conformance.json, r2-*-image-metrics.json, native calibrations, actual daemonless provider, 30 tests
Result: delivered (two actual different engines; same original archive/config/6 diff IDs/147265024 expanded bytes; CLI comparison exit 0)
Residual/follow-up: EV-3.4 current full CI must rerun fixed daemonless provider code and bounded MySQL fixture TCP readiness; original run failed and is preserved
```

```text
Round: 3/5
Roadmap item: EV-3 (IT-9 / EV-3.3)
Profile/path: standalone packaging-startup-restore; default native linux/amd64 and linux/arm64 sizes
Objective: 显式 policy 让观测超限 exit 0、体积强制超限 exit 1，错误测量/方法/单位/选择器不被预算警告掩盖
Eligibility: EV-3.2 passed; clean native AMD64 CI merge 653ce171 and ARM64 branch d2d7c1d full v3 calibrations passed and share the actual build-input fingerprint
Scope: hack/bench_baseline.py, hack/test_bench_baseline.py, hack/bench-budgets.json, IT-9 budget decision/acceptance/evidence and state
Non-goals: time/RSS blocking, runtime admission/backpressure, extra connectors, cross-sink delivery guarantees, changes to workflows in this round
Acceptance: EV-3.3 1–4 / A4+A5+A6; valid/low/missing/wrong-unit/wrong-policy CLI; v2/dirty/no applicable policy cannot enforce
Evidence: Preserve calibration report hashes; derive limits as measured bytes ×1.25 rounded up to MiB; positive, low-policy, rollback observation and malformed-input outputs
Result: active
Residual/follow-up: EV-3.4 explicit CI enforcement, current source/candidate aggregate and documentation/strict source binding
```
