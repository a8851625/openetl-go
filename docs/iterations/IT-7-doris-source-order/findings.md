# IT-7 / T7.1 语义实测结论（findings）

> 状态：`done`（2026-10-30）｜ 环境：`podman` + `docker.io/apache/doris:fe-2.1.11` / `be-2.1.11`（与 `hack/e2e-doris.sh` 一致的版本）｜ 探针：`hack/it7-doris-semantics-probe.sh`（一条命令复现全部结论：`CONTAINER_CLI=podman sh hack/it7-doris-semantics-probe.sh`；原始日志 `data-it7/semantics-probe.log`，`data-it7/` 已被 `.gitignore` 的 `/data-*` 排除，不入库）

## 结论摘要（gate 判定）

| # | 待证实事实 | 结论 | 依据 |
| --- | --- | --- | --- |
| 1 | sequence column 阻止旧写覆盖新写 | **成立** ✅ | 事实 1 / B3-B4 / H3-H6 / K1-K2 |
| 2 | 旧写不复活已删除的行 | **部分成立** ⚠️ | 事实 I（成立）/ 反例 1、3（存在残窗） |
| 3 | Stream Load 可写 `__DORIS_DELETE_SIGN` 墓碑 | **不成立** ❌ | 事实 A/B/C/D/H |
| 4 | 存量表可后设 sequence 属性 | **不成立** ❌ | 事实 4 |

**Gate 判定：主路径成立，但实现范围必须收缩**（删掉"墓碑走 Stream Load"与"混合批放行"两项），并接受两条已证明的残窗。不进入 STOP 分支（事实 1 成立，写入乱序保护可交付）。

---

## 事实 1：sequence column 有效（主收益成立）

```sql
CREATE TABLE it7.t_seq (
  id INT NOT NULL, name VARCHAR(64) NULL, _version BIGINT NOT NULL
) ENGINE=OLAP UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
  "replication_allocation" = "tag.location.default: 1",
  "enable_unique_key_merge_on_write" = "true",
  "function_column.sequence_col" = "_version"
);
```

| 动作 | 结果 |
| --- | --- |
| `INSERT (1,'v2',2)` 然后 `INSERT (1,'v1',1)` | `SELECT` → `1 v2 2` ✅ 旧值被忽略 |
| Stream Load `{"id":1,"name":"old","_version":1}` 在 `_version=3` 之后 | 仍为 `1 new 3` ✅ |
| LARGEINT 列写 `18446744073709551614` 再写 `1` | 仍为 `18446744073709551614` ✅ |

`SHOW CREATE TABLE` 回显包含 `"function_column.sequence_col" = "_version"`，属性生效可见。

## 事实 2：删除的序保护——成立但只覆盖"严格更旧"

merge-on-write + sequence 表，行当前 `_version=100`：

| 动作顺序 | 结果 | 判定 |
| --- | --- | --- |
| `DELETE FROM ... WHERE id=10` → Stream Load `_version=50` | 仍不可见 | ✅ 旧写不复活 |
| 同上 → Stream Load `_version=101` | 可见 `10 s101 101` | ✅ 新事件正确复活 |
| 行 `_version=1`，DELETE → 写 `_version=0` | 不可见 | ✅ |
| 行 `_version=1`，DELETE → 写 `_version=2` | 可见 | ✅ |

**机制推断（与观察一致）**：Doris 的 `DELETE` 在 merge-on-write 表上写一条 delete-sign 行，其**序列值继承被删行当时的 `_version`**，而不是删除事件的序列值。因此保护边界是"严格小于被删行最后版本"的重放。

### 残窗 1（已证明）：删除一个从未存在的行 → 无墓碑

```
DELETE FROM t_n1 WHERE id=99;      -- 影响 0 行，不产生 delete-sign
Stream Load {"id":99,"name":"old-ghost","_version":1}
SELECT → 99 old-ghost 1            -- 幽灵行出现
```

sink 侧含义：若 CDC DELETE 先到而对应行尚未落库（sink 落后、跨表顺序、历史缺失），该 key 没有任何墓碑，之后到达的旧 INSERT 会把行插入。

### 残窗 2（已证明）：同序（equal sequence）后写者胜

```
行 id=1 @_version=1
DELETE FROM t_n3 WHERE id=1;       -- 墓碑序列 = 1
Stream Load {"id":1,"name":"stale-old-key","_version":1}
SELECT → 1 stale-old-key 1         -- 复活
```

即比较是**严格小于**：`seq_new < seq_tombstone` 才被忽略，等值时后到达者生效。主键变更（写新键 + 删旧键）正好落在这一格：旧键墓碑继承旧行序列，重放同序列的旧行会复活旧键。

> 这两条残窗不是实现缺陷，而是 Doris 2.1.11 删除语义的下界；写侧乱序（lost update）**没有**残窗。

## 事实 3：`__DORIS_DELETE_SIGN` 无法通过 Stream Load 写入（否定结论）

四种写法全部 `Status=Success, Message=OK, NumberLoadedRows=1`，但**行都仍然可见**（只被当作普通行/部分列写入）：

| 写法 | 载荷 | 结果 |
| --- | --- | --- |
| JSON，无 `columns` 头 | `{"id":1,"_version":2,"__DORIS_DELETE_SIGN":1}` | 可见，`name=NULL,_version=2` |
| JSON + `columns: id,name,_version,__DORIS_DELETE_SIGN` | 同上 | 可见，`name=NULL,_version=2` |
| JSON + `columns` + `partial_columns: true` | `{"id":5,"_version":2,"__DORIS_DELETE_SIGN":1}` | 可见，`5 p1 2` |
| JSON + `hidden_columns: __DORIS_DELETE_SIGN`（±`partial_columns`） | 同上 | 可见，`1 NULL 2` |
| CSV + `columns` 含 `__DORIS_DELETE_SIGN` | `6,2,1` | 可见，`6 c1 2` |

隐藏列本身也不可见：`SELECT __DORIS_DELETE_SIGN ...` → `ERROR 1054 Unknown column`；`DESC` 不列出；`SHOW CREATE TABLE` 不列出。建表回显含 `"enable_mow_light_delete" = "false"`。

`merge_type: DELETE` 亦无效：`-H "merge_type: DELETE"` 载入 `{"id":7,"_version":3}` 后该行仍为 `7 v2 5`（未删除、未变更）。

**结论：delete-sign 只能由 Doris 自身的 `DELETE` 语句隐式产生，不能在载荷里显式表达。**

## 事实 4：存量表不能后设 sequence 属性（否定结论）

```sql
ALTER TABLE t_alt ADD COLUMN _version BIGINT NOT NULL DEFAULT 0;
-- ERROR 1105: Syntax error ... Encountered: INTEGER LITERAL   （ADD COLUMN 不支持 DEFAULT）
ALTER TABLE t_alt SET ("function_column.sequence_col" = "_version");
-- ERROR 1105: Unknown table property: [function_column.sequence_col]
```

`SHOW CREATE TABLE` 追加列后仍无 `function_column.sequence_col`。**结论：sequence 契约只能在建表时声明 → 存量表必须 fail-closed 并给"新建表 + 回填 + 切换"迁移路径。**

## 事实 5：类型选择——LARGEINT 必需，BIGINT 会静默回绕，DECIMAL 被拒绝

| 类型 | 结果 |
| --- | --- |
| `DECIMAL(20,0)` | **建表失败**：`Sequence type only support integer types and date types` |
| `BIGINT` 写 `9223372036854775808`（2^63） | `INSERT` 返回成功，落盘值 **`-9223372036854775808`（静默回绕成 int64 最小值）** ⚠️ |
| `LARGEINT` 写 `18446744073709551614`（uint64 max-1） | 精确保存，且更新值不覆盖 ✅ |

复现命令：`INSERT INTO it7.t_big VALUES (1,'x',9223372036854775808);` → `SELECT` 得 `1  x  -9223372036854775808`。

**结论：`sequence_type` 默认必须是 `largeint`。`bigint` 只能作为显式 opt-in，且必须在写入前挡住 `> MaxInt64` 的值（否则静默回绕会让"更新的记录"看起来更旧）。**

## 事实 6：`enable_unique_key_merge_on_write` 对删除保护是必需的

| 配置 | 行 `_version=100` → DELETE → 写 `_version=50` | 判定 |
| --- | --- | --- |
| `enable_unique_key_merge_on_write=true` | 不可见 | ✅ 删除受序保护 |
| `enable_unique_key_merge_on_write=false`（同样声明 sequence） | **可见 `20 s50 50`** | ❌ 复活 |

注意：**纯写入**的序比较在 mow=false 时也生效（写 seq1 不覆盖 seq2）。所以 mow 是**删除**保护的前提，不是写入保护的前提。实现仍应强制 mow=true（删除是 CDC 的常规路径）。

## 事实 7：运维细节（对 e2e 与排障有直接影响）

1. **Stream Load 必须打 BE**：FE `:8030` 对 `_stream_load` 返回 `307 Location: http://root:@<be>:8040/...`。curl 即使 `-L -u` 也**不会**在跨主机重定向时重发 Authorization → 得到 `[NOT_AUTHORIZED]no valid Basic authorization`。Go 的 `http.Client` 会使用 Location 里的 userinfo，所以现有 sink（`root` + 空密码）正常。探针因此直连 `BE:8040`。
2. **`root` 空密码在 BE 直连可用**；为可读性探针另建了 `it7/it7pass` 用户验证了等价的授权路径。
3. label 相同的重试会被 Doris 去重（现有 sink 用载荷 hash 作 label，天然受益）。

## 对实现方案的修订（据此收敛 IT-7 范围）

| 原计划 | 实测后 | 理由 |
| --- | --- | --- |
| 删除转成 delete-sign 行并入同一 Stream Load | **删除，保留现有 MySQL `DELETE`** | 事实 3：delete-sign 无法写入 |
| `version_mode=source_order` 下混合写/删不再要求 `allow_mixed_cdc_non_atomic` | **删除该收益** | 删除仍在另一条路径，原子性问题不变 |
| `sequence_type: bigint\|decimal`，默认 bigint | **改为 `largeint`（默认）\| `bigint`（opt-in + 越界拦截）** | 事实 5：DECIMAL 被拒、BIGINT 静默回绕 |
| 存量表：尝试加列 + 设属性 | **fail-closed + 新表迁移指引** | 事实 4：属性无法后设 |
| （新增）merge-on-write 校验 | **必须强制并 fail-closed** | 事实 6 |
| （新增）残窗文档化 | 写进 spec 与组件文档 | 残窗 1、2 |

**保留不变**：序列值只来自 `core.SourceOrder`；默认 `version_mode: off`；写侧乱序保护是本迭代主收益；preflight 能力门禁；`hack/e2e-doris-source-order.sh`（最终落地名；写入侧排序 + 迟到旧序拒绝）。

## 已确证的可交付验收（据此重排 spec 验收项）

| 场景 | 期望终态 | 已被本探针预演 |
| --- | --- | --- |
| 新 UPDATE(seq=2) 后重放旧 UPDATE(seq=1) | 保持 seq=2 的值 | ✅ 事实 1 |
| 行 seq=100 被 DELETE 后重放 seq=50 | 保持删除 | ✅ 事实 I3 |
| 行 seq=100 被 DELETE 后写入 seq=101 | 复活为新值 | ✅ 事实 I4 |
| uint64 全域序列值（Kafka offset / binlog pos） | 精确、不回绕 | ✅ 事实 K |
| mow=false 表 | 启动即 fail-closed | ✅ 事实 6 |
| 存量表无 sequence 属性 | 启动即 fail-closed | ✅ 事实 4 |
