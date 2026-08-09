# MyDuckServer 替代 Doris 验证记录(MySQL → MyDuck)

> 验证日期:2026-08-09
> 复现命令:`bash hack/e2e-mysql-myduck.sh`(或 `E2E_SKIP_BUILD=1 bash hack/e2e-mysql-myduck.sh`)
> Spec 样例:`testdata/pipes-mysql-myduck/*.yaml`

## 背景

评估 `apecloud/myduckserver`(DuckDB + MySQL/Postgres wire 协议,单容器 OLAP)能否作为
OpenETL-Go 的 Doris 替代目标。验证聚焦三个风险点:

1. `INSERT ... ON DUPLICATE KEY UPDATE`(OpenETL mysql sink upsert 核心语法)能否被翻译执行;
2. 批量写入吞吐;
3. checkpoint reset 重放时重复数据的吸收(幂等);

**结论:当前三条 OpenETL 通道全部不可用;MyDuck 只适合作为"MyDuck 自带 MySQL binlog 副本"
或外部 COPY/LOAD 工具的落点,不能作为 OpenETL-Go 的 sink。**

## 环境

| 项 | 值 |
| --- | --- |
| MyDuck 镜像 | `apecloud/myduckserver:latest`(DuckDB 内核 v1.1.3,报告 MySQL 8.0.23) |
| MySQL 源 | mysql:8.0(ROW binlog + GTID,13306) |
| OpenETL | dev 镜像 0.2.12-beta.1,standalone + SQLite storage |
| 容器运行时 | podman 5.8.2(macOS,`docker` shim) |
| 网络 | 单容器网络,app 容器与 MyDuck 同网 |

## 验证矩阵

| # | 场景 | OpenETL 通道 | 结果 | 证据 |
| --- | --- | --- | --- | --- |
| 1 | mysql_batch → mysql sink(insert 模式) | MySQL wire | ❌ FAILED | `Error 1105: Binder Error: Ambiguous reference to catalog or schema "etl_analytics"`;目标表 0 行 |
| 2 | mysql_batch → mysql sink(upsert 模式) | MySQL wire | ❌ FAILED | `Parser Error: syntax error at or near "DUPLICATE"` |
| 3 | mysql_cdc → mysql sink(insert+update) | MySQL wire | ❌ FAILED | 同 #1;0 行 |
| 4 | mysql_batch → postgres sink(upsert) | PG wire | ❌ FAILED | `pgx v5 Ping(): ERROR: empty query (SQLSTATE XX000)`,sink 无法打开 |
| 5 | psql 裸协议 INSERT/ON CONFLICT/DELETE | PG wire | ✅ 引擎可用 | 插 2 次同 id(upsert 生效)、DELETE 均成功 |
| 6 | 纯 INSERT 10 万行(单事务,5000 行/条) | MySQL wire | ✅ 引擎能力 | 9–11.5s ≈ 1 万行/秒 |

## 根因(MyDuck 侧方言/协议缺口)

1. **`INSERT IGNORE` 不支持**:OpenETL mysql sink 的 insert 模式生成 `INSERT IGNORE INTO ...`
   (为保证 at-least-once 不因主键冲突中断),MyDuck 解析器报 `Parser Error: syntax error at or near "IGNORE"`。
   —— 与场景 1 直接对应;场景 1 的完整错误由默认库 attach 模型的 binder 歧义叠加产生。
2. **`INSERT ... ON DUPLICATE KEY UPDATE` 不支持**:upsert/increment 模式的生成语句同样 `Parser Error`。
   `REPLACE INTO` 也不支持。
3. **DEFAULT_DB 启动的库存在 catalog/schema 歧义**:容器以 `DEFAULT_DB=etl_analytics` 启动后,
   该库内**普通 `INSERT` 也偶发** `Ambiguous reference to catalog or schema`(同样的语句与库,
   在前几分钟可执行、后边失败);显式 `USE` 下的 `SELECT` 可以,写入绑定不稳定。手动 `CREATE DATABASE`
   的库(如 `e2e_myduck`)无此问题。判断为 MyDuck 多库 attach 实现缺陷。
4. **PG 端空查询(empty query)被当错误**:pgx v5 `Ping()` 发送 `;`,PostgreSQL 视为空响应,
   MyDuck 返回 `XX000` 错误 → OpenETL postgres sink 打开失败。
5. **PG 端 COPY FROM STDIN 终止符处理不兼容**(非交互 psql 下 `\.` 被当数据行)。

## 与 Doris 对比结论(验证后更新)

- 作为 **OpenETL sink**,当前版本 MyDuck 不可用:两种 wire 协议、三种写入模式(insert/upsert/CDC)
  全部被方言层拦截,且是语法/协议级兼容问题,not 性能问题。修好需等待 MyDuck 侧完成 SQLGlot/DuckDB
  翻译(INSERT IGNORE、ON DUPLICATE KEY UPDATE)与 PG `Ping` 空查询兼容,从 2025-01 起
  main 分支无提交,迭代节奏不确定。
- 若只把 MyDuck 当 **MySQL 的分析副本**(它自带的 `SETUP_MODE=REPLICA` 从库模式,事务批量写入),
  与 Doris 定位不冲突但**绕过 OpenETL**,不构成"替代 sink"。
- MyDuck 引擎本身(裸 sql 直连)表现正常:INSERT/UPDATE/DELETE、ON CONFLICT、information_schema、
  事务都通;批量 ≈ 1 万行/秒(单连接)。作为对照,Doris Stream Load 批量导入通常数十万行/秒起。

## 残余风险与后续

- 建议:如确需"MySQL→Doris 类 OLAP"单机会话,优先考虑已被 e2e 覆盖的 ClickHouse
  (`hack/e2e-clickhouse.sh` / `e2e-snapshot-cdc-clickhouse.sh`),其 upsert 语义、
  checkpoint 重放吸收、DLQ/replay 均已在本仓验证;MyDuck 整改后另行复核。
- 复核条件:`INSERT IGNORE` / `ON DUPLICATE KEY UPDATE` 经 MySQL 端口执行成功,
  且 pgx Ping 通过;预期下个验收在此之上直接启用 `mysql_batch/mysql_cdc → (mysql|postgres) sink`。