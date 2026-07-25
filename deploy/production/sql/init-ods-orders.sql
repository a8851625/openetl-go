-- ODS 目标表（Kafka → MySQL upsert）
-- 在业务 MySQL（ODS_MYSQL_*）上执行，不是 OpenETL 元数据库。
--
-- 要求：
--   - 主键/唯一键与管道 pk_columns 一致（默认 id）
--   - 字段名与 Kafka JSON 字段对齐（或在 transform project/rename 中映射）

CREATE DATABASE IF NOT EXISTS ods
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE ods;

CREATE TABLE IF NOT EXISTS orders (
  id           BIGINT       NOT NULL COMMENT '业务主键，对应消息 id',
  status       VARCHAR(32)  NULL,
  amount       DECIMAL(18,4) NULL,
  currency     VARCHAR(8)   NULL,
  user_id      BIGINT       NULL,
  updated_at   DATETIME(3)  NULL,
  _synced_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                             ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_orders_user_id (user_id),
  KEY idx_orders_updated_at (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Kafka orders ODS upsert target';

-- 同步账号最小权限示例（按实际主机收紧）：
-- CREATE USER IF NOT EXISTS 'sync'@'%' IDENTIFIED BY 'change-me';
-- GRANT SELECT, INSERT, UPDATE ON ods.orders TO 'sync'@'%';
-- FLUSH PRIVILEGES;
