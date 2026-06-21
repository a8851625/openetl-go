CREATE DATABASE IF NOT EXISTS db_order;

USE db_order;

CREATE TABLE IF NOT EXISTS order_denormalized (
    id Int32,
    user_id Int32,
    user_name String,
    user_email Nullable(String),
    order_no String,
    total_amount Nullable(Decimal(12,2)),
    status Nullable(String),
    created_at Nullable(DateTime),
    updated_at Nullable(DateTime),
    _version Int64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS dayliy_order (
    id Int32,
    order_id Int32,
    product_name String,
    quantity Nullable(Int32),
    price Nullable(Decimal(12,2)),
    created_at Nullable(DateTime),
    updated_at Nullable(DateTime),
    _version Int64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id
SETTINGS index_granularity = 8192;
