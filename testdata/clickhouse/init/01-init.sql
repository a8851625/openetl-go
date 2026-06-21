CREATE DATABASE IF NOT EXISTS dzh3136_go;
CREATE DATABASE IF NOT EXISTS sync_monitor;

USE dzh3136_go;

CREATE TABLE IF NOT EXISTS customers (
    id Int32,
    name String,
    email Nullable(String),
    phone Nullable(String),
    status Nullable(String),
    amount Nullable(Decimal(12,2)),
    created_at Nullable(DateTime),
    updated_at Nullable(DateTime),
    deleted_at Nullable(DateTime),
    _version Int64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS orders (
    id Int32,
    order_no String,
    customer_id Nullable(Int32),
    product_name Nullable(String),
    quantity Nullable(Int32),
    price Nullable(Decimal(12,2)),
    order_status Nullable(String),
    created_at Nullable(DateTime),
    updated_at Nullable(DateTime),
    deleted_at Nullable(DateTime),
    _version Int64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS products (
    id Int32,
    sku String,
    name String,
    category Nullable(String),
    price Nullable(Decimal(12,2)),
    stock Nullable(Int32),
    created_at Nullable(DateTime),
    updated_at Nullable(DateTime),
    deleted_at Nullable(DateTime),
    _version Int64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id
SETTINGS index_granularity = 8192;
