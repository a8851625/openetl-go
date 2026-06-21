CREATE DATABASE IF NOT EXISTS dzh3136_go;

USE dzh3136_go;

CREATE TABLE IF NOT EXISTS customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200),
    phone VARCHAR(50),
    status VARCHAR(20) DEFAULT 'active',
    amount DECIMAL(12,2) DEFAULT 0.00,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at DATETIME NULL,
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    order_no VARCHAR(64) NOT NULL UNIQUE,
    customer_id INT NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    quantity INT DEFAULT 1,
    price DECIMAL(12,2) NOT NULL,
    order_status VARCHAR(20) DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at DATETIME NULL,
    INDEX idx_customer (customer_id),
    INDEX idx_status (order_status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sku VARCHAR(64) NOT NULL UNIQUE,
    name VARCHAR(200) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    stock INT DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at DATETIME NULL,
    INDEX idx_sku (sku),
    INDEX idx_category (category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO customers (id, name, email, phone, status, amount) VALUES
(1, 'Alice Wang', 'alice@example.com', '13800000001', 'active', 15000.00),
(2, 'Bob Li', 'bob@example.com', '13800000002', 'active', 25000.00),
(3, 'Charlie Zhang', 'charlie@example.com', '13800000003', 'inactive', 5000.00),
(4, 'Diana Chen', 'diana@example.com', '13800000004', 'active', 32000.00),
(5, 'Eve Liu', 'eve@example.com', '13800000005', 'active', 8000.00);

INSERT INTO orders (id, order_no, customer_id, product_name, quantity, price, order_status) VALUES
(1, 'ORD-2024-001', 1, 'Laptop Pro', 1, 8999.00, 'completed'),
(2, 'ORD-2024-002', 1, 'Mouse Wireless', 2, 199.00, 'completed'),
(3, 'ORD-2024-003', 2, 'Monitor 4K', 1, 3499.00, 'shipped'),
(4, 'ORD-2024-004', 3, 'Keyboard', 1, 599.00, 'pending'),
(5, 'ORD-2024-005', 4, 'Headphones', 1, 1299.00, 'completed'),
(6, 'ORD-2024-006', 4, 'USB-C Hub', 3, 299.00, 'shipped'),
(7, 'ORD-2024-007', 5, 'Tablet', 1, 4299.00, 'pending');

INSERT INTO products (id, sku, name, category, price, stock) VALUES
(1, 'ELEC-001', 'Laptop Pro', 'Electronics', 8999.00, 50),
(2, 'ELEC-002', 'Monitor 4K', 'Electronics', 3499.00, 30),
(3, 'ELEC-003', 'Tablet', 'Electronics', 4299.00, 25),
(4, 'ACC-001', 'Mouse Wireless', 'Accessories', 199.00, 200),
(5, 'ACC-002', 'Keyboard', 'Accessories', 599.00, 150),
(6, 'ACC-003', 'Headphones', 'Accessories', 1299.00, 80),
(7, 'ACC-004', 'USB-C Hub', 'Accessories', 299.00, 120);

GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'sync_user'@'%';
FLUSH PRIVILEGES;
