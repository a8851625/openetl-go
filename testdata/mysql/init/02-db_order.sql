-- db_order: order/user + daily shard demo
CREATE DATABASE IF NOT EXISTS db_order;

USE db_order;

CREATE TABLE IF NOT EXISTS user (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200),
    phone VARCHAR(50),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `order` (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    order_no VARCHAR(64) NOT NULL UNIQUE,
    total_amount DECIMAL(12,2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_user (user_id),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS daliy_order_20260617 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    quantity INT DEFAULT 1,
    price DECIMAL(12,2) NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO user (id, name, email, phone) VALUES
(1, 'Alice Wang', 'alice@example.com', '13800000001'),
(2, 'Bob Li', 'bob@example.com', '13800000002'),
(3, 'Charlie Zhang', 'charlie@example.com', '13800000003');

INSERT INTO `order` (id, user_id, order_no, total_amount, status) VALUES
(1, 1, 'ORD-20260617-001', 8999.00, 'completed'),
(2, 1, 'ORD-20260617-002', 299.00, 'shipped'),
(3, 2, 'ORD-20260617-003', 4299.00, 'pending'),
(4, 3, 'ORD-20260617-004', 1599.00, 'completed');

INSERT INTO daliy_order_20260617 (id, order_id, product_name, quantity, price) VALUES
(1, 1, 'Laptop Pro', 1, 8999.00),
(2, 1, 'Mouse Wireless', 1, 199.00),
(3, 2, 'Monitor 4K', 1, 3499.00),
(4, 3, 'Keyboard', 2, 299.50),
(5, 4, 'Headphones', 1, 1299.00);
