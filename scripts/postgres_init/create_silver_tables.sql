-- Silver Layer: Cleaned and enriched sales data
CREATE TABLE IF NOT EXISTS silver.sales (
    sale_id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) UNIQUE NOT NULL,
    sale_date DATE NOT NULL,
    sale_hour INTEGER NOT NULL CHECK (sale_hour >= 0 AND sale_hour <= 23),
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(100),
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(100),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL CHECK (unit_price >= 0),
    discount_percentage DECIMAL(5, 2) NOT NULL CHECK (discount_percentage >= 0 AND discount_percentage <= 100),
    discount_amount DECIMAL(10, 2) NOT NULL CHECK (discount_amount >= 0),
    gross_amount DECIMAL(10, 2) NOT NULL CHECK (gross_amount >= 0),
    net_amount DECIMAL(10, 2) NOT NULL CHECK (net_amount >= 0),
    profit_margin DECIMAL(5, 2),
    payment_method VARCHAR(50),
    payment_category VARCHAR(20),
    store_location VARCHAR(100),
    region VARCHAR(50),
    is_weekend BOOLEAN DEFAULT FALSE,
    is_holiday BOOLEAN DEFAULT FALSE,
    ingest_date DATE,
    source_file VARCHAR(255),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customer dimension for silver layer
CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(100),
    first_purchase_date DATE,
    total_purchases INTEGER DEFAULT 0,
    total_revenue DECIMAL(12, 2) DEFAULT 0,
    average_order_value DECIMAL(10, 2),
    customer_segment VARCHAR(20),
    last_purchase_date DATE,
    days_since_last_purchase INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_silver_date ON silver.sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_silver_customer ON silver.sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_silver_product ON silver.sales(product_id);
CREATE INDEX IF NOT EXISTS idx_silver_category ON silver.sales(category);
