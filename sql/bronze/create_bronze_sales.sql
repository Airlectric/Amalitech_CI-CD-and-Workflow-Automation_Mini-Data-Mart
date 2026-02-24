-- Bronze Layer: Raw sales data (ingested directly from parquet files)
CREATE TABLE IF NOT EXISTS bronze_sales (
    transaction_id VARCHAR(50) PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(100),
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(100),
    category VARCHAR(50),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL CHECK (unit_price >= 0),
    discount_percentage DECIMAL(5, 2) NOT NULL CHECK (discount_percentage >= 0 AND discount_percentage <= 100),
    discount_amount DECIMAL(10, 2) NOT NULL CHECK (discount_amount >= 0),
    total_amount DECIMAL(10, 2) NOT NULL CHECK (total_amount >= 0),
    payment_method VARCHAR(50),
    store_location VARCHAR(100),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_bronze_timestamp ON bronze_sales(timestamp);
CREATE INDEX IF NOT EXISTS idx_bronze_customer_id ON bronze_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_bronze_product_id ON bronze_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_bronze_category ON bronze_sales(category);
