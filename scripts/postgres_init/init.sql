-- Create metabase database
SELECT 'CREATE DATABASE metabase'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec

-- Create Silver schema  
CREATE SCHEMA IF NOT EXISTS silver;

-- Create Gold schema
CREATE SCHEMA IF NOT EXISTS gold;

-- Create metadata schema for tracking ingestion state
CREATE SCHEMA IF NOT EXISTS metadata;

-- Ingestion metadata table
CREATE TABLE IF NOT EXISTS metadata.ingestion_metadata (
    file_path TEXT PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    checksum TEXT,
    ingest_date DATE NOT NULL,
    status TEXT NOT NULL DEFAULT 'NEW',
    record_count INTEGER,
    processed_at TIMESTAMP,
    airflow_run_id TEXT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_metadata_status ON metadata.ingestion_metadata(status);
CREATE INDEX IF NOT EXISTS idx_metadata_ingest_date ON metadata.ingestion_metadata(ingest_date);
CREATE INDEX IF NOT EXISTS idx_metadata_dataset ON metadata.ingestion_metadata(dataset_name);

-- Create Silver tables
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

CREATE TABLE IF NOT EXISTS silver.products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    min_unit_price DECIMAL(10, 2),
    max_unit_price DECIMAL(10, 2),
    avg_unit_price DECIMAL(10, 2),
    total_quantity_sold INTEGER DEFAULT 0,
    total_revenue DECIMAL(15, 2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_silver_date ON silver.sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_silver_customer ON silver.sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_silver_product ON silver.sales(product_id);
CREATE INDEX IF NOT EXISTS idx_silver_category ON silver.sales(category);

-- Create Gold tables
CREATE TABLE IF NOT EXISTS gold.daily_sales (
    sale_date DATE PRIMARY KEY,
    total_transactions INTEGER,
    total_quantity_sold INTEGER,
    gross_revenue DECIMAL(15, 2),
    total_discounts DECIMAL(15, 2),
    net_revenue DECIMAL(15, 2),
    average_order_value DECIMAL(10, 2),
    unique_customers INTEGER,
    unique_products INTEGER,
    top_category VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.product_performance (
    product_id VARCHAR(50),
    product_name VARCHAR(100),
    category VARCHAR(50),
    total_quantity_sold INTEGER,
    total_revenue DECIMAL(15, 2),
    average_unit_price DECIMAL(10, 2),
    total_discount_given DECIMAL(15, 2),
    number_of_transactions INTEGER,
    average_quantity_per_transaction DECIMAL(10, 2),
    PRIMARY KEY (product_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.customer_analytics (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(100),
    total_purchases INTEGER,
    total_revenue DECIMAL(15, 2),
    average_order_value DECIMAL(10, 2),
    favorite_category VARCHAR(50),
    favorite_payment_method VARCHAR(50),
    most_visited_store VARCHAR(100),
    customer_tier VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.store_performance (
    store_location VARCHAR(100) PRIMARY KEY,
    region VARCHAR(50),
    total_transactions INTEGER,
    total_revenue DECIMAL(15, 2),
    average_order_value DECIMAL(10, 2),
    total_customers_served INTEGER,
    top_selling_category VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.category_insights (
    category VARCHAR(50) PRIMARY KEY,
    total_products_sold INTEGER,
    total_revenue DECIMAL(15, 2),
    average_discount_percentage DECIMAL(5, 2),
    number_of_transactions INTEGER,
    average_order_value DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create views
CREATE OR REPLACE VIEW gold.v_monthly_sales AS
SELECT 
    DATE_TRUNC('month', sale_date) AS month,
    COUNT(*) AS total_transactions,
    SUM(quantity) AS total_quantity,
    SUM(gross_amount) AS gross_revenue,
    SUM(discount_amount) AS total_discounts,
    SUM(net_amount) AS net_revenue,
    AVG(net_amount) AS avg_order_value
FROM silver.sales
GROUP BY DATE_TRUNC('month', sale_date)
ORDER BY month;

CREATE OR REPLACE VIEW gold.v_regional_sales AS
SELECT 
    region,
    store_location,
    COUNT(*) AS total_transactions,
    SUM(net_amount) AS total_revenue,
    AVG(net_amount) AS avg_order_value,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM silver.sales
GROUP BY region, store_location
ORDER BY total_revenue DESC;

-- Grant privileges
GRANT USAGE ON SCHEMA silver TO airflow;
GRANT USAGE ON SCHEMA gold TO airflow;
GRANT USAGE ON SCHEMA metadata TO airflow;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA metadata TO airflow;

-- Grant privileges for metabase
GRANT ALL PRIVILEGES ON DATABASE metabase TO airflow;
