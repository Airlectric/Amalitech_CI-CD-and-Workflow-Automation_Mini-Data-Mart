-- Gold Layer: Aggregated analytics tables

-- 1. Daily Sales Summary
CREATE TABLE IF NOT EXISTS gold_daily_sales (
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

-- 2. Product Performance
CREATE TABLE IF NOT EXISTS gold_product_performance (
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

-- 3. Customer Analytics
CREATE TABLE IF NOT EXISTS gold_customer_analytics (
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

-- 4. Store Performance
CREATE TABLE IF NOT EXISTS gold_store_performance (
    store_location VARCHAR(100) PRIMARY KEY,
    region VARCHAR(50),
    total_transactions INTEGER,
    total_revenue DECIMAL(15, 2),
    average_order_value DECIMAL(10, 2),
    total_customers_served INTEGER,
    top_selling_category VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. Category Insights
CREATE TABLE IF NOT EXISTS gold_category_insights (
    category VARCHAR(50) PRIMARY KEY,
    total_products_sold INTEGER,
    total_revenue DECIMAL(15, 2),
    average_discount_percentage DECIMAL(5, 2),
    number_of_transactions INTEGER,
    average_order_value DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Materialized views for fast querying (PostgreSQL 9.3+)
-- Note: These need to be refreshed periodically

-- Monthly Trend View
CREATE OR REPLACE VIEW v_gold_monthly_sales AS
SELECT 
    DATE_TRUNC('month', sale_date) AS month,
    COUNT(*) AS total_transactions,
    SUM(quantity) AS total_quantity,
    SUM(gross_amount) AS gross_revenue,
    SUM(discount_amount) AS total_discounts,
    SUM(net_amount) AS net_revenue,
    AVG(net_amount) AS avg_order_value
FROM silver_sales
GROUP BY DATE_TRUNC('month', sale_date)
ORDER BY month;

-- Regional Sales View
CREATE OR REPLACE VIEW v_gold_regional_sales AS
SELECT 
    region,
    store_location,
    COUNT(*) AS total_transactions,
    SUM(net_amount) AS total_revenue,
    AVG(net_amount) AS avg_order_value,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM silver_sales
GROUP BY region, store_location
ORDER BY total_revenue DESC;
