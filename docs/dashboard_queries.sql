-- ============================================================
-- All Metabase Dashboard SQL Queries
-- Mini Data Platform - Complete Documentation
-- ============================================================
-- This file contains all SQL queries used across all dashboards
-- Database: PostgreSQL (airflow)
-- 
-- Table of Contents:
--   1. Main Dashboard (ID: 8)
--   2. Executive Summary (ID: 3)
--   3. Sales Overview (ID: 2)
--   4. Product Analytics (ID: 4)
--   5. Customer Analytics (ID: 5)
--   6. Store Performance (ID: 6)
--   7. Data Quality (ID: 7)
-- ============================================================

-- ************************************************************
-- SECTION 1: MAIN DASHBOARD (ID: 8)
-- The main unified dashboard with KPIs and charts
-- ************************************************************

-- -----------------------------------------------------------
-- 1.1 Total Revenue (KPI)
-- -----------------------------------------------------------
SELECT SUM(net_revenue)::numeric AS total_revenue 
FROM gold.daily_sales;

-- -----------------------------------------------------------
-- 1.2 Total Transactions (KPI)
-- -----------------------------------------------------------
SELECT SUM(total_transactions)::int AS total_transactions 
FROM gold.daily_sales;

-- -----------------------------------------------------------
-- 1.3 Total Customers (KPI)
-- -----------------------------------------------------------
SELECT COUNT(DISTINCT customer_id)::int AS total_customers 
FROM gold.customer_analytics;

-- -----------------------------------------------------------
-- 1.4 Average Order Value (KPI)
-- -----------------------------------------------------------
SELECT AVG(average_order_value)::numeric AS avg_order_value 
FROM gold.daily_sales;

-- -----------------------------------------------------------
-- 1.5 Total Products (KPI)
-- -----------------------------------------------------------
SELECT COUNT(DISTINCT product_id)::int AS total_products 
FROM gold.product_performance;

-- -----------------------------------------------------------
-- 1.6 Quarantine Records (KPI)
-- -----------------------------------------------------------
SELECT COUNT(*) AS total_failed 
FROM quarantine.sales_failed;

-- -----------------------------------------------------------
-- 1.7 Daily Sales Trend
-- -----------------------------------------------------------
SELECT 
    sale_date, 
    total_transactions, 
    net_revenue 
FROM gold.daily_sales 
ORDER BY sale_date DESC 
LIMIT 30;

-- -----------------------------------------------------------
-- 1.8 Product Performance
-- -----------------------------------------------------------
SELECT 
    product_name, 
    category, 
    total_revenue, 
    number_of_transactions 
FROM gold.product_performance 
ORDER BY total_revenue DESC 
LIMIT 20;

-- -----------------------------------------------------------
-- 1.9 Category Insights
-- -----------------------------------------------------------
SELECT 
    category, 
    total_revenue, 
    total_products_sold, 
    average_order_value 
FROM gold.category_insights 
ORDER BY total_revenue DESC;

-- -----------------------------------------------------------
-- 1.10 Store Performance
-- -----------------------------------------------------------
SELECT 
    store_location, 
    region, 
    total_revenue, 
    total_transactions 
FROM gold.store_performance 
ORDER BY total_revenue DESC;

-- -----------------------------------------------------------
-- 1.11 Customer Analytics
-- -----------------------------------------------------------
SELECT 
    customer_name, 
    customer_tier, 
    total_revenue, 
    total_purchases 
FROM gold.customer_analytics 
ORDER BY total_revenue DESC 
LIMIT 20;


-- ************************************************************
-- SECTION 2: EXECUTIVE SUMMARY (ID: 3)
-- High-level KPIs and daily trends for executives
-- ************************************************************

-- -----------------------------------------------------------
-- 2.1 Total Revenue
-- -----------------------------------------------------------
SELECT SUM(net_revenue)::numeric AS total_revenue 
FROM gold.daily_sales;

-- -----------------------------------------------------------
-- 2.2 Total Transactions
-- -----------------------------------------------------------
SELECT SUM(total_transactions)::int AS total_transactions 
FROM gold.daily_sales;

-- -----------------------------------------------------------
-- 2.3 Total Customers
-- -----------------------------------------------------------
SELECT COUNT(DISTINCT customer_id)::int AS total_customers 
FROM gold.customer_analytics;

-- -----------------------------------------------------------
-- 2.4 Average Order Value
-- -----------------------------------------------------------
SELECT AVG(average_order_value)::numeric AS avg_order_value 
FROM gold.daily_sales;

-- -----------------------------------------------------------
-- 2.5 Total Products
-- -----------------------------------------------------------
SELECT COUNT(DISTINCT product_id)::int AS total_products 
FROM gold.product_performance;

-- -----------------------------------------------------------
-- 2.6 Daily Sales Trend
-- -----------------------------------------------------------
SELECT 
    sale_date, 
    total_transactions, 
    net_revenue 
FROM gold.daily_sales 
ORDER BY sale_date DESC 
LIMIT 30;


-- ************************************************************
-- SECTION 3: SALES OVERVIEW (ID: 2)
-- Combined sales metrics and trends
-- ************************************************************

-- -----------------------------------------------------------
-- 3.1 Daily Sales
-- -----------------------------------------------------------
SELECT 
    sale_date, 
    total_transactions, 
    net_revenue 
FROM gold.daily_sales 
ORDER BY sale_date DESC 
LIMIT 30;

-- -----------------------------------------------------------
-- 3.2 Product Performance
-- -----------------------------------------------------------
SELECT 
    product_name, 
    category, 
    total_revenue, 
    number_of_transactions 
FROM gold.product_performance 
ORDER BY total_revenue DESC 
LIMIT 20;

-- -----------------------------------------------------------
-- 3.3 Category Insights
-- -----------------------------------------------------------
SELECT 
    category, 
    total_revenue, 
    total_products_sold, 
    average_order_value 
FROM gold.category_insights 
ORDER BY total_revenue DESC;

-- -----------------------------------------------------------
-- 3.4 Customer Analytics
-- -----------------------------------------------------------
SELECT 
    customer_name, 
    customer_tier, 
    total_revenue, 
    total_purchases 
FROM gold.customer_analytics 
ORDER BY total_revenue DESC 
LIMIT 20;

-- -----------------------------------------------------------
-- 3.5 Store Performance
-- -----------------------------------------------------------
SELECT 
    store_location, 
    region, 
    total_revenue, 
    total_transactions 
FROM gold.store_performance 
ORDER BY total_revenue DESC;


-- ************************************************************
-- SECTION 4: PRODUCT ANALYTICS (ID: 4)
-- Detailed product and category performance
-- ************************************************************

-- -----------------------------------------------------------
-- 4.1 Top Products by Revenue
-- -----------------------------------------------------------
SELECT 
    product_name, 
    category, 
    total_revenue, 
    number_of_transactions,
    total_quantity_sold,
    average_unit_price
FROM gold.product_performance 
ORDER BY total_revenue DESC 
LIMIT 20;

-- -----------------------------------------------------------
-- 4.2 Category Breakdown
-- -----------------------------------------------------------
SELECT 
    category, 
    total_revenue, 
    total_products_sold, 
    average_order_value,
    number_of_transactions,
    average_discount_percentage
FROM gold.category_insights 
ORDER BY total_revenue DESC;

-- -----------------------------------------------------------
-- 4.3 Products by Category
-- -----------------------------------------------------------
SELECT 
    product_name,
    category,
    total_revenue,
    total_quantity_sold,
    number_of_transactions
FROM gold.product_performance
ORDER BY category, total_revenue DESC;

-- -----------------------------------------------------------
-- 4.4 Category Performance Bar Chart
-- -----------------------------------------------------------
SELECT 
    category, 
    total_revenue, 
    total_products_sold 
FROM gold.category_insights 
ORDER BY total_revenue DESC;


-- ************************************************************
-- SECTION 5: CUSTOMER ANALYTICS (ID: 5)
-- Customer insights, tiers, and behavior
-- ************************************************************

-- -----------------------------------------------------------
-- 5.1 Customer Overview
-- -----------------------------------------------------------
SELECT 
    customer_name, 
    customer_tier, 
    total_revenue, 
    total_purchases,
    average_order_value,
    favorite_category,
    favorite_payment_method
FROM gold.customer_analytics 
ORDER BY total_revenue DESC 
LIMIT 20;

-- -----------------------------------------------------------
-- 5.2 Top Customers by Revenue
-- -----------------------------------------------------------
SELECT 
    customer_name, 
    customer_tier, 
    total_revenue, 
    total_purchases, 
    favorite_category 
FROM gold.customer_analytics 
ORDER BY total_revenue DESC 
LIMIT 15;

-- -----------------------------------------------------------
-- 5.3 Customer Tier Distribution
-- -----------------------------------------------------------
SELECT 
    customer_tier, 
    COUNT(*) AS count, 
    SUM(total_revenue) AS total_revenue, 
    AVG(total_revenue) AS avg_revenue 
FROM gold.customer_analytics 
GROUP BY customer_tier 
ORDER BY total_revenue DESC;

-- -----------------------------------------------------------
-- 5.4 Customer Tier Pie Chart
-- -----------------------------------------------------------
SELECT 
    customer_tier, 
    COUNT(*) AS count, 
    SUM(total_revenue) AS total_revenue 
FROM gold.customer_analytics 
GROUP BY customer_tier;

-- -----------------------------------------------------------
-- 5.5 Customer Details with Favorite Info
-- -----------------------------------------------------------
SELECT 
    customer_id,
    customer_name, 
    customer_tier, 
    total_revenue, 
    total_purchases,
    favorite_category,
    favorite_payment_method,
    most_visited_store
FROM gold.customer_analytics 
ORDER BY total_revenue DESC;


-- ************************************************************
-- SECTION 6: STORE PERFORMANCE (ID: 6)
-- Store-level metrics and comparisons
-- ************************************************************

-- -----------------------------------------------------------
-- 6.1 Store Performance Overview
-- -----------------------------------------------------------
SELECT 
    store_location, 
    region, 
    total_revenue, 
    total_transactions,
    average_order_value,
    total_customers_served,
    top_selling_category
FROM gold.store_performance 
ORDER BY total_revenue DESC;

-- -----------------------------------------------------------
-- 6.2 Store Revenue Comparison
-- -----------------------------------------------------------
SELECT 
    store_location, 
    region, 
    total_revenue, 
    total_transactions, 
    total_customers_served,
    top_selling_category 
FROM gold.store_performance 
ORDER BY total_revenue DESC;

-- -----------------------------------------------------------
-- 6.3 Stores by Revenue
-- -----------------------------------------------------------
SELECT 
    store_location,
    region,
    total_revenue,
    total_transactions
FROM gold.store_performance
ORDER BY total_revenue DESC;

-- -----------------------------------------------------------
-- 6.4 Store Metrics Table
-- -----------------------------------------------------------
SELECT 
    store_location,
    region,
    total_revenue,
    total_transactions,
    average_order_value,
    total_customers_served
FROM gold.store_performance
ORDER BY total_revenue DESC;


-- ************************************************************
-- SECTION 7: DATA QUALITY (ID: 7)
-- Data quality metrics and quarantine analysis
-- ************************************************************

-- -----------------------------------------------------------
-- 7.1 Quarantine Records (KPI)
-- -----------------------------------------------------------
SELECT COUNT(*) AS total_failed 
FROM quarantine.sales_failed;

-- -----------------------------------------------------------
-- 7.2 Data Quality Score (KPI)
-- -----------------------------------------------------------
SELECT ROUND(
    (SELECT COUNT(*)::numeric FROM silver.sales) / 
    ((SELECT COUNT(*) FROM silver.sales) + (SELECT COUNT(*) FROM quarantine.sales_failed)) * 100
, 2) AS quality_score;

-- -----------------------------------------------------------
-- 7.3 Total Records Processed (KPI)
-- -----------------------------------------------------------
SELECT COUNT(*) AS total FROM silver.sales
UNION ALL
SELECT COUNT(*) FROM quarantine.sales_failed;

-- -----------------------------------------------------------
-- 7.4 Pending Remediation (KPI)
-- -----------------------------------------------------------
SELECT COUNT(*) AS pending 
FROM quarantine.sales_failed 
WHERE replayed = FALSE;

-- -----------------------------------------------------------
-- 7.5 Records by Error Type (Bar Chart)
-- -----------------------------------------------------------
SELECT 
    LEFT(error_reason, 50) AS error_type,
    COUNT(*) AS count 
FROM quarantine.sales_failed 
GROUP BY LEFT(error_reason, 50) 
ORDER BY count DESC;

-- -----------------------------------------------------------
-- 7.6 Failed Records Trend (Line Chart)
-- -----------------------------------------------------------
SELECT 
    DATE(failed_at) AS date,
    COUNT(*) AS failed_count 
FROM quarantine.sales_failed 
GROUP BY DATE(failed_at) 
ORDER BY date;

-- -----------------------------------------------------------
-- 7.7 Records by Source File (Pie Chart)
-- -----------------------------------------------------------
SELECT 
    source_file,
    COUNT(*) AS failed_count 
FROM quarantine.sales_failed 
WHERE source_file IS NOT NULL 
GROUP BY source_file 
ORDER BY failed_count DESC;

-- -----------------------------------------------------------
-- 7.8 Recent Failed Records (Table)
-- -----------------------------------------------------------
SELECT 
    id,
    error_reason,
    failed_at,
    source_file,
    corrected_by,
    corrected_at,
    replayed,
    replayed_at
FROM quarantine.sales_failed 
ORDER BY failed_at DESC 
LIMIT 20;

-- -----------------------------------------------------------
-- 7.9 Remediation Status (Pie Chart)
-- -----------------------------------------------------------
SELECT 
    CASE 
        WHEN replayed = TRUE THEN 'Replayed'
        ELSE 'Pending'
    END AS status,
    COUNT(*) AS count 
FROM quarantine.sales_failed 
GROUP BY 
    CASE 
        WHEN replayed = TRUE THEN 'Replayed'
        ELSE 'Pending'
    END;


-- ************************************************************
-- ADDITIONAL INSIGHTS
-- Bonus queries for deeper analysis
-- ************************************************************

-- -----------------------------------------------------------
-- Revenue by Day of Week
-- -----------------------------------------------------------
SELECT 
    TO_CHAR(sale_date, 'Day') AS day_of_week,
    SUM(net_revenue) AS revenue,
    COUNT(*) AS transactions 
FROM gold.daily_sales 
GROUP BY TO_CHAR(sale_date, 'Day'), EXTRACT(DOW FROM sale_date) 
ORDER BY EXTRACT(DOW FROM sale_date);

-- -----------------------------------------------------------
-- Monthly Revenue Trend
-- -----------------------------------------------------------
SELECT 
    TO_CHAR(sale_date, 'YYYY-MM') AS month,
    SUM(net_revenue) AS revenue,
    SUM(total_transactions) AS transactions 
FROM gold.daily_sales 
GROUP BY TO_CHAR(sale_date, 'YYYY-MM') 
ORDER BY month;

-- -----------------------------------------------------------
-- Daily Sales with Growth Analysis
-- -----------------------------------------------------------
SELECT 
    sale_date, 
    net_revenue, 
    LAG(net_revenue) OVER (ORDER BY sale_date) AS prev_revenue,
    net_revenue - LAG(net_revenue) OVER (ORDER BY sale_date) AS growth 
FROM gold.daily_sales 
ORDER BY sale_date DESC 
LIMIT 30;

-- -----------------------------------------------------------
-- Category Performance Detailed
-- -----------------------------------------------------------
SELECT 
    category, 
    total_revenue, 
    total_products_sold, 
    average_order_value, 
    number_of_transactions 
FROM gold.category_insights 
ORDER BY total_revenue DESC;

-- -----------------------------------------------------------
-- Store Revenue Detailed
-- -----------------------------------------------------------
SELECT 
    store_location,
    region,
    total_revenue,
    total_transactions,
    total_customers_served,
    top_selling_category
FROM gold.store_performance 
ORDER BY total_revenue DESC;


-- ************************************************************
-- SCHEMA REFERENCE
-- ************************************************************

-- Gold Layer Tables
/*
gold.daily_sales
- sale_date              : date
- total_transactions     : integer
- total_quantity_sold    : integer
- gross_revenue         : numeric
- total_discounts       : numeric
- net_revenue           : numeric
- average_order_value   : numeric
- unique_customers      : integer
- unique_products       : integer
- top_category          : varchar

gold.product_performance
- product_id            : integer
- product_name          : varchar
- category             : varchar
- total_quantity_sold   : integer
- total_revenue        : numeric
- average_unit_price   : numeric
- total_discount_given  : numeric
- number_of_transactions: integer
- average_quantity_per_transaction: numeric

gold.customer_analytics
- customer_id           : integer
- customer_name        : varchar
- total_purchases      : integer
- total_revenue        : numeric
- average_order_value  : numeric
- favorite_category    : varchar
- favorite_payment_method: varchar
- most_visited_store  : varchar
- customer_tier       : varchar

gold.store_performance
- store_location       : varchar
- region               : varchar
- total_transactions   : integer
- total_revenue        : numeric
- average_order_value : numeric
- total_customers_served: integer
- top_selling_category: varchar

gold.category_insights
- category             : varchar
- total_products_sold : integer
- total_revenue       : numeric
- average_discount_percentage: numeric
- number_of_transactions: integer
- average_order_value : numeric

quarantine.sales_failed
- id                   : bigint
- payload             : jsonb
- error_reason        : text
- failed_at           : timestamptz
- ingestion_run_id    : uuid
- source_file         : text
- corrected_by        : text
- corrected_at        : timestamptz
- replayed            : boolean
- replayed_at         : timestamptz
*/
