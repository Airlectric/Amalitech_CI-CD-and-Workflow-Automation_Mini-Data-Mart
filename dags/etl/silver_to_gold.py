from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from utils.postgres_hook import PostgresLayerHook

SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

ALERT_EMAIL = Variable.get("alert_email", default_var="daniel.doe@a2sv.org")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email": [ALERT_EMAIL],
}


with DAG(
    dag_id="silver_to_gold",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["etl", "silver", "gold", "star-schema", "aggregation"],
    description="Build star schema: Silver dimensions → Gold facts. Triggered by data_quality_checks.",
) as dag:

    @task
    def populate_silver_customers():
        """Populate silver.customers dimension from sales (Star Schema)"""
        import logging

        logger = logging.getLogger(__name__)
        pg_hook = PostgresLayerHook()

        query = """
            INSERT INTO silver.customers (
                customer_id, customer_name, first_purchase_date, total_purchases,
                total_revenue, average_order_value, customer_segment,
                last_purchase_date, days_since_last_purchase
            )
            SELECT
                customer_id,
                MAX(customer_name) as customer_name,
                MIN(sale_date) as first_purchase_date,
                COUNT(*) as total_purchases,
                SUM(net_amount) as total_revenue,
                AVG(net_amount) as average_order_value,
                CASE
                    WHEN SUM(net_amount) > 10000 THEN 'Platinum'
                    WHEN SUM(net_amount) > 5000 THEN 'Gold'
                    WHEN SUM(net_amount) > 1000 THEN 'Silver'
                    ELSE 'Bronze'
                END as customer_segment,
                MAX(sale_date) as last_purchase_date,
                (CURRENT_DATE - MAX(sale_date))::INTEGER as days_since_last_purchase
            FROM silver.sales
            WHERE customer_id IS NOT NULL
            GROUP BY customer_id
            ON CONFLICT (customer_id) DO UPDATE SET
                customer_name = EXCLUDED.customer_name,
                first_purchase_date = EXCLUDED.first_purchase_date,
                total_purchases = EXCLUDED.total_purchases,
                total_revenue = EXCLUDED.total_revenue,
                average_order_value = EXCLUDED.average_order_value,
                customer_segment = EXCLUDED.customer_segment,
                last_purchase_date = EXCLUDED.last_purchase_date,
                days_since_last_purchase = EXCLUDED.days_since_last_purchase
        """
        try:
            pg_hook.execute_query(query)
            logger.info("Silver customers dimension populated")
        except Exception as e:
            logger.error(f"Failed to populate silver.customers: {e}")
            raise
        return {"table": "silver.customers", "status": "completed"}

    @task
    def populate_silver_products():
        """Populate silver.products dimension from sales (Star Schema)"""
        import logging

        logger = logging.getLogger(__name__)
        pg_hook = PostgresLayerHook()

        query = """
            INSERT INTO silver.products (
                product_id, product_name, category, sub_category,
                min_unit_price, max_unit_price, avg_unit_price,
                total_quantity_sold, total_revenue
            )
            SELECT
                product_id,
                MAX(product_name) as product_name,
                MAX(category) as category,
                MAX(sub_category) as sub_category,
                MIN(unit_price) as min_unit_price,
                MAX(unit_price) as max_unit_price,
                AVG(unit_price) as avg_unit_price,
                SUM(quantity) as total_quantity_sold,
                SUM(net_amount) as total_revenue
            FROM silver.sales
            WHERE product_id IS NOT NULL
            GROUP BY product_id
            ON CONFLICT (product_id) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                category = EXCLUDED.category,
                sub_category = EXCLUDED.sub_category,
                min_unit_price = EXCLUDED.min_unit_price,
                max_unit_price = EXCLUDED.max_unit_price,
                avg_unit_price = EXCLUDED.avg_unit_price,
                total_quantity_sold = EXCLUDED.total_quantity_sold,
                total_revenue = EXCLUDED.total_revenue
        """
        try:
            pg_hook.execute_query(query)
            logger.info("Silver products dimension populated")
        except Exception as e:
            logger.error(f"Failed to populate silver.products: {e}")
            raise
        return {"table": "silver.products", "status": "completed"}

    @task
    def aggregate_daily_sales():
        """Gold fact table: daily sales aggregates"""
        import logging

        logger = logging.getLogger(__name__)
        pg_hook = PostgresLayerHook()

        query = """
            INSERT INTO gold.daily_sales (sale_date, total_transactions, total_quantity_sold, gross_revenue, total_discounts, net_revenue, average_order_value, unique_customers, unique_products, top_category)
            SELECT
                sale_date as sale_date,
                COUNT(*) as total_transactions,
                SUM(quantity) as total_quantity_sold,
                SUM(gross_amount) as gross_revenue,
                SUM(discount_amount) as total_discounts,
                SUM(net_amount) as net_revenue,
                AVG(net_amount) as average_order_value,
                COUNT(DISTINCT customer_id) as unique_customers,
                COUNT(DISTINCT product_id) as unique_products,
                (SELECT category FROM silver.sales s2
                 WHERE s2.sale_date = silver.sales.sale_date
                 GROUP BY category ORDER BY SUM(s2.net_amount) DESC LIMIT 1) as top_category
            FROM silver.sales
            GROUP BY sale_date
            ON CONFLICT (sale_date) DO UPDATE SET
                total_transactions = EXCLUDED.total_transactions,
                total_quantity_sold = EXCLUDED.total_quantity_sold,
                gross_revenue = EXCLUDED.gross_revenue,
                total_discounts = EXCLUDED.total_discounts,
                net_revenue = EXCLUDED.net_revenue,
                average_order_value = EXCLUDED.average_order_value,
                unique_customers = EXCLUDED.unique_customers,
                unique_products = EXCLUDED.unique_products,
                top_category = EXCLUDED.top_category
        """
        try:
            pg_hook.execute_query(query)
            logger.info("Daily sales aggregation completed")
        except Exception as e:
            logger.error(f"Daily sales aggregation failed: {e}")
            raise
        return {"table": "daily_sales", "status": "completed"}

    @task
    def aggregate_product_performance():
        """Gold fact table: product performance"""
        import logging

        logger = logging.getLogger(__name__)
        pg_hook = PostgresLayerHook()

        query = """
            INSERT INTO gold.product_performance (
                product_id, product_name, category,
                total_quantity_sold, total_revenue, average_unit_price,
                total_discount_given, number_of_transactions, average_quantity_per_transaction
            )
            SELECT
                s.product_id,
                MAX(s.product_name) as product_name,
                MAX(s.category) as category,
                SUM(s.quantity) as total_quantity_sold,
                SUM(s.net_amount) as total_revenue,
                AVG(s.unit_price) as average_unit_price,
                SUM(s.discount_amount) as total_discount_given,
                COUNT(*) as number_of_transactions,
                AVG(s.quantity) as average_quantity_per_transaction
            FROM silver.sales s
            GROUP BY s.product_id
            ON CONFLICT (product_id) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                category = EXCLUDED.category,
                total_quantity_sold = EXCLUDED.total_quantity_sold,
                total_revenue = EXCLUDED.total_revenue,
                average_unit_price = EXCLUDED.average_unit_price,
                total_discount_given = EXCLUDED.total_discount_given,
                number_of_transactions = EXCLUDED.number_of_transactions,
                average_quantity_per_transaction = EXCLUDED.average_quantity_per_transaction
        """
        try:
            pg_hook.execute_query(query)
            logger.info("Product performance aggregation completed")
        except Exception as e:
            logger.error(f"Product performance aggregation failed: {e}")
            raise
        return {"table": "product_performance", "status": "completed"}

    @task
    def aggregate_store_performance():
        """Gold fact table: store performance"""
        import logging

        logger = logging.getLogger(__name__)
        pg_hook = PostgresLayerHook()

        query = """
            INSERT INTO gold.store_performance (
                store_location, region,
                total_transactions, total_revenue, average_order_value,
                total_customers_served, top_selling_category
            )
            SELECT
                s.store_location,
                MAX(s.region) as region,
                COUNT(*) as total_transactions,
                SUM(s.net_amount) as total_revenue,
                AVG(s.net_amount) as average_order_value,
                COUNT(DISTINCT s.customer_id) as total_customers_served,
                (SELECT category FROM silver.sales s2
                 WHERE s2.store_location = s.store_location
                 GROUP BY category ORDER BY SUM(s2.net_amount) DESC LIMIT 1) as top_selling_category
            FROM silver.sales s
            GROUP BY s.store_location
            ON CONFLICT (store_location) DO UPDATE SET
                region = EXCLUDED.region,
                total_transactions = EXCLUDED.total_transactions,
                total_revenue = EXCLUDED.total_revenue,
                average_order_value = EXCLUDED.average_order_value,
                total_customers_served = EXCLUDED.total_customers_served,
                top_selling_category = EXCLUDED.top_selling_category
        """
        try:
            pg_hook.execute_query(query)
            logger.info("Store performance aggregation completed")
        except Exception as e:
            logger.error(f"Store performance aggregation failed: {e}")
            raise
        return {"table": "store_performance", "status": "completed"}

    @task
    def aggregate_customer_analytics():
        """Gold fact table: customer analytics"""
        import logging

        logger = logging.getLogger(__name__)
        pg_hook = PostgresLayerHook()

        query = """
            INSERT INTO gold.customer_analytics (
                customer_id, customer_name, total_purchases, total_revenue,
                average_order_value, favorite_category, favorite_payment_method,
                most_visited_store, customer_tier
            )
            SELECT
                s.customer_id,
                MAX(s.customer_name) as customer_name,
                COUNT(*) as total_purchases,
                SUM(s.net_amount) as total_revenue,
                AVG(s.net_amount) as average_order_value,
                (SELECT category FROM silver.sales s2
                 WHERE s2.customer_id = s.customer_id
                 GROUP BY category ORDER BY COUNT(*) DESC LIMIT 1) as favorite_category,
                (SELECT payment_method FROM silver.sales s2
                 WHERE s2.customer_id = s.customer_id
                 GROUP BY payment_method ORDER BY COUNT(*) DESC LIMIT 1) as favorite_payment_method,
                (SELECT store_location FROM silver.sales s2
                 WHERE s2.customer_id = s.customer_id
                 GROUP BY store_location ORDER BY COUNT(*) DESC LIMIT 1) as most_visited_store,
                CASE
                    WHEN SUM(s.net_amount) > 10000 THEN 'Platinum'
                    WHEN SUM(s.net_amount) > 5000 THEN 'Gold'
                    WHEN SUM(s.net_amount) > 1000 THEN 'Silver'
                    ELSE 'Bronze'
                END as customer_tier
            FROM silver.sales s
            WHERE s.customer_id IS NOT NULL
            GROUP BY s.customer_id
            ON CONFLICT (customer_id) DO UPDATE SET
                customer_name = EXCLUDED.customer_name,
                total_purchases = EXCLUDED.total_purchases,
                total_revenue = EXCLUDED.total_revenue,
                average_order_value = EXCLUDED.average_order_value,
                favorite_category = EXCLUDED.favorite_category,
                favorite_payment_method = EXCLUDED.favorite_payment_method,
                most_visited_store = EXCLUDED.most_visited_store,
                customer_tier = EXCLUDED.customer_tier
        """
        try:
            pg_hook.execute_query(query)
            logger.info("Customer analytics aggregation completed")
        except Exception as e:
            logger.error(f"Customer analytics aggregation failed: {e}")
            raise
        return {"table": "customer_analytics", "status": "completed"}

    @task
    def aggregate_category_insights():
        """Gold fact table: category insights"""
        import logging

        logger = logging.getLogger(__name__)
        pg_hook = PostgresLayerHook()

        query = """
            INSERT INTO gold.category_insights (
                category, total_products_sold, total_revenue,
                average_discount_percentage, number_of_transactions, average_order_value
            )
            SELECT
                category,
                SUM(quantity) as total_products_sold,
                SUM(net_amount) as total_revenue,
                AVG(discount_percentage) as average_discount_percentage,
                COUNT(*) as number_of_transactions,
                AVG(net_amount) as average_order_value
            FROM silver.sales
            GROUP BY category
            ON CONFLICT (category) DO UPDATE SET
                total_products_sold = EXCLUDED.total_products_sold,
                total_revenue = EXCLUDED.total_revenue,
                average_discount_percentage = EXCLUDED.average_discount_percentage,
                number_of_transactions = EXCLUDED.number_of_transactions,
                average_order_value = EXCLUDED.average_order_value
        """
        try:
            pg_hook.execute_query(query)
            logger.info("Category insights aggregation completed")
        except Exception as e:
            logger.error(f"Category insights aggregation failed: {e}")
            raise
        return {"table": "category_insights", "status": "completed"}

    # Star Schema: Dimension tables first, then Fact tables
    # Quality gate must pass before any ETL tasks run
    customers = populate_silver_customers()
    products = populate_silver_products()

    # Gold aggregations depend on silver dimensions
    daily_sales = aggregate_daily_sales()
    product_perf = aggregate_product_performance()
    store_perf = aggregate_store_performance()
    customer_analytics = aggregate_customer_analytics()
    category_insights = aggregate_category_insights()

    # Set dependencies: dimensions before facts
    daily_sales.set_upstream([customers, products])
    product_perf.set_upstream([customers, products])
    store_perf.set_upstream([customers, products])
    customer_analytics.set_upstream([customers, products])
    category_insights.set_upstream([customers, products])
