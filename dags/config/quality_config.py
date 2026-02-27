"""
Configuration for Data Quality Checks
"""

SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

QUALITY_RULES = {
    "sales": {
        "not_null": [
            "transaction_id",
            "sale_date",
            "product_id",
            "quantity",
            "unit_price",
            "net_amount"
        ],
        "positive_values": [
            "quantity",
            "unit_price",
            "net_amount",
            "gross_amount"
        ],
        "value_ranges": {
            "quantity": {"min": 1, "max": 1000},
            "unit_price": {"min": 0.01, "max": 100000},
            "net_amount": {"min": 0.01, "max": 1000000},
            "gross_amount": {"min": 0.01, "max": 1000000},
            "sale_hour": {"min": 0, "max": 23}
        },
        "unique": ["transaction_id"]
    },
    "customers": {
        "not_null": ["customer_id"],
        "unique": ["customer_id"]
    }
}

TABLES_TO_CHECK = ["sales", "customers"]

ALERT_DEFAULTS = {
    "retries": 2,
    "retry_delay_minutes": 5,
    "schedule": "0 */3 * * *"
}
