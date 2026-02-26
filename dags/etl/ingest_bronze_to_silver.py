from datetime import datetime, timedelta
from typing import List, Optional

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

import sys
sys.path.insert(0, "/opt/airflow/dags")

from utils.minio_hook import MinIOHook
from utils.postgres_hook import PostgresLayerHook
from utils.duckdb_utils import DuckDBValidator, create_validator


BRONZE_BUCKET = "bronze"
SILVER_SCHEMA = "silver"
METADATA_SCHEMA = "metadata"

DATASET_SPECS = {
    "sales": {
        "expected_columns": [
            "transaction_id", "sale_date", "sale_hour", "customer_id", "customer_name",
            "product_id", "product_name", "category", "sub_category", "quantity",
            "unit_price", "discount_percentage", "discount_amount", "gross_amount",
            "net_amount", "profit_margin", "payment_method", "payment_category",
            "store_location", "region", "is_weekend", "is_holiday"
        ],
        "required_columns": ["transaction_id", "sale_date", "product_id", "quantity", "unit_price", "net_amount"],
        "unique_columns": ["transaction_id"],
        "silver_table": "sales"
    }
}


def get_dataset_spec(dataset: str) -> dict:
    return DATASET_SPECS.get(dataset, {})


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="ingest_bronze_to_silver",
    start_date=datetime(2026, 1, 1),
    schedule="0 */6 * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["etl", "bronze", "silver", "ingestion"],
    description="Ingest data from Bronze (MinIO) to Silver (PostgreSQL) with validation"
) as dag:

    @task
    def discover_and_ingest():
        import pandas as pd
        import logging

        logger = logging.getLogger(__name__)

        minio_hook = MinIOHook(bucket_name=BRONZE_BUCKET)
        pg_hook = PostgresLayerHook()

        files = minio_hook.list_files(prefix="", suffix=".parquet")

        processed_count = 0
        errors = []

        for file_key in files:
            try:
                if "/sales/" not in file_key and "sales/" not in file_key:
                    continue

                logger.info(f"Processing file: {file_key}")

                existing_files = pg_hook.get_processed_files("sales")
                if file_key in existing_files:
                    logger.info(f"Skipping already processed file: {file_key}")
                    continue

                pg_hook.update_metadata(file_key, "sales", "PROCESSING", 0)

                df = minio_hook.read_parquet(file_key)
                logger.info(f"Read {len(df)} rows from {file_key}")

                spec = get_dataset_spec("sales")
                
                actual_cols = set(df.columns)
                expected_cols = set(spec.get("expected_columns", []))
                if not expected_cols.issubset(actual_cols):
                    missing = expected_cols - actual_cols
                    error_msg = f"Missing columns: {missing}"
                    pg_hook.update_metadata(file_key, "sales", "FAILED", 0, error_message=error_msg)
                    errors.append(f"{file_key}: {error_msg}")
                    continue

                logger.info("Schema validation passed")

                df["is_weekend"] = df["is_weekend"].astype(bool)
                df["is_holiday"] = df["is_holiday"].astype(bool)
                df["ingest_date"] = datetime.now().date()

                pg_hook.insert_dataframe(df, "sales", schema=SILVER_SCHEMA)

                pg_hook.update_metadata(file_key, "sales", "PROCESSED", len(df))
                processed_count += len(df)
                logger.info(f"Inserted {len(df)} rows from {file_key}")

            except Exception as e:
                import traceback
                tb = traceback.format_exc()
                logger.error(f"Error processing {file_key}: {e}\n{tb}")
                pg_hook.update_metadata(file_key, "sales", "FAILED", 0, error_message=str(e))
                errors.append(f"{file_key}: {str(e)}")

        return {
            "records_processed": processed_count,
            "errors": errors
        }

    result = discover_and_ingest()
