from datetime import datetime, timedelta
from typing import List, Optional

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

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
            "store_location", "region", "is_weekend", "is_holiday", "ingest_date"
        ],
        "required_columns": ["transaction_id", "sale_date", "product_id", "quantity", "unit_price", "net_amount"],
        "unique_columns": ["transaction_id"],
        "silver_table": "sales"
    },
    "customers": {
        "expected_columns": [
            "customer_id", "customer_name", "first_purchase_date", "total_purchases",
            "total_revenue", "average_order_value", "customer_segment",
            "last_purchase_date", "days_since_last_purchase"
        ],
        "required_columns": ["customer_id"],
        "unique_columns": ["customer_id"],
        "silver_table": "customers"
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
    schedule_interval="0 */6 * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["etl", "bronze", "silver", "ingestion"],
    description="Ingest data from Bronze (MinIO) to Silver (PostgreSQL) with validation"
) as dag:

    @task
    def initialize():
        minio_hook = MinIOHook(bucket_name=BRONZE_BUCKET)
        pg_hook = PostgresLayerHook()
        return {"status": "initialized"}

    @task
    def discover_datasets():
        minio_hook = MinIOHook(bucket_name=BRONZE_BUCKET)
        files = minio_hook.list_files(prefix="", suffix=".parquet")

        datasets = {}
        for f in files:
            parts = f.split("/")
            if len(parts) >= 2:
                dataset = parts[0]
                if dataset not in datasets:
                    datasets[dataset] = []
                datasets[dataset].append(f)

        return datasets

    @task
    def validate_and_ingest(dataset: str, files: List[str]):
        import pandas as pd

        minio_hook = MinIOHook(bucket_name=BRONZE_BUCKET)
        pg_hook = PostgresLayerHook()
        validator = create_validator()

        spec = get_dataset_spec(dataset)
        if not spec:
            return {"status": "skipped", "reason": "Unknown dataset"}

        processed_count = 0
        errors = []

        for file_key in files:
            try:
                metadata = minio_hook.get_file_metadata(file_key)

                existing_files = pg_hook.get_processed_files(dataset)
                if file_key in existing_files:
                    continue

                pg_hook.update_metadata(file_key, dataset, "PROCESSING", 0)

                df = minio_hook.read_parquet(file_key)

                validation_result = validator.run_full_validation(df, spec)

                if not validation_result["schema_valid"] or not validation_result["data_valid"]:
                    error_msg = "; ".join(validation_result["errors"])
                    pg_hook.update_metadata(file_key, dataset, "FAILED", 0, error_message=error_msg)
                    errors.append(f"{file_key}: {error_msg}")
                    continue

                df["ingest_date"] = datetime.now().date()

                table_name = spec["silver_table"]
                pg_hook.insert_dataframe(df, table_name, schema=SILVER_SCHEMA)

                pg_hook.update_metadata(file_key, dataset, "PROCESSED", len(df))
                processed_count += len(df)

            except Exception as e:
                pg_hook.update_metadata(file_key, dataset, "FAILED", 0, error_message=str(e))
                errors.append(f"{file_key}: {str(e)}")

        validator.close()

        return {
            "status": "completed" if not errors else "completed_with_errors",
            "records_processed": processed_count,
            "errors": errors
        }

    @task
    def summarize(results: List[dict]):
        total_records = sum(r.get("records_processed", 0) for r in results)
        all_errors = []
        for r in results:
            all_errors.extend(r.get("errors", []))

        return {
            "total_records": total_records,
            "error_count": len(all_errors),
            "errors": all_errors[:10]
        }

    init = initialize()
    datasets = discover_datasets()

    with TaskGroup("ingestion_tasks") as ingestion_group:
        for dataset, files in datasets.items():
            validate_and_ingest(dataset, files)

    summary = summarize(ingestion_group)

    init >> datasets >> ingestion_group >> summary
