from datetime import datetime, timedelta
from typing import List, Optional
import uuid

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
QUARANTINE_SCHEMA = "quarantine"
METADATA_SCHEMA = "metadata"
AUDIT_SCHEMA = "audit"

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
        "value_ranges": {
            "quantity": {"min": 1, "max": 1000},
            "unit_price": {"min": 0, "max": 100000},
            "discount_percentage": {"min": 0, "max": 100}
        },
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
    description="Ingest data from Bronze (MinIO) to Silver (PostgreSQL) with DuckDB quality gate"
) as dag:

    @task
    def discover_and_ingest():
        import pandas as pd
        import logging

        logger = logging.getLogger(__name__)

        # Generate unique run ID for this ingestion
        run_id = str(uuid.uuid4())
        logger.info(f"Starting ingestion run: {run_id}")

        minio_hook = MinIOHook(bucket_name=BRONZE_BUCKET)
        pg_hook = PostgresLayerHook()
        
        # Record start of run in audit table
        pg_hook.execute_query(f"""
            INSERT INTO {AUDIT_SCHEMA}.ingestion_runs (ingestion_run_id, started_at, status)
            VALUES ('{run_id}', NOW(), 'RUNNING')
        """)
        
        duckdb_validator = create_validator()

        files = minio_hook.list_files(prefix="", suffix=".parquet")
        
        # Track files scanned
        files_scanned = []
        total_rows_read = 0
        total_rows_silver = 0
        total_rows_quarantined = 0
        errors = []

        for file_key in files:
            try:
                if "/sales/" not in file_key and "sales/" not in file_key:
                    continue

                logger.info(f"Processing file: {file_key}")
                files_scanned.append(file_key)

                existing_files = pg_hook.get_processed_files("sales")
                if file_key in existing_files:
                    logger.info(f"Skipping already processed file: {file_key}")
                    continue

                pg_hook.update_metadata(file_key, "sales", "PROCESSING", 0)

                logger.info(f"Reading {file_key} with DuckDB schema-on-read...")
                
                parts = file_key.split("/")
                prefix = "/".join(parts[:-1]) + "/*.parquet" if len(parts) > 1 else "*.parquet"
                
                df = duckdb_validator.read_parquet_from_minio(
                    bucket=BRONZE_BUCKET,
                    key_pattern=prefix
                )
                
                if df.empty:
                    logger.warning(f"No data read from {file_key}")
                    pg_hook.update_metadata(file_key, "sales", "FAILED", 0, error_message="No data found")
                    continue
                
                logger.info(f"Read {len(df)} rows from {file_key}")
                total_rows_read += len(df)

                spec = get_dataset_spec("sales")
                
                # Validate schema first
                validation_result = duckdb_validator.run_full_validation(df, spec)
                
                if not validation_result["schema_valid"]:
                    error_msg = "; ".join(validation_result["errors"])
                    logger.error(f"Schema validation failed: {error_msg}")
                    # Send entire file to quarantine
                    quarantine_records = []
                    for _, row in df.iterrows():
                        quarantine_records.append({
                            "id": row.get("transaction_id", 0),
                            "payload": row.to_dict(),
                            "error_reason": f"Schema validation failed: {error_msg[:100]}",
                            "source_file": file_key
                        })
                    
                    pg_hook.insert_quarantine(
                        quarantine_records,
                        "sales_failed",
                        QUARANTINE_SCHEMA,
                        run_id
                    )
                    total_rows_quarantined += len(quarantine_records)
                    pg_hook.update_metadata(file_key, "sales", "FAILED", 0, error_message=error_msg)
                    errors.append(f"{file_key}: {error_msg}")
                    continue

                # Row-level validation - tag each row with error_reason
                df["_validation_errors"] = None
                
                for idx, row in df.iterrows():
                    errors_list = []
                    
                    # Check required columns for nulls
                    for col in spec.get("required_columns", []):
                        if pd.isna(row.get(col)) or row.get(col) is None:
                            errors_list.append(f"null:{col}")
                    
                    # Check value ranges
                    for col, range_spec in spec.get("value_ranges", {}).items():
                        val = row.get(col)
                        if pd.notna(val):
                            min_val = range_spec.get("min", float('-inf'))
                            max_val = range_spec.get("max", float('inf'))
                            if val < min_val or val > max_val:
                                errors_list.append(f"range:{col}")
                    
                    if errors_list:
                        df.at[idx, "_validation_errors"] = ";".join(errors_list)

                # Split good and bad rows
                good_rows = df[df["_validation_errors"].isna()].copy()
                bad_rows = df[~df["_validation_errors"].isna()].copy()

                logger.info(f"Split: {len(good_rows)} good, {len(bad_rows)} bad")

                # Load good rows to Silver
                rows_to_silver = 0
                if not good_rows.empty:
                    good_rows_clean = good_rows.drop(columns=["_validation_errors"])
                    good_rows_clean["is_weekend"] = good_rows_clean["is_weekend"].astype(bool)
                    good_rows_clean["is_holiday"] = good_rows_clean["is_holiday"].astype(bool)
                    good_rows_clean["ingest_date"] = datetime.now().date()

                    pg_hook.upsert_dataframe(
                        good_rows_clean, 
                        "sales", 
                        schema=SILVER_SCHEMA,
                        conflict_columns=["transaction_id"],
                        update_columns=["sale_date", "sale_hour", "customer_id", "customer_name", "product_id", 
                                       "product_name", "category", "sub_category", "quantity", "unit_price",
                                       "discount_percentage", "discount_amount", "gross_amount", "net_amount",
                                       "profit_margin", "payment_method", "payment_category", "store_location",
                                       "region", "is_weekend", "is_holiday", "ingest_date"]
                    )
                    rows_to_silver = len(good_rows_clean)
                    total_rows_silver += rows_to_silver

                # Load bad rows to Quarantine
                rows_quarantined = 0
                if not bad_rows.empty:
                    quarantine_records = []
                    for _, row in bad_rows.iterrows():
                        quarantine_records.append({
                            "id": row.get("transaction_id", 0),
                            "payload": row.drop("_validation_errors").to_dict(),
                            "error_reason": row["_validation_errors"],
                            "source_file": file_key
                        })
                    
                    pg_hook.insert_quarantine(
                        quarantine_records,
                        "sales_failed",
                        QUARANTINE_SCHEMA,
                        run_id
                    )
                    rows_quarantined = len(quarantine_records)
                    total_rows_quarantined += rows_quarantined

                pg_hook.update_metadata(file_key, "sales", "PROCESSED", rows_to_silver)
                logger.info(f"Inserted {rows_to_silver} rows to Silver, {rows_quarantined} to Quarantine from {file_key}")

            except Exception as e:
                import traceback
                tb = traceback.format_exc()
                logger.error(f"Error processing {file_key}: {e}\n{tb}")
                pg_hook.update_metadata(file_key, "sales", "FAILED", 0, error_message=str(e))
                errors.append(f"{file_key}: {str(e)}")

        duckdb_validator.close()

        # Update audit table with final counts
        pg_hook.update_audit_run(
            run_id=run_id,
            rows_read=total_rows_read,
            rows_written_silver=total_rows_silver,
            rows_quarantined=total_rows_quarantined,
            status="COMPLETED" if not errors else "COMPLETED_WITH_ERRORS",
            files_scanned=files_scanned
        )

        logger.info(f"Ingestion run {run_id} completed: {total_rows_silver} to Silver, {total_rows_quarantined} to Quarantine")

        return {
            "run_id": run_id,
            "records_silver": total_rows_silver,
            "records_quarantined": total_rows_quarantined,
            "records_read": total_rows_read,
            "errors": errors
        }

    result = discover_and_ingest()
