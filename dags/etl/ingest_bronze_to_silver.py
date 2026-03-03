from datetime import datetime, timedelta
from typing import List, Optional
import uuid

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

import sys
import importlib.util

def _import_email_utils():
    spec = importlib.util.spec_from_file_location(
        "email_utils", 
        "/opt/airflow/scripts/utils/email_utils.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

email_utils = _import_email_utils()
send_ingestion_alert = email_utils.send_ingestion_alert

sys.path.insert(0, "/opt/airflow/scripts")
sys.path.insert(0, "/opt/airflow/dags")

from utils.minio_hook import MinIOHook
from utils.postgres_hook import PostgresLayerHook
from utils.duckdb_utils import DuckDBValidator, create_validator


BRONZE_BUCKET = "bronze"
SILVER_SCHEMA = "silver"
QUARANTINE_SCHEMA = "quarantine"
METADATA_SCHEMA = "metadata"
AUDIT_SCHEMA = "audit"
ALERT_EMAIL = Variable.get("alert_email", default_var="daniel.doe@a2sv.org")

DATASET_SPECS = {
    "sales": {
        "expected_columns": [
            "transaction_id", "sale_date", "sale_hour", "customer_id", "customer_name",
            "product_id", "product_name", "category", "sub_category", "quantity",
            "unit_price", "discount_percentage", "discount_amount", "gross_amount",
            "net_amount", "profit_margin", "payment_method", "payment_category",
            "store_location", "region", "is_weekend", "is_holiday", "ingest_date"
        ],
        "required_columns": ["transaction_id", "sale_date", "customer_id", "product_id", "quantity", "unit_price", "net_amount"],
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


def check_schema_drift(df_columns: List[str], expected_columns: List[str]) -> dict:
    """
    Check for schema drift between incoming data and expected schema.
    
    Returns dict with:
    - has_drift: bool - True if schema mismatch detected
    - missing_columns: List[str] - columns expected but not in data
    - extra_columns: List[str] - columns in data but not expected
    - drift_details: str - human-readable description
    """
    actual_set = set(df_columns)
    expected_set = set(expected_columns)
    
    missing_columns = list(expected_set - actual_set)
    extra_columns = list(actual_set - expected_set)
    
    has_drift = len(missing_columns) > 0 or len(extra_columns) > 0
    
    drift_details = ""
    if missing_columns:
        drift_details += f"Missing columns: {', '.join(missing_columns)}. "
    if extra_columns:
        drift_details += f"Extra columns: {', '.join(extra_columns)}."
    
    return {
        "has_drift": has_drift,
        "missing_columns": missing_columns,
        "extra_columns": extra_columns,
        "drift_details": drift_details.strip(),
        "expected_columns": expected_columns,
        "actual_columns": df_columns
    }


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

                logger.info(f"Reading {file_key} with DuckDB...")
                
                df = duckdb_validator.read_parquet_from_minio(
                    bucket=BRONZE_BUCKET,
                    key_pattern=file_key
                )
                
                if df.empty:
                    logger.warning(f"No data read from {file_key}")
                    pg_hook.update_metadata(file_key, "sales", "FAILED", 0, error_message="No data found")
                    continue
                
                logger.info(f"Read {len(df)} rows from {file_key}")
                total_rows_read += len(df)

                spec = get_dataset_spec("sales")
                expected_columns = spec.get("expected_columns", [])
                
                # Schema Drift Detection - Strict Mode
                schema_drift = check_schema_drift(list(df.columns), expected_columns)
                
                if schema_drift["has_drift"]:
                    drift_msg = schema_drift["drift_details"]
                    logger.error(f"SCHEMA DRIFT DETECTED in {file_key}: {drift_msg}")
                    
                    # Update metadata with SCHEMA_DRIFT status
                    pg_hook.update_metadata(
                        file_key, 
                        "sales", 
                        "SCHEMA_DRIFT", 
                        0, 
                        error_message=f"Schema drift: {drift_msg}"
                    )
                    
                    # Track for alerting
                    errors.append(f"SCHEMA_DRIFT: {file_key} - {drift_msg}")
                    
                    # Skip this file entirely - don't process any rows
                    logger.warning(f"Skipping file {file_key} due to schema drift. Update schema to process.")
                    continue
                
                # If schema is valid, proceed with row-level validation
                schema_valid = True
                validation_result = {}
                
                try:
                    validation_result = duckdb_validator.run_full_validation(df, spec)
                    schema_valid = validation_result.get("schema_valid", True)
                except KeyError as e:
                    # This shouldn't happen since we checked schema drift above
                    schema_valid = True
                    logger.warning(f"Unexpected column mismatch: {e}")

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
                    for idx, row in bad_rows.iterrows():
                        quarantine_records.append({
                            "id": idx,
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

        send_ingestion_alert(
            run_id=run_id,
            total_read=total_rows_read,
            silver_count=total_rows_silver,
            quarantine_count=total_rows_quarantined,
            files_scanned=files_scanned,
            errors=errors,
            recipient=ALERT_EMAIL
        )

        return {
            "run_id": run_id,
            "records_silver": total_rows_silver,
            "records_quarantined": total_rows_quarantined,
            "records_read": total_rows_read,
            "errors": errors
        }

    @task
    def populate_customers():
        """Extract unique customers from bronze sales and populate silver.customers"""
        import pandas as pd
        import logging
        
        logger = logging.getLogger(__name__)
        logger.info("Populating silver.customers from bronze sales")
        
        pg_hook = PostgresLayerHook()
        duckdb_validator = create_validator()
        
        try:
            all_customers = []
            
            for file_key in duckdb_validator.list_parquet_files("bronze"):
                if not file_key.startswith("sales/"):
                    continue
                    
                try:
                    df = duckdb_validator.read_parquet_from_minio("bronze", file_key)
                    
                    if df.empty:
                        continue
                    
                    if "customer_id" not in df.columns:
                        continue
                    
                    customer_cols = ["customer_id", "customer_name", "region", "store_location"]
                    existing_cols = [c for c in customer_cols if c in df.columns]
                    
                    customers = df[existing_cols].drop_duplicates(subset=["customer_id"])
                    all_customers.append(customers)
                        
                except Exception as e:
                    logger.warning(f"Error reading {file_key}: {e}")
            
            if all_customers:
                combined = pd.concat(all_customers, ignore_index=True)
                combined = combined.drop_duplicates(subset=["customer_id"])
                combined = combined.dropna(subset=["customer_id"])
                
                inserted = 0
                for _, row in combined.iterrows():
                    try:
                        pg_hook.execute_query("""
                            INSERT INTO silver.customers (customer_id, customer_name, created_at)
                            VALUES (%s, %s, NOW())
                            ON CONFLICT (customer_id) DO UPDATE SET
                                customer_name = EXCLUDED.customer_name
                        """, parameters=(
                            row.get("customer_id"),
                            row.get("name", "")
                        ))
                        inserted += 1
                    except Exception as e:
                        logger.warning(f"Error inserting customer {row.get('customer_id')}: {e}")
                
                logger.info(f"Populated {inserted} customers in silver.customers")
                return {"customers_added": inserted, "status": "success"}
            else:
                logger.info("No customers found to populate")
                return {"customers_added": 0, "status": "no_data"}
                
        except Exception as e:
            logger.error(f"Failed to populate customers: {e}")
            return {"customers_added": 0, "status": "error", "error": str(e)}
        finally:
            duckdb_validator.close()

    @task
    def populate_products():
        """Extract unique products from bronze sales and populate silver.products"""
        import pandas as pd
        import logging
        
        logger = logging.getLogger(__name__)
        logger.info("Populating silver.products from bronze sales")
        
        pg_hook = PostgresLayerHook()
        duckdb_validator = create_validator()
        
        try:
            all_products = []
            
            for file_key in duckdb_validator.list_parquet_files("bronze"):
                if not file_key.startswith("sales/"):
                    continue
                    
                try:
                    df = duckdb_validator.read_parquet_from_minio("bronze", file_key)
                    
                    if df.empty:
                        continue
                    
                    if "product_id" not in df.columns:
                        continue
                    
                    product_cols = ["product_id", "product_name", "category", "sub_category"]
                    existing_cols = [c for c in product_cols if c in df.columns]
                    
                    products = df[existing_cols].drop_duplicates(subset=["product_id"])
                    products = products.rename(columns={
                        "product_name": "product_name",
                    })
                    all_products.append(products)
                        
                except Exception as e:
                    logger.warning(f"Error reading {file_key}: {e}")
            
            if all_products:
                combined = pd.concat(all_products, ignore_index=True)
                combined = combined.drop_duplicates(subset=["product_id"])
                combined = combined.dropna(subset=["product_id"])
                
                inserted = 0
                for _, row in combined.iterrows():
                    try:
                        pg_hook.execute_query("""
                            INSERT INTO silver.products (product_id, product_name, category, sub_category, created_at)
                            VALUES (%s, %s, %s, %s, NOW())
                            ON CONFLICT (product_id) DO UPDATE SET
                                product_name = EXCLUDED.product_name,
                                category = EXCLUDED.category,
                                sub_category = EXCLUDED.sub_category
                        """, parameters=(
                            row.get("product_id"),
                            row.get("product_name", ""),
                            row.get("category", ""),
                            row.get("sub_category", "")
                        ))
                        inserted += 1
                    except Exception as e:
                        logger.warning(f"Error inserting product {row.get('product_id')}: {e}")
                
                logger.info(f"Populated {inserted} products in silver.products")
                return {"products_added": inserted, "status": "success"}
            else:
                logger.info("No products found to populate")
                return {"products_added": 0, "status": "no_data"}
                
        except Exception as e:
            logger.error(f"Failed to populate products: {e}")
            return {"products_added": 0, "status": "error", "error": str(e)}
        finally:
            duckdb_validator.close()

    result = discover_and_ingest()
    customers_result = populate_customers()
    products_result = populate_products()
