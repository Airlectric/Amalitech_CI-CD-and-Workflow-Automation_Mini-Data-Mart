import sys
sys.path.insert(0, "/opt/airflow/dags")
import pandas as pd
from datetime import datetime
from utils.minio_hook import MinIOHook
from utils.postgres_hook import PostgresLayerHook
from utils.duckdb_utils import DuckDBValidator, create_validator

BRONZE_BUCKET = "bronze"
SILVER_SCHEMA = "silver"

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

minio_hook = MinIOHook(bucket_name=BRONZE_BUCKET)
pg_hook = PostgresLayerHook()
validator = create_validator()

files = minio_hook.list_files(prefix="", suffix=".parquet")
print(f"Total files: {len(files)}")

processed_count = 0
errors = []

for file_key in files:
    try:
        print(f"\nProcessing: {file_key}")
        
        if "/sales/" not in file_key:
            print("  Skipping - not sales file")
            continue

        metadata = minio_hook.get_file_metadata(file_key)
        print(f"  Metadata: {metadata}")

        existing_files = pg_hook.get_processed_files("sales")
        print(f"  Existing files: {existing_files}")
        
        if file_key in existing_files:
            print("  Skipping - already processed")
            continue

        pg_hook.update_metadata(file_key, "sales", "PROCESSING", 0)

        df = minio_hook.read_parquet(file_key)
        print(f"  Read {len(df)} rows")

        spec = get_dataset_spec("sales")
        validation_result = validator.run_full_validation(df, spec)
        print(f"  Validation: {validation_result}")

        if not validation_result["schema_valid"] or not validation_result["data_valid"]:
            error_msg = "; ".join(validation_result["errors"])
            pg_hook.update_metadata(file_key, "sales", "FAILED", 0, error_message=error_msg)
            errors.append(f"{file_key}: {error_msg}")
            print(f"  FAILED: {error_msg}")
            continue

        df["ingest_date"] = datetime.now().date()
        print(f"  Inserting into silver.sales...")

        pg_hook.insert_dataframe(df, "sales", schema=SILVER_SCHEMA)

        pg_hook.update_metadata(file_key, "sales", "PROCESSED", len(df))
        processed_count += len(df)
        print(f"  SUCCESS: {len(df)} rows inserted")

    except Exception as e:
        import traceback
        print(f"  EXCEPTION: {e}")
        traceback.print_exc()
        pg_hook.update_metadata(file_key, "sales", "FAILED", 0, error_message=str(e))
        errors.append(f"{file_key}: {str(e)}")

validator.close()

print(f"\n=== SUMMARY ===")
print(f"Records processed: {processed_count}")
print(f"Errors: {errors}")
