import hashlib
import os
import random
import sys
import uuid
from datetime import datetime, timedelta
from io import BytesIO

import numpy as np
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(SCRIPT_DIR))
sys.path.insert(0, "/opt/airflow")
sys.path.insert(0, "/opt/airflow/scripts")

from log_utils import StructuredLogger

logger = StructuredLogger("GENERATOR")


def generate_sales_data(num_rows: int = 1000, mode: str = "clean") -> pd.DataFrame:
    """
    Generate sales data with different quality modes.

    Modes:
    - clean: Valid data that passes all validations
    - dirty: Random nulls and invalid values (row-level quarantine)
    - duplicates: Contains duplicate transaction_ids
    - edge_cases: Boundary values and special characters
    - mixed: Mix of good and bad rows
    - schema_invalid: Missing/wrong columns (file-level skip)
    """
    np.random.seed(42)
    random.seed(42)

    logger.info("Generating sales data", rows=num_rows, mode=mode)

    if mode == "schema_invalid":
        return generate_schema_invalid_data(num_rows)

    start_date = datetime(2024, 1, 1)

    timestamps = [
        start_date + timedelta(days=random.randint(0, 365), hours=random.randint(0, 23), minutes=random.randint(0, 59))
        for _ in range(num_rows)
    ]

    data = {
        "transaction_id": [f"TXN{i:08d}" for i in range(1, num_rows + 1)],
        "sale_date": [ts.date() for ts in timestamps],
        "sale_hour": [ts.hour for ts in timestamps],
        "customer_id": [f"CUST{random.randint(1000, 9999)}" for _ in range(num_rows)],
        "customer_name": [
            random.choice(["John", "Jane", "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"])
            + " "
            + random.choice(
                ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
            )
            for _ in range(num_rows)
        ],
        "product_id": [f"PROD{random.randint(100, 999)}" for _ in range(num_rows)],
        "product_name": [
            random.choice(
                [
                    "Laptop",
                    "Phone",
                    "Tablet",
                    "Headphones",
                    "Camera",
                    "Watch",
                    "Speaker",
                    "Monitor",
                    "Keyboard",
                    "Mouse",
                ]
            )
            + " "
            + random.choice(["Pro", "Plus", "Max", "Ultra", "Lite", "Standard", "Premium"])
            for _ in range(num_rows)
        ],
        "category": random.choices(
            ["Electronics", "Accessories", "Software", "Services", "Peripherals"],
            weights=[40, 25, 15, 12, 8],
            k=num_rows,
        ),
        "sub_category": random.choices(
            ["Gaming", "Business", "Home", "Office", "Mobile", "Audio", "Video"], k=num_rows
        ),
        "quantity": np.random.randint(1, 10, size=num_rows).tolist(),
        "unit_price": np.round(np.random.uniform(9.99, 1999.99, size=num_rows), 2).tolist(),
        "discount_percentage": np.round(np.random.uniform(0, 25, size=num_rows), 2).tolist(),
        "payment_method": random.choices(
            ["Credit Card", "Debit Card", "Cash", "Digital Wallet", "Bank Transfer"],
            weights=[35, 25, 20, 15, 5],
            k=num_rows,
        ),
        "store_location": random.choices(
            [
                "New York",
                "Los Angeles",
                "Chicago",
                "Houston",
                "Phoenix",
                "Philadelphia",
                "San Antonio",
                "San Diego",
                "Dallas",
                "San Jose",
            ],
            k=num_rows,
        ),
        "region": random.choices(["Northeast", "West", "Midwest", "Southwest", "Southeast"], k=num_rows),
        "is_weekend": [1 if ts.weekday() >= 5 else 0 for ts in timestamps],
        "is_holiday": [0] * num_rows,
    }

    df = pd.DataFrame(data)

    df["discount_amount"] = np.round(df["unit_price"] * df["discount_percentage"] / 100, 2)
    df["gross_amount"] = np.round(df["unit_price"] * df["quantity"], 2)
    df["net_amount"] = np.round(df["gross_amount"] - df["discount_amount"], 2)
    df["profit_margin"] = np.round(np.random.uniform(5, 30, size=num_rows), 2).tolist()

    df["payment_category"] = df["payment_method"].apply(
        lambda x: "Online" if x in ["Credit Card", "Debit Card", "Digital Wallet"] else "Offline"
    )

    if mode == "duplicates":
        duplicate_indices = random.sample(range(num_rows), min(num_rows // 4, 250))
        for idx in duplicate_indices:
            df.at[idx, "transaction_id"] = df.at[random.randint(0, num_rows - 1), "transaction_id"]
        logger.info("Added duplicates", duplicates=len(duplicate_indices))

    elif mode == "dirty":
        null_cols = random.sample(["customer_id", "product_id", "quantity", "unit_price"], k=2)
        for col in null_cols:
            null_indices = random.sample(range(num_rows), min(num_rows // 5, 200))
            df.loc[null_indices, col] = None
        df.loc[random.sample(range(num_rows), 50), "quantity"] = -999
        logger.info("Added dirty data", null_cols=null_cols)

    elif mode == "edge_cases":
        df.loc[0, "quantity"] = 999999
        df.loc[1, "unit_price"] = -1.00
        df.loc[2, "discount_percentage"] = 150
        df.loc[3, "customer_name"] = "Robert'); DROP TABLE sales;--"
        df.loc[4, "product_name"] = "Test\x00Null"
        logger.info("Added edge cases")

    elif mode == "mixed":
        bad_indices = random.sample(range(num_rows), num_rows // 5)
        df.loc[bad_indices, "customer_id"] = None
        df.loc[bad_indices[:100], "quantity"] = -5
        logger.info("Mixed data", good=len(df) - len(bad_indices), bad=len(bad_indices))

    column_order = [
        "transaction_id",
        "sale_date",
        "sale_hour",
        "customer_id",
        "customer_name",
        "product_id",
        "product_name",
        "category",
        "sub_category",
        "quantity",
        "unit_price",
        "discount_percentage",
        "discount_amount",
        "gross_amount",
        "net_amount",
        "profit_margin",
        "payment_method",
        "payment_category",
        "store_location",
        "region",
        "is_weekend",
        "is_holiday",
    ]

    df = df[column_order]

    logger.info("Sales data generated successfully", rows=len(df), columns=len(df.columns), mode=mode)

    return df


def generate_schema_invalid_data(num_rows: int = 1000) -> pd.DataFrame:
    """
    Generate data with schema issues that will cause DuckDB to reject the entire file.
    This tests file-level validation - entire file goes to quarantine.
    """
    logger.info("Generating SCHEMA INVALID data", rows=num_rows)

    start_date = datetime(2024, 1, 1)

    timestamps = [start_date + timedelta(days=random.randint(0, 365)) for _ in range(num_rows)]

    schema_modes = ["missing_required_columns", "wrong_column_types", "extra_unknown_columns", "missing_entire_column"]
    chosen_mode = random.choice(schema_modes)

    logger.info(f"Using schema invalid mode: {chosen_mode}")

    if chosen_mode == "missing_required_columns":
        data = {
            "transaction_id": [f"TXN{i:08d}" for i in range(1, num_rows + 1)],
            "sale_date": [ts.date() for ts in timestamps],
            "quantity": np.random.randint(1, 10, size=num_rows).tolist(),
            "unit_price": np.round(np.random.uniform(10, 1000, size=num_rows), 2).tolist(),
        }
        logger.info("Missing: customer_id, product_id, store_id, is_weekend, is_holiday")

    elif chosen_mode == "wrong_column_types":
        data = {
            "transaction_id": np.random.randint(10000000, 99999999, size=num_rows).tolist(),
            "sale_date": [f"{ts.date()}" for ts in timestamps],
            "sale_hour": [str(ts.hour) for ts in timestamps],
            "customer_id": [f"CUST{i}" for i in range(1000, 1000 + num_rows)],
            "customer_name": ["Test User"] * num_rows,
            "product_id": [f"PROD{i % 100 + 100}" for i in range(num_rows)],
            "product_name": ["Test Product"] * num_rows,
            "category": ["Electronics"] * num_rows,
            "sub_category": ["Gaming"] * num_rows,
            "quantity": [random.choice([1, 2, "three", "five"]) for _ in range(num_rows)],
            "unit_price": [random.choice([10.0, "ten", None]) for _ in range(num_rows)],
            "discount_percentage": np.round(np.random.uniform(0, 25, size=num_rows), 2).tolist(),
            "discount_amount": np.round(np.random.uniform(0, 50, size=num_rows), 2).tolist(),
            "gross_amount": np.round(np.random.uniform(10, 10000, size=num_rows), 2).tolist(),
            "net_amount": np.round(np.random.uniform(10, 10000, size=num_rows), 2).tolist(),
            "profit_margin": np.round(np.random.uniform(5, 30, size=num_rows), 2).tolist(),
            "payment_method": ["Credit Card"] * num_rows,
            "payment_category": ["Online"] * num_rows,
            "store_location": ["New York"] * num_rows,
            "region": ["Northeast"] * num_rows,
            "is_weekend": [random.choice([0, 1, "yes", True]) for _ in range(num_rows)],
            "is_holiday": [random.choice([0, 1, "no"]) for _ in range(num_rows)],
        }

    elif chosen_mode == "extra_unknown_columns":
        data = {
            "transaction_id": [f"TXN{i:08d}" for i in range(1, num_rows + 1)],
            "sale_date": [ts.date() for ts in timestamps],
            "sale_hour": [ts.hour for ts in timestamps],
            "customer_id": [f"CUST{random.randint(1000, 9999)}" for _ in range(num_rows)],
            "customer_name": ["Test User"] * num_rows,
            "product_id": [f"PROD{random.randint(100, 999)}" for _ in range(num_rows)],
            "product_name": ["Test Product"] * num_rows,
            "category": ["Electronics"] * num_rows,
            "sub_category": ["Gaming"] * num_rows,
            "quantity": np.random.randint(1, 10, size=num_rows).tolist(),
            "unit_price": np.round(np.random.uniform(10, 1000, size=num_rows), 2).tolist(),
            "discount_percentage": np.round(np.random.uniform(0, 25, size=num_rows), 2).tolist(),
            "discount_amount": np.round(np.random.uniform(0, 50, size=num_rows), 2).tolist(),
            "gross_amount": np.round(np.random.uniform(10, 10000, size=num_rows), 2).tolist(),
            "net_amount": np.round(np.random.uniform(10, 10000, size=num_rows), 2).tolist(),
            "profit_margin": np.round(np.random.uniform(5, 30, size=num_rows), 2).tolist(),
            "payment_method": ["Credit Card"] * num_rows,
            "payment_category": ["Online"] * num_rows,
            "store_location": ["New York"] * num_rows,
            "region": ["Northeast"] * num_rows,
            "is_weekend": [0] * num_rows,
            "is_holiday": [0] * num_rows,
            "unknown_field_1": ["unknown"] * num_rows,
            "unknown_field_2": np.random.randint(1, 100, size=num_rows).tolist(),
            "unknown_field_3": [True, False] * (num_rows // 2),
        }

    else:
        base_data = {
            "transaction_id": [f"TXN{i:08d}" for i in range(1, num_rows + 1)],
            "customer_id": [f"CUST{random.randint(1000, 9999)}" for _ in range(num_rows)],
            "product_id": [f"PROD{random.randint(100, 999)}" for _ in range(num_rows)],
        }
        return pd.DataFrame(base_data)

    df = pd.DataFrame(data)
    logger.info("Schema invalid data generated", mode=chosen_mode, columns=len(df.columns), rows=len(df))

    return df


def compute_checksum(df: pd.DataFrame) -> str:
    """Compute MD5 checksum of dataframe"""
    return hashlib.md5(df.to_csv(index=False).encode(), usedforsecurity=False).hexdigest()


def save_to_parquet_local(df: pd.DataFrame, output_path: str) -> str:
    """Save parquet to local filesystem"""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
    logger.info("Saved parquet file", path=output_path, rows=len(df))
    return output_path


def upload_to_minio(
    df: pd.DataFrame, bucket: str, dataset_name: str, s3_client, ingest_date: str | None = None
) -> dict:
    """
    Upload parquet to MinIO with proper path convention.

    Path: s3://bronze/<dataset_name>/ingest_date=YYYY-MM-DD/<dataset>_<uuid>.parquet

    Returns metadata dict with file_path, checksum, record_count
    """
    if ingest_date is None:
        ingest_date = datetime.now().strftime("%Y-%m-%d")

    unique_id = str(uuid.uuid4())[:8]
    filename = f"{dataset_name}_{unique_id}.parquet"
    s3_path = f"s3://{bucket}/{dataset_name}/ingest_date={ingest_date}/{filename}"

    logger.info("Uploading to MinIO", bucket=bucket, path=s3_path, rows=len(df))

    buffer = BytesIO()
    df.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)
    buffer.seek(0)

    checksum = compute_checksum(df)

    s3_client.put_object(
        Bucket=bucket,
        Key=f"{dataset_name}/ingest_date={ingest_date}/{filename}",
        Body=buffer.getvalue(),
        ContentType="application/parquet",
    )

    logger.info("Upload complete", path=s3_path, rows=len(df), checksum=checksum[:8])

    return {
        "file_path": s3_path,
        "dataset_name": dataset_name,
        "checksum": checksum,
        "ingest_date": ingest_date,
        "record_count": len(df),
        "status": "NEW",
    }


def generate_batch_to_minio(
    batch_size: int = 1000,
    num_batches: int = 1,
    dataset_name: str = "sales",
    bucket: str = "bronze",
    s3_client=None,
    ingest_date: str | None = None,
    mode: str = "clean",
) -> list[dict]:
    """
    Generate parquet files and upload to MinIO.

    Modes:
    - clean: Valid data
    - dirty: Random nulls/invalid values
    - duplicates: Duplicate transaction_ids
    - edge_cases: Boundary values
    - mixed: Mix of good/bad rows
    - schema_invalid: Missing/wrong columns (file-level skip)

    Returns list of metadata dicts for each uploaded file.
    """
    logger.info("Starting batch generation", batch_size=batch_size, num_batches=num_batches, mode=mode)

    uploaded_files = []

    for _batch in range(1, num_batches + 1):
        df = generate_sales_data(num_rows=batch_size, mode=mode)

        metadata = upload_to_minio(
            df=df, bucket=bucket, dataset_name=dataset_name, s3_client=s3_client, ingest_date=ingest_date
        )

        uploaded_files.append(metadata)

    logger.info("Batch generation complete", total_files=len(uploaded_files))

    return uploaded_files


def generate_batch_local(
    batch_size: int = 1000, num_batches: int = 1, output_dir: str = "data/raw", mode: str = "clean"
) -> list[str]:
    """Generate parquet files locally (for testing without MinIO)"""
    logger.info("Generating local parquet files", batch_size=batch_size, num_batches=num_batches, mode=mode)

    generated_files = []

    for batch in range(1, num_batches + 1):
        df = generate_sales_data(num_rows=batch_size, mode=mode)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        filename = f"sales_data_batch_{batch}_{timestamp}_{unique_id}.parquet"
        filepath = os.path.join(output_dir, filename)
        save_to_parquet_local(df, filepath)
        generated_files.append(filepath)

    logger.info("Local generation complete", total_files=len(generated_files))

    return generated_files


if __name__ == "__main__":
    import argparse

    import boto3
    from dotenv import load_dotenv

    parser = argparse.ArgumentParser(description="Generate test data")
    parser.add_argument(
        "--mode",
        type=str,
        default="clean",
        choices=["clean", "dirty", "duplicates", "edge_cases", "mixed", "schema_invalid"],
        help="Data quality mode",
    )
    parser.add_argument("--rows", type=int, default=1000, help="Number of rows")
    parser.add_argument("--batches", type=int, default=1, help="Number of batches")
    args = parser.parse_args()

    load_dotenv()

    logger.info("Starting data generator", mode=args.mode, rows=args.rows, batches=args.batches)

    s3_client = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="minio123",
    )

    files = generate_batch_to_minio(
        batch_size=args.rows,
        num_batches=args.batches,
        dataset_name="sales",
        bucket="bronze",
        s3_client=s3_client,
        mode=args.mode,
    )

    logger.info("Data generation complete", files_generated=len(files), mode=args.mode)
    print(f"\nUploaded {len(files)} file(s) to MinIO (mode: {args.mode})")
    for f in files:
        print(f"  - {f['file_path']} ({f['record_count']} rows)")
