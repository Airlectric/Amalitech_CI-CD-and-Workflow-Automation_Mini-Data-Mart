import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import uuid
import hashlib
from io import BytesIO
import sys
import os

if __name__ != "__main__":
    os.chdir('/opt/airflow')
    sys.path.insert(0, '/opt/airflow/scripts')

from log_utils import get_logger, StructuredLogger

logger = StructuredLogger('GENERATOR')


def generate_sales_data(num_rows: int = 1000) -> pd.DataFrame:
    np.random.seed(42)
    random.seed(42)
    
    logger.info("Generating sales data", rows=num_rows)
    
    start_date = datetime(2024, 1, 1)
    
    timestamps = [
        start_date + timedelta(
            days=random.randint(0, 365),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        ) for _ in range(num_rows)
    ]
    
    data = {
        "transaction_id": [f"TXN{i:08d}" for i in range(1, num_rows + 1)],
        "sale_date": [ts.date() for ts in timestamps],
        "sale_hour": [ts.hour for ts in timestamps],
        "customer_id": [f"CUST{random.randint(1000, 9999)}" for _ in range(num_rows)],
        "customer_name": [
            random.choice(["John", "Jane", "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"])
            + " " +
            random.choice(["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"])
            for _ in range(num_rows)
        ],
        "product_id": [f"PROD{random.randint(100, 999)}" for _ in range(num_rows)],
        "product_name": [
            random.choice(["Laptop", "Phone", "Tablet", "Headphones", "Camera", "Watch", "Speaker", "Monitor", "Keyboard", "Mouse"])
            + " " +
            random.choice(["Pro", "Plus", "Max", "Ultra", "Lite", "Standard", "Premium"])
            for _ in range(num_rows)
        ],
        "category": random.choices(
            ["Electronics", "Accessories", "Software", "Services", "Peripherals"],
            weights=[40, 25, 15, 12, 8],
            k=num_rows
        ),
        "sub_category": random.choices(
            ["Gaming", "Business", "Home", "Office", "Mobile", "Audio", "Video"],
            k=num_rows
        ),
        "quantity": np.random.randint(1, 10, size=num_rows).tolist(),
        "unit_price": np.round(np.random.uniform(9.99, 1999.99, size=num_rows), 2).tolist(),
        "discount_percentage": np.round(np.random.uniform(0, 25, size=num_rows), 2).tolist(),
        "payment_method": random.choices(
            ["Credit Card", "Debit Card", "Cash", "Digital Wallet", "Bank Transfer"],
            weights=[35, 25, 20, 15, 5],
            k=num_rows
        ),
        "store_location": random.choices(
            ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", 
             "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"],
            k=num_rows
        ),
        "region": random.choices(
            ["Northeast", "West", "Midwest", "Southwest", "Southeast"],
            k=num_rows
        ),
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
    
    column_order = [
        "transaction_id", "sale_date", "sale_hour", "customer_id", "customer_name",
        "product_id", "product_name", "category", "sub_category", "quantity",
        "unit_price", "discount_percentage", "discount_amount", "gross_amount",
        "net_amount", "profit_margin", "payment_method", "payment_category",
        "store_location", "region", "is_weekend", "is_holiday"
    ]
    
    df = df[column_order]
    
    logger.info("Sales data generated successfully", rows=len(df), columns=len(df.columns))
    
    return df


def compute_checksum(df: pd.DataFrame) -> str:
    """Compute MD5 checksum of dataframe"""
    return hashlib.md5(df.to_csv(index=False).encode()).hexdigest()


def save_to_parquet_local(df: pd.DataFrame, output_path: str) -> str:
    """Save parquet to local filesystem"""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
    logger.info("Saved parquet file", path=output_path, rows=len(df))
    return output_path


def upload_to_minio(df: pd.DataFrame, bucket: str, dataset_name: str, 
                    minio_client, ingest_date: str = None) -> dict:
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
    
    minio_client.put_object(
        bucket_name=bucket,
        object_name=f"{dataset_name}/ingest_date={ingest_date}/{filename}",
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/parquet"
    )
    
    logger.info("Upload complete", path=s3_path, rows=len(df), checksum=checksum[:8])
    
    return {
        "file_path": s3_path,
        "dataset_name": dataset_name,
        "checksum": checksum,
        "ingest_date": ingest_date,
        "record_count": len(df),
        "status": "NEW"
    }


def generate_batch_to_minio(batch_size: int = 1000, num_batches: int = 1,
                            dataset_name: str = "sales",
                            bucket: str = "bronze",
                            minio_client=None,
                            ingest_date: str = None) -> list[dict]:
    """
    Generate parquet files and upload to MinIO.
    
    Returns list of metadata dicts for each uploaded file.
    """
    logger.info("Starting batch generation", batch_size=batch_size, num_batches=num_batches)
    
    uploaded_files = []
    
    for batch in range(1, num_batches + 1):
        df = generate_sales_data(num_rows=batch_size)
        
        metadata = upload_to_minio(
            df=df,
            bucket=bucket,
            dataset_name=dataset_name,
            minio_client=minio_client,
            ingest_date=ingest_date
        )
        
        uploaded_files.append(metadata)
    
    logger.info("Batch generation complete", total_files=len(uploaded_files))
    
    return uploaded_files


def generate_batch_local(batch_size: int = 1000, num_batches: int = 1, 
                        output_dir: str = "data/raw") -> list[str]:
    """Generate parquet files locally (for testing without MinIO)"""
    logger.info("Generating local parquet files", batch_size=batch_size, num_batches=num_batches)
    
    generated_files = []
    
    for batch in range(1, num_batches + 1):
        df = generate_sales_data(num_rows=batch_size)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        filename = f"sales_data_batch_{batch}_{timestamp}_{unique_id}.parquet"
        filepath = os.path.join(output_dir, filename)
        save_to_parquet_local(df, filepath)
        generated_files.append(filepath)
    
    logger.info("Local generation complete", total_files=len(generated_files))
    
    return generated_files


if __name__ == "__main__":
    import boto3
    from dotenv import load_dotenv
    
    load_dotenv()
    
    logger.info("Starting data generator")
    
    minio_endpoint = os.getenv("MINIO_ROOT_USER", "minio") + ":" + os.getenv("MINIO_API_PORT", "9002")
    
    s3_client = boto3.client(
        's3',
        endpoint_url=f"http://{minio_endpoint}",
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minio"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
    )
    
    files = generate_batch_to_minio(
        batch_size=1000,
        num_batches=1,
        dataset_name="sales",
        bucket="bronze",
        minio_client=s3_client
    )
    
    logger.info("Data generation complete", files_generated=len(files))
    print(f"\nUploaded {len(files)} file(s) to MinIO")
    for f in files:
        print(f"  - {f['file_path']} ({f['record_count']} rows)")
