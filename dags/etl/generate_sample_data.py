from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

import sys
sys.path.insert(0, "/opt/airflow/dags")

from utils.minio_hook import MinIOHook
from utils.postgres_hook import PostgresLayerHook


BRONZE_BUCKET = "bronze"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="generate_sample_data",
    start_date=datetime(2026, 1, 1),
    schedule="0 6,12,18 * * *",  # 3 times daily: 6am, 12pm, 6pm
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["etl", "data-generation", "bronze"],
    description="Generate sample data and upload to MinIO Bronze layer"
) as dag:

    @task
    def generate_and_upload():
        """Generate sample sales data and upload to MinIO"""
        import pandas as pd
        import numpy as np
        from datetime import datetime, timedelta
        import random
        import uuid
        import hashlib
        from io import BytesIO
        import boto3
        import logging

        logger = logging.getLogger(__name__)

        NUM_ROWS = 1000
        
        np.random.seed(datetime.now().microsecond)
        random.seed(datetime.now().microsecond)
        
        start_date = datetime(2024, 1, 1)
        
        timestamps = [
            start_date + timedelta(
                days=random.randint(0, 365),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            ) for _ in range(NUM_ROWS)
        ]
        
        data = {
            "transaction_id": [f"TXN{datetime.now().strftime('%Y%m%d')}{i:06d}" for i in range(1, NUM_ROWS + 1)],
            "sale_date": [ts.date() for ts in timestamps],
            "sale_hour": [ts.hour for ts in timestamps],
            "customer_id": [f"CUST{random.randint(1000, 9999)}" for _ in range(NUM_ROWS)],
            "customer_name": [
                random.choice(["John", "Jane", "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"])
                + " " +
                random.choice(["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"])
                for _ in range(NUM_ROWS)
            ],
            "product_id": [f"PROD{random.randint(100, 999)}" for _ in range(NUM_ROWS)],
            "product_name": [
                random.choice(["Laptop", "Phone", "Tablet", "Headphones", "Camera", "Watch", "Speaker", "Monitor", "Keyboard", "Mouse"])
                + " " +
                random.choice(["Pro", "Plus", "Max", "Ultra", "Lite", "Standard", "Premium"])
                for _ in range(NUM_ROWS)
            ],
            "category": random.choices(
                ["Electronics", "Accessories", "Software", "Services", "Peripherals"],
                weights=[40, 25, 15, 12, 8],
                k=NUM_ROWS
            ),
            "sub_category": random.choices(
                ["Gaming", "Business", "Home", "Office", "Mobile", "Audio", "Video"],
                k=NUM_ROWS
            ),
            "quantity": np.random.randint(1, 10, size=NUM_ROWS).tolist(),
            "unit_price": np.round(np.random.uniform(9.99, 1999.99, size=NUM_ROWS), 2).tolist(),
            "discount_percentage": np.round(np.random.uniform(0, 25, size=NUM_ROWS), 2).tolist(),
            "payment_method": random.choices(
                ["Credit Card", "Debit Card", "Cash", "Digital Wallet", "Bank Transfer"],
                weights=[35, 25, 20, 15, 5],
                k=NUM_ROWS
            ),
            "store_location": random.choices(
                ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", 
                 "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"],
                k=NUM_ROWS
            ),
            "region": random.choices(
                ["Northeast", "West", "Midwest", "Southwest", "Southeast"],
                k=NUM_ROWS
            ),
            "is_weekend": [1 if ts.weekday() >= 5 else 0 for ts in timestamps],
            "is_holiday": [0] * NUM_ROWS,
        }
        
        df = pd.DataFrame(data)
        
        df["discount_amount"] = np.round(df["unit_price"] * df["discount_percentage"] / 100, 2)
        df["gross_amount"] = np.round(df["unit_price"] * df["quantity"], 2)
        df["net_amount"] = np.round(df["gross_amount"] - df["discount_amount"], 2)
        df["profit_margin"] = np.round(np.random.uniform(5, 30, size=NUM_ROWS), 2).tolist()
        
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
        
        logger.info(f"Generated {len(df)} rows")
        
        ingest_date = datetime.now().strftime("%Y-%m-%d")
        unique_id = str(uuid.uuid4())[:8]
        filename = f"sales_{unique_id}.parquet"
        key = f"sales/ingest_date={ingest_date}/{filename}"
        
        buffer = BytesIO()
        df.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)
        buffer.seek(0)
        
        s3_client = boto3.client(
            's3',
            endpoint_url="http://minio:9000",
            aws_access_key_id="minio",
            aws_secret_access_key="minio123",
        )
        
        s3_client.put_object(
            Bucket=BRONZE_BUCKET,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/parquet"
        )
        
        logger.info(f"Uploaded to MinIO: {key}")
        
        return {
            "rows": len(df),
            "file_path": key,
            "ingest_date": ingest_date
        }

    result = generate_and_upload()
