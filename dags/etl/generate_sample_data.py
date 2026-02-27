"""
Data Generator DAG
Generates different data quality scenarios to test the pipeline
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow/dags")
sys.path.insert(0, "/opt/airflow/scripts")

from utils.minio_hook import MinIOHook
from data_generator.generator import generate_sales_data


BRONZE_BUCKET = "bronze"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def upload_to_minio(df, mode: str):
    """Upload generated dataframe to MinIO"""
    import uuid
    import boto3
    from io import BytesIO
    
    ingest_date = datetime.now().strftime("%Y-%m-%d")
    unique_id = str(uuid.uuid4())[:8]
    filename = f"sales_{mode}_{unique_id}.parquet"
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
    
    return {"rows": len(df), "file_path": key, "mode": mode}


def generate_and_upload(mode: str, num_rows: int = 1000, **kwargs):
    """Generate data and upload to MinIO"""
    import logging
    logger = logging.getLogger(__name__)
    
    logger.info(f"Generating data with mode={mode}, rows={num_rows}")
    df = generate_sales_data(num_rows=num_rows, mode=mode)
    result = upload_to_minio(df, mode)
    logger.info(f"Uploaded to MinIO: {result['file_path']}")
    
    return result


MODES = [
    ("generate_clean", "clean", 1000),
    ("generate_dirty", "dirty", 1000),
    ("generate_duplicates", "duplicates", 1000),
    ("generate_mixed", "mixed", 1000),
    ("generate_schema_invalid", "schema_invalid", 1000),
]


with DAG(
    dag_id="generate_sample_data",
    start_date=datetime(2026, 1, 1),
    schedule="0 6,12,18 * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["etl", "data-generation", "bronze"],
    description="Generate sample data and upload to MinIO Bronze layer"
) as dag:

    for task_id, mode, rows in MODES:
        PythonOperator(
            task_id=task_id,
            python_callable=generate_and_upload,
            op_kwargs={"mode": mode, "num_rows": rows}
        )
