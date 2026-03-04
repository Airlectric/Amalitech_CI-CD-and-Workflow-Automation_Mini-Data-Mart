from datetime import datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError


class MinIOHook(S3Hook):
    def __init__(self, aws_conn_id: str = "minio_default", bucket_name: str = "bronze", **kwargs):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)
        self.bucket_name = bucket_name
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        try:
            s3_client = self.get_conn()
            s3_client.head_bucket(Bucket=self.bucket_name)
        except ClientError:
            self.get_conn().create_bucket(Bucket=self.bucket_name)

    def list_files(self, prefix: str = "", suffix: str | None = None, ingest_date: datetime | None = None) -> list[str]:
        if ingest_date:
            date_prefix = f"{prefix}ingest_date={ingest_date.strftime('%Y-%m-%d')}/"
        else:
            date_prefix = prefix

        files = []
        paginator = self.get_conn().get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket_name, Prefix=date_prefix)

        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    key = obj["Key"]
                    if suffix is None or key.endswith(suffix):
                        files.append(key)

        return files

    def read_parquet(self, key: str, engine: str = "pyarrow"):
        from io import BytesIO

        import pandas as pd

        s3_client = self.get_conn()
        response = s3_client.get_object(Bucket=self.bucket_name, Key=key)
        body = response["Body"].read()
        return pd.read_parquet(BytesIO(body), engine=engine)

    def get_file_metadata(self, key: str) -> dict:
        s3_client = self.get_conn()
        response = s3_client.head_object(Bucket=self.bucket_name, Key=key)
        return {
            "key": key,
            "size": response.get("ContentLength"),
            "last_modified": response.get("LastModified"),
            "etag": response.get("ETag"),
            "metadata": response.get("Metadata", {}),
        }

    def upload_file(self, local_file: str, key: str, metadata: dict | None = None):
        extra_args = {}
        if metadata:
            extra_args["Metadata"] = metadata

        self.get_conn().upload_file(local_file, self.bucket_name, key, ExtraArgs=extra_args)

    def delete_file(self, key: str):
        self.get_conn().delete_object(Bucket=self.bucket_name, Key=key)

    def get_ingest_dates(self, prefix: str = "") -> list[datetime]:
        files = self.list_files(prefix=prefix)
        dates = set()

        for f in files:
            parts = f.split("/")
            for part in parts:
                if part.startswith("ingest_date="):
                    date_str = part.split("=")[1]
                    dates.add(datetime.strptime(date_str, "%Y-%m-%d"))

        return sorted(list(dates))
