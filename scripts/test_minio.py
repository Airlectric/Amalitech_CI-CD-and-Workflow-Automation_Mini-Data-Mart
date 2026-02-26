import sys
sys.path.insert(0, "/opt/airflow/dags")
from utils.minio_hook import MinIOHook

h = MinIOHook(bucket_name="bronze")
files = h.list_files()
print(f"Files found: {len(files)}")
for f in files:
    print(f"  {f}")
