FROM apache/airflow:2.10.5-python3.12

USER airflow

# Install Python dependencies for the data pipeline
RUN pip install --no-cache-dir \
    boto3==1.35.* \
    pandas==2.2.* \
    psycopg2-binary==2.9.* \
    apache-airflow-providers-amazon==9.1.* \
    apache-airflow-providers-postgres==5.13.*
