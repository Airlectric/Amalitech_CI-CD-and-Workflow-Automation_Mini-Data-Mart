FROM apache/airflow:3.1.1-python3.11
# Switch to root to install system packages first
USER root
# Optional: Install any OS-level deps if needed (e.g., for pandas or other libs)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# Switch to airflow user for Python package installation
USER airflow
# Install Python dependencies for MySQL and PostgreSQL connectors
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
# Create directories with correct permissions (optional, for mounted volumes)
USER root
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/data /opt/airflow/scripts \
    && chown -R airflow: /opt/airflow
