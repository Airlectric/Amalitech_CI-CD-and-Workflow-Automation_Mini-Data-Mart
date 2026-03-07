#!/bin/bash
set -e

# Fix ownership of mounted volumes so the airflow user (UID 50000) can write
echo "Fixing directory permissions..."
chown -R 50000:0 /opt/airflow/data /opt/airflow/logs
mkdir -p /opt/airflow/data/data_docs
chown -R 50000:0 /opt/airflow/data/data_docs

# Run airflow commands as the airflow user
run_as_airflow() {
    su -s /bin/bash airflow -c "$*"
}

echo "Waiting for database to be ready..."
until run_as_airflow "airflow db check" 2>/dev/null; do
    echo "Database not ready, waiting..."
    sleep 2
done

echo "Running database migrations..."
run_as_airflow "airflow db migrate"

echo "Creating admin user if needed..."
run_as_airflow "airflow users create \
    --username '${_AIRFLOW_WWW_USER_USERNAME:-admin}' \
    --firstname '${_AIRFLOW_WWW_USER_FIRSTNAME:-Admin}' \
    --lastname '${_AIRFLOW_WWW_USER_LASTNAME:-Admin}' \
    --email '${_AIRFLOW_WWW_USER_EMAIL:-admin@example.com}' \
    --role '${_AIRFLOW_WWW_USER_ROLE:-Admin}' \
    --password '${_AIRFLOW_WWW_USER_PASSWORD:-airflow}'" \
    || echo "User already exists, skipping..."

echo "Initialization complete!"
