#!/bin/bash
set -e

echo "Waiting for database to be ready..."
until airflow db check; do
    echo "Database not ready, waiting..."
    sleep 2
done

echo "Running database migrations..."
airflow db migrate

echo "Creating admin user if needed..."
airflow users create \
    --username "${_AIRFLOW_WWW_USER_USERNAME:-admin}" \
    --firstname "${_AIRFLOW_WWW_USER_FIRSTNAME:-Admin}" \
    --lastname "${_AIRFLOW_WWW_USER_LASTNAME:-Admin}" \
    --email "${_AIRFLOW_WWW_USER_EMAIL:-admin@example.com}" \
    --role "${_AIRFLOW_WWW_USER_ROLE:-Admin}" \
    --password "${_AIRFLOW_WWW_USER_PASSWORD:-airflow}" \
    || echo "User already exists, skipping..."

echo "Initialization complete!"
