#!/bin/bash

set -e

echo "Setting up Airflow connections..."

airflow connections add 'minio_default' \
    --conn-type 'aws' \
    --conn-host 'http://minio:9000' \
    --conn-login 'minio' \
    --conn-password 'minio123' \
    --conn-extra '{"endpoint_url": "http://minio:9000"}'

airflow connections add 'postgres_default' \
    --conn-type 'postgres' \
    --conn-host 'postgres' \
    --conn-login 'airflow' \
    --conn-password 'airflow' \
    --conn-schema 'airflow' \
    --conn-port '5432'

echo "Connections created successfully!"
