#!/bin/sh

# Wait for MinIO to be ready by retrying mc alias set
echo "==> Waiting for MinIO to start..."
until mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" > /dev/null 2>&1; do
    sleep 3
done
echo "==> MinIO is ready"

# Create buckets (--ignore-existing makes this idempotent)
mc mb local/raw-data --ignore-existing
mc mb local/processed-data --ignore-existing

echo "==> Buckets created: raw-data, processed-data"
mc ls local/
