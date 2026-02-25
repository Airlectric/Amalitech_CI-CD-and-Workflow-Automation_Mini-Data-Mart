#!/bin/sh

echo "==> Waiting for MinIO to start..."
until mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" > /dev/null 2>&1; do
    sleep 3
done
echo "==> MinIO is ready"

mc mb local/bronze --ignore-existing

echo "==> Bucket created: bronze"
mc ls local/
