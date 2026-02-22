#!/bin/bash
set -e

# This script runs ONCE on first PostgreSQL startup (fresh volume only).
# It creates additional databases and users for Airflow and Metabase.
# The primary database ($POSTGRES_DB / sales_db) is created automatically by the postgres image.

echo "==> Creating additional databases and users..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create Airflow database and user (idempotent)
    SELECT 'CREATE USER airflow WITH PASSWORD ''airflow'''
        WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow')\gexec
    SELECT 'CREATE DATABASE airflow OWNER airflow'
        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

    -- Create Metabase database and user (idempotent)
    SELECT 'CREATE USER metabase WITH PASSWORD ''metabase'''
        WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'metabase')\gexec
    SELECT 'CREATE DATABASE metabase OWNER metabase'
        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec
EOSQL

# Grant privileges (safe to re-run)
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
    GRANT ALL PRIVILEGES ON DATABASE metabase TO metabase;
    GRANT ALL PRIVILEGES ON DATABASE airflow TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON DATABASE metabase TO $POSTGRES_USER;
EOSQL

echo "==> Done. Databases: $POSTGRES_DB (app), airflow, metabase"
