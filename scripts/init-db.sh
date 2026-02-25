#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create Silver schema  
    CREATE SCHEMA IF NOT EXISTS silver;
    
    -- Create Gold schema
    CREATE SCHEMA IF NOT EXISTS gold;
    
    -- Create metadata schema for tracking ingestion state
    CREATE SCHEMA IF NOT EXISTS metadata;
    
    -- Ingestion metadata table - tracks processing state
    CREATE TABLE IF NOT EXISTS metadata.ingestion_metadata (
        file_path TEXT PRIMARY KEY,
        dataset_name TEXT NOT NULL,
        checksum TEXT,
        ingest_date DATE NOT NULL,
        status TEXT NOT NULL DEFAULT 'NEW',
        record_count INTEGER,
        processed_at TIMESTAMP,
        airflow_run_id TEXT,
        error_message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Index for faster lookups
    CREATE INDEX IF NOT EXISTS idx_metadata_status ON metadata.ingestion_metadata(status);
    CREATE INDEX IF NOT EXISTS idx_metadata_ingest_date ON metadata.ingestion_metadata(ingest_date);
    CREATE INDEX IF NOT EXISTS idx_metadata_dataset ON metadata.ingestion_metadata(dataset_name);
    
    -- Create tables
    \i /opt/airflow/sql/silver/create_silver_tables.sql
    \i /opt/airflow/sql/gold/create_gold_tables.sql
    
    -- Grant privileges
    GRANT USAGE ON SCHEMA silver TO airflow;
    GRANT USAGE ON SCHEMA gold TO airflow;
    GRANT USAGE ON SCHEMA metadata TO airflow;
    
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO airflow;
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO airflow;
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA metadata TO airflow;
EOSQL

echo "==> Database schemas initialized: silver, gold, metadata"
