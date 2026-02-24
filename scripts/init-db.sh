#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create Bronze schema
    CREATE SCHEMA IF NOT EXISTS bronze;
    
    -- Create Silver schema  
    CREATE SCHEMA IF NOT EXISTS silver;
    
    -- Create Gold schema
    CREATE SCHEMA IF NOT EXISTS gold;
    
    -- Create tables in correct order (respecting foreign keys)
    \i /opt/airflow/sql/bronze/create_bronze_sales.sql
    \i /opt/airflow/sql/silver/create_silver_tables.sql
    \i /opt/airflow/sql/gold/create_gold_tables.sql
    
    GRANT USAGE ON SCHEMA bronze TO airflow;
    GRANT USAGE ON SCHEMA silver TO airflow;
    GRANT USAGE ON SCHEMA gold TO airflow;
    
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO airflow;
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO airflow;
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO airflow;
EOSQL

echo "==> Database schemas initialized: bronze, silver, gold"
