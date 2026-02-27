#!/usr/bin/env python
"""
Test runner for data quality pipeline
Run this from inside the Airflow container: python /opt/airflow/scripts/run_tests.py
"""
import os
import sys
import subprocess

os.chdir("/opt/airflow")
sys.path.insert(0, "/opt/airflow")
sys.path.insert(0, "/opt/airflow/dags")
sys.path.insert(0, "/opt/airflow/scripts")

def run_tests():
    print("=" * 60)
    print("Running Unit Tests")
    print("=" * 60)
    
    # Test data generator
    print("\n[1/3] Testing Data Generator...")
    try:
        from scripts.data_generator.generator import (
            generate_clean_data,
            generate_dirty_data,
            generate_duplicates,
        )
        
        # Test clean data
        df = generate_clean_data(num_records=10)
        print(f"  - generate_clean_data: {len(df)} records OK")
        
        # Test dirty data
        df = generate_dirty_data(num_records=10, null_percentage=20)
        print(f"  - generate_dirty_data: {len(df)} records OK")
        
        # Test duplicates
        df = generate_duplicates(num_records=10, duplicate_percentage=30)
        print(f"  - generate_duplicates: {len(df)} records OK")
        
        print("  [PASS] Data Generator")
    except Exception as e:
        print(f"  [FAIL] Data Generator: {e}")
    
    # Test DAG imports
    print("\n[2/3] Testing DAG Imports...")
    try:
        sys.path.insert(0, "/opt/airflow/dags/etl")
        import data_quality
        print(f"  - data_quality: OK (dag_id={data_quality.dag.dag_id})")
        
        import ingest_bronze_to_silver
        print(f"  - ingest_bronze_to_silver: OK (dag_id={ingest_bronze_to_silver.dag.dag_id})")
        
        import generate_sample_data
        print(f"  - generate_sample_data: OK (dag_id={generate_sample_data.dag.dag_id})")
        
        import remediation
        print(f"  - remediation: OK (dag_id={remediation.dag.dag_id})")
        
        import silver_to_gold
        print(f"  - silver_to_gold: OK (dag_id={silver_to_gold.dag.dag_id})")
        
        print("  [PASS] DAG Imports")
    except Exception as e:
        print(f"  [FAIL] DAG Imports: {e}")
    
    # Test Postgres Hook
    print("\n[3/3] Testing Postgres Hook...")
    try:
        from utils.postgres_hook import PostgresLayerHook
        hook = PostgresLayerHook()
        result = hook.execute_query("SELECT 1 as test")
        print(f"  - PostgresLayerHook: connection OK")
        print("  [PASS] Postgres Hook")
    except Exception as e:
        print(f"  [FAIL] Postgres Hook: {e}")
    
    print("\n" + "=" * 60)
    print("All Basic Tests Completed")
    print("=" * 60)

if __name__ == "__main__":
    run_tests()
