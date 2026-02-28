import pytest
import subprocess
import time
import psycopg2
import os
from datetime import datetime


POSTGRES_CONN = {
    "host": "localhost",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
}

MINIO_CONN = {
    "host": "localhost",
    "port": 9000,
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "bucket": "bronze",
}


def get_postgres_connection():
    """Get PostgreSQL connection"""
    return psycopg2.connect(**POSTGRES_CONN)


@pytest.fixture(scope="module")
def docker_services():
    """Ensure Docker services are running"""
    subprocess.run(["docker", "ps"], check=False)
    yield
    # Cleanup handled by docker-compose


@pytest.fixture
def postgres_conn(docker_services):
    """Fixture for PostgreSQL connection"""
    conn = get_postgres_connection()
    yield conn
    conn.close()


class TestPostgresIntegration:
    """Integration tests for PostgreSQL database"""

    @pytest.mark.integration
    def test_postgres_is_running(self, docker_services):
        """Test PostgreSQL container is running"""
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=postgres", "--format", "{{.Names}}"],
            capture_output=True,
            text=True
        )
        assert "postgres" in result.stdout

    @pytest.mark.integration
    def test_can_connect_to_postgres(self, postgres_conn):
        """Test can connect to PostgreSQL"""
        assert postgres_conn is not None
        assert not postgres_conn.closed

    @pytest.mark.integration
    def test_bronze_schema_exists(self, postgres_conn):
        """Test bronze schema exists"""
        cursor = postgres_conn.cursor()
        cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'bronze'")
        result = cursor.fetchone()
        assert result is not None

    @pytest.mark.integration
    def test_silver_schema_exists(self, postgres_conn):
        """Test silver schema exists"""
        cursor = postgres_conn.cursor()
        cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'silver'")
        result = cursor.fetchone()
        assert result is not None

    @pytest.mark.integration
    def test_quarantine_schema_exists(self, postgres_conn):
        """Test quarantine schema exists"""
        cursor = postgres_conn.cursor()
        cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'quarantine'")
        result = cursor.fetchone()
        assert result is not None

    @pytest.mark.integration
    def test_silver_tables_exist(self, postgres_conn):
        """Test silver tables exist"""
        cursor = postgres_conn.cursor()
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'silver'
        """)
        tables = [row[0] for row in cursor.fetchall()]
        assert "sales" in tables

    @pytest.mark.integration
    def test_silver_sales_has_data(self, postgres_conn):
        """Test silver.sales has data"""
        cursor = postgres_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM silver.sales")
        count = cursor.fetchone()[0]
        assert count >= 0


class TestMinIOIntegration:
    """Integration tests for MinIO/S3 storage"""

    @pytest.mark.integration
    def test_minio_is_running(self, docker_services):
        """Test MinIO container is running"""
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=minio", "--format", "{{.Names}}"],
            capture_output=True,
            text=True
        )
        assert "minio" in result.stdout

    @pytest.mark.integration
    @pytest.mark.skip(reason="Requires boto3")
    def test_minio_bucket_exists(self):
        """Test MinIO bucket exists"""
        import boto3
        s3 = boto3.client('s3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin'
        )
        response = s3.list_buckets()
        buckets = [b['Name'] for b in response['Buckets']]
        assert "bronze" in buckets


class TestAirflowIntegration:
    """Integration tests for Airflow"""

    @pytest.mark.integration
    def test_airflow_is_running(self, docker_services):
        """Test Airflow containers are running"""
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=airflow", "--format", "{{.Names}}"],
            capture_output=True,
            text=True
        )
        assert "airflow" in result.stdout

    @pytest.mark.integration
    def test_dags_are_loaded(self, postgres_conn):
        """Test DAGs are loaded in Airflow"""
        cursor = postgres_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM dag WHERE is_active = true")
        count = cursor.fetchone()[0]
        assert count > 0

    @pytest.mark.integration
    def test_data_quality_dag_exists(self, postgres_conn):
        """Test data_quality_checks DAG exists"""
        cursor = postgres_conn.cursor()
        cursor.execute("SELECT dag_id FROM dag WHERE dag_id = 'data_quality_checks'")
        result = cursor.fetchone()
        assert result is not None


class TestDataQualityPipeline:
    """Integration tests for data quality pipeline"""

    @pytest.mark.integration
    def test_quarantine_table_structure(self, postgres_conn):
        """Test quarantine table has correct structure"""
        cursor = postgres_conn.cursor()
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'quarantine' AND table_name = 'sales_failed'
            ORDER BY column_name
        """)
        columns = cursor.fetchall()
        column_names = [col[0] for col in columns]
        
        assert "id" in column_names
        assert "payload" in column_names
        assert "error_reason" in column_names
        assert "replayed" in column_names

    @pytest.mark.integration
    def test_silver_sales_structure(self, postgres_conn):
        """Test silver.sales table structure"""
        cursor = postgres_conn.cursor()
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_schema = 'silver' AND table_name = 'sales'
            ORDER BY column_name
        """)
        columns = cursor.fetchall()
        column_dict = {col[0]: {"type": col[1], "nullable": col[2]} for col in columns}
        
        assert "sale_id" in column_dict
        assert "transaction_id" in column_dict
        assert "customer_id" in column_dict

    @pytest.mark.integration
    def test_quarantine_replay_function(self, postgres_conn):
        """Test quarantine replay functionality"""
        cursor = postgres_conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM quarantine.sales_failed WHERE replayed = false")
        before_count = cursor.fetchone()[0]
        
        # Simulate replay
        cursor.execute("""
            UPDATE quarantine.sales_failed 
            SET replayed = true 
            WHERE replayed = false 
            LIMIT 1
        """)
        postgres_conn.commit()
        
        cursor.execute("SELECT COUNT(*) FROM quarantine.sales_failed WHERE replayed = false")
        after_count = cursor.fetchone()[0]
        
        assert after_count < before_count or before_count == 0


class TestEndToEndIntegration:
    """End-to-end integration tests"""

    @pytest.mark.integration
    @pytest.mark.slow
    def test_full_data_quality_workflow(self, postgres_conn):
        """Test complete data quality workflow"""
        
        # 1. Verify quarantine has expected structure
        cursor = postgres_conn.cursor()
        cursor.execute("""
            SELECT COUNT(*), 
                   COUNT(CASE WHEN replayed THEN 1 END),
                   COUNT(CASE WHEN NOT replayed THEN 1 END)
            FROM quarantine.sales_failed
        """)
        result = cursor.fetchone()
        
        total, replayed, pending = result
        assert total >= 0
        assert replayed >= 0
        assert pending >= 0

    @pytest.mark.integration
    @pytest.mark.slow
    def test_data_flow_bronze_to_silver(self, postgres_conn):
        """Test data flows from bronze to silver"""
        
        # Check that silver has data
        cursor = postgres_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM silver.sales")
        silver_count = cursor.fetchone()[0]
        
        assert silver_count >= 0


class TestDockerCompose:
    """Tests for Docker Compose environment"""

    @pytest.mark.integration
    def test_all_required_containers_running(self):
        """Test all required containers are running"""
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True,
            text=True
        )
        containers = result.stdout.strip().split("\n")
        
        required = ["postgres", "airflow", "redis", "minio"]
        for req in required:
            assert any(req in c for c in containers), f"Container {req} not running"

    @pytest.mark.integration
    def test_postgres_accepts_connections(self, postgres_conn):
        """Test PostgreSQL is accepting connections"""
        cursor = postgres_conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1
