import pytest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
import json
from datetime import datetime

sys.path.insert(0, "/opt/airflow")
sys.path.insert(0, "/opt/airflow/dags")
sys.path.insert(0, "/opt/airflow/dags/etl")
sys.path.insert(0, "/opt/airflow/scripts")


class TestRemediationDAG:
    """Unit tests for remediation DAG"""

    def test_dag_file_imports(self):
        """Test that remediation.py can be imported"""
        try:
            import remediation
            assert remediation is not None
        except ImportError as e:
            pytest.skip(f"Cannot import remediation: {e}")

    def test_dag_has_correct_id(self):
        """Test DAG has correct ID"""
        from remediation import dag
        assert dag.dag_id == "remediation_workflow"

    def test_dag_has_schedule(self):
        """Test DAG is manual (schedule=None)"""
        from remediation import dag
        assert dag.schedule is None

    def test_dag_has_tasks(self):
        """Test DAG has all required tasks"""
        from remediation import dag
        task_ids = [task.task_id for task in dag.tasks]
        
        assert "get_quarantined_records" in task_ids
        assert "validate_records" in task_ids
        assert "fix_and_replay" in task_ids
        assert "mark_invalid_records" in task_ids
        assert "get_quarantine_stats" in task_ids
        assert "send_remediation_notification" in task_ids

    def test_dag_default_args(self):
        """Test DAG has correct default args"""
        from remediation import default_args
        
        assert default_args["owner"] == "airflow"
        assert default_args["retries"] == 1
        assert default_args["email_on_failure"] is True


class TestRemediationTasks:
    """Test remediation task logic"""

    def test_get_quarantined_records_mock(self):
        """Test get_quarantined_records with mock"""
        from remediation import get_quarantined_records
        
        with patch('remediation.PostgresLayerHook') as mock_hook:
            mock_instance = Mock()
            mock_instance.execute_query.return_value = [
                [],
                [[1, '{"transaction_id": "TXN001"}', "NULL customer_id", "file1.parquet", "2026-01-01"]]
            ]
            mock_hook.return_value = mock_instance
            
            result = get_quarantined_records()
            
            assert len(result) == 1
            assert result[0]["id"] == 1
            assert result[0]["payload"] == '{"transaction_id": "TXN001"}'

    def test_validate_records_with_valid_data(self):
        """Test validate_records with valid data"""
        from remediation import validate_records
        
        records = [
            {
                "id": 1,
                "payload": json.dumps({
                    "transaction_id": "TXN001",
                    "sale_date": "2026-01-01",
                    "product_id": "PROD001",
                    "quantity": 5,
                    "unit_price": 10.00,
                    "customer_id": "CUST001"
                })
            }
        ]
        
        result = validate_records(records)
        
        assert len(result["valid"]) == 1
        assert len(result["invalid"]) == 0

    def test_validate_records_with_missing_fields(self):
        """Test validate_records with missing fields"""
        from remediation import validate_records
        
        records = [
            {
                "id": 1,
                "payload": json.dumps({
                    "transaction_id": "TXN001"
                })
            }
        ]
        
        result = validate_records(records)
        
        assert len(result["valid"]) == 0
        assert len(result["invalid"]) == 1

    def test_validate_records_with_invalid_quantity(self):
        """Test validate_records with invalid quantity"""
        from remediation import validate_records
        
        records = [
            {
                "id": 1,
                "payload": json.dumps({
                    "transaction_id": "TXN001",
                    "sale_date": "2026-01-01",
                    "product_id": "PROD001",
                    "quantity": -5,
                    "unit_price": 10.00,
                    "customer_id": "CUST001"
                })
            }
        ]
        
        result = validate_records(records)
        
        assert len(result["valid"]) == 0
        assert len(result["invalid"]) == 1

    def test_validate_records_with_invalid_price(self):
        """Test validate_records with negative price"""
        from remediation import validate_records
        
        records = [
            {
                "id": 1,
                "payload": json.dumps({
                    "transaction_id": "TXN001",
                    "sale_date": "2026-01-01",
                    "product_id": "PROD001",
                    "quantity": 5,
                    "unit_price": -10.00,
                    "customer_id": "CUST001"
                })
            }
        ]
        
        result = validate_records(records)
        
        assert len(result["valid"]) == 0
        assert len(result["invalid"]) == 1

    def test_validate_records_with_string_payload(self):
        """Test validate_records handles string payload"""
        from remediation import validate_records
        
        records = [
            {
                "id": 1,
                "payload": '{"transaction_id": "TXN001", "sale_date": "2026-01-01", "product_id": "PROD001", "quantity": 5, "unit_price": 10.00, "customer_id": "CUST001"}'
            }
        ]
        
        result = validate_records(records)
        
        assert len(result["valid"]) == 1

    def test_validate_records_with_invalid_json(self):
        """Test validate_records handles invalid JSON"""
        from remediation import validate_records
        
        records = [
            {
                "id": 1,
                "payload": "invalid json"
            }
        ]
        
        result = validate_records(records)
        
        assert len(result["valid"]) == 0
        assert len(result["invalid"]) == 1


class TestFixAndReplay:
    """Test fix_and_replay task logic"""

    def test_fix_and_replay_with_empty_records(self):
        """Test fix_and_replay with no valid records"""
        from remediation import fix_and_replay
        
        validation_result = {"valid": [], "invalid": []}
        
        result = fix_and_replay(validation_result)
        
        assert result["fixed"] == 0
        assert result["failed"] == 0
        assert result["message"] == "No valid records to fix"

    @patch('remediation.PostgresLayerHook')
    def test_fix_and_replay_with_valid_records(self, mock_hook):
        """Test fix_and_replay with valid records"""
        from remediation import fix_and_replay
        
        mock_instance = Mock()
        mock_instance.execute_query.return_value = [[], []]
        mock_hook.return_value = mock_instance
        
        validation_result = {
            "valid": [
                {
                    "id": 1,
                    "payload": json.dumps({
                        "transaction_id": "TXN001",
                        "sale_date": "2026-01-01",
                        "sale_hour": 10,
                        "product_id": "PROD001",
                        "product_name": "Test Product",
                        "category": "Electronics",
                        "sub_category": "Phones",
                        "quantity": 5,
                        "unit_price": 10.00,
                        "discount_percentage": 0,
                        "customer_id": "CUST001",
                        "customer_name": "John Doe",
                        "payment_method": "Credit Card",
                        "payment_category": "Online",
                        "store_location": "Store A",
                        "region": "North",
                        "is_weekend": False,
                        "is_holiday": False,
                        "source_file": "test.parquet"
                    })
                }
            ],
            "invalid": []
        }
        
        result = fix_and_replay(validation_result)
        
        assert result["fixed"] == 1
        assert result["failed"] == 0


class TestQuarantineStats:
    """Test get_quarantine_stats task"""

    @patch('remediation.PostgresLayerHook')
    def test_get_quarantine_stats(self, mock_hook):
        """Test get_quarantine_stats returns correct stats"""
        from remediation import get_quarantine_stats
        
        mock_instance = Mock()
        mock_instance.execute_query.return_value = [
            [],
            [[100, 10, 90, 85, 5]]
        ]
        mock_hook.return_value = mock_instance
        
        result = get_quarantine_stats()
        
        assert result["total"] == 100
        assert result["pending"] == 10
        assert result["processed"] == 90
        assert result["remediated"] == 85
        assert result["rejected"] == 5


class TestMarkInvalidRecords:
    """Test mark_invalid_records task"""

    @patch('remediation.PostgresLayerHook')
    def test_mark_invalid_records_with_invalid(self, mock_hook):
        """Test marking invalid records"""
        from remediation import mark_invalid_records
        
        mock_instance = Mock()
        mock_instance.execute_query.return_value = [[], []]
        mock_hook.return_value = mock_instance
        
        validation_result = {
            "invalid": [
                {"id": 1, "reason": "Missing fields"},
                {"id": 2, "reason": "Invalid quantity"}
            ]
        }
        
        result = mark_invalid_records(validation_result)
        
        assert result["marked"] == 2

    def test_mark_invalid_records_with_no_invalid(self):
        """Test marking when no invalid records"""
        from remediation import mark_invalid_records
        
        validation_result = {"invalid": []}
        
        result = mark_invalid_records(validation_result)
        
        assert result["marked"] == 0


class TestRemediationIntegration:
    """Integration tests for remediation (require database)"""

    @pytest.mark.integration
    def test_quarantine_table_exists(self):
        """Test quarantine table exists"""
        import psycopg2
        
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="airflow",
            user="airflow",
            password="airflow"
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'quarantine' 
                AND table_name = 'sales_failed'
            )
        """)
        result = cursor.fetchone()[0]
        conn.close()
        
        assert result is True

    @pytest.mark.integration
    def test_quarantine_table_structure(self):
        """Test quarantine table has correct columns"""
        import psycopg2
        
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="airflow",
            user="airflow",
            password="airflow"
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'quarantine' 
            AND table_name = 'sales_failed'
            ORDER BY column_name
        """)
        columns = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        required = ["id", "payload", "error_reason", "replayed", "replayed_at", "failed_at"]
        for col in required:
            assert col in columns, f"Column {col} not found"

    @pytest.mark.integration
    def test_silver_sales_table_writable(self):
        """Test silver.sales table is writable"""
        import psycopg2
        
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="airflow",
            user="airflow",
            password="airflow"
        )
        cursor = conn.cursor()
        
        test_transaction_id = f"TEST_REMEDIATION_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        cursor.execute("""
            INSERT INTO silver.sales (
                transaction_id, sale_date, sale_hour, customer_id,
                product_id, quantity, unit_price, discount_percentage,
                discount_amount, gross_amount, net_amount, profit_margin,
                payment_method, store_location, is_weekend, is_holiday
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (transaction_id) DO NOTHING
        """, (
            test_transaction_id,
            datetime.now().date(),
            12,
            "TEST_CUSTOMER",
            "TEST_PRODUCT",
            1,
            10.00,
            0,
            0.00,
            10.00,
            10.00,
            0.00,
            "Cash",
            "Test Store",
            False,
            False
        ))
        
        conn.commit()
        
        cursor.execute("SELECT COUNT(*) FROM silver.sales WHERE transaction_id = %s", (test_transaction_id,))
        count = cursor.fetchone()[0]
        
        conn.close()
        
        assert count > 0
