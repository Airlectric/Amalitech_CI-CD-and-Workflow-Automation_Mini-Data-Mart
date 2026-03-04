import json
from unittest.mock import MagicMock, Mock, patch

import pytest


class TestRemediationDAGStructure:
    """Test remediation DAG structure and configuration."""

    def test_dag_imports(self):
        import remediation

        assert remediation is not None

    def test_dag_id(self):
        from remediation import dag

        assert dag.dag_id == "remediation_workflow"

    def test_dag_schedule_is_manual(self):
        from remediation import dag

        assert dag.schedule is None

    def test_dag_has_expected_tasks(self):
        from remediation import dag

        task_ids = [t.task_id for t in dag.tasks]
        assert "remediate_all_batches" in task_ids
        assert "send_remediation_notification" in task_ids
        assert "trigger_gold_rebuild" in task_ids

    def test_default_args(self):
        from remediation import default_args

        assert default_args["owner"] == "airflow"
        assert default_args["retries"] == 1
        assert default_args["email_on_failure"] is True


class TestValidateRecords:
    """Test _validate_records helper."""

    def _get_validate(self):
        from remediation import _validate_records

        return _validate_records

    def test_valid_record(self):
        validate = self._get_validate()
        records = [
            {
                "id": 1,
                "ingestion_run_id": "00000000-0000-0000-0000-000000000001",
                "payload": json.dumps(
                    {
                        "transaction_id": "TXN001",
                        "sale_date": "2026-01-01",
                        "product_id": "PROD001",
                        "quantity": 5,
                        "unit_price": 10.00,
                        "customer_id": "CUST001",
                    }
                ),
                "error_reason": "test",
                "source_file": "test.parquet",
                "failed_at": "2026-01-01",
                "retry_count": 0,
            }
        ]
        valid, invalid = validate(records)
        assert len(valid) == 1
        assert len(invalid) == 0

    def test_missing_fields(self):
        validate = self._get_validate()
        records = [
            {
                "id": 1,
                "ingestion_run_id": "00000000-0000-0000-0000-000000000001",
                "payload": json.dumps({"transaction_id": "TXN001"}),
                "error_reason": "test",
                "source_file": "test.parquet",
                "failed_at": "2026-01-01",
                "retry_count": 0,
            }
        ]
        valid, invalid = validate(records)
        assert len(valid) == 0
        assert len(invalid) == 1

    def test_invalid_quantity(self):
        validate = self._get_validate()
        records = [
            {
                "id": 1,
                "ingestion_run_id": "00000000-0000-0000-0000-000000000001",
                "payload": json.dumps(
                    {
                        "transaction_id": "TXN001",
                        "sale_date": "2026-01-01",
                        "product_id": "PROD001",
                        "quantity": -5,
                        "unit_price": 10.00,
                        "customer_id": "CUST001",
                    }
                ),
                "error_reason": "test",
                "source_file": "test.parquet",
                "failed_at": "2026-01-01",
                "retry_count": 0,
            }
        ]
        valid, invalid = validate(records)
        assert len(valid) == 0
        assert len(invalid) == 1

    def test_invalid_price(self):
        validate = self._get_validate()
        records = [
            {
                "id": 1,
                "ingestion_run_id": "00000000-0000-0000-0000-000000000001",
                "payload": json.dumps(
                    {
                        "transaction_id": "TXN001",
                        "sale_date": "2026-01-01",
                        "product_id": "PROD001",
                        "quantity": 5,
                        "unit_price": -10.00,
                        "customer_id": "CUST001",
                    }
                ),
                "error_reason": "test",
                "source_file": "test.parquet",
                "failed_at": "2026-01-01",
                "retry_count": 0,
            }
        ]
        valid, invalid = validate(records)
        assert len(valid) == 0
        assert len(invalid) == 1

    def test_invalid_json_payload(self):
        validate = self._get_validate()
        records = [
            {
                "id": 1,
                "ingestion_run_id": "00000000-0000-0000-0000-000000000001",
                "payload": "not valid json",
                "error_reason": "test",
                "source_file": "test.parquet",
                "failed_at": "2026-01-01",
                "retry_count": 0,
            }
        ]
        valid, invalid = validate(records)
        assert len(valid) == 0
        assert len(invalid) == 1


class TestBatchReplay:
    """Test _batch_replay helper."""

    def test_empty_input_returns_zeros(self):
        from remediation import _batch_replay

        fixed, failed, errors = _batch_replay(Mock(), [])
        assert fixed == 0
        assert failed == 0
        assert errors == []

    @patch("utils.postgres_hook.PostgresLayerHook")
    def test_replay_commits_on_success(self, mock_hook_cls):
        from remediation import _batch_replay

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_hook = Mock()
        mock_hook.get_conn.return_value = mock_conn

        valid_records = [
            {
                "id": 1,
                "ingestion_run_id": "00000000-0000-0000-0000-000000000001",
                "payload": {
                    "transaction_id": "TXN001",
                    "sale_date": "2026-01-01",
                    "sale_hour": 10,
                    "customer_id": "CUST001",
                    "customer_name": "Test",
                    "product_id": "PROD001",
                    "product_name": "Widget",
                    "category": "Electronics",
                    "sub_category": "Gadgets",
                    "quantity": 5,
                    "unit_price": 10.00,
                    "discount_percentage": 0,
                    "payment_method": "Cash",
                    "payment_category": "Offline",
                    "store_location": "NYC",
                    "region": "Northeast",
                    "is_weekend": False,
                    "is_holiday": False,
                },
            }
        ]

        fixed, failed, errors = _batch_replay(mock_hook, valid_records)
        assert fixed == 1
        assert failed == 0
        mock_conn.commit.assert_called_once()

    @patch("utils.postgres_hook.PostgresLayerHook")
    def test_replay_rollback_on_failure(self, mock_hook_cls):
        from remediation import _batch_replay

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.executemany.side_effect = Exception("DB error")
        mock_conn.cursor.return_value = mock_cursor

        mock_hook = Mock()
        mock_hook.get_conn.return_value = mock_conn

        valid_records = [
            {
                "id": 1,
                "ingestion_run_id": "00000000-0000-0000-0000-000000000001",
                "payload": {
                    "transaction_id": "TXN001",
                    "sale_date": "2026-01-01",
                    "sale_hour": 10,
                    "customer_id": "CUST001",
                    "product_id": "PROD001",
                    "quantity": 5,
                    "unit_price": 10.00,
                    "discount_percentage": 0,
                },
            }
        ]

        fixed, failed, errors = _batch_replay(mock_hook, valid_records)
        assert fixed == 0
        assert failed > 0
        mock_conn.rollback.assert_called()


class TestBatchReject:
    """Test _batch_reject helper."""

    def test_empty_input(self):
        from remediation import _batch_reject

        result = _batch_reject(Mock(), [])
        assert result == 0

    @patch("utils.postgres_hook.PostgresLayerHook")
    def test_reject_records(self, mock_hook_cls):
        from remediation import _batch_reject

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_hook = Mock()
        mock_hook.get_conn.return_value = mock_conn

        invalid_records = [
            {"id": 1, "ingestion_run_id": "00000000-0000-0000-0000-000000000001", "reject_reason": "Missing fields"},
            {"id": 2, "ingestion_run_id": "00000000-0000-0000-0000-000000000001", "reject_reason": "Invalid quantity"},
        ]

        result = _batch_reject(mock_hook, invalid_records)
        assert result == 2
        mock_conn.commit.assert_called_once()


class TestGetQuarantineStats:
    """Test _get_quarantine_stats helper."""

    def test_returns_stats(self):
        from remediation import _get_quarantine_stats

        mock_hook = Mock()
        mock_hook.execute_query.return_value = [[], [[100, 10, 80, 5, 5]]]

        result = _get_quarantine_stats(mock_hook)
        assert result["total"] == 100
        assert result["pending"] == 10
        assert result["remediated"] == 80
        assert result["rejected"] == 5
        assert result["dead_letter"] == 5

    def test_empty_results(self):
        from remediation import _get_quarantine_stats

        mock_hook = Mock()
        mock_hook.execute_query.return_value = [[], []]

        result = _get_quarantine_stats(mock_hook)
        assert result["total"] == 0


class TestRemediationIntegration:
    """Integration tests for remediation (require database)."""

    @pytest.mark.integration
    def test_quarantine_table_exists(self):
        import psycopg2

        conn = psycopg2.connect(host="localhost", port=5433, dbname="airflow", user="airflow", password="airflow")
        cursor = conn.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'quarantine' AND table_name = 'sales_failed'
            )
        """)
        assert cursor.fetchone()[0] is True
        conn.close()

    @pytest.mark.integration
    def test_quarantine_table_has_required_columns(self):
        import psycopg2

        conn = psycopg2.connect(host="localhost", port=5433, dbname="airflow", user="airflow", password="airflow")
        cursor = conn.cursor()
        cursor.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'quarantine' AND table_name = 'sales_failed'
            ORDER BY column_name
        """)
        columns = [row[0] for row in cursor.fetchall()]
        conn.close()

        required = [
            "id",
            "payload",
            "error_reason",
            "replayed",
            "replayed_at",
            "failed_at",
            "remediation_status",
            "retry_count",
        ]
        for col in required:
            assert col in columns, f"Column {col} not found in quarantine.sales_failed"
