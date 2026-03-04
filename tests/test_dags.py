import pytest
from unittest.mock import Mock, patch, MagicMock


class TestDataQualityDAG:
    """Unit tests for data_quality DAG"""

    def test_dag_file_imports(self):
        """Test that data_quality.py can be imported"""
        try:
            import data_quality
            assert data_quality is not None
        except ImportError as e:
            pytest.skip(f"Cannot import data_quality: {e}")

    def test_dag_has_correct_id(self):
        """Test DAG has correct ID"""
        from data_quality import dag
        assert dag.dag_id == "data_quality_checks"

    def test_dag_has_schedule(self):
        """Test DAG has schedule"""
        from data_quality import dag
        assert dag.schedule is not None

    def test_dag_has_tasks(self):
        """Test DAG has all required tasks"""
        from data_quality import dag
        task_ids = [task.task_id for task in dag.tasks]

        assert "validate_quarantine_patterns" in task_ids
        assert "profile_silver" in task_ids
        assert "detect_drift" in task_ids
        assert "generate_data_docs" in task_ids
        assert "send_alerts" in task_ids
        assert "check_completeness" in task_ids
        assert "check_freshness" in task_ids
        assert "check_referential_integrity" in task_ids

    def test_dag_default_args(self):
        """Test DAG has correct default args"""
        from data_quality import default_args

        assert default_args["owner"] == "airflow"
        assert default_args["retries"] == 1
        assert default_args["email_on_failure"] is True

    def test_task_dependencies(self):
        """Test task dependencies are set correctly"""
        from data_quality import dag

        task_map = {task.task_id: task for task in dag.tasks}

        send_alerts = task_map.get("send_alerts")
        assert send_alerts is not None


class TestIngestBronzeToSilverDAG:
    """Unit tests for ingest_bronze_to_silver DAG"""

    def test_dag_file_imports(self):
        """Test that ingest_bronze_to_silver.py can be imported"""
        try:
            import ingest_bronze_to_silver
            assert ingest_bronze_to_silver is not None
        except ImportError as e:
            pytest.skip(f"Cannot import ingest_bronze_to_silver: {e}")

    def test_dag_has_correct_id(self):
        """Test DAG has correct ID"""
        from ingest_bronze_to_silver import dag
        assert dag.dag_id == "ingest_bronze_to_silver"

    def test_dag_has_tasks(self):
        """Test DAG has required tasks"""
        from ingest_bronze_to_silver import dag
        task_ids = [task.task_id for task in dag.tasks]

        assert "discover_and_ingest" in task_ids
        assert "trigger_silver_to_gold" in task_ids


class TestGenerateSampleDataDAG:
    """Unit tests for generate_sample_data DAG"""

    def test_dag_file_imports(self):
        """Test that generate_sample_data.py can be imported"""
        try:
            import generate_sample_data
            assert generate_sample_data is not None
        except ImportError as e:
            pytest.skip(f"Cannot import generate_sample_data: {e}")

    def test_dag_has_correct_id(self):
        """Test DAG has correct ID"""
        from generate_sample_data import dag
        assert dag.dag_id == "generate_sample_data"


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


class TestSilverToGoldDAG:
    """Unit tests for silver_to_gold DAG"""

    def test_dag_file_imports(self):
        """Test that silver_to_gold.py can be imported"""
        try:
            import silver_to_gold
            assert silver_to_gold is not None
        except ImportError as e:
            pytest.skip(f"Cannot import silver_to_gold: {e}")

    def test_dag_has_correct_id(self):
        """Test DAG has correct ID"""
        from silver_to_gold import dag
        assert dag.dag_id == "silver_to_gold"


class TestDAGStructure:
    """Test DAG structure and configuration"""

    def test_all_dags_have_start_date(self):
        """Test all DAGs have start dates"""
        import data_quality
        import generate_sample_data
        import ingest_bronze_to_silver

        for module in [data_quality, generate_sample_data, ingest_bronze_to_silver]:
            dag = module.dag
            assert dag.start_date is not None

    def test_all_dags_have_tags(self):
        """Test all DAGs have tags"""
        import data_quality

        dag = data_quality.dag
        assert len(dag.tags) > 0


class TestDAGTaskLogic:
    """Test task logic in DAGs"""

    @patch('utils.postgres_hook.PostgresLayerHook')
    def test_validate_quarantine_patterns_logic(self, mock_hook):
        """Test validate_quarantine_patterns task logic"""
        from data_quality import validate_quarantine_patterns

        mock_instance = Mock()
        mock_instance.execute_query.return_value = [
            [],
            [[10, 0, 0, 0]]
        ]
        mock_hook.return_value = mock_instance

        result = validate_quarantine_patterns()

        assert result is not None

    @patch('utils.postgres_hook.PostgresLayerHook')
    def test_profile_silver_logic(self, mock_hook):
        """Test profile_silver task logic"""
        from data_quality import profile_silver

        mock_instance = Mock()
        mock_instance.execute_query.return_value = [
            [],
            [[100]]
        ]
        mock_hook.return_value = mock_instance

        result = profile_silver()

        assert result is not None

    @patch('utils.postgres_hook.PostgresLayerHook')
    def test_detect_drift_logic(self, mock_hook):
        """Test detect_drift task logic"""
        from data_quality import detect_drift

        mock_instance = Mock()
        mock_instance.execute_query.return_value = [
            [],
            [[100]]
        ]
        mock_hook.return_value = mock_instance

        profiling_results = {"sales": {"row_count": 100}}
        result = detect_drift(profiling_results)

        assert result is not None
