import pytest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from datetime import datetime, date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts", "data_generator"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

from generator import (
    generate_clean_data,
    generate_dirty_data,
    generate_duplicates,
    generate_mixed_data,
    generate_schema_invalid_data,
    DataGenerator,
    ValidationError,
)


class TestDataGenerator:
    """Unit tests for DataGenerator class"""

    def test_generator_initialization(self):
        """Test DataGenerator can be initialized"""
        generator = DataGenerator()
        assert generator is not None

    def test_generate_clean_data_basic(self):
        """Test clean data generation produces valid output"""
        df = generate_clean_data(num_records=10)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 10
        assert "transaction_id" in df.columns
        assert "customer_id" in df.columns
        assert "product_id" in df.columns

    def test_generate_clean_data_no_nulls(self):
        """Test clean data has no null values in required fields"""
        df = generate_clean_data(num_records=100)
        
        required_fields = ["transaction_id", "customer_id", "product_id", "quantity", "unit_price"]
        for field in required_fields:
            assert df[field].notna().all(), f"Field {field} has null values"

    def test_generate_dirty_data_has_nulls(self):
        """Test dirty data contains null values"""
        df = generate_dirty_data(num_records=50, null_percentage=20)
        
        null_count = df.isnull().sum().sum()
        assert null_count > 0, "Dirty data should contain null values"

    def test_generate_duplicates(self):
        """Test duplicate data generation"""
        df = generate_duplicates(num_records=50, duplicate_percentage=30)
        
        total_rows = len(df)
        unique_transactions = df["transaction_id"].nunique()
        
        assert unique_transactions < total_rows, "Should have duplicate transactions"

    def test_generate_mixed_data(self):
        """Test mixed data (good and bad records)"""
        df = generate_mixed_data(num_records=100)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 100

    def test_generate_schema_invalid_data(self):
        """Test schema invalid data generation"""
        df = generate_schema_invalid_data(num_records=20)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 20


class TestDataGeneratorValidation:
    """Tests for data validation in generator"""

    @patch('generator.DataGenerator.validate_row')
    def test_validate_valid_row(self, mock_validate):
        """Test validation passes for valid row"""
        from generator import DataGenerator
        generator = DataGenerator()
        
        valid_row = {
            "transaction_id": "TXN001",
            "customer_id": "CUST001",
            "product_id": "PROD001",
            "quantity": 5,
            "unit_price": 10.00,
        }
        
        result = generator.validate_row(valid_row)
        assert result is True

    def test_validate_invalid_quantity(self):
        """Test validation fails for invalid quantity"""
        from generator import DataGenerator
        generator = DataGenerator()
        
        invalid_row = {
            "transaction_id": "TXN001",
            "customer_id": "CUST001",
            "product_id": "PROD001",
            "quantity": -1,
            "unit_price": 10.00,
        }
        
        result = generator.validate_row(invalid_row)
        assert result is False


class TestDataGeneratorEdgeCases:
    """Edge case tests for data generator"""

    def test_zero_records(self):
        """Test generating zero records"""
        df = generate_clean_data(num_records=0)
        assert len(df) == 0

    def test_single_record(self):
        """Test generating single record"""
        df = generate_clean_data(num_records=1)
        assert len(df) == 1

    def test_large_dataset(self):
        """Test generating large dataset"""
        df = generate_clean_data(num_records=10000)
        assert len(df) == 10000


class TestDataGeneratorOutputFormats:
    """Tests for different output formats"""

    @patch('generator.write_to_minio')
    @patch('generator.create_dataframe')
    def test_output_to_minio(self, mock_create, mock_minio):
        """Test output can be sent to MinIO"""
        mock_create.return_value = pd.DataFrame({"id": [1, 2, 3]})
        
        from generator import DataGenerator
        generator = DataGenerator()
        
        generator.generate_and_store("clean", num_records=10, bucket="test-bucket")
        
        mock_minio.assert_called_once()

    @patch('generator.create_dataframe')
    def test_output_to_local_parquet(self, mock_create):
        """Test output can be saved locally as parquet"""
        mock_create.return_value = pd.DataFrame({"id": [1, 2, 3]})
        
        from generator import DataGenerator
        generator = DataGenerator()
        
        with patch('builtins.open', create=True):
            result = generator.generate_and_store("clean", num_records=10, output_path="/tmp/test.parquet")
            assert result is not None


class TestDataGeneratorIntegration:
    """Integration tests for data generator (require database)"""

    @pytest.mark.integration
    def test_generate_clean_data_integration(self):
        """Test clean data generation with actual DataFrame operations"""
        df = generate_clean_data(num_records=100)
        
        assert df["quantity"].min() >= 0
        assert df["unit_price"].min() >= 0
        assert df["transaction_id"].str.startswith("TXN").all()

    @pytest.mark.integration
    def test_data_types(self):
        """Test generated data has correct types"""
        df = generate_clean_data(num_records=10)
        
        assert pd.api.types.is_string_dtype(df["transaction_id"])
        assert pd.api.types.is_string_dtype(df["customer_id"])
        assert pd.api.types.is_numeric_dtype(df["quantity"])
        assert pd.api.types.is_numeric_dtype(df["unit_price"])
