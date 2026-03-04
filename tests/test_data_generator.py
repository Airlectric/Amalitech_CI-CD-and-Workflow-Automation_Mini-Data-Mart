import pandas as pd
from generator import generate_sales_data, generate_schema_invalid_data


class TestGenerateSalesData:
    """Tests for generate_sales_data with different modes."""

    def test_clean_mode_basic(self):
        df = generate_sales_data(num_rows=10, mode="clean")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 10
        assert "transaction_id" in df.columns
        assert "customer_id" in df.columns
        assert "product_id" in df.columns

    def test_clean_mode_no_nulls_in_required_fields(self):
        df = generate_sales_data(num_rows=100, mode="clean")
        required = ["transaction_id", "customer_id", "product_id", "quantity", "unit_price"]
        for field in required:
            assert df[field].notna().all(), f"Field {field} has null values in clean mode"

    def test_dirty_mode_has_nulls(self):
        df = generate_sales_data(num_rows=100, mode="dirty")
        null_count = df.isnull().sum().sum()
        assert null_count > 0, "Dirty mode should produce null values"

    def test_duplicates_mode(self):
        df = generate_sales_data(num_rows=100, mode="duplicates")
        unique_txns = df["transaction_id"].nunique()
        assert unique_txns < len(df), "Duplicates mode should have duplicate transaction_ids"

    def test_mixed_mode(self):
        df = generate_sales_data(num_rows=100, mode="mixed")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 100

    def test_edge_cases_mode(self):
        df = generate_sales_data(num_rows=10, mode="edge_cases")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 10

    def test_schema_invalid_mode(self):
        df = generate_sales_data(num_rows=20, mode="schema_invalid")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 20


class TestGenerateSchemaInvalidData:
    """Tests for generate_schema_invalid_data."""

    def test_returns_dataframe(self):
        df = generate_schema_invalid_data(num_rows=10)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 10


class TestEdgeCases:
    """Edge case tests for data generation."""

    def test_zero_rows(self):
        df = generate_sales_data(num_rows=0, mode="clean")
        assert len(df) == 0

    def test_single_row(self):
        df = generate_sales_data(num_rows=1, mode="clean")
        assert len(df) == 1

    def test_large_dataset(self):
        df = generate_sales_data(num_rows=10000, mode="clean")
        assert len(df) == 10000

    def test_clean_data_positive_quantity(self):
        df = generate_sales_data(num_rows=100, mode="clean")
        assert (df["quantity"] > 0).all()

    def test_clean_data_positive_price(self):
        df = generate_sales_data(num_rows=100, mode="clean")
        assert (df["unit_price"] >= 0).all()

    def test_column_order(self):
        df = generate_sales_data(num_rows=5, mode="clean")
        expected_first_cols = ["transaction_id", "sale_date", "sale_hour", "customer_id"]
        assert list(df.columns[:4]) == expected_first_cols
