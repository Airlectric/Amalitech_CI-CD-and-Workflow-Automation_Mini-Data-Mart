import duckdb
import pandas as pd
from typing import List, Optional, Dict, Any
import io


class DuckDBValidator:
    def __init__(self, *args, **kwargs):
        self.conn = duckdb.connect(*args, **kwargs)
        self.last_query_result = None

    def close(self):
        if self.conn:
            self.conn.close()

    def read_parquet_files(self, s3_paths: List[str]) -> pd.DataFrame:
        if not s3_paths:
            return pd.DataFrame()

        s3_path_str = ", ".join([f"'{p}'" for p in s3_paths])
        query = f"""
            SELECT * FROM read_parquet([{s3_path_str}])
        """
        return self.conn.execute(query).df()

    def read_parquet_from_s3(
        self,
        bucket: str,
        key: str,
        endpoint_url: str = "http://minio:9000",
        access_key: str = "minio",
        secret_key: str = "minio123"
    ) -> pd.DataFrame:
        query = f"""
            SELECT * FROM read_parquet(
                's3://{bucket}/{key}',
                endpoint='{endpoint_url}',
                access_key_id='{access_key}',
                secret_access_key='{secret_key}'
            )
        """
        return self.conn.execute(query).df()

    def validate_schema(
        self,
        df: pd.DataFrame,
        expected_columns: List[str],
        required_columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        result = {
            "valid": True,
            "errors": [],
            "warnings": []
        }

        actual_columns = set(df.columns)
        expected_columns_set = set(expected_columns)

        missing_columns = expected_columns_set - actual_columns
        if missing_columns:
            result["valid"] = False
            result["errors"].append(f"Missing columns: {missing_columns}")

        extra_columns = actual_columns - expected_columns_set
        if extra_columns:
            result["warnings"].append(f"Extra columns (ignored): {extra_columns}")

        if required_columns:
            null_counts = df[required_columns].isnull().sum()
            columns_with_nulls = null_counts[null_counts > 0]
            if not columns_with_nulls.empty:
                result["valid"] = False
                result["errors"].append(
                    f"Null values in required columns: {columns_with_nulls.to_dict()}"
                )

        return result

    def validate_data_types(
        self,
        df: pd.DataFrame,
        type_spec: Dict[str, str]
    ) -> Dict[str, Any]:
        result = {
            "valid": True,
            "errors": []
        }

        for col, expected_type in type_spec.items():
            if col not in df.columns:
                result["valid"] = False
                result["errors"].append(f"Column '{col}' not found")
                continue

            actual_dtype = str(df[col].dtype)
            if expected_type not in actual_dtype:
                result["valid"] = False
                result["errors"].append(
                    f"Column '{col}': expected {expected_type}, got {actual_dtype}"
                )

        return result

    def validate_value_range(
        self,
        df: pd.DataFrame,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None
    ) -> Dict[str, Any]:
        result = {
            "valid": True,
            "errors": []
        }

        if column not in df.columns:
            result["valid"] = False
            result["errors"].append(f"Column '{column}' not found")
            return result

        if min_value is not None:
            below_min = df[df[column] < min_value]
            if not below_min.empty:
                result["valid"] = False
                result["errors"].append(
                    f"{len(below_min)} rows below minimum value {min_value}"
                )

        if max_value is not None:
            above_max = df[df[column] > max_value]
            if not above_max.empty:
                result["valid"] = False
                result["errors"].append(
                    f"{len(above_max)} rows above maximum value {max_value}"
                )

        return result

    def validate_uniqueness(
        self,
        df: pd.DataFrame,
        columns: List[str]
    ) -> Dict[str, Any]:
        result = {
            "valid": True,
            "errors": []
        }

        duplicates = df[df.duplicated(subset=columns, keep=False)]
        if not duplicates.empty:
            result["valid"] = False
            result["errors"].append(
                f"Found {len(duplicates)} duplicate rows in columns: {columns}"
            )

        return result

    def get_row_count(self, df: pd.DataFrame) -> int:
        return len(df)

    def get_column_stats(self, df: pd.DataFrame) -> Dict[str, Dict]:
        stats = {}
        for col in df.columns:
            stats[col] = {
                "dtype": str(df[col].dtype),
                "null_count": int(df[col].isnull().sum()),
                "unique_count": int(df[col].nunique()),
                "min": df[col].min() if df[col].dtype in ['int64', 'float64'] else None,
                "max": df[col].max() if df[col].dtype in ['int64', 'float64'] else None,
            }
        return stats

    def run_full_validation(
        self,
        df: pd.DataFrame,
        schema_spec: Dict[str, Any]
    ) -> Dict[str, Any]:
        validation_results = {
            "row_count": self.get_row_count(df),
            "schema_valid": True,
            "data_valid": True,
            "errors": [],
            "warnings": []
        }

        if "expected_columns" in schema_spec:
            col_result = self.validate_schema(
                df,
                schema_spec["expected_columns"],
                schema_spec.get("required_columns")
            )
            if not col_result["valid"]:
                validation_results["schema_valid"] = False
                validation_results["errors"].extend(col_result["errors"])
            validation_results["warnings"].extend(col_result.get("warnings", []))

        if "column_types" in schema_spec:
            type_result = self.validate_data_types(df, schema_spec["column_types"])
            if not type_result["valid"]:
                validation_results["schema_valid"] = False
                validation_results["errors"].extend(type_result["errors"])

        if "unique_columns" in schema_spec:
            unique_result = self.validate_uniqueness(df, schema_spec["unique_columns"])
            if not unique_result["valid"]:
                validation_results["data_valid"] = False
                validation_results["errors"].extend(unique_result["errors"])

        validation_results["column_stats"] = self.get_column_stats(df)

        return validation_results


def create_validator() -> DuckDBValidator:
    return DuckDBValidator()
