import duckdb
import pandas as pd
from typing import List, Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)

MINIO_CONFIG = {
    "endpoint": "minio:9000",
    "access_key": "minio",
    "secret_key": "minio123",
    "use_ssl": False
}


class DuckDBValidator:
    def __init__(self, minio_config: Dict[str, Any] = None):
        self.minio_config = minio_config or MINIO_CONFIG
        self.conn = None
    
    def _get_connection(self):
        """Get DuckDB connection with httpfs configured"""
        if self.conn is None:
            self.conn = duckdb.connect()
            self._setup_httpfs()
        return self.conn
    
    def _setup_httpfs(self):
        """Setup httpfs extension for S3/MinIO access"""
        try:
            self.conn.execute("INSTALL httpfs;")
            self.conn.execute("LOAD httpfs;")
            
            self.conn.execute(f"""
                SET s3_endpoint='{self.minio_config['endpoint']}';
                SET s3_access_key_id='{self.minio_config['access_key']}';
                SET s3_secret_access_key='{self.minio_config['secret_key']}';
                SET s3_use_ssl={str(self.minio_config['use_ssl']).lower()};
                SET s3_url_style='path';
            """)
            logger.info("DuckDB httpfs configured for MinIO")
        except Exception as e:
            logger.warning(f"Failed to setup httpfs: {e}")
    
    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def read_parquet_from_minio(self, bucket: str, key_pattern: str) -> pd.DataFrame:
        """
        Read Parquet files DIRECTLY from MinIO using DuckDB schema-on-read.
        This queries MinIO without downloading the full file first.
        """
        conn = self._get_connection()
        query = f"""
            SELECT * FROM read_parquet('s3://{bucket}/{key_pattern}')
        """
        try:
            result = conn.execute(query)
            df = result.df()
            logger.info(f"Read {len(df)} rows from s3://{bucket}/{key_pattern}")
            return df
        except Exception as e:
            logger.error(f"Failed to read from MinIO: {e}")
            return pd.DataFrame()
    
    def query_minio_parquet(self, bucket: str, key_pattern: str, 
                           columns: List[str] = None,
                           where_clause: str = None,
                           group_by: List[str] = None) -> pd.DataFrame:
        """
        Execute SQL query directly on Parquet files in MinIO.
        This is TRUE schema-on-read - no download needed.
        """
        conn = self._get_connection()
        
        col_str = ", ".join(columns) if columns else "*"
        query = f"SELECT {col_str} FROM read_parquet('s3://{bucket}/{key_pattern}')"
        
        if where_clause:
            query += f" WHERE {where_clause}"
        
        if group_by:
            query += f" GROUP BY {', '.join(group_by)}"
        
        try:
            result = conn.execute(query)
            return result.df()
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return pd.DataFrame()

    def list_parquet_files(self, bucket: str, prefix: str = "") -> List[str]:
        """List all Parquet files in a MinIO bucket/prefix using MinIO hook"""
        try:
            from utils.minio_hook import MinIOHook
            minio = MinIOHook(bucket_name=bucket)
            files = minio.list_files(prefix=prefix, suffix=".parquet")
            return files
        except Exception as e:
            logger.warning(f"Failed to list parquet files: {e}")
            return []
    
    def read_parquet_from_minio(
        self,
        bucket: str,
        key_pattern: str,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Read Parquet files directly from MinIO using DuckDB.
        
        Args:
            bucket: MinIO bucket name (e.g., 'bronze')
            key_pattern: S3 key pattern (e.g., 'sales/*.parquet' or 'sales/ingest_date=2026-02-26/*.parquet')
            columns: Optional list of columns to select
            
        Returns:
            pandas DataFrame
        """
        conn = self._get_connection()
        
        col_str = ", ".join(columns) if columns else "*"
        query = f"""
            SELECT {col_str} FROM read_parquet('s3://{bucket}/{key_pattern}')
        """
        
        try:
            result = conn.execute(query)
            df = result.df()
            logger.info(f"Read {len(df)} rows from s3://{bucket}/{key_pattern}")
            return df
        except Exception as e:
            logger.error(f"Failed to read from MinIO: {e}")
            return pd.DataFrame()
    
    def validate_schema(
        self,
        df: pd.DataFrame,
        expected_columns: List[str],
        required_columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Validate DataFrame schema against expected columns.
        
        Args:
            df: DataFrame to validate
            expected_columns: List of expected column names
            required_columns: Optional list of required (non-null) columns
            
        Returns:
            Dict with 'valid', 'errors', 'warnings'
        """
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
            result["errors"].append(f"Missing columns: {sorted(missing_columns)}")
        
        extra_columns = actual_columns - expected_columns_set
        if extra_columns:
            result["warnings"].append(f"Extra columns (will be ignored): {sorted(extra_columns)}")
        
        if required_columns:
            null_counts = df[required_columns].isnull().sum()
            columns_with_nulls = null_counts[null_counts > 0]
            if not columns_with_nulls.empty:
                result["valid"] = False
                result["errors"].append(
                    f"Null values in required columns: {dict(columns_with_nulls)}"
                )
        
        return result
    
    def validate_data_types(
        self,
        df: pd.DataFrame,
        type_spec: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Validate column data types.
        
        Args:
            df: DataFrame to validate
            type_spec: Dict mapping column names to expected types
                       (e.g., {'quantity': 'integer', 'unit_price': 'float'})
            
        Returns:
            Dict with 'valid' and 'errors'
        """
        result = {
            "valid": True,
            "errors": []
        }
        
        type_mapping = {
            'integer': 'int',
            'float': 'float',
            'string': 'object',
            'boolean': 'bool',
            'datetime': 'datetime',
            'date': 'datetime'
        }
        
        for col, expected_type in type_spec.items():
            if col not in df.columns:
                result["valid"] = False
                result["errors"].append(f"Column '{col}' not found")
                continue
            
            actual_dtype = str(df[col].dtype).lower()
            expected_normalized = type_mapping.get(expected_type, expected_type).lower()
            
            if expected_normalized not in actual_dtype:
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
        """Validate numeric column is within range."""
        result = {"valid": True, "errors": []}
        
        if column not in df.columns:
            result["valid"] = False
            result["errors"].append(f"Column '{column}' not found")
            return result
        
        if min_value is not None:
            below_min = df[df[column] < min_value]
            if not below_min.empty:
                result["valid"] = False
                result["errors"].append(f"{len(below_min)} rows below minimum {min_value}")
        
        if max_value is not None:
            above_max = df[df[column] > max_value]
            if not above_max.empty:
                result["valid"] = False
                result["errors"].append(f"{len(above_max)} rows above maximum {max_value}")
        
        return result
    
    def validate_uniqueness(
        self,
        df: pd.DataFrame,
        columns: List[str]
    ) -> Dict[str, Any]:
        """Check for duplicate values in specified columns."""
        result = {"valid": True, "errors": []}
        
        duplicates = df[df.duplicated(subset=columns, keep=False)]
        if not duplicates.empty:
            result["valid"] = False
            result["errors"].append(
                f"Found {len(duplicates)} duplicate rows in columns: {columns}"
            )
        
        return result
    
    def run_full_validation(
        self,
        df: pd.DataFrame,
        schema_spec: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Run complete validation against a schema specification.
        
        Args:
            df: DataFrame to validate
            schema_spec: Dict with:
                - expected_columns: List of expected columns
                - required_columns: List of required (non-null) columns
                - column_types: Dict of column -> expected type
                - unique_columns: List of columns that should be unique
                - value_ranges: Dict of column -> {min, max}
                
        Returns:
            Dict with validation results
        """
        validation_results = {
            "row_count": len(df),
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
        
        if "value_ranges" in schema_spec:
            for column, range_spec in schema_spec["value_ranges"].items():
                range_result = self.validate_value_range(
                    df, column,
                    range_spec.get("min"),
                    range_spec.get("max")
                )
                if not range_result["valid"]:
                    validation_results["data_valid"] = False
                    validation_results["errors"].extend(range_result["errors"])
        
        return validation_results
    
    def get_column_stats(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """Get statistics for each column."""
        stats = {}
        for col in df.columns:
            col_stats = {
                "dtype": str(df[col].dtype),
                "null_count": int(df[col].isnull().sum()),
                "unique_count": int(df[col].nunique())
            }
            
            if df[col].dtype in ['int64', 'float64', 'int32', 'float32']:
                col_stats["min"] = df[col].min()
                col_stats["max"] = df[col].max()
                col_stats["mean"] = float(df[col].mean())
            
            stats[col] = col_stats
        
        return stats
    
    def query_minio_parquet(
        self,
        bucket: str,
        key_pattern: str,
        where_clause: Optional[str] = None,
        group_by: Optional[List[str]] = None,
        select: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Execute a query directly on Parquet files in MinIO.
        
        Args:
            bucket: MinIO bucket
            key_pattern: S3 key pattern (e.g., 'sales/*.parquet')
            where_clause: WHERE clause (e.g., "sale_date > '2024-01-01'")
            group_by: Columns to group by
            select: Columns to select
            
        Returns:
            DataFrame with query results
        """
        conn = self._get_connection()
        
        col_str = ", ".join(select) if select else "*"
        
        query = f"SELECT {col_str} FROM read_parquet('s3://{bucket}/{key_pattern}')"
        
        if where_clause:
            query += f" WHERE {where_clause}"
        
        if group_by:
            query += f" GROUP BY {', '.join(group_by)}"
        
        try:
            result = conn.execute(query)
            return result.df()
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return pd.DataFrame()


def create_validator(minio_config: Dict[str, Any] = None) -> DuckDBValidator:
    """Factory function to create a DuckDB validator."""
    return DuckDBValidator(minio_config)


def validate_parquet_from_minio(
    bucket: str,
    key_pattern: str,
    schema_spec: Dict[str, Any],
    minio_config: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Convenience function to validate Parquet files directly from MinIO.
    
    Args:
        bucket: MinIO bucket name
        key_pattern: S3 key pattern
        schema_spec: Schema specification for validation
        minio_config: Optional MinIO configuration
        
    Returns:
        Validation results dict with DataFrame if valid
    """
    validator = create_validator(minio_config)
    
    try:
        df = validator.read_parquet_from_minio(bucket, key_pattern)
        
        if df.empty:
            return {
                "valid": False,
                "errors": ["No data found in MinIO path"],
                "dataframe": df
            }
        
        validation_result = validator.run_full_validation(df, schema_spec)
        validation_result["dataframe"] = df
        
        return validation_result
    
    finally:
        validator.close()
