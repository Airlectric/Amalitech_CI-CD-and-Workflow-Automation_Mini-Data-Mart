"""
SQL Queries for Data Quality Checks
"""

QUALITY_CHECKS = {
    "null_check": """
        SELECT COUNT(*) FROM {schema}.{table}
        WHERE {column} IS NULL
    """,
    
    "unique_check": """
        SELECT {column}, COUNT(*) as cnt
        FROM {schema}.{table}
        GROUP BY {column}
        HAVING COUNT(*) > 1
        LIMIT 10
    """,
    
    "value_range_check": """
        SELECT COUNT(*) FROM {schema}.{table}
        WHERE {column} < {min_val} OR {column} > {max_val}
    """,
    
    "source_files_query": """
        SELECT DISTINCT file_path, ingest_date 
        FROM metadata.ingestion_metadata 
        WHERE status = 'SUCCESS' 
        ORDER BY processed_at DESC 
        LIMIT 10
    """,
    
    "bad_record_sources": """
        SELECT ingest_date, COUNT(*) as cnt
        FROM {schema}.{table}
        WHERE {condition}
        GROUP BY ingest_date
        ORDER BY cnt DESC
        LIMIT 3
    """
}


def build_null_query(schema: str, table: str, column: str) -> str:
    """Build NULL check query"""
    return QUALITY_CHECKS["null_check"].format(
        schema=schema,
        table=table,
        column=column
    )


def build_unique_query(schema: str, table: str, column: str) -> str:
    """Build uniqueness check query"""
    return QUALITY_CHECKS["unique_check"].format(
        schema=schema,
        table=table,
        column=column
    )


def build_value_range_query(schema: str, table: str, column: str, min_val: float, max_val: float) -> str:
    """Build value range check query"""
    return QUALITY_CHECKS["value_range_check"].format(
        schema=schema,
        table=table,
        column=column,
        min_val=min_val,
        max_val=max_val
    )


def get_source_files_query() -> str:
    """Get source files query"""
    return QUALITY_CHECKS["source_files_query"]


def build_bad_records_query(schema: str, table: str, column: str, check_type: str) -> str:
    """Build query to find bad record sources"""
    if check_type == "not_null":
        condition = f"{column} IS NULL"
    elif check_type == "value_range":
        condition = f"{column} < 0"
    else:
        condition = "1=1"
    
    return QUALITY_CHECKS["bad_record_sources"].format(
        schema=schema,
        table=table,
        condition=condition
    )
