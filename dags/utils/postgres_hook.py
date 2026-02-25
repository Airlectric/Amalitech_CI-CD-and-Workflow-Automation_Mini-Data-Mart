import os
from typing import List, Optional, Any

from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresLayerHook(PostgresHook):
    def __init__(
        self,
        postgres_conn_id: str = "postgres_default",
        *args,
        **kwargs
    ):
        super().__init__(postgres_conn_id=postgres_conn_id, *args, **kwargs)

    def insert_dataframe(
        self,
        df,
        table: str,
        schema: str = "silver",
        if_exists: str = "append"
    ):
        full_table = f"{schema}.{table}"
        df.to_sql(
            table,
            self.get_sqlalchemy_engine(),
            schema=schema,
            if_exists=if_exists,
            index=False,
            method="multi",
            chunksize=1000
        )
        return len(df)

    def execute_query(
        self,
        query: str,
        parameters: Optional[tuple] = None
    ) -> List[tuple]:
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(query, parameters)
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()
                return columns, results
            conn.commit()
            return [], []
        finally:
            cursor.close()
            conn.close()

    def get_table_row_count(self, table: str, schema: str = "silver") -> int:
        query = f"SELECT COUNT(*) FROM {schema}.{table}"
        result = self.execute_query(query)
        if result[1]:
            return result[1][0][0]
        return 0

    def table_exists(self, table: str, schema: str = "silver") -> bool:
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = %s
                AND table_name = %s
            )
        """
        result = self.execute_query(query, (schema, table))
        if result[1]:
            return result[1][0][0]
        return False

    def get_max_value(self, column: str, table: str, schema: str = "silver") -> Any:
        query = f"SELECT MAX({column}) FROM {schema}.{table}"
        result = self.execute_query(query)
        if result[1] and result[1][0][0]:
            return result[1][0][0]
        return None

    def bulk_insert(
        self,
        table: str,
        rows: List[tuple],
        schema: str = "silver"
    ):
        if not rows:
            return 0

        full_table = f"{schema}.{table}"
        placeholders = ",".join(["%s"] * len(rows[0]))
        query = f"INSERT INTO {full_table} VALUES ({placeholders})"

        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            cursor.executemany(query, rows)
            conn.commit()
            return cursor.rowcount
        finally:
            cursor.close()
            conn.close()

    def update_metadata(
        self,
        file_path: str,
        dataset_name: str,
        status: str,
        record_count: int = 0,
        checksum: str = None,
        error_message: Optional[str] = None
    ):
        query = """
            INSERT INTO metadata.ingestion_metadata
            (file_path, dataset_name, ingest_date, status, record_count, checksum, error_message, created_at)
            VALUES (%s, %s, CURRENT_DATE, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (file_path) DO UPDATE SET
                status = EXCLUDED.status,
                record_count = EXCLUDED.record_count,
                processed_at = CURRENT_TIMESTAMP,
                error_message = EXCLUDED.error_message
        """
        self.execute_query(query, (file_path, dataset_name, status, record_count, checksum, error_message))

    def get_processed_files(self, dataset: str) -> List[str]:
        query = """
            SELECT file_path FROM metadata.ingestion_metadata
            WHERE dataset_name = %s AND status = 'PROCESSED'
        """
        result = self.execute_query(query, (dataset,))
        if result[1]:
            return [row[0] for row in result[1]]
        return []

    def truncate_table(self, table: str, schema: str = "silver"):
        query = f"TRUNCATE TABLE {schema}.{table} RESTART IDENTITY CASCADE"
        self.execute_query(query)
