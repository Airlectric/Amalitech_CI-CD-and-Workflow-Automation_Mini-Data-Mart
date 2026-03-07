import json
from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresLayerHook(PostgresHook):
    def __init__(self, postgres_conn_id: str = "postgres_default", **kwargs):
        super().__init__(postgres_conn_id=postgres_conn_id, **kwargs)

    def insert_dataframe(self, df, table: str, schema: str = "silver", if_exists: str = "append"):
        df.to_sql(
            table,
            self.get_sqlalchemy_engine(),
            schema=schema,
            if_exists=if_exists,
            index=False,
            method="multi",
            chunksize=1000,
        )
        return len(df)

    def execute_query(self, query: str, parameters: tuple | None = None) -> tuple[list, list]:
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

    def bulk_insert(self, table: str, rows: list[tuple], schema: str = "silver"):
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
        checksum: str | None = None,
        error_message: str | None = None,
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

    def get_processed_files(self, dataset: str) -> list[str]:
        """Return files in any terminal state so they are not re-read."""
        query = """
            SELECT file_path FROM metadata.ingestion_metadata
            WHERE dataset_name = %s AND status IN ('PROCESSED', 'SCHEMA_DRIFT', 'FAILED')
        """
        result = self.execute_query(query, (dataset,))
        if result[1]:
            return [row[0] for row in result[1]]
        return []

    def truncate_table(self, table: str, schema: str = "silver"):
        query = f"TRUNCATE TABLE {schema}.{table} RESTART IDENTITY CASCADE"
        self.execute_query(query)

    def upsert_dataframe(
        self,
        df,
        table: str,
        schema: str = "silver",
        conflict_columns: list[str] | None = None,
        update_columns: list[str] | None = None,
    ):
        if df.empty:
            return 0

        full_table = f"{schema}.{table}"

        if conflict_columns is None:
            conflict_columns = ["transaction_id"]

        columns = list(df.columns)

        if update_columns is None:
            update_columns = [c for c in columns if c not in conflict_columns]

        col_names = ",".join(columns)
        placeholders = ",".join(["%s"] * len(columns))

        insert_sql = f"INSERT INTO {full_table} ({col_names}) VALUES ({placeholders})"

        if update_columns:
            update_set = ",".join([f"{c} = EXCLUDED.{c}" for c in update_columns])
            upsert_sql = insert_sql + f" ON CONFLICT ({','.join(conflict_columns)}) DO UPDATE SET {update_set}"
        else:
            upsert_sql = insert_sql + f" ON CONFLICT ({','.join(conflict_columns)}) DO NOTHING"

        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            records = [tuple(row[col] for col in columns) for row in df.to_dict("records")]
            cursor.executemany(upsert_sql, records)
            conn.commit()
            return len(df)
        finally:
            cursor.close()
            conn.close()

    def insert_quarantine(
        self, records: list[dict], table: str, schema: str = "quarantine", run_id: str | None = None
    ) -> int:
        """Insert bad records into quarantine table"""
        if not records:
            return 0

        full_table = f"{schema}.{table}"

        conn = self.get_conn()
        cursor = conn.cursor()

        inserted = 0
        try:
            for record in records:
                payload = record.get("payload", {})
                payload_clean = {}
                for k, v in payload.items():
                    if hasattr(v, "isoformat"):
                        payload_clean[k] = v.isoformat()
                    elif v is None:
                        payload_clean[k] = None
                    elif isinstance(v, float) and (str(v) == "nan" or str(v) == "NaN" or str(v) == "inf"):
                        payload_clean[k] = None
                    else:
                        payload_clean[k] = v

                payload_json = json.dumps(payload_clean, allow_nan=False)
                cursor.execute(
                    f"""
                    INSERT INTO {full_table}
                    (id, payload, error_reason, source_file, ingestion_run_id, failed_at)
                    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (ingestion_run_id, id) DO NOTHING
                """,
                    (
                        record.get("id", 0),
                        payload_json,
                        record.get("error_reason", "unknown"),
                        record.get("source_file", ""),
                        run_id,
                    ),
                )
                inserted += cursor.rowcount

            conn.commit()
            return inserted
        finally:
            cursor.close()
            conn.close()

    def update_audit_run(
        self,
        run_id: str,
        rows_read: int = 0,
        rows_written_silver: int = 0,
        rows_quarantined: int = 0,
        status: str = "COMPLETED",
        files_scanned: list[str] | None = None,
    ):
        """Update audit.ingestion_runs with final counts"""
        query = """
            UPDATE audit.ingestion_runs
            SET finished_at = CURRENT_TIMESTAMP,
                rows_read = %s,
                rows_written_silver = %s,
                rows_quarantined = %s,
                status = %s,
                files_scanned = %s
            WHERE ingestion_run_id = %s
        """
        self.execute_query(query, (rows_read, rows_written_silver, rows_quarantined, status, files_scanned, run_id))
