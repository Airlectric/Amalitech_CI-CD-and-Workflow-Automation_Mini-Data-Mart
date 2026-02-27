from datetime import datetime, timedelta
from typing import Dict, List, Any

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.email import send_email_smtp

import sys
sys.path.insert(0, "/opt/airflow/dags")

from utils.postgres_hook import PostgresLayerHook
from config.quality_config import (
    SILVER_SCHEMA,
    QUALITY_RULES,
    TABLES_TO_CHECK,
    ALERT_DEFAULTS
)
from sql.quality_queries import (
    build_null_query,
    build_unique_query,
    build_value_range_query,
    get_source_files_query,
    build_bad_records_query
)
from templates.email.quality_alerts import format_failure_email


ALERT_EMAIL = Variable.get("alert_email", default_var="daniel.doe@a2sv.org")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": ALERT_DEFAULTS["retries"],
    "retry_delay": timedelta(minutes=ALERT_DEFAULTS["retry_delay_minutes"]),
    "email": [ALERT_EMAIL],
}


with DAG(
    dag_id="data_quality_checks",
    start_date=datetime(2026, 1, 1),
    schedule=ALERT_DEFAULTS["schedule"],
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["quality", "validation"],
    description="Data quality checks using configurable rules"
) as dag:

    @task
    def check_table_exists():
        pg_hook = PostgresLayerHook()
        results = {}

        for table in TABLES_TO_CHECK:
            exists = pg_hook.table_exists(table, schema=SILVER_SCHEMA)
            results[table] = exists

        return results

    @task
    def get_source_files():
        pg_hook = PostgresLayerHook()
        query = get_source_files_query()
        result = pg_hook.execute_query(query)
        
        files = []
        if result[1]:
            for row in result[1]:
                files.append({
                    "file_path": row[0],
                    "ingest_date": str(row[1])
                })
        
        return files

    @task
    def run_null_checks(table: str):
        pg_hook = PostgresLayerHook()
        rules = QUALITY_RULES.get(table, {})
        not_null_cols = rules.get("not_null", [])

        results = {}
        for col in not_null_cols:
            query = build_null_query(SILVER_SCHEMA, table, col)
            result = pg_hook.execute_query(query)
            if result[1]:
                null_count = result[1][0][0]
                results[col] = {
                    "check": "not_null",
                    "passed": null_count == 0,
                    "null_count": null_count
                }

        return {"table": table, "results": results}

    @task
    def run_value_range_checks(table: str):
        pg_hook = PostgresLayerHook()
        rules = QUALITY_RULES.get(table, {})
        value_ranges = rules.get("value_ranges", {})

        results = {}
        for col, range_config in value_ranges.items():
            min_val = range_config.get("min", 0)
            max_val = range_config.get("max", 999999)
            
            query = build_value_range_query(SILVER_SCHEMA, table, col, min_val, max_val)
            result = pg_hook.execute_query(query)
            
            if result[1]:
                invalid_count = result[1][0][0]
                results[col] = {
                    "check": "value_range",
                    "passed": invalid_count == 0,
                    "invalid_count": invalid_count,
                    "issues": [] if invalid_count == 0 else [f"Values outside range {min_val}-{max_val}"]
                }

        return {"table": table, "results": results}

    @task
    def run_uniqueness_checks(table: str):
        pg_hook = PostgresLayerHook()
        rules = QUALITY_RULES.get(table, {})
        unique_cols = rules.get("unique", [])

        results = {}
        for col in unique_cols:
            query = build_unique_query(SILVER_SCHEMA, table, col)
            result = pg_hook.execute_query(query)
            duplicate_count = len(result[1]) if result[1] else 0

            results[col] = {
                "check": "uniqueness",
                "passed": duplicate_count == 0,
                "duplicate_count": duplicate_count
            }

        return {"table": table, "results": results}

    @task
    def generate_quality_report(check_results: List[dict]) -> Dict[str, Any]:
        total_checks = 0
        passed_checks = 0
        failed_checks = 0
        failed_details = []

        for result in check_results:
            if "results" in result:
                for col, check_result in result["results"].items():
                    total_checks += 1
                    if check_result.get("passed", False):
                        passed_checks += 1
                    else:
                        failed_checks += 1
                        failed_details.append({
                            "table": result.get("table", "unknown"),
                            "column": col,
                            "check": check_result.get("check", "unknown"),
                            "details": check_result
                        })

        report = {
            "total_checks": total_checks,
            "passed": passed_checks,
            "failed": failed_checks,
            "pass_rate": passed_checks / total_checks if total_checks > 0 else 0,
            "details": check_results,
            "failed_details": failed_details
        }

        if failed_checks > 0:
            raise ValueError(f"Data quality checks failed: {failed_checks} out of {total_checks} checks failed")

        return report

    @task
    def get_failed_source_files(check_results: List[dict], source_files: List[dict]) -> Dict[str, Any]:
        pg_hook = PostgresLayerHook()
        
        failed_sources = {}
        
        for result in check_results:
            if "results" not in result:
                continue
                
            table = result.get("table", "unknown")
            
            for col, check_result in result["results"].items():
                if not check_result.get("passed", True):
                    try:
                        query = build_bad_records_query(SILVER_SCHEMA, table, col, check_result.get("check"))
                        result = pg_hook.execute_query(query)
                        
                        bad_sources = []
                        if result[1]:
                            for row in result[1]:
                                bad_sources.append({
                                    "ingest_date": str(row[0]),
                                    "bad_record_count": row[1]
                                })
                        
                        key = f"{table}.{col}"
                        failed_sources[key] = bad_sources
                    except:
                        pass
        
        return {
            "source_files": source_files,
            "failed_sources": failed_sources
        }

    @task
    def send_failure_email_task(**context):
        from datetime import datetime
        
        ti = context['ti']
        report = ti.xcom_pull(task_ids='generate_quality_report')
        source_info = ti.xcom_pull(task_ids='get_failed_source_files')
        
        if not report:
            report = {"failed_details": [], "total_checks": 0, "passed": 0, "failed": 0}
        
        if not source_info:
            source_info = {"source_files": [], "failed_sources": {}}
        
        html_content = format_failure_email(
            total_checks=report.get('total_checks', 0),
            passed=report.get('passed', 0),
            failed=report.get('failed', 0),
            pass_rate=report.get('pass_rate', 0) * 100,
            failed_details=report.get('failed_details', []),
            failed_sources=source_info.get('failed_sources', {}),
            source_files=source_info.get('source_files', []),
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            run_id=context.get('run_id', 'N/A')
        )
        
        subject = f"🚨 Data Quality Failed - {report.get('failed', 0)} checks failed"
        
        send_email_smtp(to=ALERT_EMAIL, subject=subject, html_content=html_content)
        
        return "Email sent"

    table_exists = check_table_exists()
    source_files = get_source_files()

    null_checks = [run_null_checks(table) for table in TABLES_TO_CHECK]
    range_checks = [run_value_range_checks(table) for table in TABLES_TO_CHECK]
    unique_checks = [run_uniqueness_checks(table) for table in TABLES_TO_CHECK]

    all_checks = null_checks + range_checks + unique_checks

    report = generate_quality_report(all_checks)
    
    source_info = get_failed_source_files(all_checks, source_files)

    email_task = send_failure_email_task()

    table_exists >> all_checks >> report
    source_files >> source_info >> email_task
    report >> email_task
