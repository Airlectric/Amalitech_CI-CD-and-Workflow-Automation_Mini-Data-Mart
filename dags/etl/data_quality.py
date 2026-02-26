from datetime import datetime, timedelta
from typing import Dict, List, Any

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.email import EmailOperator

import sys
sys.path.insert(0, "/opt/airflow/dags")

from utils.postgres_hook import PostgresLayerHook


SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

QUALITY_RULES = {
    "sales": {
        "not_null": ["transaction_id", "sale_date", "product_id", "quantity", "unit_price", "net_amount"],
        "positive_values": ["quantity", "unit_price", "net_amount", "gross_amount"],
        "value_ranges": {
            "quantity": {"min": 1, "max": 1000},
            "unit_price": {"min": 0.01, "max": 100000},
            "net_amount": {"min": 0.01, "max": 1000000},
            "gross_amount": {"min": 0.01, "max": 1000000},
            "sale_hour": {"min": 0, "max": 23}
        }
    },
    "customers": {
        "not_null": ["customer_id"],
        "unique": ["customer_id"]
    }
}


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email": ["daniel.doe@amalitech.com"],
}


with DAG(
    dag_id="data_quality_checks",
    start_date=datetime(2026, 1, 1),
    schedule="0 */3 * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["quality", "validation", "great_expectations"],
    description="Data quality checks using Great Expectations"
) as dag:

    @task
    def check_table_exists():
        pg_hook = PostgresLayerHook()
        tables = ["sales", "customers"]
        results = {}

        for table in tables:
            exists = pg_hook.table_exists(table, schema=SILVER_SCHEMA)
            results[table] = exists

        return results

    @task
    def run_null_checks(table: str):
        pg_hook = PostgresLayerHook()
        rules = QUALITY_RULES.get(table, {})
        not_null_cols = rules.get("not_null", [])

        results = {}
        for col in not_null_cols:
            query = f"""
                SELECT COUNT(*) FROM {SILVER_SCHEMA}.{table}
                WHERE {col} IS NULL
            """
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
        ranges = rules.get("value_ranges", {})

        results = {}
        for col, range_spec in ranges.items():
            issues = []

            if "min" in range_spec:
                query = f"""
                    SELECT COUNT(*) FROM {SILVER_SCHEMA}.{table}
                    WHERE {col} < {range_spec['min']}
                """
                result = pg_hook.execute_query(query)
                if result[1] and result[1][0][0] > 0:
                    issues.append(f"Below minimum: {result[1][0][0]} rows")

            if "max" in range_spec:
                query = f"""
                    SELECT COUNT(*) FROM {SILVER_SCHEMA}.{table}
                    WHERE {col} > {range_spec['max']}
                """
                result = pg_hook.execute_query(query)
                if result[1] and result[1][0][0] > 0:
                    issues.append(f"Above maximum: {result[1][0][0]} rows")

            results[col] = {
                "check": "value_range",
                "passed": len(issues) == 0,
                "issues": issues
            }

        return {"table": table, "results": results}

    @task
    def run_uniqueness_checks(table: str):
        pg_hook = PostgresLayerHook()
        rules = QUALITY_RULES.get(table, {})
        unique_cols = rules.get("unique", [])

        results = {}
        for col in unique_cols:
            query = f"""
                SELECT {col}, COUNT(*) as cnt
                FROM {SILVER_SCHEMA}.{table}
                GROUP BY {col}
                HAVING COUNT(*) > 1
                LIMIT 10
            """
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
            raise ValueError(f"Data quality checks failed: {failed_checks} out of {total_checks} checks failed. Failed details: {failed_details}")

        return report

    table_exists = check_table_exists()

    null_checks = []
    range_checks = []
    unique_checks = []

    for table in ["sales", "customers"]:
        null_checks.append(run_null_checks(table))
        range_checks.append(run_value_range_checks(table))
        unique_checks.append(run_uniqueness_checks(table))

    all_checks = null_checks + range_checks + unique_checks

    report = generate_quality_report(all_checks)

    send_failure_email = EmailOperator(
        task_id="send_failure_email",
        to="daniel.doe@amalitech.com",
        subject="Data Quality Check Failed - {{ ds }}",
        html_content="""
            <h3>Data Quality Check Failed</h3>
            <p><strong>Date:</strong> {{ ds }}</p>
            <p><strong>DAG:</strong> data_quality_checks</p>
            <p>The data quality checks have failed. Please review the quality report in Airflow.</p>
            <p>Check the DAG logs for detailed failure information.</p>
        """,
        trigger_rule="all_failed",
    )

    table_exists >> all_checks >> report
    report >> send_failure_email
