"""
Data Quality DAG using Great Expectations
Validates quarantine patterns, profiles Silver, detects drift, generates Data Docs, sends alerts
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import PythonOperator

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json
import logging

logger = logging.getLogger(__name__)

ALERT_EMAIL = Variable.get("alert_email", default_var="daniel.doe@a2sv.org")
GX_DIR = "/opt/airflow/scripts/gx"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
}


def get_postgres_connection():
    """Get PostgreSQL connection for GX"""
    return {
        "drivername": "postgresql+psycopg2",
        "username": "airflow",
        "password": "airflow",
        "host": "postgres",
        "port": 5432,
        "database": "airflow"
    }


with DAG(
    dag_id="data_quality_gx",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["quality", "great_expectations", "validation"],
    description="Data quality checks using Great Expectations"
) as dag:

    @task
    def validate_quarantine_patterns():
        """Validate quarantine table for bad records and patterns"""
        import great_expectations as gx
        from great_expectations.datasource.fluent import SqlSource
        from great_expectations.validator.validator import Validator

        logger.info("Starting quarantine pattern validation")

        context = gx.get_context(mode="ephemeral")
        conn_params = get_postgres_connection()

        datasource = context.sources.add_postgres(
            name="postgres_quarantine",
            connection_string="postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
        )

        batch_request = datasource.build_batch_request(
            schema_name="quarantine",
            table_name="sales_failed"
        )

        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name="quarantine_suite"
        )

        expectations = [
            {
                "expectation_type": "expect_table_column_count_to_be_between",
                "kwargs": {"min_value": 0, "max_value": 100000}
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "id"}
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "payload"}
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "error_reason"}
            },
            {
                "expectation_type": "expect_column_distinct_values_to_be_in_set",
                "kwargs": {"column": "replayed", "value_set": [True, False]}
            },
        ]

        for exp in expectations:
            getattr(validator, exp["expectation_type"])(**exp["kwargs"])

        results = validator.validate()

        context.save_expectation_suite(
            expectation_suite_name="quarantine_suite",
            overwrite_existing=True
        )

        quarantine_stats = {
            "success": results.success,
            "statistics": results.statistics,
            "expectations": [
                {"expectation_type": r.expectation_type, "success": r.success, "kwargs": r.kwargs}
                for r in results.results
            ]
        }

        logger.info(f"Quarantine validation complete: {quarantine_stats}")
        return quarantine_stats

    @task
    def profile_silver():
        """Profile Silver tables and generate expectations automatically"""
        import great_expectations as gx

        logger.info("Starting Silver table profiling")

        context = gx.get_context(mode="ephemeral")

        datasource = context.sources.add_postgres(
            name="postgres_silver",
            connection_string="postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
        )

        tables = ["sales", "products", "stores", "customers"]
        profiling_results = {}

        for table in tables:
            try:
                batch_request = datasource.build_batch_request(
                    schema_name="silver",
                    table_name=table
                )

                batch = context.get_batch_list(batch_request)[0]

                profiler = gx.profile.SimpleProfiler()
                suite = profiler.profile(
                    data_asset=batch,
                    expectation_suite_name=f"{table}_profiled"
                )

                context.save_expectation_suite(
                    expectation_suite_name=f"{table}_profiled",
                    overwrite_existing=True
                )

                profiling_results[table] = {
                    "success": True,
                    "expectation_count": len(suite.expectations),
                    "suite_name": f"{table}_profiled"
                }
                logger.info(f"Profiled {table}: {len(suite.expectations)} expectations")

            except Exception as e:
                profiling_results[table] = {
                    "success": False,
                    "error": str(e)
                }
                logger.error(f"Failed to profile {table}: {e}")

        return profiling_results

    @task
    def detect_drift():
        """Detect schema and data drift between runs"""
        import great_expectations as gx

        logger.info("Starting drift detection")

        context = gx.get_context(mode="ephemeral")

        datasource = context.sources.add_postgres(
            name="postgres_silver",
            connection_string="postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
        )

        tables = ["sales", "products", "stores", "customers"]
        drift_results = {}

        for table in tables:
            try:
                batch_request = datasource.build_batch_request(
                    schema_name="silver",
                    table_name=table
                )

                checkpoint = gx.checkpoint.SimpleCheckpoint(
                    name=f"{table}_drift_check",
                    data_context=context,
                    expectation_suite_name=f"{table}_profiled"
                )

                result = checkpoint.run(batch_request=batch_request)

                unexpected_values = []
                for expectation_result in result.results:
                    if not expectation_result.success:
                        if hasattr(expectation_result, 'result'):
                            unexpected_values.append({
                                "expectation": expectation_result.expectation_type,
                                "details": expectation_result.result
                            })

                drift_detected = len(unexpected_values) > 0

                drift_results[table] = {
                    "drift_detected": drift_detected,
                    "unexpected_count": len(unexpected_values),
                    "unexpected_details": unexpected_values[:5],
                    "success": result.success
                }

                logger.info(f"Drift check for {table}: drift_detected={drift_detected}")

            except Exception as e:
                drift_results[table] = {
                    "drift_detected": False,
                    "error": str(e)
                }
                logger.error(f"Drift check failed for {table}: {e}")

        return drift_results

    @task
    def generate_data_docs():
        """Generate HTML data docs from validation results"""
        import great_expectations as gx
        from great_expectations.data_context import FileDataContext

        logger.info("Generating Data Docs")

        context = FileDataContext(project_root_dir=GX_DIR)

        context.build_data_docs()

        data_docs_site = context.get_docs_sites_urls()
        docs_url = data_docs_site[0]["site_url"] if data_docs_site else "N/A"

        logger.info(f"Data Docs generated: {docs_url}")
        return {"docs_url": docs_url, "sites": data_docs_site}

    @task
    def send_alerts(quarantine_results, profiling_results, drift_results, docs_info):
        """Send email alerts with all validation results"""
        logger.info("Sending quality alert emails")

        has_issues = (
            not quarantine_results.get("success", True) or
            any(r.get("drift_detected", False) for r in drift_results.values()) or
            any(not r.get("success", False) for r in profiling_results.values())
        )

        quarantine_count_query = """
            SELECT COUNT(*) as total,
                   COUNT(CASE WHEN replayed = FALSE THEN 1 END) as pending,
                   COUNT(CASE WHEN replayed = TRUE THEN 1 END) as replayed
            FROM quarantine.sales_failed
        """

        from utils.postgres_hook import PostgresLayerHook
        pg_hook = PostgresLayerHook()
        q_result = pg_hook.execute_query(quarantine_count_query)
        q_stats = q_result[1][0] if q_result[1] else (0, 0, 0)

        subject = f"[{'ALERT' if has_issues else 'OK'}] Data Quality Report - {datetime.now().strftime('%Y-%m-%d')}"

        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; margin: 20px;">
            <h1>Data Quality Report</h1>
            <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            
            <h2 style="color: #2c3e50;">Quarantine Status</h2>
            <table style="border-collapse: collapse; width: 100%; margin-bottom: 20px;">
                <tr style="background: #ecf0f1;">
                    <th style="padding: 10px; border: 1px solid #bdc3c7;">Total</th>
                    <th style="padding: 10px; border: 1px solid #bdc3c7;">Pending</th>
                    <th style="padding: 10px; border: 1px solid #bdc3c7;">Replayed</th>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #bdc3c7; text-align: center;">{q_stats[0]}</td>
                    <td style="padding: 10px; border: 1px solid #bdc3c7; text-align: center; background: {'#fadbd8' if q_stats[1] > 0 else '#d5f5e3'};">{q_stats[1]}</td>
                    <td style="padding: 10px; border: 1px solid #bdc3c7; text-align: center;">{q_stats[2]}</td>
                </tr>
            </table>
            
            <h2 style="color: #2c3e50;">Silver Profiling Results</h2>
            <table style="border-collapse: collapse; width: 100%; margin-bottom: 20px;">
                <tr style="background: #ecf0f1;">
                    <th style="padding: 10px; border: 1px solid #bdc3c7;">Table</th>
                    <th style="padding: 10px; border: 1px solid #bdc3c7;">Status</th>
                    <th style="padding: 10px; border: 1px solid #bdc3c7;">Expectations</th>
                </tr>
        """

        for table, result in profiling_results.items():
            status = "✓ OK" if result.get("success") else "✗ FAILED"
            exp_count = result.get("expectation_count", "N/A")
            bg = "#d5f5e3" if result.get("success") else "#fadbd8"
            html_body += f"""
                <tr>
                    <td style="padding: 10px; border: 1px solid #bdc3c7;">{table}</td>
                    <td style="padding: 10px; border: 1px solid #bdc3c7; background: {bg};">{status}</td>
                    <td style="padding: 10px; border: 1px solid #bdc3c7; text-align: center;">{exp_count}</td>
                </tr>
            """

        html_body += """
            </table>
            
            <h2 style="color: #2c3e50;">Drift Detection Results</h2>
            <table style="border-collapse: collapse; width: 100%; margin-bottom: 20px;">
                <tr style="background: #ecf0f1;">
                    <th style="padding: 10px; border: 1px solid #bdc3c7;">Table</th>
                    <th style="padding: 10px; border: 1px solid #bdc3c7;">Drift Detected</th>
                    <th style="padding: 10px; border: 1px solid #bdc3c7;">Issues</th>
                </tr>
        """

        for table, result in drift_results.items():
            drift = "⚠ YES" if result.get("drift_detected") else "✓ NO"
            issues = result.get("unexpected_count", 0)
            bg = "#fadbd8" if result.get("drift_detected") else "#d5f5e3"
            html_body += f"""
                <tr>
                    <td style="padding: 10px; border: 1px solid #bdc3c7;">{table}</td>
                    <td style="padding: 10px; border: 1px solid #bdc3c7; background: {bg};">{drift}</td>
                    <td style="padding: 10px; border: 1px solid #bdc3c7; text-align: center;">{issues}</td>
                </tr>
            """

        docs_url = docs_info.get("docs_url", "N/A")
        html_body += f"""
            </table>
            
            <h2 style="color: #2c3e50;">Data Docs</h2>
            <p>View detailed reports: <a href="{docs_url}">{docs_url}</a></p>
            
            <hr>
            <p style="color: #7f8c8d; font-size: 12px;">
                Generated by Great Expectations Data Quality Pipeline
            </p>
        </body>
        </html>
        """

        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = 'airflow@amalitech.local'
            msg['To'] = ALERT_EMAIL

            part = MIMEText(html_body, 'html')
            msg.attach(part)

            with smtplib.SMTP('smtp.gmail.com', 587) as server:
                server.starttls()
                server.login('daniel.agudey.doe@gmail.com', 'your_app_password')
                server.send_message(msg)

            logger.info(f"Alert email sent to {ALERT_EMAIL}")
            return {"status": "Email sent", "recipient": ALERT_EMAIL}

        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return {"status": "Failed", "error": str(e)}

    quarantine_results = validate_quarantine_patterns()
    profiling_results = profile_silver()
    drift_results = detect_drift()
    docs_info = generate_data_docs()

    send_alerts(
        quarantine_results=quarantine_results,
        profiling_results=profiling_results,
        drift_results=drift_results,
        docs_info=docs_info
    )
