from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
import logging
import sys
import importlib.util

def _import_email_utils():
    spec = importlib.util.spec_from_file_location(
        "email_utils",
        "/opt/airflow/scripts/utils/email_utils.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

email_utils = _import_email_utils()
send_data_quality_alert = email_utils.send_data_quality_alert

sys.path.insert(0, "/opt/airflow/scripts")
sys.path.insert(0, "/opt/airflow/dags")

logger = logging.getLogger(__name__)

ALERT_EMAIL = Variable.get("alert_email", default_var="daniel.doe@a2sv.org")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="data_quality_gx",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["quality", "sql", "validation"],
    description="Data quality checks using SQL queries",
) as dag:

    @task
    def validate_quarantine_patterns():
        from utils.postgres_hook import PostgresLayerHook
        pg_hook = PostgresLayerHook()
        
        logger.info("Validating quarantine patterns")
        
        result = pg_hook.execute_query("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(CASE WHEN id IS NULL THEN 1 END) as null_ids,
                COUNT(CASE WHEN payload IS NULL THEN 1 END) as null_payloads,
                COUNT(CASE WHEN error_reason IS NULL THEN 1 END) as null_errors
            FROM quarantine.sales_failed
        """)
        
        if len(result) > 1 and result[1]:
            row = result[1][0]
            if isinstance(row, dict):
                total = row.get("total_rows", 0)
                null_ids = row.get("null_ids", 0)
                null_payloads = row.get("null_payloads", 0)
                null_errors = row.get("null_errors", 0)
            else:
                total, null_ids, null_payloads, null_errors = row
        else:
            total = null_ids = null_payloads = null_errors = 0
        
        success = null_ids == 0 and null_payloads == 0 and null_errors == 0
        
        return {
            "success": success,
            "total_rows": total,
            "null_ids": null_ids,
            "null_payloads": null_payloads,
            "null_errors": null_errors,
        }

    @task
    def profile_silver():
        from utils.postgres_hook import PostgresLayerHook
        pg_hook = PostgresLayerHook()
        
        logger.info("Profiling Silver tables")
        
        tables = ["sales", "customers", "products"]
        profiling_results = {}
        
        for table in tables:
            try:
                result = pg_hook.execute_query(f"""
                    SELECT COUNT(*) as row_count FROM silver.{table}
                """)
                
                if len(result) > 1 and result[1]:
                    row = result[1][0]
                    if isinstance(row, dict):
                        row_count = row.get("row_count", 0)
                    else:
                        row_count = row[0] if row else 0
                else:
                    row_count = 0
                
                profiling_results[table] = {
                    "success": True,
                    "row_count": row_count,
                    "populated": row_count > 0
                }
                logger.info(f"Profiled {table}: {row_count} rows (populated={row_count > 0})")
                
            except Exception as e:
                profiling_results[table] = {"success": False, "error": str(e), "populated": False}
                logger.error(f"Failed to profile {table}: {e}")
        
        return profiling_results

    @task
    def detect_drift(profiling_results):
        from utils.postgres_hook import PostgresLayerHook
        pg_hook = PostgresLayerHook()
        
        logger.info("Detecting drift - comparing with previous runs")
        
        tables = ["sales", "customers", "products"]
        drift_results = {}
        
        for table in tables:
            is_populated = profiling_results.get(table, {}).get("populated", False)
            
            if not is_populated:
                drift_results[table] = {
                    "drift_detected": False,
                    "current_count": 0,
                    "previous_count": 0,
                    "change_pct": 0,
                    "status": "not_populated"
                }
                logger.info(f"Skipping {table}: not yet populated (will be populated by silver_to_gold)")
                continue
            
            try:
                current_result = pg_hook.execute_query(f"""
                    SELECT COUNT(*) as row_count FROM silver.{table}
                """)
                
                if len(current_result) > 1 and current_result[1]:
                    row = current_result[1][0]
                    if isinstance(row, dict):
                        current_count = row.get("row_count", 0)
                    else:
                        current_count = row[0] if row else 0
                else:
                    current_count = 0
                
                previous_count = profiling_results.get(table, {}).get("row_count", 0)
                
                has_drift = False
                if previous_count > 0:
                    change_pct = abs(current_count - previous_count) / previous_count * 100
                    has_drift = change_pct > 10
                
                drift_results[table] = {
                    "drift_detected": has_drift,
                    "current_count": current_count,
                    "previous_count": previous_count,
                    "change_pct": abs(current_count - previous_count) / previous_count * 100 if previous_count > 0 else 0,
                    "status": "checked"
                }
                logger.info(f"Drift check {table}: current={current_count}, previous={previous_count}, drift={has_drift}")
                
            except Exception as e:
                drift_results[table] = {"drift_detected": False, "error": str(e)}
                logger.error(f"Drift check failed for {table}: {e}")
        
        return drift_results

    @task
    def generate_data_docs(quarantine_results=None, profiling_results=None, drift_results=None):
        import os
        from datetime import datetime
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import letter
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        
        logger.info("Generating Data Quality Report")
        
        report_dir = "/opt/airflow/data/data_docs"
        os.makedirs(report_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = os.path.join(report_dir, f"quality_report_{timestamp}.pdf")
        
        doc = SimpleDocTemplate(report_file, pagesize=letter)
        styles = getSampleStyleSheet()
        elements = []
        
        # Title
        title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=20, spaceAfter=20)
        elements.append(Paragraph("Data Quality Report", title_style))
        elements.append(Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Normal']))
        elements.append(Spacer(1, 20))
        
        # Quarantine Summary
        elements.append(Paragraph("Quarantine Summary", styles['Heading2']))
        q_data = [
            ['Metric', 'Value'],
            ['Total Rows', str(quarantine_results.get('total_rows', 'N/A') if quarantine_results else 'N/A')],
            ['Null IDs', str(quarantine_results.get('null_ids', 'N/A') if quarantine_results else 'N/A')],
            ['Null Payloads', str(quarantine_results.get('null_payloads', 'N/A') if quarantine_results else 'N/A')],
            ['Null Errors', str(quarantine_results.get('null_errors', 'N/A') if quarantine_results else 'N/A')],
        ]
        q_table = Table(q_data, colWidths=[200, 200])
        q_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.green),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ]))
        elements.append(q_table)
        elements.append(Spacer(1, 20))
        
        # Silver Tables Profiling
        elements.append(Paragraph("Silver Tables Profiling", styles['Heading2']))
        p_data = [['Table', 'Row Count', 'Status']]
        if profiling_results:
            for table, data in profiling_results.items():
                status = "OK" if data.get("success") else "FAILED"
                p_data.append([table, str(data.get("row_count", 0)), status])
        
        p_table = Table(p_data, colWidths=[150, 150, 100])
        p_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.green),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ]))
        elements.append(p_table)
        elements.append(Spacer(1, 20))
        
        # Drift Detection
        elements.append(Paragraph("Drift Detection", styles['Heading2']))
        d_data = [['Table', 'Drift Detected', 'Change %']]
        if drift_results:
            for table, data in drift_results.items():
                drift = "YES" if data.get("drift_detected", False) else "NO"
                change_pct = f"{data.get('change_pct', 0):.2f}%"
                d_data.append([table, drift, change_pct])
        
        d_table = Table(d_data, colWidths=[150, 150, 100])
        d_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.green),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ]))
        elements.append(d_table)
        
        # Build PDF
        doc.build(elements)
        
        docs_url = f"file://{report_file}"
        logger.info(f"Data quality report generated: {report_file}")
        
        return {"docs_url": docs_url, "report_file": report_file}

    @task
    def send_alerts(quarantine_results, profiling_results, drift_results, docs_info):
        logger.info("Sending quality alert emails")

        from utils.postgres_hook import PostgresLayerHook
        pg_hook = PostgresLayerHook()

        q_result = pg_hook.execute_query("""
            SELECT COUNT(*) as total,
                   COUNT(CASE WHEN replayed = FALSE THEN 1 END) as pending,
                   COUNT(CASE WHEN replayed = TRUE THEN 1 END) as replayed
            FROM quarantine.sales_failed
        """)

        if len(q_result) > 1 and q_result[1]:
            row = q_result[1][0]
            if isinstance(row, dict):
                total = row.get("total", 0)
                pending = row.get("pending", 0)
                replayed = row.get("replayed", 0)
            else:
                total, pending, replayed = row if len(row) == 3 else (row[0] if row else 0, 0, 0)
        else:
            total = pending = replayed = 0

        quarantine_stats = {
            "stats": {
                "total_quarantined": total,
                "pending": pending,
                "replayed": replayed,
            },
            "pending": pending,
            "replayed": replayed,
        }

        # Get PDF report path from docs_info
        pdf_path = docs_info.get("report_file") if isinstance(docs_info, dict) else None

        result = send_data_quality_alert(
            quarantine_stats=quarantine_stats,
            profiling_results=profiling_results,
            drift_results=drift_results,
            recipient=ALERT_EMAIL,
            attachment_path=pdf_path
        )

        logger.info(f"Email send result: {result}")
        return result

    # Dependencies
    # Phase 1: Run all quality checks in parallel
    quarantine_results = validate_quarantine_patterns()
    profiling_results = profile_silver()
    drift_results = detect_drift(profiling_results)
    
    # Phase 2: Generate data docs AFTER quality checks complete
    docs_info = generate_data_docs(quarantine_results, profiling_results, drift_results)

    # Phase 3: Send alerts AFTER everything is done
    send_alerts(
        quarantine_results=quarantine_results,
        profiling_results=profiling_results,
        drift_results=drift_results,
        docs_info=docs_info
    )
