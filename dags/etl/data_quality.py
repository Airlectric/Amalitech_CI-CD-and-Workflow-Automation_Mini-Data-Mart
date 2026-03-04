from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
import logging

from utils.email_utils import send_data_quality_alert

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
    dag_id="data_quality_checks",
    start_date=datetime(2026, 1, 1),
    schedule="0 */6 * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["quality", "sql", "validation"],
    description="Data quality checks — monitoring and alerting only",
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
        """
        Detect drift by comparing current metrics against historical baselines stored in metadata.quality_baselines.
        Creates baseline if none exists, then compares against baseline from 24 hours ago.
        """
        from utils.postgres_hook import PostgresLayerHook
        pg_hook = PostgresLayerHook()

        logger.info("Detecting drift against historical baselines")

        tables = ["sales"]  # Only check incoming data (fact table) for drift, not dimensions (rebuilt from sales)
        drift_results = {}
        DRIFT_THRESHOLD_PCT = 10.0

        for table in tables:
            is_populated = profiling_results.get(table, {}).get("populated", False)

            if not is_populated:
                drift_results[table] = {
                    "drift_detected": False,
                    "current_count": 0,
                    "baseline_count": 0,
                    "change_pct": 0,
                    "threshold_pct": DRIFT_THRESHOLD_PCT,
                    "status": "not_populated"
                }
                logger.info(f"Skipping {table}: not yet populated")
                continue

            try:
                current_count = profiling_results.get(table, {}).get("row_count", 0)

                # Get baseline from 24 hours ago (most recent baseline before 24 hours ago)
                baseline_result = pg_hook.execute_query("""
                    SELECT metric_value
                    FROM metadata.quality_baselines
                    WHERE table_name = %s
                      AND metric_name = 'row_count'
                      AND recorded_at < NOW() - INTERVAL '24 hours'
                    ORDER BY recorded_at DESC
                    LIMIT 1
                """, (table,))

                if baseline_result and len(baseline_result) > 1 and baseline_result[1]:
                    baseline_count = float(baseline_result[1][0][0])
                else:
                    baseline_count = 0

                # Calculate drift
                if baseline_count > 0:
                    change_pct = abs(current_count - baseline_count) / baseline_count * 100
                else:
                    change_pct = 0.0

                drift_detected = change_pct > DRIFT_THRESHOLD_PCT and baseline_count > 0

                drift_results[table] = {
                    "drift_detected": drift_detected,
                    "current_count": current_count,
                    "baseline_count": int(baseline_count),
                    "change_pct": round(change_pct, 2),
                    "threshold_pct": DRIFT_THRESHOLD_PCT,
                    "status": "drift_alert" if drift_detected else "normal"
                }

                # Store current count as new baseline
                pg_hook.execute_query("""
                    INSERT INTO metadata.quality_baselines (table_name, metric_name, metric_value)
                    VALUES (%s, 'row_count', %s)
                """, (table, current_count))

                logger.info(f"Drift check {table}: current={current_count}, baseline={int(baseline_count)}, "
                           f"change={change_pct:.2f}%, drift={drift_detected}")

            except Exception as e:
                drift_results[table] = {"drift_detected": False, "error": str(e)}
                logger.error(f"Drift check failed for {table}: {e}")

        return drift_results

    @task
    def check_completeness():
        """Check completeness (non-null %) for critical columns"""
        from utils.postgres_hook import PostgresLayerHook

        pg_hook = PostgresLayerHook()
        logger.info("Checking data completeness")

        critical_columns = {
            "silver.sales": ["transaction_id", "customer_id", "product_id", "sale_date", "net_amount"],
            "silver.customers": ["customer_id", "customer_name"],
            "silver.products": ["product_id", "product_name", "category"]
        }

        completeness_results = {}
        COMPLETENESS_THRESHOLD = 95.0

        for table, columns in critical_columns.items():
            table_results = {}
            for col in columns:
                try:
                    result = pg_hook.execute_query(f"""
                        SELECT
                            COUNT(*) as total,
                            COUNT({col}) as non_null,
                            ROUND(COUNT({col})::DECIMAL / NULLIF(COUNT(*), 0) * 100, 2) as completeness_pct
                        FROM {table}
                    """)

                    if result and len(result) > 1 and result[1]:
                        row = result[1][0]
                        if isinstance(row, dict):
                            completeness_pct = float(row.get("completeness_pct", 0) or 0)
                        else:
                            completeness_pct = float(row[2]) if row and len(row) > 2 else 0
                    else:
                        completeness_pct = 0

                    table_results[col] = {
                        "completeness_pct": completeness_pct,
                        "passes_threshold": completeness_pct >= COMPLETENESS_THRESHOLD
                    }
                    logger.info(f"{table}.{col}: {completeness_pct}% complete (threshold: {COMPLETENESS_THRESHOLD}%)")

                except Exception as e:
                    table_results[col] = {"completeness_pct": 0, "passes_threshold": False, "error": str(e)}
                    logger.error(f"Completeness check failed for {table}.{col}: {e}")

            completeness_results[table] = table_results

        return completeness_results

    @task
    def check_freshness():
        """Check if data is arriving within expected SLA"""
        from utils.postgres_hook import PostgresLayerHook
        from datetime import datetime, timedelta

        pg_hook = PostgresLayerHook()
        logger.info("Checking data freshness")

        FRESHNESS_SLA_HOURS = 24

        try:
            result = pg_hook.execute_query("""
                SELECT
                    MAX(sale_date) as latest_sale_date,
                    MAX(processed_at) as latest_processed,
                    EXTRACT(EPOCH FROM (NOW() - MAX(processed_at)))/3600 as hours_since_last_update
                FROM silver.sales
            """)

            if result and len(result) > 1 and result[1]:
                row = result[1][0]
                if isinstance(row, dict):
                    hours_since_update = float(row.get("hours_since_update", 0) or 0) if row.get("hours_since_update") else float('inf')
                else:
                    hours_since_update = float(row[2]) if row and len(row) > 2 and row[2] else float('inf')
            else:
                hours_since_update = float('inf')

            is_fresh = hours_since_update <= FRESHNESS_SLA_HOURS

            freshness_result = {
                "is_fresh": is_fresh,
                "hours_since_update": round(hours_since_update, 2) if hours_since_update != float('inf') else None,
                "sla_hours": FRESHNESS_SLA_HOURS,
                "status": "ok" if is_fresh else "stale_data_alert"
            }
            logger.info(f"Freshness check: {freshness_result}")
            return freshness_result

        except Exception as e:
            logger.error(f"Freshness check failed: {e}")
            return {"is_fresh": False, "status": "error", "error": str(e)}

    @task
    def check_referential_integrity():
        """Check FK consistency between tables"""
        from utils.postgres_hook import PostgresLayerHook

        pg_hook = PostgresLayerHook()
        logger.info("Checking referential integrity")

        integrity_results = {"integrity_valid": True, "issues": []}

        try:
            # Check: All customer_ids in sales exist in customers
            orphan_customers = pg_hook.execute_query("""
                SELECT COUNT(DISTINCT s.customer_id) as orphan_count
                FROM silver.sales s
                LEFT JOIN silver.customers c ON s.customer_id = c.customer_id
                WHERE c.customer_id IS NULL AND s.customer_id IS NOT NULL
            """)

            if orphan_customers and len(orphan_customers) > 1 and orphan_customers[1]:
                orphan_customer_count = orphan_customers[1][0][0] if isinstance(orphan_customers[1][0], tuple) else orphan_customers[1][0].get("orphan_count", 0)
                if isinstance(orphan_customer_count, dict):
                    orphan_customer_count = orphan_customer_count.get("orphan_count", 0)
            else:
                orphan_customer_count = 0

            if orphan_customer_count > 0:
                integrity_results["integrity_valid"] = False
                integrity_results["issues"].append(f"{orphan_customer_count} orphan customer_ids in sales")

        except Exception as e:
            logger.error(f"Customer integrity check failed: {e}")

        try:
            # Check: All product_ids in sales exist in products
            orphan_products = pg_hook.execute_query("""
                SELECT COUNT(DISTINCT s.product_id) as orphan_count
                FROM silver.sales s
                LEFT JOIN silver.products p ON s.product_id = p.product_id
                WHERE p.product_id IS NULL AND s.product_id IS NOT NULL
            """)

            if orphan_products and len(orphan_products) > 1 and orphan_products[1]:
                orphan_product_count = orphan_products[1][0][0] if isinstance(orphan_products[1][0], tuple) else orphan_products[1][0].get("orphan_count", 0)
                if isinstance(orphan_product_count, dict):
                    orphan_product_count = orphan_product_count.get("orphan_count", 0)
            else:
                orphan_product_count = 0

            if orphan_product_count > 0:
                integrity_results["integrity_valid"] = False
                integrity_results["issues"].append(f"{orphan_product_count} orphan product_ids in sales")

        except Exception as e:
            logger.error(f"Product integrity check failed: {e}")

        logger.info(f"Referential integrity check: {integrity_results}")
        return integrity_results

    @task
    def generate_data_docs(
        quarantine_results=None,
        profiling_results=None,
        drift_results=None,
        completeness_results=None,
        freshness_results=None,
        integrity_results=None
    ):
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
        elements.append(Spacer(1, 20))

        # Completeness Check
        elements.append(Paragraph("Data Completeness", styles['Heading2']))
        c_data = [['Table.Column', 'Completeness %', 'Status']]
        if completeness_results:
            for table, cols in completeness_results.items():
                for col, stats in cols.items():
                    status = "OK" if stats.get("passes_threshold", False) else "FAIL"
                    pct = f"{stats.get('completeness_pct', 0):.1f}%"
                    c_data.append([f"{table}.{col}", pct, status])

        c_table = Table(c_data, colWidths=[200, 120, 80])
        c_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.green),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ]))
        elements.append(c_table)
        elements.append(Spacer(1, 20))

        # Freshness Check
        elements.append(Paragraph("Data Freshness", styles['Heading2']))
        if freshness_results:
            f_status = "FRESH" if freshness_results.get("is_fresh", False) else "STALE"
            f_hours = freshness_results.get("hours_since_update")
            f_sla = freshness_results.get("sla_hours", "N/A")
            f_data = [
                ['Status', f_status],
                ['Hours Since Update', str(f_hours) if f_hours else "N/A"],
                ['SLA (hours)', str(f_sla)]
            ]
            f_table = Table(f_data, colWidths=[200, 200])
            f_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.green),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ]))
            elements.append(f_table)
            elements.append(Spacer(1, 20))

        # Referential Integrity Check
        elements.append(Paragraph("Referential Integrity", styles['Heading2']))
        if integrity_results:
            i_valid = "VALID" if integrity_results.get("integrity_valid", True) else "INVALID"
            i_issues = integrity_results.get("issues", [])
            i_data = [
                ['Status', i_valid],
                ['Issues', ', '.join(i_issues) if i_issues else 'None']
            ]
            i_table = Table(i_data, colWidths=[200, 300])
            i_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.green),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ]))
            elements.append(i_table)

        # Build PDF
        doc.build(elements)

        docs_url = f"file://{report_file}"
        logger.info(f"Data quality report generated: {report_file}")

        return {"docs_url": docs_url, "report_file": report_file}

    @task
    def send_alerts(
        quarantine_results,
        profiling_results,
        drift_results,
        docs_info,
        completeness_results=None,
        freshness_results=None,
        integrity_results=None
    ):
        logger.info("Evaluating quality results for alerting")

        from utils.postgres_hook import PostgresLayerHook
        pg_hook = PostgresLayerHook()

        q_result = pg_hook.execute_query("""
            SELECT COUNT(*) as total,
                   COUNT(CASE WHEN remediation_status = 'pending' THEN 1 END) as pending,
                   COUNT(CASE WHEN remediation_status = 'remediated' THEN 1 END) as remediated,
                   COUNT(CASE WHEN remediation_status = 'rejected' THEN 1 END) as rejected,
                   COUNT(CASE WHEN remediation_status = 'dead_letter' THEN 1 END) as dead_letter
            FROM quarantine.sales_failed
        """)

        if len(q_result) > 1 and q_result[1]:
            row = q_result[1][0]
            if isinstance(row, dict):
                total = row.get("total", 0)
                pending = row.get("pending", 0)
                remediated = row.get("remediated", 0)
                rejected = row.get("rejected", 0)
                dead_letter = row.get("dead_letter", 0)
            else:
                total, pending, remediated, rejected, dead_letter = row if len(row) == 5 else (row[0] if row else 0, 0, 0, 0, 0)
        else:
            total = pending = remediated = rejected = dead_letter = 0

        quarantine_stats = {
            "stats": {
                "total_quarantined": total,
                "pending": pending,
                "remediated": remediated,
                "rejected": rejected,
                "dead_letter": dead_letter,
            },
            "pending": pending,
            "remediated": remediated,
        }

        has_quarantine_issues = pending > 0

        has_drift_issues = any(r.get("drift_detected", False) for r in drift_results.values())
        has_profiling_issues = any(not r.get("success", True) for r in profiling_results.values())

        has_completeness_issues = False
        if completeness_results:
            for table_cols in completeness_results.values():
                for col_stats in table_cols.values():
                    if not col_stats.get("passes_threshold", True):
                        has_completeness_issues = True
                        break

        has_freshness_issues = False
        if freshness_results and not freshness_results.get("is_fresh", True):
            has_freshness_issues = True

        has_integrity_issues = False
        if integrity_results and not integrity_results.get("integrity_valid", True):
            has_integrity_issues = True

        has_any_issues = (
            has_quarantine_issues or has_drift_issues or has_profiling_issues
            or has_completeness_issues or has_freshness_issues or has_integrity_issues
        )

        logger.info(
            f"Quality summary: quarantine={has_quarantine_issues}, drift={has_drift_issues}, "
            f"profiling={has_profiling_issues}, completeness={has_completeness_issues}, "
            f"freshness={has_freshness_issues}, integrity={has_integrity_issues}"
        )

        if has_any_issues:
            pdf_path = docs_info.get("report_file") if isinstance(docs_info, dict) else None

            result = send_data_quality_alert(
                quarantine_stats=quarantine_stats,
                profiling_results=profiling_results,
                drift_results=drift_results,
                recipient=ALERT_EMAIL,
                attachment_path=pdf_path
            )
            logger.info(f"Alert email sent: {result}")
        else:
            logger.info("All quality checks passed — no alert needed")

        return {
            "has_issues": has_any_issues,
            "quarantine_pending": pending,
            "drift": has_drift_issues,
            "completeness": has_completeness_issues,
            "freshness": has_freshness_issues,
            "integrity": has_integrity_issues,
        }

    # Phase 1: Run all quality checks in parallel
    quarantine_results = validate_quarantine_patterns()
    profiling_results = profile_silver()
    drift_results = detect_drift(profiling_results)
    completeness_results = check_completeness()
    freshness_results = check_freshness()
    integrity_results = check_referential_integrity()

    # Phase 2: Generate data docs AFTER quality checks complete
    docs_info = generate_data_docs(
        quarantine_results,
        profiling_results,
        drift_results,
        completeness_results,
        freshness_results,
        integrity_results
    )

    # Phase 3: Send alerts AFTER everything is done
    send_alerts(
        quarantine_results=quarantine_results,
        profiling_results=profiling_results,
        drift_results=drift_results,
        completeness_results=completeness_results,
        freshness_results=freshness_results,
        integrity_results=integrity_results,
        docs_info=docs_info
    )
