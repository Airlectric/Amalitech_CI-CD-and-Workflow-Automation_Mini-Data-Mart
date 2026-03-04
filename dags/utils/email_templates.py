"""
Email Templates for Data Quality Alerts
"""

QUALITY_FAILURE_EMAIL = """
<html>
<body style="font-family: Arial, sans-serif;">
    <h2 style="color: #c53030;">⚠️ Data Quality Check Failed</h2>

    <table style="border-collapse: collapse; width: 100%; margin: 20px 0;">
        <tr style="background: #fed7d7;">
            <th style="padding: 10px; border: 1px solid #e53e3e; text-align: left;">Metric</th>
            <th style="padding: 10px; border: 1px solid #e53e3e; text-align: left;">Value</th>
        </tr>
        <tr>
            <td style="padding: 10px; border: 1px solid #e53e3e;">Total Checks</td>
            <td style="padding: 10px; border: 1px solid #e53e3e;">{total_checks}</td>
        </tr>
        <tr>
            <td style="padding: 10px; border: 1px solid #e53e3e;">Passed</td>
            <td style="padding: 10px; border: 1px solid #e53e3e; color: green;">{passed}</td>
        </tr>
        <tr>
            <td style="padding: 10px; border: 1px solid #e53e3e;">Failed</td>
            <td style="padding: 10px; border: 1px solid #e53e3e; color: red; font-weight: bold;">{failed}</td>
        </tr>
        <tr style="padding: 10px; border: 1px solid #e53e3e;">Pass Rate>
            <td</td>
            <td style="padding: 10px; border: 1px solid #e53e3e;">{pass_rate}%</td>
        </tr>
    </table>

    <h3 style="color: #744210;">Failed Checks Details (with Source Files):</h3>
    <table style="border-collapse: collapse; width: 100%; margin: 20px 0;">
        <tr style="background: #feebc8;">
            <th style="padding: 10px; border: 1px solid #c05621; text-align: left;">Table</th>
            <th style="padding: 10px; border: 1px solid #c05621; text-align: left;">Column</th>
            <th style="padding: 10px; border: 1px solid #c05621; text-align: left;">Check Type</th>
            <th style="padding: 10px; border: 1px solid #c05621; text-align: left;">Issue</th>
            <th style="padding: 10px; border: 1px solid #c05621; text-align: left;">⚠️ Source File</th>
        </tr>
        {failure_rows}
    </table>

    <h3 style="color: #744210;">Recently Ingested Source Files:</h3>
    <ul style="background: #f7fafc; padding: 15px; border-radius: 5px;">
        {source_list}
    </ul>

    <h3 style="color: #744210;">Action Required:</h3>
    <ol style="background: #f7fafc; padding: 15px 30px; border-radius: 5px;">
        <li>Identify the source file(s) with bad data from the table above</li>
        <li>Investigate and fix the source data in MinIO Bronze layer</li>
        <li>Re-run the ingestion pipeline after fixing</li>
    </ol>

    <p style="color: #718096; margin-top: 20px;">
        <strong>DAG:</strong> data_quality_checks<br>
        <strong>Date:</strong> {timestamp}<br>
        <strong>Run ID:</strong> {run_id}
    </p>

    <p style="color: #718096;">
        Please review the quality report in Airflow UI for more details.
    </p>
</body>
</html>
"""

SIMPLE_FAILURE_ROW = """
<tr>
    <td>{table}</td>
    <td>{column}</td>
    <td>{check}</td>
    <td>{issue}</td>
    <td style="background: #fed7d7;"><strong>{source}</strong></td>
</tr>
"""

SIMPLE_SOURCE_ITEM = "<li><code>{file_path}</code> (ingested: {ingest_date})</li>"


def format_failure_email(
    total_checks: int,
    passed: int,
    failed: int,
    pass_rate: float,
    failed_details: list,
    failed_sources: dict,
    source_files: list,
    timestamp: str,
    run_id: str,
) -> str:
    """Format the failure email with all data"""

    failure_rows = ""
    for f in failed_details:
        table = f.get("table", "N/A")
        column = f.get("column", "N/A")
        check = f.get("check", "N/A")
        details = f.get("details", {})

        key = f"{table}.{column}"
        sources = failed_sources.get(key, [])
        if sources:
            source_str = ", ".join(
                [f"{s.get('ingest_date', '?')} ({s.get('bad_record_count', 0)} bad)" for s in sources]
            )
        else:
            source_str = "N/A"

        if check == "not_null":
            issue = f"Null count: {details.get('null_count', 'N/A')}"
        elif check == "uniqueness":
            issue = f"Duplicates: {details.get('duplicate_count', 'N/A')}"
        elif check == "value_range":
            issue = f"Issues: {len(details.get('issues', []))}"
        else:
            issue = str(details)

        failure_rows += SIMPLE_FAILURE_ROW.format(
            table=table, column=column, check=check, issue=issue, source=source_str
        )

    if not failure_rows:
        failure_rows = "<tr><td colspan='5'>No specific details available</td></tr>"

    source_list = ""
    for sf in source_files[:5]:
        source_list += SIMPLE_SOURCE_ITEM.format(
            file_path=sf.get("file_path", "N/A"), ingest_date=sf.get("ingest_date", "N/A")
        )

    if not source_list:
        source_list = "<li>No source files found</li>"

    return QUALITY_FAILURE_EMAIL.format(
        total_checks=total_checks,
        passed=passed,
        failed=failed,
        pass_rate=pass_rate,
        failure_rows=failure_rows,
        source_list=source_list,
        timestamp=timestamp,
        run_id=run_id,
    )
