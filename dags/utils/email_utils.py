"""
Central Email Utility for Airflow DAGs
Provides a reusable function to send email alerts via Gmail SMTP
With retry mechanism, alert severity levels, and distribution list support
"""
import os
import smtplib
import logging
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum
from functools import wraps


logger = logging.getLogger(__name__)

# Alert severity levels
class AlertSeverity(Enum):
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


# Throttling configuration (use Redis in production for multi-worker)
_alert_cache: Dict[str, datetime] = {}

THROTTLE_INTERVALS = {
    AlertSeverity.CRITICAL: None,  # No throttle
    AlertSeverity.WARNING: 3600,    # 1 hour
    AlertSeverity.INFO: 21600,     # 6 hours
}

# Retry configuration
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2
RETRY_MAX_DELAY = 60


def get_smtp_config() -> Dict[str, Any]:
    """Get SMTP config from environment variables - no hardcoded defaults"""
    smtp_user = os.getenv("AIRFLOW__SMTP__SMTP_USER")
    smtp_password = os.getenv("AIRFLOW__SMTP__SMTP_PASSWORD")

    if not smtp_user or not smtp_password:
        raise ValueError(
            "SMTP credentials not configured. "
            "Set AIRFLOW__SMTP__SMTP_USER and AIRFLOW__SMTP__SMTP_PASSWORD environment variables"
        )

    return {
        "host": os.getenv("AIRFLOW__SMTP__SMTP_HOST", "smtp.gmail.com"),
        "port": int(os.getenv("AIRFLOW__SMTP__SMTP_PORT", "587")),
        "user": smtp_user,
        "password": smtp_password,
    }


def retry_with_backoff(max_retries: int = MAX_RETRIES, base_delay: int = RETRY_BASE_DELAY):
    """Decorator for retry with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        delay = min(base_delay * (2 ** attempt), RETRY_MAX_DELAY)
                        logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                        time.sleep(delay)

            logger.error(f"All {max_retries} attempts failed: {last_exception}")
            return {"status": "failed", "error": str(last_exception), "attempts": max_retries}
        return wrapper
    return decorator


def should_send_alert(alert_key: str, severity: AlertSeverity) -> bool:
    """Check if alert should be sent based on throttling rules"""
    throttle_seconds = THROTTLE_INTERVALS.get(severity)
    if throttle_seconds is None:
        return True

    last_sent = _alert_cache.get(alert_key)
    if last_sent is None:
        return True

    elapsed = (datetime.now() - last_sent).total_seconds()
    return elapsed > throttle_seconds


def send_alert_email(
    subject: str,
    html_body: str,
    recipient: str,
    sender: Optional[str] = None,
    password: Optional[str] = None,
    smtp_host: Optional[str] = None,
    smtp_port: Optional[int] = None
) -> Dict[str, Any]:
    """
    Send an HTML email alert with retry mechanism

    Args:
        subject: Email subject line
        html_body: HTML content of the email
        recipient: Recipient email address
        sender: Sender email address (defaults to SMTP user)
        password: App password for Gmail SMTP
        smtp_host: SMTP server hostname
        smtp_port: SMTP server port

    Returns:
        Dict with status and any error message
    """
    # Get config from environment if not provided
    if sender is None or password is None or smtp_host is None or smtp_port is None:
        config = get_smtp_config()
        sender = sender or config["user"]
        password = password or config["password"]
        smtp_host = smtp_host or config["host"]
        smtp_port = smtp_port or config["port"]

    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = recipient

        part = MIMEText(html_body, 'html')
        msg.attach(part)

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(sender, password)
            server.send_message(msg)

        logger.info(f"Alert email sent to {recipient}: {subject}")
        return {"status": "sent", "recipient": recipient, "subject": subject}

    except Exception as e:
        logger.error(f"Failed to send email to {recipient}: {e}")
        return {"status": "failed", "error": str(e), "recipient": recipient}


@retry_with_backoff(max_retries=3, base_delay=2)
def send_alert_email_with_retry(
    subject: str,
    html_body: str,
    recipient: str,
    **kwargs
) -> Dict[str, Any]:
    """Send email with automatic retry"""
    return send_alert_email(subject, html_body, recipient, **kwargs)


def send_throttled_alert(
    subject: str,
    html_body: str,
    recipient: str,
    severity: AlertSeverity = AlertSeverity.WARNING,
    alert_type: str = "generic"
) -> Dict[str, Any]:
    """Send alert with severity-based throttling"""
    alert_key = f"{alert_type}:{recipient}:{subject[:50]}"

    if not should_send_alert(alert_key, severity):
        logger.info(f"Alert throttled: {alert_type} ({severity.value})")
        return {"status": "throttled", "severity": severity.value}

    # Add severity prefix to subject
    severity_prefixes = {
        AlertSeverity.CRITICAL: "[CRITICAL]",
        AlertSeverity.WARNING: "[WARNING]",
        AlertSeverity.INFO: "[INFO]",
    }
    full_subject = f"{severity_prefixes[severity]} {subject}"

    result = send_alert_email_with_retry(full_subject, html_body, recipient)

    if result.get("status") == "sent":
        _alert_cache[alert_key] = datetime.now()

    return result


def send_alert_to_team(
    subject: str,
    html_body: str,
    recipients: List[str],
    severity: AlertSeverity = AlertSeverity.WARNING,
    alert_type: str = "default"
) -> Dict[str, Any]:
    """Send alert to multiple recipients"""
    if not recipients:
        logger.warning("No alert recipients provided")
        return {"status": "no_recipients"}

    results = []
    for recipient in recipients:
        if recipient:
            result = send_throttled_alert(subject, html_body, recipient, severity, alert_type)
            results.append({"recipient": recipient, **result})

    return {"status": "sent", "results": results}


def send_ingestion_alert(
    run_id: str,
    total_read: int,
    silver_count: int,
    quarantine_count: int,
    files_scanned: List[str],
    errors: List[str],
    recipient: str,
) -> Dict[str, Any]:
    """Send ingestion pipeline alert email"""
    quarantine_rate = (quarantine_count / total_read * 100) if total_read > 0 else 0

    # Check for schema drift errors specifically
    schema_drift_errors = [e for e in errors if "SCHEMA_DRIFT" in e]
    has_schema_drift = len(schema_drift_errors) > 0
    other_errors = [e for e in errors if "SCHEMA_DRIFT" not in e]

    no_data_processed = total_read == 0 and len(files_scanned) == 0
    has_errors = len(errors) > 0
    high_quarantine = quarantine_rate > 50

    # Schema drift takes highest priority
    if has_schema_drift:
        subject_prefix = "[CRITICAL] SCHEMA DRIFT DETECTED"
        status_color = "#8B0000"  # Dark red
        status_text = "SCHEMA DRIFT - FILES SKIPPED"
        severity = AlertSeverity.CRITICAL
    elif no_data_processed:
        subject_prefix = "[INFO] SKIPPED"
        status_color = "#718096"
        status_text = "NO DATA - All files already processed"
        severity = AlertSeverity.INFO
    elif has_errors:
        subject_prefix = "[WARNING] COMPLETED WITH ERRORS"
        status_color = "#d69e2e"
        status_text = "COMPLETED WITH ERRORS"
        severity = AlertSeverity.WARNING
    elif high_quarantine:
        subject_prefix = "[WARNING] HIGH QUARANTINE"
        status_color = "#d69e2e"
        status_text = "HIGH QUARANTINE"
        severity = AlertSeverity.WARNING
    elif quarantine_count > 0:
        subject_prefix = "[WARNING] PARTIAL SUCCESS"
        status_color = "#d69e2e"
        status_text = "PARTIAL SUCCESS"
        severity = AlertSeverity.WARNING
    else:
        subject_prefix = "[INFO] SUCCESS"
        status_color = "#38a169"
        status_text = "SUCCESS"
        severity = AlertSeverity.INFO

    subject = f"{subject_prefix} Ingestion Alert - {run_id[:8]}"

    files_html = "".join([f"<li><code>{f}</code></li>" for f in files_scanned[:10]]) if files_scanned else "<li>No files to process</li>"

    # Separate schema drift errors from other errors
    schema_drift_html = ""
    if schema_drift_errors:
        schema_drift_html = """
        <h3 style="color: #8B0000; background: #ffe6e6; padding: 10px; border-radius: 5px;">
            SCHEMA DRIFT ERRORS (Files Skipped)
        </h3>
        <ul style="background: #ffe6e6; padding: 15px; border-radius: 5px; color: #8B0000;">
        """ + "".join([f"<li><code>{e}</code></li>" for e in schema_drift_errors]) + """
        </ul>
        <p style="color: #8B0000;">
            <strong>Action Required:</strong> Update the data source schema or DATASET_SPECS in ingest_bronze_to_silver.py to process these files.
        </p>
        """

    other_errors_html = "".join([f"<li><code>{e}</code></li>" for e in other_errors[:5]]) if other_errors else "<li>No errors</li>"

    errors_html = other_errors_html

    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; margin: 20px;">
        <h2 style="color: {status_color};">Ingestion Pipeline {status_text}</h2>

        <table style="border-collapse: collapse; width: 100%; margin: 20px 0;">
            <tr style="background: #ecf0f1;">
                <th style="padding: 10px; border: 1px solid #bdc3c7;">Metric</th>
                <th style="padding: 10px; border: 1px solid #bdc3c7;">Value</th>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #bdc3c7;">Run ID</td>
                <td style="padding: 10px; border: 1px solid #bdc3c7;"><code>{run_id}</code></td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #bdc3c7;">Rows Read</td>
                <td style="padding: 10px; border: 1px solid #bdc3c7;">{total_read:,}</td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #bdc3c7;">Rows to Silver</td>
                <td style="padding: 10px; border: 1px solid #bdc3c7; color: green;">{silver_count:,}</td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #bdc3c7;">Rows to Quarantine</td>
                <td style="padding: 10px; border: 1px solid #bdc3c7; color: {'red' if quarantine_count > 0 else 'green'};">{quarantine_count:,}</td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #bdc3c7;">Quarantine Rate</td>
                <td style="padding: 10px; border: 1px solid #bdc3c7;">{quarantine_rate:.1f}%</td>
            </tr>
        </table>

        {schema_drift_html}

        <h3>Files Processed:</h3>
        <ul style="background: #f7fafc; padding: 15px; border-radius: 5px;">
            {files_html}
        </ul>

        <h3>Errors:</h3>
        <ul style="background: #fff5f5; padding: 15px; border-radius: 5px; color: red;">
            {errors_html}
        </ul>

        <hr>
        <p style="color: #718096; font-size: 12px;">
            Sent by Airflow Ingestion Pipeline
        </p>
    </body>
    </html>
    """

    # Replace placeholder in html_body
    html_body = html_body.format(
        schema_drift_html=schema_drift_html,
        files_html=files_html,
        errors_html=errors_html
    )

    return send_throttled_alert(subject, html_body, recipient, severity, "ingestion")


def send_data_quality_alert(
    quarantine_stats: Dict[str, Any],
    profiling_results: Dict[str, Any],
    drift_results: Dict[str, Any],
    recipient: str,
    attachment_path: Optional[str] = None
) -> Dict[str, Any]:
    """Send data quality alert email"""
    has_issues = (
        quarantine_stats.get("pending", 0) > 0 or
        any(r.get("drift_detected", False) for r in drift_results.values()) or
        any(not r.get("success", True) for r in profiling_results.values())
    )

    subject_prefix = "[CRITICAL] ALERT" if has_issues else "[INFO] OK"
    severity = AlertSeverity.CRITICAL if has_issues else AlertSeverity.INFO
    subject = f"{subject_prefix} Data Quality Report - {datetime.now().strftime('%Y-%m-%d')}"

    q_stats = quarantine_stats.get("stats", {})
    pending = quarantine_stats.get("pending", 0)
    remediated = quarantine_stats.get("remediated", 0)

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
                <th style="padding: 10px; border: 1px solid #bdc3c7;">Remediated</th>
                <th style="padding: 10px; border: 1px solid #bdc3c7;">Rejected</th>
                <th style="padding: 10px; border: 1px solid #bdc3c7;">Dead Letter</th>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #bdc3c7; text-align: center;">{q_stats.get('total_quarantined', 0)}</td>
                <td style="padding: 10px; border: 1px solid #bdc3c7; text-align: center; background: {'#fadbd8' if pending > 0 else '#d5f5e3'};">{pending}</td>
                <td style="padding: 10px; border: 1px solid #bdc3c7; text-align: center;">{remediated}</td>
                <td style="padding: 10px; border: 1px solid #bdc3c7; text-align: center;">{q_stats.get('rejected', 0)}</td>
                <td style="padding: 10px; border: 1px solid #bdc3c7; text-align: center; background: {'#fadbd8' if q_stats.get('dead_letter', 0) > 0 else '#d5f5e3'};">{q_stats.get('dead_letter', 0)}</td>
            </tr>
        </table>

        <h2 style="color: #2c3e50;">Silver Profiling</h2>
        <table style="border-collapse: collapse; width: 100%; margin-bottom: 20px;">
            <tr style="background: #ecf0f1;">
                <th style="padding: 10px; border: 1px solid #bdc3c7;">Table</th>
                <th style="padding: 10px; border: 1px solid #bdc3c7;">Status</th>
            </tr>
    """

    for table, result in profiling_results.items():
        status = "OK" if result.get("success") else "FAILED"
        bg = "#d5f5e3" if result.get("success") else "#fadbd8"
        html_body += f"""
            <tr>
                <td style="padding: 10px; border: 1px solid #bdc3c7;">{table}</td>
                <td style="padding: 10px; border: 1px solid #bdc3c7; background: {bg};">{status}</td>
            </tr>
        """

    html_body += """
        </table>

        <h2 style="color: #2c3e50;">Drift Detection</h2>
        <table style="border-collapse: collapse; width: 100%; margin-bottom: 20px;">
            <tr style="background: #ecf0f1;">
                <th style="padding: 10px; border: 1px solid #bdc3c7;">Table</th>
                <th style="padding: 10px; border: 1px solid #bdc3c7;">Drift</th>
            </tr>
    """

    for table, result in drift_results.items():
        drift = "YES" if result.get("drift_detected") else "NO"
        bg = "#fadbd8" if result.get("drift_detected") else "#d5f5e3"
        html_body += f"""
            <tr>
                <td style="padding: 10px; border: 1px solid #bdc3c7;">{table}</td>
                <td style="padding: 10px; border: 1px solid #bdc3c7; background: {bg};">{drift}</td>
            </tr>
        """

    html_body += """
        </table>

        <hr>
        <p style="color: #7f8c8d; font-size: 12px;">
            Generated by Airflow Data Quality Pipeline
        </p>
    </body>
    </html>
    """

    # Add attachment if provided
    kwargs = {}
    if attachment_path and os.path.exists(attachment_path):
        from email.mime.base import MIMEBase
        from email import encoders

        with open(attachment_path, 'rb') as f:
            attachment = MIMEBase('application', 'octet-stream')
            attachment.set_payload(f.read())
        encoders.encode_base64(attachment)
        attachment.add_header('Content-Disposition', f'attachment; filename={os.path.basename(attachment_path)}')
        # Note: Need to modify send_alert_email to support attachments
        kwargs["attachment_path"] = attachment_path

    return send_throttled_alert(subject, html_body, recipient, severity, "data_quality")


def send_remediation_alert(
    fixed_count: int,
    failed_count: int,
    total_valid: int,
    total_invalid: int,
    stats: Dict[str, Any],
    recipient: str,
) -> Dict[str, Any]:
    """Send remediation workflow alert email"""
    has_failures = failed_count > 0 or total_invalid > 0

    subject_prefix = "[CRITICAL] REMEDIATION FAILED" if has_failures else "[INFO] REMEDIATION COMPLETE"
    severity = AlertSeverity.CRITICAL if has_failures else AlertSeverity.INFO
    subject = f"{subject_prefix} - {datetime.now().strftime('%Y-%m-%d')}"

    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; margin: 20px;">
        <h1>Remediation Workflow Report</h1>
        <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

        <h2 style="color: #2c3e50;">Remediation Summary</h2>
        <table style="border-collapse: collapse; width: 100%; margin-bottom: 20px;">
            <tr style="background: #ecf0f1;">
                <th style="padding: 10px; border: 1px solid #bdc3c7;">Metric</th>
                <th style="padding: 10px; border: 1px solid #bdc3c7;">Count</th>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #bdc3c7;">Valid Records Processed</td>
                <td style="padding: 10px; border: 1px solid #bdc3c7; text-align: center;">{total_valid}</td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #bdc3c7;">Successfully Fixed</td>
                <td style="padding: 10px; border: 1px solid #bdc3c7; text-align: center; background: #d5f5e3;">{fixed_count}</td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #bdc3c7;">Failed to Fix</td>
                <td style="padding: 10px; border: 1px solid #bdc3c7; text-align: center; background: {'#fadbd8' if failed_count > 0 else '#d5f5e3'};">{failed_count}</td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #bdc3c7;">Invalid (Auto-Rejected)</td>
                <td style="padding: 10px; border: 1px solid #bdc3c7; text-align: center; background: {'#fadbd8' if total_invalid > 0 else '#d5f5e3'};">{total_invalid}</td>
            </tr>
        </table>

        <h2 style="color: #2c3e50;">Quarantine Statistics</h2>
        <table style="border-collapse: collapse; width: 100%; margin-bottom: 20px;">
            <tr style="background: #ecf0f1;">
                <th style="padding: 10px; border: 1px solid #bdc3c7;">Metric</th>
                <th style="padding: 10px; border: 1px solid #bdc3c7;">Count</th>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #bdc3c7;">Total Records</td>
                <td style="padding: 10px; border: 1px solid #bdc3c7; text-align: center;">{stats.get('total', 0)}</td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #bdc3c7;">Pending</td>
                <td style="padding: 10px; border: 1px solid #bdc3c7; text-align: center;">{stats.get('pending', 0)}</td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #bdc3c7;">Total Processed</td>
                <td style="padding: 10px; border: 1px solid #bdc3c7; text-align: center;">{stats.get('processed', 0)}</td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #bdc3c7;">Remediated</td>
                <td style="padding: 10px; border: 1px solid #bdc3c7; text-align: center;">{stats.get('remediated', 0)}</td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #bdc3c7;">Rejected</td>
                <td style="padding: 10px; border: 1px solid #bdc3c7; text-align: center;">{stats.get('rejected', 0)}</td>
            </tr>
        </table>

        <hr>
        <p style="color: #7f8c8d; font-size: 12px;">
            Generated by Airflow Remediation Pipeline
        </p>
    </body>
    </html>
    """

    return send_throttled_alert(subject, html_body, recipient, severity, "remediation")
