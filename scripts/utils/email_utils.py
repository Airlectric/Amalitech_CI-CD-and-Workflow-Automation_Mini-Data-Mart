"""
Central Email Utility for Airflow DAGs
Provides a reusable function to send email alerts via Gmail SMTP
"""
import os
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, List, Dict, Any
from datetime import datetime


logger = logging.getLogger(__name__)

SMTP_HOST = os.getenv("AIRFLOW__SMTP__SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("AIRFLOW__SMTP__SMTP_PORT", "587"))
SENDER_EMAIL = os.getenv("AIRFLOW__SMTP__SMTP_USER", "agudeydaniel8@gmail.com")
SENDER_PASSWORD = os.getenv("AIRFLOW__SMTP__SMTP_PASSWORD", "viavvmwhinhjyfax")


def send_alert_email(
    subject: str,
    html_body: str,
    recipient: str,
    sender: str = SENDER_EMAIL,
    password: str = SENDER_PASSWORD,
    smtp_host: str = SMTP_HOST,
    smtp_port: int = SMTP_PORT,
    attachment_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Send an HTML email alert
    
    Args:
        subject: Email subject line
        html_body: HTML content of the email
        recipient: Recipient email address
        sender: Sender email address
        password: App password for Gmail SMTP
        smtp_host: SMTP server hostname
        smtp_port: SMTP server port
    
    Returns:
        Dict with status and any error message
    """
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = recipient
        
        part = MIMEText(html_body, 'html')
        msg.attach(part)
        
        if attachment_path and os.path.exists(attachment_path):
            from email.mime.base import MIMEBase
            from email import encoders
            
            with open(attachment_path, 'rb') as f:
                attachment = MIMEBase('application', 'octet-stream')
                attachment.set_payload(f.read())
            encoders.encode_base64(attachment)
            attachment.add_header('Content-Disposition', f'attachment; filename={os.path.basename(attachment_path)}')
            msg.attach(attachment)
            logger.info(f"Attached file: {attachment_path}")
        
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(sender, password)
            server.send_message(msg)
        
        logger.info(f"Alert email sent to {recipient}: {subject}")
        return {"status": "sent", "recipient": recipient, "subject": subject}
    
    except Exception as e:
        logger.error(f"Failed to send email to {recipient}: {e}")
        return {"status": "failed", "error": str(e), "recipient": recipient}


def send_ingestion_alert(
    run_id: str,
    total_read: int,
    silver_count: int,
    quarantine_count: int,
    files_scanned: List[str],
    errors: List[str],
    recipient: str,
    password: str = SENDER_PASSWORD
) -> Dict[str, Any]:
    """
    Send ingestion pipeline alert email
    
    Args:
        run_id: The ingestion run ID
        total_read: Total rows read
        silver_count: Rows written to Silver
        quarantine_count: Rows sent to Quarantine
        files_scanned: List of files processed
        errors: List of error messages
        recipient: Recipient email
        password: SMTP password
    
    Returns:
        Email send result
    """
    quarantine_rate = (quarantine_count / total_read * 100) if total_read > 0 else 0
    
    # Determine status based on actual processing
    no_data_processed = total_read == 0 and len(files_scanned) == 0
    has_errors = len(errors) > 0
    high_quarantine = quarantine_rate > 50
    
    if no_data_processed:
        subject_prefix = "ℹ️ SKIPPED"
        status_color = "#718096"
        status_text = "NO DATA - All files already processed"
    elif has_errors:
        subject_prefix = "🔴 CRITICAL"
        status_color = "#c53030"
        status_text = "FAILED"
    elif high_quarantine:
        subject_prefix = "🟡 WARNING"
        status_color = "#d69e2e"
        status_text = "HIGH QUARANTINE"
    elif quarantine_count > 0:
        subject_prefix = "🟡 PARTIAL"
        status_color = "#d69e2e"
        status_text = "PARTIAL SUCCESS"
    else:
        subject_prefix = "✅ SUCCESS"
        status_color = "#38a169"
        status_text = "SUCCESS"
    
    subject = f"{subject_prefix} Ingestion Alert - {run_id[:8]}"
    
    files_html = "".join([f"<li><code>{f}</code></li>" for f in files_scanned[:10]]) if files_scanned else "<li>No files to process</li>"
    errors_html = "".join([f"<li><code>{e}</code></li>" for e in errors[:5]]) if errors else "<li>No errors</li>"
    
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
    
    return send_alert_email(subject, html_body, recipient, password=password)


def send_data_quality_alert(
    quarantine_stats: Dict[str, Any],
    profiling_results: Dict[str, Any],
    drift_results: Dict[str, Any],
    recipient: str,
    password: str = SENDER_PASSWORD,
    attachment_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Send data quality GX alert email
    
    Args:
        quarantine_stats: Quarantine table statistics
        profiling_results: Silver table profiling results
        drift_results: Drift detection results
        recipient: Recipient email
        password: SMTP password
        attachment_path: Path to PDF report attachment
    
    Returns:
        Email send result
    """
    has_issues = (
        quarantine_stats.get("pending", 0) > 0 or
        any(r.get("drift_detected", False) for r in drift_results.values()) or
        any(not r.get("success", True) for r in profiling_results.values())
    )
    
    subject_prefix = "🔴 ALERT" if has_issues else "✅ OK"
    subject = f"{subject_prefix} Data Quality Report - {datetime.now().strftime('%Y-%m-%d')}"
    
    q_stats = quarantine_stats.get("stats", {})
    pending = quarantine_stats.get("pending", 0)
    replayed = quarantine_stats.get("replayed", 0)
    
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
                <td style="padding: 10px; border: 1px solid #bdc3c7; text-align: center;">{q_stats.get('total_quarantined', 0)}</td>
                <td style="padding: 10px; border: 1px solid #bdc3c7; text-align: center; background: {'#fadbd8' if pending > 0 else '#d5f5e3'};">{pending}</td>
                <td style="padding: 10px; border: 1px solid #bdc3c7; text-align: center;">{replayed}</td>
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
        status = "✓ OK" if result.get("success") else "✗ FAILED"
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
        drift = "⚠ YES" if result.get("drift_detected") else "✓ NO"
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
            Generated by Great Expectations Data Quality Pipeline
        </p>
    </body>
    </html>
    """
    
    return send_alert_email(subject, html_body, recipient, password=password, attachment_path=attachment_path)


