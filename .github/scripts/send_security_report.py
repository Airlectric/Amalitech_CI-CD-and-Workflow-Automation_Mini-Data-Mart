"""
Send security scan report via Gmail SMTP.
Used by the CI/CD pipeline to email bandit results.
"""

import json
import os
import smtplib
import sys
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def load_bandit_report(path: str) -> dict:
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Warning: Could not load bandit report: {e}")
        return {"results": [], "errors": [], "metrics": {}}


def build_html(report: dict, repo: str, sha: str, run_url: str) -> str:
    results = report.get("results", [])
    metrics = report.get("metrics", {})
    totals = metrics.get("_totals", {})

    severity_high = sum(1 for r in results if r.get("issue_severity") == "HIGH")
    severity_medium = sum(1 for r in results if r.get("issue_severity") == "MEDIUM")
    severity_low = sum(1 for r in results if r.get("issue_severity") == "LOW")
    total_issues = len(results)

    if severity_high > 0:
        status_color = "#e74c3c"
        status_text = "HIGH SEVERITY ISSUES FOUND"
        subject_prefix = "[CRITICAL]"
    elif severity_medium > 0:
        status_color = "#f39c12"
        status_text = "MEDIUM SEVERITY ISSUES FOUND"
        subject_prefix = "[WARNING]"
    elif total_issues > 0:
        status_color = "#3498db"
        status_text = "LOW SEVERITY ISSUES FOUND"
        subject_prefix = "[INFO]"
    else:
        status_color = "#27ae60"
        status_text = "NO ISSUES FOUND"
        subject_prefix = "[INFO]"

    subject = f"{subject_prefix} Security Scan Report - {repo} ({sha[:7]})"

    # Build findings table rows
    findings_html = ""
    if results:
        for r in results[:20]:  # Limit to 20 findings
            sev = r.get("issue_severity", "UNKNOWN")
            sev_color = {"HIGH": "#e74c3c", "MEDIUM": "#f39c12", "LOW": "#3498db"}.get(sev, "#95a5a6")
            findings_html += f"""
            <tr>
                <td style="padding: 8px; border: 1px solid #ddd;">{r.get("test_id", "")}</td>
                <td style="padding: 8px; border: 1px solid #ddd; color: {sev_color}; font-weight: bold;">{sev}</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{r.get("issue_text", "")}</td>
                <td style="padding: 8px; border: 1px solid #ddd;"><code>{r.get("filename", "")}:{r.get("line_number", "")}</code></td>
            </tr>"""
        if len(results) > 20:
            findings_html += f"""
            <tr>
                <td colspan="4" style="padding: 8px; border: 1px solid #ddd; text-align: center; color: #7f8c8d;">
                    ... and {len(results) - 20} more findings. See full report in artifacts.
                </td>
            </tr>"""
    else:
        findings_html = """
        <tr>
            <td colspan="4" style="padding: 12px; border: 1px solid #ddd; text-align: center; color: #27ae60;">
                No security issues detected.
            </td>
        </tr>"""

    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; margin: 20px; color: #2c3e50;">
        <h2 style="color: {status_color};">Security Scan: {status_text}</h2>

        <table style="border-collapse: collapse; width: 100%; margin: 15px 0;">
            <tr style="background: #ecf0f1;">
                <th style="padding: 10px; border: 1px solid #ddd;">Detail</th>
                <th style="padding: 10px; border: 1px solid #ddd;">Value</th>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #ddd;">Repository</td>
                <td style="padding: 10px; border: 1px solid #ddd;"><code>{repo}</code></td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #ddd;">Commit</td>
                <td style="padding: 10px; border: 1px solid #ddd;"><code>{sha[:7]}</code></td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #ddd;">Scan Time</td>
                <td style="padding: 10px; border: 1px solid #ddd;">{datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}</td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #ddd;">Lines Scanned</td>
                <td style="padding: 10px; border: 1px solid #ddd;">{totals.get("loc", "N/A"):,}</td>
            </tr>
        </table>

        <h3>Issue Summary</h3>
        <table style="border-collapse: collapse; width: 100%; margin: 15px 0;">
            <tr style="background: #ecf0f1;">
                <th style="padding: 10px; border: 1px solid #ddd;">Severity</th>
                <th style="padding: 10px; border: 1px solid #ddd;">Count</th>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #ddd; color: #e74c3c; font-weight: bold;">High</td>
                <td style="padding: 10px; border: 1px solid #ddd; text-align: center; background: {"#fadbd8" if severity_high > 0 else "#d5f5e3"};">{severity_high}</td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #ddd; color: #f39c12; font-weight: bold;">Medium</td>
                <td style="padding: 10px; border: 1px solid #ddd; text-align: center; background: {"#fdebd0" if severity_medium > 0 else "#d5f5e3"};">{severity_medium}</td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #ddd; color: #3498db; font-weight: bold;">Low</td>
                <td style="padding: 10px; border: 1px solid #ddd; text-align: center;">{severity_low}</td>
            </tr>
        </table>

        <h3>Findings</h3>
        <table style="border-collapse: collapse; width: 100%; margin: 15px 0;">
            <tr style="background: #ecf0f1;">
                <th style="padding: 8px; border: 1px solid #ddd;">Test ID</th>
                <th style="padding: 8px; border: 1px solid #ddd;">Severity</th>
                <th style="padding: 8px; border: 1px solid #ddd;">Issue</th>
                <th style="padding: 8px; border: 1px solid #ddd;">Location</th>
            </tr>
            {findings_html}
        </table>

        <p style="margin-top: 20px;">
            <a href="{run_url}" style="color: #3498db;">View full CI/CD run</a>
        </p>

        <hr>
        <p style="color: #7f8c8d; font-size: 12px;">
            Sent by CI/CD Pipeline — Bandit Security Scanner
        </p>
    </body>
    </html>
    """
    return subject, html


def send_email(subject: str, html_body: str, smtp_user: str, smtp_password: str, recipient: str):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = recipient
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)

    print(f"Security report emailed to {recipient}")


def main():
    report_path = os.environ.get("REPORT_PATH", "bandit-report.json")
    smtp_user = os.environ["SMTP_USER"]
    smtp_password = os.environ["SMTP_PASSWORD"]
    recipient = os.environ.get("RECIPIENT", smtp_user)
    repo = os.environ.get("GITHUB_REPOSITORY", "unknown/repo")
    sha = os.environ.get("GITHUB_SHA", "unknown")
    run_id = os.environ.get("GITHUB_RUN_ID", "0")
    server_url = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    run_url = f"{server_url}/{repo}/actions/runs/{run_id}"

    report = load_bandit_report(report_path)
    subject, html = build_html(report, repo, sha, run_url)
    send_email(subject, html, smtp_user, smtp_password, recipient)


if __name__ == "__main__":
    main()
