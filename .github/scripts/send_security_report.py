"""
Send security scan report via Gmail SMTP.
Combines bandit (source code) and pip-audit (dependency) results into one email.
"""

import json
import os
import smtplib
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def load_json_report(path: str) -> dict | list:
    if not path:
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Warning: Could not load {path}: {e}")
        return {}


def build_bandit_section(report: dict) -> tuple[str, int, int, int]:
    results = report.get("results", [])
    metrics = report.get("metrics", {})
    totals = metrics.get("_totals", {})

    high = sum(1 for r in results if r.get("issue_severity") == "HIGH")
    medium = sum(1 for r in results if r.get("issue_severity") == "MEDIUM")
    low = sum(1 for r in results if r.get("issue_severity") == "LOW")

    findings_html = ""
    if results:
        for r in results[:20]:
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
                    ... and {len(results) - 20} more. See full report in artifacts.
                </td>
            </tr>"""
    else:
        findings_html = """
        <tr>
            <td colspan="4" style="padding: 12px; border: 1px solid #ddd; text-align: center; color: #27ae60;">
                No source code security issues detected.
            </td>
        </tr>"""

    html = f"""
        <h2 style="color: #2c3e50;">Source Code Scan (Bandit)</h2>
        <p>Lines scanned: {totals.get("loc", "N/A"):,}</p>
        <table style="border-collapse: collapse; width: 100%; margin: 10px 0;">
            <tr style="background: #ecf0f1;">
                <th style="padding: 8px; border: 1px solid #ddd;">Severity</th>
                <th style="padding: 8px; border: 1px solid #ddd;">Count</th>
            </tr>
            <tr>
                <td style="padding: 8px; border: 1px solid #ddd; color: #e74c3c; font-weight: bold;">High</td>
                <td style="padding: 8px; border: 1px solid #ddd; text-align: center; background: {"#fadbd8" if high > 0 else "#d5f5e3"};">{high}</td>
            </tr>
            <tr>
                <td style="padding: 8px; border: 1px solid #ddd; color: #f39c12; font-weight: bold;">Medium</td>
                <td style="padding: 8px; border: 1px solid #ddd; text-align: center; background: {"#fdebd0" if medium > 0 else "#d5f5e3"};">{medium}</td>
            </tr>
            <tr>
                <td style="padding: 8px; border: 1px solid #ddd; color: #3498db; font-weight: bold;">Low</td>
                <td style="padding: 8px; border: 1px solid #ddd; text-align: center;">{low}</td>
            </tr>
        </table>

        <h3>Findings</h3>
        <table style="border-collapse: collapse; width: 100%; margin: 10px 0;">
            <tr style="background: #ecf0f1;">
                <th style="padding: 8px; border: 1px solid #ddd;">Test ID</th>
                <th style="padding: 8px; border: 1px solid #ddd;">Severity</th>
                <th style="padding: 8px; border: 1px solid #ddd;">Issue</th>
                <th style="padding: 8px; border: 1px solid #ddd;">Location</th>
            </tr>
            {findings_html}
        </table>
    """
    return html, high, medium, low


def build_pip_audit_section(report: dict | list) -> tuple[str, int]:
    # pip-audit JSON output is a dict with "dependencies" key (newer versions)
    # or a list of vulnerability objects
    vulnerabilities = []

    if isinstance(report, dict):
        deps = report.get("dependencies", [])
        for dep in deps:
            for vuln in dep.get("vulns", []):
                vulnerabilities.append({
                    "name": dep.get("name", ""),
                    "version": dep.get("version", ""),
                    "id": vuln.get("id", ""),
                    "fix_versions": ", ".join(vuln.get("fix_versions", [])),
                    "description": vuln.get("description", ""),
                })
    elif isinstance(report, list):
        vulnerabilities = report

    vuln_count = len(vulnerabilities)

    if not vulnerabilities:
        html = """
        <h2 style="color: #2c3e50;">Dependency Scan (pip-audit)</h2>
        <p style="color: #27ae60; font-weight: bold;">No known vulnerabilities in dependencies.</p>
        """
        return html, 0

    rows = ""
    for v in vulnerabilities[:30]:
        rows += f"""
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">{v.get("name", "")}</td>
            <td style="padding: 8px; border: 1px solid #ddd;">{v.get("version", "")}</td>
            <td style="padding: 8px; border: 1px solid #ddd;"><code>{v.get("id", "")}</code></td>
            <td style="padding: 8px; border: 1px solid #ddd; color: #27ae60;">{v.get("fix_versions", "N/A")}</td>
        </tr>"""

    if len(vulnerabilities) > 30:
        rows += f"""
        <tr>
            <td colspan="4" style="padding: 8px; border: 1px solid #ddd; text-align: center; color: #7f8c8d;">
                ... and {len(vulnerabilities) - 30} more. See full report in artifacts.
            </td>
        </tr>"""

    html = f"""
        <h2 style="color: #2c3e50;">Dependency Scan (pip-audit)</h2>
        <p style="color: {"#e74c3c" if vuln_count > 0 else "#27ae60"}; font-weight: bold;">
            {vuln_count} known {"vulnerability" if vuln_count == 1 else "vulnerabilities"} found
        </p>
        <table style="border-collapse: collapse; width: 100%; margin: 10px 0;">
            <tr style="background: #ecf0f1;">
                <th style="padding: 8px; border: 1px solid #ddd;">Package</th>
                <th style="padding: 8px; border: 1px solid #ddd;">Version</th>
                <th style="padding: 8px; border: 1px solid #ddd;">CVE</th>
                <th style="padding: 8px; border: 1px solid #ddd;">Fix Version</th>
            </tr>
            {rows}
        </table>
    """
    return html, vuln_count


def build_email(bandit_report: dict, pip_audit_report: dict | list, repo: str, sha: str, run_url: str) -> tuple[str, str]:
    bandit_html, high, medium, low = build_bandit_section(bandit_report)
    pip_audit_html, vuln_count = build_pip_audit_section(pip_audit_report)

    # Determine overall status
    if high > 0 or vuln_count >= 5:
        status_color = "#e74c3c"
        status_text = "ISSUES FOUND"
        subject_prefix = "[CRITICAL]"
    elif medium > 0 or vuln_count > 0:
        status_color = "#f39c12"
        status_text = "WARNINGS"
        subject_prefix = "[WARNING]"
    else:
        status_color = "#27ae60"
        status_text = "ALL CLEAR"
        subject_prefix = "[INFO]"

    subject = f"{subject_prefix} Security Scan Report - {repo} ({sha[:7]})"

    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; margin: 20px; color: #2c3e50;">
        <h1 style="color: {status_color};">Security Scan: {status_text}</h1>

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
        </table>

        {bandit_html}

        <hr style="margin: 30px 0; border: 1px solid #ecf0f1;">

        {pip_audit_html}

        <hr style="margin: 30px 0;">
        <p>
            <a href="{run_url}" style="color: #3498db;">View full CI/CD run</a>
        </p>
        <p style="color: #7f8c8d; font-size: 12px;">
            Sent by CI/CD Pipeline — Bandit + pip-audit Security Scanner
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
    bandit_path = os.environ.get("BANDIT_REPORT_PATH", "bandit-report.json")
    pip_audit_path = os.environ.get("PIP_AUDIT_REPORT_PATH", "")
    smtp_user = os.environ["SMTP_USER"]
    smtp_password = os.environ["SMTP_PASSWORD"]
    recipient = os.environ.get("RECIPIENT", smtp_user)
    repo = os.environ.get("GITHUB_REPOSITORY", "unknown/repo")
    sha = os.environ.get("GITHUB_SHA", "unknown")
    run_id = os.environ.get("GITHUB_RUN_ID", "0")
    server_url = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    run_url = f"{server_url}/{repo}/actions/runs/{run_id}"

    bandit_report = load_json_report(bandit_path)
    pip_audit_report = load_json_report(pip_audit_path)

    subject, html = build_email(bandit_report, pip_audit_report, repo, sha, run_url)
    send_email(subject, html, smtp_user, smtp_password, recipient)


if __name__ == "__main__":
    main()
