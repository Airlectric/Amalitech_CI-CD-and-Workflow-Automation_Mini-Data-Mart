from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable

ALERT_EMAIL = Variable.get("alert_email", default_var="daniel.doe@a2sv.org")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "email": [ALERT_EMAIL],
}

with DAG(
    dag_id="test_email_alert",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["test", "email"],
    description="Test DAG to verify email alerts work"
) as dag:

    def always_fail():
        raise ValueError("This is a test failure to verify email alerts are working!")

    fail_task = PythonOperator(
        task_id="trigger_failure",
        python_callable=always_fail
    )

    send_alert = EmailOperator(
        task_id="send_test_email",
        to=ALERT_EMAIL,
        subject="TEST: Email Alert from Airflow",
        html_content="""
            <h2>Test Email</h2>
            <p>This is a test email from Airflow to verify the email alerting system is working.</p>
            <p>Timestamp: {{ ds }}</p>
        """
    )

    fail_task >> send_alert
