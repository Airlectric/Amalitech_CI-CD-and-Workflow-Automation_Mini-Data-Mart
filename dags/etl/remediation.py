"""
Remediation DAG for fixing quarantined records
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
}

ALERT_EMAIL = Variable.get("alert_email", default_var="daniel.doe@a2sv.org")

with DAG(
    dag_id="remediation_workflow",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args=default_args,
    catchup=False,
    description="Fix and replay quarantined records to silver",
) as dag:

    def get_quarantined_records(**context):
        """Get all unremediated quarantined records"""
        from dags.utils.postgres_hook import PostgresLayerHook
        
        pg_hook = PostgresLayerHook()
        
        query = """
            SELECT id, payload, error_reason, source_file, failed_at
            FROM quarantine.sales_failed
            WHERE replayed = FALSE
            ORDER BY failed_at DESC
            LIMIT 1000
        """
        result = pg_hook.execute_query(query)
        
        records = []
        if result[1]:
            for row in result[1]:
                records.append({
                    "id": row[0],
                    "payload": row[1],
                    "error_reason": row[2],
                    "source_file": row[3],
                    "failed_at": row[4]
                })
        
        return records

    def fix_and_replay(**context):
        """Fix records and replay to silver"""
        from dags.utils.postgres_hook import PostgresLayerHook
        import json
        
        ti = context['ti']
        records = ti.xcom_pull(task_ids='get_quarantined_records')
        
        if not records:
            return {"fixed": 0, "message": "No records to fix"}
        
        pg_hook = PostgresLayerHook()
        fixed_count = 0
        
        for record in records:
            try:
                payload = record["payload"]
                
                if isinstance(payload, str):
                    payload = json.loads(payload)
                
                pg_hook.execute_query(f"""
                    INSERT INTO silver.sales (transaction_id, sale_date, product_id, quantity, unit_price, total_amount, store_id, customer_id, payment_method, is_weekend, is_holiday, ingest_date)
                    VALUES (
                        '{payload.get('transaction_id')}',
                        '{payload.get('sale_date')}',
                        '{payload.get('product_id')}',
                        {payload.get('quantity', 0)},
                        {payload.get('unit_price', 0)},
                        {payload.get('total_amount', 0)},
                        '{payload.get('store_id')}',
                        '{payload.get('customer_id')}',
                        '{payload.get('payment_method')}',
                        {payload.get('is_weekend', False)},
                        {payload.get('is_holiday', False)},
                        CURRENT_DATE
                    )
                    ON CONFLICT (transaction_id) DO UPDATE SET
                        sale_date = EXCLUDED.sale_date,
                        quantity = EXCLUDED.quantity,
                        unit_price = EXCLUDED.unit_price,
                        total_amount = EXCLUDED.total_amount
                """)
                
                pg_hook.execute_query(f"""
                    UPDATE quarantine.sales_failed
                    SET replayed = TRUE, 
                        replayed_at = NOW(),
                        corrected_by = 'remediation_dag'
                    WHERE id = {record['id']}
                """)
                
                fixed_count += 1
            except Exception as e:
                print(f"Error fixing record {record['id']}: {e}")
        
        return {"fixed": fixed_count, "total": len(records)}

    def notify_completion(**context):
        """Send notification on completion"""
        from dags.utils.postgres_hook import PostgresLayerHook
        import smtplib
        from email.mime.text import MIMEText
        
        ti = context['ti']
        result = ti.xcom_pull(task_ids='fix_and_replay') or {}
        
        pg_hook = PostgresLayerHook()
        
        pending_query = """
            SELECT COUNT(*) FROM quarantine.sales_failed WHERE replayed = FALSE
        """
        pending_result = pg_hook.execute_query(pending_query)
        pending_count = pending_result[1][0][0] if pending_result[1] else 0
        
        subject = f"Remediation Complete: {result.get('fixed', 0)} records fixed"
        body = f"""
        <h2>Remediation Workflow Completed</h2>
        <p><strong>Fixed:</strong> {result.get('fixed', 0)} records</p>
        <p><strong>Total:</strong> {result.get('total', 0)} records</p>
        <p><strong>Remaining in Quarantine:</strong> {pending_count}</p>
        """
        
        try:
            msg = MIMEText(body, 'html')
            msg['Subject'] = subject
            msg['From'] = 'airflow@amalitech.local'
            msg['To'] = ALERT_EMAIL
            
            with smtplib.SMTP('smtp.gmail.com', 587) as server:
                server.starttls()
                server.login('daniel.agudey.doe@gmail.com', 'your_app_password')
                server.send_message(msg)
        except Exception as e:
            print(f"Email notification failed: {e}")
        
        return result

    get_records = PythonOperator(
        task_id="get_quarantined_records",
        python_callable=get_quarantined_records
    )

    fix_records = PythonOperator(
        task_id="fix_and_replay",
        python_callable=fix_and_replay
    )

    notify = PythonOperator(
        task_id="notify_completion",
        python_callable=notify_completion
    )

    get_records >> fix_records >> notify
