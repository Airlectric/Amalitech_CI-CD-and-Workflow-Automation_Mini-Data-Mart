"""
Remediation DAG for fixing quarantined records
Production-grade implementation with proper error handling, logging, and retry logic
"""
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
send_remediation_alert = email_utils.send_remediation_alert

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
    "retry_delay": 300,
}

with DAG(
    dag_id="remediation_workflow",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["remediation", "quarantine", "data-quality"],
    description="Fix and replay quarantined records to silver",
) as dag:

    @task
    def get_quarantined_records():
        """Get all unremediated quarantined records"""
        from utils.postgres_hook import PostgresLayerHook
        import logging
        
        logger = logging.getLogger(__name__)
        logger.info("Fetching quarantined records")
        
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
        
        logger.info(f"Found {len(records)} quarantined records")
        return records

    @task
    def validate_records(records):
        """Validate records before fixing"""
        import json
        import logging
        
        logger = logging.getLogger(__name__)
        logger.info(f"Validating {len(records)} records")
        
        valid_records = []
        invalid_records = []
        
        required_fields = [
            "transaction_id", "sale_date", "product_id", "quantity", 
            "unit_price", "customer_id"
        ]
        
        for record in records:
            try:
                payload = record["payload"]
                
                if isinstance(payload, str):
                    payload = json.loads(payload)
                
                missing_fields = [f for f in required_fields if not payload.get(f)]
                
                if missing_fields:
                    invalid_records.append({
                        "id": record["id"],
                        "reason": f"Missing fields: {missing_fields}"
                    })
                    continue
                
                if payload.get("quantity", 0) <= 0:
                    invalid_records.append({
                        "id": record["id"],
                        "reason": "Invalid quantity"
                    })
                    continue
                
                if payload.get("unit_price", 0) < 0:
                    invalid_records.append({
                        "id": record["id"],
                        "reason": "Invalid unit_price"
                    })
                    continue
                
                valid_records.append(record)
                
            except json.JSONDecodeError as e:
                invalid_records.append({
                    "id": record["id"],
                    "reason": f"JSON decode error: {str(e)}"
                })
            except Exception as e:
                invalid_records.append({
                    "id": record["id"],
                    "reason": f"Validation error: {str(e)}"
                })
        
        logger.info(f"Validation complete: {len(valid_records)} valid, {len(invalid_records)} invalid")
        
        return {
            "valid": valid_records,
            "invalid": invalid_records
        }

    @task
    def fix_and_replay(validation_result):
        """Fix valid records and replay to silver"""
        import json
        import logging
        
        logger = logging.getLogger(__name__)
        
        valid_records = validation_result.get("valid", [])
        
        if not valid_records:
            logger.info("No valid records to fix")
            return {
                "fixed": 0, 
                "failed": 0, 
                "message": "No valid records to fix"
            }
        
        from utils.postgres_hook import PostgresLayerHook
        pg_hook = PostgresLayerHook()
        
        fixed_count = 0
        failed_count = 0
        errors = []
        
        for record in valid_records:
            try:
                payload = record["payload"]
                
                if isinstance(payload, str):
                    payload = json.loads(payload)
                
                discount_pct = float(payload.get("discount_percentage", 0))
                unit_price = float(payload.get("unit_price", 0))
                quantity = int(payload.get("quantity", 0))
                discount_amount = round(unit_price * quantity * (discount_pct / 100), 2)
                gross_amount = round(unit_price * quantity, 2)
                net_amount = round(gross_amount - discount_amount, 2)
                
                query = """
                    INSERT INTO silver.sales (
                        transaction_id, sale_date, sale_hour, customer_id, customer_name,
                        product_id, product_name, category, sub_category,
                        quantity, unit_price, discount_percentage, discount_amount,
                        gross_amount, net_amount, profit_margin,
                        payment_method, payment_category, store_location, region,
                        is_weekend, is_holiday, ingest_date, source_file, processed_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                    )
                    ON CONFLICT (transaction_id) DO UPDATE SET
                        sale_date = EXCLUDED.sale_date,
                        quantity = EXCLUDED.quantity,
                        unit_price = EXCLUDED.unit_price,
                        discount_percentage = EXCLUDED.discount_percentage,
                        discount_amount = EXCLUDED.discount_amount,
                        gross_amount = EXCLUDED.gross_amount,
                        net_amount = EXCLUDED.net_amount,
                        processed_at = NOW()
                """
                
                pg_hook.execute_query(query, parameters=(
                    payload.get("transaction_id"),
                    payload.get("sale_date"),
                    payload.get("sale_hour", 0),
                    payload.get("customer_id"),
                    payload.get("customer_name", ""),
                    payload.get("product_id"),
                    payload.get("product_name", ""),
                    payload.get("category", ""),
                    payload.get("sub_category", ""),
                    quantity,
                    unit_price,
                    discount_pct,
                    discount_amount,
                    gross_amount,
                    net_amount,
                    payload.get("profit_margin", 0),
                    payload.get("payment_method", ""),
                    payload.get("payment_category", ""),
                    payload.get("store_location", ""),
                    payload.get("region", ""),
                    payload.get("is_weekend", False),
                    payload.get("is_holiday", False),
                    payload.get("ingest_date", datetime.now().date()),
                    payload.get("source_file", "")
                ))
                
                update_query = """
                    UPDATE quarantine.sales_failed
                    SET replayed = TRUE, 
                        replayed_at = NOW(),
                        corrected_by = 'remediation_dag'
                    WHERE id = %s
                """
                pg_hook.execute_query(update_query, parameters=(record["id"],))
                
                fixed_count += 1
                logger.debug(f"Fixed record {record['id']}")
                
            except Exception as e:
                failed_count += 1
                error_msg = f"Error fixing record {record['id']}: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)
        
        result = {
            "fixed": fixed_count,
            "failed": failed_count,
            "total": len(valid_records),
            "errors": errors[:10]
        }
        
        logger.info(f"Remediation complete: {fixed_count} fixed, {failed_count} failed")
        return result

    @task
    def mark_invalid_records(validation_result):
        """Mark invalid records as reviewed (not fixable)"""
        import logging
        
        logger = logging.getLogger(__name__)
        
        invalid_records = validation_result.get("invalid", [])
        
        if not invalid_records:
            return {"marked": 0}
        
        from utils.postgres_hook import PostgresLayerHook
        pg_hook = PostgresLayerHook()
        
        marked_count = 0
        for record in invalid_records:
            try:
                query = """
                    UPDATE quarantine.sales_failed
                    SET replayed = TRUE,
                        replayed_at = NOW(),
                        corrected_by = 'auto_rejected',
                        error_reason = CONCAT(error_reason, ' | Auto-rejected: ', %s)
                    WHERE id = %s
                """
                pg_hook.execute_query(query, parameters=(record["reason"], record["id"]))
                marked_count += 1
            except Exception as e:
                logger.error(f"Error marking record {record['id']}: {e}")
        
        logger.info(f"Marked {marked_count} invalid records")
        return {"marked": marked_count}

    @task
    def get_quarantine_stats():
        """Get current quarantine statistics"""
        from utils.postgres_hook import PostgresLayerHook
        
        pg_hook = PostgresLayerHook()
        
        query = """
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN replayed = FALSE THEN 1 END) as pending,
                COUNT(CASE WHEN replayed = TRUE THEN 1 END) as processed,
                COUNT(CASE WHEN replayed = TRUE AND corrected_by = 'remediation_dag' THEN 1 END) as remediated,
                COUNT(CASE WHEN replayed = TRUE AND corrected_by = 'auto_rejected' THEN 1 END) as rejected
            FROM quarantine.sales_failed
        """
        result = pg_hook.execute_query(query)
        
        if result[1]:
            row = result[1][0]
            return {
                "total": row[0],
                "pending": row[1],
                "processed": row[2],
                "remediated": row[3],
                "rejected": row[4]
            }
        
        return {"total": 0, "pending": 0, "processed": 0, "remediated": 0, "rejected": 0}

    @task
    def send_remediation_notification(fix_result, validation_result, stats):
        """Send notification on completion"""
        import logging
        
        logger = logging.getLogger(__name__)
        
        valid_records = validation_result.get("valid", [])
        invalid_records = validation_result.get("invalid", [])
        
        result = send_remediation_alert(
            fixed_count=fix_result.get("fixed", 0),
            failed_count=fix_result.get("failed", 0),
            total_valid=len(valid_records),
            total_invalid=len(invalid_records),
            stats=stats,
            recipient=ALERT_EMAIL
        )
        
        logger.info(f"Remediation notification sent: {result}")
        return result

    # Task dependencies
    records = get_quarantined_records()
    validation_result = validate_records(records)
    fix_result = fix_and_replay(validation_result)
    marked_result = mark_invalid_records(validation_result)
    stats = get_quarantine_stats()
    
    send_remediation_notification(fix_result, validation_result, stats)
