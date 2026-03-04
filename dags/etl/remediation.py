"""
Remediation DAG for fixing quarantined records.
Production-grade: batch operations, transactional consistency, dead-letter escalation.
"""

import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utils.email_utils import send_remediation_alert

logger = logging.getLogger(__name__)

ALERT_EMAIL = Variable.get("alert_email", default_var="daniel.doe@a2sv.org")
MAX_RETRY_COUNT = int(Variable.get("remediation_max_retries", default_var="3"))
BATCH_SIZE = int(Variable.get("remediation_batch_size", default_var="1000"))

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
    def remediate_all_batches():
        """
        Process ALL pending quarantine records in batches.
        Uses batch upsert, transactional consistency, and dead-letter escalation.
        Returns cumulative results across all batches.
        """
        import logging

        logger = logging.getLogger(__name__)

        from utils.postgres_hook import PostgresLayerHook

        pg_hook = PostgresLayerHook()

        # Ensure new columns exist (migration for existing deployments)
        _ensure_schema_migration(pg_hook)

        total_fixed = 0
        total_failed = 0
        total_rejected = 0
        total_dead_lettered = 0
        all_errors = []
        batches_processed = 0

        while True:
            # Fetch next batch of pending records below max retries
            records = _fetch_pending_batch(pg_hook)

            if not records:
                logger.info("No more pending records to process")
                break

            batches_processed += 1
            logger.info(f"Processing batch {batches_processed}: {len(records)} records")

            # Validate records
            valid_records, invalid_records = _validate_records(records)

            # Batch replay valid records in a single transaction
            fixed, failed, errors = _batch_replay(pg_hook, valid_records)
            total_fixed += fixed
            total_failed += failed
            all_errors.extend(errors)

            # Batch reject invalid records
            rejected = _batch_reject(pg_hook, invalid_records)
            total_rejected += rejected

            # Escalate records that hit max retries to dead-letter
            dead_lettered = _escalate_dead_letters(pg_hook)
            total_dead_lettered += dead_lettered

            logger.info(
                f"Batch {batches_processed}: fixed={fixed}, failed={failed}, "
                f"rejected={rejected}, dead_lettered={dead_lettered}"
            )

        # Get final stats
        stats = _get_quarantine_stats(pg_hook)

        result = {
            "batches_processed": batches_processed,
            "fixed": total_fixed,
            "failed": total_failed,
            "rejected": total_rejected,
            "dead_lettered": total_dead_lettered,
            "errors": all_errors[:20],
            "stats": stats,
        }

        logger.info(f"Remediation complete: {result}")
        return result

    @task
    def send_remediation_notification(result):
        """Send notification with cumulative results."""
        import logging

        logger = logging.getLogger(__name__)

        notification = send_remediation_alert(
            fixed_count=result.get("fixed", 0),
            failed_count=result.get("failed", 0),
            total_valid=result.get("fixed", 0) + result.get("failed", 0),
            total_invalid=result.get("rejected", 0),
            stats=result.get("stats", {}),
            recipient=ALERT_EMAIL,
        )

        logger.info(f"Remediation notification sent: {notification}")
        return notification

    def _ensure_schema_migration(pg_hook):
        """Add new columns if missing (safe for existing deployments)."""
        pg_hook.execute_query("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'quarantine' AND table_name = 'sales_failed'
                    AND column_name = 'remediation_status'
                ) THEN
                    ALTER TABLE quarantine.sales_failed
                        ADD COLUMN remediation_status TEXT NOT NULL DEFAULT 'pending';
                END IF;

                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'quarantine' AND table_name = 'sales_failed'
                    AND column_name = 'retry_count'
                ) THEN
                    ALTER TABLE quarantine.sales_failed
                        ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0;
                END IF;
            END $$;
        """)
        # Sync legacy rows: replayed=TRUE without new status → mark as remediated
        pg_hook.execute_query("""
            UPDATE quarantine.sales_failed
            SET remediation_status = 'remediated'
            WHERE replayed = TRUE AND remediation_status = 'pending'
        """)

    def _fetch_pending_batch(pg_hook):
        """Fetch a batch of pending records that haven't exceeded max retries."""
        query = """
            SELECT id, ingestion_run_id, payload, error_reason, source_file, failed_at, retry_count
            FROM quarantine.sales_failed
            WHERE remediation_status = 'pending'
              AND retry_count < %s
            ORDER BY failed_at ASC
            LIMIT %s
        """
        result = pg_hook.execute_query(query, (MAX_RETRY_COUNT, BATCH_SIZE))

        records = []
        if result[1]:
            for row in result[1]:
                records.append(
                    {
                        "id": row[0],
                        "ingestion_run_id": str(row[1]),
                        "payload": row[2],
                        "error_reason": row[3],
                        "source_file": row[4],
                        "failed_at": row[5],
                        "retry_count": row[6],
                    }
                )
        return records

    def _validate_records(records):
        """Split records into valid (fixable) and invalid (unfixable)."""
        import json

        required_fields = ["transaction_id", "sale_date", "product_id", "quantity", "unit_price", "customer_id"]

        valid_records = []
        invalid_records = []

        for record in records:
            try:
                payload = record["payload"]
                if isinstance(payload, str):
                    payload = json.loads(payload)
                record["payload"] = payload

                missing = [f for f in required_fields if not payload.get(f)]
                if missing:
                    record["reject_reason"] = f"Missing fields: {missing}"
                    invalid_records.append(record)
                    continue

                if payload.get("quantity", 0) <= 0:
                    record["reject_reason"] = "Invalid quantity (<= 0)"
                    invalid_records.append(record)
                    continue

                if payload.get("unit_price", 0) < 0:
                    record["reject_reason"] = "Invalid unit_price (< 0)"
                    invalid_records.append(record)
                    continue

                valid_records.append(record)

            except (json.JSONDecodeError, Exception) as e:
                record["reject_reason"] = f"Parse error: {str(e)}"
                invalid_records.append(record)

        logger.info(f"Validation: {len(valid_records)} valid, {len(invalid_records)} invalid")
        return valid_records, invalid_records

    def _batch_replay(pg_hook, valid_records):
        """Batch upsert valid records to silver + update quarantine in one transaction."""
        if not valid_records:
            return 0, 0, []

        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        fixed = 0
        failed = 0
        errors = []

        try:
            # Build batch values for silver upsert
            silver_values = []
            quarantine_keys = []

            for record in valid_records:
                try:
                    payload = record["payload"]
                    discount_pct = float(payload.get("discount_percentage", 0))
                    unit_price = float(payload.get("unit_price", 0))
                    quantity = int(payload.get("quantity", 0))
                    discount_amount = round(unit_price * quantity * (discount_pct / 100), 2)
                    gross_amount = round(unit_price * quantity, 2)
                    net_amount = round(gross_amount - discount_amount, 2)

                    silver_values.append(
                        (
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
                            payload.get("source_file", ""),
                        )
                    )
                    quarantine_keys.append((record["ingestion_run_id"], record["id"]))
                except Exception as e:
                    failed += 1
                    errors.append(f"Record {record['id']}: {str(e)}")

            if not silver_values:
                return 0, failed, errors

            # Single transaction: upsert to silver + mark quarantine as remediated
            upsert_sql = """
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

            cursor.executemany(upsert_sql, silver_values)

            # Bulk update quarantine status
            update_sql = """
                UPDATE quarantine.sales_failed
                SET remediation_status = 'remediated',
                    replayed = TRUE,
                    replayed_at = NOW(),
                    corrected_by = 'remediation_dag'
                WHERE ingestion_run_id = %s::uuid AND id = %s
            """
            cursor.executemany(update_sql, quarantine_keys)

            conn.commit()
            fixed = len(silver_values)
            logger.info(f"Batch replayed {fixed} records to silver")

        except Exception as e:
            conn.rollback()
            # Increment retry_count for the entire batch so they aren't stuck forever
            try:
                increment_sql = """
                    UPDATE quarantine.sales_failed
                    SET retry_count = retry_count + 1
                    WHERE ingestion_run_id = %s::uuid AND id = %s
                """
                cursor.executemany(increment_sql, quarantine_keys)
                conn.commit()
            except Exception:
                conn.rollback()

            failed = len(valid_records)
            errors.append(f"Batch replay failed: {str(e)}")
            logger.error(f"Batch replay failed, rolled back: {e}")
        finally:
            cursor.close()
            conn.close()

        return fixed, failed, errors

    def _batch_reject(pg_hook, invalid_records):
        """Batch reject unfixable records."""
        if not invalid_records:
            return 0

        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        try:
            values = []
            for record in invalid_records:
                values.append(
                    (
                        record.get("reject_reason", "unknown"),
                        record["ingestion_run_id"],
                        record["id"],
                    )
                )

            reject_sql = """
                UPDATE quarantine.sales_failed
                SET remediation_status = 'rejected',
                    replayed = TRUE,
                    replayed_at = NOW(),
                    corrected_by = 'auto_rejected',
                    error_reason = CONCAT(error_reason, ' | Rejected: ', %s)
                WHERE ingestion_run_id = %s::uuid AND id = %s
            """
            cursor.executemany(reject_sql, values)
            conn.commit()

            rejected = len(values)
            logger.info(f"Batch rejected {rejected} unfixable records")
            return rejected

        except Exception as e:
            conn.rollback()
            logger.error(f"Batch reject failed: {e}")
            return 0
        finally:
            cursor.close()
            conn.close()

    def _escalate_dead_letters(pg_hook):
        """Move records that exceeded max retries to dead-letter status."""
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                UPDATE quarantine.sales_failed
                SET remediation_status = 'dead_letter',
                    replayed = TRUE,
                    replayed_at = NOW(),
                    corrected_by = 'dead_letter_escalation',
                    error_reason = CONCAT(error_reason, ' | Dead-lettered after ', retry_count, ' retries')
                WHERE remediation_status = 'pending'
                  AND retry_count >= %s
            """,
                (MAX_RETRY_COUNT,),
            )

            dead_lettered = cursor.rowcount
            conn.commit()

            if dead_lettered > 0:
                logger.warning(f"Escalated {dead_lettered} records to dead-letter after {MAX_RETRY_COUNT} retries")
            return dead_lettered

        except Exception as e:
            conn.rollback()
            logger.error(f"Dead-letter escalation failed: {e}")
            return 0
        finally:
            cursor.close()
            conn.close()

    def _get_quarantine_stats(pg_hook):
        """Get current quarantine statistics."""
        query = """
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN remediation_status = 'pending' THEN 1 END) as pending,
                COUNT(CASE WHEN remediation_status = 'remediated' THEN 1 END) as remediated,
                COUNT(CASE WHEN remediation_status = 'rejected' THEN 1 END) as rejected,
                COUNT(CASE WHEN remediation_status = 'dead_letter' THEN 1 END) as dead_letter
            FROM quarantine.sales_failed
        """
        result = pg_hook.execute_query(query)

        if result[1]:
            row = result[1][0]
            return {
                "total": row[0],
                "pending": row[1],
                "remediated": row[2],
                "rejected": row[3],
                "dead_letter": row[4],
            }

        return {"total": 0, "pending": 0, "remediated": 0, "rejected": 0, "dead_letter": 0}

    # Task dependencies
    result = remediate_all_batches()
    send_remediation_notification(result)

    trigger_gold_rebuild = TriggerDagRunOperator(
        task_id="trigger_gold_rebuild",
        trigger_dag_id="silver_to_gold",
        wait_for_downstream=False,
    )

    result >> trigger_gold_rebuild
