# src/dq_checks.py
import logging

def run_dq_checks(cursor):
    logging.info("Running data quality checks...")

    checks = {
        "Null customer_id count": "SELECT COUNT(*) FROM raw_customer_demographics WHERE customer_id IS NULL",
        "Unique customer_id count": "SELECT COUNT(DISTINCT customer_id) FROM raw_customer_demographics",
        "Total customer_id count": "SELECT COUNT(*) FROM raw_customer_demographics"
    }

    for check_name, sql in checks.items():
        try:
            cursor.execute(sql)
            result = cursor.fetchone()[0]
            logging.info(f"{check_name}: {result}")
        except Exception as e:
            logging.error(f"Error running DQ check '{check_name}': {e}")
