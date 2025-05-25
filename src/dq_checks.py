import logging

def log_dq_result_to_snowflake(cursor, check_name, status, result_value, message):
    """
    Inserts a DQ check result into the Snowflake logging table.
    """
    insert_sql = """
    INSERT INTO dq_check_logs (check_name, status, result_value, message)
    VALUES (%s, %s, %s, %s)
    """
    cursor.execute(insert_sql, (check_name, status, result_value, message))


def send_email_alert(subject, body):
    """
    Example placeholder function to send an email alert.
    This is commented out because email sending is not implemented.
    """
    # import smtplib
    # from email.mime.text import MIMEText
    #
    # msg = MIMEText(body)
    # msg['Subject'] = subject
    # msg['From'] = 'your_email@example.com'
    # msg['To'] = 'alert_recipient@example.com'
    #
    # with smtplib.SMTP('smtp.example.com') as server:
    #     server.login('username', 'password')
    #     server.send_message(msg)
    pass


def run_dq_checks(cursor):
    logging.info("Running data quality checks...")

    try:
        # 1. Null customer_id check
        cursor.execute("SELECT COUNT(*) FROM raw_customer_demographics WHERE customer_id IS NULL")
        null_count = cursor.fetchone()[0]
        if null_count > 0:
            msg = f"{null_count} NULL customer_id(s) found"
            logging.error(f"DQ Check Failed: {msg}")
            log_dq_result_to_snowflake(cursor, "Null customer_id check", "FAILED", null_count, msg)
            # Uncomment below to send alert email
            # send_email_alert("DQ Check Failed: Null customer_id", msg)
        else:
            logging.info("Null check passed: No NULL customer_id.")
            log_dq_result_to_snowflake(cursor, "Null customer_id check", "PASSED", 0, "No NULLs found")

        # 2. Unique customer_id check
        cursor.execute("SELECT COUNT(DISTINCT customer_id) FROM raw_customer_demographics")
        unique_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM raw_customer_demographics")
        total_count = cursor.fetchone()[0]

        if unique_count != total_count:
            duplicates = total_count - unique_count
            msg = f"{duplicates} duplicate customer_id(s) found"
            logging.error(f"DQ Check Failed: {msg}")
            log_dq_result_to_snowflake(cursor, "Unique customer_id check", "FAILED", duplicates, msg)
            # Uncomment below to send alert email
            # send_email_alert("DQ Check Failed: Duplicate customer_id", msg)
        else:
            logging.info("Uniqueness check passed: All customer_id values are unique.")
            log_dq_result_to_snowflake(cursor, "Unique customer_id check", "PASSED", unique_count, "All values unique")

        # 3. Row count > 0
        if total_count == 0:
            msg = "No rows found in raw_customer_demographics"
            logging.error(f"DQ Check Failed: {msg}")
            log_dq_result_to_snowflake(cursor, "Row count check", "FAILED", 0, msg)
            # Uncomment below to send alert email
            # send_email_alert("DQ Check Failed: No rows", msg)
        else:
            logging.info(f"Row count check passed: {total_count} rows found.")
            log_dq_result_to_snowflake(cursor, "Row count check", "PASSED", total_count, "Rows exist")

    except Exception as e:
        logging.error(f"Error running data quality checks: {e}")
        log_dq_result_to_snowflake(cursor, "DQ Check Exception", "ERROR", 0, str(e))
        # Uncomment below to send alert email
        # send_email_alert("DQ Check Exception", str(e))
        raise
