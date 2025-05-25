import logging
from utils import log_to_snowflake

def load_dim_customer(cursor):
    logging.info("Populating dim_customer...")

    try:
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS dim_customer (
            customer_id VARCHAR PRIMARY KEY,
            first_name VARCHAR,
            email VARCHAR,
            signup_date DATE,
            region VARCHAR,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_sql)

        merge_sql = """
        MERGE INTO dim_customer AS target
        USING (
            SELECT DISTINCT customer_id, first_name, email, signup_date, region
            FROM raw_customer_demographics
            WHERE customer_id IS NOT NULL
        ) AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN
            UPDATE SET
                target.first_name = source.first_name,
                target.email = source.email,
                target.signup_date = source.signup_date,
                target.region = source.region
        WHEN NOT MATCHED THEN
            INSERT (customer_id, first_name, email, signup_date, region)
            VALUES (source.customer_id, source.first_name, source.email, source.signup_date, source.region);
        """
        cursor.execute(merge_sql)

        logging.info("dim_customer table populated successfully.")
        log_to_snowflake(cursor, "INFO", "load_dim_customer", "dim_customer loaded successfully.")

    except Exception as e:
        logging.error(f"Error populating dim_customer: {e}")
        log_to_snowflake(cursor, "ERROR", "load_dim_customer", "Failed to load dim_customer", error_details=str(e))
        raise


def load_fact_click_events(cursor):
    logging.info("Populating fact_click_events...")

    try:
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS fact_click_events (
            event_id VARCHAR PRIMARY KEY,
            user_id NUMBER,
            event_type VARCHAR,
            page_url VARCHAR,
            duration_ms NUMBER,
            event_time TIMESTAMP_LTZ,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            -- Note: Foreign key not enforced in Snowflake, can be added as info only
        );
        """
        cursor.execute(create_table_sql)

        insert_sql = """
        MERGE INTO fact_click_events AS target
        USING (
            SELECT
                EVENT_ID,
                USER_ID,
                EVENT_TYPE,
                PAGE_URL,
                DURATION_MS,
                TIMESTAMP
            FROM MARKETING_DATE.STAGE_DATE.RAW_CLICKSTREAM
            WHERE EVENT_ID IS NOT NULL
        ) AS source
        ON target.event_id = source.EVENT_ID
        WHEN NOT MATCHED THEN
            INSERT (event_id, user_id, event_type, page_url, duration_ms, event_time)
            VALUES (source.EVENT_ID, source.USER_ID, source.EVENT_TYPE, source.PAGE_URL, source.DURATION_MS, source.TIMESTAMP);
        """
        cursor.execute(insert_sql)

        logging.info("fact_click_events table populated successfully.")
        log_to_snowflake(cursor, "INFO", "load_fact_click_events", "fact_click_events loaded successfully.", records_loaded=cursor.rowcount)

    except Exception as e:
        logging.error(f"Error populating fact_click_events: {e}")
        log_to_snowflake(cursor, "ERROR", "load_fact_click_events", "Failed to load fact_click_events", error_details=str(e))
        raise
