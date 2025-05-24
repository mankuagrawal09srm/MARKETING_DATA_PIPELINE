import logging
from utils import log_to_snowflake

# External stages and file format names used for COPY INTO commands
S3_STAGE_CSV = "my_csv_stage"
S3_STAGE_JSON = "my_json_stage"

def load_csv(cursor):
    step = "load_csv"  # Identifier for logging

    try:
        # Log start of CSV load (both local logging and Snowflake)
        log_to_snowflake(cursor, 'INFO', step, 'Starting CSV data load')
        logging.info("Loading CSV data into raw_customer_demographics...")

        # Create target table if it does not exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS raw_customer_demographics (
            customer_id INTEGER PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            email VARCHAR,
            region VARCHAR,
            signup_date DATE
        );
        """

        # Clear existing data to avoid duplicates (idempotency)
        truncate_sql = "TRUNCATE TABLE raw_customer_demographics;"

        # Copy data from the external CSV stage into Snowflake table
        copy_into_sql = f"""
        COPY INTO raw_customer_demographics
        FROM @{S3_STAGE_CSV}/
        FILE_FORMAT = csv_format
        PATTERN = '.*\\.csv';
        """

        # Execute SQL statements sequentially
        cursor.execute(create_table_sql)
        cursor.execute(truncate_sql)
        cursor.execute(copy_into_sql)

        # Query to get number of rows loaded for logging
        cursor.execute("SELECT COUNT(*) FROM raw_customer_demographics;")
        rows_loaded = cursor.fetchone()[0]

        # Log successful load with rows count
        message = f"CSV file loaded successfully with {rows_loaded} rows."
        log_to_snowflake(cursor, 'INFO', step, message, records_loaded=rows_loaded)
        logging.info(message)

    except Exception as e:
        # Log error details if anything fails during the load
        error_msg = f"Failed to load CSV data: {e}"
        log_to_snowflake(cursor, 'ERROR', step, error_msg, error_details=str(e))
        logging.error(error_msg)
        raise  # Re-raise exception to stop further execution

def load_json(cursor):
    step = "load_json"  # Identifier for logging

    try:
        # Log start of JSON load (both local logging and Snowflake)
        log_to_snowflake(cursor, 'INFO', step, 'Starting JSON data load')
        logging.info("Loading JSON data into raw_clickstream...")

        # Create target table if not exists
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS raw_clickstream (
            event_id VARCHAR,
            timestamp TIMESTAMP_LTZ,
            user_id INTEGER,
            event_type VARCHAR,
            page_url VARCHAR,
            duration_ms INTEGER
        );
        """

        # Clear existing data for idempotency
        truncate_sql = "TRUNCATE TABLE raw_clickstream;"

        # Copy data from external JSON stage with case-insensitive matching on column names
        copy_into_sql = f"""
        COPY INTO raw_clickstream
        FROM @{S3_STAGE_JSON}/
        FILE_FORMAT = json_format
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """

        # Execute SQL statements
        cursor.execute(create_table_sql)
        cursor.execute(truncate_sql)
        cursor.execute(copy_into_sql)

        # Query to find rows loaded
        cursor.execute("SELECT COUNT(*) FROM raw_clickstream;")
        rows_loaded = cursor.fetchone()[0]

        # Log success with rows loaded info
        message = f"JSON file loaded successfully with {rows_loaded} rows."
        log_to_snowflake(cursor, 'INFO', step, message, records_loaded=rows_loaded)
        logging.info(message)

    except Exception as e:
        # Log error and details if loading fails
        error_msg = f"Failed to load JSON data: {e}"
        log_to_snowflake(cursor, 'ERROR', step, error_msg, error_details=str(e))
        logging.error(error_msg)
        raise
