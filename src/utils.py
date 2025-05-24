# src/utils.py
import logging
import snowflake.connector
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

def get_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA")
        )
        logging.info("Connected to Snowflake.")
        return conn
    except Exception as e:
        logging.error(f"Snowflake connection failed: {e}")
        raise


    
def log_to_snowflake(cursor, log_level, step_name, message, records_loaded=None, error_details=None):
    """
    Insert a log record into the Snowflake ingestion_logs table.

    Args:
        cursor: Snowflake cursor object
        log_level: str - e.g., 'INFO', 'ERROR'
        step_name: str - e.g., 'load_csv', 'dq_check'
        message: str - descriptive log message
        records_loaded: int or None - number of records processed
        error_details: str or None - error info if any
    """
    insert_sql = """
    INSERT INTO ingestion_logs (log_level, step_name, message, records_loaded, error_details)
    VALUES (%s, %s, %s, %s, %s)
    """
    try:
        cursor.execute(insert_sql, (log_level, step_name, message, records_loaded, error_details))
    except Exception as e:
        # Fallback: log error locally if Snowflake insert fails
        logging.error(f"Failed to insert log into Snowflake: {e}")
