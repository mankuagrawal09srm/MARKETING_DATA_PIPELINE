# src/snowflake_ingestion_from_s3.py
import logging
import snowflake.connector

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Snowflake Configuration ---
SNOWFLAKE_ACCOUNT = "KXCRTSK-JT76968"
SNOWFLAKE_USER = "Mayank"
SNOWFLAKE_PASSWORD = "Mayank@09051995"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DATABASE = "MARKETING_DATE"
SNOWFLAKE_SCHEMA = "STAGE_DATE"

# --- External Stage and Files ---
S3_STAGE_CSV = "my_csv_stage"
S3_STAGE_json = "my_json_stage"
CSV_FILE = "my_data.csv"
JSON_FILE = "my_data.json"

def get_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        logging.info("Connected to Snowflake.")
        return conn
    except Exception as e:
        logging.error(f"Snowflake connection failed: {e}")
        raise

def load_csv(cursor):
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
    copy_into_sql = f"""
    COPY INTO raw_customer_demographics
    FROM @{S3_STAGE_CSV}/
    FILE_FORMAT = csv_format
    PATTERN = '.*\\.csv';
    """
    cursor.execute(create_table_sql)
    cursor.execute(copy_into_sql)
    logging.info("CSV file loaded into raw_customer_demographics.")

def load_json(cursor):
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
    copy_into_sql = f"""
    COPY INTO raw_clickstream
    FROM @{S3_STAGE_json}/
    FILE_FORMAT = json_format
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
    """
    cursor.execute(create_table_sql)
    cursor.execute(copy_into_sql)
    logging.info("JSON file loaded into raw_clickstream.")

def main():
    conn = None
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        load_csv(cursor)
        load_json(cursor)

        cursor.execute("SELECT COUNT(*) FROM raw_customer_demographics;")
        logging.info(f"CSV Table Record Count: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM raw_clickstream;")
        logging.info(f"JSON Table Record Count: {cursor.fetchone()[0]}")

    except Exception as e:
        logging.error(f"Data ingestion failed: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Snowflake connection closed.")

if __name__ == "__main__":
    main()
