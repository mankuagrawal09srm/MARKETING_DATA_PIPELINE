# src/main.py
from utils import get_snowflake_connection
from data_ingestion import load_csv, load_json
import logging
from dq_checks import run_dq_checks
from transformations import load_dim_customer, load_fact_click_events



logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    conn = None
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        logging.info("Starting data ingestion pipeline...")

        load_csv(cursor)
        load_json(cursor)
        run_dq_checks(cursor)
        load_dim_customer(cursor)
        load_fact_click_events(cursor)

        logging.info("Pipeline completed successfully.")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Snowflake connection closed.")

if __name__ == "__main__":
    main()
