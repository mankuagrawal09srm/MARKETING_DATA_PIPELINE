# src/data_ingestion.py
import os
import json
import pandas as pd
import boto3
import sqlite3
from botocore.client import Config
from botocore.exceptions import ClientError

# --- Configuration for MinIO (Local S3 Mock) ---
# IMPORTANT: This is the data port (9000), not the console port (9001)
MINIO_ENDPOINT = "http://127.0.0.1:9000"
# Use your custom username and password if you changed them
MINIO_ACCESS_KEY = "Mayank_1995" # Make sure to update this!
MINIO_SECRET_KEY = "May09@95#" # Make sure to update this!
RAW_DATA_BUCKET = "raw-marketing-data"

# --- Configuration for SQLite (Local Database) ---
# This will be created in your data/ directory
DB_FILE = "data/marketing_pipeline.db"

def get_s3_client():
    """
    Initializes and returns an S3 client connected to MinIO.
    """
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version='s3v4')
        )
        # Verify connection by listing buckets (optional, but good for debugging)
        s3_client.list_buckets()
        print(f"Successfully connected to MinIO at {MINIO_ENDPOINT}")
        return s3_client
    except ClientError as e:
        print(f"Error connecting to MinIO: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred during MinIO client setup: {e}")
        raise

def create_sqlite_connection():
    """
    Establishes a connection to the SQLite database.
    Creates the database file if it doesn't exist.
    """
    # Ensure the data directory exists for the DB file
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    try:
        conn = sqlite3.connect(DB_FILE)
        print(f"Successfully connected to SQLite database: {DB_FILE}")
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to SQLite database: {e}")
        raise

def ingest_clickstream_data(s3_client, db_conn):
    """
    Reads clickstream data from MinIO and loads it into a SQLite table.
    """
    print("Ingesting clickstream data...")
    try:
        obj = s3_client.get_object(Bucket=RAW_DATA_BUCKET, Key="mock_clickstream_data.json")
        # --- CHANGE START ---
        # Access the 'Body' key of the response object to get the StreamingBody, then read it.
        #clickstream_data = json.loads(obj.read().decode('utf-8'))
        clickstream_data = json.loads(obj['Body'].read().decode('utf-8'))
        # --- CHANGE END ---
        df = pd.DataFrame(clickstream_data)

        # Basic schema definition for SQLite
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS raw_clickstream (
            event_id TEXT PRIMARY KEY,
            timestamp TEXT,
            user_id INTEGER,
            event_type TEXT,
            page_url TEXT,
            duration_ms INTEGER
        );
        """
        cursor = db_conn.cursor()
        cursor.execute(create_table_sql)
        db_conn.commit()

        # Load data into SQLite
        df.to_sql('raw_clickstream', db_conn, if_exists='replace', index=False)
        print(f"Successfully ingested {len(df)} records into raw_clickstream table.")
    except ClientError as e:
        print(f"Error reading clickstream data from MinIO: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred during clickstream ingestion: {e}")
        raise

def ingest_customer_demographics_data(s3_client, db_conn):
    """
    Reads customer demographics data from MinIO and loads it into a SQLite table.
    """
    print("Ingesting customer demographics data...")
    try:
        obj = s3_client.get_object(Bucket=RAW_DATA_BUCKET, Key="mock_customer_demographics.csv")
        # --- CHANGE START ---
        # Pass the 'Body' StreamingBody directly to pandas.read_csv
        #df = pd.read_csv(obj)
        df = pd.read_csv(obj['Body'])
        # --- CHANGE END ---

        # Basic schema definition for SQLite
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS raw_customer_demographics (
            customer_id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            region TEXT,
            signup_date TEXT
        );
        """
        cursor = db_conn.cursor()
        cursor.execute(create_table_sql)
        db_conn.commit()

        # Load data into SQLite
        df.to_sql('raw_customer_demographics', db_conn, if_exists='replace', index=False)
        print(f"Successfully ingested {len(df)} records into raw_customer_demographics table.")
    except ClientError as e:
        print(f"Error reading customer demographics data from MinIO: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred during customer demographics ingestion: {e}")
        raise

def main():
    s3_client = None
    db_conn = None
    try:
        s3_client = get_s3_client()
        db_conn = create_sqlite_connection()

        ingest_clickstream_data(s3_client, db_conn)
        ingest_customer_demographics_data(s3_client, db_conn)

        print("\nData ingestion pipeline completed successfully.")

        # Optional: Verify data in SQLite
        cursor = db_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM raw_clickstream;")
        print(f"Total records in raw_clickstream: {cursor.fetchone()}")
        cursor.execute("SELECT COUNT(*) FROM raw_customer_demographics;")
        print(f"Total records in raw_customer_demographics: {cursor.fetchone()}")

    except Exception as e:
        print(f"\nData ingestion pipeline failed: {e}")
    finally:
        if db_conn:
            db_conn.close()
            print("SQLite connection closed.")

if __name__ == "__main__":
    main()