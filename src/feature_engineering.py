# src/feature_engineering.py

import logging
from datetime import datetime
from utils import get_snowflake_connection

def register_feature(cursor, feature_metadata):
    insert_sql = """
    MERGE INTO feature_catalog AS target
    USING (SELECT %(feature_name)s AS feature_name) AS source
    ON target.feature_name = source.feature_name
    WHEN MATCHED THEN UPDATE SET
        description = %(description)s,
        data_type = %(data_type)s,
        source_table = %(source_table)s,
        transformation_summary = %(transformation_summary)s,
        update_frequency = %(update_frequency)s,
        quality_metrics = %(quality_metrics)s,
        last_updated_at = CURRENT_TIMESTAMP
    WHEN NOT MATCHED THEN INSERT (
        feature_name, description, data_type,
        source_table, transformation_summary,
        update_frequency, quality_metrics
    )
    VALUES (
        %(feature_name)s, %(description)s, %(data_type)s,
        %(source_table)s, %(transformation_summary)s,
        %(update_frequency)s, %(quality_metrics)s
    );
    """
    try:
        cursor.execute(insert_sql, feature_metadata)
        logging.info(f"Feature '{feature_metadata['feature_name']}' registered successfully.")
    except Exception as e:
        logging.error(f"Failed to register feature '{feature_metadata['feature_name']}': {e}")
        raise

def register_all_features():
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    # Define your features
    features = [
        {
            "feature_name": "customer_click_rate",
            "description": "Average number of clicks per user session",
            "data_type": "FLOAT",
            "source_table": "fact_click_events",
            "transformation_summary": "Aggregated clicks grouped by customer_id over sessions",
            "update_frequency": "daily",
            "quality_metrics": "null_pct < 2%, value_range: 0-50"
        },
        {
            "feature_name": "days_since_signup",
            "description": "Number of days since a customer signed up",
            "data_type": "INTEGER",
            "source_table": "dim_customer",
            "transformation_summary": "DATEDIFF(current_date, signup_date)",
            "update_frequency": "daily",
            "quality_metrics": "null_pct = 0, value_range: 0+"
        },
    ]

    for feature in features:
        register_feature(cursor, feature)

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("All features registered in catalog.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    register_all_features()
