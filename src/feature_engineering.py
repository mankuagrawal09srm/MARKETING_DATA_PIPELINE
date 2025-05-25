# src/feature_engineering.py

import pandas as pd
from utils import get_snowflake_connection
import logging
from datetime import datetime,  timezone

def register_feature(cursor, feature_metadata):
    """Upsert metadata info about features in feature_catalog table."""
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
        last_updated_at  = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        feature_name, description, data_type,
        source_table, transformation_summary,
        update_frequency, quality_metrics, created_at
    )
    VALUES (
        %(feature_name)s, %(description)s, %(data_type)s,
        %(source_table)s, %(transformation_summary)s,
        %(update_frequency)s, %(quality_metrics)s, CURRENT_TIMESTAMP()
    )
    """
    cursor.execute(insert_sql, feature_metadata)
    logging.info(f"Feature '{feature_metadata['feature_name']}' registered/updated in catalog.")

def register_all_features(cursor):
    """Register metadata for all features we engineer (Feature 1 & 2 only). Done manually for now.""" 
    features = [
        {
            "feature_name": "days_since_signup",
            "description": "Number of days since customer signed up",
            "data_type": "INTEGER",
            "source_table": "dim_customer",
            "transformation_summary": "DATEDIFF(current_date, signup_date)",
            "update_frequency": "daily",
            "quality_metrics": "null_pct = 0, value_range: 0+"
        },
        {
            "feature_name": "days_since_last_activity",
            "description": "Days since last recorded click event",
            "data_type": "INTEGER",
            "source_table": "fact_click_events",
            "transformation_summary": "DATEDIFF(current_date, MAX(event_time)) grouped by customer_id",
            "update_frequency": "daily",
            "quality_metrics": "null_pct < 5%"
        },
    ]
    for feature in features:
        register_feature(cursor, feature)

def load_raw_data(cursor):
    """Load raw data from Snowflake into pandas DataFrames."""
    #conn = get_snowflake_connection()
    #cursor = conn.cursor()

    query_events = """
    SELECT user_id, event_time
    FROM fact_click_events
    WHERE event_time >= DATEADD(day, -90, CURRENT_DATE())
    """
    cursor.execute(query_events)
    events = cursor.fetchall()
    events_df = pd.DataFrame(events, columns=[desc[0] for desc in cursor.description])

    query_customers = """
    SELECT customer_id AS user_id, signup_date
    FROM dim_customer
    """
    cursor.execute(query_customers)
    customers = cursor.fetchall()
    customers_df = pd.DataFrame(customers, columns=[desc[0] for desc in cursor.description])

    #cursor.close()
    #conn.close()

    return events_df, customers_df

def engineer_features(events_df, customers_df):
    """Compute Features 1 and 2 only."""
    customers_df.columns = [col.lower() for col in customers_df.columns]
    customers_df["signup_date"] = pd.to_datetime(customers_df["signup_date"]).dt.tz_localize("UTC")
    today = pd.Timestamp.today(tz="UTC").normalize()
    customers_df["days_since_signup"] = (today - customers_df["signup_date"]).dt.days

    events_df.columns = [col.lower() for col in events_df.columns]
    events_df["event_time"] = pd.to_datetime(events_df["event_time"])

    last_activity = (
        events_df.groupby("user_id")["event_time"]
        .max()
        .reset_index()
        .rename(columns={"event_time": "last_event_time"})
    )
    today = pd.Timestamp.today(tz="UTC").normalize()
    last_activity["days_since_last_activity"] = (
        today - last_activity["last_event_time"]
    ).dt.days

    # Fix: Ensure consistent data types for merge
    customers_df["user_id"] = customers_df["user_id"].astype(str)
    last_activity["user_id"] = last_activity["user_id"].astype(str)

    # Merge only Feature 1 & 2
    features_df = customers_df[["user_id", "days_since_signup"]].merge(
        last_activity[["user_id", "days_since_last_activity"]],
        on="user_id",
        how="left"
    )

    features_df["days_since_last_activity"] = features_df["days_since_last_activity"].fillna(9999)

    #features_df["days_since_last_activity"].fillna(9999, inplace=True)

    # Reshape to long format
    features_long = pd.melt(
        features_df,
        id_vars=["user_id"],
        var_name="feature_name",
        value_name="feature_value"
    )
    features_long["feature_date"] = pd.Timestamp.today().normalize()

    return features_long
def write_features_to_iceberg(cursor,features_df):
    """Write the features DataFrame to Snowflake Iceberg table."""
    #conn = get_snowflake_connection()
    #cursor = conn.cursor()

    # Step 1: Delete today's existing rows
    cursor.execute("DELETE FROM feature_engineered_iceberg WHERE feature_date = CURRENT_DATE()")

    # Step 2: Get feature_id mapping from feature_catalog
    cursor.execute("SELECT feature_id, feature_name FROM feature_catalog")
    feature_map = {name: fid for fid, name in cursor.fetchall()}

    # Step 3: Add feature_id to DataFrame
    features_df = features_df.rename(columns={"user_id": "customer_id"})
    features_df["feature_id"] = features_df["feature_name"].map(feature_map)
    features_df["feature_date"] = features_df["feature_date"].dt.strftime("%Y-%m-%d")
    features_df["created_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Step 4: Insert feature rows with feature_id
    insert_sql = """
    INSERT INTO feature_engineered_iceberg (
        customer_id, feature_id, feature_name, feature_value, feature_date, created_at
    )
    VALUES (
        %(customer_id)s, %(feature_id)s, %(feature_name)s, %(feature_value)s, %(feature_date)s, %(created_at)s
    )
    """

    for _, row in features_df.iterrows():
        cursor.execute(insert_sql, row.to_dict())

    #conn.commit()
    #cursor.close()
    #conn.close()
    logging.info("Features written to Iceberg table successfully.")

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting feature engineering pipeline (features 1 & 2 only).")
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    events_df, customers_df = load_raw_data(cursor)
    features_df = engineer_features(events_df, customers_df)
    write_features_to_iceberg(cursor,features_df)

    
    register_all_features(cursor)
    conn.commit()
    cursor.close()
    conn.close()

    logging.info("Feature engineering pipeline completed successfully.")

if __name__ == "__main__":
    main()
