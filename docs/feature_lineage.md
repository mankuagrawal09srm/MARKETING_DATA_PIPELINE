Feature Engineering Pipeline Documentation
==========================================

This document describes the design, lineage, transformations, and cataloging of features generated by the feature engineering pipeline.

1. Feature Lineage & Transformation Details
------------------------------------------

Feature 1: days_since_signup
----------------------------
- Source Table: dim_customer
- Source Column: signup_date
- Transformation:
    days_since_signup = DATEDIFF(current_date, signup_date)
- Business Logic:
    Measures the number of days since a customer signed up.
- Use Cases:
    - Customer lifecycle segmentation
    - Retention analysis
    - New vs. existing customer modeling

Feature 2: days_since_last_activity
-----------------------------------
- Source Table: fact_click_events
- Source Column: event_time
- Transformation:
    last_event_time = MAX(event_time) GROUP BY user_id
    days_since_last_activity = DATEDIFF(current_date, last_event_time)
- Business Logic:
    Measures the number of days since the last user activity.
- Use Cases:
    - Churn prediction
    - Engagement monitoring
    - Inactivity scoring

2. Feature Engineering Pipeline Architecture
--------------------------------------------

Inputs:
- Snowflake tables: dim_customer, fact_click_events

Pipeline Steps:
1. Load raw data from Snowflake
2. Apply pandas transformations to compute features
3. Merge feature dataframes
4. Reshape data to long format with: user_id, feature_name, feature_value, feature_date
5. Write output to Snowflake Iceberg table: feature_engineered_iceberg
6. Register feature metadata into feature_catalog table

Output Format:
--------------
| customer_id | feature_name              | feature_value | feature_date |
|-------------|---------------------------|---------------|--------------|
| 101         | days_since_signup         | 45            | 2025-05-25   |
| 101         | days_since_last_activity  | 3             | 2025-05-25   |

Execution Frequency:
- Daily (can be scheduled using Airflow, dbt Cloud, or similar orchestration tool)

3. Feature Catalog Metadata Design
----------------------------------

The pipeline stores metadata in a centralized catalog (Snowflake table: feature_catalog) with the following fields:

- feature_name: Unique name
- description: Explanation of the feature
- data_type: INTEGER, STRING, etc.
- source_table: Raw table source
- transformation_summary: Human-readable formula or logic
- update_frequency: Daily, hourly, etc.
- quality_metrics: Example - null_pct < 5%
- created_at: Timestamp of registration

Example Feature Metadata Entry:
-------------------------------
{
  "feature_name": "days_since_signup",
  "description": "Number of days since customer signed up",
  "data_type": "INTEGER",
  "source_table": "dim_customer",
  "transformation_summary": "DATEDIFF(current_date, signup_date)",
  "update_frequency": "daily",
  "quality_metrics": "null_pct = 0, value_range: 0+"
}

4. Future Enhancements
----------------------

- Add automated lineage tracking via dbt.
- Add support for versioning features
- Build a web-based metadata explorer for non-technical users


