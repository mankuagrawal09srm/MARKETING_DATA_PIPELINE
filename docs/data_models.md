# Data Model

This document describes the structure of Snowflake tables used in the pipeline.

---

## Table: raw_customer_demographics

Stores structured customer demographic data from CSV files.

| Column Name   | Data Type | Description                          |
|---------------|-----------|------------------------------------|
| customer_id   | VARCHAR   | Unique identifier (Primary Key)    |
| first_name    | VARCHAR   | First name of the customer         |
| last_name     | VARCHAR   | Last name of the customer          |
| email         | VARCHAR   | Email address                      |
| region        | VARCHAR   | Customer's geographic region       |
| signup_date   | DATE      | Date of customer sign-up           |

**Clustering Key**: `customer_id`  
**Loaded From**: CSV via external Snowflake stage  
**DQ Checks**: Null check, uniqueness, non-zero row count

---

## Table: raw_clickstream
Stores clickstream event data from JSON files.

| Column Name   | Data Type     | Description                          |
|---------------|---------------|------------------------------------|
| event_id      | VARCHAR       | Unique identifier for each event   |
| timestamp     | TIMESTAMP_LTZ | Timestamp of event occurrence      |
| user_id       | NUMBER        | Related customer ID (user_id)      |
| event_type    | VARCHAR       | Type of interaction (e.g., click)  |
| page_url      | VARCHAR       | Page URL where event occurred      |
| duration_ms   | NUMBER        | Event duration in milliseconds     |

**Loaded From**: JSON via external Snowflake stage  
**DQ Checks**: Not implemented yet

---

## Table: dim_customer

Dimension table storing customer details for analytics.

| Column Name   | Data Type | Description                          |
|---------------|-----------|------------------------------------|
| customer_id   | VARCHAR   | Primary key                        |
| first_name    | VARCHAR   | First name of customer             |
| email         | VARCHAR   | Email address                     |
| signup_date   | DATE      | Customer sign-up date             |
| region        | VARCHAR   | Customer's geographic region      |
| is_active     | BOOLEAN   | Active status, default TRUE       |
| created_at    | TIMESTAMP | Record creation timestamp         |

**Populated From**: `raw_customer_demographics` via transformation  
**Upsert Method**: MERGE on `customer_id`

---

## Table: fact_click_events

Fact table storing customer click events.

| Column Name   | Data Type | Description                           |
|---------------|-----------|-------------------------------------|
| event_id      | VARCHAR   | Primary key, matches raw event_id   |
| user_id   | VARCHAR   | Foreign key to `dim_customer`       |
| event_type    | VARCHAR   | Event type                         |
| page_url      | VARCHAR   | Page URL where event occurred       |
| duration_ms   | NUMBER    | Event duration in milliseconds      |
| event_time    | TIMESTAMP | Timestamp of the event              |
| created_at    | TIMESTAMP | Record creation timestamp           |

**Populated From**: `raw_clickstream` via transformation  
**Insert Method**: Insert new records (no duplicates assumed in source)

---

## Table: dq_check_logs

Stores results of each data quality check performed on `raw_customer_demographics`.

| Column Name   | Data Type | Description                               |
|---------------|-----------|-------------------------------------------|
| check_name    | VARCHAR   | Name of the data quality check            |
| status        | VARCHAR   | PASSED / FAILED / ERROR                    |
| result_value  | NUMBER    | Actual numeric result (e.g., 0 nulls)     |
| message       | VARCHAR   | Explanation of the check result            |
| run_time      | TIMESTAMP | Time when the check was executed           |

**Logged From**: `dq_checks.py`

---

## Table: ingestion_logs

Tracks general pipeline step execution and errors.

| Column Name     | Data Type | Description                            |
|-----------------|-----------|----------------------------------------|
| log_id          | INTEGER   | Auto-increment ID                      |
| log_timestamp   | TIMESTAMP | Timestamp of the log entry             |
| log_level       | VARCHAR   | INFO / ERROR / DEBUG                   |
| step_name       | VARCHAR   | Name of the pipeline step              |
| message         | STRING    | Log message                           |
| records_loaded  | INTEGER   | Number of records processed (optional)|
| error_details   | STRING    | Error message if any                   |

**Logged From**: ingestion, transformation, and other pipeline components

---

---

## Table: feature_catalog

Stores metadata for engineered features to support documentation and discovery.

| Column Name            | Data Type     | Description                                      |
|------------------------|---------------|--------------------------------------------------|
| feature_name           | VARCHAR       | Unique feature identifier (Primary Key)          |
| description            | STRING        | Business logic or purpose of the feature         |
| data_type              | VARCHAR       | Snowflake-compatible data type                   |
| source_table           | VARCHAR       | Table or source used to derive the feature       |
| transformation_summary | STRING        | Summary of transformation logic                  |
| update_frequency       | VARCHAR       | Refresh rate (e.g., daily, weekly)               |
| quality_metrics        | STRING        | Range or null thresholds                         |
| created_at             | TIMESTAMP     | Feature registration timestamp                   |
| last_updated_at        | TIMESTAMP     | data updated timestamp                           |

**Populated from**: `src/feature_engineering.py`
