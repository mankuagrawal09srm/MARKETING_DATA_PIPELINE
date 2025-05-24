

### Updated `docs/data_model.md`


# Data Model

This document describes the structure of Snowflake tables used in the pipeline.

---

## Table: raw_customer_demographics

Stores structured customer demographic data from CSV files.

| Column Name   | Data Type | Description                          |
|---------------|-----------|--------------------------------------|
| customer_id   | INTEGER   | Unique identifier (Primary Key)      |
| first_name    | VARCHAR   | First name of the customer           |
| last_name     | VARCHAR   | Last name of the customer            |
| email         | VARCHAR   | Email address                        |
| region        | VARCHAR   | Customer's geographic region         |
| signup_date   | DATE      | Date of customer sign-up             |

**Clustering Key**: `customer_id`  
**Loaded From**: CSV via external Snowflake stage  
**DQ Checks**: Null check, uniqueness, non-zero row count

---

## Table: raw_clickstream

Stores clickstream event data from JSON files.

| Column Name   | Data Type     | Description                          |
|---------------|---------------|--------------------------------------|
| event_id      | VARCHAR       | Unique identifier for each event     |
| timestamp     | TIMESTAMP_LTZ | Timestamp of event occurrence        |
| user_id       | INTEGER       | Related customer_id                  |
| event_type    | VARCHAR       | Type of interaction (e.g., click)    |
| page_url      | VARCHAR       | Page URL where event occurred        |
| duration_ms   | INTEGER       | Event duration in milliseconds       |

**Loaded From**: JSON via external Snowflake stage  
**DQ Checks**: Not implemented yet

---

## Table: dq_check_logs

Stores results of each data quality check performed on `raw_customer_demographics`.

| Column Name   | Data Type | Description                               |
|---------------|-----------|-------------------------------------------|
| check_name    | VARCHAR   | Name of the data quality check            |
| status        | VARCHAR   | PASSED / FAILED                           |
| result_value  | NUMBER    | Actual numeric result (e.g., 0 nulls)     |
| message       | VARCHAR   | Explanation of the check result           |
| run_time      | TIMESTAMP | Time when the check was executed          |

**Logged From**: `dq_checks.py`

---

## Optional: ingestion_logs (if enabled)

Tracks general pipeline step execution.

| Column Name     | Data Type | Description                            |
|-----------------|-----------|----------------------------------------|
| log_id          | INTEGER   | Auto-increment ID                      |
| log_timestamp   | TIMESTAMP | Timestamp of the log entry             |
| log_level       | VARCHAR   | INFO / ERROR / DEBUG                   |
| step_name       | VARCHAR   | Name of the pipeline step              |
| message         | STRING    | Log message                            |
| records_loaded  | INTEGER   | Number of records processed (optional) |
| error_details   | STRING    | Error message if any                   |
