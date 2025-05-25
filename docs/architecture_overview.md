# Architecture Overview

## Objective

This pipeline simulates a production-grade system for processing marketing engagement data from AWS S3 into a Snowflake data warehouse. The pipeline is designed to be modular, scalable, testable, and reliable, with a focus on logging, data quality, idempotency, and scalability for large datasets.

---

## Components

### 1. Input Data Sources

- **CSV**: Customer demographics (e.g., customer_id, first_name, email, region, signup_date)  
- **JSON**: Clickstream data (e.g., event_id, user_id, event_type, page_url, duration_ms, timestamp)

These files are assumed to be uploaded to AWS S3 and accessed using Snowflake external stages.

---

### 2. Ingestion Pipeline

- `ingestion.py`: Loads both CSV and JSON data into Snowflake **raw staging tables** (`raw_customer_demographics`, `raw_clickstream_events`).  
- Raw tables are created if they do not exist.  
- **Truncation before load** ensures **idempotency** and prevents duplicate data ingestion.  
- Detailed logging is implemented:  
  - Logs written locally (`logs/ingestion.log`).  
  - Pipeline steps logged into Snowflake `ingestion_logs` table with metadata such as step name, status, records loaded, and error details.

---

### 3. Data Transformation Layer

- `transformation.py`: Implements transformation logic to populate **dimension** and **fact** tables (`dim_customer`, `fact_click_events`) from raw staging tables.  
- Uses Snowflake `MERGE` statements for upserts to maintain idempotency and avoid duplicates.  
- Table definitions align with raw source schemas (e.g., `fact_click_events.event_id` is `VARCHAR` matching source `RAW_CLICKSTREAM` table).  
- Error handling includes logging any failures into the `ingestion_logs` table.

---

### 4. Data Quality Checks

- `dq_checks.py` performs multiple checks:  
  - Null check on `customer_id` in `raw_customer_demographics`  
  - Uniqueness check on `customer_id`  
  - Row count validation  
- Results are:  
  - Logged locally and into `dq_check_logs` Snowflake table with status, timestamps, and messages.  
  - In case of critical failures, alerts can be sent (email/Slack integration code is provided but commented out for demo).  
  - This design allows graceful handling of DQ failures with clear auditability.

---

### 5. Logging and Monitoring

- Local logging to files under `logs/`  
- Snowflake tables for logging:  
  - `ingestion_logs`: Tracks ingestion and transformation step outcomes  
  - `dq_check_logs`: Tracks data quality check outcomes and details  
- Logging includes timestamps, step names, status (INFO/ERROR), and error details where applicable.

---

### 6. Idempotency

- Achieved by:  
  - Truncating raw staging tables before ingestion  
  - Using `MERGE` statements in transformations to avoid duplicates on repeated runs  
- Ensures pipeline reruns with the same input data result in consistent state in the warehouse.

---

### 7. Performance Tuning & Scalability

- Cluster keys applied on key columns (e.g., `customer_id`) in raw and dimension tables to optimize query performance.  
- Transformation queries optimized with `MERGE` and selective filtering.  
- Conceptual scalability designed to handle **10x data volume increase** by:  
  - Scaling Snowflake warehouse size dynamically  
  - Partitioning or clustering large tables for efficient pruning  
  - Leveraging Airflow for parallel task orchestration and retries  
  - Adding incremental loading strategies if raw data volume grows significantly

---

### 8. Orchestration

- `main.py` acts as a simple orchestrator calling ingestion, transformation, and DQ check functions sequentially.  
- High level Dag model available in airflow_dags/ folder readme file.

---

### 9. Tools and Technologies

- Python (with Snowflake Connector)  
- Snowflake Data Warehouse (tables, merges, clustering)  
- AWS S3 (as external staged data source)  
- dotenv for environment configuration  
- Python logging library  
- Airflow (conceptual DAGs for orchestration)  
- Optional email/Slack alerting (demo code commented)

---
