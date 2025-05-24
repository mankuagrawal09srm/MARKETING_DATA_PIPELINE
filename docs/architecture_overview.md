# Architecture Overview
```mermaid
graph TD
    A[Raw Data (CSV, JSON)] --> B[Data Ingestion (ingestion.py)]
    B --> C[Data Quality Checks (dq_checks.py)]
    C --> D[Data Transformations (transformations.py)]
    D --> E[Feature Engineering (feature_engineering.py)]
    E --> F[Data Warehouse (Snowflake)]
    F --> G[Analytics / ML Models / Dashboards]

    subgraph Logging
      L[Logs (logs/ingestion.log)]
    end

    B --> L
    C --> L

## Objective

This pipeline simulates a production-grade system for processing marketing engagement data from AWS S3 into a Snowflake data warehouse. The pipeline is designed to be modular, scalable, testable, and reliable, with a focus on logging, data quality, and idempotency.

---

## Components

### 1. Input Data Sources

- **CSV**: Customer demographics (e.g., customer_id, name, region)
- **JSON**: Clickstream data (e.g., event type, page URL, timestamps)

These files are assumed to be uploaded to AWS S3 and accessed using Snowflake external stages.

---

### 2. Ingestion Pipeline

- `ingestion.py`: Loads both CSV and JSON data into Snowflake raw tables.
- Tables are created if they do not exist.
- Tables are truncated before loading to ensure **idempotency**.

---

### 3. Data Quality Checks

- `dq_checks.py` performs:
  - Null check on `customer_id`
  - Uniqueness check on `customer_id`
  - Total row count check

Each check is:
- Logged locally
- Logged into the `dq_check_logs` Snowflake table with status, results, and timestamp

---

### 4. Logging

- Local logs written to: `logs/ingestion.log`
- Pipeline steps and DQ results logged into Snowflake tables:
  - `ingestion_logs`: General steps (optional)
  - `dq_check_logs`: Detailed DQ check status

---

### 5. Idempotency

- Achieved by truncating tables before loading.
- Ensures rerunning the pipeline with the same data doesn’t create duplicates.

---

### 6. Performance Tuning

- Clustering key added on `customer_id` in `raw_customer_demographics` to improve filter performance:

```sql
ALTER TABLE raw_customer_demographics CLUSTER BY (customer_id);


### 7. Orchestration
main.py acts as the orchestrator:

Establishes Snowflake connection

Calls CSV and JSON loaders

Runs data quality checks

Can be converted into an Airflow DAG or Step Function if extended.



### 8. Tools and Technologies
Python

Snowflake (warehouse, external stages)

AWS S3 (mocked for testing)

dotenv for config management

Logging via Python standard library
