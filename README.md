
# Marketing Data Pipeline

This project implements a data pipeline to process and load marketing data from AWS S3 into Snowflake. It includes ingestion of CSV and JSON data, data quality (DQ) checks, idempotent loading, logging, and basic performance tuning using Snowflake best practices.

---

## 📁 Project Structure

```
Marketing_data_pipeline/
├── data/                         # Sample input files (CSV, JSON)
├── docs/                         # Documentation (architecture, data model)
│   ├── architecture_overview.md
│   └── data_model.md
├── logs/                         # Log outputs
│   └── ingestion.log
├── src/                          
│   ├── dq_checks.py              # Data quality checks
│   ├── data_ingestion.py         # Ingestion logic
│   ├── utils.py                  # Helper functions (Snowflake connection, logger)
│   ├── main.py                   # Script to orchestrate the pipeline
│   ├── feature_engineering.py    # Conceptual feature testing
│   └── transformations.py        # data transformations
├── .env                          # Snowflake credentials (not committed)
├── README.md
```

---

## 📊 Overview

This pipeline is designed to load marketing engagement data — customer demographics and clickstream events — from S3 into Snowflake. The processed data can be used in dashboards and ML models.

---

## 🚀 Key Features

- Loads structured (CSV) and semi-structured (JSON) data
- Performs data quality checks to validate the integrity of incoming data
- Ensures idempotency by truncating target tables before loading
- Logs events both locally and optionally to Snowflake
- Adds clustering for basic performance tuning

---

## 📥 Input Datasets

- **CSV**: Customer demographics (`customer_id`, `first_name`, `email`, `signup_date`)
- **JSON**: Clickstream events (`event_type`, `page_url`, `duration_ms`)

Both datasets are stored in an S3 bucket and read via Snowflake external stages.

---

## 🛠 How to Run the Pipeline

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/Marketing_data_pipeline.git
   cd Marketing_data_pipeline
   ```

2. **(Optional) Create and activate a virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate    # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**

   Create a `.env` file in the root directory with the following:
   ```ini
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_WAREHOUSE=your_warehouse
   SNOWFLAKE_DATABASE=your_database
   SNOWFLAKE_SCHEMA=your_schema
   ```

5. **Run the pipeline**
   ```bash
   python src/main.py
   ```

---

## ⚙️ Features Implemented

### Ingestion Logic
- Loads files via `COPY INTO` from S3 external stages
- Automatically creates tables if they don't exist
- Truncates before loading to ensure idempotency

### Data Quality Checks
- Null checks on critical columns
- Uniqueness validation on `customer_id`
- Row count validation

Logged into:
- Local log file
- Optionally: Snowflake table `dq_check_logs`

### Logging
- Local: `logs/ingestion.log`
- Optional Snowflake table: `PIPELINE_LOGS`

### Performance Optimization
- Adds clustering key on:
   ```sql
   ALTER TABLE raw_customer_demographics CLUSTER BY (customer_id);
   ```

---

### Feature Catalog

A lightweight feature catalog is implemented as a Snowflake table named `feature_catalog`. It stores metadata for engineered features including:

- `feature_name`: Unique feature identifier
- `description`: Business logic or purpose
- `data_type`: Data type in Snowflake
- `source_table`: Raw or intermediate table used
- `transformation_summary`: Logic used to generate the feature
- `update_frequency`: How often the feature is refreshed
- `quality_metrics`: Basic data quality expectations

Features are inserted/updated via `src/feature_engineering.py`.

This helps track available features, their lineage, and quality expectations for downstream consumers like data scientists.

---

## 📚 Documentation

- [`docs/architecture_overview.md`](docs/architecture_overview.md): Design and flow of the pipeline
- [`docs/data_model.md`](docs/data_model.md): Schema and table descriptions

---

## 🧰 Technologies Used

- Python 3.9+
- Snowflake (data warehouse)
- AWS S3 (source data)
- `dotenv` for credential management
- Python’s standard `logging` module

---


## 🔮 Potential Future Enhancements

- Orchestration with Apache Airflow or AWS Step Functions
- Alerting for failed DQ checks (Slack, email)
- Quarantine zone for failed records
- Schema validation and record freshness checks
- Unit testing using `pytest` and `unittest.mock`
