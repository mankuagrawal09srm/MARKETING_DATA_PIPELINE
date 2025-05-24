Marketing Data Pipeline: 

This project implements a data pipeline to process and load marketing data from AWS S3 into Snowflake. It includes ingestion of CSV and JSON data, data quality (DQ) checks, idempotent loading, logging, and basic performance tuning using Snowflake best practices.

Project Structure

Marketing_data_pipeline/
├── data/                         # Sample input files (CSV, JSON)
├── docs/                         # Documentation (architecture, data model)
│   ├── architecture_overview.md
│   └── data_model.md
├── logs/                         # Log outputs
│   └── ingestion.log
├── src/                          
│   ├── dq_checks.py              # Data quality checks
│   ├── data_ingestion.py              # Ingestion logic
│   ├── utils.py                  # Helper functions (Snowflake connection, logger)
│   ├── main.py                   # Script to orchestrate the pipeline
|   ├── feature_engineering.py    # script to test conceptual features
|   └── transformations.py        # Future implementation in case of large data ingestion.
├── .env                          # Environment variables (Snowflake credentials)
├── README.md

Overview: 
This pipeline is designed to load marketing engagement data 
(customer demographics and clickstream events) from S3 into Snowflake. The processed data can be used in downstream dashboards and machine learning models.

Key features:
Loads structured (CSV) and semi-structured (JSON) data.

Performs data quality checks to validate the integrity of incoming data.

Ensures idempotency by truncating the target tables before reloading.

Implements logging to both a local file and optionally to a Snowflake logging table.

Applies basic performance optimizations like clustering.

Input Datasets
CSV file: Contains customer demographics such as customer_id, first_name, email, signup_date.

JSON file: Contains clickstream event data such as event_type, page_url, duration_ms.

Both datasets are placed in an S3 bucket and accessed through Snowflake external stages.

How to Run the Pipeline
1. Clone the repository

git clone https://github.com/yourusername/Marketing_data_pipeline.git
cd Marketing_data_pipeline

2. (Optional) Create and activate a virtual environment

python -m venv venv
source venv/bin/activate    # On Windows, use venv\Scripts\activate

3. Install Python dependencies

pip install -r requirements.txt

4. Set up your environment file
Create a .env file in the project root with the following content:

SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
5. Run the pipeline

python src/main.py

This script will:
Load CSV and JSON data into Snowflake.

Run data quality checks.

Log results to the local log file and optionally into Snowflake.

Features Implemented:
Ingestion Logic
Loads CSV and JSON files using Snowflake’s COPY INTO command from external stages.

Tables are created if they do not exist.

Existing data is truncated before load to ensure idempotency.

Data Quality Checks
Null check on critical columns like customer_id, email, and signup_date.

Uniqueness check on customer_id.

Row count check to verify records were loaded.

Implemented in dq_checks.py.

Logging
Pipeline logs are stored in logs/ingestion.log.

Optionally logs execution metadata to a Snowflake table PIPELINE_LOGS if created.

Performance Optimization
A clustering key is added on signup_date in the raw_customer_demographics table to improve query performance.

sql
ALTER TABLE raw_customer_demographics CLUSTER BY (customer_id);

Documentation
Refer to the following files inside the docs/ directory:

architecture_overview.md: Explains the design and flow of the pipeline.

data_model.md: Details the structure of the input and output tables.

Technologies Used
Python 3.9+

Snowflake (data warehouse)

AWS S3 (data source)

Logging module (Python standard library)

dotenv for managing credentials via .env file

Potential Future Enhancements
Add orchestration using Apache Airflow or Step Functions

Implement alerting for failed data quality checks (e.g., via Slack, email)

Store failed records separately for quarantine and debugging

Add unit tests using pytest and unittest.mock

Add schema validation and record freshness checks