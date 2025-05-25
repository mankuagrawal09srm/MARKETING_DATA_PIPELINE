                start
                  |
        -----------------------
        |                     |
   ingest_csv            ingest_json
        |                     |
        -----------+----------
                    |
              run_dq_checks
                    |
           log_status_to_snowflake
                    |
        (optional) send_alert


you can wrap this DAG around your existing functions in main.py