# ELT architecture

1. **Extract and load:** The Airflow DAG scans the worker-mounted
   `RETAILFLOW_DATA_DIR` for CSV files. The loader creates a raw table for each
   filename, uses the CSV header for column names, creates columns as
   `VARCHAR`, uploads the file to an internal Snowflake stage, and executes
   `COPY INTO`.
2. **Transform:** After ingestion succeeds, the DAG launches `dbt build` from
   `RETAILFLOW_DBT_DIR`. dbt obtains its connection settings from the Airflow
   Snowflake connection. Models are views by default.
3. **Operate:** `retailflow_daily_elt` runs once daily at 05:00 UTC with
   one retry. Airflow workers must have the repository, dbt executable, and
   the same mounted input directory available.

Raw data is append-oriented. Use unique, immutable filenames for each batch;
Snowflake's COPY load history is used to avoid reloading files already seen.
The pipeline does not infer business types or transformations. Define source
schemas and typed dbt models after the CSV contracts are known.