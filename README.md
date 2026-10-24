# Retailflow data platform

Retailflow uses Airflow to load CSV files into Snowflake raw tables and then
run dbt transformations. Each CSV becomes a table named after its filename;
header values become column names and are stored as `VARCHAR`. Table and
column names are normalized to uppercase letters, digits, and underscores.
Add typed staging and business models to dbt once the source schemas are
confirmed.

## Configure

1. Provision a Snowflake database and schema, for example by running
   [`snowflake/01_setup.sql`](./snowflake/01_setup.sql). Create a warehouse
   separately and grant the Airflow role permission to use it and create
   schemas, stages, and tables in the target database.
2. Install [`requirements-airflow.txt`](./requirements-airflow.txt) into the
   existing Airflow environment, using constraints compatible with its
   installed Airflow version.
3. Create an Airflow connection (default ID `snowflake_default`, type
   `Snowflake`). Set its host to the Snowflake account identifier, login and
   password to the service user, and schema to the target schema. Set these
   connection extras:

   ```json
   {
     "account": "your-account-identifier",
     "database": "RETAILFLOW",
     "schema": "RAW",
     "warehouse": "RETAILFLOW_WH",
     "role": "RETAILFLOW_INGEST_ROLE"
   }
   ```

   Keep the password in the Airflow connection/secret backend; do not put it
   in source control or a `.env` file.
4. Deploy this repository so the Airflow worker can import `ingestion/` and
   access `retailflow_dbt/`. Make the same CSV directory available to workers
   and set `RETAILFLOW_DATA_DIR` to its path. Set `RETAILFLOW_DBT_DIR` to the
   deployed dbt project and, if needed, `SNOWFLAKE_CONN_ID` to the connection
   ID from step 3. `.env.example` lists these non-secret settings.
5. Place immutable CSV files in the input directory. Each file must have a
   header row; use a new filename for each new batch. The DAG runs daily at
   05:00 UTC and loads files before calling `dbt build`.

The DAG is `retailflow_daily_elt`. It intentionally fails when the input
directory has no CSVs, when loading fails, or when dbt exits unsuccessfully.
Snowflake tracks loaded staged files, so do not overwrite previously ingested
filenames with changed contents.

## Local checks

Run the loader's unit tests with `python -m unittest discover -s tests`.
The actual DAG requires Airflow, its Snowflake provider, the configured
connection, a reachable Snowflake account, and CSV files mounted on a worker.
