# ELT runbook

## First deployment

- Create the Snowflake database/schema and warehouse, then grant the Airflow
  role warehouse usage and permission to create schemas, stages, and tables.
- Configure the `Snowflake` Airflow connection with account, database, schema,
  warehouse, role, user, and password. Store credentials in Airflow's secret
  backend where available.
- Install the Snowflake Airflow provider and `dbt-snowflake` into the existing
  Airflow runtime with constraints matching its Airflow version.
- Deploy the repository and mount the CSV input directory to the worker.
  Configure `RETAILFLOW_DATA_DIR`, `RETAILFLOW_DBT_DIR`, and optionally
  `SNOWFLAKE_CONN_ID` on workers.
- Upload at least one headered `.csv` file and trigger `retailflow_daily_elt`
  manually before relying on its daily schedule.

## Troubleshooting

- **No CSV files found:** Confirm `RETAILFLOW_DATA_DIR` exists on the worker
  executing the ingestion task and contains `.csv` files.
- **Snowflake access denied:** Verify the connection fields, network access,
  warehouse usage, and privileges to create/use the target schema, stage, and
  raw tables.
- **Load fails after a header change:** Raw table schemas are created from the
  first header. Keep a stable header for each table; plan an explicit schema
  migration before changing a source contract.
- **dbt command or connection fails:** Confirm `dbt` and `dbt-snowflake` are
  installed in the Airflow runtime and that the connection's account, user,
  password, role, database, schema, and warehouse are available to the dbt
  task.
- **A source file was replaced:** Inputs should be immutable. Do not reuse a
  previously loaded filename with different contents; Snowflake may skip a
  file based on its load history.