"""Daily CSV ingestion and dbt build for Retailflow."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_ROOT))

from ingestion.load_to_snowflake import load_csvs  # noqa: E402

SNOWFLAKE_CONN_ID = os.environ.get("SNOWFLAKE_CONN_ID", "snowflake_default")


def _snowflake_settings() -> tuple[str, str, dict[str, str]]:
    connection = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    extra = connection.extra_dejson
    database = extra.get("database")
    schema = extra.get("schema") or connection.schema
    account = extra.get("account") or connection.host
    warehouse = extra.get("warehouse")
    role = extra.get("role")
    if not all((database, schema, account, warehouse, role, connection.login, connection.password)):
        raise ValueError(
            "Airflow Snowflake connection must define account, database, schema, "
            "warehouse, role, username, and password"
        )

    environment = {
        "SNOWFLAKE_ACCOUNT": account,
        "SNOWFLAKE_USER": connection.login,
        "SNOWFLAKE_PASSWORD": connection.password,
        "SNOWFLAKE_DATABASE": database,
        "SNOWFLAKE_SCHEMA": schema,
        "SNOWFLAKE_WAREHOUSE": warehouse,
        "SNOWFLAKE_ROLE": role,
    }
    return database, schema, environment


def ingest_csv_files() -> None:
    data_dir_value = os.environ.get("RETAILFLOW_DATA_DIR")
    if not data_dir_value:
        raise ValueError("RETAILFLOW_DATA_DIR must point to the worker's mounted CSV directory")

    database, schema, _ = _snowflake_settings()
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    connection = hook.get_conn()
    cursor = connection.cursor()
    try:
        loaded_tables = load_csvs(Path(data_dir_value), cursor, database, schema)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()

    print(f"Processed CSV files into raw tables: {', '.join(loaded_tables)}")


def run_dbt_build() -> None:
    dbt_dir_value = os.environ.get("RETAILFLOW_DBT_DIR")
    if not dbt_dir_value:
        raise ValueError("RETAILFLOW_DBT_DIR must point to the dbt project on the worker")

    _, _, snowflake_environment = _snowflake_settings()
    environment = os.environ.copy()
    environment.update(snowflake_environment)
    dbt_dir = Path(dbt_dir_value).expanduser().resolve()
    if not dbt_dir.is_dir():
        raise FileNotFoundError(f"dbt project directory does not exist: {dbt_dir}")

    subprocess.run(
        ["dbt", "build", "--project-dir", str(dbt_dir), "--profiles-dir", str(dbt_dir)],
        cwd=dbt_dir,
        env=environment,
        check=True,
    )


with DAG(
    dag_id="retailflow_daily_elt",
    description="Load CSV files into Snowflake and run dbt transformations",
    start_date=datetime(2024, 1, 1),
    schedule="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["retailflow", "snowflake", "dbt"],
) as dag:
    ingest = PythonOperator(
        task_id="ingest_csv_to_snowflake",
        python_callable=ingest_csv_files,
    )
    transform = PythonOperator(
        task_id="dbt_build",
        python_callable=run_dbt_build,
    )

    ingest >> transform