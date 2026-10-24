# dbt models

The CSV loader creates one raw table per file in the configured Snowflake
database and schema. It infers column names from each CSV header and stores
every source value as `VARCHAR`.

Add dbt source declarations and typed staging models here after the incoming
file names and column definitions are agreed. For example, a source table
derived from `orders.csv` is named `ORDERS`; use the same database and schema
as the Airflow Snowflake connection. Avoid adding guessed business logic or
types before the source schema is known.
