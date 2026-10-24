-- Raw tables are created dynamically by ingestion/load_to_snowflake.py:
-- one table per CSV, with VARCHAR columns inferred from the header row.
-- The internal stage is also created by the loader, but may be provisioned
-- ahead of time with this statement.
CREATE STAGE IF NOT EXISTS RETAILFLOW.RAW.RETAILFLOW_INGEST;