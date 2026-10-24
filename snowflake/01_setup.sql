-- Example initial objects. Create a warehouse separately and grant the
-- Airflow role the required database/schema/stage/table privileges.
CREATE DATABASE IF NOT EXISTS RETAILFLOW;
CREATE SCHEMA IF NOT EXISTS RETAILFLOW.RAW;
