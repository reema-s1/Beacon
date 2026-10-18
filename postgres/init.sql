-- Passwords are deliberately not plain words: Airflow masks every occurrence of a
-- connection password in task logs, so a password like 'warehouse' would turn every
-- mention of the word into '***'.
-- One Postgres instance, three databases: Airflow metadata, Beacon's incident
-- store, and the "warehouse" the demo pipelines load into.
CREATE USER airflow WITH PASSWORD 'af-meta-7Qx2';
CREATE DATABASE airflow OWNER airflow;

CREATE USER beacon WITH PASSWORD 'beacon-db-3Rt8';
CREATE DATABASE beacon OWNER beacon;

CREATE USER warehouse WITH PASSWORD 'wh-load-9Kp4';
CREATE DATABASE warehouse OWNER warehouse;
