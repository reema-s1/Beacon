-- One Postgres instance, three databases: Airflow metadata, Beacon's incident
-- store, and the "warehouse" the demo pipelines load into.
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

CREATE USER beacon WITH PASSWORD 'beacon';
CREATE DATABASE beacon OWNER beacon;

CREATE USER warehouse WITH PASSWORD 'warehouse';
CREATE DATABASE warehouse OWNER warehouse;
