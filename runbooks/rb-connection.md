---
id: rb-connection
title: Database connection or credentials rejected
root_cause: credentials_or_connection
symptoms:
  - password authentication failed for user
  - OperationalError connecting to Postgres or the warehouse
  - The conn_id isn't defined, or connection refused on the database port
  - Load task fails at connect time, before any rows are written
---
# Database connection or credentials rejected

The task couldn't open a connection to the database because the Airflow connection it
uses is wrong: bad password, wrong host/port, missing connection, or a rotated secret.

## Diagnosis
1. In the load task log, find which Airflow connection id the task used and the exact database error.
2. In Airflow (Admin → Connections) compare that connection with the known-good warehouse connection.
3. Check whether the credentials were rotated recently, or whether a Variable now points the task at a different connection id.

## Fix
1. Point the task back at the correct connection, or update the connection's credentials from the secret store.
2. Test the connection (Admin → Connections → Test) before rerunning.
3. Rerun the failed load task.
4. Never paste credentials into Slack or tickets; rotate them if they were exposed.
