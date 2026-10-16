---
id: rb-dag-import-error
title: DAG fails to import (broken DAG)
root_cause: dag_import_error
symptoms:
  - Broken DAG banner in the Airflow UI
  - dag_processing import_errors above zero
  - ModuleNotFoundError or SyntaxError while parsing the DAG file
  - DAG disappears from the UI or stops being scheduled
---
# DAG fails to import

Airflow cannot parse the DAG file, so the DAG is not scheduled at all. There are no
failed task logs because no task ever starts.

## Diagnosis
1. Check the import error shown in the UI (Browse → DAG import errors) or `airflow dags list-import-errors`.
2. Look for a missing dependency, a syntax error or a top-level call that fails at parse time.

## Fix
1. Fix or revert the DAG file; install any missing package in the Airflow image.
2. Confirm the DAG shows up again and trigger it if a run was missed.
