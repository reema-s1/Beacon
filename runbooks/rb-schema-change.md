---
id: rb-schema-change
title: Upstream table schema changed
root_cause: schema_change
symptoms:
  - UndefinedColumn column does not exist
  - DatatypeMismatch or invalid input syntax for type
  - Load or query fails after an upstream team migrated a table
  - Columns added, dropped or renamed in a source table
---
# Upstream table schema changed

A table the pipeline reads or writes changed shape (column renamed, dropped or retyped)
and the pipeline's SQL no longer matches it.

## Diagnosis
1. Find the column and table named in the database error.
2. Compare the current table definition with what the pipeline expects.
3. Find the migration that changed it and who owns it.

## Fix
1. Update the pipeline's SQL/mapping to the new schema, or ask the owner to add a compatibility view.
2. Rerun the failed task and backfill any missed windows.
