---
id: rb-data-quality
title: Data quality check failed
root_cause: data_quality
symptoms:
  - Validation task fails with No valid events in batch
  - Row counts far below normal, empty batch, null required fields
  - Duplicate keys or unexpected values detected by a check
  - Pipeline ran fine but the data looks wrong
---
# Data quality check failed

The pipeline ran, but a validation step rejected the data. The producer sent data that
breaks the contract (empty, nulls, duplicates, out-of-range values).

## Diagnosis
1. Read the validation task log: which check failed and on how many rows.
2. Sample the raw input to see the bad records.
3. Ask the producer whether something changed on their side.

## Fix
1. Have the producer correct and resend the data, then rerun from the extract task.
2. If the check itself is wrong, fix the check and document the new expectation.
