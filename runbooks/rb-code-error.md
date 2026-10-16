---
id: rb-code-error
title: Pipeline task fails with a Python exception (code bug)
root_cause: code_error
symptoms:
  - Task log ends in a Python Traceback
  - KeyError, TypeError, AttributeError, NameError or ValueError raised from DAG or transform code
  - Failure started right after a deploy or code change
  - Every run fails at the same task with the same exception
---
# Pipeline task fails with a Python exception

The pipeline's own code raised an unhandled exception. Retrying will not help: the
same input hits the same bug every time.

## Diagnosis
1. Open the failed task's log and find the last `Traceback`. Note the exception type and the line of DAG code it points to.
2. A `KeyError` naming a column or field usually means the code and the data disagree on a name (a rename in code or in the source).
3. Check what changed recently: the last deploy of the DAG repo, library upgrades, or Variables the task reads.
4. Confirm the input data still has the expected shape (header row, field names).

## Fix
1. Roll back the most recent change to the pipeline code, or fix the field/attribute name it references.
2. Add a unit test that runs the transform on a sample of real input.
3. Rerun the failed task once the fix is deployed.
4. If you can't fix it quickly, pause the DAG so it stops paging and tell downstream consumers the data is stale.
