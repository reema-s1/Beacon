---
id: rb-missing-input
title: Expected input file or upstream data is missing
root_cause: missing_upstream_data
symptoms:
  - FileNotFoundError or "No such file or directory" in the extract task
  - Expected input file not found
  - Upstream drop has not arrived, landing directory is empty
  - Extract task fails but downstream code was not changed
---
# Expected input file or upstream data is missing

The pipeline started but the file or dataset it reads from was not there. The code is
fine; the producer of the data didn't deliver it (late, renamed, deleted or written elsewhere).

## Diagnosis
1. In the extract task log, find the path the task tried to read and the list of files that were present.
2. Check whether the producer is late (compare with its normal delivery time) or wrote the file under a different name or location.
3. Check whether someone archived, moved or cleaned up the landing directory.

## Fix
1. Ask the upstream producer to (re)deliver the file to the expected path, or copy it from its archive location.
2. Once the file is present, rerun the extract task; downstream tasks will follow.
3. If late deliveries are common, add a FileSensor in front of the extract task instead of failing outright.
