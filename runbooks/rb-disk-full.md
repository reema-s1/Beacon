---
id: rb-disk-full
title: Disk or volume is full
root_cause: storage_full
symptoms:
  - OSError Errno 28 No space left on device
  - Writing a file fails, volume free space is zero
  - Report or export task fails while writing output
  - Disk usage at 100 percent on a shared volume
---
# Disk or volume is full

The volume the task writes to has no free space left. The task itself is fine; there is
nowhere to put its output.

## Diagnosis
1. In the task log, find the `No space left on device` error and the path being written.
2. Check the volume's free/total bytes (the task logs them before writing) and what is taking up the space.
3. Look for large unexpected files, failed cleanups, or old outputs that were never pruned.

## Fix
1. Delete or archive files that are not needed on the volume (old exports, stray temp files).
2. Rerun the failed task once space is free.
3. Add retention/pruning for that directory, or move outputs to object storage, so it does not fill up again.
