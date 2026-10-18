---
id: rb-resource
title: Task killed for running out of memory
root_cause: resource_limit
symptoms:
  - Task exited with return code -9 (Negsignal.SIGKILL)
  - Killed, out of memory, OOM killer, MemoryError
  - Memory usage grows steadily in the log before the task dies
  - Task dies without a Python traceback
---
# Task killed for running out of memory

The task used more memory than it is allowed and the runtime killed it (SIGKILL). There
is usually no Python traceback because the process was killed from outside.

## Diagnosis
1. In the task log, look for `return code -9` / `SIGKILL` and the last memory figures logged before it.
2. Check whether the input grew (more users, more rows) or a code change made the job build something much larger in memory.
3. Check the host/container memory metrics for the time of the failure.

## Fix
1. Process the data in chunks or stream it instead of materialising everything in memory.
2. As a short-term fix, give the task more memory (pool / executor config) and rerun it.
3. Revert the change that made memory grow, if there was one.
