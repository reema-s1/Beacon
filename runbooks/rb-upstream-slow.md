---
id: rb-upstream-slow
title: External dependency is slow (timeouts, SLA misses)
root_cause: dependency_latency
symptoms:
  - ReadTimeout or "Read timed out" calling an upstream API
  - Request did not respond within the timeout
  - Task duration far above its normal baseline
  - SLA missed on an ingest task
---
# External dependency is slow

The service responds, but too slowly. Requests time out or tasks run so long that they
miss their SLA. The service is up, so this is not an outage.

## Diagnosis
1. In the fetch task log, look for timeout errors and how long the call waited.
2. Compare the task's recent duration with its baseline (Prometheus `airflow_task_duration`).
3. Check the dependency's latency dashboards or status page.

## Fix
1. Tell the owning team about the latency, with timings from the log.
2. If the slowness is expected for a while, raise the client timeout and the task's SLA temporarily, then rerun.
3. Fetch in smaller pages so each call finishes within the timeout.
4. Revert the temporary timeout change once latency is back to normal.
