---
id: rb-upstream-down
title: External dependency is down (HTTP 5xx / connection refused)
root_cause: dependency_unavailable
symptoms:
  - HTTPError 503 Service Unavailable from an upstream API
  - 500, 502 or 504 errors returned by an external service
  - ConnectionError, connection refused, max retries exceeded to an external host
  - Ingest task fails immediately while the code is unchanged
---
# External dependency is down

A service the pipeline calls is returning errors (5xx) or refusing connections. The
failure is outside the pipeline; retries fail until the service recovers.

## Diagnosis
1. In the fetch task log, find the URL called and the HTTP status code or connection error.
2. Call the service's health endpoint, or check its status page / on-call channel.
3. Confirm the failure is fast (an error response), not slow: a timeout points to the latency runbook instead.

## Fix
1. Contact the owning team; link the incident and the failing endpoint.
2. Leave the DAG scheduled with retries; once the service is healthy, rerun the failed task to backfill the missed window.
3. If the outage is long, pause the DAG and tell downstream users their data will be late.
4. Consider a circuit breaker / longer retry backoff for this dependency.
