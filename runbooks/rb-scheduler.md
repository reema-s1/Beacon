---
id: rb-scheduler
title: Airflow scheduler is down or not heartbeating
root_cause: scheduler_unhealthy
symptoms:
  - No scheduler heartbeat for 60 seconds
  - Airflow health endpoint reports scheduler unhealthy
  - DAG runs stay queued and nothing starts
  - All pipelines stop at the same time
---
# Airflow scheduler is down or not heartbeating

The scheduler process is not running (or is stuck), so no new tasks start. Every
pipeline is affected at once, but no individual task shows an error.

## Diagnosis
1. Check `/api/v1/health`: the scheduler status and the time of the latest heartbeat.
2. Check the scheduler container/pod: is it running, restarting, or OOM-killed?
3. Look at the scheduler's own logs for a crash or a lost metadata-database connection.

## Fix
1. Restart the scheduler (`docker compose start airflow-scheduler`, or restart the pod).
2. Confirm heartbeats resume and queued runs start.
3. Missed runs are caught up automatically or can be triggered by hand; check that critical DAGs ran.
4. If it keeps crashing, check the metadata database and the scheduler's resources.
