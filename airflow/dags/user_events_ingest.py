"""Pull user events from the upstream events API -> validate -> store in the warehouse."""
from __future__ import annotations

import time
from datetime import datetime, timedelta

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from platform_lib import DEFAULT_ARGS, UPSTREAM_URL, WAREHOUSE_CONN_ID, log, log_sla_miss

REQUEST_TIMEOUT_SECONDS = 10
REQUIRED_FIELDS = ("event_id", "user_id", "event_type", "ts")


@dag(
    dag_id="user_events_ingest",
    schedule="*/3 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    sla_miss_callback=log_sla_miss,
    tags=["events", "ingest"],
)
def user_events_ingest():
    @task(sla=timedelta(minutes=2))
    def fetch_events() -> list[dict]:
        url = f"{UPSTREAM_URL}/events"
        log.info("GET %s (timeout=%ss)", url, REQUEST_TIMEOUT_SECONDS)
        started = time.monotonic()
        try:
            resp = requests.get(url, params={"limit": 200}, timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.exceptions.Timeout:
            log.error("Upstream events API did not respond within %ss (waited %.1fs)",
                      REQUEST_TIMEOUT_SECONDS, time.monotonic() - started)
            raise
        log.info("Upstream responded %s in %.2fs", resp.status_code, time.monotonic() - started)
        if resp.status_code >= 400:
            log.error("Upstream error body: %s", resp.text[:300])
        resp.raise_for_status()
        events = resp.json()["events"]
        log.info("Fetched %d events", len(events))
        return events

    @task
    def validate(events: list[dict]) -> list[dict]:
        valid = [e for e in events if all(e.get(k) for k in REQUIRED_FIELDS)]
        log.info("%d/%d events passed validation", len(valid), len(events))
        if not valid:
            raise ValueError("No valid events in batch")
        return valid

    @task
    def store(events: list[dict]) -> int:
        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        hook.run("""CREATE TABLE IF NOT EXISTS user_events (
                      event_id text PRIMARY KEY, user_id text, event_type text, ts timestamptz,
                      ingested_at timestamptz DEFAULT now())""")
        hook.insert_rows("user_events", [(e["event_id"], e["user_id"], e["event_type"], e["ts"]) for e in events],
                         target_fields=["event_id", "user_id", "event_type", "ts"],
                         replace=True, replace_index=["event_id"])
        log.info("Stored %d events", len(events))
        return len(events)

    store(validate(fetch_events()))


user_events_ingest()
