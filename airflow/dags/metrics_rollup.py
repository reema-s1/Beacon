"""Hourly-style rollup of event counts per event type."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from platform_lib import DEFAULT_ARGS, WAREHOUSE_CONN_ID, log, log_sla_miss


@dag(
    dag_id="metrics_rollup",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    sla_miss_callback=log_sla_miss,
    tags=["metrics"],
)
def metrics_rollup():
    @task
    def count_events() -> list[list]:
        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        hook.run("""CREATE TABLE IF NOT EXISTS user_events (
                      event_id text PRIMARY KEY, user_id text, event_type text, ts timestamptz,
                      ingested_at timestamptz DEFAULT now())""")
        rows = hook.get_records("""SELECT date_trunc('hour', ts) AS hour, event_type, count(*)
                                   FROM user_events WHERE ts > now() - interval '6 hours'
                                   GROUP BY 1, 2 ORDER BY 1, 2""")
        log.info("Counted %d (hour, event_type) buckets", len(rows))
        return [[r[0].isoformat(), r[1], r[2]] for r in rows]

    @task
    def rollup(buckets: list[list]) -> list[list]:
        totals: dict[str, int] = {}
        for hour, _, n in buckets:
            totals[hour] = totals.get(hour, 0) + n
        log.info("Rolled up %d hours, %d events total", len(totals), sum(totals.values()))
        return buckets + [[hour, "_all", n] for hour, n in totals.items()]

    @task(sla=timedelta(minutes=4))
    def publish(rows: list[list]) -> int:
        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        hook.run("""CREATE TABLE IF NOT EXISTS event_rollups (
                      hour timestamptz, event_type text, events int, PRIMARY KEY (hour, event_type))""")
        hook.insert_rows("event_rollups", [tuple(r) for r in rows], target_fields=["hour", "event_type", "events"],
                         replace=True, replace_index=["hour", "event_type"])
        log.info("Published %d rollup rows", len(rows))
        return len(rows)

    publish(rollup(count_events()))


metrics_rollup()
