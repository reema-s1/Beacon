"""Read loaded warehouse tables -> compute per-user features -> write the feature table."""
from __future__ import annotations

import os
import signal
import threading
import time
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from platform_lib import DEFAULT_ARGS, WAREHOUSE_CONN_ID, fault, log, log_sla_miss

# Per-task memory budget. In production the container runtime OOM-kills a task above
# its limit; the watchdog plays that role here so a runaway task can't take the
# whole scheduler container down with it.
TASK_MEMORY_LIMIT_MB = int(os.getenv("TASK_MEMORY_LIMIT_MB", "300"))


def _rss_mb() -> float:
    with open("/proc/self/statm") as f:
        pages = int(f.read().split()[1])
    return pages * os.sysconf("SC_PAGE_SIZE") / 1024 / 1024


def _oom_watchdog() -> None:
    while True:
        if _rss_mb() > TASK_MEMORY_LIMIT_MB:
            os.kill(os.getpid(), signal.SIGKILL)
        time.sleep(0.05)


@dag(
    dag_id="feature_build",
    schedule="*/10 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    sla_miss_callback=log_sla_miss,
    tags=["ml", "features"],
)
def feature_build():
    @task
    def read_tables() -> list[tuple]:
        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        hook.run("""CREATE TABLE IF NOT EXISTS user_events (
                      event_id text PRIMARY KEY, user_id text, event_type text, ts timestamptz,
                      ingested_at timestamptz DEFAULT now())""")
        rows = hook.get_records("""SELECT user_id, event_type, count(*) FROM user_events
                                   WHERE ts > now() - interval '1 day' GROUP BY 1, 2""")
        log.info("Read %d (user, event_type) groups from user_events", len(rows))
        return [tuple(r) for r in rows]

    @task(sla=timedelta(minutes=8))
    def compute_features(groups: list[tuple]) -> list[dict]:
        features: dict[str, dict] = {}
        for user_id, event_type, n in groups:
            f = features.setdefault(user_id, {"user_id": user_id, "events": 0, "purchases": 0})
            f["events"] += n
            if event_type == "purchase":
                f["purchases"] += n
        if fault("resource_exhaustion"):
            # Dense user x user interaction matrix: grows without bound for large user sets.
            log.info("Building dense interaction matrix for %d users", len(features))
            threading.Thread(target=_oom_watchdog, daemon=True).start()
            matrix = []
            while True:
                matrix.append(bytearray(20 * 1024 * 1024))
                log.info("Interaction matrix buffer at %d MB (rss=%.0f MB)", len(matrix) * 20, _rss_mb())
                time.sleep(0.2)
        out = [dict(f, conversion=round(f["purchases"] / f["events"], 4)) for f in features.values()]
        log.info("Computed features for %d users", len(out))
        return out

    @task
    def write_features(rows: list[dict]) -> int:
        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        hook.run("""CREATE TABLE IF NOT EXISTS user_features (
                      user_id text PRIMARY KEY, events int, purchases int, conversion numeric,
                      built_at timestamptz DEFAULT now())""")
        hook.insert_rows("user_features",
                         [(r["user_id"], r["events"], r["purchases"], r["conversion"]) for r in rows],
                         target_fields=["user_id", "events", "purchases", "conversion"],
                         replace=True, replace_index=["user_id"])
        log.info("Wrote %d feature rows", len(rows))
        return len(rows)

    write_features(compute_features(read_tables()))


feature_build()
