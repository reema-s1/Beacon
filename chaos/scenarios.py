"""Fault scenarios with ground-truth labels (design section 5)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from chaos.platform import INCOMING, QUARANTINE_DIR, REPORTS_FILLER, Platform

BROKEN_CONN_ID = "warehouse_db_v2"


@dataclass(frozen=True)
class Scenario:
    name: str
    description: str
    ground_truth_cause: str
    expected_runbook: str
    target_dag: str | None
    inject: Callable[[Platform], None]


def _bad_code(p: Platform) -> None:
    p.set_variable("fault_bad_code", "true")


def _missing_input(p: Platform) -> None:
    p.exec_scheduler(f"mkdir -p {QUARANTINE_DIR} && if [ -f {INCOMING} ]; then mv {INCOMING} {QUARANTINE_DIR}/; fi")


def _upstream_down(p: Platform) -> None:
    p.set_upstream_mode("down")


def _upstream_slow(p: Platform) -> None:
    p.set_upstream_mode("slow")


def _bad_connection(p: Platform) -> None:
    p.create_connection({"connection_id": BROKEN_CONN_ID, "conn_type": "postgres", "host": "postgres",
                         "port": 5432, "schema": "warehouse", "login": "warehouse",
                         "password": "rotated-but-not-updated"})
    p.set_variable("sales_load_conn_id", BROKEN_CONN_ID)


def _resource_exhaustion(p: Platform) -> None:
    p.set_variable("fault_resource_exhaustion", "true")


def _scheduler_down(p: Platform) -> None:
    p.stop_scheduler()


def _disk_full(p: Platform) -> None:
    # dd stops with ENOSPC once the 8 MB tmpfs is full; that's the point.
    p.exec_scheduler(f"dd if=/dev/zero of={REPORTS_FILLER} bs=1M count=64 2>/dev/null; df -h /opt/airflow/reports")


SCENARIOS: dict[str, Scenario] = {s.name: s for s in [
    Scenario("bad_code", "Variable makes the sales clean step reference a renamed field (KeyError)",
             "code_error", "rb-code-error", "sales_daily_etl", _bad_code),
    Scenario("missing_input", "The sales CSV drop is moved out of the landing directory",
             "missing_upstream_data", "rb-missing-input", "sales_daily_etl", _missing_input),
    Scenario("upstream_down", "upstream-mock returns 503 for every request",
             "dependency_unavailable", "rb-upstream-down", "user_events_ingest", _upstream_down),
    Scenario("upstream_slow", "upstream-mock delays every response past the client timeout",
             "dependency_latency", "rb-upstream-slow", "user_events_ingest", _upstream_slow),
    Scenario("bad_connection", "The sales load is pointed at a connection with stale credentials",
             "credentials_or_connection", "rb-connection", "sales_daily_etl", _bad_connection),
    Scenario("resource_exhaustion", "feature_build allocates memory until it is killed",
             "resource_limit", "rb-resource", "feature_build", _resource_exhaustion),
    Scenario("scheduler_down", "The Airflow scheduler container is stopped",
             "scheduler_unhealthy", "rb-scheduler", None, _scheduler_down),
    Scenario("disk_full", "The size-limited reports volume used by report_export is filled",
             "storage_full", "rb-disk-full", "report_export", _disk_full),
]}


def reset_all(p: Platform) -> list[str]:
    """Undo every scenario. Idempotent."""
    done = []
    p.start_scheduler()
    done.append("scheduler running")
    for key in ("fault_bad_code", "fault_resource_exhaustion", "sales_load_conn_id"):
        p.delete_variable(key)
    p.delete_connection(BROKEN_CONN_ID)
    done.append("fault variables and broken connection removed")
    p.set_upstream_mode("healthy")
    done.append("upstream-mock healthy")
    p.exec_scheduler(f"if [ ! -f {INCOMING} ]; then if [ -f {QUARANTINE_DIR}/sales.csv ]; then "
                     f"mv {QUARANTINE_DIR}/sales.csv {INCOMING}; else python /opt/airflow/include/seed_data.py; fi; fi; "
                     f"rm -f {REPORTS_FILLER}")
    done.append("sales input restored, reports volume freed")
    return done
