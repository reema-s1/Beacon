"""Shared helpers for the demo pipelines.

Fault switches live in Airflow Variables (fault_<name> = "true") so the chaos CLI
can turn failures on and off without touching DAG code.
"""
from __future__ import annotations

import logging
import os
from datetime import timedelta
from pathlib import Path

from airflow.models import Variable

log = logging.getLogger("airflow.task")

DATA_DIR = Path(os.getenv("BEACON_DATA_DIR", "/opt/airflow/data"))
INCOMING_DIR = DATA_DIR / "incoming"
STAGING_DIR = DATA_DIR / "staging"
REPORTS_DIR = Path(os.getenv("BEACON_REPORTS_DIR", "/opt/airflow/reports"))
UPSTREAM_URL = os.getenv("UPSTREAM_URL", "http://upstream-mock:8000")
WAREHOUSE_CONN_ID = "warehouse_db"

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}


def fault(name: str) -> bool:
    """True when the chaos CLI has switched on fault `name`."""
    return Variable.get(f"fault_{name}", default_var="false").lower() == "true"


def warehouse_conn_id() -> str:
    """Connection used for warehouse loads. Overridable so a broken connection can be swapped in."""
    return Variable.get("sales_load_conn_id", default_var=WAREHOUSE_CONN_ID)


def log_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    log.warning("SLA missed on DAG %s for tasks: %s", dag.dag_id, task_list)
