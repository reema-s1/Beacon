"""Levers the chaos CLI pulls: Airflow Variables/Connections, upstream-mock mode, and the
scheduler container (via the Docker API)."""
from __future__ import annotations

import os
import time
from urllib.parse import quote

import httpx

from beacon.config import get_settings

SCHEDULER_SERVICE = "airflow-scheduler"
INCOMING = "/opt/airflow/data/incoming/sales.csv"
QUARANTINE_DIR = "/opt/airflow/data/.quarantine"
REPORTS_FILLER = "/opt/airflow/reports/.reserved"


class PlatformError(RuntimeError):
    pass


class Platform:
    def __init__(self, project: str | None = None):
        s = get_settings()
        self.airflow = httpx.Client(base_url=s.airflow_url.rstrip("/") + "/api/v1",
                                    auth=(s.airflow_user, s.airflow_password), timeout=20.0)
        self.upstream_url = s.upstream_url.rstrip("/")
        self.project = project or os.getenv("COMPOSE_PROJECT_NAME", "beacon")
        self._docker = None

    # ── Airflow ───────────────────────────────────────────────────────────────

    def _af(self, method: str, path: str, ok: tuple[int, ...] = (), **kw) -> dict:
        resp = self.airflow.request(method, path, **kw)
        if resp.status_code >= 400 and resp.status_code not in ok:
            raise PlatformError(f"Airflow {method} {path} -> {resp.status_code}: {resp.text[:200]}")
        return resp.json() if resp.content and resp.status_code < 400 else {}

    def set_variable(self, key: str, value: str) -> None:
        self._af("DELETE", f"/variables/{key}", ok=(404,))
        self._af("POST", "/variables", json={"key": key, "value": value})

    def delete_variable(self, key: str) -> None:
        self._af("DELETE", f"/variables/{key}", ok=(404,))

    def create_connection(self, conn: dict) -> None:
        self._af("DELETE", f"/connections/{conn['connection_id']}", ok=(404,))
        self._af("POST", "/connections", json=conn)

    def delete_connection(self, conn_id: str) -> None:
        self._af("DELETE", f"/connections/{conn_id}", ok=(404,))

    def trigger_dag(self, dag_id: str) -> str:
        self._af("PATCH", f"/dags/{dag_id}", params={"update_mask": "is_paused"}, json={"is_paused": False})
        run = self._af("POST", f"/dags/{dag_id}/dagRuns", json={"conf": {}})
        return run["dag_run_id"]

    def dag_run_state(self, dag_id: str, run_id: str) -> str:
        return self._af("GET", f"/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}").get("state", "unknown")

    def scheduler_healthy(self) -> bool:
        try:
            return self._af("GET", "/health").get("scheduler", {}).get("status") == "healthy"
        except (PlatformError, httpx.HTTPError):
            return False

    # ── upstream-mock ─────────────────────────────────────────────────────────

    def set_upstream_mode(self, mode: str) -> None:
        resp = httpx.post(f"{self.upstream_url}/admin/mode", json={"mode": mode}, timeout=10.0)
        resp.raise_for_status()

    def upstream_mode(self) -> str:
        return httpx.get(f"{self.upstream_url}/admin/mode", timeout=10.0).json()["mode"]

    # ── Scheduler container ───────────────────────────────────────────────────

    @property
    def docker(self):
        if self._docker is None:
            import docker
            self._docker = docker.from_env()
        return self._docker

    def scheduler(self):
        found = self.docker.containers.list(all=True, filters={"label": [
            f"com.docker.compose.project={self.project}", f"com.docker.compose.service={SCHEDULER_SERVICE}"]})
        if not found:
            raise PlatformError(f"no {SCHEDULER_SERVICE} container for compose project '{self.project}'")
        return found[0]

    def stop_scheduler(self) -> None:
        self.scheduler().stop(timeout=10)

    def start_scheduler(self, wait_seconds: int = 120) -> None:
        c = self.scheduler()
        c.reload()
        if c.status != "running":
            c.start()
        deadline = time.monotonic() + wait_seconds
        while time.monotonic() < deadline:
            c.reload()
            if c.status == "running":
                return
            time.sleep(2)
        raise PlatformError("scheduler container did not start")

    def exec_scheduler(self, shell: str) -> str:
        code, out = self.scheduler().exec_run(["bash", "-c", shell], user="airflow")
        text = out.decode(errors="replace") if out else ""
        if code != 0:
            raise PlatformError(f"exec failed ({code}): {text[:300]}")
        return text

    # ── Health ────────────────────────────────────────────────────────────────

    def wait_scheduler_healthy(self, timeout: int = 180) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.scheduler_healthy():
                return True
            time.sleep(5)
        return False

    def wait_dag_success(self, dag_id: str, timeout: int = 300) -> bool:
        run_id = self.trigger_dag(dag_id)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = self.dag_run_state(dag_id, run_id)
            if state == "success":
                return True
            if state == "failed":
                return False
            time.sleep(5)
        return False
