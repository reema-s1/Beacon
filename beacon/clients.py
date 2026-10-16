"""Thin async clients for the Airflow REST API (2.10, /api/v1) and Prometheus HTTP API."""
from __future__ import annotations

from datetime import datetime
from typing import Any
from urllib.parse import quote

import httpx

from beacon.config import Settings, get_settings


class UpstreamError(RuntimeError):
    """Airflow or Prometheus could not be reached or returned an error."""


def _q(part: str) -> str:
    return quote(part, safe="")


def _iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


class AirflowClient:
    def __init__(self, settings: Settings | None = None, timeout: float = 15.0):
        s = settings or get_settings()
        self.base = s.airflow_url.rstrip("/") + "/api/v1"
        self.auth = (s.airflow_user, s.airflow_password)
        self.timeout = timeout

    async def _request(self, method: str, path: str, *, accept_text: bool = False, **kw) -> Any:
        headers = {"Accept": "text/plain"} if accept_text else {}
        try:
            async with httpx.AsyncClient(auth=self.auth, timeout=self.timeout) as client:
                resp = await client.request(method, self.base + path, headers=headers, **kw)
        except httpx.HTTPError as exc:
            raise UpstreamError(f"Airflow unreachable: {exc.__class__.__name__}: {exc}") from exc
        if resp.status_code >= 400:
            raise UpstreamError(f"Airflow {method} {path} -> {resp.status_code}: {resp.text[:200]}")
        if accept_text:
            return resp.text
        return resp.json() if resp.content else {}

    async def health(self) -> dict:
        return await self._request("GET", "/health")

    async def list_dags(self) -> list[dict]:
        data = await self._request("GET", "/dags", params={"limit": 100, "only_active": "true"})
        return data.get("dags", [])

    async def list_dag_runs(self, dag_id: str = "~", *, state: str | None = None,
                            start_date_gte: datetime | None = None, limit: int = 20,
                            order_by: str = "-execution_date") -> dict:
        params: dict[str, Any] = {"limit": limit, "order_by": order_by}
        if state:
            params["state"] = state
        if start_date_gte:
            params["start_date_gte"] = _iso(start_date_gte)
        return await self._request("GET", f"/dags/{_q(dag_id)}/dagRuns", params=params)

    async def get_dag_run(self, dag_id: str, run_id: str) -> dict:
        return await self._request("GET", f"/dags/{_q(dag_id)}/dagRuns/{_q(run_id)}")

    async def task_instances(self, dag_id: str, run_id: str) -> list[dict]:
        data = await self._request("GET", f"/dags/{_q(dag_id)}/dagRuns/{_q(run_id)}/taskInstances")
        return data.get("task_instances", [])

    async def task_instance(self, dag_id: str, run_id: str, task_id: str) -> dict:
        return await self._request("GET", f"/dags/{_q(dag_id)}/dagRuns/{_q(run_id)}/taskInstances/{_q(task_id)}")

    async def task_log(self, dag_id: str, run_id: str, task_id: str, try_number: int) -> str:
        return await self._request(
            "GET", f"/dags/{_q(dag_id)}/dagRuns/{_q(run_id)}/taskInstances/{_q(task_id)}/logs/{try_number}",
            params={"full_content": "true"}, accept_text=True)

    async def clear_task(self, dag_id: str, run_id: str, task_id: str) -> dict:
        body = {"dry_run": False, "task_ids": [task_id], "dag_run_id": run_id, "only_failed": False,
                "include_downstream": True, "include_upstream": False, "reset_dag_runs": True}
        return await self._request("POST", f"/dags/{_q(dag_id)}/clearTaskInstances", json=body)


class PrometheusClient:
    def __init__(self, settings: Settings | None = None, timeout: float = 15.0):
        s = settings or get_settings()
        self.base = s.prometheus_url.rstrip("/") + "/api/v1"
        self.timeout = timeout

    async def _get(self, path: str, params: dict) -> dict:
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.get(self.base + path, params=params)
        except httpx.HTTPError as exc:
            raise UpstreamError(f"Prometheus unreachable: {exc.__class__.__name__}: {exc}") from exc
        body = resp.json() if resp.content else {}
        if resp.status_code >= 400 or body.get("status") != "success":
            raise UpstreamError(f"Prometheus error {resp.status_code}: {body.get('error', resp.text[:200])}")
        return body["data"]

    async def query(self, promql: str) -> dict:
        return await self._get("/query", {"query": promql})

    async def query_range(self, promql: str, start: datetime, end: datetime, step_seconds: int) -> dict:
        return await self._get("/query_range", {"query": promql, "start": start.timestamp(),
                                                "end": end.timestamp(), "step": step_seconds})
