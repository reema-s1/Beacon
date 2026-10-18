"""Synchronous client for beacon-api, with errors turned into messages a user can read."""
from __future__ import annotations

import httpx


class BeaconAPIError(RuntimeError):
    """Carries a user-facing message."""


class BeaconAPI:
    def __init__(self, base_url: str, timeout: float = 240.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _call(self, method: str, path: str, **kw) -> dict | list:
        try:
            with httpx.Client(base_url=self.base_url, timeout=self.timeout) as client:
                resp = client.request(method, path, **kw)
        except httpx.TimeoutException as exc:
            raise BeaconAPIError("Beacon took too long to answer. Please try again in a minute.") from exc
        except httpx.HTTPError as exc:
            raise BeaconAPIError("Beacon's API can't be reached right now. The on-call team has been notified "
                                 "via monitoring; please try again shortly.") from exc
        if resp.status_code >= 400:
            try:
                detail = resp.json().get("detail")
            except ValueError:
                detail = None
            if resp.status_code == 403:
                raise BeaconAPIError(detail or "You are not allowed to do that.")
            raise BeaconAPIError(f"Beacon couldn't complete that: {detail or resp.text[:200]}")
        return resp.json()

    def status(self) -> dict:
        return self._call("GET", "/status")

    def ask(self, question: str, user_id: str | None, dag_id: str | None = None, run_id: str | None = None) -> dict:
        return self._call("POST", "/ask", json={"question": question, "user_id": user_id,
                                                "dag_id": dag_id, "run_id": run_id})

    def runbook_search(self, query: str, k: int = 3) -> dict:
        return self._call("GET", "/runbooks/search", params={"q": query, "k": k})

    def runbook(self, runbook_id: str) -> dict:
        return self._call("GET", f"/runbooks/{runbook_id}")

    def incident(self, incident_id: int) -> dict:
        return self._call("GET", f"/incidents/{incident_id}")

    def feedback(self, incident_id: int, value: str, user_id: str) -> dict:
        return self._call("POST", f"/incidents/{incident_id}/feedback", json={"value": value, "user_id": user_id})

    def acknowledge(self, incident_id: int, user_id: str) -> dict:
        return self._call("POST", f"/incidents/{incident_id}/ack", json={"user_id": user_id})

    def rerun(self, incident_id: int, user_id: str) -> dict:
        return self._call("POST", "/actions/rerun", json={"incident_id": incident_id, "user_id": user_id})
