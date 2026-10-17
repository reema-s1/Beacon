"""Incident lifecycle: Alertmanager webhook -> incident -> agent -> Slack -> resolution."""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import datetime
from typing import Any, Callable

from sqlalchemy import func, select

from beacon import metrics
from beacon.agent.loop import Agent, AgentResult, fallback_diagnosis
from beacon.agent.prompts import alert_context
from beacon.api.notifier import SlackNotifier
from beacon.clients import AirflowClient, UpstreamError
from beacon.store import FaultInjection, Incident, session, utcnow

log = logging.getLogger("beacon.service")

# Alerts about slowness point at the latest run, failures at the latest failed run.
_SLOW_ALERTS = {"TaskDurationAnomaly", "SLAMissed"}


def _parse_ts(value: str | None) -> datetime | None:
    if not value or value.startswith("0001-"):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def alert_fingerprint(alert: dict) -> str:
    if alert.get("fingerprint"):
        return alert["fingerprint"]
    labels = json.dumps(alert.get("labels", {}), sort_keys=True)
    return hashlib.sha256(labels.encode()).hexdigest()[:16]


class IncidentService:
    def __init__(self, agent_factory: Callable[[], Agent], notifier: SlackNotifier | None = None,
                 airflow: AirflowClient | None = None, runbook_lookup: Callable[[str], Any] | None = None,
                 concurrency: int = 1):
        self.agent_factory = agent_factory
        self.notifier = notifier or SlackNotifier()
        self.airflow = airflow or AirflowClient()
        self.runbook_lookup = runbook_lookup or (lambda _id: None)
        self._create_lock = asyncio.Lock()
        self._agent_sem = asyncio.Semaphore(concurrency)
        self._tasks: set[asyncio.Task] = set()

    # ── Webhook ───────────────────────────────────────────────────────────────

    async def handle_alertmanager(self, payload: dict) -> dict:
        created, resolved, deduplicated = [], [], 0
        for alert in payload.get("alerts", []):
            fp = alert_fingerprint(alert)
            if alert.get("status") == "resolved":
                resolved += await self.resolve(fp)
                continue
            incident_id = await self._create_from_alert(alert, fp)
            if incident_id is None:
                deduplicated += 1
            else:
                created.append(incident_id)
        return {"created": created, "deduplicated": deduplicated, "resolved": resolved}

    async def _locate_run(self, alert_name: str, dag_id: str, task_label: str | None) -> dict:
        """Find the run the alert is about, so incidents can be deduplicated per run."""
        try:
            state = None if alert_name in _SLOW_ALERTS else "failed"
            runs = (await self.airflow.list_dag_runs(dag_id, state=state, limit=1)).get("dag_runs", [])
            if not runs:
                return {}
            run = runs[0]
            tis = await self.airflow.task_instances(dag_id, run["dag_run_id"])
            failed = [t["task_id"] for t in tis if t.get("state") == "failed"]
            slow = sorted(tis, key=lambda t: t.get("duration") or 0, reverse=True)
            task_id = task_label or (failed[0] if failed else (slow[0]["task_id"] if alert_name in _SLOW_ALERTS and slow else None))
            return {"run_id": run["dag_run_id"], "failed_tasks": failed, "task_id": task_id,
                    "event_time": _parse_ts(run.get("end_date")) or _parse_ts(run.get("start_date"))}
        except (UpstreamError, KeyError) as exc:
            log.warning("could not locate run for %s/%s: %s", alert_name, dag_id, exc)
            return {}

    async def _create_from_alert(self, alert: dict, fp: str) -> int | None:
        labels = alert.get("labels", {})
        annotations = alert.get("annotations", {})
        name = labels.get("alertname", "unknown")
        dag_id = labels.get("dag_id")
        run: dict = await self._locate_run(name, dag_id, labels.get("task_id")) if dag_id else {}
        run_id = run.get("run_id")
        event_time = run.get("event_time") or _parse_ts(alert.get("startsAt")) or utcnow()

        async with self._create_lock:
            with session() as db:
                # Dedupe key is fingerprint + run id. Run-less alerts (scheduler) dedupe
                # against the still-open incident for the same fingerprint.
                dup = select(Incident.id).where(Incident.fingerprint == fp)
                if run_id is None:
                    dup = dup.where(Incident.run_id.is_(None), Incident.status != "resolved")
                else:
                    dup = dup.where(Incident.run_id == run_id)
                if db.scalar(dup.limit(1)) is not None:
                    return None
                if dag_id and run_id and db.scalar(select(Incident.id).where(
                        Incident.dag_id == dag_id, Incident.run_id == run_id).limit(1)) is not None:
                    return None  # same failed run already reported by another alert rule
                injection = db.scalar(
                    select(FaultInjection).where(FaultInjection.injected_at <= event_time)
                    .where((FaultInjection.reset_at.is_(None)) | (FaultInjection.reset_at >= event_time))
                    .order_by(FaultInjection.injected_at.desc()).limit(1))
                inc = Incident(fingerprint=fp, alert_name=name, dag_id=dag_id, task_id=run.get("task_id"),
                               run_id=run_id, status="open", labels=labels,
                               fault_injection_id=injection.id if injection else None)
                db.add(inc)
                db.commit()
                incident_id = inc.id
        self.refresh_open_gauge()
        context = alert_context(name, labels, annotations, alert.get("startsAt"), run_id, run.get("failed_tasks"))
        self._spawn(self.investigate_incident(incident_id, context))
        return incident_id

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def wait_idle(self) -> None:
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    # ── Investigation ─────────────────────────────────────────────────────────

    async def investigate_incident(self, incident_id: int, context: str) -> None:
        async with self._agent_sem:
            error = None
            try:
                agent = self.agent_factory()
                result = await agent.investigate(context, trace_name="incident-investigation",
                                                 metadata={"incident_id": incident_id})
            except Exception as exc:
                log.exception("agent failed on incident %s", incident_id)
                error = f"{exc.__class__.__name__}: {exc}"
                result = AgentResult(diagnosis=fallback_diagnosis(f"agent error: {error}"[:500]), steps=0)

        d = result.diagnosis
        with session() as db:
            inc = db.get(Incident, incident_id)
            inc.summary, inc.user_impact = d.summary, d.user_impact
            inc.root_cause, inc.confidence = d.root_cause.value, d.confidence
            inc.evidence, inc.runbook_id, inc.suggested_action = d.evidence, d.runbook_id, d.suggested_action
            inc.agent_steps = result.steps
            inc.investigation = [tc.as_dict() for tc in result.tool_calls]
            inc.error = error
            if inc.status != "resolved":
                inc.status = "triaged"
            db.commit()
            snapshot = inc.to_dict()

        runbook = self.runbook_lookup(d.runbook_id) if d.runbook_id else None
        channel, ts = await self.notifier.post_incident(snapshot, runbook)
        with session() as db:
            inc = db.get(Incident, incident_id)
            inc.slack_channel, inc.slack_ts = channel, ts
            inc.triaged_at = utcnow()
            db.commit()
            elapsed = (inc.triaged_at - inc.created_at).total_seconds()
        metrics.TRIAGE_SECONDS.observe(elapsed)
        metrics.INCIDENTS.labels(root_cause=d.root_cause.value).inc()
        self.refresh_open_gauge()

    # ── Resolution ────────────────────────────────────────────────────────────

    async def resolve(self, fingerprint: str) -> list[int]:
        with session() as db:
            rows = db.scalars(select(Incident).where(Incident.fingerprint == fingerprint,
                                                     Incident.status != "resolved")).all()
            for inc in rows:
                inc.status, inc.resolved_at = "resolved", utcnow()
            db.commit()
            snapshots = [i.to_dict() for i in rows]
        for snap in snapshots:
            runbook = self.runbook_lookup(snap["runbook_id"]) if snap.get("runbook_id") else None
            await self.notifier.update_incident(snap, runbook)
            await self.notifier.post_thread(snap, f"✅ The alert has resolved ({snap['resolved_at'][:19]} UTC). "
                                                  "Beacon has marked this incident as resolved.")
        self.refresh_open_gauge()
        return [s["id"] for s in snapshots]

    # ── Misc ──────────────────────────────────────────────────────────────────

    @staticmethod
    def count_open() -> int:
        with session() as db:
            return db.scalar(select(func.count()).select_from(Incident).where(Incident.status != "resolved")) or 0

    def refresh_open_gauge(self) -> None:
        try:
            metrics.OPEN_INCIDENTS.set(self.count_open())
        except Exception as exc:
            log.warning("could not refresh open-incident gauge: %s", exc)
