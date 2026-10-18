"""beacon-api: FastAPI service for alerts, questions, status, reruns and incidents."""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field
from sqlalchemy import select

from beacon import metrics, tools
from beacon.agent.llm import LLMError, OpenAICompatibleLLM
from beacon.agent.loop import Agent
from beacon.agent.prompts import question_context
from beacon.agent.triage import classify
from beacon.api.service import IncidentService
from beacon.clients import AirflowClient, UpstreamError
from beacon.config import get_settings
from beacon.models import TriageCategory
from beacon.store import EvalMetric, Incident, get_engine, session, utcnow

log = logging.getLogger("beacon.api")

OTHER_REPLY = ("I can help with failing or slow pipelines and with how-to questions about the data platform. "
               "For anything else, please ask the on-call engineer in #data-platform-oncall.")


class AskRequest(BaseModel):
    question: str = Field(min_length=2, max_length=2000)
    user_id: str | None = None
    dag_id: str | None = None
    run_id: str | None = None


class RerunRequest(BaseModel):
    user_id: str
    incident_id: int | None = None
    dag_id: str | None = None
    run_id: str | None = None
    task_id: str | None = None


class FeedbackRequest(BaseModel):
    value: Literal["helpful", "not_helpful"]
    user_id: str | None = None


class AckRequest(BaseModel):
    user_id: str


class EvalMetricsRequest(BaseModel):
    root_cause_accuracy: dict[str, float]
    runbook_accuracy: dict[str, float]


def default_agent() -> Agent:
    return Agent(OpenAICompatibleLLM())


def lookup_runbook(runbook_id: str):
    from beacon.retrieval import get_retriever
    try:
        return get_retriever().by_id.get(runbook_id)
    except Exception as exc:
        log.warning("runbook lookup failed: %s", exc)
        return None


def _load_eval_gauges() -> None:
    with session() as db:
        for m in db.scalars(select(EvalMetric)).all():
            gauge = metrics.EVAL_ROOT_CAUSE_ACCURACY if m.name == "root_cause_accuracy" else metrics.EVAL_RUNBOOK_ACCURACY
            gauge.labels(system=m.system).set(m.value)


def create_app(service: IncidentService | None = None, warm_retriever: bool = True) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        get_engine()
        _load_eval_gauges()
        app.state.service = service or IncidentService(default_agent, runbook_lookup=lookup_runbook,
                                                       concurrency=get_settings().agent_concurrency)
        app.state.service.refresh_open_gauge()
        if warm_retriever:
            from beacon.retrieval import get_retriever
            asyncio.get_running_loop().run_in_executor(None, get_retriever)
        yield
        await app.state.service.wait_idle()

    app = FastAPI(title="beacon-api", version="0.1.0", lifespan=lifespan)

    def svc(request: Request) -> IncidentService:
        return request.app.state.service

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    @app.get("/metrics")
    async def prom_metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.post("/webhooks/alertmanager")
    async def alertmanager_webhook(payload: dict, request: Request):
        return await svc(request).handle_alertmanager(payload)

    @app.post("/ask")
    async def ask(req: AskRequest):
        try:
            known = [d["dag_id"] for d in await AirflowClient().list_dags()]
        except UpstreamError:
            known = []
        try:
            llm = OpenAICompatibleLLM()
        except LLMError:
            llm = None
        category, dag_id = await classify(llm, req.question, known)
        dag_id = req.dag_id or dag_id
        if req.dag_id and category in (TriageCategory.how_to, TriageCategory.other):
            category = TriageCategory.job_failure  # /why always names a job to investigate
        if category is TriageCategory.other:
            return {"category": category.value, "message": OTHER_REPLY}
        if category is TriageCategory.how_to:
            found = await tools.call_tool(tools.READ_ONLY_TOOLS["search_runbooks"], {"query": req.question, "k": 3})
            return {"category": category.value, "runbooks": found.get("results", []),
                    "message": "Here are the runbooks that match your question." if found.get("results")
                    else "I couldn't find a runbook for that. Try describing the error you see."}
        if llm is None:
            raise HTTPException(503, "The AI model is not configured (LLM_MODEL / LLM_API_KEY), so I can't investigate.")
        try:
            result = await Agent(llm).investigate(question_context(req.question, dag_id, req.run_id, category.value),
                                                  trace_name="ask", metadata={"user_id": req.user_id})
        except LLMError as exc:
            raise HTTPException(503, f"The AI model is unavailable right now ({str(exc)[:160]}). Try again shortly.")
        return {"category": category.value, "dag_id": dag_id, "diagnosis": result.diagnosis.model_dump(mode="json"),
                "steps": result.steps, "tool_calls": [t.as_dict() for t in result.tool_calls]}

    @app.get("/status")
    async def status():
        return await tools.get_platform_status()

    @app.post("/actions/rerun")
    async def rerun(req: RerunRequest):
        dag_id, run_id, task_id = req.dag_id, req.run_id, req.task_id
        if req.incident_id is not None:
            with session() as db:
                inc = db.get(Incident, req.incident_id)
                if inc is None:
                    raise HTTPException(404, "incident not found")
                dag_id, run_id, task_id = dag_id or inc.dag_id, run_id or inc.run_id, task_id or inc.task_id
        if not (dag_id and run_id and task_id):
            raise HTTPException(422, "dag_id, run_id and task_id are required")
        try:
            result = await tools.rerun_task(dag_id, run_id, task_id, req.user_id)
        except tools.RerunNotAllowed as exc:
            raise HTTPException(403, str(exc))
        except UpstreamError as exc:
            raise HTTPException(502, f"Airflow rejected the rerun: {exc}")
        log.info("rerun by %s: %s", req.user_id, result)
        return result

    @app.get("/incidents")
    async def list_incidents(status: str | None = None, dag_id: str | None = None,
                             fault_injection_id: int | None = None, limit: int = Query(50, ge=1, le=500)):
        with session() as db:
            q = select(Incident).order_by(Incident.created_at.desc()).limit(limit)
            if status:
                q = q.where(Incident.status == status)
            if dag_id:
                q = q.where(Incident.dag_id == dag_id)
            if fault_injection_id is not None:
                q = q.where(Incident.fault_injection_id == fault_injection_id)
            return [i.to_dict() for i in db.scalars(q).all()]

    @app.get("/incidents/{incident_id}")
    async def get_incident(incident_id: int):
        with session() as db:
            inc = db.get(Incident, incident_id)
            if inc is None:
                raise HTTPException(404, "incident not found")
            return inc.to_dict()

    @app.post("/incidents/{incident_id}/feedback")
    async def feedback(incident_id: int, req: FeedbackRequest):
        with session() as db:
            inc = db.get(Incident, incident_id)
            if inc is None:
                raise HTTPException(404, "incident not found")
            inc.feedback = req.value
            db.commit()
        metrics.FEEDBACK.labels(value=req.value).inc()
        return {"id": incident_id, "feedback": req.value}

    @app.post("/incidents/{incident_id}/ack")
    async def acknowledge(incident_id: int, req: AckRequest, request: Request):
        with session() as db:
            inc = db.get(Incident, incident_id)
            if inc is None:
                raise HTTPException(404, "incident not found")
            inc.acknowledged_by = req.user_id
            db.commit()
            snap = inc.to_dict()
        runbook = lookup_runbook(snap["runbook_id"]) if snap.get("runbook_id") else None
        await svc(request).notifier.update_incident(snap, runbook)
        return snap

    @app.get("/runbooks/search")
    async def runbook_search(q: str = Query(min_length=2), k: int = Query(3, ge=1, le=5)):
        return await tools.call_tool(tools.READ_ONLY_TOOLS["search_runbooks"], {"query": q, "k": k})

    @app.get("/runbooks/{runbook_id}")
    async def runbook(runbook_id: str):
        found = await tools.get_runbook(runbook_id)
        if "error" in found:
            raise HTTPException(404, found["error"])
        return found

    @app.post("/eval/metrics")
    async def eval_metrics(req: EvalMetricsRequest):
        with session() as db:
            for name, values in (("root_cause_accuracy", req.root_cause_accuracy),
                                 ("runbook_accuracy", req.runbook_accuracy)):
                for system, value in values.items():
                    row = db.get(EvalMetric, (name, system)) or EvalMetric(name=name, system=system, value=value)
                    row.value, row.updated_at = value, utcnow()
                    db.merge(row)
            db.commit()
        _load_eval_gauges()
        return {"ok": True}

    return app


def run() -> None:
    import uvicorn
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    uvicorn.run(create_app(), host="0.0.0.0", port=8000, log_level="info")
