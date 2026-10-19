"""Investigation tools. The agent and the MCP server both import from here, so each tool
has exactly one implementation.

Every tool is read-only except `rerun_task`, which is deliberately absent from
AGENT_TOOLS and MCP: only the Slack button calls it, after a human confirms.
"""
from __future__ import annotations

import asyncio
import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from beacon.clients import AirflowClient, PrometheusClient, UpstreamError
from beacon.config import get_settings
from beacon.security import mask_obj, mask_secrets

# ── Argument schemas ──────────────────────────────────────────────────────────


class _Args(BaseModel):
    model_config = ConfigDict(extra="forbid")


class NoArgs(_Args):
    pass


class ListRecentFailuresArgs(_Args):
    dag_id: str | None = Field(None, description="Only this DAG; omit for all DAGs")
    since_minutes: int = Field(60, ge=1, le=1440, description="Look-back window in minutes")


class GetDagRunArgs(_Args):
    dag_id: str
    run_id: str


class GetTaskLogsArgs(_Args):
    dag_id: str
    run_id: str
    task_id: str
    try_number: int | None = Field(None, ge=1, description="Defaults to the latest try")
    tail_lines: int = Field(40, ge=5, le=150, description="How many lines from the end of the log")


class QueryPrometheusArgs(_Args):
    promql: str = Field(description="A plain PromQL expression, e.g. airflow_ti_finish{state=\"failed\"}")
    minutes: int = Field(0, ge=0, description="0 = instant query; otherwise a range query over the last N minutes")


class SearchRunbooksArgs(_Args):
    query: str = Field(min_length=3, description="Symptoms or the key error message")
    k: int = Field(3, ge=1, le=5)


class GetRunbookArgs(_Args):
    runbook_id: str


class RerunTaskArgs(_Args):
    dag_id: str
    run_id: str
    task_id: str
    user_id: str


# ── Helpers ───────────────────────────────────────────────────────────────────

_LOG_NOISE = re.compile(
    r"Dependencies all met|Starting attempt|Executing <Task|Running <TaskInstance|"
    r"Started process|Job \d+: Subtask|Exporting env vars|Tmp dir root location|"
    r"Pre task execution logs|Post task execution logs|::group::|::endgroup::|Filling up the DagBag|"
    r"Setting task instance|Task is in the 'running' state|"
    r"\*\*\* Found local files|\*\*\* Reading local file|\*\*\* Log file does not exist|"
    r"Running: \[|downstream tasks scheduled from follow-on|AIRFLOW_CTX_")
_LOG_PREFIX = re.compile(r"^\[(\d{4}-\d\d-\d\dT[\d:.]+[+-]\d{4})\] \{[^}]+\} ")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _duration(start: str | None, end: str | None) -> float | None:
    s, e = _parse_ts(start), _parse_ts(end)
    return round((e - s).total_seconds(), 1) if s and e else None


_FRAME = re.compile(r'^\s*File "([^"]+)", line \d+, in ')
_LIBRARY_PATH = re.compile(r"/site-packages/|/dist-packages/|/usr/local/lib/python\d|/usr/lib/python\d")
_CARETS = re.compile(r"^\s*[\^~]+\s*$")
_LOG_SOURCE = re.compile(r"^\*\*\*|^[0-9a-f]{12}$")


def clean_log(text: str, tail_lines: int) -> tuple[list[str], bool]:
    """Drop Airflow boilerplate and keep the relevant tail of a task log.

    Traceback frames inside libraries/the stdlib are collapsed into one marker line, so the
    tail shows the pipeline's own frame and the exception instead of Airflow internals.
    """
    lines: list[str] = []
    hidden, in_library_frame = 0, False
    for raw in text.splitlines():
        line = raw.rstrip()
        if not line or _LOG_NOISE.search(line) or _CARETS.match(line) or _LOG_SOURCE.match(line):
            continue
        frame = _FRAME.match(line)
        if frame:
            in_library_frame = bool(_LIBRARY_PATH.search(frame.group(1)))
            if in_library_frame:
                hidden += 1
                continue
        elif in_library_frame and raw.startswith("    "):
            continue  # source line of a hidden frame
        else:
            in_library_frame = False
        if hidden:
            lines.append(f"  [... {hidden} library frame(s) hidden ...]")
            hidden = 0
        lines.append(_LOG_PREFIX.sub(lambda m: m.group(1)[11:19] + " ", line))
    truncated = len(lines) > tail_lines
    return lines[-tail_lines:], truncated


def _compact_ti(ti: dict) -> dict:
    return {"task_id": ti["task_id"], "state": ti.get("state"), "try_number": ti.get("try_number"),
            "duration_s": round(ti["duration"], 1) if ti.get("duration") else None,
            "start": ti.get("start_date"), "end": ti.get("end_date")}


# ── PromQL guard ──────────────────────────────────────────────────────────────

_DURATION = re.compile(r"(\d+)(ms|s|m|h|d|w|y)")
_UNIT_SECONDS = {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800, "y": 31536000}
_ALLOWED_CHARS = re.compile(r"^[\w\s{}()\[\]\"'=!~<>.,:+\-*/^%|@$]+$")


class PromQLRejected(ValueError):
    pass


def _seconds(expr: str) -> float:
    return sum(int(n) * _UNIT_SECONDS[u] for n, u in _DURATION.findall(expr))


def validate_promql(promql: str, minutes: int, max_minutes: int) -> None:
    """Reject anything that isn't a plain, bounded read query."""
    q = promql.strip()
    if not q:
        raise PromQLRejected("empty query")
    if len(q) > 400:
        raise PromQLRejected("query longer than 400 characters")
    if "\n" in q or ";" in q or not _ALLOWED_CHARS.match(q):
        raise PromQLRejected("only a single plain PromQL expression is allowed")
    if re.search(r"https?:|/api/|\.\./", q, re.I):
        raise PromQLRejected("URLs and API paths are not allowed in a query")
    if re.search(r"__name__\s*=~\s*\"\.[*+]\"", q) or re.fullmatch(r"\{[^}]*\}", q):
        raise PromQLRejected("selecting every metric is not allowed; name a metric")
    if re.search(r"(?<![\w\"])@\s*\d", q):
        raise PromQLRejected("@ timestamp modifiers are not allowed")
    if minutes > max_minutes:
        raise PromQLRejected(f"time range capped at {max_minutes} minutes")
    for window in re.findall(r"\[([^\]]+)\]", q):
        for part in window.split(":"):
            if part and _seconds(part) > max_minutes * 60:
                raise PromQLRejected(f"range selector [{window}] exceeds the {max_minutes}m cap")
    for off in re.findall(r"offset\s+(-?\w+)", q):
        if _seconds(off.lstrip("-")) > max_minutes * 60:
            raise PromQLRejected(f"offset {off} exceeds the {max_minutes}m cap")


# ── Tools ─────────────────────────────────────────────────────────────────────

def _open_incidents(limit: int = 10) -> list[dict] | str:
    try:
        from sqlalchemy import select

        from beacon.store import Incident, session
        with session() as db:
            rows = db.scalars(select(Incident).where(Incident.status != "resolved")
                              .order_by(Incident.created_at.desc()).limit(limit)).all()
            return [{"id": i.id, "alert": i.alert_name, "dag_id": i.dag_id, "status": i.status,
                     "root_cause": i.root_cause} for i in rows]
    except Exception as exc:  # the store is optional for MCP clients
        return f"unavailable ({exc.__class__.__name__})"


async def get_platform_status() -> dict:
    """Scheduler health, per-DAG success rate over 24h, and open incidents."""
    af = AirflowClient()
    out: dict[str, Any] = {}
    try:
        health = await af.health()
        sched = health.get("scheduler", {})
        hb = _parse_ts(sched.get("latest_scheduler_heartbeat"))
        out["scheduler"] = {"status": sched.get("status"), "latest_heartbeat": sched.get("latest_scheduler_heartbeat"),
                            "seconds_since_heartbeat": round((_now() - hb).total_seconds()) if hb else None}
        out["metadatabase"] = health.get("metadatabase", {}).get("status")
    except UpstreamError as exc:
        out["scheduler"] = {"status": "unknown", "error": str(exc)}
        out["dags"] = []
        out["open_incidents"] = _open_incidents()
        return out

    since = _now() - timedelta(hours=24)
    dags = await af.list_dags()

    async def per_dag(dag: dict) -> dict:
        dag_id = dag["dag_id"]
        ok, failed, last = await asyncio.gather(
            af.list_dag_runs(dag_id, state="success", start_date_gte=since, limit=1),
            af.list_dag_runs(dag_id, state="failed", start_date_gte=since, limit=1),
            af.list_dag_runs(dag_id, limit=1))
        n_ok, n_failed = ok.get("total_entries", 0), failed.get("total_entries", 0)
        total = n_ok + n_failed
        last_run = (last.get("dag_runs") or [{}])[0]
        return {"dag_id": dag_id, "paused": dag.get("is_paused"), "runs_24h": total,
                "failed_24h": n_failed, "success_rate_24h": round(n_ok / total, 3) if total else None,
                "last_run_state": last_run.get("state"), "last_run_id": last_run.get("dag_run_id")}

    out["dags"] = sorted(await asyncio.gather(*(per_dag(d) for d in dags)), key=lambda d: d["dag_id"])
    out["open_incidents"] = _open_incidents()
    return out


async def list_recent_failures(dag_id: str | None = None, since_minutes: int = 60) -> dict:
    """Failed DAG runs in the window, each with its failed task instances."""
    af = AirflowClient()
    since = _now() - timedelta(minutes=since_minutes)
    data = await af.list_dag_runs(dag_id or "~", state="failed", start_date_gte=since, limit=10)
    runs = data.get("dag_runs", [])

    async def detail(run: dict) -> dict:
        tis = await af.task_instances(run["dag_id"], run["dag_run_id"])
        bad = [_compact_ti(t) for t in tis if t.get("state") in ("failed", "up_for_retry", "upstream_failed")]
        return {"dag_id": run["dag_id"], "run_id": run["dag_run_id"], "run_type": run.get("run_type"),
                "start": run.get("start_date"), "end": run.get("end_date"),
                "failed_tasks": [t for t in bad if t["state"] != "upstream_failed"],
                "upstream_failed": [t["task_id"] for t in bad if t["state"] == "upstream_failed"]}

    details = await asyncio.gather(*(detail(r) for r in runs[:5]))
    return {"since_minutes": since_minutes, "failed_runs_total": data.get("total_entries", len(runs)),
            "failed_runs": list(details)}


async def get_dag_run(dag_id: str, run_id: str) -> dict:
    """Run details and the state of every task in the run."""
    af = AirflowClient()
    run, tis = await asyncio.gather(af.get_dag_run(dag_id, run_id), af.task_instances(dag_id, run_id))
    return {"dag_id": dag_id, "run_id": run_id, "state": run.get("state"), "run_type": run.get("run_type"),
            "start": run.get("start_date"), "end": run.get("end_date"),
            "duration_s": _duration(run.get("start_date"), run.get("end_date")),
            "tasks": [_compact_ti(t) for t in sorted(tis, key=lambda t: t.get("start_date") or "~")]}


async def get_task_logs(dag_id: str, run_id: str, task_id: str, try_number: int | None = None,
                        tail_lines: int = 40) -> dict:
    """The last lines of one task try's log (Airflow boilerplate removed, secrets masked)."""
    af = AirflowClient()
    if try_number is None:
        ti = await af.task_instance(dag_id, run_id, task_id)
        try_number = max(int(ti.get("try_number") or 1), 1)
    text = await af.task_log(dag_id, run_id, task_id, try_number)
    lines, truncated = clean_log(text, tail_lines)
    s = get_settings()
    return {"dag_id": dag_id, "run_id": run_id, "task_id": task_id, "try_number": try_number,
            "truncated": truncated, "log": [mask_secrets(line, s.secret_values) for line in lines]}


async def query_prometheus(promql: str, minutes: int = 0) -> dict:
    """Run a bounded PromQL query (instant, or a range over the last `minutes`)."""
    s = get_settings()
    validate_promql(promql, minutes, s.promql_max_minutes)
    prom = PrometheusClient()
    if minutes:
        end = _now()
        step = max(15, math.ceil(minutes * 60 / s.promql_max_points))
        data = await prom.query_range(promql, end - timedelta(minutes=minutes), end, step)
    else:
        data = await prom.query(promql)
    rtype, result = data.get("resultType"), data.get("result", [])
    if rtype in ("scalar", "string"):
        return {"result_type": rtype, "series": [{"value": result[1]}], "truncated": False}
    series = []
    for item in result[: s.promql_max_series]:
        entry: dict[str, Any] = {"metric": item.get("metric", {})}
        if "value" in item:
            entry["value"] = item["value"][1]
        else:
            entry["values"] = [[datetime.fromtimestamp(t, timezone.utc).strftime("%H:%M:%S"), v]
                               for t, v in item.get("values", [])[-s.promql_max_points:]]
        series.append(entry)
    return {"result_type": rtype, "series": series, "truncated": len(result) > s.promql_max_series}


async def search_runbooks(query: str, k: int = 3) -> dict:
    """Find the runbooks most relevant to a symptom or error message (CRAG-graded)."""
    from beacon.retrieval import get_retriever
    retriever = await asyncio.to_thread(get_retriever)
    result = await asyncio.to_thread(retriever.search, query, k)
    return result.as_dict()


async def get_runbook(runbook_id: str) -> dict:
    """Full text of one runbook."""
    from beacon.retrieval import get_retriever
    retriever = await asyncio.to_thread(get_retriever)
    rb = retriever.by_id.get(runbook_id)
    if rb is None:
        return {"error": f"no runbook with id '{runbook_id}'", "known_ids": sorted(retriever.by_id)}
    return {"runbook_id": rb.id, "title": rb.title, "root_cause": rb.root_cause, "body": rb.body}


class RerunNotAllowed(PermissionError):
    pass


async def rerun_task(dag_id: str, run_id: str, task_id: str, user_id: str) -> dict:
    """Clear a failed task (and its downstream tasks) so Airflow reruns it.

    Write action: never exposed to the agent or MCP. Only allowlisted humans may call it.
    """
    if user_id not in get_settings().rerun_allowlist:
        raise RerunNotAllowed(f"user {user_id} is not allowed to rerun tasks")
    result = await AirflowClient().clear_task(dag_id, run_id, task_id)
    cleared = [t.get("task_id") for t in result.get("task_instances", [])]
    return {"dag_id": dag_id, "run_id": run_id, "task_id": task_id, "cleared_tasks": cleared}


# ── Registry ──────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Tool:
    name: str
    description: str
    args_model: type[BaseModel]
    func: Callable[..., Awaitable[dict]]

    def openai_spec(self) -> dict:
        schema = self.args_model.model_json_schema()
        schema.pop("title", None)
        for prop in schema.get("properties", {}).values():
            prop.pop("title", None)
        schema.setdefault("properties", {})
        return {"type": "function",
                "function": {"name": self.name, "description": self.description, "parameters": schema}}


READ_ONLY_TOOLS: dict[str, Tool] = {t.name: t for t in [
    Tool("get_platform_status", "Scheduler health, per-DAG success rate over 24h, and open incidents.",
         NoArgs, get_platform_status),
    Tool("list_recent_failures", "Recent failed DAG runs and their failed tasks.",
         ListRecentFailuresArgs, list_recent_failures),
    Tool("get_dag_run", "Details of one DAG run and the state of each task.", GetDagRunArgs, get_dag_run),
    Tool("get_task_logs", "The last lines of a task's log. Use it to find the actual error.",
         GetTaskLogsArgs, get_task_logs),
    Tool("query_prometheus", "Run a read-only PromQL query against platform metrics "
         "(airflow_ti_finish, airflow_task_duration_bucket, airflow_dagrun_duration_count, "
         "airflow_scheduler_heartbeat, airflow_sla_missed).", QueryPrometheusArgs, query_prometheus),
    Tool("search_runbooks", "Search runbooks by symptom or error message. Returns ids, titles and relevance scores.",
         SearchRunbooksArgs, search_runbooks),
    Tool("get_runbook", "Full text of a runbook by id.", GetRunbookArgs, get_runbook),
]}

# The agent gets exactly the read-only set. rerun_task is not in it.
AGENT_TOOLS: dict[str, Tool] = dict(READ_ONLY_TOOLS)

RERUN_TOOL = Tool("rerun_task", "Clear a failed task so Airflow reruns it (human-only).", RerunTaskArgs, rerun_task)


class ToolArgumentError(ValueError):
    pass


def validate_args(tool: Tool, raw: dict | None) -> dict:
    try:
        return tool.args_model.model_validate(raw or {}).model_dump()
    except ValidationError as exc:
        problems = "; ".join(f"{'.'.join(map(str, e['loc'])) or 'args'}: {e['msg']}" for e in exc.errors())
        raise ToolArgumentError(f"invalid arguments for {tool.name}: {problems}") from exc


async def call_tool(tool: Tool, raw_args: dict | None, timeout: float | None = None) -> dict:
    """Validate, run with a timeout, and mask secrets. Errors come back as {"error": ...}."""
    s = get_settings()
    try:
        args = validate_args(tool, raw_args)
        result = await asyncio.wait_for(tool.func(**args), timeout=timeout or s.tool_timeout_seconds)
    except ToolArgumentError as exc:
        return {"error": str(exc)}
    except PromQLRejected as exc:
        return {"error": f"query rejected: {exc}"}
    except asyncio.TimeoutError:
        return {"error": f"{tool.name} timed out after {timeout or s.tool_timeout_seconds:.0f}s"}
    except UpstreamError as exc:
        return {"error": str(exc)}
    return mask_obj(result, s.secret_values)
