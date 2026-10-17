"""Prompts. Kept short on purpose: free-tier token budgets are tight."""
from __future__ import annotations

import json

from beacon.models import ROOT_CAUSE_DESCRIPTIONS

_CAUSES = "\n".join(f"- {c.value}: {d}" for c, d in ROOT_CAUSE_DESCRIPTIONS.items())

_SCHEMA = json.dumps({
    "summary": "<=3 plain-English sentences for a pipeline USER (not an SRE)",
    "user_impact": "which pipelines/data are affected",
    "root_cause": "one of the categories above",
    "confidence": "0.0-1.0",
    "evidence": ["exact log lines or metric values you saw"],
    "runbook_id": "id returned by search_runbooks, or null",
    "suggested_action": "what the user or on-call should do next",
}, indent=None)

SYSTEM_PROMPT = f"""You are Beacon, the support copilot for an Apache Airflow data platform.
Investigate with the tools BEFORE concluding. Typical path: identify the failed run and task, read
that task's log, check metrics or platform status if the log is not conclusive, then search runbooks
with the key error message.

Rules:
- Base the conclusion only on evidence you saw in tool results. Quote it verbatim in "evidence".
- If the evidence does not support a category, answer root_cause "unknown" with low confidence. Do not guess.
- Only use a runbook_id that search_runbooks returned and that matches your root cause.
- A timeout means the dependency is slow (dependency_latency); an error status like 503 means it is down.
- Be efficient: you have at most 8 tool rounds.

Root cause categories:
{_CAUSES}

When done, reply with ONLY a JSON object (no prose, no code fences) with these keys:
{_SCHEMA}"""

FIX_JSON_PROMPT = ("Your previous reply was not valid. Problems: {errors}\n"
                   "Reply with ONLY the JSON object with keys summary, user_impact, root_cause, confidence, "
                   "evidence, runbook_id, suggested_action. root_cause must be one of: {causes}.")

FINALIZE_PROMPT = ("You have used all your tool rounds. Based only on the evidence gathered so far, reply now "
                   "with ONLY the final JSON object.")

TRIAGE_PROMPT = """Classify a data-platform user's question into one category:
- job_failure: a pipeline/DAG/job failed or errored and they want to know why
- job_slow: a pipeline is slow, late, stuck, or missed its SLA
- how_to: how to do something / what to do about a kind of problem (no specific failing run)
- other: anything else (access requests, unrelated topics, chit-chat)
Reply with ONLY JSON: {"category": "...", "dag_id": "<dag id mentioned, or null>"}"""


def alert_context(alert_name: str, labels: dict, annotations: dict, starts_at: str | None,
                  run_id: str | None, failed_tasks: list[str] | None) -> str:
    lines = [f"Alert {alert_name} is firing (started {starts_at or 'just now'})."]
    if annotations.get("summary"):
        lines.append(f"Alert summary: {annotations['summary']}")
    useful = {k: v for k, v in labels.items() if k in ("dag_id", "task_id", "severity", "state")}
    if useful:
        lines.append("Labels: " + ", ".join(f"{k}={v}" for k, v in useful.items()))
    if run_id:
        lines.append(f"Most recent affected run: dag_id={labels.get('dag_id')} run_id={run_id}"
                     + (f"; failed task(s): {', '.join(failed_tasks)}" if failed_tasks else ""))
    lines.append("Investigate and diagnose the root cause.")
    return "\n".join(lines)


def question_context(question: str, dag_id: str | None, run_id: str | None, category: str) -> str:
    lines = [f"A platform user asks: \"{question}\"", f"Triage category: {category}."]
    if dag_id:
        lines.append(f"DAG in question: {dag_id}" + (f", run_id={run_id}" if run_id else " (find its latest failed or slow run)"))
    lines.append("Investigate and diagnose the root cause.")
    return "\n".join(lines)
