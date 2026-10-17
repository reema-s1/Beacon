"""Block Kit builders shared by beacon-api (incident posts) and beacon-slack (commands)."""
from __future__ import annotations

from typing import Any

from beacon.security import mask_secrets

ALERT_PHRASES = {
    "DagRunFailed": "pipeline run failed",
    "TaskFailureSpike": "a task keeps failing",
    "SLAMissed": "a task missed its SLA",
    "SchedulerHeartbeatMissing": "the scheduler has stopped",
    "TaskDurationAnomaly": "running much slower than usual",
}

CAUSE_LABELS = {
    "code_error": "Bug in pipeline code",
    "missing_upstream_data": "Missing input data",
    "dependency_unavailable": "External service down",
    "dependency_latency": "External service slow",
    "credentials_or_connection": "Connection / credentials problem",
    "resource_limit": "Ran out of memory",
    "scheduler_unhealthy": "Airflow scheduler down",
    "storage_full": "Disk full",
    "unknown": "Unknown",
}


def _text(s: str | None, limit: int = 2900) -> str:
    s = mask_secrets(s or "")
    return s if len(s) <= limit else s[: limit - 1] + "…"


def _section(md: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": _text(md)}}


def _button(text: str, action_id: str, value: str, style: str | None = None) -> dict:
    b: dict[str, Any] = {"type": "button", "text": {"type": "plain_text", "text": text, "emoji": True},
                         "action_id": action_id, "value": value}
    if style:
        b["style"] = style
    return b


def incident_title(inc: dict) -> str:
    subject = inc.get("dag_id") or "Airflow platform"
    return f"{subject}: {ALERT_PHRASES.get(inc.get('alert_name', ''), inc.get('alert_name', 'alert'))}"


def cause_line(root_cause: str | None, confidence: float | None) -> str:
    label = CAUSE_LABELS.get(root_cause or "unknown", root_cause or "unknown")
    conf = f" ({confidence:.0%} confidence)" if confidence is not None else ""
    return f"*Likely cause:* {label} `{root_cause}`{conf}"


def runbook_section(runbook: Any | None) -> list[dict]:
    if runbook is None:
        return [_section("*Runbook:* none matched")]
    steps = "\n".join(f"{i}. {s}" for i, s in enumerate(runbook.fix_steps[:6], 1))
    return [_section(f"*Runbook:* {runbook.title} (`{runbook.id}`)\n{steps}")]


def incident_blocks(inc: dict, runbook: Any | None = None) -> list[dict]:
    resolved = inc.get("status") == "resolved"
    emoji = "✅" if resolved else "🔴"
    header = f"{emoji} {'[RESOLVED] ' if resolved else ''}{incident_title(inc)}"
    blocks: list[dict] = [{"type": "header", "text": {"type": "plain_text", "text": header[:150], "emoji": True}}]
    blocks.append(_section(inc.get("summary") or "_Beacon is investigating…_"))
    if inc.get("root_cause"):
        blocks.append({"type": "section", "fields": [
            {"type": "mrkdwn", "text": _text(f"*Who is affected:*\n{inc.get('user_impact') or 'unknown'}", 1900)},
            {"type": "mrkdwn", "text": cause_line(inc.get("root_cause"), inc.get("confidence"))},
        ]})
        evidence = inc.get("evidence") or []
        if evidence:
            first = evidence[0] if len(evidence[0]) < 180 else evidence[0][:177] + "…"
            blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": _text(
                f"Evidence ({len(evidence)}): `{first}` — click *Show evidence* for all of it")}]})
        blocks += runbook_section(runbook)
        if inc.get("suggested_action"):
            blocks.append(_section(f"*Next step:* {inc['suggested_action']}"))
    meta = [f"Incident #{inc.get('id')}", f"alert `{inc.get('alert_name')}`"]
    if inc.get("run_id"):
        meta.append(f"run `{inc['run_id']}`")
    if inc.get("task_id"):
        meta.append(f"task `{inc['task_id']}`")
    if inc.get("acknowledged_by"):
        meta.append(f"acknowledged by <@{inc['acknowledged_by']}>")
    if resolved and inc.get("resolved_at"):
        meta.append(f"resolved {inc['resolved_at'][:19].replace('T', ' ')} UTC")
    blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": " · ".join(meta)}]})

    iid = str(inc.get("id"))
    actions = []
    if not resolved and inc.get("dag_id") and inc.get("run_id") and inc.get("task_id"):
        actions.append(_button("Rerun task", "rerun_task", iid, "primary"))
    if not resolved and not inc.get("acknowledged_by"):
        actions.append(_button("Acknowledge", "acknowledge", iid))
    if inc.get("evidence"):
        actions.append(_button("Show evidence", "show_evidence", iid))
    actions.append(_button("Helpful 👍", "feedback_helpful", iid))
    actions.append(_button("Not helpful 👎", "feedback_not_helpful", iid))
    blocks.append({"type": "actions", "block_id": f"incident-{iid}", "elements": actions})
    return blocks


def incident_fallback_text(inc: dict) -> str:
    return mask_secrets(f"{incident_title(inc)}: {inc.get('summary') or 'investigating'}")


def evidence_modal(inc: dict) -> dict:
    items = "\n".join(f"• `{_text(e, 280)}`" for e in (inc.get("evidence") or [])[:15]) or "_none_"
    return {"type": "modal", "title": {"type": "plain_text", "text": "Evidence"},
            "close": {"type": "plain_text", "text": "Close"},
            "blocks": [_section(f"*{incident_title(inc)}*"), _section(items)]}


def rerun_modal(inc: dict, channel: str | None, message_ts: str | None) -> dict:
    import json
    return {"type": "modal", "callback_id": "rerun_confirm",
            "private_metadata": json.dumps({"incident_id": inc["id"], "channel": channel, "ts": message_ts}),
            "title": {"type": "plain_text", "text": "Rerun task?"},
            "submit": {"type": "plain_text", "text": "Rerun"},
            "close": {"type": "plain_text", "text": "Cancel"},
            "blocks": [_section(f"This clears *{inc['task_id']}* (and its downstream tasks) in run "
                                f"`{inc['run_id']}` of *{inc['dag_id']}* so Airflow runs it again.\n\n"
                                "Only rerun once the cause is fixed, or the task will just fail again.")]}


def diagnosis_blocks(title: str, diag: dict, runbook: Any | None = None) -> list[dict]:
    blocks: list[dict] = [{"type": "header", "text": {"type": "plain_text", "text": title[:150]}},
                          _section(diag.get("summary", "")),
                          {"type": "section", "fields": [
                              {"type": "mrkdwn", "text": _text(f"*Who is affected:*\n{diag.get('user_impact', '')}", 1900)},
                              {"type": "mrkdwn", "text": cause_line(diag.get("root_cause"), diag.get("confidence"))}]}]
    if diag.get("evidence"):
        blocks.append(_section("*Evidence:*\n" + "\n".join(f"• `{_text(e, 280)}`" for e in diag["evidence"][:6])))
    blocks += runbook_section(runbook)
    if diag.get("suggested_action"):
        blocks.append(_section(f"*Next step:* {diag['suggested_action']}"))
    return blocks


def status_blocks(status: dict) -> list[dict]:
    sched = status.get("scheduler", {})
    ok = sched.get("status") == "healthy"
    hb = sched.get("seconds_since_heartbeat")
    lines = [f"{'🟢' if ok else '🔴'} *Scheduler:* {sched.get('status', 'unknown')}"
             + (f" (last heartbeat {hb}s ago)" if hb is not None else "")]
    for d in status.get("dags", []):
        rate = d.get("success_rate_24h")
        icon = "⚪" if rate is None else ("🟢" if rate >= 0.95 else "🟡" if rate >= 0.8 else "🔴")
        rate_s = "no runs" if rate is None else f"{rate:.0%} of {d['runs_24h']} runs"
        last = d.get("last_run_state") or "n/a"
        lines.append(f"{icon} `{d['dag_id']}` — {rate_s} succeeded (24h), last run: {last}"
                     + (" (paused)" if d.get("paused") else ""))
    inc = status.get("open_incidents")
    if isinstance(inc, list):
        lines.append(f"\n*Open incidents:* {len(inc)}")
        lines += [f"• #{i['id']} {i.get('dag_id') or 'platform'} — {i['alert']} "
                  f"({CAUSE_LABELS.get(i.get('root_cause') or '', 'investigating')})" for i in inc[:8]]
    return [{"type": "header", "text": {"type": "plain_text", "text": "Platform health"}},
            _section("\n".join(lines))]


def runbook_search_blocks(query: str, results: list[dict], runbooks: dict[str, Any]) -> list[dict]:
    if not results:
        return [_section(f"No runbook matched *{query}*. Try describing the error message you see.")]
    blocks: list[dict] = [_section(f"Runbooks for *{query}*:")]
    for r in results:
        rb = runbooks.get(r["runbook_id"])
        steps = "\n".join(f"{i}. {s}" for i, s in enumerate(rb.fix_steps[:5], 1)) if rb else ""
        blocks.append(_section(f"*{r['title']}* (`{r['runbook_id']}`, relevance {r['score']:.2f})\n{steps}"))
    return blocks
