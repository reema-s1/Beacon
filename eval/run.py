"""beacon-eval: inject random faults, wait for Beacon's diagnosis, score it against ground
truth and against the rule-based baseline.

  beacon-eval --runs 30 --seed 42
"""
from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import httpx

from beacon import baseline, tools
from beacon.config import get_settings
from chaos.cli import inject, reset
from chaos.platform import Platform
from chaos.scenarios import SCENARIOS, Scenario

RESULTS_DIR = Path(__file__).resolve().parent / "results"
CAUSES = ["code_error", "missing_upstream_data", "dependency_unavailable", "dependency_latency",
          "credentials_or_connection", "resource_limit", "scheduler_unhealthy", "storage_full", "unknown"]


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _ts(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _matches(sc: Scenario, inc: dict) -> bool:
    if sc.target_dag is None:
        return inc["alert_name"] == "SchedulerHeartbeatMissing"
    return inc.get("dag_id") == sc.target_dag


def wait_for_incident(api: httpx.Client, injection_id: int, sc: Scenario, timeout: int) -> dict | None:
    """First triaged incident linked to this injection that concerns the scenario's target."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        incs = api.get("/incidents", params={"fault_injection_id": injection_id, "limit": 50}).json()
        relevant = sorted([i for i in incs if _matches(sc, i)], key=lambda i: i["created_at"])
        if relevant and relevant[0].get("triaged_at"):
            return relevant[0]
        time.sleep(5)
    return None


async def _baseline_text(sc: Scenario, inc: dict | None) -> str:
    """The same evidence the agent starts from: the alert name plus the failed tasks' log tails."""
    parts = [inc["alert_name"]] if inc else []
    if sc.target_dag is None:
        status = await tools.call_tool(tools.READ_ONLY_TOOLS["get_platform_status"], {})
        parts.append(json.dumps(status.get("scheduler", {})))
        return "\n".join(parts)
    run_id = inc.get("run_id") if inc else None
    if not run_id:
        failures = await tools.list_recent_failures(sc.target_dag, since_minutes=30)
        runs = failures.get("failed_runs", [])
        run_id = runs[0]["run_id"] if runs else None
    if not run_id:
        return "\n".join(parts)
    run = await tools.get_dag_run(sc.target_dag, run_id)
    for task in run["tasks"]:
        if task["state"] == "failed":
            logs = await tools.call_tool(tools.READ_ONLY_TOOLS["get_task_logs"],
                                         {"dag_id": sc.target_dag, "run_id": run_id, "task_id": task["task_id"]})
            parts.extend(logs.get("log", []))
    return "\n".join(parts)


def run_one(i: int, sc: Scenario, platform: Platform, api: httpx.Client, timeout: int) -> dict:
    injection, run_id = inject(platform, sc.name, trigger=True)
    log(f"run {i}: injected {sc.name} (injection {injection.id}){' run ' + run_id if run_id else ''}")
    inc = wait_for_incident(api, injection.id, sc, timeout)
    try:
        text = asyncio.run(_baseline_text(sc, inc))
    except Exception as exc:  # baseline evidence is best-effort
        text = f"(could not fetch evidence: {exc})"
    base_cause = baseline.classify(text).value
    record = {
        "run": i, "scenario": sc.name, "injection_id": injection.id, "ground_truth": sc.ground_truth_cause,
        "expected_runbook": sc.expected_runbook, "injected_at": injection.injected_at.isoformat(),
        "incident_id": inc["id"] if inc else None, "timed_out": inc is None,
        "agent_root_cause": inc.get("root_cause") if inc else None,
        "agent_runbook": inc.get("runbook_id") if inc else None,
        "agent_confidence": inc.get("confidence") if inc else None,
        "agent_steps": inc.get("agent_steps") if inc else None,
        "baseline_root_cause": base_cause, "baseline_runbook": baseline.runbook_for(baseline.RootCause(base_cause)),
    }
    record["agent_cause_correct"] = record["agent_root_cause"] == sc.ground_truth_cause
    record["agent_runbook_correct"] = record["agent_runbook"] == sc.expected_runbook
    record["baseline_cause_correct"] = base_cause == sc.ground_truth_cause
    record["baseline_runbook_correct"] = record["baseline_runbook"] == sc.expected_runbook
    if inc:
        created, triaged = _ts(inc["created_at"]), _ts(inc["triaged_at"])
        record["end_to_end_seconds"] = round((triaged - injection.injected_at).total_seconds(), 1)
        record["triage_seconds"] = round((triaged - created).total_seconds(), 1)
        record["summary"] = inc.get("summary")
        record["evidence"] = inc.get("evidence")
        record["suggested_action"] = inc.get("suggested_action")
        record["tool_calls"] = [{"tool": t["tool"], "arguments": t["arguments"], "error": t.get("error")}
                                for t in (inc.get("investigation") or [])]
        record["agent_error"] = inc.get("error")
    log(f"run {i}: agent={record['agent_root_cause']} ({'ok' if record['agent_cause_correct'] else 'WRONG'}), "
        f"baseline={base_cause} ({'ok' if record['baseline_cause_correct'] else 'WRONG'}), "
        f"e2e={record.get('end_to_end_seconds')}s")
    return record


def error_record(i: int, sc: Scenario, exc: Exception) -> dict:
    return {"run": i, "scenario": sc.name, "ground_truth": sc.ground_truth_cause,
            "expected_runbook": sc.expected_runbook, "timed_out": True, "harness_error": repr(exc)[:300],
            "agent_root_cause": None, "agent_runbook": None, "agent_cause_correct": False,
            "agent_runbook_correct": False, "baseline_root_cause": None, "baseline_runbook": None,
            "baseline_cause_correct": False, "baseline_runbook_correct": False}


def recover(platform: Platform, sc: Scenario) -> None:
    for step in reset(platform):
        log(f"  reset: {step}")
    if not platform.wait_scheduler_healthy(240):
        log("  warning: scheduler not healthy after reset")
    if sc.target_dag and not platform.wait_dag_success(sc.target_dag, timeout=300):
        log(f"  warning: {sc.target_dag} did not succeed after reset")


# ── Reporting ─────────────────────────────────────────────────────────────────

def _pct(xs: list[bool]) -> float:
    return round(sum(xs) / len(xs), 3) if xs else 0.0


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    v = sorted(values)
    idx = q * (len(v) - 1)
    lo, hi = int(idx), min(int(idx) + 1, len(v) - 1)
    return round(v[lo] + (v[hi] - v[lo]) * (idx - lo), 1)


def summarise(records: list[dict]) -> dict:
    e2e = [r["end_to_end_seconds"] for r in records if r.get("end_to_end_seconds") is not None]
    tri = [r["triage_seconds"] for r in records if r.get("triage_seconds") is not None]
    per = defaultdict(list)
    for r in records:
        per[r["scenario"]].append(r)
    confusion: dict[str, Counter] = defaultdict(Counter)
    for r in records:
        confusion[r["ground_truth"]][r["agent_root_cause"] or "no_incident"] += 1
    return {
        "runs": len(records),
        "timeouts": sum(r["timed_out"] for r in records),
        "agent": {"root_cause_accuracy": _pct([r["agent_cause_correct"] for r in records]),
                  "runbook_accuracy": _pct([r["agent_runbook_correct"] for r in records])},
        "baseline": {"root_cause_accuracy": _pct([r["baseline_cause_correct"] for r in records]),
                     "runbook_accuracy": _pct([r["baseline_runbook_correct"] for r in records])},
        "latency_seconds": {"end_to_end_p50": _quantile(e2e, 0.5), "end_to_end_p95": _quantile(e2e, 0.95),
                            "triage_p50": _quantile(tri, 0.5), "triage_p95": _quantile(tri, 0.95)},
        "per_scenario": {s: {"runs": len(rs), "agent_root_cause": _pct([r["agent_cause_correct"] for r in rs]),
                             "agent_runbook": _pct([r["agent_runbook_correct"] for r in rs]),
                             "baseline_root_cause": _pct([r["baseline_cause_correct"] for r in rs])}
                         for s, rs in sorted(per.items())},
        "confusion": {gt: dict(c) for gt, c in confusion.items()},
    }


def markdown_report(summary: dict, records: list[dict], meta: dict) -> str:
    a, b, lat = summary["agent"], summary["baseline"], summary["latency_seconds"]
    out = [f"# Beacon evaluation — {meta['started_at'][:19]} UTC", "",
           f"{summary['runs']} runs, seed {meta['seed']}, model `{meta['model']}`, "
           f"{summary['timeouts']} run(s) with no triaged incident before the timeout.", "",
           "## Accuracy", "", "| | Root cause | Runbook |", "|---|---|---|",
           f"| Beacon agent | {a['root_cause_accuracy']:.0%} | {a['runbook_accuracy']:.0%} |",
           f"| Rule-based baseline | {b['root_cause_accuracy']:.0%} | {b['runbook_accuracy']:.0%} |", "",
           "## Latency", "", "| | p50 | p95 |", "|---|---|---|",
           f"| Injection → Slack post (end to end) | {lat['end_to_end_p50']}s | {lat['end_to_end_p95']}s |",
           f"| Alert received → summary posted (triage) | {lat['triage_p50']}s | {lat['triage_p95']}s |", "",
           "## Per scenario", "", "| Scenario | Runs | Agent root cause | Agent runbook | Baseline root cause |",
           "|---|---|---|---|---|"]
    for s, v in summary["per_scenario"].items():
        out.append(f"| {s} | {v['runs']} | {v['agent_root_cause']:.0%} | {v['agent_runbook']:.0%} | "
                   f"{v['baseline_root_cause']:.0%} |")
    cols = CAUSES + ["no_incident"]
    used = [c for c in cols if any(c in row for row in summary["confusion"].values())]
    out += ["", "## Confusion matrix (agent)", "", "Rows: ground truth. Columns: agent's answer.", "",
            "| ground truth \\ predicted | " + " | ".join(used) + " |", "|---" * (len(used) + 1) + "|"]
    for gt in CAUSES:
        if gt in summary["confusion"]:
            row = summary["confusion"][gt]
            out.append(f"| {gt} | " + " | ".join(str(row.get(c, 0) or "") for c in used) + " |")
    misses = [r for r in records if not (r["agent_cause_correct"] and r["agent_runbook_correct"])]
    out += ["", "## Failures and the agent's reasoning", ""]
    if not misses:
        out.append("None: every run got both the root cause and the runbook right.")
    for r in misses:
        out += [f"### Run {r['run']}: {r['scenario']} (expected `{r['ground_truth']}` / `{r['expected_runbook']}`)", ""]
        if r.get("harness_error"):
            out += [f"The evaluation harness failed on this run: `{r['harness_error']}`", ""]
            continue
        if r["timed_out"]:
            out += ["No triaged incident arrived before the timeout.", ""]
            continue
        out += [f"- Agent answered `{r['agent_root_cause']}` / `{r['agent_runbook']}` "
                f"(confidence {r['agent_confidence']}, {r['agent_steps']} steps)",
                f"- Summary: {r.get('summary')}",
                "- Evidence: " + "; ".join(f"`{e}`" for e in (r.get("evidence") or [])[:4]),
                "- Tool calls: " + (", ".join(t["tool"] for t in r.get("tool_calls") or []) or "none")]
        if r.get("agent_error"):
            out.append(f"- Agent error: {r['agent_error']}")
        out.append("")
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="beacon-eval")
    parser.add_argument("--runs", type=int, default=30)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--timeout", type=int, default=420, help="seconds to wait for a triaged incident")
    parser.add_argument("--pace", type=int, default=20, help="pause between runs (free-tier rate limits)")
    parser.add_argument("--scenarios", nargs="*", choices=sorted(SCENARIOS), help="restrict to these scenarios")
    parser.add_argument("--no-push", action="store_true", help="don't push accuracy gauges to beacon-api")
    args = parser.parse_args(argv)

    s = get_settings()
    rng = random.Random(args.seed)
    pool = [SCENARIOS[n] for n in (args.scenarios or sorted(SCENARIOS))]
    platform = Platform()
    api = httpx.Client(base_url=s.beacon_api_url, timeout=30.0)
    started = datetime.now(timezone.utc)
    meta = {"started_at": started.isoformat(), "seed": args.seed, "runs": args.runs, "model": s.llm_model,
            "llm_base_url": s.llm_base_url}
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = started.strftime("%Y%m%dT%H%M%SZ")
    json_path, md_path = RESULTS_DIR / f"{stamp}.json", RESULTS_DIR / f"{stamp}.md"

    log(f"beacon-eval: {args.runs} runs, seed {args.seed}, model {s.llm_model}")
    recover(platform, SCENARIOS["scheduler_down"])
    records: list[dict] = []
    try:
        for i in range(1, args.runs + 1):
            sc = rng.choice(pool)
            try:
                records.append(run_one(i, sc, platform, api, args.timeout))
            except Exception as exc:  # one broken run must not end the evaluation
                log(f"run {i}: harness error, scored as a miss: {exc!r}")
                records.append(error_record(i, sc, exc))
            finally:
                recover(platform, sc)
            summary = summarise(records)
            json_path.write_text(json.dumps({"meta": meta, "summary": summary, "records": records}, indent=2))
            if i < args.runs:
                time.sleep(args.pace)
    except KeyboardInterrupt:
        log("interrupted; writing partial results")

    summary = summarise(records)
    meta["finished_at"] = datetime.now(timezone.utc).isoformat()
    json_path.write_text(json.dumps({"meta": meta, "summary": summary, "records": records}, indent=2))
    md_path.write_text(markdown_report(summary, records, meta), encoding="utf-8")
    log(f"wrote {json_path} and {md_path}")
    log(f"agent: {summary['agent']}  baseline: {summary['baseline']}  latency: {summary['latency_seconds']}")
    if not args.no_push and records:
        resp = api.post("/eval/metrics", json={
            "root_cause_accuracy": {"agent": summary["agent"]["root_cause_accuracy"],
                                    "baseline": summary["baseline"]["root_cause_accuracy"]},
            "runbook_accuracy": {"agent": summary["agent"]["runbook_accuracy"],
                                 "baseline": summary["baseline"]["runbook_accuracy"]}})
        log(f"pushed accuracy gauges to beacon-api: {resp.status_code}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
