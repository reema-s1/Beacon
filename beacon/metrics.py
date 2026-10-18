"""Beacon's own Prometheus metrics, exported by beacon-api at /metrics."""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

INCIDENTS = Counter("beacon_incidents_total", "Incidents triaged, by diagnosed root cause", ["root_cause"])
TRIAGE_SECONDS = Histogram("beacon_triage_seconds", "Time from alert received to summary posted",
                           buckets=(5, 10, 15, 20, 30, 45, 60, 90, 120, 180, 300, 600))
AGENT_TOOL_CALLS = Counter("beacon_agent_tool_calls_total", "Tool calls made by the agent", ["tool"])
AGENT_STEPS = Histogram("beacon_agent_steps", "LLM rounds per agent run", buckets=(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
FEEDBACK = Counter("beacon_feedback_total", "User feedback on incident diagnoses", ["value"])
INCIDENTS_RESOLVED = Counter("beacon_incidents_resolved_total", "Incidents marked resolved")
OPEN_INCIDENTS = Gauge("beacon_open_incidents", "Incidents not yet resolved")
EVAL_ROOT_CAUSE_ACCURACY = Gauge("beacon_eval_root_cause_accuracy", "Latest evaluation root-cause accuracy",
                                 ["system"])
EVAL_RUNBOOK_ACCURACY = Gauge("beacon_eval_runbook_accuracy", "Latest evaluation runbook accuracy", ["system"])
