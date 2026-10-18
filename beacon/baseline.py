"""Rule-based baseline: keyword/regex rules that map log text to a root cause.

The evaluation runs this on the same evidence the agent saw, to show what the LLM adds
(or doesn't) over a cheap deterministic classifier. Rules are checked in order; the first
match wins, so more specific signals come before generic ones like "Traceback".
"""
from __future__ import annotations

import re

from beacon.models import CAUSE_TO_RUNBOOK, RootCause

RULES: list[tuple[re.Pattern, RootCause]] = [
    (re.compile(r"SchedulerHeartbeatMissing|scheduler[^\n]{0,40}unhealthy|no scheduler heartbeat", re.I),
     RootCause.scheduler_unhealthy),
    (re.compile(r"No space left on device|Errno 28|disk (?:is )?full", re.I), RootCause.storage_full),
    (re.compile(r"SIGKILL|return code -9|MemoryError|out of memory|\bOOM\b|Killed", re.I), RootCause.resource_limit),
    (re.compile(r"password authentication failed|could not connect to server|OperationalError|"
                r"conn_id `?\w+`? isn't defined|connection to server .* failed", re.I),
     RootCause.credentials_or_connection),
    (re.compile(r"\b50[0234] Server Error|Service Unavailable|Connection refused|Max retries exceeded", re.I),
     RootCause.dependency_unavailable),
    (re.compile(r"ReadTimeout|timed out|did not respond within|SLA miss", re.I), RootCause.dependency_latency),
    (re.compile(r"FileNotFoundError|No such file or directory|input file not found", re.I),
     RootCause.missing_upstream_data),
    (re.compile(r"Traceback|KeyError|TypeError|AttributeError|NameError|ValueError|IndexError", re.I),
     RootCause.code_error),
]


def classify(text: str) -> RootCause:
    for pattern, cause in RULES:
        if pattern.search(text or ""):
            return cause
    return RootCause.unknown


def runbook_for(cause: RootCause) -> str | None:
    return CAUSE_TO_RUNBOOK.get(cause)
