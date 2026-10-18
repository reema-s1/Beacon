"""Test configuration: in-memory SQLite, fake service URLs, no API keys."""
from __future__ import annotations

import os

# Must be set before any beacon module reads settings.
os.environ.update({
    "BEACON_DATABASE_URL": "sqlite://",
    "AIRFLOW_URL": "http://airflow.test",
    "PROMETHEUS_URL": "http://prom.test",
    "BEACON_API_URL": "http://beacon.test",
    "LLM_MODEL": "",
    "LLM_API_KEY": "",
    "SLACK_BOT_TOKEN": "",
    "SLACK_APP_TOKEN": "",
    "SLACK_INCIDENT_CHANNEL": "",
    "PULSE_URL": "",
    "PULSE_API_KEY": "",
    "RERUN_ALLOWED_USERS": "UALLOWED",
    "AGENT_MAX_STEPS": "8",
})

import json  # noqa: E402
from dataclasses import dataclass, field  # noqa: E402

import pytest  # noqa: E402

from beacon.agent.llm import ChatResult, ToolCall  # noqa: E402
from beacon.store import Base, get_engine  # noqa: E402


@pytest.fixture(autouse=True)
def fresh_db():
    engine = get_engine()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield


def tool_call(name: str, args: dict, call_id: str | None = None) -> ChatResult:
    return ChatResult(content=None, tool_calls=[ToolCall(call_id or f"call_{name}", name, json.dumps(args))],
                      finish_reason="tool_calls", usage={"total_tokens": 100})


def final(payload: dict | str) -> ChatResult:
    text = payload if isinstance(payload, str) else json.dumps(payload)
    return ChatResult(content=text, finish_reason="stop", usage={"total_tokens": 50})


GOOD_DIAGNOSIS = {
    "summary": "The sales pipeline failed because its input file was missing.",
    "user_impact": "sales_daily_etl did not load today's sales.",
    "root_cause": "missing_upstream_data",
    "confidence": 0.9,
    "evidence": ["FileNotFoundError: Expected input file not found: /opt/airflow/data/incoming/sales.csv"],
    "runbook_id": "rb-missing-input",
    "suggested_action": "Ask the producer to redeliver sales.csv, then rerun extract.",
}


@dataclass
class ScriptedLLM:
    """Returns pre-scripted replies in order; records every request."""
    replies: list[ChatResult]
    model: str = "fake-model"
    provider: str = "fake"
    calls: list[dict] = field(default_factory=list)

    async def chat(self, messages, tools=None, tool_choice=None, json_mode=False):
        self.calls.append({"messages": [dict(m) for m in messages], "tools": tools, "tool_choice": tool_choice,
                           "json_mode": json_mode})
        if not self.replies:
            raise AssertionError("ScriptedLLM ran out of replies")
        reply = self.replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply


@dataclass
class FakeNotifier:
    posted: list[dict] = field(default_factory=list)
    updated: list[dict] = field(default_factory=list)
    threads: list[tuple[dict, str]] = field(default_factory=list)
    enabled: bool = True

    async def post_incident(self, inc, runbook):
        self.posted.append(inc)
        return "C123", f"1700000000.{len(self.posted):06d}"

    async def update_incident(self, inc, runbook):
        self.updated.append(inc)

    async def post_thread(self, inc, text):
        self.threads.append((inc, text))
