"""Trace agent runs into Pulse (the author's agent observability platform).

Matches Pulse's ingest API:
  POST  /api/v1/ingest/trace               -> {"trace_id"}
  POST  /api/v1/ingest/spans               {"trace_id", "spans": [...]}
  PATCH /api/v1/ingest/trace/{id}/finish   {"status", "output", "error", "end_time"}
authenticated with an `X-API-Key: ps_...` project key.

Fail-open: if PULSE_URL is unset or Pulse is unreachable, tracing does nothing and the
agent carries on.
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx

from beacon.config import Settings, get_settings

log = logging.getLogger("beacon.pulse")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _jsonable(obj: Any) -> Any:
    return json.loads(json.dumps(obj, default=str))


def _as_dict(value: Any) -> dict | None:
    if value is None:
        return None
    return value if isinstance(value, dict) else {"value": value}


@dataclass
class Span:
    name: str
    span_type: str
    parent_id: uuid.UUID | None
    input: dict | None = None
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    start: datetime = field(default_factory=_now)
    _t0: float = field(default_factory=time.monotonic)
    end: datetime | None = None
    duration_ms: int | None = None
    status: str = "success"
    output: dict | None = None
    error: str | None = None
    metadata: dict | None = None
    llm_call: dict | None = None
    tool_call: dict | None = None
    depth: int = 0

    def finish(self, output: Any = None, error: str | None = None) -> None:
        self.end = _now()
        self.duration_ms = int((time.monotonic() - self._t0) * 1000)
        self.output = _as_dict(output)
        if error:
            self.status, self.error = "error", error

    def to_payload(self) -> dict:
        p = {"id": str(self.id), "parent_span_id": str(self.parent_id) if self.parent_id else None,
             "name": self.name, "span_type": self.span_type, "status": self.status,
             "input": self.input, "output": self.output, "error": self.error,
             "start_time": self.start.isoformat(), "end_time": (self.end or _now()).isoformat(),
             "duration_ms": self.duration_ms, "metadata": self.metadata}
        if self.llm_call:
            p["llm_call"] = self.llm_call
        if self.tool_call:
            p["tool_call"] = self.tool_call
        return _jsonable(p)


class Trace:
    """Collects spans for one agent run; ships them to Pulse when finished."""

    def __init__(self, tracer: "Tracer", name: str, input: dict | None, metadata: dict | None, tags: list[str] | None):
        self.tracer, self.name, self.input, self.metadata, self.tags = tracer, name, input, metadata, tags
        self.trace_id: str | None = None
        self.spans: list[Span] = []
        self.root: Span | None = None

    @property
    def enabled(self) -> bool:
        return self.trace_id is not None

    def start_span(self, name: str, span_type: str = "custom", input: Any = None,
                   parent: Span | None = None) -> Span:
        parent = parent or self.root
        span = Span(name=name, span_type=span_type, parent_id=parent.id if parent else None,
                    input=_as_dict(input), depth=(parent.depth + 1) if parent else 0)
        self.spans.append(span)
        return span

    def record_llm(self, span: Span, *, model: str, provider: str, messages: list, response: dict | None,
                   usage: dict | None, finish_reason: str | None, temperature: float | None) -> None:
        usage = usage or {}
        span.llm_call = {"provider": provider, "model": model, "messages": _jsonable(messages[-6:]),
                         "response": _jsonable(response) if response else None,
                         "prompt_tokens": usage.get("prompt_tokens"),
                         "completion_tokens": usage.get("completion_tokens"),
                         "total_tokens": usage.get("total_tokens"),
                         "temperature": temperature, "finish_reason": finish_reason}

    def record_tool(self, span: Span, *, tool_name: str, tool_input: dict, tool_output: dict) -> None:
        err = tool_output.get("error") if isinstance(tool_output, dict) else None
        span.tool_call = {"tool_name": tool_name, "tool_input": _jsonable(tool_input),
                          "tool_output": _jsonable(tool_output), "status": "error" if err else "success",
                          "error": err}


class Tracer:
    def __init__(self, settings: Settings | None = None):
        s = settings or get_settings()
        self.base_url = s.pulse_url.rstrip("/")
        self.api_key = s.pulse_api_key
        self.enabled = bool(self.base_url)

    async def _send(self, method: str, path: str, body: dict) -> dict | None:
        try:
            async with httpx.AsyncClient(base_url=self.base_url, timeout=5.0,
                                         headers={"X-API-Key": self.api_key}) as client:
                resp = await client.request(method, path, json=_jsonable(body))
                resp.raise_for_status()
                return resp.json()
        except Exception as exc:
            log.warning("Pulse %s %s failed: %s", method, path, exc)
            return None

    @asynccontextmanager
    async def trace(self, name: str, input: dict | None = None, metadata: dict | None = None,
                    tags: list[str] | None = None):
        t = Trace(self, name, input, metadata, tags)
        t.root = t.start_span(name, "agent", input)
        if self.enabled:
            resp = await self._send("POST", "/api/v1/ingest/trace",
                                    {"name": name, "input": input, "metadata": metadata, "tags": tags,
                                     "start_time": t.root.start.isoformat()})
            t.trace_id = resp.get("trace_id") if resp else None
        error: str | None = None
        try:
            yield t
        except Exception as exc:
            error = f"{exc.__class__.__name__}: {exc}"
            raise
        finally:
            if t.root.end is None:
                t.root.finish(error=error)
            if t.enabled:
                # Parents before children so Pulse can resolve parent_span_id.
                spans = sorted(t.spans, key=lambda s: s.depth)
                await self._send("POST", "/api/v1/ingest/spans",
                                 {"trace_id": t.trace_id, "spans": [s.to_payload() for s in spans]})
                await self._send("PATCH", f"/api/v1/ingest/trace/{t.trace_id}/finish",
                                 {"status": "error" if (error or t.root.status == "error") else "success",
                                  "output": t.root.output, "error": error or t.root.error,
                                  "end_time": (t.root.end or _now()).isoformat()})
