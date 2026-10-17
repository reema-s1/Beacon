"""The investigation agent: a bounded tool-calling loop that ends in a validated Diagnosis."""
from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field

from pydantic import ValidationError

from beacon import metrics
from beacon.agent.llm import ChatModel, ChatResult, LLMError, ToolCallFormatError
from beacon.agent.prompts import FINALIZE_PROMPT, FIX_JSON_PROMPT, SYSTEM_PROMPT
from beacon.config import get_settings
from beacon.models import Diagnosis, RootCause
from beacon.pulse import Tracer
from beacon.tools import AGENT_TOOLS, Tool, call_tool

log = logging.getLogger("beacon.agent")


@dataclass
class ToolCallRecord:
    step: int
    tool: str
    arguments: dict
    result_preview: str
    error: str | None = None
    duration_ms: int = 0

    def as_dict(self) -> dict:
        return {"step": self.step, "tool": self.tool, "arguments": self.arguments,
                "result_preview": self.result_preview, "error": self.error, "duration_ms": self.duration_ms}


@dataclass
class AgentResult:
    diagnosis: Diagnosis
    steps: int
    tool_calls: list[ToolCallRecord] = field(default_factory=list)
    output_retries: int = 0
    hit_step_cap: bool = False
    total_tokens: int = 0


def extract_json(text: str | None) -> dict:
    """Pull the first JSON object out of a model reply (tolerates code fences and prose)."""
    if not text:
        raise ValueError("empty reply")
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.M)
    try:
        obj = json.loads(cleaned)
        if isinstance(obj, dict):
            return obj
    except json.JSONDecodeError:
        pass
    start = cleaned.find("{")
    while start != -1:
        depth = 0
        in_str = esc = False
        for i in range(start, len(cleaned)):
            ch = cleaned[i]
            if in_str:
                esc = (ch == "\\") and not esc
                if ch == '"' and not esc:
                    in_str = False
                continue
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(cleaned[start:i + 1])
                    except json.JSONDecodeError:
                        break
        start = cleaned.find("{", start + 1)
    raise ValueError("no JSON object found in reply")


def parse_diagnosis(text: str | None) -> Diagnosis:
    data = extract_json(text)
    if isinstance(data.get("root_cause"), str):
        data["root_cause"] = data["root_cause"].strip().lower()
    return Diagnosis.model_validate(data)


def _errors(exc: Exception) -> str:
    if isinstance(exc, ValidationError):
        return "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors())[:600]
    return str(exc)[:300]


def fallback_diagnosis(reason: str) -> Diagnosis:
    return Diagnosis(summary="Beacon could not complete an automated diagnosis for this incident.",
                     user_impact="Unknown; see the alert details.", root_cause=RootCause.unknown,
                     confidence=0.0, evidence=[reason], runbook_id=None,
                     suggested_action="Ask the on-call engineer to investigate manually.")


class Agent:
    def __init__(self, llm: ChatModel, tools: dict[str, Tool] | None = None, max_steps: int | None = None,
                 tool_timeout: float | None = None, tracer: Tracer | None = None):
        s = get_settings()
        self.llm = llm
        self.tools = tools if tools is not None else AGENT_TOOLS
        self.max_steps = max_steps or s.agent_max_steps
        self.tool_timeout = tool_timeout or s.tool_timeout_seconds
        self.tracer = tracer or Tracer()
        self.max_tool_chars = s.tool_output_max_chars
        self.specs = [t.openai_spec() for t in self.tools.values()]

    async def _chat(self, trace, messages: list[dict], *, tools: bool, label: str) -> ChatResult:
        span = trace.start_span(label, "llm", {"messages": len(messages)})
        try:
            if tools:
                result = await self.llm.chat(messages, tools=self.specs)
            else:
                result = await self.llm.chat(messages, tools=self.specs, tool_choice="none")
        except Exception as exc:
            span.finish(error=f"{exc.__class__.__name__}: {exc}")
            raise
        span.finish(output={"content": (result.content or "")[:500],
                            "tool_calls": [tc.name for tc in result.tool_calls]})
        trace.record_llm(span, model=self.llm.model, provider=self.llm.provider, messages=messages,
                         response=result.raw, usage=result.usage, finish_reason=result.finish_reason,
                         temperature=get_settings().llm_temperature)
        return result

    async def _run_tool(self, trace, step: int, name: str, raw_args: str) -> tuple[dict, ToolCallRecord]:
        t0 = time.monotonic()
        tool = self.tools.get(name)
        try:
            args = json.loads(raw_args or "{}")
            if not isinstance(args, dict):
                raise ValueError("arguments must be a JSON object")
        except (json.JSONDecodeError, ValueError) as exc:
            args, result = {}, {"error": f"arguments are not valid JSON: {exc}"}
        else:
            if tool is None:
                result = {"error": f"unknown tool '{name}'. Available: {', '.join(self.tools)}"}
            else:
                span = trace.start_span(f"tool:{name}", "tool", args)
                result = await call_tool(tool, args, timeout=self.tool_timeout)
                span.finish(output=result, error=result.get("error"))
                trace.record_tool(span, tool_name=name, tool_input=args, tool_output=result)
        metrics.AGENT_TOOL_CALLS.labels(tool=name if tool else "invalid").inc()
        text = json.dumps(result, default=str)
        record = ToolCallRecord(step=step, tool=name, arguments=args, result_preview=text[:300],
                                error=result.get("error"), duration_ms=int((time.monotonic() - t0) * 1000))
        return result, record

    def _truncate(self, result: dict) -> str:
        text = json.dumps(result, default=str, separators=(",", ":"))
        if len(text) > self.max_tool_chars:
            text = text[: self.max_tool_chars] + '..."(truncated)"'
        return text

    async def _final_answer(self, trace, messages: list[dict], first: ChatResult | None,
                            result: AgentResult) -> Diagnosis:
        """Validate the final reply; on failure ask once more with the validation errors."""
        reply = first if first is not None else await self._chat(trace, messages, tools=False, label="llm:finalize")
        try:
            return parse_diagnosis(reply.content)
        except (ValueError, ValidationError) as exc:
            result.output_retries += 1
            messages.append({"role": "assistant", "content": reply.content or ""})
            messages.append({"role": "user", "content": FIX_JSON_PROMPT.format(
                errors=_errors(exc), causes=", ".join(c.value for c in RootCause))})
            retry = await self._chat(trace, messages, tools=False, label="llm:fix-output")
            try:
                return parse_diagnosis(retry.content)
            except (ValueError, ValidationError) as exc2:
                return fallback_diagnosis(f"model output failed validation twice: {_errors(exc2)}")

    async def investigate(self, context: str, *, trace_name: str = "beacon-investigation",
                          metadata: dict | None = None) -> AgentResult:
        messages: list[dict] = [{"role": "system", "content": SYSTEM_PROMPT},
                                {"role": "user", "content": context}]
        result = AgentResult(diagnosis=fallback_diagnosis("not started"), steps=0)
        async with self.tracer.trace(trace_name, input={"context": context}, metadata=metadata,
                                     tags=["beacon"]) as trace:
            final: ChatResult | None = None
            format_retries = 0
            while result.steps < self.max_steps:
                result.steps += 1
                try:
                    reply = await self._chat(trace, messages, tools=True, label=f"llm:step-{result.steps}")
                except ToolCallFormatError as exc:
                    if format_retries >= 1:
                        raise LLMError(f"model kept producing malformed tool calls: {exc}") from exc
                    format_retries += 1
                    messages.append({"role": "user", "content": "Your last tool call was malformed. "
                                     "Call one tool at a time with valid JSON arguments."})
                    continue
                result.total_tokens += (reply.usage or {}).get("total_tokens") or 0
                if not reply.tool_calls:
                    final = reply
                    break
                messages.append(reply.assistant_message())
                for tc in reply.tool_calls:
                    out, record = await self._run_tool(trace, result.steps, tc.name, tc.arguments)
                    result.tool_calls.append(record)
                    messages.append({"role": "tool", "tool_call_id": tc.id, "content": self._truncate(out)})
            else:
                result.hit_step_cap = True
                messages.append({"role": "user", "content": FINALIZE_PROMPT})

            result.diagnosis = await self._final_answer(trace, messages, final, result)
            trace.root.finish(output=result.diagnosis.model_dump(mode="json"))
        metrics.AGENT_STEPS.observe(result.steps)
        return result
