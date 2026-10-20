import asyncio
import json

import httpx
import pytest
import respx

from beacon.agent.llm import LLMError, OpenAICompatibleLLM, ToolCallFormatError
from beacon.agent.loop import Agent, extract_json
from beacon.config import Settings
from beacon.models import RootCause
from beacon.pulse import Tracer
from beacon.tools import NoArgs, SearchRunbooksArgs, Tool
from tests.conftest import GOOD_DIAGNOSIS, ScriptedLLM, final, tool_call


def fake_tools(calls: list):
    async def search_runbooks(query: str, k: int = 3) -> dict:
        calls.append(("search_runbooks", query, k))
        return {"results": [{"runbook_id": "rb-missing-input", "score": 0.8}]}

    async def get_platform_status() -> dict:
        calls.append(("get_platform_status",))
        return {"scheduler": {"status": "healthy"}}

    async def slow() -> dict:
        await asyncio.sleep(5)
        return {}

    return {
        "search_runbooks": Tool("search_runbooks", "search", SearchRunbooksArgs, search_runbooks),
        "get_platform_status": Tool("get_platform_status", "status", NoArgs, get_platform_status),
        "slow_tool": Tool("slow_tool", "never finishes in time", NoArgs, slow),
    }


async def test_happy_path_calls_tools_then_returns_validated_diagnosis():
    seen = []
    llm = ScriptedLLM([
        tool_call("get_platform_status", {}),
        tool_call("search_runbooks", {"query": "FileNotFoundError sales.csv", "k": 2}),
        final("Here you go:\n```json\n" + json.dumps(GOOD_DIAGNOSIS) + "\n```"),
    ])
    result = await Agent(llm, tools=fake_tools(seen)).investigate("Alert DagRunFailed for sales_daily_etl")
    assert result.diagnosis.root_cause is RootCause.missing_upstream_data
    assert result.diagnosis.runbook_id == "rb-missing-input"
    assert result.steps == 3 and result.output_retries == 0 and not result.hit_step_cap
    assert [t.tool for t in result.tool_calls] == ["get_platform_status", "search_runbooks"]
    assert seen[1] == ("search_runbooks", "FileNotFoundError sales.csv", 2)
    # Tool results are fed back to the model as tool messages.
    last_messages = llm.calls[-1]["messages"]
    assert any(m["role"] == "tool" and "rb-missing-input" in m["content"] for m in last_messages)


async def test_invalid_output_triggers_exactly_one_retry():
    llm = ScriptedLLM([final({"summary": "x", "root_cause": "gremlins"}), final(GOOD_DIAGNOSIS)])
    result = await Agent(llm, tools=fake_tools([])).investigate("ctx")
    assert result.output_retries == 1
    assert result.diagnosis.root_cause is RootCause.missing_upstream_data
    retry_prompt = llm.calls[1]["messages"][-1]["content"]
    assert "root_cause" in retry_prompt and "not valid" in retry_prompt
    assert llm.calls[1]["tool_choice"] == "none"


async def test_output_invalid_twice_falls_back_to_unknown():
    llm = ScriptedLLM([final("I think it is the network."), final("still not json")])
    result = await Agent(llm, tools=fake_tools([])).investigate("ctx")
    assert result.output_retries == 1
    assert result.diagnosis.root_cause is RootCause.unknown and result.diagnosis.confidence == 0.0


async def test_step_cap_holds_and_forces_a_final_answer():
    llm = ScriptedLLM([tool_call("get_platform_status", {}, f"c{i}") for i in range(8)] + [final(GOOD_DIAGNOSIS)])
    result = await Agent(llm, tools=fake_tools([]), max_steps=8).investigate("ctx")
    assert result.steps == 8 and result.hit_step_cap
    assert len(result.tool_calls) == 8
    assert len(llm.calls) == 9  # 8 tool rounds + one forced final answer
    assert llm.calls[-1]["tool_choice"] == "none"
    assert "used all your tool rounds" in llm.calls[-1]["messages"][-1]["content"]
    assert result.diagnosis.root_cause is RootCause.missing_upstream_data


async def test_bad_tool_arguments_are_returned_to_the_model():
    llm = ScriptedLLM([tool_call("search_runbooks", {"query": "x", "k": 99}),
                       tool_call("nope", {}), final(GOOD_DIAGNOSIS)])
    result = await Agent(llm, tools=fake_tools([])).investigate("ctx")
    assert "invalid arguments for search_runbooks" in result.tool_calls[0].error
    assert "unknown tool 'nope'" in result.tool_calls[1].error
    tool_msgs = [m for m in llm.calls[2]["messages"] if m["role"] == "tool"]
    assert "invalid arguments" in tool_msgs[0]["content"]


async def test_malformed_json_arguments_are_reported():
    from beacon.agent.llm import ChatResult, ToolCall
    llm = ScriptedLLM([ChatResult(content=None, tool_calls=[ToolCall("c1", "search_runbooks", "{not json")]),
                       final(GOOD_DIAGNOSIS)])
    result = await Agent(llm, tools=fake_tools([])).investigate("ctx")
    assert "not valid JSON" in result.tool_calls[0].error


async def test_per_tool_timeout():
    llm = ScriptedLLM([tool_call("slow_tool", {}), final(GOOD_DIAGNOSIS)])
    result = await Agent(llm, tools=fake_tools([]), tool_timeout=0.05).investigate("ctx")
    assert "timed out" in result.tool_calls[0].error


async def test_one_malformed_tool_call_from_provider_is_tolerated():
    llm = ScriptedLLM([ToolCallFormatError("tool_use_failed"), final(GOOD_DIAGNOSIS)])
    result = await Agent(llm, tools=fake_tools([])).investigate("ctx")
    assert result.diagnosis.root_cause is RootCause.missing_upstream_data


def test_extract_json_variants():
    assert extract_json('{"a": 1}') == {"a": 1}
    assert extract_json('```json\n{"a": {"b": "}"}}\n```') == {"a": {"b": "}"}}
    assert extract_json('Sure! {"a": 2} hope that helps') == {"a": 2}
    with pytest.raises(ValueError):
        extract_json("no json here")


# ── LLM client ────────────────────────────────────────────────────────────────

LLM_SETTINGS = Settings(llm_base_url="http://llm.test/v1", llm_model="test-model", llm_api_key="gsk_testtesttesttesttest",
                        llm_max_retries=3)
COMPLETION = {"id": "x", "object": "chat.completion", "created": 1, "model": "test-model",
              "choices": [{"index": 0, "finish_reason": "stop",
                           "message": {"role": "assistant", "content": "{\"ok\": true}"}}],
              "usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8}}


@respx.mock
async def test_llm_retries_429_respecting_retry_after(monkeypatch):
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)
    monkeypatch.setattr("beacon.agent.llm.asyncio.sleep", fake_sleep)
    route = respx.post("http://llm.test/v1/chat/completions").mock(side_effect=[
        httpx.Response(429, headers={"retry-after": "7"}, json={"error": {"message": "rate limited"}}),
        httpx.Response(429, json={"error": {"message": "rate limited"}}),
        httpx.Response(200, json=COMPLETION)])
    result = await OpenAICompatibleLLM(LLM_SETTINGS).chat([{"role": "user", "content": "hi"}])
    assert route.call_count == 3 and result.content == '{"ok": true}'
    assert 7 <= sleeps[0] < 8          # honoured Retry-After
    assert 4 <= sleeps[1] < 5          # exponential backoff (2**2) without a header


@respx.mock
async def test_provider_extras_on_tool_calls_are_echoed_back():
    body = dict(COMPLETION, choices=[{"index": 0, "finish_reason": "tool_calls", "message": {
        "role": "assistant", "content": None, "tool_calls": [{
            "id": "c1", "type": "function", "function": {"name": "get_platform_status", "arguments": "{}"},
            "extra_content": {"google": {"thought_signature": "sig-123"}}}]}}])
    respx.post("http://llm.test/v1/chat/completions").respond(json=body)
    result = await OpenAICompatibleLLM(LLM_SETTINGS).chat([{"role": "user", "content": "hi"}])
    echoed = result.assistant_message()["tool_calls"][0]
    assert echoed["extra_content"] == {"google": {"thought_signature": "sig-123"}}
    assert echoed["function"]["name"] == "get_platform_status"


@respx.mock
async def test_llm_gives_up_after_max_retries(monkeypatch):
    async def fake_sleep(s):
        return None
    monkeypatch.setattr("beacon.agent.llm.asyncio.sleep", fake_sleep)
    respx.post("http://llm.test/v1/chat/completions").respond(429, json={"error": {"message": "slow down"}})
    with pytest.raises(LLMError, match="rate limited"):
        await OpenAICompatibleLLM(LLM_SETTINGS).chat([{"role": "user", "content": "hi"}])


@respx.mock
async def test_llm_maps_tool_use_failed():
    respx.post("http://llm.test/v1/chat/completions").respond(
        400, json={"error": {"message": "Failed to call a function", "code": "tool_use_failed"}})
    with pytest.raises(ToolCallFormatError):
        await OpenAICompatibleLLM(LLM_SETTINGS).chat([{"role": "user", "content": "hi"}])


def test_llm_requires_configuration():
    with pytest.raises(LLMError, match="not configured"):
        OpenAICompatibleLLM(Settings(llm_model=""))


# ── Pulse tracing ─────────────────────────────────────────────────────────────

@respx.mock
async def test_agent_runs_are_traced_to_pulse():
    create = respx.post("http://pulse.test/api/v1/ingest/trace").respond(201, json={"trace_id": "tr-1"})
    spans = respx.post("http://pulse.test/api/v1/ingest/spans").respond(json={"accepted": 4, "rejected": 0})
    finish = respx.patch("http://pulse.test/api/v1/ingest/trace/tr-1/finish").respond(json={"trace_id": "tr-1"})
    tracer = Tracer(Settings(pulse_url="http://pulse.test", pulse_api_key="ps_key"))
    llm = ScriptedLLM([tool_call("get_platform_status", {}), final(GOOD_DIAGNOSIS)])
    await Agent(llm, tools=fake_tools([]), tracer=tracer).investigate("ctx")
    assert create.called and spans.called and finish.called
    assert create.calls[0].request.headers["X-API-Key"] == "ps_key"
    body = json.loads(spans.calls[0].request.content)
    types = [s["span_type"] for s in body["spans"]]
    assert types[0] == "agent" and types.count("llm") == 2 and types.count("tool") == 1
    root_id = body["spans"][0]["id"]
    assert all(s["parent_span_id"] == root_id for s in body["spans"][1:])
    tool_span = next(s for s in body["spans"] if s["span_type"] == "tool")
    assert tool_span["tool_call"]["tool_name"] == "get_platform_status"
    assert json.loads(finish.calls[0].request.content)["status"] == "success"


@respx.mock
async def test_tracing_is_a_noop_without_pulse_url():
    respx.route(host="pulse.test").respond(500)
    tracer = Tracer(Settings(pulse_url=""))
    llm = ScriptedLLM([final(GOOD_DIAGNOSIS)])
    result = await Agent(llm, tools=fake_tools([]), tracer=tracer).investigate("ctx")
    assert result.diagnosis.root_cause is RootCause.missing_upstream_data
    assert not respx.calls


@respx.mock
async def test_pulse_outage_does_not_break_the_agent():
    respx.route(host="pulse.test").mock(side_effect=httpx.ConnectError("down"))
    tracer = Tracer(Settings(pulse_url="http://pulse.test", pulse_api_key="ps_key"))
    result = await Agent(ScriptedLLM([final(GOOD_DIAGNOSIS)]), tools=fake_tools([]), tracer=tracer).investigate("ctx")
    assert result.diagnosis.confidence == 0.9
