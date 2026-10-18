import json

import respx
from sqlalchemy import select

from beacon.store import FaultInjection, session
from chaos import cli
from chaos.scenarios import SCENARIOS


class FakePlatform:
    def __init__(self):
        self.calls = []

    def __getattr__(self, name):
        def record(*args, **kw):
            self.calls.append((name, args))
            return "manual__x" if name == "trigger_dag" else ""
        return record


def test_every_scenario_has_ground_truth():
    assert len(SCENARIOS) == 8
    for s in SCENARIOS.values():
        assert s.ground_truth_cause and s.expected_runbook.startswith("rb-")


def test_inject_records_ground_truth_and_triggers_target_dag():
    p = FakePlatform()
    injection, run_id = cli.inject(p, "bad_code")
    assert run_id == "manual__x"
    assert ("set_variable", ("fault_bad_code", "true")) in p.calls
    assert ("trigger_dag", ("sales_daily_etl",)) in p.calls
    with session() as db:
        row = db.scalars(select(FaultInjection)).one()
    assert (row.scenario, row.ground_truth_cause, row.expected_runbook) == ("bad_code", "code_error", "rb-code-error")


def test_scheduler_down_does_not_trigger_a_dag():
    p = FakePlatform()
    _, run_id = cli.inject(p, "scheduler_down")
    assert run_id is None and ("stop_scheduler", ()) in p.calls


def test_reset_undoes_everything_and_closes_injections():
    p = FakePlatform()
    cli.inject(p, "bad_connection", trigger=False)
    steps = cli.reset(p)
    names = [c[0] for c in p.calls]
    assert {"start_scheduler", "delete_variable", "delete_connection", "set_upstream_mode", "exec_scheduler"} <= set(names)
    assert ("set_upstream_mode", ("healthy",)) in p.calls
    assert steps[-1] == "1 open injection(s) marked reset"
    with session() as db:
        assert db.scalars(select(FaultInjection)).one().reset_at is not None


def test_chaos_list_runs_without_infrastructure(capsys):
    assert cli.main(["list"]) == 0
    out = capsys.readouterr().out
    assert "disk_full" in out and "storage_full" in out


# ── MCP server ────────────────────────────────────────────────────────────────

async def test_mcp_exposes_read_only_tools_and_runbook_resources():
    from beacon_mcp.server import build_server
    mcp = build_server()
    names = {t.name for t in await mcp.list_tools()}
    assert names == {"get_platform_status", "list_recent_failures", "get_dag_run", "get_task_logs",
                     "query_prometheus", "search_runbooks", "get_runbook"}
    assert "rerun_task" not in names
    resources = {str(r.uri) for r in await mcp.list_resources()}
    assert len(resources) == 12 and "runbook://rb-disk-full" in resources
    content = await mcp.read_resource("runbook://rb-disk-full")
    assert "No space left on device" in content[0].content


@respx.mock
async def test_mcp_tool_call_goes_through_shared_tools():
    from beacon_mcp.server import build_server
    respx.get("http://airflow.test/api/v1/dags/sales_daily_etl/dagRuns/r1").respond(
        json={"state": "failed", "run_type": "manual", "start_date": None, "end_date": None})
    respx.get("http://airflow.test/api/v1/dags/sales_daily_etl/dagRuns/r1/taskInstances").respond(
        json={"task_instances": [{"task_id": "extract", "state": "failed", "try_number": 1}]})
    mcp = build_server()
    result = await mcp.call_tool("get_dag_run", {"dag_id": "sales_daily_etl", "run_id": "r1"})
    payload = json.loads(result[0].text)
    assert payload["state"] == "failed" and payload["tasks"][0]["task_id"] == "extract"
    rejected = await mcp.call_tool("query_prometheus", {"promql": '{__name__=~".+"}'})
    assert "query rejected" in rejected[0].text
