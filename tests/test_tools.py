import httpx
import pytest
import respx

from beacon import tools
from beacon.tools import PromQLRejected, validate_promql

AF = "http://airflow.test/api/v1"
PROM = "http://prom.test/api/v1"
RUN = "manual__2026-10-22T10:00:00+00:00"
RUN_Q = "manual__2026-10-22T10%3A00%3A00%2B00%3A00"

SAMPLE_LOG = """[2026-10-22T10:00:01.000+0000] {local_task_job_runner.py:123} INFO - ::group::Pre task execution logs
[2026-10-22T10:00:01.100+0000] {taskinstance.py:2613} INFO - Dependencies all met for dep_context=non-requeueable
[2026-10-22T10:00:01.200+0000] {taskinstance.py:2866} INFO - Starting attempt 2 of 2
[2026-10-22T10:00:02.000+0000] {sales_daily_etl.py:30} INFO - Reading upstream sales drop from /opt/airflow/data/incoming/sales.csv
[2026-10-22T10:00:02.100+0000] {sales_daily_etl.py:33} INFO - connecting with postgresql://warehouse:s3cr3tpw@postgres:5432/warehouse password=hunter2
[2026-10-22T10:00:02.200+0000] {taskinstance.py:3313} ERROR - Task failed with exception
Traceback (most recent call last):
FileNotFoundError: Expected input file not found: /opt/airflow/data/incoming/sales.csv
"""


@respx.mock
async def test_get_task_logs_defaults_to_latest_try_filters_noise_and_masks_secrets():
    respx.get(f"{AF}/dags/sales_daily_etl/dagRuns/{RUN_Q}/taskInstances/extract").respond(
        json={"task_id": "extract", "try_number": 2, "state": "failed"})
    logs = respx.get(f"{AF}/dags/sales_daily_etl/dagRuns/{RUN_Q}/taskInstances/extract/logs/2").respond(
        text=SAMPLE_LOG)
    out = await tools.get_task_logs("sales_daily_etl", RUN, "extract", tail_lines=10)
    assert logs.called
    assert out["try_number"] == 2
    text = "\n".join(out["log"])
    assert "Dependencies all met" not in text and "::group::" not in text
    assert "FileNotFoundError: Expected input file not found" in text
    assert "s3cr3tpw" not in text and "hunter2" not in text
    assert "postgresql://warehouse:***@postgres" in text


def test_clean_log_hides_library_frames_but_keeps_pipeline_frame_and_exception():
    log = """[2026-10-22T14:39:55.000+0000] {taskinstance.py:3313} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.11/site-packages/airflow/models/taskinstance.py", line 768, in _execute_task
    result = _execute_callable(context=context, **execute_callable_kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.11/site-packages/airflow/operators/python.py", line 238, in execute
    return_value = self.execute_callable()
  File "/opt/airflow/dags/sales_daily_etl.py", line 48, in clean
    price = float(row[price_field])
                  ~~~^^^^^^^^^^^^^
KeyError: 'unit_price_usd'
d69a9627bbde
***   * /opt/airflow/logs/dag_id=x/attempt=2.log"""
    lines, _ = tools.clean_log(log, 40)
    assert lines == [
        "14:39:55 ERROR - Task failed with exception",
        "Traceback (most recent call last):",
        "  [... 2 library frame(s) hidden ...]",
        '  File "/opt/airflow/dags/sales_daily_etl.py", line 48, in clean',
        "    price = float(row[price_field])",
        "KeyError: 'unit_price_usd'",
    ]


def test_clean_log_keeps_relevant_tail():
    lines, truncated = tools.clean_log("\n".join(f"line {i}" for i in range(100)), 10)
    assert truncated and lines == [f"line {i}" for i in range(90, 100)]


@respx.mock
async def test_list_recent_failures_includes_failed_tasks():
    respx.get(f"{AF}/dags/~/dagRuns").respond(json={"total_entries": 1, "dag_runs": [
        {"dag_id": "sales_daily_etl", "dag_run_id": RUN, "run_type": "manual",
         "start_date": "2026-10-22T10:00:00+00:00", "end_date": "2026-10-22T10:00:40+00:00"}]})
    respx.get(f"{AF}/dags/sales_daily_etl/dagRuns/{RUN_Q}/taskInstances").respond(json={"task_instances": [
        {"task_id": "extract", "state": "failed", "try_number": 2, "duration": 1.2},
        {"task_id": "clean", "state": "upstream_failed", "try_number": 0},
        {"task_id": "load", "state": "upstream_failed", "try_number": 0}]})
    out = await tools.list_recent_failures(since_minutes=30)
    run = out["failed_runs"][0]
    assert run["failed_tasks"][0]["task_id"] == "extract"
    assert run["upstream_failed"] == ["clean", "load"]


@respx.mock
async def test_get_platform_status_computes_success_rates():
    respx.get(f"{AF}/health").respond(json={"metadatabase": {"status": "healthy"}, "scheduler": {
        "status": "unhealthy", "latest_scheduler_heartbeat": "2026-10-22T10:00:00+00:00"}})
    respx.get(f"{AF}/dags").respond(json={"dags": [{"dag_id": "sales_daily_etl", "is_paused": False}]})

    def runs(request):
        state = request.url.params.get("state")
        total = {"success": 9, "failed": 1}.get(state, 10)
        return httpx.Response(200, json={"total_entries": total, "dag_runs": [
            {"dag_run_id": "r1", "state": "failed"}]})
    respx.get(f"{AF}/dags/sales_daily_etl/dagRuns").mock(side_effect=runs)
    out = await tools.get_platform_status()
    assert out["scheduler"]["status"] == "unhealthy"
    dag = out["dags"][0]
    assert dag["runs_24h"] == 10 and dag["success_rate_24h"] == 0.9 and dag["last_run_state"] == "failed"
    assert out["open_incidents"] == []


@respx.mock
async def test_platform_status_when_airflow_is_down():
    respx.get(f"{AF}/health").mock(side_effect=httpx.ConnectError("refused"))
    out = await tools.get_platform_status()
    assert out["scheduler"]["status"] == "unknown"
    assert "Airflow unreachable" in out["scheduler"]["error"]


@respx.mock
async def test_query_prometheus_caps_series_and_points():
    route = respx.get(f"{PROM}/query").respond(json={"status": "success", "data": {
        "resultType": "vector",
        "result": [{"metric": {"dag_id": f"d{i}"}, "value": [1, str(i)]} for i in range(100)]}})
    out = await tools.query_prometheus('sum by (dag_id) (airflow_ti_finish{state="failed"})')
    assert route.called and out["truncated"] is True and len(out["series"]) == 30

    respx.get(f"{PROM}/query_range").respond(json={"status": "success", "data": {
        "resultType": "matrix", "result": [{"metric": {}, "values": [[1700000000 + i, "1"] for i in range(500)]}]}})
    out = await tools.query_prometheus("airflow_scheduler_heartbeat", minutes=60)
    assert len(out["series"][0]["values"]) == 60


@pytest.mark.parametrize("query,minutes", [
    ("up; drop", 0),
    ('{__name__=~".+"}', 0),
    ('{job="airflow"}', 0),
    ("rate(airflow_ti_failures[30d])", 0),
    ("airflow_ti_failures offset 2w", 0),
    ("airflow_ti_failures", 100000),
    ("http://evil.example/api/v1/admin/tsdb/delete_series", 0),
    ("up\nup", 0),
    ("up" + " + up" * 200, 0),
    ("airflow_ti_failures @ 1609746000", 0),
    ("max_over_time(airflow_ti_failures[1h:1s][30d:1m])", 0),
])
def test_promql_guard_rejects_unsafe_or_oversized(query, minutes):
    with pytest.raises(PromQLRejected):
        validate_promql(query, minutes, max_minutes=360)


@pytest.mark.parametrize("query", [
    'sum by (dag_id) (increase(airflow_ti_finish{state="failed"}[15m]))',
    "histogram_quantile(0.95, sum by (le) (rate(airflow_task_duration_bucket[10m])))",
    "changes(airflow_scheduler_heartbeat[1m]) == 0",
    'airflow_dagrun_duration_count{status=~"fail.*"} offset 5m',
])
def test_promql_guard_accepts_plain_queries(query):
    validate_promql(query, 60, max_minutes=360)


async def test_call_tool_returns_clear_error_for_bad_arguments():
    t = tools.READ_ONLY_TOOLS["get_task_logs"]
    out = await tools.call_tool(t, {"dag_id": "x", "run_id": "y"})
    assert "invalid arguments for get_task_logs" in out["error"] and "task_id" in out["error"]
    out = await tools.call_tool(t, {"dag_id": "x", "run_id": "y", "task_id": "z", "tail_lines": 10000})
    assert "tail_lines" in out["error"]
    out = await tools.call_tool(t, {"dag_id": "x", "run_id": "y", "task_id": "z", "rm_rf": True})
    assert "rm_rf" in out["error"]


async def test_call_tool_reports_rejected_promql_to_the_model():
    out = await tools.call_tool(tools.READ_ONLY_TOOLS["query_prometheus"], {"promql": '{__name__=~".+"}'})
    assert out["error"].startswith("query rejected")


@respx.mock
async def test_call_tool_reports_airflow_outage():
    respx.get(url__startswith=AF).mock(side_effect=httpx.ConnectError("refused"))
    out = await tools.call_tool(tools.READ_ONLY_TOOLS["get_dag_run"], {"dag_id": "a", "run_id": "b"})
    assert "Airflow unreachable" in out["error"]


def test_agent_never_gets_write_tools():
    assert "rerun_task" not in tools.AGENT_TOOLS
    assert "rerun_task" not in tools.READ_ONLY_TOOLS
    specs = [t.openai_spec()["function"]["name"] for t in tools.AGENT_TOOLS.values()]
    assert set(specs) == {"get_platform_status", "list_recent_failures", "get_dag_run", "get_task_logs",
                          "query_prometheus", "search_runbooks", "get_runbook"}


@respx.mock
async def test_rerun_requires_allowlisted_user():
    clear = respx.post(f"{AF}/dags/sales_daily_etl/clearTaskInstances").respond(
        json={"task_instances": [{"task_id": "extract"}, {"task_id": "clean"}]})
    with pytest.raises(tools.RerunNotAllowed):
        await tools.rerun_task("sales_daily_etl", RUN, "extract", "USTRANGER")
    assert not clear.called
    out = await tools.rerun_task("sales_daily_etl", RUN, "extract", "UALLOWED")
    assert clear.called and out["cleared_tasks"] == ["extract", "clean"]
