from datetime import datetime, timedelta, timezone

import httpx
import respx
from sqlalchemy import select

from beacon.agent.loop import Agent
from beacon.api.service import IncidentService
from beacon.store import FaultInjection, Incident, session
from tests.conftest import GOOD_DIAGNOSIS, FakeNotifier, ScriptedLLM, final, tool_call

AF = "http://airflow.test/api/v1"


def alert(name="DagRunFailed", status="firing", fp="fp-1", dag_id="sales_daily_etl", **labels):
    lab = {"alertname": name, "severity": "page", **labels}
    if dag_id:
        lab["dag_id"] = dag_id
    return {"status": status, "labels": lab, "annotations": {"summary": f"{name} test"},
            "startsAt": "2026-10-22T10:01:00Z", "endsAt": "0001-01-01T00:00:00Z", "fingerprint": fp}


def payload(*alerts):
    return {"version": "4", "status": alerts[0]["status"], "receiver": "beacon", "alerts": list(alerts)}


def mock_airflow(run_id="scheduled__2026-10-22T10:00:00+00:00", end="2026-10-22T10:00:50+00:00"):
    respx.get(f"{AF}/dags/sales_daily_etl/dagRuns").respond(json={"total_entries": 1, "dag_runs": [
        {"dag_id": "sales_daily_etl", "dag_run_id": run_id, "state": "failed",
         "start_date": "2026-10-22T10:00:00+00:00", "end_date": end}]})
    respx.get(url__regex=rf"{AF}/dags/sales_daily_etl/dagRuns/.+/taskInstances").respond(json={"task_instances": [
        {"task_id": "extract", "state": "failed"}, {"task_id": "clean", "state": "upstream_failed"}]})


def make_service(llm_replies_per_run=None):
    notifier = FakeNotifier()
    llms = []

    def factory():
        llm = ScriptedLLM(list(llm_replies_per_run or [final(GOOD_DIAGNOSIS)]))
        llms.append(llm)
        return Agent(llm, tools={})
    return IncidentService(factory, notifier=notifier), notifier, llms


@respx.mock
async def test_firing_alert_creates_incident_and_posts_diagnosis():
    mock_airflow()
    with session() as db:
        db.add(FaultInjection(scenario="missing_input", ground_truth_cause="missing_upstream_data",
                              expected_runbook="rb-missing-input",
                              injected_at=datetime(2026, 10, 22, 9, 59, tzinfo=timezone.utc)))
        db.commit()
    svc, notifier, llms = make_service()
    out = await svc.handle_alertmanager(payload(alert()))
    await svc.wait_idle()
    assert len(out["created"]) == 1
    with session() as db:
        inc = db.get(Incident, out["created"][0])
    assert inc.status == "triaged" and inc.root_cause == "missing_upstream_data"
    assert inc.run_id == "scheduled__2026-10-22T10:00:00+00:00" and inc.task_id == "extract"
    assert inc.fault_injection_id == 1
    assert inc.slack_ts and inc.triaged_at >= inc.created_at
    assert notifier.posted[0]["summary"].startswith("The sales pipeline failed")
    # The agent was told which run and task failed.
    context = llms[0].calls[0]["messages"][1]["content"]
    assert "run_id=scheduled__2026-10-22T10:00:00+00:00" in context and "extract" in context


@respx.mock
async def test_repeat_notification_for_same_run_is_deduplicated():
    mock_airflow()
    svc, notifier, _ = make_service()
    first = await svc.handle_alertmanager(payload(alert()))
    again = await svc.handle_alertmanager(payload(alert()))
    other_rule = await svc.handle_alertmanager(payload(alert("TaskFailureSpike", fp="fp-2", task_id="extract")))
    await svc.wait_idle()
    assert len(first["created"]) == 1
    assert again == {"created": [], "deduplicated": 1, "resolved": []}
    assert other_rule["deduplicated"] == 1
    assert len(notifier.posted) == 1


@respx.mock
async def test_new_failed_run_under_same_alert_creates_new_incident():
    svc, notifier, _ = make_service()
    mock_airflow(run_id="scheduled__run-1")
    await svc.handle_alertmanager(payload(alert()))
    respx.reset()
    mock_airflow(run_id="scheduled__run-2")
    out = await svc.handle_alertmanager(payload(alert()))
    await svc.wait_idle()
    assert len(out["created"]) == 1
    with session() as db:
        assert sorted(i.run_id for i in db.scalars(select(Incident))) == ["scheduled__run-1", "scheduled__run-2"]


@respx.mock
async def test_resolved_notification_resolves_and_updates_thread():
    mock_airflow()
    svc, notifier, _ = make_service()
    created = (await svc.handle_alertmanager(payload(alert())))["created"]
    await svc.wait_idle()
    out = await svc.handle_alertmanager(payload(alert(status="resolved")))
    assert out["resolved"] == created
    with session() as db:
        inc = db.get(Incident, created[0])
        assert inc.status == "resolved" and inc.resolved_at is not None
    assert notifier.updated[0]["status"] == "resolved"
    assert "resolved" in notifier.threads[0][1]


@respx.mock
async def test_scheduler_alert_without_dag_dedupes_while_open():
    svc, notifier, _ = make_service()
    a = alert("SchedulerHeartbeatMissing", fp="sched", dag_id=None)
    first = await svc.handle_alertmanager(payload(a))
    second = await svc.handle_alertmanager(payload(a))
    await svc.wait_idle()
    assert len(first["created"]) == 1 and second["deduplicated"] == 1
    await svc.handle_alertmanager(payload(alert("SchedulerHeartbeatMissing", status="resolved", fp="sched", dag_id=None)))
    third = await svc.handle_alertmanager(payload(a))
    await svc.wait_idle()
    assert len(third["created"]) == 1  # a new outage after resolution is a new incident
    assert not respx.calls  # no Airflow lookups for run-less alerts


@respx.mock
async def test_airflow_unreachable_still_creates_incident():
    respx.get(url__startswith=AF).mock(side_effect=httpx.ConnectError("refused"))
    svc, notifier, _ = make_service()
    out = await svc.handle_alertmanager(payload(alert()))
    await svc.wait_idle()
    with session() as db:
        inc = db.get(Incident, out["created"][0])
    assert inc.run_id is None and inc.status == "triaged"


@respx.mock
async def test_agent_failure_produces_fallback_post_not_silence():
    mock_airflow()

    def broken_factory():
        raise RuntimeError("LLM is not configured")
    notifier = FakeNotifier()
    svc = IncidentService(broken_factory, notifier=notifier)
    out = await svc.handle_alertmanager(payload(alert()))
    await svc.wait_idle()
    with session() as db:
        inc = db.get(Incident, out["created"][0])
    assert inc.root_cause == "unknown" and "LLM is not configured" in inc.error
    assert len(notifier.posted) == 1


@respx.mock
async def test_injection_linking_ignores_failures_before_injection():
    mock_airflow(end="2026-10-22T10:00:50+00:00")
    with session() as db:
        db.add(FaultInjection(scenario="bad_code", ground_truth_cause="code_error", expected_runbook="rb-code-error",
                              injected_at=datetime(2026, 10, 22, 10, 5, tzinfo=timezone.utc)))
        db.commit()
    svc, _, _ = make_service()
    out = await svc.handle_alertmanager(payload(alert()))
    await svc.wait_idle()
    with session() as db:
        assert db.get(Incident, out["created"][0]).fault_injection_id is None


@respx.mock
async def test_agent_tool_trail_is_stored():
    mock_airflow()
    svc, _, _ = make_service([tool_call("get_platform_status", {}), final(GOOD_DIAGNOSIS)])
    out = await svc.handle_alertmanager(payload(alert()))
    await svc.wait_idle()
    with session() as db:
        inc = db.get(Incident, out["created"][0])
    assert inc.agent_steps == 2
    assert inc.investigation[0]["tool"] == "get_platform_status"  # unknown to the empty tool set
    assert "unknown tool" in inc.investigation[0]["error"]


def test_triage_seconds_measured_from_alert_receipt():
    created = datetime.now(timezone.utc) - timedelta(seconds=30)
    with session() as db:
        db.add(Incident(fingerprint="f", alert_name="DagRunFailed", created_at=created,
                        triaged_at=created + timedelta(seconds=12)))
        db.commit()
        inc = db.scalars(select(Incident)).one()
    assert (inc.triaged_at - inc.created_at).total_seconds() == 12
