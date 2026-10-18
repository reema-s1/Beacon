import httpx
import pytest
import respx
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY

from beacon.api.main import create_app
from beacon.api.service import IncidentService
from beacon.store import Incident, session
from tests.conftest import FakeNotifier

AF = "http://airflow.test/api/v1"


@pytest.fixture
def client():
    svc = IncidentService(lambda: (_ for _ in ()).throw(RuntimeError("no agent in this test")), notifier=FakeNotifier())
    with respx.mock(assert_all_called=False) as mock:
        mock.route(host="testserver").pass_through()
        mock.get(f"{AF}/dags").respond(json={"dags": [{"dag_id": "sales_daily_etl"}]})
        with TestClient(create_app(service=svc, warm_retriever=False)) as c:
            c.respx = mock
            yield c


def _incident(**kw) -> int:
    with session() as db:
        inc = Incident(fingerprint="f", alert_name="DagRunFailed", dag_id="sales_daily_etl",
                       run_id="manual__1", task_id="extract", status="triaged", **kw)
        db.add(inc)
        db.commit()
        return inc.id


def test_healthz_and_metrics(client):
    assert client.get("/healthz").json() == {"ok": True}
    body = client.get("/metrics").text
    assert "beacon_open_incidents" in body and "beacon_triage_seconds" in body


def test_ask_other_gets_polite_redirect(client):
    out = client.post("/ask", json={"question": "can you book me a meeting room?"}).json()
    assert out["category"] == "other" and "on-call" in out["message"]


def test_ask_how_to_searches_runbooks_only(client, monkeypatch):
    async def fake_search(query, k=3):
        return {"results": [{"runbook_id": "rb-disk-full", "title": "Disk full", "score": 0.7}]}
    from beacon import tools
    monkeypatch.setitem(tools.READ_ONLY_TOOLS, "search_runbooks",
                        tools.Tool("search_runbooks", "s", tools.SearchRunbooksArgs, fake_search))
    out = client.post("/ask", json={"question": "how do I free up space on the reports volume?"}).json()
    assert out["category"] == "how_to" and out["runbooks"][0]["runbook_id"] == "rb-disk-full"


def test_ask_job_failure_without_llm_is_a_clear_503(client):
    resp = client.post("/ask", json={"question": "why did sales_daily_etl fail?"})
    assert resp.status_code == 503 and "not configured" in resp.json()["detail"]


def test_rerun_enforces_allowlist(client):
    iid = _incident()
    clear = client.respx.post(f"{AF}/dags/sales_daily_etl/clearTaskInstances").respond(
        json={"task_instances": [{"task_id": "extract"}]})
    denied = client.post("/actions/rerun", json={"incident_id": iid, "user_id": "USTRANGER"})
    assert denied.status_code == 403 and not clear.called
    ok = client.post("/actions/rerun", json={"incident_id": iid, "user_id": "UALLOWED"})
    assert ok.status_code == 200 and ok.json()["cleared_tasks"] == ["extract"]


def test_rerun_reports_airflow_failure(client):
    iid = _incident()
    client.respx.post(f"{AF}/dags/sales_daily_etl/clearTaskInstances").mock(side_effect=httpx.ConnectError("x"))
    resp = client.post("/actions/rerun", json={"incident_id": iid, "user_id": "UALLOWED"})
    assert resp.status_code == 502


def test_incidents_feedback_and_ack(client):
    iid = _incident(root_cause="code_error")
    assert client.get("/incidents").json()[0]["id"] == iid
    assert client.get(f"/incidents/{iid}").json()["root_cause"] == "code_error"
    assert client.get("/incidents/999").status_code == 404
    before = REGISTRY.get_sample_value("beacon_feedback_total", {"value": "helpful"}) or 0
    assert client.post(f"/incidents/{iid}/feedback", json={"value": "helpful"}).status_code == 200
    assert REGISTRY.get_sample_value("beacon_feedback_total", {"value": "helpful"}) == before + 1
    assert client.post(f"/incidents/{iid}/feedback", json={"value": "meh"}).status_code == 422
    assert client.post(f"/incidents/{iid}/ack", json={"user_id": "U5"}).json()["acknowledged_by"] == "U5"


def test_eval_metrics_set_gauges(client):
    client.post("/eval/metrics", json={"root_cause_accuracy": {"agent": 0.9, "baseline": 0.7},
                                       "runbook_accuracy": {"agent": 0.85, "baseline": 0.7}})
    assert REGISTRY.get_sample_value("beacon_eval_root_cause_accuracy", {"system": "agent"}) == 0.9
    assert REGISTRY.get_sample_value("beacon_eval_runbook_accuracy", {"system": "baseline"}) == 0.7


def test_webhook_endpoint_creates_incident(client):
    client.respx.get(f"{AF}/dags/sales_daily_etl/dagRuns").respond(json={"dag_runs": []})
    resp = client.post("/webhooks/alertmanager", json={"alerts": [{
        "status": "firing", "fingerprint": "abc", "labels": {"alertname": "DagRunFailed", "dag_id": "sales_daily_etl"},
        "annotations": {}, "startsAt": "2026-10-22T10:00:00Z"}]})
    assert resp.status_code == 200 and len(resp.json()["created"]) == 1
