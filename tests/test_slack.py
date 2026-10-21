import json
from unittest.mock import MagicMock

import httpx
import respx

from beacon_slack.api_client import BeaconAPI, BeaconAPIError
from beacon_slack.handlers import Handlers, register
from tests.conftest import GOOD_DIAGNOSIS

INCIDENT = {"id": 7, "alert_name": "DagRunFailed", "dag_id": "sales_daily_etl", "run_id": "manual__1",
            "task_id": "extract", "status": "triaged", "summary": "Input missing.", "root_cause": "missing_upstream_data",
            "confidence": 0.9, "evidence": ["FileNotFoundError: sales.csv"], "runbook_id": "rb-missing-input",
            "user_impact": "Sales data is stale.", "suggested_action": "Redeliver the file."}
RUNBOOK = {"runbook_id": "rb-missing-input", "title": "Expected input file is missing",
           "root_cause": "missing_upstream_data", "body": "## Fix\n1. Redeliver the file.\n2. Rerun extract.\n"}


def setup(allowlist=("UALLOWED",)):
    api = MagicMock(spec=BeaconAPI)
    client = MagicMock()
    client.chat_postMessage.return_value = {"ts": "111.222", "channel": "C1"}
    return Handlers(api, set(allowlist)), api, client


def action_body(action_id, user="UALLOWED", value="7"):
    return {"user": {"id": user}, "trigger_id": "trig", "channel": {"id": "C1"}, "message": {"ts": "999.000"},
            "actions": [{"action_id": action_id, "value": value}]}


# ── /platform-status ───────────────────────────────────────────────────────────────────

def test_status_posts_health_blocks():
    h, api, _ = setup()
    api.status.return_value = {"scheduler": {"status": "healthy", "seconds_since_heartbeat": 3},
                               "dags": [{"dag_id": "sales_daily_etl", "runs_24h": 10, "success_rate_24h": 0.9,
                                         "last_run_state": "success", "paused": False}],
                               "open_incidents": []}
    ack, respond = MagicMock(), MagicMock()
    h.status(ack=ack, respond=respond)
    ack.assert_called_once()
    text = json.dumps(respond.call_args.kwargs["blocks"])
    assert "sales_daily_etl" in text and "90%" in text and "healthy" in text


def test_status_when_api_or_airflow_is_down():
    h, api, _ = setup()
    api.status.side_effect = BeaconAPIError("Beacon's API can't be reached right now.")
    respond = MagicMock()
    h.status(ack=MagicMock(), respond=respond)
    assert "can't be reached" in respond.call_args.kwargs["text"]
    api.status.side_effect = None
    api.status.return_value = {"scheduler": {"status": "unknown", "error": "Airflow unreachable"}, "dags": []}
    h.status(ack=MagicMock(), respond=respond)
    assert "Airflow can't be reached" in respond.call_args.kwargs["text"]


# ── /why ──────────────────────────────────────────────────────────────────────

def test_why_without_args_shows_usage():
    h, api, client = setup()
    ack = MagicMock()
    h.why(ack=ack, command={"text": ""}, client=client, respond=MagicMock())
    assert "Usage" in ack.call_args.kwargs["text"]
    api.ask.assert_not_called()


def test_why_posts_looking_into_it_then_answers_in_thread():
    h, api, client = setup()
    api.ask.return_value = {"category": "job_failure", "diagnosis": GOOD_DIAGNOSIS}
    api.runbook.return_value = RUNBOOK
    h.why(ack=MagicMock(), command={"text": "sales_daily_etl manual__1", "user_id": "U1", "channel_id": "C1"},
          client=client, respond=MagicMock())
    first, second = client.chat_postMessage.call_args_list
    assert "Looking into" in first.kwargs["text"]
    assert second.kwargs["thread_ts"] == "111.222"
    assert "Redeliver the file" in json.dumps(second.kwargs["blocks"])
    api.ask.assert_called_once_with("Why did sales_daily_etl fail? (run manual__1)", "U1",
                                    dag_id="sales_daily_etl", run_id="manual__1")


def test_why_reports_llm_failure_clearly():
    h, api, client = setup()
    api.ask.side_effect = BeaconAPIError("The AI model is unavailable right now. Try again shortly.")
    h.why(ack=MagicMock(), command={"text": "sales_daily_etl", "user_id": "U1", "channel_id": "C1"},
          client=client, respond=MagicMock())
    assert "unavailable" in client.chat_postMessage.call_args.kwargs["text"]


def test_why_falls_back_to_response_url_when_bot_not_in_channel():
    h, api, client = setup()
    client.chat_postMessage.side_effect = Exception("not_in_channel")
    api.ask.return_value = {"category": "job_failure", "diagnosis": dict(GOOD_DIAGNOSIS, runbook_id=None)}
    respond = MagicMock()
    h.why(ack=MagicMock(), command={"text": "sales_daily_etl", "user_id": "U1", "channel_id": "C1"},
          client=client, respond=respond)
    assert "Looking into" in respond.call_args_list[0].kwargs["text"]
    assert "blocks" in respond.call_args_list[1].kwargs


# ── /runbook ──────────────────────────────────────────────────────────────────

def test_runbook_search():
    h, api, _ = setup()
    api.runbook_search.return_value = {"results": [{"runbook_id": "rb-missing-input", "title": RUNBOOK["title"],
                                                    "score": 0.81}]}
    api.runbook.return_value = RUNBOOK
    respond = MagicMock()
    h.runbook(ack=MagicMock(), command={"text": "file not found"}, respond=respond)
    assert "Rerun extract" in json.dumps(respond.call_args.kwargs["blocks"])


# ── Buttons ───────────────────────────────────────────────────────────────────

def test_rerun_button_opens_confirmation_modal():
    h, api, client = setup()
    api.incident.return_value = INCIDENT
    h.open_rerun(ack=MagicMock(), body=action_body("rerun_task"), client=client)
    view = client.views_open.call_args.kwargs["view"]
    assert view["callback_id"] == "rerun_confirm"
    assert json.loads(view["private_metadata"]) == {"incident_id": 7, "channel": "C1", "ts": "999.000"}


def _submission(user):
    view = {"private_metadata": json.dumps({"incident_id": 7, "channel": "C1", "ts": "999.000"})}
    return {"user": {"id": user}}, view


def test_rerun_denied_for_user_not_on_allowlist():
    h, api, client = setup()
    body, view = _submission("USTRANGER")
    h.confirm_rerun(ack=MagicMock(), body=body, view=view, client=client)
    api.rerun.assert_not_called()
    assert "allowlist" in client.chat_postEphemeral.call_args.kwargs["text"]


def test_rerun_allowed_posts_result_in_thread():
    h, api, client = setup()
    api.rerun.return_value = {"task_id": "extract", "run_id": "manual__1", "cleared_tasks": ["extract", "clean"]}
    body, view = _submission("UALLOWED")
    h.confirm_rerun(ack=MagicMock(), body=body, view=view, client=client)
    api.rerun.assert_called_once_with(7, "UALLOWED")
    msg = client.chat_postMessage.call_args.kwargs
    assert msg["thread_ts"] == "999.000" and "reran `extract`" in msg["text"]


def test_rerun_api_refusal_is_reported():
    h, api, client = setup()
    api.rerun.side_effect = BeaconAPIError("user UALLOWED is not allowed to rerun tasks")
    body, view = _submission("UALLOWED")
    h.confirm_rerun(ack=MagicMock(), body=body, view=view, client=client)
    assert "failed" in client.chat_postMessage.call_args.kwargs["text"]


def test_feedback_acknowledge_and_evidence():
    h, api, client = setup()
    h.feedback("helpful")(ack=MagicMock(), body=action_body("feedback_helpful", "U9"), client=client)
    api.feedback.assert_called_once_with(7, "helpful", "U9")
    assert "Thanks" in client.chat_postEphemeral.call_args.kwargs["text"]

    h.acknowledge(ack=MagicMock(), body=action_body("acknowledge", "U9"), client=client)
    api.acknowledge.assert_called_once_with(7, "U9")
    assert "looking at this" in client.chat_postMessage.call_args.kwargs["text"]

    api.incident.return_value = INCIDENT
    h.show_evidence(ack=MagicMock(), body=action_body("show_evidence"), client=client)
    assert "FileNotFoundError" in json.dumps(client.views_open.call_args.kwargs["view"])


def test_register_wires_every_handler():
    app = MagicMock()
    register(app, setup()[0])
    commands = {c.args[0] for c in app.command.call_args_list}
    actions = {c.args[0] for c in app.action.call_args_list}
    assert commands == {"/platform-status", "/why", "/runbook"}
    assert actions == {"rerun_task", "acknowledge", "feedback_helpful", "feedback_not_helpful", "show_evidence"}
    app.view.assert_called_once_with("rerun_confirm")


# ── API client error mapping ──────────────────────────────────────────────────

@respx.mock
def test_api_client_turns_errors_into_readable_messages():
    api = BeaconAPI("http://beacon.test")
    respx.get("http://beacon.test/status").mock(side_effect=httpx.ConnectError("refused"))
    try:
        api.status()
        raise AssertionError("expected BeaconAPIError")
    except BeaconAPIError as exc:
        assert "can't be reached" in str(exc)
    respx.post("http://beacon.test/actions/rerun").respond(403, json={"detail": "user U1 is not allowed to rerun tasks"})
    try:
        api.rerun(7, "U1")
        raise AssertionError("expected BeaconAPIError")
    except BeaconAPIError as exc:
        assert "not allowed" in str(exc)


def test_real_notifier_builds_async_slack_client():
    from beacon.api.notifier import SlackNotifier
    from beacon.config import Settings
    n = SlackNotifier(Settings(slack_bot_token="xoxb-test", slack_incident_channel="C1"))
    assert n.enabled and n.client.__class__.__name__ == "AsyncWebClient"
