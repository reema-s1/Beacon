"""Slack command and button handlers.

Plain functions (no Bolt globals) so they can be unit-tested with mocks. Every path
answers the user, including when beacon-api, Airflow or the LLM is down.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Callable

from beacon.retrieval.runbooks import Runbook
from beacon.slack_blocks import (
    diagnosis_blocks, evidence_modal, rerun_modal, runbook_search_blocks, status_blocks,
)
from beacon_slack.api_client import BeaconAPI, BeaconAPIError

log = logging.getLogger("beacon.slack")

WHY_USAGE = "Usage: `/why <dag_id> [run_id]`, e.g. `/why sales_daily_etl`"
RUNBOOK_USAGE = "Usage: `/runbook <what you are seeing>`, e.g. `/runbook no space left on device`"


def _warn(text: str) -> str:
    return f":warning: {text}"


def _runbook_obj(data: dict) -> Runbook:
    return Runbook(id=data["runbook_id"], title=data["title"], root_cause=data.get("root_cause", ""),
                   symptoms=(), body=data.get("body", ""))


class Handlers:
    def __init__(self, api: BeaconAPI, allowlist: set[str]):
        self.api = api
        self.allowlist = allowlist

    # ── Commands ──────────────────────────────────────────────────────────────

    def status(self, ack: Callable, respond: Callable, **_: Any) -> None:
        ack()
        try:
            status = self.api.status()
        except BeaconAPIError as exc:
            respond(text=_warn(str(exc)))
            return
        if status.get("scheduler", {}).get("error"):
            respond(text=_warn("Airflow can't be reached right now, so I can't check pipeline health. "
                               "Please try again shortly or ask in #data-platform-oncall."))
            return
        respond(response_type="in_channel", text="Platform health", blocks=status_blocks(status))

    def why(self, ack: Callable, command: dict, client: Any, respond: Callable, **_: Any) -> None:
        parts = (command.get("text") or "").split()
        if not parts:
            ack(text=WHY_USAGE)
            return
        ack()
        dag_id, run_id = parts[0], (parts[1] if len(parts) > 1 else None)
        user = command.get("user_id")
        channel = command.get("channel_id")
        thread_ts = None
        target = f"`{dag_id}`" + (f" run `{run_id}`" if run_id else "")
        try:
            msg = client.chat_postMessage(channel=channel, text=f"🔎 Looking into {target} for <@{user}>…")
            thread_ts = msg["ts"]
        except Exception as exc:  # e.g. bot not in channel: fall back to the response_url
            log.info("chat_postMessage failed (%s); replying via response_url", exc)
            respond(text=f"🔎 Looking into {target}…")

        def reply(**kw):
            if thread_ts:
                client.chat_postMessage(channel=channel, thread_ts=thread_ts, **kw)
            else:
                respond(**kw)

        question = f"Why did {dag_id} fail?" + (f" (run {run_id})" if run_id else "")
        try:
            result = self.api.ask(question, user, dag_id=dag_id, run_id=run_id)
        except BeaconAPIError as exc:
            reply(text=_warn(str(exc)))
            return
        diag = result.get("diagnosis")
        if not diag:
            reply(text=result.get("message") or "I couldn't investigate that.")
            return
        runbook = None
        if diag.get("runbook_id"):
            try:
                runbook = _runbook_obj(self.api.runbook(diag["runbook_id"]))
            except BeaconAPIError:
                runbook = None
        reply(text=diag.get("summary", "Diagnosis"), blocks=diagnosis_blocks(f"Why {dag_id} failed", diag, runbook))

    def runbook(self, ack: Callable, command: dict, respond: Callable, **_: Any) -> None:
        query = (command.get("text") or "").strip()
        if len(query) < 3:
            ack(text=RUNBOOK_USAGE)
            return
        ack()
        try:
            found = self.api.runbook_search(query)
            if "error" in found:
                raise BeaconAPIError(found["error"])
            results = found.get("results", [])
            books = {r["runbook_id"]: _runbook_obj(self.api.runbook(r["runbook_id"])) for r in results}
        except BeaconAPIError as exc:
            respond(text=_warn(str(exc)))
            return
        respond(text=f"Runbooks for {query}", blocks=runbook_search_blocks(query, results, books))

    # ── Buttons ───────────────────────────────────────────────────────────────

    @staticmethod
    def _incident_id(body: dict) -> int:
        return int(body["actions"][0]["value"])

    def _ephemeral(self, client: Any, body: dict, text: str) -> None:
        channel = (body.get("channel") or {}).get("id")
        user = body["user"]["id"]
        if channel:
            client.chat_postEphemeral(channel=channel, user=user, text=text)

    def open_rerun(self, ack: Callable, body: dict, client: Any, **_: Any) -> None:
        ack()
        try:
            inc = self.api.incident(self._incident_id(body))
        except BeaconAPIError as exc:
            self._ephemeral(client, body, _warn(str(exc)))
            return
        client.views_open(trigger_id=body["trigger_id"],
                          view=rerun_modal(inc, (body.get("channel") or {}).get("id"),
                                           (body.get("message") or {}).get("ts")))

    def confirm_rerun(self, ack: Callable, body: dict, view: dict, client: Any, **_: Any) -> None:
        ack()
        meta = json.loads(view.get("private_metadata") or "{}")
        user = body["user"]["id"]
        channel, ts = meta.get("channel"), meta.get("ts")

        def post(text: str) -> None:
            if channel and ts:
                client.chat_postMessage(channel=channel, thread_ts=ts, text=text)

        if user not in self.allowlist:
            if channel:
                client.chat_postEphemeral(channel=channel, user=user,
                                          text=_warn("You're not on the rerun allowlist. Ask the on-call engineer."))
            return
        try:
            result = self.api.rerun(int(meta["incident_id"]), user)
        except BeaconAPIError as exc:
            post(_warn(f"Rerun requested by <@{user}> failed: {exc}"))
            return
        cleared = ", ".join(f"`{t}`" for t in result.get("cleared_tasks", [])) or "nothing"
        post(f"🔁 <@{user}> reran `{result['task_id']}` in run `{result['run_id']}`. Cleared: {cleared}. "
             "Airflow will pick it up within a few seconds.")

    def acknowledge(self, ack: Callable, body: dict, client: Any, **_: Any) -> None:
        ack()
        user = body["user"]["id"]
        try:
            self.api.acknowledge(self._incident_id(body), user)
        except BeaconAPIError as exc:
            self._ephemeral(client, body, _warn(str(exc)))
            return
        channel = (body.get("channel") or {}).get("id")
        ts = (body.get("message") or {}).get("ts")
        if channel and ts:
            client.chat_postMessage(channel=channel, thread_ts=ts, text=f"👀 <@{user}> is looking at this.")

    def feedback(self, value: str) -> Callable:
        def handler(ack: Callable, body: dict, client: Any, **_: Any) -> None:
            ack()
            try:
                self.api.feedback(self._incident_id(body), value, body["user"]["id"])
            except BeaconAPIError as exc:
                self._ephemeral(client, body, _warn(str(exc)))
                return
            self._ephemeral(client, body, "Thanks! Your feedback helps tune Beacon." if value == "helpful"
                            else "Thanks for telling us. We'll review this diagnosis.")
        return handler

    def show_evidence(self, ack: Callable, body: dict, client: Any, **_: Any) -> None:
        ack()
        try:
            inc = self.api.incident(self._incident_id(body))
        except BeaconAPIError as exc:
            self._ephemeral(client, body, _warn(str(exc)))
            return
        client.views_open(trigger_id=body["trigger_id"], view=evidence_modal(inc))


def register(app: Any, handlers: Handlers) -> None:
    app.command("/status")(handlers.status)
    app.command("/why")(handlers.why)
    app.command("/runbook")(handlers.runbook)
    app.action("rerun_task")(handlers.open_rerun)
    app.view("rerun_confirm")(handlers.confirm_rerun)
    app.action("acknowledge")(handlers.acknowledge)
    app.action("feedback_helpful")(handlers.feedback("helpful"))
    app.action("feedback_not_helpful")(handlers.feedback("not_helpful"))
    app.action("show_evidence")(handlers.show_evidence)
