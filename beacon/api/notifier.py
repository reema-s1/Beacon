"""Posts incident summaries to Slack and keeps the thread up to date."""
from __future__ import annotations

import logging
from typing import Any

from beacon.config import Settings, get_settings
from beacon.slack_blocks import incident_blocks, incident_fallback_text

log = logging.getLogger("beacon.notifier")


class SlackNotifier:
    def __init__(self, settings: Settings | None = None, client: Any | None = None):
        s = settings or get_settings()
        self.channel = s.slack_incident_channel
        self.enabled = bool((s.slack_bot_token or client) and self.channel)
        if client is not None:
            self.client = client
        elif self.enabled:
            from slack_sdk.web.async_client import AsyncWebClient
            self.client = AsyncWebClient(token=s.slack_bot_token)
        else:
            self.client = None

    async def post_incident(self, inc: dict, runbook: Any | None) -> tuple[str | None, str | None]:
        if not self.enabled:
            return None, None
        try:
            resp = await self.client.chat_postMessage(channel=self.channel, text=incident_fallback_text(inc),
                                                      blocks=incident_blocks(inc, runbook), unfurl_links=False)
            return resp["channel"], resp["ts"]
        except Exception as exc:
            log.error("Slack post failed for incident %s: %s", inc.get("id"), exc)
            return None, None

    async def update_incident(self, inc: dict, runbook: Any | None) -> None:
        if not (self.enabled and inc.get("slack_ts")):
            return
        try:
            await self.client.chat_update(channel=inc["slack_channel"], ts=inc["slack_ts"],
                                          text=incident_fallback_text(inc), blocks=incident_blocks(inc, runbook))
        except Exception as exc:
            log.error("Slack update failed for incident %s: %s", inc.get("id"), exc)

    async def post_thread(self, inc: dict, text: str) -> None:
        if not (self.enabled and inc.get("slack_ts")):
            return
        try:
            await self.client.chat_postMessage(channel=inc["slack_channel"], thread_ts=inc["slack_ts"], text=text)
        except Exception as exc:
            log.error("Slack thread reply failed for incident %s: %s", inc.get("id"), exc)
