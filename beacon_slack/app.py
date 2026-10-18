"""beacon-slack: Bolt app in Socket Mode (no public URL needed)."""
from __future__ import annotations

import logging
import threading

from beacon.config import get_settings
from beacon_slack.api_client import BeaconAPI
from beacon_slack.handlers import Handlers, register

log = logging.getLogger("beacon.slack")


def build_app():
    from slack_bolt import App
    s = get_settings()
    app = App(token=s.slack_bot_token)
    register(app, Handlers(BeaconAPI(s.beacon_api_url), s.rerun_allowlist))
    return app


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    s = get_settings()
    if not (s.slack_bot_token and s.slack_app_token):
        log.warning("SLACK_BOT_TOKEN / SLACK_APP_TOKEN not set: beacon-slack is idle. See README → Slack setup.")
        threading.Event().wait()
        return
    from slack_bolt.adapter.socket_mode import SocketModeHandler
    SocketModeHandler(build_app(), s.slack_app_token).start()


if __name__ == "__main__":
    main()
