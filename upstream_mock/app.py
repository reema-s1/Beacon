"""Fake external events API that `user_events_ingest` depends on.

The chaos CLI flips it between three modes through POST /admin/mode:
  healthy  normal responses
  down     every data request returns 503
  slow     every data request sleeps SLOW_DELAY_SECONDS before answering
"""
import asyncio
import os
import random
from datetime import datetime, timedelta, timezone
from typing import Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

SLOW_DELAY_SECONDS = float(os.getenv("SLOW_DELAY_SECONDS", "30"))

app = FastAPI(title="upstream-mock")
_state = {"mode": "healthy"}

EVENT_TYPES = ["page_view", "click", "signup", "purchase", "logout"]


class ModeRequest(BaseModel):
    mode: Literal["healthy", "down", "slow"]


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/admin/mode")
def get_mode():
    return _state


@app.post("/admin/mode")
def set_mode(req: ModeRequest):
    _state["mode"] = req.mode
    return _state


@app.get("/events")
async def events(limit: int = 200):
    mode = _state["mode"]
    if mode == "down":
        raise HTTPException(status_code=503, detail="events service unavailable (maintenance)")
    if mode == "slow":
        await asyncio.sleep(SLOW_DELAY_SECONDS)
    now = datetime.now(timezone.utc)
    limit = max(1, min(limit, 1000))
    return {
        "generated_at": now.isoformat(),
        "events": [
            {
                "event_id": f"evt-{now.timestamp():.0f}-{i}",
                "user_id": f"user-{random.randint(1, 500)}",
                "event_type": random.choice(EVENT_TYPES),
                "ts": (now - timedelta(seconds=random.randint(0, 180))).isoformat(),
            }
            for i in range(limit)
        ],
    }
