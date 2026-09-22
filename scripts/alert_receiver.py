"""Minimal HTTP webhook target for the Confluent HTTP Sink connector."""

from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, Request

app = FastAPI(title="ApiaryPulse Alert Receiver")
_recent: deque[dict[str, Any]] = deque(maxlen=50)


@app.post("/webhook")
async def webhook(request: Request) -> dict[str, str]:
    body = await request.json()
    entry = {"received_at": datetime.now(timezone.utc).isoformat(), "payload": body}
    _recent.appendleft(entry)
    return {"status": "ok"}


@app.get("/alerts")
def list_alerts() -> list[dict[str, Any]]:
    return list(_recent)
