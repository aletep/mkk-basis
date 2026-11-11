"""Optional local receiver with explicit barriers for failure acceptance tests."""

import asyncio

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
events: dict[str, dict[str, object]] = {}
attempts: dict[str, int] = {}
gates: dict[str, asyncio.Event] = {}
gate_entries: dict[str, list[str]] = {}


class DemoEvent(BaseModel):
    event_id: str
    payment_id: str
    status: str


@app.post("/webhook")
async def webhook(
    event: DemoEvent,
    failures: int = Query(default=0, ge=0, le=3),
    status: int = Query(default=200, ge=200, le=599),
    gate: str | None = Query(default=None, min_length=1, max_length=100),
) -> JSONResponse:
    attempts[event.event_id] = attempts.get(event.event_id, 0) + 1
    if gate is not None:
        gate_entries.setdefault(gate, []).append(event.event_id)
        await gates.setdefault(gate, asyncio.Event()).wait()
    code = 500 if attempts[event.event_id] <= failures else status
    if 200 <= code < 300:
        events.setdefault(event.event_id, event.model_dump())
    return JSONResponse({"accepted": 200 <= code < 300}, status_code=code)


@app.get("/events")
async def received() -> dict[str, object]:
    return {"events": list(events.values()), "attempts": attempts}


@app.get("/gates/{name}")
async def gate_state(name: str) -> dict[str, object]:
    gate = gates.get(name)
    return {
        "entered": len(gate_entries.get(name, [])),
        "released": gate.is_set() if gate else False,
    }


@app.post("/gates/{name}/release")
async def release_gate(name: str) -> dict[str, bool]:
    gates.setdefault(name, asyncio.Event()).set()
    return {"released": True}
