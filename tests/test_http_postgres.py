import asyncio
import os

import httpx
import pytest
from asgi_lifespan import LifespanManager
from sqlalchemy import func, select

from mkk_basis.adapters.external import WebhookPolicy
from mkk_basis.adapters.http import build_api
from mkk_basis.adapters.postgres import OutboxRow, PaymentRow
from mkk_basis.application.payments import CreatePayment, GetPayment
from tests.fakes import FakeClock
from tests.test_http import KEY, raw_body
from tests.test_integration import pg as pg

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="isolated Compose only"),
]


@pytest.fixture
async def pg_client(pg):
    engine, _, factory = pg
    app = build_api(
        CreatePayment(factory, FakeClock()),
        GetPayment(factory),
        engine,
        KEY,
        WebhookPolicy(["http://receiver"]),
    )
    async with LifespanManager(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as client:
            yield client


async def test_raw_money_concurrency_precision_and_conflict_on_postgres(pg_client, pg):
    headers = {"X-API-Key": KEY, "Idempotency-Key": "exact-money"}
    payload = raw_body("1000000000000000.01", metadata={"nested": [0.125, {"n": 1.5}]})
    responses = await asyncio.gather(
        *[pg_client.post("/api/v1/payments", content=payload, headers=headers) for _ in range(20)]
    )
    assert {response.status_code for response in responses} == {202}
    payment_ids = {response.json()["payment_id"] for response in responses}
    assert len(payment_ids) == 1
    payment_id = payment_ids.pop()
    saved = await pg_client.get(f"/api/v1/payments/{payment_id}", headers=headers)
    assert saved.json()["amount"] == "1000000000000000.01"
    assert saved.json()["metadata"] == {"nested": [0.125, {"n": 1.5}]}
    conflict = await pg_client.post(
        "/api/v1/payments",
        content=raw_body("1000000000000000.02", metadata={"nested": [0.125, {"n": 1.5}]}),
        headers=headers,
    )
    assert conflict.status_code == 409
    async with pg[1]() as session:
        assert await session.scalar(select(func.count()).select_from(PaymentRow)) == 1
        assert await session.scalar(select(func.count()).select_from(OutboxRow)) == 1


@pytest.mark.parametrize(
    "payload",
    [
        raw_body("99.99999999999999999"),
        raw_body(description="a\x00b"),
        raw_body(metadata={"nested": {"bad\x00key": "a\x00b"}}),
        raw_body().replace('"metadata": {}', '"metadata": {"x": NaN}'),
    ],
)
async def test_invalid_raw_payloads_return_422_without_postgres_writes(pg_client, pg, payload):
    response = await pg_client.post(
        "/api/v1/payments",
        content=payload,
        headers={"X-API-Key": KEY, "Idempotency-Key": "invalid-payload"},
    )
    assert response.status_code == 422
    async with pg[1]() as session:
        assert await session.scalar(select(func.count()).select_from(PaymentRow)) == 0
        assert await session.scalar(select(func.count()).select_from(OutboxRow)) == 0
