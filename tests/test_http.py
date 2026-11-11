import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
from asgi_lifespan import LifespanManager
from sqlalchemy.ext.asyncio import create_async_engine

from mkk_basis.adapters.external import HttpWebhookSender, WebhookPolicy
from mkk_basis.adapters.http import build_api
from mkk_basis.application.errors import TransientProcessingError
from mkk_basis.application.payments import CreatePayment, GetPayment
from mkk_basis.domain.models import Currency, Notification, Status
from tests.fakes import FakeClock, FakeUow, Store

KEY = "test-api-key-for-local-tests"


@pytest.fixture
async def client():
    store, clock = Store(), FakeClock()

    def factory():
        return FakeUow(store)

    engine = create_async_engine("postgresql+asyncpg://unused:unused@127.0.0.1:1/unused")
    app = build_api(
        CreatePayment(factory, clock),
        GetPayment(factory),
        engine,
        KEY,
        WebhookPolicy(["http://receiver"]),
    )
    async with LifespanManager(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as c:
            yield c


def body(**changes):
    return {
        "amount": "12.30",
        "currency": "RUB",
        "description": "order",
        "metadata": {},
        "webhook_url": "http://receiver/webhook",
        **changes,
    }


async def test_api_contract_and_auth(client):
    assert (await client.post("/api/v1/payments", json=body())).status_code == 401
    assert (await client.get("/health")).status_code == 401
    headers = {"X-API-Key": KEY, "Idempotency-Key": "order-1"}
    first = await client.post("/api/v1/payments", json=body(), headers=headers)
    assert first.status_code == 202
    repeated = await client.post("/api/v1/payments", json=body(amount="12.3"), headers=headers)
    assert repeated.json() == first.json()
    conflict = await client.post("/api/v1/payments", json=body(amount="22"), headers=headers)
    assert conflict.status_code == 409
    response = await client.get("/api/v1/payments/" + first.json()["payment_id"], headers=headers)
    assert response.json()["amount"] == "12.30"
    assert "fingerprint" not in response.json() and "last_error" not in response.json()
    assert (await client.get(f"/api/v1/payments/{uuid4()}", headers=headers)).status_code == 404
    assert (await client.get("/docs", headers=headers)).status_code == 404


@pytest.mark.parametrize(
    "change",
    [
        {"amount": "0"},
        {"amount": "1.001"},
        {"currency": "GBP"},
        {"webhook_url": "http://postgres:5432/"},
        {"webhook_url": "http://receiver.evil/"},
        {"webhook_url": "http://user:password@receiver/"},
        {"webhook_url": "file:///etc/passwd"},
        {"status": "succeeded"},
        {"metadata": []},
    ],
)
async def test_validation(client, change):
    response = await client.post(
        "/api/v1/payments",
        json=body(**change),
        headers={"X-API-Key": KEY, "Idempotency-Key": "key"},
    )
    assert response.status_code == 422


async def test_missing_idempotency_key(client):
    response = await client.post("/api/v1/payments", json=body(), headers={"X-API-Key": KEY})
    assert response.status_code == 422


def raw_body(amount_literal='"12.30"', **changes):
    return json.dumps(body(amount="__RAW_AMOUNT__", **changes)).replace(
        '"__RAW_AMOUNT__"', amount_literal
    )


async def test_raw_numeric_money_keeps_precision_and_idempotency(client):
    headers = {"X-API-Key": KEY, "Idempotency-Key": "large-money"}
    first = await client.post(
        "/api/v1/payments", content=raw_body("1000000000000000.01"), headers=headers
    )
    assert first.status_code == 202
    payment_id = first.json()["payment_id"]
    saved = await client.get(f"/api/v1/payments/{payment_id}", headers=headers)
    assert saved.json()["amount"] == "1000000000000000.01"
    repeated = await client.post(
        "/api/v1/payments", content=raw_body('"1000000000000000.01"'), headers=headers
    )
    assert repeated.status_code == 202 and repeated.json()["payment_id"] == payment_id
    conflict = await client.post(
        "/api/v1/payments", content=raw_body("1000000000000000.02"), headers=headers
    )
    assert conflict.status_code == 409


@pytest.mark.parametrize(
    "literal",
    ["99.99999999999999999", "1.001", "10000000000000000", "true", "NaN", "Infinity"],
)
async def test_raw_numeric_money_rejects_invalid_values(client, literal):
    response = await client.post(
        "/api/v1/payments",
        content=raw_body(literal),
        headers={"X-API-Key": KEY, "Idempotency-Key": "invalid-money"},
    )
    assert response.status_code == 422


async def test_raw_fractional_metadata_is_json_compatible_and_canonical(client):
    headers = {"X-API-Key": KEY, "Idempotency-Key": "fractional-metadata"}
    payload = raw_body("12.30", metadata={"nested": [0.125, {"fraction": 1.5}], "n": 2})
    first = await client.post("/api/v1/payments", content=payload, headers=headers)
    assert first.status_code == 202
    payment_id = first.json()["payment_id"]
    saved = await client.get(f"/api/v1/payments/{payment_id}", headers=headers)
    assert saved.json()["metadata"] == {"nested": [0.125, {"fraction": 1.5}], "n": 2}
    reordered = raw_body('"12.3"', metadata={"n": 2, "nested": [0.125, {"fraction": 1.5}]})
    repeat = await client.post("/api/v1/payments", content=reordered, headers=headers)
    assert repeat.status_code == 202 and repeat.json()["payment_id"] == payment_id


@pytest.mark.parametrize(
    "changes",
    [
        {"description": "a\x00b"},
        {"description": "a\ud800b"},
        {"metadata": {"nested": ["a\x00b"]}},
        {"metadata": {"bad\x00key": "value"}},
        {"metadata": {"nested": {"surrogate\udfff": 1}}},
    ],
)
async def test_postgres_incompatible_json_strings_are_rejected(client, changes):
    response = await client.post(
        "/api/v1/payments",
        content=raw_body(**changes),
        headers={"X-API-Key": KEY, "Idempotency-Key": "bad-text"},
    )
    assert response.status_code == 422


@pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity", "1e9999"])
async def test_non_finite_nested_metadata_is_rejected(client, literal):
    payload = raw_body().replace('"metadata": {}', '"metadata": {"x": [' + literal + "]}")
    response = await client.post(
        "/api/v1/payments",
        content=payload,
        headers={"X-API-Key": KEY, "Idempotency-Key": "non-finite"},
    )
    assert response.status_code == 422


@pytest.mark.parametrize("payload", [b"{", b"\xff", b"[]", b"null"])
async def test_invalid_json_has_controlled_response(client, payload):
    response = await client.post(
        "/api/v1/payments",
        content=payload,
        headers={"X-API-Key": KEY, "Idempotency-Key": "bad-json"},
    )
    assert response.status_code == 422


async def test_validation_errors_omit_input_values(client):
    response = await client.post(
        "/api/v1/payments",
        json=body(amount="private-payload-marker"),
        headers={"X-API-Key": KEY, "Idempotency-Key": "private"},
    )
    assert response.status_code == 422
    assert "private-payload-marker" not in response.text
    assert all("input" not in error and "ctx" not in error for error in response.json()["detail"])


@pytest.mark.parametrize("code", [200, 202, 204, 301, 400, 429, 500])
async def test_webhook_success_codes_no_redirect_no_api_key(code):
    observed = []

    def handler(request):
        observed.append(request)
        return httpx.Response(code, headers={"Location": "http://evil/"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
        sender = HttpWebhookSender(c, WebhookPolicy(["http://receiver"]))
        notice = Notification(
            uuid4(), uuid4(), Status.FAILED, Decimal("10.00"), Currency.RUB, datetime.now(UTC)
        )
        if code < 300:
            await sender.send("http://receiver/webhook", notice)
        else:
            with pytest.raises(TransientProcessingError):
                await sender.send("http://receiver/webhook", notice)
    assert len(observed) == 1
    assert "X-API-Key" not in observed[0].headers
    assert observed[0].headers["Idempotency-Key"] == str(notice.event_id)
