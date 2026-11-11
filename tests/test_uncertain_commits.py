import os
from datetime import timedelta

import httpx
import pytest
from asgi_lifespan import LifespanManager
from sqlalchemy import func, select

from mkk_basis.adapters.external import WebhookPolicy
from mkk_basis.adapters.http import build_api
from mkk_basis.adapters.postgres import OutboxRow, PaymentRow, PostgresUow
from mkk_basis.adapters.rabbit import QUEUE, RabbitPublisher
from mkk_basis.application.errors import DependencyUnavailable
from mkk_basis.application.outbox import PublishOutbox
from mkk_basis.application.payments import CreatePayment, Decision, GetPayment, ProcessPayment
from mkk_basis.domain.models import Status
from tests.fakes import FakeClock, Gateway, Webhook
from tests.test_integration import broker as broker
from tests.test_integration import data as data
from tests.test_integration import pg as pg
from tests.test_integration import receive

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="isolated Compose only"),
]


class CommitResponseLoss:
    """Commit to PostgreSQL, then lose one response at a specified boundary."""

    def __init__(self, sessions, commit_number):
        self.sessions = sessions
        self.commit_number = commit_number
        self.commits = 0
        self.triggered = False

    def __call__(self):
        fault = self

        class UncertainUow(PostgresUow):
            async def commit(self):
                await super().commit()
                fault.commits += 1
                if fault.commits == fault.commit_number:
                    fault.triggered = True
                    raise ConnectionResetError("response_lost_after_real_commit")

        return UncertainUow(self.sessions)


async def test_create_commit_response_loss_returns_503_then_same_payment_202(pg):
    engine, sessions, factory = pg
    clock = FakeClock()
    uncertain = CommitResponseLoss(sessions, 1)
    app = build_api(
        CreatePayment(uncertain, clock),
        GetPayment(factory),
        engine,
        "test-secret",
        WebhookPolicy(["http://receiver"]),
    )
    headers = {"X-API-Key": "test-secret", "Idempotency-Key": "lost-create-response"}
    body = {
        "amount": "12.30",
        "currency": "RUB",
        "description": "uncertain create",
        "metadata": {"order_id": 1},
        "webhook_url": "http://receiver/webhook",
    }
    async with LifespanManager(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test", headers=headers
        ) as client:
            first = await client.post("/api/v1/payments", json=body)
            assert uncertain.triggered and first.status_code == 503
            async with sessions() as session:
                committed_id = await session.scalar(select(PaymentRow.id))
                event_id = await session.scalar(select(OutboxRow.id))
            assert committed_id is not None and event_id is not None
            repeated = await client.post("/api/v1/payments", json=body)
            assert repeated.status_code == 202
            assert repeated.json()["payment_id"] == str(committed_id)
            conflicting = await client.post("/api/v1/payments", json=body | {"amount": "12.31"})
            assert conflicting.status_code == 409
            async with sessions() as session:
                assert await session.scalar(select(func.count()).select_from(PaymentRow)) == 1
                assert await session.scalar(select(func.count()).select_from(OutboxRow)) == 1
                assert await session.scalar(select(OutboxRow.id)) == event_id


@pytest.mark.parametrize("commit_number", [2, 3], ids=["complete", "delivered"])
async def test_stage_commit_response_loss_does_not_repeat_side_effects(pg, data, commit_number):
    _, sessions, factory = pg
    clock = FakeClock()
    payment = await CreatePayment(factory, clock).execute("lost-stage-response", data)
    uncertain = CommitResponseLoss(sessions, commit_number)
    gateway, webhook = Gateway(), Webhook()
    result = await ProcessPayment(uncertain, clock, gateway, webhook).execute(payment.id)
    saved = await GetPayment(factory).execute(payment.id)
    assert uncertain.triggered
    assert result.decision == Decision.ACK
    assert saved.status == Status.SUCCEEDED and saved.processed_at is not None
    assert saved.webhook_delivered_at is not None and saved.processing_attempts == 1
    assert gateway.calls == len(webhook.calls) == 1
    assert clock.sleeps == [1]
    # A later delivery must also observe the committed completed stage.
    assert (
        await ProcessPayment(factory, clock, gateway, webhook).execute(payment.id)
    ).decision == Decision.ACK
    assert gateway.calls == len(webhook.calls) == 1


async def test_outbox_claim_commit_response_loss_waits_for_lease_then_publishes(pg, broker, data):
    _, sessions, factory = pg
    clock = FakeClock()
    payment = await CreatePayment(factory, clock).execute("lost-claim-response", data)
    uncertain = CommitResponseLoss(sessions, 1)
    published = []
    publisher = RabbitPublisher(broker)

    class CountingPublisher:
        async def publish(self, event):
            published.append(event.id)
            await publisher.publish(event)

    relay = PublishOutbox(uncertain, clock, CountingPublisher())
    with pytest.raises(DependencyUnavailable):
        await relay.execute()
    assert uncertain.triggered and not published
    async with sessions() as session:
        claimed = await session.scalar(select(OutboxRow))
        event_id = claimed.id
        assert claimed.payment_id == payment.id and claimed.attempts == 1
        assert claimed.published_at is None and claimed.lock_token is not None
        assert claimed.locked_until == clock.now() + timedelta(seconds=30)
    assert not await relay.execute()
    assert not published
    clock.value += timedelta(seconds=31)
    assert await relay.execute()
    message = await receive(await broker.declare_queue(QUEUE))
    assert message.message_id == str(event_id)
    await message.ack()
    async with sessions() as session:
        recovered = await session.get(OutboxRow, event_id)
        assert recovered.published_at is not None and recovered.attempts == 2
        assert recovered.lock_token is None and recovered.locked_until is None
    assert published == [event_id]


async def test_outbox_mark_commit_response_loss_observes_published_without_duplicate(
    pg, broker, data
):
    _, sessions, factory = pg
    clock = FakeClock()
    await CreatePayment(factory, clock).execute("lost-mark-response", data)
    uncertain = CommitResponseLoss(sessions, 2)
    published = []
    publisher = RabbitPublisher(broker)

    class CountingPublisher:
        async def publish(self, event):
            published.append(event.id)
            await publisher.publish(event)

    relay = PublishOutbox(uncertain, clock, CountingPublisher())
    with pytest.raises(DependencyUnavailable):
        await relay.execute()
    assert uncertain.triggered and len(published) == 1
    async with sessions() as session:
        committed = await session.get(OutboxRow, published[0])
        assert committed.published_at is not None
        assert committed.lock_token is None and committed.locked_until is None
    message = await receive(await broker.declare_queue(QUEUE))
    assert message.message_id == str(published[0])
    await message.ack()
    clock.value += timedelta(seconds=31)
    assert not await relay.execute()
    assert len(published) == 1
