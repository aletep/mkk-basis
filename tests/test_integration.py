import asyncio
import os
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import aio_pika
import httpx
import pytest
from asgi_lifespan import LifespanManager
from sqlalchemy import func, select, text

from mkk_basis.adapters.external import HttpWebhookSender, WebhookPolicy
from mkk_basis.adapters.http import build_api
from mkk_basis.adapters.postgres import OutboxRow, PaymentRow, PostgresUow
from mkk_basis.adapters.rabbit import (
    DLQ,
    EXCHANGE,
    QUEUE,
    RabbitPublisher,
    declare_topology,
    make_broker,
    register_consumer,
)
from mkk_basis.application.outbox import PublishOutbox
from mkk_basis.application.payments import CreatePayment, Decision, GetPayment, ProcessPayment
from mkk_basis.bootstrap.config import Settings
from mkk_basis.bootstrap.resources import database
from mkk_basis.domain.models import Currency, OutboxEvent, Payment, PaymentInput, Status
from tests.fakes import FakeClock, Gateway, Webhook
from tests.safety import require_isolated_resources
from tests.test_application import run_to_completion

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="isolated Compose only"),
]


@pytest.fixture
async def pg():
    require_isolated_resources()
    settings = Settings()
    engine, sessions = database(settings)
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE outbox, payments"))
    yield engine, sessions, lambda: PostgresUow(sessions)
    await engine.dispose()


@pytest.fixture
async def broker():
    require_isolated_resources()
    instance = make_broker(Settings().amqp_url.get_secret_value())
    await instance.connect()
    await declare_topology(instance)
    for schema in (QUEUE, DLQ):
        queue = await instance.declare_queue(schema)
        await queue.purge()
    yield instance
    await instance.stop()


@pytest.fixture
def data():
    return PaymentInput(
        Decimal("12.30"), Currency.RUB, "order", {"order_id": 1}, "http://receiver/webhook"
    )


async def receive(queue):
    async with asyncio.timeout(10):
        while True:
            message = await queue.get(fail=False, timeout=1)
            if message:
                return message
            await asyncio.sleep(0.05)


async def test_rollback_is_atomic(pg, data):
    _, sessions, factory = pg
    clock = FakeClock()
    payment = Payment(uuid4(), data, "rollback", "0" * 64, clock.now())
    with pytest.raises(RuntimeError):
        async with factory() as tx:
            await tx.payments.add(payment)
            await tx.outbox.add(OutboxEvent(uuid4(), payment.id, clock.now(), {}))
            raise RuntimeError("failure before commit")
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(PaymentRow)) == 0
        assert await session.scalar(select(func.count()).select_from(OutboxRow)) == 0


async def test_concurrent_http_idempotency_uses_database_constraint(pg):
    engine, _, factory = pg
    clock = FakeClock()
    app = build_api(
        CreatePayment(factory, clock),
        GetPayment(factory),
        engine,
        "test-secret",
        WebhookPolicy(["http://receiver"]),
    )
    headers = {"X-API-Key": "test-secret", "Idempotency-Key": "concurrent"}
    body = {
        "amount": "1.00",
        "currency": "RUB",
        "description": "x",
        "webhook_url": "http://receiver/webhook",
    }
    async with LifespanManager(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test", headers=headers
        ) as client:
            responses = await asyncio.gather(
                *[client.post("/api/v1/payments", json=body) for _ in range(12)]
            )
            assert {response.status_code for response in responses} == {202}
            assert len({response.json()["payment_id"] for response in responses}) == 1
            assert (
                await client.post("/api/v1/payments", json=body | {"amount": "2"})
            ).status_code == 409
    async with pg[1]() as session:
        assert await session.scalar(select(func.count()).select_from(PaymentRow)) == 1
        assert await session.scalar(select(func.count()).select_from(OutboxRow)) == 1


async def test_skip_locked_lease_and_stale_owner(pg, data):
    _, _, factory = pg
    clock = FakeClock()
    await CreatePayment(factory, clock).execute("claim", data)
    first_token, second_token = uuid4(), uuid4()
    async with factory() as first:
        event = await first.outbox.claim(
            clock.now(), clock.now() + timedelta(seconds=30), first_token
        )
        async with factory() as second:
            assert await second.outbox.claim(clock.now(), clock.now(), second_token) is None
        await first.commit()
    async with factory() as tx:
        assert not await tx.outbox.published(event.id, second_token, clock.now())
        await tx.commit()
    clock.value += timedelta(seconds=31)
    async with factory() as tx:
        reclaimed = await tx.outbox.claim(
            clock.now(), clock.now() + timedelta(seconds=30), second_token
        )
        assert reclaimed.id == event.id and reclaimed.attempts == 2
        await tx.commit()
    async with factory() as tx:
        assert not await tx.outbox.published(event.id, first_token, clock.now())
        assert await tx.outbox.published(event.id, second_token, clock.now())
        await tx.commit()


async def test_confirm_unroutable_and_crash_republication(pg, broker, data):
    _, sessions, factory = pg
    clock = FakeClock()
    await CreatePayment(factory, clock).execute("confirm", data)
    # Mandatory return must not be treated as success, even if broker confirms it.
    await PublishOutbox(factory, clock, RabbitPublisher(broker, "no-binding")).execute()
    async with sessions() as session:
        row = await session.scalar(select(OutboxRow))
        assert row.published_at is None and row.last_error
    clock.value += timedelta(seconds=2)
    publisher = RabbitPublisher(broker)

    class CrashAfterConfirm:
        async def publish(self, event):
            await publisher.publish(event)
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await PublishOutbox(factory, clock, CrashAfterConfirm()).execute()
    queue = await broker.declare_queue(QUEUE)
    first = await receive(queue)
    assert first.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    await first.ack()
    clock.value += timedelta(seconds=31)
    assert await PublishOutbox(factory, clock, publisher).execute()
    second = await receive(queue)
    assert second.message_id == first.message_id
    await second.ack()
    async with sessions() as session:
        assert (await session.scalar(select(OutboxRow))).published_at


async def test_manual_ack_redelivery(broker):
    await broker.publish(
        {"version": 1, "payment_id": str(uuid4())},
        exchange=EXCHANGE,
        routing_key="payments.new",
        persist=True,
    )
    connection = await aio_pika.connect(Settings().amqp_url.get_secret_value())
    channel = await connection.channel()
    queue = await channel.get_queue("payments.new")
    first = await receive(queue)
    assert not first.redelivered
    await connection.close()  # No ACK: broker must retain and redeliver.
    second = await receive(await broker.declare_queue(QUEUE))
    assert second.redelivered and second.body == first.body
    await second.ack()


@pytest.mark.parametrize("failures", [2, 3])
async def test_consumer_http_retries_and_dead_letter(pg, broker, data, failures):
    _, _, factory = pg
    clock = FakeClock()
    payment = await CreatePayment(factory, clock).execute("retry", data)
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(500 if len(calls) <= failures else 200)

    gateway = Gateway(Status.SUCCEEDED)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        process = ProcessPayment(
            factory, clock, gateway, HttpWebhookSender(client, WebhookPolicy(["http://receiver"]))
        )
        ready = asyncio.Event()
        ready.set()
        register_consumer(broker, process, clock, ready)
        await broker.start()
        await PublishOutbox(factory, clock, RabbitPublisher(broker)).execute()
        async with asyncio.timeout(10):
            while True:
                saved = await GetPayment(factory).execute(payment.id)
                if saved.webhook_delivered_at or saved.exhausted_at:
                    break
                await asyncio.sleep(0.05)
        assert saved.status == Status.SUCCEEDED and saved.processing_attempts == 3
        assert gateway.calls == 1 and len(calls) == 3
        assert len({request.headers["Idempotency-Key"] for request in calls}) == 1
        if failures == 3:
            message = await receive(await broker.declare_queue(DLQ))
            assert message.headers["x-death"][0]["reason"] == "rejected"
            await message.ack()
        else:
            # Duplicate event invokes the use case without repeating side effects.
            assert (await process.execute(payment.id)).decision == Decision.ACK
            assert len(calls) == 3


async def test_invalid_message_goes_to_dlq(pg, broker):
    ready = asyncio.Event()
    ready.set()
    register_consumer(
        broker, ProcessPayment(pg[2], FakeClock(), Gateway(), Webhook()), FakeClock(), ready
    )
    await broker.start()
    await broker.publish(
        {"unexpected": "payload"}, exchange=EXCHANGE, routing_key="payments.new", persist=True
    )
    message = await receive(await broker.declare_queue(DLQ))
    assert b"unexpected" in message.body
    await message.ack()


async def test_restart_preserves_attempt_budget_and_result(pg, data):
    _, _, factory = pg
    clock = FakeClock()
    payment = await CreatePayment(factory, clock).execute("restart", data)
    gateway = Gateway()

    class CrashWebhook:
        async def send(self, url, notification):
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await ProcessPayment(factory, clock, gateway, CrashWebhook()).execute(payment.id)
    before = await GetPayment(factory).execute(payment.id)
    webhook = Webhook(10)
    result = await run_to_completion(
        ProcessPayment(factory, clock, gateway, webhook), payment.id, clock
    )
    after = await GetPayment(factory).execute(payment.id)
    assert result.decision == Decision.DEAD_LETTER
    assert after.processing_attempts == 3 and len(webhook.calls) == 2
    assert before.processed_at == after.processed_at and gateway.calls == 1
