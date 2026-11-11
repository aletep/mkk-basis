import asyncio
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from mkk_basis.application.errors import (
    DependencyUnavailable,
    IdempotencyConflict,
    PublicationFailed,
)
from mkk_basis.application.outbox import PublishOutbox
from mkk_basis.application.payments import CreatePayment, Decision, ProcessPayment
from mkk_basis.domain.models import Currency, PaymentInput, Status
from tests.fakes import FakeClock, FakeUow, Gateway, Store, Webhook


@pytest.fixture
def setup():
    store, clock = Store(), FakeClock()

    def factory():
        return FakeUow(store)

    data = PaymentInput(
        Decimal("12.30"), Currency.RUB, "order", {"order_id": 1}, "http://receiver/webhook"
    )
    return store, clock, factory, data


async def test_create_and_normalized_idempotency(setup):
    store, clock, factory, data = setup
    create = CreatePayment(factory, clock)
    first = await create.execute("key", data)
    repeated = await create.execute("key", replace(data, amount=Decimal("12.3")))
    assert first.id == repeated.id
    assert len(store.payments) == len(store.events) == 1
    with pytest.raises(IdempotencyConflict):
        await create.execute("key", replace(data, amount=Decimal("99")))


@pytest.mark.parametrize("amount", ["0", "-1", "1.001", "NaN", "Infinity", "10000000000000000"])
def test_money_rejects_invalid_values(amount):
    with pytest.raises(ValueError):
        PaymentInput(Decimal(amount), Currency.RUB, "", {}, "http://receiver/")


async def run_to_completion(process, payment_id, clock):
    while True:
        result = await process.execute(payment_id)
        if result.decision != Decision.RETRY:
            return result
        await clock.sleep((result.retry_at - clock.now()).total_seconds())


@pytest.mark.parametrize("status", [Status.SUCCEEDED, Status.FAILED])
async def test_gateway_result_is_final_and_webhook_retries_remaining_stage(setup, status):
    store, clock, factory, data = setup
    payment = await CreatePayment(factory, clock).execute("key", data)
    gateway, webhook = Gateway(status, store), Webhook(2, store)
    process = ProcessPayment(factory, clock, gateway, webhook)
    result = await run_to_completion(process, payment.id, clock)
    assert result.decision == Decision.ACK
    assert gateway.calls == 1
    assert len(webhook.calls) == 3
    assert len({call.event_id for call in webhook.calls}) == 1
    assert clock.sleeps == [1, 2]
    saved = store.payments[payment.id]
    assert saved.status == status and saved.processing_attempts == 3
    assert saved.webhook_delivered_at and not saved.exhausted_at
    assert (await process.execute(payment.id)).decision == Decision.ACK
    assert len(webhook.calls) == 3  # ACK before the attempt limit on redelivery.


async def test_exhaustion_preserves_payment_result(setup):
    store, clock, factory, data = setup
    payment = await CreatePayment(factory, clock).execute("key", data)
    webhook = Webhook(10)
    process = ProcessPayment(factory, clock, Gateway(), webhook)
    result = await run_to_completion(process, payment.id, clock)
    assert result.decision == Decision.DEAD_LETTER
    assert store.payments[payment.id].status == Status.SUCCEEDED
    assert store.payments[payment.id].exhausted_at
    assert (await process.execute(payment.id)).decision == Decision.DEAD_LETTER
    assert len(webhook.calls) == 3


async def test_restart_after_result_commit_does_not_repeat_gateway(setup):
    store, clock, factory, data = setup
    payment = await CreatePayment(factory, clock).execute("key", data)
    gateway = Gateway()

    class CrashWebhook:
        async def send(self, url, notification):
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await ProcessPayment(factory, clock, gateway, CrashWebhook()).execute(payment.id)
    assert store.payments[payment.id].processed_at
    webhook = Webhook()
    result = await run_to_completion(
        ProcessPayment(factory, clock, gateway, webhook), payment.id, clock
    )
    assert result.decision == Decision.ACK
    assert gateway.calls == 1 and len(webhook.calls) == 1
    assert store.payments[payment.id].processing_attempts == 2


async def test_unknown_payment_dead_letters(setup):
    _, clock, factory, _ = setup
    result = await ProcessPayment(factory, clock, Gateway(), Webhook()).execute(uuid4())
    assert result.decision == Decision.DEAD_LETTER


async def test_uncertain_attempt_commit_does_not_spend_another_attempt(setup):
    store, clock, factory, data = setup
    payment = await CreatePayment(factory, clock).execute("uncertain", data)

    class UncertainCommit(FakeUow):
        async def commit(self):
            await super().commit()
            if self.store.payments[payment.id].processing_attempts == 1:
                if not getattr(self.store, "lost_response", False):
                    self.store.lost_response = True
                    raise DependencyUnavailable("response_lost_after_commit")

    gateway, webhook = Gateway(), Webhook()
    result = await ProcessPayment(lambda: UncertainCommit(store), clock, gateway, webhook).execute(
        payment.id
    )
    assert result.decision == Decision.ACK
    assert store.payments[payment.id].processing_attempts == 1
    assert gateway.calls == len(webhook.calls) == 1
    assert clock.sleeps == [1]


async def test_outbox_confirm_crash_and_lease_recovery(setup):
    store, clock, factory, data = setup
    await CreatePayment(factory, clock).execute("key", data)
    publications = []

    class Publisher:
        async def publish(self, event):
            assert not store.active
            publications.append(event.id)
            if len(publications) == 1:
                raise asyncio.CancelledError  # Confirm arrived, process died before mark.

    relay = PublishOutbox(factory, clock, Publisher())
    with pytest.raises(asyncio.CancelledError):
        await relay.execute()
    assert not await relay.execute()  # Lease still owned by the crashed publisher.
    clock.value += timedelta(seconds=31)
    assert await relay.execute()
    assert publications[0] == publications[1]
    assert next(iter(store.events.values())).published_at


async def test_outbox_failure_stays_pending_until_recovery(setup):
    store, clock, factory, data = setup
    await CreatePayment(factory, clock).execute("key", data)

    class Publisher:
        async def publish(self, event):
            raise PublicationFailed("broker_unavailable")

    assert await PublishOutbox(factory, clock, Publisher()).execute()
    event = next(iter(store.events.values()))
    assert event.published_at is None and event.next_attempt_at > clock.now()
    assert event.lock_token is None and event.attempts == 1
