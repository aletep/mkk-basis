import asyncio
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from aio_pika.exceptions import ChannelInvalidStateError
from pamqp.commands import Basic

from mkk_basis.adapters.processing import ProcessingSupervisor
from mkk_basis.adapters.rabbit import RabbitPublisher
from mkk_basis.application.errors import PublicationFailed
from mkk_basis.application.payments import CreatePayment, Decision, ProcessPayment
from mkk_basis.domain.models import OutboxEvent, Status
from tests.fakes import Webhook
from tests.test_application import setup as setup


@pytest.mark.parametrize("error", [ChannelInvalidStateError("closed"), OSError(), TimeoutError()])
async def test_publisher_classifies_known_transport_errors(error):
    broker = Mock(publish=AsyncMock(side_effect=error))
    event = OutboxEvent(uuid4(), uuid4(), Mock(), {})
    with pytest.raises(PublicationFailed):
        await RabbitPublisher(broker).publish(event)


@pytest.mark.parametrize("confirmation", [None, Basic.Nack(), Basic.Reject()])
async def test_publisher_requires_positive_confirmation(confirmation):
    broker = Mock(publish=AsyncMock(return_value=confirmation))
    with pytest.raises(PublicationFailed):
        await RabbitPublisher(broker).publish(OutboxEvent(uuid4(), uuid4(), Mock(), {}))


async def test_publisher_propagates_programming_errors_and_cancellation():
    for error in (RuntimeError("bug"), asyncio.CancelledError()):
        broker = Mock(publish=AsyncMock(side_effect=error))
        with pytest.raises(type(error)):
            await RabbitPublisher(broker).publish(OutboxEvent(uuid4(), uuid4(), Mock(), {}))


class GatedGateway:
    def __init__(self):
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0

    async def process(self, payment_id):
        self.calls += 1
        self.entered.set()
        await self.release.wait()
        return Status.SUCCEEDED


async def test_delivery_cancellation_rejoins_same_attempt(setup):
    store, clock, factory, data = setup
    payment = await CreatePayment(factory, clock).execute("reconnect", data)
    gateway, webhook = GatedGateway(), Webhook()
    supervisor = ProcessingSupervisor(
        ProcessPayment(factory, clock, gateway, webhook), clock, AsyncMock(return_value=True)
    )
    delivery = asyncio.create_task(supervisor.execute(payment.id))
    await gateway.entered.wait()
    try:
        for _ in range(3):
            delivery.cancel()
            with pytest.raises(asyncio.CancelledError):
                await delivery
            delivery = asyncio.create_task(supervisor.execute(payment.id))
            await asyncio.sleep(0)
            assert store.payments[payment.id].processing_attempts == 1
            assert gateway.calls == 1
        gateway.release.set()
        assert (await delivery).decision == Decision.ACK
        assert len(webhook.calls) == 1
    finally:
        await supervisor.close()


async def test_supervisor_shutdown_spends_begun_attempt_and_leaves_no_task(setup):
    store, clock, factory, data = setup
    payment = await CreatePayment(factory, clock).execute("shutdown", data)
    gateway = GatedGateway()
    supervisor = ProcessingSupervisor(
        ProcessPayment(factory, clock, gateway, Webhook()), clock, AsyncMock(return_value=True)
    )
    delivery = asyncio.create_task(supervisor.execute(payment.id))
    await gateway.entered.wait()
    await supervisor.close()
    with pytest.raises(asyncio.CancelledError):
        await delivery
    assert store.payments[payment.id].processing_attempts == 1
    assert store.payments[payment.id].status == Status.PENDING
    assert not any(task.get_name() == "payment-processing" for task in asyncio.all_tasks())
    with pytest.raises(asyncio.CancelledError):
        await supervisor.execute(payment.id)


async def test_supervisor_serializes_different_payments_after_cancelled_delivery(setup):
    store, clock, factory, data = setup
    first = await CreatePayment(factory, clock).execute("first", data)
    second = await CreatePayment(factory, clock).execute("second", data)
    gateway = GatedGateway()
    supervisor = ProcessingSupervisor(
        ProcessPayment(factory, clock, gateway, Webhook()), clock, AsyncMock(return_value=True)
    )
    first_delivery = asyncio.create_task(supervisor.execute(first.id))
    await gateway.entered.wait()
    first_delivery.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first_delivery
    next_delivery = asyncio.create_task(supervisor.execute(second.id))
    await asyncio.sleep(0)
    assert gateway.calls == 1
    assert store.payments[second.id].processing_attempts == 0
    gateway.release.set()
    assert (await next_delivery).decision == Decision.ACK
    assert gateway.calls == 2
    await supervisor.close()


async def test_unavailable_broker_does_not_begin_attempt(setup):
    store, clock, factory, data = setup
    payment = await CreatePayment(factory, clock).execute("wait", data)
    gateway = GatedGateway()
    available = AsyncMock(side_effect=[False, False, True])
    supervisor = ProcessingSupervisor(
        ProcessPayment(factory, clock, gateway, Webhook()), clock, available
    )
    delivery = asyncio.create_task(supervisor.execute(payment.id))
    await gateway.entered.wait()
    assert clock.sleeps == [1, 1]
    assert store.payments[payment.id].processing_attempts == 1
    gateway.release.set()
    await delivery
    await supervisor.close()


async def test_orphaned_failed_task_is_observed_and_released_on_shutdown(setup):
    _, clock, _, _ = setup
    entered, release = asyncio.Event(), asyncio.Event()

    class BrokenProcess:
        async def execute(self, payment_id):
            entered.set()
            await release.wait()
            raise RuntimeError("broken invariant")

    supervisor = ProcessingSupervisor(BrokenProcess(), clock, AsyncMock(return_value=True))
    delivery = asyncio.create_task(supervisor.execute(uuid4()))
    await entered.wait()
    delivery.cancel()
    with pytest.raises(asyncio.CancelledError):
        await delivery
    release.set()
    await asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="broken invariant"):
        await supervisor.close()
    assert supervisor._task is None
    assert not any(task.get_name() == "payment-processing" for task in asyncio.all_tasks())
