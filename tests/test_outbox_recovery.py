import os
from unittest.mock import AsyncMock, Mock

import aio_pika
import pytest
from aio_pika.exceptions import ChannelInvalidStateError
from pamqp.commands import Basic
from sqlalchemy import select

from mkk_basis.adapters.postgres import OutboxRow
from mkk_basis.adapters.rabbit import QUEUE, RabbitPublisher
from mkk_basis.application.outbox import PublishOutbox
from mkk_basis.application.payments import CreatePayment
from tests.fakes import FakeClock
from tests.test_integration import broker as broker
from tests.test_integration import data as data
from tests.test_integration import pg as pg
from tests.test_integration import receive

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="isolated Compose only"),
]


@pytest.mark.parametrize("failure", ["nack", "timeout", "closed-channel"])
async def test_publish_failure_stays_pending_then_recovers_with_real_rabbitmq(
    pg, broker, data, failure
):
    _, sessions, factory = pg
    clock = FakeClock()
    payment = await CreatePayment(factory, clock).execute("outbox-recovery", data)
    # Inject a precise transport outcome; recovery uses the actual broker and confirms.
    if failure == "nack":
        failing_broker = Mock(publish=AsyncMock(return_value=Basic.Nack()))
    else:
        error = TimeoutError() if failure == "timeout" else ChannelInvalidStateError("closed")
        failing_broker = Mock(publish=AsyncMock(side_effect=error))
    assert await PublishOutbox(factory, clock, RabbitPublisher(failing_broker)).execute()
    async with sessions() as session:
        pending = await session.scalar(select(OutboxRow))
        event_id = pending.id
        assert pending.payment_id == payment.id and pending.published_at is None
        assert pending.attempts == 1 and pending.last_error
        assert pending.lock_token is None and pending.locked_until is None
        assert pending.next_attempt_at > clock.now()
        clock.value = pending.next_attempt_at
    assert await PublishOutbox(factory, clock, RabbitPublisher(broker)).execute()
    message = await receive(await broker.declare_queue(QUEUE))
    assert message.message_id == str(event_id)
    assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    await message.ack()
    async with sessions() as session:
        recovered = await session.get(OutboxRow, event_id)
        assert recovered.published_at is not None and recovered.attempts == 2
        assert recovered.last_error is None and recovered.next_attempt_at is None
        assert recovered.lock_token is None and recovered.locked_until is None
