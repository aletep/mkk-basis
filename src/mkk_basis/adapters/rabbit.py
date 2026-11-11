import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from uuid import UUID

from aio_pika.exceptions import AMQPException, ChannelInvalidStateError
from faststream import AckPolicy
from faststream.rabbit import (
    Channel,
    ExchangeType,
    QueueType,
    RabbitBroker,
    RabbitExchange,
    RabbitMessage,
    RabbitQueue,
)
from pamqp.commands import Basic
from pydantic import BaseModel, ConfigDict, ValidationError

from mkk_basis.adapters.processing import ProcessingSupervisor
from mkk_basis.application.errors import DependencyUnavailable, PublicationFailed
from mkk_basis.application.payments import Decision, ProcessPayment
from mkk_basis.application.ports import Clock
from mkk_basis.domain.models import OutboxEvent

logger = logging.getLogger(__name__)

EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
DLX = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)
QUEUE = RabbitQueue(
    "payments.new",
    queue_type=QueueType.QUORUM,
    durable=True,
    routing_key="payments.new",
    arguments={
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "payments.failed",
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    },
)
DLQ = RabbitQueue("payments.dlq", queue_type=QueueType.QUORUM, durable=True)


def make_broker(url: str) -> RabbitBroker:
    return RabbitBroker(
        url,
        default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
        graceful_timeout=30,
        reconnect_interval=1,
        timeout=5,
    )


async def declare_topology(broker: RabbitBroker) -> None:
    exchange = await broker.declare_exchange(EXCHANGE)
    dead_exchange = await broker.declare_exchange(DLX)
    dead_queue = await broker.declare_queue(DLQ)
    await dead_queue.bind(dead_exchange, routing_key="payments.failed")
    queue = await broker.declare_queue(QUEUE)
    await queue.bind(exchange, routing_key="payments.new")


class RabbitPublisher:
    def __init__(self, broker: RabbitBroker, routing_key: str = "payments.new") -> None:
        self.broker, self.routing_key = broker, routing_key

    async def publish(self, event: OutboxEvent) -> None:
        try:
            async with asyncio.timeout(5):
                confirmation = await self.broker.publish(
                    event.payload,
                    exchange=EXCHANGE,
                    routing_key=self.routing_key,
                    mandatory=True,
                    persist=True,
                    timeout=5,
                    message_id=str(event.id),
                    message_type=event.event_type,
                )
            if not isinstance(confirmation, Basic.Ack):
                raise PublicationFailed("publish_not_confirmed")
        except (AMQPException, ChannelInvalidStateError, OSError, TimeoutError):
            raise PublicationFailed("publish_failed_or_unroutable") from None


class PaymentEventDto(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: int
    payment_id: UUID


def register_consumer(
    broker: RabbitBroker,
    process: ProcessPayment,
    clock: Clock,
    ready: asyncio.Event,
    on_dependency_wait: Callable[[], Awaitable[None]] | None = None,
) -> ProcessingSupervisor:
    async def available() -> bool:
        return await broker.ping(timeout=1)

    supervisor = ProcessingSupervisor(process, clock, available)

    async def raw_decoder(message: RabbitMessage) -> bytes:
        # Decode inside the delivery boundary so malformed JSON is also rejected.
        return message.body

    @broker.subscriber(QUEUE, EXCHANGE, ack_policy=AckPolicy.MANUAL, decoder=raw_decoder)
    async def handle(body: object, message: RabbitMessage) -> None:
        await ready.wait()
        try:
            event = PaymentEventDto.model_validate(json.loads(message.body))
            if event.version != 1:
                raise ValueError("unsupported_event_version")
        except (ValidationError, ValueError, UnicodeError, RecursionError):
            logger.warning("invalid_payment_event", extra={"event_id": message.message_id})
            await message.reject(requeue=False)
            return
        while True:
            if not await broker.ping(timeout=1):
                await clock.sleep(1)
                continue
            try:
                result = await supervisor.execute(event.payment_id)
            except DependencyUnavailable:
                if on_dependency_wait:
                    await on_dependency_wait()
                await clock.sleep(1)
                continue
            except Exception:
                # Unexpected programming errors are not blindly retried or acknowledged.
                logger.exception(
                    "unexpected_processing_error", extra={"payment_id": str(event.payment_id)}
                )
                await message.reject(requeue=False)
                return
            logger.info(
                "payment_processing",
                extra={
                    "payment_id": str(event.payment_id),
                    "event_id": message.message_id,
                    "decision": result.decision.value,
                },
            )
            if result.decision == Decision.ACK:
                await message.ack()
                return
            if result.decision == Decision.DEAD_LETTER:
                await message.reject(requeue=False)
                return
            if result.retry_at:
                await clock.sleep((result.retry_at - clock.now()).total_seconds())

    return supervisor
