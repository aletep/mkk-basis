import asyncio
import logging
from contextlib import suppress

import httpx
from faststream import FastStream

from mkk_basis.adapters.external import (
    HttpWebhookSender,
    SimulatedGateway,
    SystemClock,
    WebhookPolicy,
)
from mkk_basis.adapters.postgres import PostgresUow
from mkk_basis.adapters.rabbit import (
    RabbitPublisher,
    declare_topology,
    make_broker,
    register_consumer,
)
from mkk_basis.application.errors import DependencyUnavailable
from mkk_basis.application.outbox import PublishOutbox
from mkk_basis.application.payments import ProcessPayment
from mkk_basis.bootstrap.config import Settings
from mkk_basis.bootstrap.resources import configure_logging, database

logger = logging.getLogger(__name__)


async def main() -> None:
    configure_logging()
    settings = Settings()
    engine, sessions = database(settings)
    clock = SystemClock()
    broker = make_broker(settings.amqp_url.get_secret_value())
    ready = asyncio.Event()
    relay_task: asyncio.Task[None] | None = None

    async with httpx.AsyncClient(timeout=5, trust_env=False) as client:
        process = ProcessPayment(
            lambda: PostgresUow(sessions),
            clock,
            SimulatedGateway(),
            HttpWebhookSender(client, WebhookPolicy(settings.webhook_allowed_origins)),
        )
        processing = register_consumer(broker, process, clock, ready)
        relay = PublishOutbox(lambda: PostgresUow(sessions), clock, RabbitPublisher(broker))
        app = FastStream(broker)

        async def relay_loop() -> None:
            while True:
                try:
                    worked = await relay.execute()
                except DependencyUnavailable:
                    await clock.sleep(1)
                else:
                    if not worked:
                        await clock.sleep(0.5)

        @app.after_startup
        async def start_relay() -> None:
            nonlocal relay_task
            await declare_topology(broker)
            ready.set()
            relay_task = asyncio.create_task(relay_loop(), name="outbox-relay")
            # A programming error in the relay must make the process fail visibly.
            relay_task.add_done_callback(lambda task: app.exit() if not task.cancelled() else None)

        @app.on_shutdown
        async def stop_relay() -> None:
            try:
                await processing.close()
            finally:
                if relay_task:
                    relay_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await relay_task

        try:
            await app.run()
        finally:
            await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
