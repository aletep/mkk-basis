import asyncio
from urllib.parse import quote, unquote, urlsplit

import aio_pika
import httpx

from mkk_basis.adapters.external import SystemClock
from mkk_basis.adapters.rabbit import DLQ, EXCHANGE, QUEUE, RabbitPublisher, register_consumer
from mkk_basis.application.outbox import PublishOutbox
from mkk_basis.application.payments import CreatePayment, GetPayment, ProcessPayment
from mkk_basis.bootstrap.config import Settings
from tests.fakes import Webhook
from tests.test_integration import broker as broker
from tests.test_integration import data as data
from tests.test_integration import pg as pg
from tests.test_integration import pytestmark as pytestmark
from tests.test_integration import receive
from tests.test_messaging import GatedGateway


async def test_malformed_messages_dead_letter_and_next_payment_runs(pg, broker, data):
    clock = SystemClock()
    payment = await CreatePayment(pg[2], clock).execute("after-malformed", data)
    gateway, webhook = GatedGateway(), Webhook()
    gateway.release.set()
    ready = asyncio.Event()
    ready.set()
    supervisor = register_consumer(
        broker, ProcessPayment(pg[2], clock, gateway, webhook), clock, ready
    )
    await broker.start()
    malformed = [
        b"{",
        b'"\xff"',
        b'{"unexpected":true}',
        b'{"version":2,"payment_id":"bad"}',
        b"[" * 2000 + b"0" + b"]" * 2000,
    ]
    try:
        for body in malformed:
            await broker.publish(
                aio_pika.Message(body, content_type="application/json", delivery_mode=2),
                exchange=EXCHANGE,
                routing_key="payments.new",
            )
        await PublishOutbox(pg[2], clock, RabbitPublisher(broker)).execute()
        async with asyncio.timeout(15):
            while True:
                if (await GetPayment(pg[2]).execute(payment.id)).webhook_delivered_at:
                    break
                await asyncio.sleep(0.05)
        dead = await broker.declare_queue(DLQ)
        bodies = []
        for _ in malformed:
            message = await receive(dead)
            bodies.append(message.body)
            await message.ack()
        assert sorted(bodies) == sorted(malformed)
        assert gateway.calls == 1 and len(webhook.calls) == 1
        await assert_queue_idle()
    finally:
        await supervisor.close()


def management():
    url = urlsplit(Settings().amqp_url.get_secret_value())
    return httpx.AsyncClient(
        base_url=f"http://{url.hostname}:15672",
        auth=(unquote(url.username), unquote(url.password)),
        timeout=10,
        trust_env=False,
    ), unquote(url.path[1:] or "/")


async def assert_queue_idle():
    client, vhost = management()
    async with client:
        async with asyncio.timeout(15):
            while True:
                response = await client.get("/api/queues")
                response.raise_for_status()
                queue = next(
                    q for q in response.json() if q["vhost"] == vhost and q["name"] == QUEUE.name
                )
                if queue.get("messages_ready", -1) == queue.get("messages_unacknowledged", -1) == 0:
                    return
                await asyncio.sleep(0.1)


async def test_three_real_amqp_disconnects_rejoin_one_durable_attempt(pg, broker, data):
    clock = SystemClock()
    payment = await CreatePayment(pg[2], clock).execute("live-reconnect", data)
    gateway, webhook = GatedGateway(), Webhook()
    ready = asyncio.Event()
    ready.set()
    supervisor = register_consumer(
        broker, ProcessPayment(pg[2], clock, gateway, webhook), clock, ready
    )
    deliveries = asyncio.Queue()
    original = supervisor.execute

    async def observe(payment_id):
        deliveries.put_nowait(payment_id)
        return await original(payment_id)

    supervisor.execute = observe
    await broker.start()
    await PublishOutbox(pg[2], clock, RabbitPublisher(broker)).execute()
    try:
        async with asyncio.timeout(45):
            assert await deliveries.get() == payment.id
            await gateway.entered.wait()
            client, vhost = management()
            async with client:
                closed_names = set()
                for _ in range(3):
                    while True:
                        response = await client.get("/api/connections")
                        response.raise_for_status()
                        connections = [
                            c
                            for c in response.json()
                            if c["vhost"] == vhost and c["name"] not in closed_names
                        ]
                        # Management statistics lag connection open/close events.
                        if len(connections) != 1:
                            await asyncio.sleep(0.1)
                            continue
                        name = connections[0]["name"]
                        closed = await client.delete("/api/connections/" + quote(name, safe=""))
                        closed_names.add(name)
                        if closed.status_code == 404:
                            continue
                        assert closed.status_code == 204
                        break
                    assert await deliveries.get() == payment.id
                    saved = await GetPayment(pg[2]).execute(payment.id)
                    assert saved.processing_attempts == 1 and not saved.exhausted_at
                    assert gateway.calls == 1 and not webhook.calls
            gateway.release.set()
            while True:
                if (await GetPayment(pg[2]).execute(payment.id)).webhook_delivered_at:
                    break
                await asyncio.sleep(0.05)
            saved = await GetPayment(pg[2]).execute(payment.id)
            assert saved.processing_attempts == 1
            assert gateway.calls == len(webhook.calls) == 1
            assert await (await broker.declare_queue(DLQ)).get(fail=False) is None
            await assert_queue_idle()
    finally:
        await supervisor.close()
