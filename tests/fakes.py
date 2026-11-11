import asyncio
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import UUID

from mkk_basis.application.errors import DuplicateKey, TransientProcessingError
from mkk_basis.domain.models import Notification, OutboxEvent, Payment, Status


class FakeClock:
    def __init__(self):
        self.value = datetime(2026, 11, 10, tzinfo=UTC)
        self.sleeps = []

    def now(self):
        return self.value

    async def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.value += timedelta(seconds=max(0, seconds))
        await asyncio.sleep(0)


@dataclass
class Store:
    payments: dict[UUID, Payment] = field(default_factory=dict)
    events: dict[UUID, OutboxEvent] = field(default_factory=dict)
    mutex: asyncio.Lock = field(default_factory=asyncio.Lock)
    active: bool = False


class FakePayments:
    def __init__(self, data):
        self.data = data

    async def add(self, payment):
        if any(p.idempotency_key == payment.idempotency_key for p in self.data.values()):
            raise DuplicateKey
        self.data[payment.id] = deepcopy(payment)

    async def by_key(self, key):
        return next((deepcopy(p) for p in self.data.values() if p.idempotency_key == key), None)

    async def get(self, payment_id, *, lock=False):
        return deepcopy(self.data.get(payment_id))

    async def save(self, payment):
        self.data[payment.id] = deepcopy(payment)


class FakeOutbox:
    def __init__(self, data):
        self.data = data

    async def add(self, event):
        self.data[event.id] = deepcopy(event)

    async def claim(self, now, until, token):
        for event in self.data.values():
            if (
                event.published_at is None
                and (event.next_attempt_at is None or event.next_attempt_at <= now)
                and (event.locked_until is None or event.locked_until <= now)
            ):
                event.lock_token, event.locked_until = token, until
                event.attempts += 1
                return deepcopy(event)
        return None

    async def published(self, event_id, token, now):
        event = self.data[event_id]
        if event.lock_token != token or event.published_at:
            return False
        event.published_at, event.lock_token, event.locked_until = now, None, None
        return True

    async def failed(self, event_id, token, retry_at, error):
        event = self.data[event_id]
        if event.lock_token == token and event.published_at is None:
            event.next_attempt_at, event.last_error = retry_at, error
            event.lock_token, event.locked_until = None, None


class FakeUow:
    def __init__(self, store):
        self.store = store

    async def __aenter__(self):
        await self.store.mutex.acquire()
        self.store.active = True
        self.payments = FakePayments(deepcopy(self.store.payments))
        self.outbox = FakeOutbox(deepcopy(self.store.events))
        return self

    async def __aexit__(self, *args):
        self.store.active = False
        self.store.mutex.release()

    async def commit(self):
        self.store.payments = deepcopy(self.payments.data)
        self.store.events = deepcopy(self.outbox.data)


class Gateway:
    def __init__(self, status=Status.SUCCEEDED, store=None):
        self.status, self.store, self.calls = status, store, 0

    async def process(self, payment_id):
        if self.store:
            assert not self.store.active
        self.calls += 1
        return self.status


class Webhook:
    def __init__(self, failures=0, store=None):
        self.failures, self.store = failures, store
        self.calls: list[Notification] = []

    async def send(self, url, notification):
        if self.store:
            assert not self.store.active
        self.calls.append(notification)
        if len(self.calls) <= self.failures:
            raise TransientProcessingError("webhook_http_500")
