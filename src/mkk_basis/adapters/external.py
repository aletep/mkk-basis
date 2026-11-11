import asyncio
import hashlib
import random
from datetime import UTC, datetime
from urllib.parse import urlsplit
from uuid import UUID

import httpx

from mkk_basis.application.errors import PermanentProcessingError, TransientProcessingError
from mkk_basis.domain.models import Notification, Status


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(max(0, seconds))


class SimulatedGateway:
    async def process(self, payment_id: UUID) -> Status:
        await asyncio.sleep(random.uniform(2, 5))
        bucket = int.from_bytes(hashlib.sha256(payment_id.bytes).digest()[:8]) % 10
        return Status.FAILED if bucket == 0 else Status.SUCCEEDED


def origin(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username is not None:
        raise ValueError("Invalid webhook URL")
    if parsed.password or parsed.fragment:
        raise ValueError("Credentials and fragments are not allowed")
    port = parsed.port if parsed.port is not None else (443 if parsed.scheme == "https" else 80)
    return f"{parsed.scheme}://{parsed.hostname.lower()}:{port}"


class WebhookPolicy:
    def __init__(self, allowed_origins: list[str]) -> None:
        self.allowed = frozenset(origin(value) for value in allowed_origins)

    def validate(self, url: str) -> str:
        if origin(url) not in self.allowed:
            raise ValueError("Webhook origin is not allowed")
        return url


class HttpWebhookSender:
    def __init__(self, client: httpx.AsyncClient, policy: WebhookPolicy) -> None:
        self.client, self.policy = client, policy

    async def send(self, url: str, notification: Notification) -> None:
        try:
            self.policy.validate(url)
        except ValueError:
            raise PermanentProcessingError("webhook_origin_forbidden") from None
        payload = {
            "event_id": str(notification.event_id),
            "event_type": notification.event_type,
            "payment_id": str(notification.payment_id),
            "status": notification.status.value,
            "amount": format(notification.amount, ".2f"),
            "currency": notification.currency.value,
            "processed_at": notification.processed_at.isoformat(),
        }
        try:
            # HTTPX timeouts are per I/O operation; asyncio.timeout is the total deadline.
            async with asyncio.timeout(5):
                async with self.client.stream(
                    "POST",
                    url,
                    json=payload,
                    headers={"Idempotency-Key": str(notification.event_id)},
                    follow_redirects=False,
                ) as response:
                    if not 200 <= response.status_code < 300:
                        raise TransientProcessingError(f"webhook_http_{response.status_code}")
        except (httpx.RequestError, TimeoutError):
            raise TransientProcessingError("webhook_transport_error") from None
