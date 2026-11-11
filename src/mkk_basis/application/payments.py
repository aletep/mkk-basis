from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from uuid import UUID, uuid4, uuid5

from mkk_basis.application.errors import (
    DependencyUnavailable,
    DuplicateKey,
    IdempotencyConflict,
    PaymentNotFound,
    PermanentProcessingError,
    TransientProcessingError,
)
from mkk_basis.application.ports import Clock, PaymentGateway, UowFactory, WebhookSender
from mkk_basis.domain.models import Notification, OutboxEvent, Payment, PaymentInput, Status

type CallableOperation[T] = Callable[[], Awaitable[T]]


def request_fingerprint(data: PaymentInput) -> str:
    payload = {
        "amount": format(data.amount, ".2f"),
        "currency": data.currency.value,
        "description": data.description,
        "metadata": data.metadata,
        "webhook_url": data.webhook_url,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


class CreatePayment:
    def __init__(self, uow: UowFactory, clock: Clock) -> None:
        self.uow, self.clock = uow, clock

    async def execute(self, key: str, data: PaymentInput) -> Payment:
        fingerprint = request_fingerprint(data)
        payment = Payment(uuid4(), data, key, fingerprint, self.clock.now())
        try:
            async with self.uow() as tx:
                existing = await tx.payments.by_key(key)
                if existing:
                    return self._matching(existing, fingerprint)
                await tx.payments.add(payment)
                await tx.outbox.add(
                    OutboxEvent(
                        uuid4(),
                        payment.id,
                        payment.created_at,
                        {"version": 1, "payment_id": str(payment.id)},
                    )
                )
                await tx.commit()
            return payment
        except DuplicateKey:
            # A new transaction is essential after a concurrent unique-key conflict.
            async with self.uow() as tx:
                existing = await tx.payments.by_key(key)
                if existing is None:
                    raise DependencyUnavailable("idempotency_conflict_not_visible") from None
                return self._matching(existing, fingerprint)

    @staticmethod
    def _matching(payment: Payment, fingerprint: str) -> Payment:
        if payment.fingerprint != fingerprint:
            raise IdempotencyConflict
        return payment


class GetPayment:
    def __init__(self, uow: UowFactory) -> None:
        self.uow = uow

    async def execute(self, payment_id: UUID) -> Payment:
        async with self.uow() as tx:
            payment = await tx.payments.get(payment_id)
            if payment is None:
                raise PaymentNotFound
            return payment


class Decision(StrEnum):
    ACK = "ack"
    RETRY = "retry"
    DEAD_LETTER = "dead_letter"


@dataclass(frozen=True)
class ProcessingResult:
    decision: Decision
    retry_at: datetime | None = None


class ProcessPayment:
    def __init__(
        self, uow: UowFactory, clock: Clock, gateway: PaymentGateway, webhook: WebhookSender
    ) -> None:
        self.uow, self.clock, self.gateway, self.webhook = uow, clock, gateway, webhook

    async def _available[T](self, operation: CallableOperation[T]) -> T:
        # Retrying a DB operation does not repeat gateway/HTTP side effects.
        while True:
            try:
                return await operation()
            except DependencyUnavailable:
                await self.clock.sleep(1)

    async def execute(self, payment_id: UUID) -> ProcessingResult:
        token = uuid4()
        payment = await self._available(lambda: self._start(payment_id, token))
        if isinstance(payment, ProcessingResult):
            return payment
        try:
            if payment.status == Status.PENDING:
                status = await self.gateway.process(payment_id)
                if status == Status.PENDING:
                    raise PermanentProcessingError("invalid_gateway_result")
                payment = await self._available(lambda: self._complete(payment_id, status))
            if payment.processed_at is None:
                raise PermanentProcessingError("missing_processed_at")
            notification = Notification(
                uuid5(payment.id, "payment.completed"),
                payment.id,
                payment.status,
                payment.data.amount,
                payment.data.currency,
                payment.processed_at,
            )
            await self.webhook.send(payment.data.webhook_url, notification)
            await self._available(lambda: self._delivered(payment_id))
            return ProcessingResult(Decision.ACK)
        except TransientProcessingError as exc:
            error = str(exc)
            return await self._available(lambda: self._failure(payment_id, error, False))
        except PermanentProcessingError as exc:
            error = str(exc)
            return await self._available(lambda: self._failure(payment_id, error, True))

    async def _start(self, payment_id: UUID, token: UUID) -> Payment | ProcessingResult:
        async with self.uow() as tx:
            payment = await tx.payments.get(payment_id, lock=True)
            if payment is None:
                return ProcessingResult(Decision.DEAD_LETTER)
            if payment.webhook_delivered_at:
                return ProcessingResult(Decision.ACK)
            # The commit may have succeeded before the connection was lost.
            # A retry within this invocation resumes the same begun attempt.
            if payment.processing_token == token:
                return payment
            now = self.clock.now()
            if payment.exhausted_at or payment.processing_attempts >= 3:
                payment.exhausted_at = payment.exhausted_at or now
                payment.last_error = payment.last_error or "attempt_interrupted"
                await tx.payments.save(payment)
                await tx.commit()
                return ProcessingResult(Decision.DEAD_LETTER)
            if payment.next_attempt_at and payment.next_attempt_at > now:
                return ProcessingResult(Decision.RETRY, payment.next_attempt_at)
            payment.processing_attempts += 1
            payment.processing_token = token
            # Also provides a restart delay if the process dies during this attempt.
            payment.next_attempt_at = now + timedelta(
                seconds=2 ** (payment.processing_attempts - 1)
            )
            await tx.payments.save(payment)
            await tx.commit()
            return payment

    async def _complete(self, payment_id: UUID, status: Status) -> Payment:
        async with self.uow() as tx:
            payment = await tx.payments.get(payment_id, lock=True)
            if payment is None:
                raise PermanentProcessingError("payment_missing")
            if payment.complete(status, self.clock.now()):
                await tx.payments.save(payment)
                await tx.commit()
            return payment

    async def _delivered(self, payment_id: UUID) -> None:
        async with self.uow() as tx:
            payment = await tx.payments.get(payment_id, lock=True)
            if payment is None:
                raise PermanentProcessingError("payment_missing")
            payment.webhook_delivered_at = payment.webhook_delivered_at or self.clock.now()
            payment.next_attempt_at = None
            payment.last_error = None
            await tx.payments.save(payment)
            await tx.commit()

    async def _failure(self, payment_id: UUID, error: str, permanent: bool) -> ProcessingResult:
        async with self.uow() as tx:
            payment = await tx.payments.get(payment_id, lock=True)
            if payment is None:
                return ProcessingResult(Decision.DEAD_LETTER)
            if payment.webhook_delivered_at:
                return ProcessingResult(Decision.ACK)
            payment.last_error = error[:200]
            if permanent or payment.processing_attempts >= 3:
                payment.exhausted_at, payment.next_attempt_at = self.clock.now(), None
                result = ProcessingResult(Decision.DEAD_LETTER)
            else:
                payment.next_attempt_at = self.clock.now() + timedelta(
                    seconds=2 ** (payment.processing_attempts - 1)
                )
                result = ProcessingResult(Decision.RETRY, payment.next_attempt_at)
            await tx.payments.save(payment)
            await tx.commit()
            return result
