from __future__ import annotations

import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Self
from uuid import UUID

from asyncpg import PostgresError  # type: ignore[import-untyped]
from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    Uuid,
    or_,
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.exc import TimeoutError as PoolTimeout
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from mkk_basis.application.errors import DependencyUnavailable, DuplicateKey
from mkk_basis.domain.models import (
    Currency,
    JsonValue,
    OutboxEvent,
    Payment,
    PaymentInput,
    Status,
)


class Base(DeclarativeBase):
    pass


class PaymentRow(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_payments_amount"),
        CheckConstraint("currency IN ('RUB','USD','EUR')", name="ck_payments_currency"),
        CheckConstraint("status IN ('pending','succeeded','failed')", name="ck_payments_status"),
        CheckConstraint("processing_attempts BETWEEN 0 AND 3", name="ck_payments_attempts"),
        CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL) OR "
            "(status <> 'pending' AND processed_at IS NOT NULL)",
            name="ck_payments_result",
        ),
    )
    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str] = mapped_column(String(1000))
    metadata_: Mapped[dict[str, JsonValue]] = mapped_column("metadata", JSONB)
    webhook_url: Mapped[str] = mapped_column(String(2048))
    idempotency_key: Mapped[str] = mapped_column(String(200), unique=True)
    fingerprint: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(16))
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processing_attempts: Mapped[int] = mapped_column(Integer, default=0)
    processing_token: Mapped[UUID | None] = mapped_column(Uuid)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
    exhausted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OutboxRow(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        Index(
            "ix_outbox_unpublished",
            "next_attempt_at",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
        CheckConstraint("attempts >= 0", name="ck_outbox_attempts"),
        CheckConstraint("(lock_token IS NULL) = (locked_until IS NULL)", name="ck_outbox_lock"),
    )
    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    payment_id: Mapped[UUID] = mapped_column(ForeignKey("payments.id"))
    event_type: Mapped[str] = mapped_column(String(50))
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lock_token: Mapped[UUID | None] = mapped_column(Uuid)
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)


def to_payment(row: PaymentRow) -> Payment:
    return Payment(
        id=row.id,
        data=PaymentInput(
            row.amount, Currency(row.currency), row.description, row.metadata_, row.webhook_url
        ),
        idempotency_key=row.idempotency_key,
        fingerprint=row.fingerprint,
        created_at=row.created_at,
        status=Status(row.status),
        processed_at=row.processed_at,
        processing_attempts=row.processing_attempts,
        processing_token=row.processing_token,
        next_attempt_at=row.next_attempt_at,
        last_error=row.last_error,
        exhausted_at=row.exhausted_at,
        webhook_delivered_at=row.webhook_delivered_at,
    )


class PostgresPayments:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def add(self, payment: Payment) -> None:
        self.session.add(
            PaymentRow(
                id=payment.id,
                amount=payment.data.amount,
                currency=payment.data.currency.value,
                description=payment.data.description,
                metadata_=payment.data.metadata,
                webhook_url=payment.data.webhook_url,
                idempotency_key=payment.idempotency_key,
                fingerprint=payment.fingerprint,
                created_at=payment.created_at,
                status=payment.status.value,
                processing_attempts=0,
            )
        )
        try:
            await self.session.flush()
        except IntegrityError as exc:
            original = exc.orig
            if getattr(original, "sqlstate", None) == "23505":
                raise DuplicateKey from None
            raise

    async def by_key(self, key: str) -> Payment | None:
        row = await self.session.scalar(select(PaymentRow).where(PaymentRow.idempotency_key == key))
        return to_payment(row) if row else None

    async def get(self, payment_id: UUID, *, lock: bool = False) -> Payment | None:
        statement = select(PaymentRow).where(PaymentRow.id == payment_id)
        if lock:
            statement = statement.with_for_update()
        row = await self.session.scalar(statement)
        return to_payment(row) if row else None

    async def save(self, payment: Payment) -> None:
        await self.session.execute(
            update(PaymentRow)
            .where(PaymentRow.id == payment.id)
            .values(
                status=payment.status.value,
                processed_at=payment.processed_at,
                processing_attempts=payment.processing_attempts,
                processing_token=payment.processing_token,
                next_attempt_at=payment.next_attempt_at,
                last_error=payment.last_error,
                exhausted_at=payment.exhausted_at,
                webhook_delivered_at=payment.webhook_delivered_at,
            )
        )


class PostgresOutbox:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def add(self, event: OutboxEvent) -> None:
        self.session.add(
            OutboxRow(
                id=event.id,
                payment_id=event.payment_id,
                event_type=event.event_type,
                payload=event.payload,
                created_at=event.created_at,
                attempts=0,
            )
        )
        await self.session.flush()

    async def claim(self, now: datetime, until: datetime, token: UUID) -> OutboxEvent | None:
        row = await self.session.scalar(
            select(OutboxRow)
            .where(
                OutboxRow.published_at.is_(None),
                or_(OutboxRow.next_attempt_at.is_(None), OutboxRow.next_attempt_at <= now),
                or_(OutboxRow.locked_until.is_(None), OutboxRow.locked_until <= now),
            )
            .order_by(OutboxRow.created_at, OutboxRow.id)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if row is None:
            return None
        row.lock_token, row.locked_until = token, until
        row.attempts += 1
        await self.session.flush()
        return OutboxEvent(
            id=row.id,
            payment_id=row.payment_id,
            created_at=row.created_at,
            payload=row.payload,
            event_type=row.event_type,
            attempts=row.attempts,
            lock_token=token,
            locked_until=until,
        )

    async def published(self, event_id: UUID, token: UUID, now: datetime) -> bool:
        result = await self.session.execute(
            update(OutboxRow)
            .where(
                OutboxRow.id == event_id,
                OutboxRow.lock_token == token,
                OutboxRow.published_at.is_(None),
            )
            .values(
                published_at=now,
                lock_token=None,
                locked_until=None,
                last_error=None,
                next_attempt_at=None,
            )
            .returning(OutboxRow.id)
        )
        return result.scalar_one_or_none() is not None

    async def failed(self, event_id: UUID, token: UUID, retry_at: datetime, error: str) -> None:
        await self.session.execute(
            update(OutboxRow)
            .where(
                OutboxRow.id == event_id,
                OutboxRow.lock_token == token,
                OutboxRow.published_at.is_(None),
            )
            .values(next_attempt_at=retry_at, last_error=error, lock_token=None, locked_until=None)
        )


class PostgresUow:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self.sessions = sessions
        self.session: AsyncSession
        self.payments: PostgresPayments
        self.outbox: PostgresOutbox

    async def __aenter__(self) -> Self:
        self.session = self.sessions()
        self.payments, self.outbox = PostgresPayments(self.session), PostgresOutbox(self.session)
        return self

    async def __aexit__(self, *args: object) -> None:
        """Release the session without replacing the operation's original failure.

        SQLAlchemy may raise again while rolling back a disconnected connection.
        That cleanup error must also be classified when it is the only failure.
        Cancellation remains cancellation, even if cleanup fails at the same time.
        """
        original = args[1] if len(args) > 1 and isinstance(args[1], BaseException) else None
        cleanup_error: BaseException | None = None
        try:
            await self.session.rollback()
        except BaseException as exc:
            cleanup_error = exc
        try:
            await self.session.close()
        except BaseException as exc:
            if cleanup_error is None or not isinstance(exc, Exception):
                cleanup_error = exc

        if original is not None and not isinstance(original, Exception):
            # Returning does not suppress the exception supplied by async with.
            return
        if cleanup_error is not None and not isinstance(cleanup_error, Exception):
            raise cleanup_error
        error = original or cleanup_error
        if error is not None and is_transient_database_error(error):
            raise DependencyUnavailable("database_unavailable") from error
        if original is None and cleanup_error is not None:
            raise cleanup_error

    async def commit(self) -> None:
        await self.session.commit()


_TRANSIENT_SQLSTATES = frozenset(
    {
        "08000",  # Connection exception.
        "08001",  # Unable to establish a connection.
        "08003",  # Connection no longer exists.
        "08006",  # Connection failure.
        "08007",  # The commit outcome is unknown; callers must reconcile persisted state.
        "57P01",  # Administrative shutdown.
        "57P02",  # Crash shutdown.
        "57P03",  # Server is starting or recovering.
        "53300",  # Connection capacity temporarily exhausted.
    }
)


def is_transient_database_error(error: BaseException) -> bool:
    """Classify connection availability, not constraints or arbitrary DB failures.

    Connect-time asyncpg errors are not necessarily wrapped in DBAPIError.
    Operation and cleanup errors normally are wrapped by SQLAlchemy's dialect.
    """
    if isinstance(error, (asyncio.CancelledError, IntegrityError)):
        return False
    if isinstance(error, (OSError, TimeoutError, PoolTimeout)):
        return True
    if isinstance(error, DBAPIError):
        return error.connection_invalidated or (
            getattr(error.orig, "sqlstate", None) in _TRANSIENT_SQLSTATES
        )
    return isinstance(error, PostgresError) and error.sqlstate in _TRANSIENT_SQLSTATES
