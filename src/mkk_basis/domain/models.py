from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]


class Status(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


@dataclass(frozen=True)
class PaymentInput:
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    webhook_url: str

    def __post_init__(self) -> None:
        if (
            not self.amount.is_finite()
            or self.amount <= 0
            or self.amount >= Decimal("10000000000000000")
            or self.amount != self.amount.quantize(Decimal("0.01"))
        ):
            raise ValueError(
                "Amount must be positive, fit NUMERIC(18,2), and have at most 2 decimals"
            )


@dataclass
class Payment:
    id: UUID
    data: PaymentInput
    idempotency_key: str
    fingerprint: str
    created_at: datetime
    status: Status = Status.PENDING
    processed_at: datetime | None = None
    processing_attempts: int = 0
    processing_token: UUID | None = None
    next_attempt_at: datetime | None = None
    last_error: str | None = None
    exhausted_at: datetime | None = None
    webhook_delivered_at: datetime | None = None

    def complete(self, status: Status, processed_at: datetime) -> bool:
        if status == Status.PENDING:
            raise ValueError("A completed payment needs a terminal status")
        if self.status != Status.PENDING:
            return False
        self.status, self.processed_at = status, processed_at
        return True


@dataclass
class OutboxEvent:
    id: UUID
    payment_id: UUID
    created_at: datetime
    payload: dict[str, JsonValue]
    event_type: str = "payment.created"
    published_at: datetime | None = None
    attempts: int = 0
    next_attempt_at: datetime | None = None
    lock_token: UUID | None = None
    locked_until: datetime | None = None
    last_error: str | None = None


@dataclass(frozen=True)
class Notification:
    event_id: UUID
    payment_id: UUID
    status: Status
    amount: Decimal
    currency: Currency
    processed_at: datetime
    event_type: str = field(default="payment.completed", init=False)
