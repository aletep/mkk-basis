import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, JsonValue, ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from mkk_basis.adapters.external import WebhookPolicy
from mkk_basis.adapters.json import InvalidJsonPayload, decode_payment_json
from mkk_basis.application.errors import DependencyUnavailable, IdempotencyConflict, PaymentNotFound
from mkk_basis.application.payments import CreatePayment, GetPayment
from mkk_basis.domain.models import Currency, Payment, PaymentInput, Status


class CreateDto(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str = Field(max_length=1000)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: HttpUrl = Field(max_length=2048)


async def parse_create_request(request: Request) -> CreateDto:
    """Validate the raw JSON without FastAPI's intermediate float conversion."""
    try:
        return CreateDto.model_validate(decode_payment_json(await request.body()))
    except InvalidJsonPayload as exc:
        raise HTTPException(422, str(exc)) from None
    except ValidationError as exc:
        # Validation inputs/context can contain non-JSON values or sensitive payloads.
        raise HTTPException(
            422, exc.errors(include_input=False, include_context=False, include_url=False)
        ) from None


class AcceptedDto(BaseModel):
    payment_id: UUID
    status: Status
    created_at: datetime


class PaymentDto(AcceptedDto):
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    webhook_url: str
    processed_at: datetime | None
    processing_attempts: int
    processing_exhausted: bool
    webhook_delivered_at: datetime | None

    @classmethod
    def from_payment(cls, payment: Payment) -> "PaymentDto":
        return cls(
            payment_id=payment.id,
            status=payment.status,
            created_at=payment.created_at,
            amount=payment.data.amount,
            currency=payment.data.currency,
            description=payment.data.description,
            metadata=payment.data.metadata,
            webhook_url=payment.data.webhook_url,
            processed_at=payment.processed_at,
            processing_attempts=payment.processing_attempts,
            processing_exhausted=payment.exhausted_at is not None,
            webhook_delivered_at=payment.webhook_delivered_at,
        )


def build_api(
    create: CreatePayment,
    get: GetPayment,
    engine: AsyncEngine,
    api_key: str,
    policy: WebhookPolicy,
) -> FastAPI:
    async def authenticate(x_api_key: Annotated[str | None, Header()] = None) -> None:
        if x_api_key is None or not secrets.compare_digest(x_api_key.encode(), api_key.encode()):
            raise HTTPException(401, "Invalid API key")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield
        await engine.dispose()

    app = FastAPI(
        title="MKК Basis payments",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        dependencies=[Depends(authenticate)],
        lifespan=lifespan,
    )

    @app.post("/api/v1/payments", status_code=202, response_model=AcceptedDto)
    async def post_payment(
        body: Annotated[CreateDto, Depends(parse_create_request)],
        idempotency_key: Annotated[str, Header(min_length=1, max_length=200)],
    ) -> AcceptedDto:
        if not idempotency_key.strip():
            raise HTTPException(422, "Idempotency-Key cannot be blank")
        try:
            url = policy.validate(str(body.webhook_url))
            data = PaymentInput(body.amount, body.currency, body.description, body.metadata, url)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from None
        try:
            payment = await create.execute(idempotency_key, data)
        except IdempotencyConflict:
            raise HTTPException(
                409, "Idempotency-Key is already used with a different request"
            ) from None
        except DependencyUnavailable:
            raise HTTPException(503, "Database unavailable") from None
        return AcceptedDto(
            payment_id=payment.id, status=payment.status, created_at=payment.created_at
        )

    @app.get("/api/v1/payments/{payment_id}", response_model=PaymentDto)
    async def get_payment(payment_id: UUID) -> PaymentDto:
        try:
            return PaymentDto.from_payment(await get.execute(payment_id))
        except PaymentNotFound:
            raise HTTPException(404, "Payment not found") from None
        except DependencyUnavailable:
            raise HTTPException(503, "Database unavailable") from None

    @app.get("/health", include_in_schema=False)
    async def health() -> dict[str, str]:
        try:
            async with engine.connect() as connection:
                await connection.execute(text("SELECT 1 FROM payments LIMIT 1"))
        except Exception:
            raise HTTPException(503, "Database unavailable or migrations incomplete") from None
        return {"status": "ok"}

    return app
