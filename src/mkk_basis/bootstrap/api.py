from fastapi import FastAPI

from mkk_basis.adapters.external import SystemClock, WebhookPolicy
from mkk_basis.adapters.http import build_api
from mkk_basis.adapters.postgres import PostgresUow
from mkk_basis.application.payments import CreatePayment, GetPayment
from mkk_basis.bootstrap.config import Settings
from mkk_basis.bootstrap.resources import configure_logging, database


def create_app() -> FastAPI:
    configure_logging()
    settings = Settings()
    engine, sessions = database(settings)
    return build_api(
        CreatePayment(lambda: PostgresUow(sessions), SystemClock()),
        GetPayment(lambda: PostgresUow(sessions)),
        engine,
        settings.api_key.get_secret_value(),
        WebhookPolicy(settings.webhook_allowed_origins),
    )
