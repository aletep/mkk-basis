import asyncio
import os
from uuid import uuid4

import asyncpg
import httpx
import pytest
from asgi_lifespan import LifespanManager
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from mkk_basis.adapters.external import WebhookPolicy
from mkk_basis.adapters.http import build_api
from mkk_basis.adapters.postgres import PostgresUow
from mkk_basis.application.errors import DependencyUnavailable
from mkk_basis.application.payments import CreatePayment, GetPayment
from mkk_basis.bootstrap.config import Settings
from tests.fakes import FakeClock
from tests.test_integration import pg as pg

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="isolated Compose only"),
]


async def test_backend_termination_during_uow_cleanup_is_classified(pg):
    engine, _, factory = pg
    with pytest.raises(DependencyUnavailable) as caught:
        async with factory() as tx:
            pid = await tx.session.scalar(text("SELECT pg_backend_pid()"))
            raw = await (await tx.session.connection()).get_raw_connection()
            driver = raw.driver_connection
            terminated = asyncio.Event()
            driver.add_termination_listener(lambda connection: terminated.set())
            async with engine.connect() as terminator:
                assert await terminator.scalar(
                    text("SELECT pg_terminate_backend(:pid)"), {"pid": pid}
                )
            async with asyncio.timeout(5):
                await terminated.wait()
    assert caught.value.__cause__ is not None
    # The failed transaction does not poison subsequent units of work.
    async with factory() as tx:
        assert await tx.session.scalar(text("SELECT 1")) == 1


async def test_direct_asyncpg_connection_limit_becomes_503_and_recovers(pg):
    engine, _, _ = pg
    role = "mkk_test_limit_" + uuid4().hex
    password = uuid4().hex
    # Both identifiers and literals are generated hex, never caller-controlled SQL.
    async with engine.begin() as connection:
        await connection.execute(
            text(f"CREATE ROLE {role} LOGIN PASSWORD '{password}' CONNECTION LIMIT 0")
        )
    url = make_url(Settings().database_url.get_secret_value()).set(username=role, password=password)
    limited = create_async_engine(
        url, poolclass=NullPool, hide_parameters=True, connect_args={"timeout": 5}
    )
    sessions = async_sessionmaker(limited, expire_on_commit=False)

    def factory():
        return PostgresUow(sessions)

    try:
        with pytest.raises(DependencyUnavailable) as caught:
            async with factory() as tx:
                await tx.payments.get(uuid4())
        assert isinstance(caught.value.__cause__, asyncpg.TooManyConnectionsError)
        clock = FakeClock()
        app = build_api(
            CreatePayment(factory, clock),
            GetPayment(factory),
            limited,
            "test-secret",
            WebhookPolicy(["http://receiver"]),
        )
        async with LifespanManager(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.get(
                    f"/api/v1/payments/{uuid4()}", headers={"X-API-Key": "test-secret"}
                )
                assert response.status_code == 503
                assert response.json() == {"detail": "Database unavailable"}
        async with engine.begin() as connection:
            await connection.execute(text(f"ALTER ROLE {role} CONNECTION LIMIT 1"))
        async with factory() as tx:
            assert await tx.session.scalar(text("SELECT 1")) == 1
    finally:
        await limited.dispose()
        async with engine.begin() as connection:
            await connection.execute(text(f"DROP ROLE {role}"))
