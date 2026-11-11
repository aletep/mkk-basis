import asyncio
from unittest.mock import AsyncMock, Mock

import asyncpg
import pytest
from sqlalchemy.exc import DBAPIError, IntegrityError, InterfaceError
from sqlalchemy.exc import TimeoutError as PoolTimeout

from mkk_basis.adapters.postgres import PostgresUow, is_transient_database_error
from mkk_basis.application.errors import DependencyUnavailable


@pytest.mark.parametrize(
    "error",
    [
        asyncpg.ConnectionDoesNotExistError("connection closed"),
        asyncpg.ConnectionFailureError("connection failed"),
        asyncpg.TransactionResolutionUnknownError("unknown commit outcome"),
        asyncpg.AdminShutdownError("server stopped"),
        asyncpg.CrashShutdownError("server crashed"),
        asyncpg.CannotConnectNowError("server recovering"),
        asyncpg.TooManyConnectionsError("connection capacity"),
        ConnectionRefusedError(),
        TimeoutError(),
        PoolTimeout(),
        InterfaceError("SELECT 1", None, RuntimeError(), connection_invalidated=True),
        DBAPIError("SELECT 1", None, asyncpg.TooManyConnectionsError()),
    ],
)
def test_connection_availability_errors_are_retryable(error):
    assert is_transient_database_error(error)


@pytest.mark.parametrize(
    "error",
    [
        asyncpg.InvalidPasswordError(),
        asyncpg.UndefinedTableError(),
        asyncpg.UniqueViolationError(),
        asyncpg.ProtocolViolationError(),
        asyncpg.InterfaceError("another operation is in progress"),
        IntegrityError("INSERT", None, asyncpg.UniqueViolationError()),
        DBAPIError("SELECT", None, asyncpg.UndefinedTableError()),
        RuntimeError("programming error"),
        asyncio.CancelledError(),
    ],
)
def test_constraints_configuration_programming_and_cancellation_are_not_retried(error):
    assert not is_transient_database_error(error)


def make_uow():
    session = Mock(rollback=AsyncMock(), close=AsyncMock(), commit=AsyncMock())
    return PostgresUow(Mock(return_value=session)), session


@pytest.mark.parametrize("method", ["rollback", "close"])
async def test_cleanup_connection_failure_is_translated_and_close_is_attempted(method):
    uow, session = make_uow()
    error = InterfaceError("ROLLBACK", None, RuntimeError(), connection_invalidated=True)
    getattr(session, method).side_effect = error
    with pytest.raises(DependencyUnavailable) as caught:
        async with uow:
            pass
    assert caught.value.__cause__ is error
    session.rollback.assert_awaited_once()
    session.close.assert_awaited_once()


async def test_primary_operation_failure_is_preserved_when_cleanup_also_fails():
    uow, session = make_uow()
    original = RuntimeError("broken invariant")
    session.rollback.side_effect = ConnectionResetError()
    session.close.side_effect = TimeoutError()
    with pytest.raises(RuntimeError) as caught:
        async with uow:
            raise original
    assert caught.value is original
    session.close.assert_awaited_once()


async def test_primary_transient_failure_keeps_its_cause_after_cleanup_failure():
    uow, session = make_uow()
    original = asyncpg.TooManyConnectionsError()
    session.rollback.side_effect = RuntimeError("cleanup failed")
    with pytest.raises(DependencyUnavailable) as caught:
        async with uow:
            raise original
    assert caught.value.__cause__ is original
    session.close.assert_awaited_once()


async def test_commit_failure_is_classified_at_the_uow_boundary():
    uow, session = make_uow()
    original = asyncpg.TransactionResolutionUnknownError()
    session.commit.side_effect = original
    with pytest.raises(DependencyUnavailable) as caught:
        async with uow:
            await uow.commit()
    assert caught.value.__cause__ is original
    session.close.assert_awaited_once()


async def test_operation_cancellation_is_preserved_despite_cleanup_failures():
    uow, session = make_uow()
    original = asyncio.CancelledError()
    session.rollback.side_effect = ConnectionResetError()
    session.close.side_effect = TimeoutError()
    with pytest.raises(asyncio.CancelledError) as caught:
        async with uow:
            raise original
    assert caught.value is original
    session.close.assert_awaited_once()


@pytest.mark.parametrize("method", ["rollback", "close"])
async def test_cancellation_during_cleanup_is_not_replaced_by_an_operation_error(method):
    uow, session = make_uow()
    getattr(session, method).side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        async with uow:
            raise RuntimeError("original operation failed")
    session.close.assert_awaited_once()


async def test_nontransient_cleanup_failure_is_not_hidden():
    uow, session = make_uow()
    original = RuntimeError("broken cleanup")
    session.close.side_effect = original
    with pytest.raises(RuntimeError) as caught:
        async with uow:
            pass
    assert caught.value is original
