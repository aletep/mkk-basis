import pytest

from tests.safety import require_isolated_resources


def isolated():
    return {
        "INTEGRATION_RESOURCE_SCOPE": "mkk-basis-tests-v1",
        "DATABASE_URL": (
            "postgresql+asyncpg://test_payments:password@test-postgres:5432/mkk_basis_test"
        ),
        "AMQP_URL": "amqp://test_payments:password@test-rabbitmq:5672/mkk_basis_test",
    }


def test_dedicated_destinations_allow_cleanup():
    require_isolated_resources(isolated())


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("INTEGRATION_RESOURCE_SCOPE", ""),
        ("DATABASE_URL", "postgresql+asyncpg://payments:secret@postgres:5432/payments"),
        ("DATABASE_URL", "postgresql+asyncpg://test_payments:p@test-postgres:5432/payments"),
        (
            "DATABASE_URL",
            "postgresql+asyncpg://test_payments:p@test-postgres:5432/mkk_basis_test?x=1",
        ),
        ("AMQP_URL", "amqp://payments:secret@rabbitmq:5672/"),
        ("AMQP_URL", "amqp://test_payments:p@test-rabbitmq:5672/"),
        ("AMQP_URL", "amqp://test_payments:p@test-rabbitmq:invalid/mkk_basis_test"),
    ],
)
def test_unsafe_destinations_refuse_cleanup_without_exposing_secrets(name, value):
    environment = isolated() | {name: value}
    with pytest.raises(RuntimeError) as error:
        require_isolated_resources(environment)
    assert "secret" not in str(error.value)
    assert value not in str(error.value) or not value


def test_empty_environment_refuses_cleanup():
    with pytest.raises(RuntimeError):
        require_isolated_resources({})


def test_migrations_are_refused_before_subprocess_runs(monkeypatch):
    from tests.conftest import migrate_integration_database

    monkeypatch.setenv("RUN_INTEGRATION", "1")
    monkeypatch.delenv("INTEGRATION_RESOURCE_SCOPE", raising=False)

    def unexpected_migration(*args, **kwargs):
        pytest.fail("Migration started before validating the test resources")

    monkeypatch.setattr("tests.conftest.subprocess.run", unexpected_migration)
    with pytest.raises(RuntimeError, match="isolated Compose test profile"):
        migrate_integration_database.__wrapped__()
