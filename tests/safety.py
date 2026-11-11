"""Refuse migrations and destructive fixtures outside dedicated test resources."""

import os
from collections.abc import Mapping
from urllib.parse import urlsplit


def require_isolated_resources(environment: Mapping[str, str] | None = None) -> None:
    """Validate explicit test scope and both destinations before any cleanup."""
    values = os.environ if environment is None else environment
    if values.get("INTEGRATION_RESOURCE_SCOPE") != "mkk-basis-tests-v1":
        raise RuntimeError("Integration cleanup requires the isolated Compose test profile")
    expected = (
        ("DATABASE_URL", "postgresql+asyncpg", "test-postgres", 5432),
        ("AMQP_URL", "amqp", "test-rabbitmq", 5672),
    )
    for name, scheme, host, port in expected:
        try:
            destination = urlsplit(values.get(name, ""))
            valid = (
                destination.scheme == scheme
                and destination.hostname == host
                and destination.port == port
                and destination.username == "test_payments"
                and destination.path == "/mkk_basis_test"
                and not destination.query
                and not destination.fragment
            )
        except ValueError:
            valid = False
        if not valid:
            # Never include URLs: they can contain deployment credentials.
            raise RuntimeError(f"Refusing integration cleanup: unsafe {name} destination")
