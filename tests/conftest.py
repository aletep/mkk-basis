import os
import subprocess

import pytest

from tests.safety import require_isolated_resources


@pytest.fixture(scope="session", autouse=True)
def migrate_integration_database():
    if os.getenv("RUN_INTEGRATION") == "1":
        require_isolated_resources()
        subprocess.run(["alembic", "upgrade", "head"], check=True)
