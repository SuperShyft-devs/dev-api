"""Local conftest for Qura tests.

Qura unit/integration tests never access the database.
Override the session-scoped ``test_engine`` fixture from the root conftest
so that Qura tests can run without a live PostgreSQL connection.
"""

import pytest


@pytest.fixture(scope="session")
def test_engine():
    """No-op override: Qura tests do not use the database."""
    yield None
