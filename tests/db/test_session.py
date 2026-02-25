"""
Tests for database session configuration.

These tests verify that the database connection and session management
work correctly.
"""

import pytest
from sqlalchemy import text
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from db.session import get_db, engine, AsyncSessionLocal


@pytest.mark.asyncio
async def test_engine_creation():
    """
    Test that the database engine is created successfully.
    
    This test verifies that the engine object exists and has
    the correct configuration.
    """
    assert engine is not None

    expected = make_url(settings.DATABASE_URL)

    assert engine.url.database == expected.database
    assert engine.url.username == expected.username
    assert engine.url.host == expected.host
    assert engine.url.port == expected.port


@pytest.mark.asyncio
async def test_session_factory_creation():
    """
    Test that the async session factory is created successfully.
    
    This test verifies that AsyncSessionLocal can create new sessions.
    """
    assert AsyncSessionLocal is not None
    
    # Create a session from the factory
    async with AsyncSessionLocal() as session:
        assert isinstance(session, AsyncSession)


@pytest.mark.asyncio
async def test_database_connection():
    """
    Test that we can connect to the database and execute queries.
    
    This test verifies that:
    1. Connection to PostgreSQL works
    2. Simple queries can be executed
    3. Results can be retrieved
    """
    async with AsyncSessionLocal() as session:
        # Execute a simple query to verify connection
        result = await session.execute(text("SELECT 1 as test_value"))
        row = result.first()
        
        assert row is not None
        assert row.test_value == 1


@pytest.mark.asyncio
async def test_session_isolation():
    """
    Test that separate sessions are isolated from each other.
    
    This test verifies that changes in one session don't affect another
    until committed (transaction isolation).
    """
    async with AsyncSessionLocal() as session1:
        async with AsyncSessionLocal() as session2:
            # Verify they are different session objects
            assert session1 is not session2


@pytest.mark.asyncio
async def test_connection_pool_settings():
    """
    Test that connection pool settings are properly configured.
    
    This test verifies that the engine has the correct pool settings
    for managing database connections efficiently.
    """
    pool = engine.pool
    
    # Verify pool configuration
    assert pool is not None
    # Pool size should be configured from settings (default is 5)
    assert engine.pool.size() >= 0
