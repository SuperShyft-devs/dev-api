"""
Database session configuration.

This module provides the database engine and session management.
Uses async SQLAlchemy for non-blocking database operations.
"""

from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from db.engine import create_api_engine
from db.observability import install_db_observability

# Create async engine with connection pooling
engine = create_api_engine()
install_db_observability(engine)

# Create async session factory
# expire_on_commit=False prevents lazy loading issues after commit
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency function to get database session.
    
    This function is used as a FastAPI dependency to provide
    database sessions to route handlers. It ensures proper
    session lifecycle management with automatic cleanup.
    
    Yields:
        AsyncSession: Database session instance.
        
    Example:
        @router.get("/users")
        async def get_users(db: AsyncSession = Depends(get_db)):
            # Use db session here
            pass
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.rollback()
            await session.close()
