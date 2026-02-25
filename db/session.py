"""
Database session configuration.

This module provides the database engine and session management.
Uses async SQLAlchemy for non-blocking database operations.
"""

from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from core.config import settings

# Create async engine with connection pooling
# echo=True in development shows SQL queries for debugging
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.is_development(),
    pool_size=settings.DATABASE_POOL_SIZE,
    max_overflow=settings.DATABASE_MAX_OVERFLOW,
    pool_pre_ping=True,  # Verify connections before using them
    pool_recycle=3600,   # Recycle connections after 1 hour
)

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
        finally:
            await session.close()
