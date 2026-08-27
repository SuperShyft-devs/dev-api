"""Shared SQLAlchemy async engine factories."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from core.config import settings


def _connect_args() -> dict:
    if settings.DATABASE_STATEMENT_TIMEOUT_MS <= 0:
        return {}
    return {
        "server_settings": {
            "statement_timeout": str(settings.DATABASE_STATEMENT_TIMEOUT_MS),
        },
    }


def create_api_engine() -> AsyncEngine:
    """Uvicorn/FastAPI request workers — pooled connections."""
    return create_async_engine(
        settings.DATABASE_URL,
        echo=settings.DATABASE_ECHO,
        pool_size=settings.DATABASE_POOL_SIZE,
        max_overflow=settings.DATABASE_MAX_OVERFLOW,
        pool_timeout=settings.DATABASE_POOL_TIMEOUT,
        pool_pre_ping=True,
        pool_recycle=3600,
        connect_args=_connect_args(),
    )


def create_job_engine() -> AsyncEngine:
    """CLI/cron jobs — one connection per run, no persistent pool."""
    return create_async_engine(
        settings.DATABASE_URL,
        echo=False,
        poolclass=NullPool,
        pool_pre_ping=True,
        connect_args=_connect_args(),
    )


def job_session_factory(engine: AsyncEngine):
    return async_sessionmaker(bind=engine, expire_on_commit=False)
