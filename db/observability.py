"""Database observability: slow-query logging and pool metrics."""

from __future__ import annotations

import logging
import time
from typing import Any

from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine

from core.config import settings
from core.logging import get_request_id

logger = logging.getLogger(__name__)

_installed_on: set[int] = set()


def _log_slow_query(
    *,
    statement: str,
    duration_ms: float,
    conn_info: str,
) -> None:
    if duration_ms < settings.DATABASE_SLOW_QUERY_MS:
        return
    request_id = get_request_id()
    logger.warning(
        "slow_query duration_ms=%.1f request_id=%s conn=%s statement=%s",
        duration_ms,
        request_id or "-",
        conn_info,
        (statement or "")[:500],
    )


def install_db_observability(engine: AsyncEngine | Engine) -> None:
    """Attach slow-query listeners once per engine instance."""
    sync_engine = engine.sync_engine if isinstance(engine, AsyncEngine) else engine
    key = id(sync_engine)
    if key in _installed_on:
        return
    _installed_on.add(key)

    @event.listens_for(sync_engine, "before_cursor_execute")
    def _before_cursor_execute(conn, cursor, statement, parameters, context, executemany):  # noqa: ARG001
        conn.info.setdefault("query_start_time", []).append(time.perf_counter())

    @event.listens_for(sync_engine, "after_cursor_execute")
    def _after_cursor_execute(conn, cursor, statement, parameters, context, executemany):  # noqa: ARG001
        started = conn.info.get("query_start_time")
        if not started:
            return
        duration_ms = (time.perf_counter() - started.pop()) * 1000
        _log_slow_query(
            statement=statement,
            duration_ms=duration_ms,
            conn_info=str(conn.connection.dbapi_connection),
        )


def pool_metrics(engine: AsyncEngine | Engine) -> dict[str, Any]:
    """Return SQLAlchemy pool utilization for health endpoints."""
    pool = engine.pool if isinstance(engine, Engine) else engine.sync_engine.pool
    return {
        "pool_size": pool.size(),
        "checked_in": pool.checkedin(),
        "checked_out": pool.checkedout(),
        "overflow": pool.overflow(),
    }
