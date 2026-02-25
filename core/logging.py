"""
Logging setup and request ID handling.
"""

from __future__ import annotations

import contextvars
import logging
import uuid
from typing import Callable, Awaitable

from fastapi import Request, Response

from core.config import settings


_request_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar(
    "request_id",
    default="system",
)


class RequestIdFilter(logging.Filter):
    """Attach the current request ID to log records.
    
    This filter ensures every log record has a request_id attribute.
    When called outside a request context (e.g., during startup or
    database initialization), it uses 'system' as the request ID.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        # Get request ID from context, default to 'system' if no context exists
        request_id = _request_id_ctx.get()
        
        # Ensure the attribute exists on the record
        if not hasattr(record, 'request_id'):
            record.request_id = request_id
        
        return True


def get_request_id() -> str:
    """Return the active request ID or 'unknown'."""
    return _request_id_ctx.get()


def configure_logging() -> None:
    """Configure application logging once."""
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return

    # Create a custom filter instance
    request_id_filter = RequestIdFilter()
    
    logging.basicConfig(
        level=settings.LOG_LEVEL,
        format="%(asctime)s %(levelname)s [%(request_id)s] %(name)s: %(message)s",
    )

    # Add the filter to the handler, not the logger.
    # Handler filters are called for every record that passes through,
    # regardless of which logger emitted it. This is necessary because
    # when log records propagate from child loggers (like sqlalchemy.engine.Engine)
    # to parent loggers, the parent's logger filter is NOT called - only
    # the original logger's filter runs.
    for handler in root_logger.handlers:
        handler.addFilter(request_id_filter)


def _generate_request_id() -> str:
    return str(uuid.uuid4())


async def request_id_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """Attach a request ID to the context and response."""
    request_id = request.headers.get("X-Request-ID") or _generate_request_id()
    token = _request_id_ctx.set(request_id)
    request.state.request_id = request_id

    try:
        response = await call_next(request)
    finally:
        _request_id_ctx.reset(token)

    response.headers["X-Request-ID"] = request_id
    return response
