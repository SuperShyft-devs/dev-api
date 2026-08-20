"""Integration sync logging helpers for Metsights API calls."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from modules.audit.cron_sync_logging import finalize_integration_call, log_integration_call, sanitize_response_payload

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class MetsightsSyncContext:
    """Optional audit context for recording Metsights HTTP calls in ``integration_sync_logs``."""

    db: AsyncSession
    engagement_id: int | None = None
    user_id: int | None = None


def metsights_api_url(path: str) -> str:
    base = settings.METSIGHTS_BASE_URL.rstrip("/")
    suffix = path.lstrip("/")
    return f"{base}/{suffix}"


async def tracked_metsights_call(
    sync_context: MetsightsSyncContext | None,
    *,
    api_url: str,
    request_payload: dict | None = None,
    operation: Callable[[], Awaitable[T]],
    reraise: bool = True,
) -> T | None:
    """Execute a Metsights HTTP call and record it in ``integration_sync_logs`` when context is set."""
    if sync_context is None:
        return await operation()

    sync_log = await log_integration_call(
        sync_context.db,
        provider="metsights",
        api_url=api_url,
        engagement_id=sync_context.engagement_id,
        user_id=sync_context.user_id,
        request_payload=request_payload,
    )
    try:
        result = await operation()
        await finalize_integration_call(
            sync_context.db,
            sync_log_id=sync_log.sync_log_id,
            status="success",
            response_payload=sanitize_response_payload(result),
        )
        return result
    except Exception as exc:
        await finalize_integration_call(
            sync_context.db,
            sync_log_id=sync_log.sync_log_id,
            status="failed",
            error_message=str(exc)[:2000],
        )
        if reraise:
            raise
        return None
