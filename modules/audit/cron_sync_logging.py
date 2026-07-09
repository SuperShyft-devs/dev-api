"""Integration sync logging for cron job external API calls."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from sqlalchemy.ext.asyncio import AsyncSession

from modules.audit.models import IntegrationSyncLog
from modules.audit.repository import AuditRepository

T = TypeVar("T")


def sanitize_response_payload(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, list):
        return {"data": value}
    return {"data": value}


async def log_integration_call(
    db: AsyncSession,
    *,
    provider: str,
    api_url: str,
    engagement_id: int | None = None,
    user_id: int | None = None,
    request_payload: dict | None = None,
    status: str = "pending",
) -> IntegrationSyncLog:
    audit_repo = AuditRepository()
    return await audit_repo.create_sync_log(
        db,
        IntegrationSyncLog(
            engagement_id=engagement_id,
            user_id=user_id,
            provider=provider,
            api_endpoint_url=api_url,
            request_payload=request_payload,
            status=status,
        ),
    )


async def finalize_integration_call(
    db: AsyncSession,
    *,
    sync_log_id: int,
    status: str,
    response_payload: dict | None = None,
    error_message: str | None = None,
) -> None:
    audit_repo = AuditRepository()
    await audit_repo.update_sync_log_status(
        db,
        sync_log_id=sync_log_id,
        status=status,
        response_payload=response_payload,
        error_message=error_message,
    )


async def tracked_integration_call(
    db: AsyncSession,
    *,
    provider: str,
    api_url: str,
    engagement_id: int | None,
    user_id: int | None,
    request_payload: dict | None,
    operation: Callable[[], Awaitable[T]],
    reraise: bool = True,
    persist: bool = True,
) -> T | None:
    """Execute an external API call and record it in integration_sync_logs."""
    sync_log = await log_integration_call(
        db,
        provider=provider,
        api_url=api_url,
        engagement_id=engagement_id,
        user_id=user_id,
        request_payload=request_payload,
    )
    try:
        result = await operation()
        await finalize_integration_call(
            db,
            sync_log_id=sync_log.sync_log_id,
            status="success",
            response_payload=sanitize_response_payload(result),
        )
        if persist:
            await db.commit()
        return result
    except Exception as exc:
        await finalize_integration_call(
            db,
            sync_log_id=sync_log.sync_log_id,
            status="failed",
            error_message=str(exc)[:2000],
        )
        if persist:
            await db.commit()
        if reraise:
            raise
        return None
