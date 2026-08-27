"""Integration sync logging for Healthians API calls."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from db.session import AsyncSessionLocal
from modules.audit.models import IntegrationSyncLog
from modules.audit.repository import AuditRepository


async def log_healthians_call(
    db: AsyncSession,
    *,
    engagement_id: int | None,
    user_id: int | None,
    provider: str,
    api_url: str,
    request_payload: dict | None,
    response_payload: dict | None = None,
    status: str,
    error_message: str | None = None,
) -> IntegrationSyncLog:
    audit_repo = AuditRepository()
    sync_log = await audit_repo.create_sync_log(
        db,
        IntegrationSyncLog(
            engagement_id=engagement_id,
            user_id=user_id,
            provider=provider,
            api_endpoint_url=api_url,
            request_payload=request_payload,
            response_payload=response_payload,
            status=status,
            error_message=error_message,
        ),
    )
    return sync_log


async def finalize_healthians_sync_log(
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


async def persist_healthians_sync_log_isolated(
    *,
    engagement_id: int | None,
    user_id: int | None,
    provider: str,
    api_url: str,
    request_payload: dict | None,
    response_payload: dict | None = None,
    status: str,
    error_message: str | None = None,
) -> int:
    """Write Healthians sync log on its own session so pending rows do not pin caller transactions."""
    audit_repo = AuditRepository()
    async with AsyncSessionLocal() as audit_db:
        sync_log = await audit_repo.create_sync_log(
            audit_db,
            IntegrationSyncLog(
                engagement_id=engagement_id,
                user_id=user_id,
                provider=provider,
                api_endpoint_url=api_url,
                request_payload=request_payload,
                response_payload=response_payload,
                status=status,
                error_message=error_message,
            ),
        )
        await audit_db.commit()
        return int(sync_log.sync_log_id)


async def finalize_healthians_sync_log_isolated(
    *,
    sync_log_id: int,
    status: str,
    response_payload: dict | None = None,
    error_message: str | None = None,
) -> None:
    audit_repo = AuditRepository()
    async with AsyncSessionLocal() as audit_db:
        await audit_repo.update_sync_log_status(
            audit_db,
            sync_log_id=sync_log_id,
            status=status,
            response_payload=response_payload,
            error_message=error_message,
        )
        await audit_db.commit()
