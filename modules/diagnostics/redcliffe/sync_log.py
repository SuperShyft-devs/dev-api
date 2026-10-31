"""Integration sync logging for Redcliffe Labs API calls."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from db.session import AsyncSessionLocal
from modules.audit.models import IntegrationSyncLog
from modules.audit.repository import AuditRepository


async def log_redcliffe_call(
    db: AsyncSession,
    *,
    engagement_id: int | None,
    user_id: int | None,
    api_url: str,
    request_payload: dict | None,
    response_payload: dict | None = None,
    status: str,
    error_message: str | None = None,
) -> IntegrationSyncLog:
    audit_repo = AuditRepository()
    return await audit_repo.create_sync_log(
        db,
        IntegrationSyncLog(
            engagement_id=engagement_id,
            user_id=user_id,
            provider="redcliffe",
            api_endpoint_url=api_url,
            request_payload=request_payload,
            response_payload=response_payload,
            status=status,
            error_message=error_message,
        ),
    )


async def persist_redcliffe_sync_log_isolated(
    *,
    engagement_id: int | None,
    user_id: int | None,
    api_url: str,
    request_payload: dict | None,
    response_payload: dict | None = None,
    status: str,
    error_message: str | None = None,
) -> int:
    audit_repo = AuditRepository()
    async with AsyncSessionLocal() as audit_db:
        sync_log = await audit_repo.create_sync_log(
            audit_db,
            IntegrationSyncLog(
                engagement_id=engagement_id,
                user_id=user_id,
                provider="redcliffe",
                api_endpoint_url=api_url,
                request_payload=request_payload,
                response_payload=response_payload,
                status=status,
                error_message=error_message,
            ),
        )
        await audit_db.commit()
        return int(sync_log.sync_log_id)


async def finalize_redcliffe_sync_log_isolated(
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
