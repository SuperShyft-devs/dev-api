"""Audit logging service.

Audit failure must fail the request.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from modules.audit.models import DataAuditLog, IntegrationSyncLog
from modules.audit.repository import AuditRepository


class AuditService:
    """Service for creating audit logs."""

    def __init__(self, repository: AuditRepository):
        self._repository = repository

    async def log_event(
        self,
        db: AsyncSession,
        *,
        action: str,
        endpoint: str,
        ip_address: str,
        user_agent: str,
        user_id: Optional[int] = None,
        session_id: Optional[int] = None,
    ) -> None:
        log = DataAuditLog(
            user_id=user_id,
            session_id=session_id,
            action=action,
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
            timestamp=datetime.now(timezone.utc),
        )
        await self._repository.create_log(db, log)

    async def list_integration_sync_logs(
        self,
        db: AsyncSession,
        *,
        page: int,
        limit: int,
        provider: str | None = None,
        statuses: list[str] | None = None,
        user_id: int | None = None,
        engagement_id: int | None = None,
        created_from: datetime | None = None,
        created_to: datetime | None = None,
        search: str | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = await self._repository.list_sync_logs(
            db,
            page=page,
            limit=limit,
            provider=provider,
            statuses=statuses,
            user_id=user_id,
            engagement_id=engagement_id,
            created_from=created_from,
            created_to=created_to,
            search=search,
        )
        total = await self._repository.count_sync_logs(
            db,
            provider=provider,
            statuses=statuses,
            user_id=user_id,
            engagement_id=engagement_id,
            created_from=created_from,
            created_to=created_to,
            search=search,
        )
        return [self._serialize_sync_log(row) for row in rows], total

    @staticmethod
    def _serialize_sync_log(row: IntegrationSyncLog) -> dict[str, Any]:
        return {
            "sync_log_id": row.sync_log_id,
            "engagement_id": row.engagement_id,
            "user_id": row.user_id,
            "provider": row.provider,
            "api_endpoint_url": row.api_endpoint_url,
            "request_payload": row.request_payload,
            "response_payload": row.response_payload,
            "status": row.status,
            "error_message": row.error_message,
            "created_at": row.created_at,
        }
