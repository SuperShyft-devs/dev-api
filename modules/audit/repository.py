"""Audit repository.

Only database writes live here.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, select, update as sql_update
from sqlalchemy.ext.asyncio import AsyncSession

from modules.audit.models import DataAuditLog, IntegrationSyncLog


class AuditRepository:
    """Audit database operations."""

    async def create_log(self, db: AsyncSession, log: DataAuditLog) -> None:
        db.add(log)

    async def create_sync_log(self, db: AsyncSession, log: IntegrationSyncLog) -> IntegrationSyncLog:
        db.add(log)
        await db.flush()
        return log

    async def update_sync_log_status(
        self,
        db: AsyncSession,
        *,
        sync_log_id: int,
        status: str,
        response_payload: dict | None = None,
        error_message: str | None = None,
    ) -> None:
        values: dict = {"status": status}
        if response_payload is not None:
            values["response_payload"] = response_payload
        if error_message is not None:
            values["error_message"] = error_message
        await db.execute(
            sql_update(IntegrationSyncLog)
            .where(IntegrationSyncLog.sync_log_id == sync_log_id)
            .values(**values)
        )
        await db.flush()

    def _apply_sync_log_filters(
        self,
        query,
        *,
        provider: str | None = None,
        statuses: list[str] | None = None,
        user_id: int | None = None,
        engagement_id: int | None = None,
        created_from: datetime | None = None,
        created_to: datetime | None = None,
    ):
        if provider is not None:
            query = query.where(IntegrationSyncLog.provider == provider)
        if statuses:
            query = query.where(IntegrationSyncLog.status.in_(statuses))
        if user_id is not None:
            query = query.where(IntegrationSyncLog.user_id == user_id)
        if engagement_id is not None:
            query = query.where(IntegrationSyncLog.engagement_id == engagement_id)
        if created_from is not None:
            query = query.where(IntegrationSyncLog.created_at >= created_from)
        if created_to is not None:
            query = query.where(IntegrationSyncLog.created_at <= created_to)
        return query

    async def list_sync_logs(
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
    ) -> list[IntegrationSyncLog]:
        offset = (page - 1) * limit
        query = (
            select(IntegrationSyncLog)
            .order_by(IntegrationSyncLog.sync_log_id.desc())
            .offset(offset)
            .limit(limit)
        )
        query = self._apply_sync_log_filters(
            query,
            provider=provider,
            statuses=statuses,
            user_id=user_id,
            engagement_id=engagement_id,
            created_from=created_from,
            created_to=created_to,
        )
        result = await db.execute(query)
        return list(result.scalars().all())

    async def count_sync_logs(
        self,
        db: AsyncSession,
        *,
        provider: str | None = None,
        statuses: list[str] | None = None,
        user_id: int | None = None,
        engagement_id: int | None = None,
        created_from: datetime | None = None,
        created_to: datetime | None = None,
    ) -> int:
        query = select(func.count()).select_from(IntegrationSyncLog)
        query = self._apply_sync_log_filters(
            query,
            provider=provider,
            statuses=statuses,
            user_id=user_id,
            engagement_id=engagement_id,
            created_from=created_from,
            created_to=created_to,
        )
        result = await db.execute(query)
        return int(result.scalar_one())
