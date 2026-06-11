"""Audit repository.

Only database writes live here.
"""

from __future__ import annotations

from sqlalchemy import update as sql_update
from sqlalchemy.ext.asyncio import AsyncSession

from modules.audit.models import DataAuditLog, IntegrationSyncLog


class AuditRepository:
    """Write-only audit operations."""

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
