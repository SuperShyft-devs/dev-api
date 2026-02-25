"""Audit repository.

Only database writes live here.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from modules.audit.models import DataAuditLog


class AuditRepository:
    """Write-only audit operations."""

    async def create_log(self, db: AsyncSession, log: DataAuditLog) -> None:
        db.add(log)
