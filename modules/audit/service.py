"""Audit logging service.

Audit failure must fail the request.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from modules.audit.models import DataAuditLog
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
