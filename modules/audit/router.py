"""Audit HTTP routes (admin read access to integration sync logs)."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.exceptions import AppError
from db.session import get_db
from modules.audit.dependencies import get_audit_service
from modules.audit.service import AuditService
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext


router = APIRouter(prefix="/audit", tags=["audit"])


@router.get("/integration-sync-logs")
async def list_integration_sync_logs(
    page: int = 1,
    limit: int = 25,
    provider: str | None = None,
    status: str | None = None,
    user_id: int | None = None,
    engagement_id: int | None = None,
    search: str | None = None,
    from_: datetime | None = Query(default=None, alias="from"),
    to: datetime | None = None,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: AuditService = Depends(get_audit_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    statuses = None
    if status:
        statuses = [s.strip() for s in status.split(",") if s.strip()]

    items, total = await service.list_integration_sync_logs(
        db,
        page=page,
        limit=limit,
        provider=provider,
        statuses=statuses,
        user_id=user_id,
        engagement_id=engagement_id,
        created_from=from_,
        created_to=to,
        search=(search or "").strip() or None,
    )
    return success_response(items, meta={"page": page, "limit": limit, "total": total})
