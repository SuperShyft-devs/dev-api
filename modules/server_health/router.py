"""Server health monitoring routes (admin read-only)."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, Query

from common.responses import success_response
from core.exceptions import AppError
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.server_health.dependencies import get_server_health_service
from modules.server_health.service import ServerHealthService

router = APIRouter(prefix="/server-health", tags=["server-health"])


@router.get("/current")
async def get_server_health_current(
    employee: EmployeeContext = Depends(get_current_employee),
    service: ServerHealthService = Depends(get_server_health_service),
):
    data = await service.get_current_status(employee)
    return success_response(data.model_dump() if data is not None else None)


@router.get("/history")
async def list_server_health_history(
    limit: int = 50,
    from_: datetime | None = Query(default=None, alias="from"),
    to: datetime | None = None,
    employee: EmployeeContext = Depends(get_current_employee),
    service: ServerHealthService = Depends(get_server_health_service),
):
    if limit < 1 or limit > 500:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    items, total = await service.list_history(
        employee,
        limit=limit,
        run_from=from_,
        run_to=to,
    )
    return success_response(
        [item.model_dump() for item in items],
        meta={"limit": limit, "total": total},
    )
