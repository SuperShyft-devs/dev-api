"""Database health and pool metrics (admin)."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from db.observability import pool_metrics
from db.session import engine, get_db
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.employee.access_control import ensure_admin

router = APIRouter(prefix="/health/db", tags=["health-db"])


@router.get("")
async def get_db_health(
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
):
    ensure_admin(employee)
    await db.execute(text("SELECT 1"))
    return success_response(
        {
            "status": "ok",
            "pool": pool_metrics(engine),
        }
    )
