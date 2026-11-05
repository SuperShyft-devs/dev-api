"""Dashboard year-stats + Command Center for admin KPI cards."""

from __future__ import annotations

from datetime import date
from typing import Literal

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from db.session import get_db
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.dashboard.command_service import (
    build_command_bootstrap,
    build_command_drill,
)
from modules.dashboard.service import (
    build_dashboard_operations,
    build_dashboard_overview,
    year_stats_payload,
)
from modules.engagements.models import Engagement, EngagementParticipant

router = APIRouter(prefix="/admin/dashboard", tags=["dashboard"])

AudienceParam = Literal["all", "b2b", "b2c"]
LifecycleParam = Literal["all", "ongoing", "upcoming", "completed"]


@router.get("/year-stats")
async def get_year_stats(
    year: int = Query(..., ge=2000, le=2100),
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
):
    del employee  # auth gate only
    return success_response(await year_stats_payload(db, year=year))


@router.get("/participant-stats")
async def get_participant_stats(
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
):
    del employee  # auth gate only

    total = (
        await db.execute(
            select(func.count())
            .select_from(EngagementParticipant)
            .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
            .where(Engagement.status.in_(("running", "scheduled")))
        )
    ).scalar_one()

    return success_response({"engagement_participants_total": int(total or 0)})


@router.get("/overview")
async def get_dashboard_overview(
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
):
    payload = await build_dashboard_overview(db, employee=employee)
    return success_response(payload)


@router.get("/operations")
async def get_dashboard_operations(
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
):
    payload = await build_dashboard_operations(db, employee=employee)
    return success_response(payload)


@router.get("/command")
async def get_dashboard_command(
    audience: AudienceParam = Query("all"),
    from_: date | None = Query(None, alias="from"),
    to: date | None = Query(None),
    lifecycle: LifecycleParam = Query("all"),
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
):
    del employee  # auth gate only
    payload = await build_command_bootstrap(
        db,
        audience=audience,
        date_from=from_,
        date_to=to,
        lifecycle=lifecycle,
    )
    return success_response(payload)


@router.get("/command/drill")
async def get_dashboard_command_drill(
    drill: str = Query(..., min_length=1),
    audience: AudienceParam = Query("all"),
    from_: date | None = Query(None, alias="from"),
    to: date | None = Query(None),
    lifecycle: LifecycleParam = Query("all"),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    package_id: int | None = Query(None),
    discount_code_id: int | None = Query(None),
    q: str | None = Query(None),
    city: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
):
    del employee  # auth gate only
    payload = await build_command_drill(
        db,
        drill=drill,
        audience=audience,
        date_from=from_,
        date_to=to,
        lifecycle=lifecycle,
        page=page,
        limit=limit,
        package_id=package_id,
        discount_code_id=discount_code_id,
        q=q,
        city=city,
    )
    return success_response(
        payload,
        meta={
            "page": payload.get("page", page),
            "limit": payload.get("limit", limit),
            "total": payload.get("total", 0),
        },
    )
