"""Engagement console HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.exceptions import AppError
from db.session import get_db
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.engagements.console.schemas import ConsoleParticipantBookRequest
from modules.engagements.console.service import ConsoleService
from modules.engagements.dependencies import get_console_service

router = APIRouter(prefix="/engagements", tags=["engagement-console"])


@router.get("/console/engagements")
async def list_console_engagements(
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    console_service: ConsoleService = Depends(get_console_service),
):
    data = await console_service.list_console_engagements(db, employee=employee)
    return success_response(data)


@router.get("/{engagement_id}/console")
async def get_engagement_for_console(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    console_service: ConsoleService = Depends(get_console_service),
):
    data = await console_service.get_engagement_for_console(
        db,
        employee=employee,
        engagement_id=engagement_id,
    )
    return success_response(data)


@router.get("/{engagement_id}/console/participants")
async def get_console_participants(
    engagement_id: int,
    page: int = 1,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    console_service: ConsoleService = Depends(get_console_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    participants, total = await console_service.list_participants_for_console(
        db,
        employee=employee,
        engagement_id=engagement_id,
        page=page,
        limit=limit,
    )

    return success_response(participants, meta={"page": page, "limit": limit, "total": total})


@router.post("/{engagement_id}/console/participants/{user_id}/book")
async def book_console_participant(
    engagement_id: int,
    user_id: int,
    payload: ConsoleParticipantBookRequest,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    console_service: ConsoleService = Depends(get_console_service),
):
    data = await console_service.book_participant(
        db,
        employee=employee,
        engagement_id=engagement_id,
        user_id=user_id,
        barcode=payload.barcode,
    )
    return success_response(data)
