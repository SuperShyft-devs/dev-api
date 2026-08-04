"""Engagement types HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from db.session import get_db
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.engagement_types.schemas import (
    EngagementTypeCreateRequest,
    EngagementTypeUpdateRequest,
)
from modules.engagement_types.service import EngagementTypesService

router = APIRouter(prefix="/engagement-types", tags=["engagement-types"])


def _get_service() -> EngagementTypesService:
    return EngagementTypesService()


@router.get("")
async def list_engagement_types(
    is_active: bool | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: EngagementTypesService = Depends(_get_service),
):
    _ = employee
    data = await service.list_all(db, is_active=is_active)
    return success_response([item.model_dump() for item in data])


@router.post("")
async def create_engagement_type(
    payload: EngagementTypeCreateRequest,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: EngagementTypesService = Depends(_get_service),
):
    _ = employee
    data = await service.create(db, payload)
    await db.commit()
    return success_response(data.model_dump())


@router.put("/{type_id}")
async def update_engagement_type(
    type_id: int,
    payload: EngagementTypeUpdateRequest,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: EngagementTypesService = Depends(_get_service),
):
    _ = employee
    data = await service.update(db, type_id, payload)
    await db.commit()
    return success_response(data.model_dump())


@router.delete("/{type_id}")
async def delete_engagement_type(
    type_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: EngagementTypesService = Depends(_get_service),
):
    _ = employee
    await service.delete(db, type_id)
    await db.commit()
    return success_response({"deleted": True})
