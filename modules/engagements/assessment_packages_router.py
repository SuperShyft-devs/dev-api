"""Engagement-level assessment packages HTTP routes.

Routes:
- ``GET /engagements/{engagement_id}/assessment-packages`` — participant or employee
- ``POST /engagements/{engagement_id}/assessment-packages`` — participant or employee
- ``DELETE /engagements/{engagement_id}/assessment-packages/{package_code}`` — employee only
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.dependencies import get_current_user
from db.session import get_db
from modules.employee.dependencies import get_current_employee, get_optional_employee
from modules.employee.service import EmployeeContext
from modules.engagements.assessment_packages_service import (
    EngagementAssessmentPackagesService,
)
from modules.engagements.dependencies import get_engagement_assessment_packages_service
from modules.engagements.schemas import EngagementAssessmentPackageAddRequest


router = APIRouter(prefix="/engagements", tags=["engagement-assessment-packages"])


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()

    if request.client is None:
        return "unknown"

    return request.client.host


@router.get("/{engagement_id}/assessment-packages")
async def list_engagement_assessment_packages(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
    employee: EmployeeContext | None = Depends(get_optional_employee),
    service: EngagementAssessmentPackagesService = Depends(
        get_engagement_assessment_packages_service
    ),
):
    data = await service.list_packages_for_engagement(
        db,
        engagement_id=engagement_id,
        current_user_id=user.user_id,
        employee=employee,
    )
    return success_response(data)


@router.post("/{engagement_id}/assessment-packages", status_code=201)
async def add_engagement_assessment_package(
    engagement_id: int,
    payload: EngagementAssessmentPackageAddRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
    employee: EmployeeContext | None = Depends(get_optional_employee),
    service: EngagementAssessmentPackagesService = Depends(
        get_engagement_assessment_packages_service
    ),
):
    data = await service.add_package_to_engagement(
        db,
        engagement_id=engagement_id,
        package_code=payload.package_code,
        current_user_id=user.user_id,
        employee=employee,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@router.delete("/{engagement_id}/assessment-packages/{package_code}")
async def remove_engagement_assessment_package(
    engagement_id: int,
    package_code: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: EngagementAssessmentPackagesService = Depends(
        get_engagement_assessment_packages_service
    ),
):
    data = await service.remove_package_from_engagement(
        db,
        engagement_id=engagement_id,
        package_code=package_code,
        employee=employee,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)
