"""Reports HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.dependencies import get_current_user
from db.session import get_db
from modules.reports.dependencies import get_reports_service
from modules.reports.schemas import BloodParametersReportResponse
from modules.reports.service import ReportsService


router = APIRouter(prefix="/reports", tags=["reports"])


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client is None:
        return "unknown"
    return request.client.host


@router.get("/{assessment_id}/blood-parameters")
async def get_blood_parameters_report(
    assessment_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
    reports_service: ReportsService = Depends(get_reports_service),
):
    blood_parameters = await reports_service.get_blood_parameters_for_user(
        db,
        assessment_id=assessment_id,
        user_id=user.user_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    response = BloodParametersReportResponse(
        assessment_id=assessment_id,
        blood_parameters=blood_parameters,
    )
    return success_response(response.model_dump())
