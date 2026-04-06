"""Assessments HTTP routes (user-facing)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.exceptions import AppError
from core.dependencies import get_current_user
from db.session import get_db
from modules.assessments.dependencies import get_assessments_service
from modules.assessments.schemas import AssessmentStatusUpdateRequest, MetsightsRecordIdUpdate
from modules.assessments.service import AssessmentsService
from modules.employee.dependencies import get_current_employee, get_optional_employee
from modules.metsights.dependencies import get_metsights_sync_service
from modules.metsights.sync_service import MetsightsSyncService
from modules.employee.service import EmployeeContext


router = APIRouter(prefix="/assessments", tags=["assessments"])


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()

    if request.client is None:
        return "unknown"

    return request.client.host


@router.get("/me")
async def list_my_assessments(
    request: Request,
    page: int = 1,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
    assessments_service: AssessmentsService = Depends(get_assessments_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    rows, total = await assessments_service.list_my_assessments(
        db,
        user_id=user.user_id,
        page=page,
        limit=limit,
    )

    data = []
    for instance, package in rows:
        data.append(
            {
                "assessment_instance_id": instance.assessment_instance_id,
                "package_id": instance.package_id,
                "package_code": getattr(package, "package_code", None) if package is not None else None,
                "package_display_name": getattr(package, "display_name", None) if package is not None else None,
                "engagement_id": instance.engagement_id,
                "status": instance.status,
                "metsights_record_id": instance.metsights_record_id,
                "assigned_at": instance.assigned_at,
                "completed_at": instance.completed_at,
            }
        )

    return success_response(data, meta={"page": page, "limit": limit, "total": total})


@router.get("/{assessment_instance_id}")
async def get_assessment_details(
    assessment_instance_id: int,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
    assessments_service: AssessmentsService = Depends(get_assessments_service),
):
    instance, package = await assessments_service.get_assessment_details_for_user(
        db,
        assessment_instance_id=assessment_instance_id,
        user_id=user.user_id,
    )

    return success_response(
        {
            "assessment_instance_id": instance.assessment_instance_id,
            "package_id": instance.package_id,
            "package_code": getattr(package, "package_code", None) if package is not None else None,
            "package_display_name": getattr(package, "display_name", None) if package is not None else None,
            "engagement_id": instance.engagement_id,
            "status": instance.status,
            "metsights_record_id": instance.metsights_record_id,
            "assigned_at": instance.assigned_at,
            "completed_at": instance.completed_at,
        }
    )


@router.post("/{assessment_instance_id}/metsights/import-answers")
async def import_metsights_questionnaire_answers(
    assessment_instance_id: int,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    employee=Depends(get_optional_employee),
    sync_service: MetsightsSyncService = Depends(get_metsights_sync_service),
):
    """Pull Metsights per-type questionnaire resources (GET + OPTIONS) and upsert responses using semantic option values."""

    result = await sync_service.import_questionnaire_answers_for_instance(
        db,
        assessment_instance_id=assessment_instance_id,
        current_user_id=current_user.user_id,
        employee_ok=employee is not None,
    )
    await db.commit()
    return success_response(result)


@router.patch("/{assessment_instance_id}/status")
async def update_assessment_status(
    assessment_instance_id: int,
    payload: AssessmentStatusUpdateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
    assessments_service: AssessmentsService = Depends(get_assessments_service),
):
    updated = await assessments_service.change_assessment_status_for_user(
        db,
        assessment_instance_id=assessment_instance_id,
        user_id=user.user_id,
        status=payload.status,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response(
        {
            "assessment_instance_id": updated.assessment_instance_id,
            "status": updated.status,
            "completed_at": updated.completed_at,
        }
    )


@router.put("/{assessment_id}/metsights-record-id")
async def set_assessment_metsights_record_id(
    assessment_id: int,
    body: MetsightsRecordIdUpdate,
    db: AsyncSession = Depends(get_db),
    current_employee: EmployeeContext = Depends(get_current_employee),
    assessments_service: AssessmentsService = Depends(get_assessments_service),
):
    updated = await assessments_service.set_metsights_record_id(
        db,
        assessment_instance_id=assessment_id,
        data=body,
        current_employee=current_employee,
    )
    await db.commit()
    return success_response(
        {
            "assessment_instance_id": updated.assessment_instance_id,
            "package_id": updated.package_id,
            "package_code": None,
            "package_display_name": None,
            "engagement_id": updated.engagement_id,
            "status": updated.status,
            "metsights_record_id": updated.metsights_record_id,
            "assigned_at": updated.assigned_at,
            "completed_at": updated.completed_at,
        }
    )
