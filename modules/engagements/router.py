"""Engagements HTTP routes."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, File, Request, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.exceptions import AppError
from db.session import get_db
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext

# NOTE: The occupied-slots endpoints are intentionally public (no auth).
from modules.engagements.dependencies import get_engagements_service, get_onboarding_assistants_service
from modules.engagements.onboarding_assistants_service import OnboardingAssistantsService
from modules.engagements.schemas import (
    EngagementCreateRequest,
    EngagementStatusUpdateRequest,
    EngagementUpdateRequest,
    OnboardingAssistantsAddRequest,
)
from modules.engagements.service import EngagementsService, DEFAULT_B2C_DIAGNOSTIC_PACKAGE_ID
from modules.users.dependencies import get_users_service
from modules.users.service import UsersService


router = APIRouter(prefix="/engagements", tags=["engagements"])


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()

    if request.client is None:
        return "unknown"

    return request.client.host


@router.post("", status_code=201)
async def create_engagement(
    payload: EngagementCreateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    engagement = await engagements_service.create_b2b_engagement(
        db,
        employee=employee,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"engagement_id": engagement.engagement_id})


@router.get("")
async def list_engagements(
    request: Request,
    page: int = 1,
    limit: int = 20,
    org_id: int | None = None,
    status: str | None = None,
    city: str | None = None,
    date: date | None = None,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    engagements, total, readiness_by_id = await engagements_service.list_engagements_for_employee(
        db,
        employee=employee,
        page=page,
        limit=limit,
        organization_id=org_id,
        status=status,
        city=city,
        on_date=date,
    )

    data = []
    for engagement in engagements:
        readiness = readiness_by_id[engagement.engagement_id]
        data.append(
            {
                "engagement_id": engagement.engagement_id,
                "engagement_name": engagement.engagement_name,
                "metsights_engagement_id": engagement.metsights_engagement_id,
                "organization_id": engagement.organization_id,
                "engagement_code": engagement.engagement_code,
                "engagement_type": engagement.engagement_type,
                "assessment_package_id": engagement.assessment_package_id,
                        "diagnostic_package_id": engagement.diagnostic_package_id or DEFAULT_B2C_DIAGNOSTIC_PACKAGE_ID,
                "city": engagement.city,
                "slot_duration": engagement.slot_duration,
                "start_date": engagement.start_date,
                "end_date": engagement.end_date,
                "status": engagement.status,
                "participant_count": engagement.participant_count,
                "readiness": readiness.model_dump(mode="json"),
            }
        )

    return success_response(data, meta={"page": page, "limit": limit, "total": total})


@router.post("/{engagement_id}/import/metsights-csv")
async def import_metsights_csv_participants(
    engagement_id: int,
    request: Request,
    file: UploadFile | None = File(None),
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    users_service: UsersService = Depends(get_users_service),
):
    if file is None:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="CSV file is required")

    raw = await file.read()
    if not raw:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="CSV file is empty")

    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message="CSV file must be valid UTF-8",
        ) from exc

    result = await users_service.import_metsights_csv_for_engagement(
        db,
        employee=employee,
        engagement_id=engagement_id,
        file_content=text,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(result)


@router.get("/{engagement_id}")
async def get_engagement_details(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    engagement = await engagements_service.get_engagement_details_for_employee(
        db,
        employee=employee,
        engagement_id=engagement_id,
    )

    return success_response(
        {
            "engagement_id": engagement.engagement_id,
            "engagement_name": engagement.engagement_name,
            "metsights_engagement_id": engagement.metsights_engagement_id,
            "organization_id": engagement.organization_id,
            "engagement_code": engagement.engagement_code,
            "engagement_type": engagement.engagement_type,
            "assessment_package_id": engagement.assessment_package_id,
            "diagnostic_package_id": engagement.diagnostic_package_id or DEFAULT_B2C_DIAGNOSTIC_PACKAGE_ID,
            "city": engagement.city,
            "slot_duration": engagement.slot_duration,
            "start_date": engagement.start_date,
            "end_date": engagement.end_date,
            "status": engagement.status,
            "participant_count": engagement.participant_count,
        }
    )


@router.put("/{engagement_id}")
async def update_engagement(
    engagement_id: int,
    payload: EngagementUpdateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    updated = await engagements_service.update_engagement_for_employee(
        db,
        employee=employee,
        engagement_id=engagement_id,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response({"engagement_id": updated.engagement_id})


@router.get("/code/{engagement_code}/occupied-slots")
async def get_engagement_occupied_slots(
    engagement_code: str,
    db: AsyncSession = Depends(get_db),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    occupied = await engagements_service.get_occupied_slots_for_engagement_code(
        db,
        engagement_code=engagement_code,
    )

    return success_response({"occupied_slots": occupied})


@router.get("/public/occupied-slots")
async def get_public_occupied_slots(
    db: AsyncSession = Depends(get_db),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    occupied = await engagements_service.get_occupied_slots_for_active_b2c_engagements(db)

    return success_response({"occupied_slots": occupied})


@router.patch("/{engagement_id}/status")
async def update_engagement_status(
    engagement_id: int,
    payload: EngagementStatusUpdateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    updated = await engagements_service.change_engagement_status_for_employee(
        db,
        employee=employee,
        engagement_id=engagement_id,
        status=payload.status,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response({"engagement_id": updated.engagement_id, "status": updated.status})


@router.get("/code/{engagement_code}/participants")
async def get_engagement_participants_by_code(
    engagement_code: str,
    request: Request,
    page: int = 1,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    """Get all distinct users enrolled in a specific engagement by code."""

    # Validate pagination parameters
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    # Fetch participants from service
    participants, total = await engagements_service.list_participants_for_engagement_code(
        db,
        employee=employee,
        engagement_code=engagement_code,
        page=page,
        limit=limit,
    )

    return success_response(participants, meta={"page": page, "limit": limit, "total": total})


@router.get("/public/participants")
async def get_public_engagement_participants(
    request: Request,
    page: int = 1,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    """Get all distinct users enrolled in all B2C engagements."""
    
    # Validate pagination parameters
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    # Fetch participants from service
    participants, total = await engagements_service.list_participants_for_b2c_engagements(
        db,
        employee=employee,
        page=page,
        limit=limit,
    )

    return success_response(participants, meta={"page": page, "limit": limit, "total": total})


@router.get("/{engagement_id}/onboarding-assistants")
async def list_onboarding_assistants_for_engagement(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: OnboardingAssistantsService = Depends(get_onboarding_assistants_service),
):
    """Returns all employees assigned to a specific engagement as onboarding assistants."""
    data = await service.list_onboarding_assistants_for_engagement(
        db,
        employee=employee,
        engagement_id=engagement_id,
    )
    return success_response(data)


@router.post("/{engagement_id}/onboarding-assistants", status_code=201)
async def assign_onboarding_assistants_to_engagement(
    engagement_id: int,
    payload: OnboardingAssistantsAddRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: OnboardingAssistantsService = Depends(get_onboarding_assistants_service),
):
    """Assigns one or more employees to an engagement as onboarding assistants."""
    data = await service.assign_onboarding_assistants_to_engagement(
        db,
        employee=employee,
        engagement_id=engagement_id,
        employee_ids=payload.employee_ids,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@router.delete("/{engagement_id}/onboarding-assistants/{employee_id}")
async def remove_onboarding_assistant_from_engagement(
    engagement_id: int,
    employee_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: OnboardingAssistantsService = Depends(get_onboarding_assistants_service),
):
    """Removes an employee's assignment from an engagement."""
    data = await service.remove_onboarding_assistant_from_engagement(
        db,
        employee=employee,
        engagement_id=engagement_id,
        employee_id=employee_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)
