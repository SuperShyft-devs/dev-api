"""Engagements HTTP routes."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Body, Depends, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.dependencies import get_current_user
from core.exceptions import AppError
from db.session import get_db
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.engagements.models import EngagementParticipant

# NOTE: The occupied-slots endpoints are intentionally public (no auth).
from modules.engagements.dependencies import get_engagements_service, get_onboarding_assistants_service
from modules.engagements.onboarding_assistants_service import OnboardingAssistantsService
from modules.engagements.schemas import (
    AssignParticipantsBatchRequest,
    CreateMetsightsProfilesRequest,
    EngagementCreateRequest,
    EngagementParticipantUpdateRequest,
    EngagementStatusUpdateRequest,
    EngagementUpdateRequest,
    OnboardingAssistantsAddRequest,
    ResolveHealthiansZoneRequest,
)
from modules.engagements.models import Engagement
from modules.engagements.service import EngagementsService

router = APIRouter(prefix="/engagements", tags=["engagements"])


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()

    if request.client is None:
        return "unknown"

    return request.client.host


def _engagement_to_dict(
    engagement: Engagement,
    *,
    readiness=None,
    participant_count: int | None = None,
) -> dict:
    data = {
        "engagement_id": engagement.engagement_id,
        "engagement_name": engagement.engagement_name,
        "metsights_engagement_id": engagement.metsights_engagement_id,
        "organization_id": engagement.organization_id,
        "camp_no": engagement.camp_no,
        "engagement_code": engagement.engagement_code,
        "engagement_type": engagement.engagement_type.value if engagement.engagement_type else None,
        "assessment_package_id": engagement.assessment_package_id,
        "diagnostic_package_id": engagement.diagnostic_package_id,
        "city": engagement.city,
        "address": engagement.address,
        "sub_locality": engagement.sub_locality,
        "landmark": engagement.landmark,
        "pincode": engagement.pincode,
        "state": engagement.state,
        "country": engagement.country,
        "latitude": engagement.latitude,
        "longitude": engagement.longitude,
        "slot_duration": engagement.slot_duration,
        "start_date": engagement.start_date,
        "end_date": engagement.end_date,
        "status": engagement.status,
        "participant_count": participant_count if participant_count is not None else 0,
        "created_at": engagement.created_at.isoformat() if engagement.created_at else None,
        "healthians_zone_id": engagement.healthians_zone_id,
        "external_camp_id": engagement.external_camp_id,
        "blood_collection_type": engagement.blood_collection_type.value if engagement.blood_collection_type else None,
        "create_profile_on_metsights": engagement.create_profile_on_metsights,
        "enroll_for_fitprint_full": engagement.enroll_for_fitprint_full,
        "onboarding_notification": engagement.onboarding_notification,
        "pretest_guidelines_notification": engagement.pretest_guidelines_notification,
        "questionnaire_reminder_1": engagement.questionnaire_reminder_1,
        "questionnaire_reminder_2": engagement.questionnaire_reminder_2,
        "blood_report_notification": engagement.blood_report_notification,
        "bioai_report_notification": engagement.bioai_report_notification,
    }
    if readiness is not None:
        data["readiness"] = readiness.model_dump(mode="json")
    return data



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


@router.get("/filter-options")
async def get_engagement_filter_options(
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    options = await engagements_service.get_engagement_filter_options_for_employee(db, employee=employee)
    return success_response(options)


@router.post("/resolve-healthians-zone")
async def resolve_healthians_zone(
    payload: ResolveHealthiansZoneRequest,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    data = await engagements_service.resolve_healthians_zone_for_employee(
        db,
        employee=employee,
        payload=payload,
    )
    return success_response(data.model_dump())


@router.get("")
async def list_engagements(
    request: Request,
    page: int = 1,
    limit: int = 20,
    org_id: int | None = None,
    camp_no: int | None = None,
    status: str | None = None,
    city: str | None = None,
    engagement_type: str | None = None,
    audience: str | None = None,
    search: str | None = None,
    sort_by: str | None = None,
    sort_dir: str | None = None,
    date: date | None = None,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    engagements, total, readiness_by_id, counts_by_id = await engagements_service.list_engagements_for_employee(
        db,
        employee=employee,
        page=page,
        limit=limit,
        organization_id=org_id,
        camp_no=camp_no,
        status=status,
        city=city,
        on_date=date,
        search=search,
        engagement_type=engagement_type,
        audience=audience,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )

    data = [
        _engagement_to_dict(
            engagement,
            readiness=readiness_by_id[engagement.engagement_id],
            participant_count=counts_by_id.get(int(engagement.engagement_id), 0),
        )
        for engagement in engagements
    ]

    return success_response(data, meta={"page": page, "limit": limit, "total": total})



@router.get("/me/{engagement_id}")
async def get_engagement_for_user(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    """Allow a user to view an engagement they are enrolled in."""
    engagement = await engagements_service.get_by_id(db, engagement_id)
    if engagement is None:
        raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")

    participant_result = await db.execute(
        select(EngagementParticipant)
        .where(EngagementParticipant.engagement_id == engagement_id)
        .where(EngagementParticipant.user_id == current_user.user_id)
        .limit(1)
    )
    if participant_result.scalar_one_or_none() is None:
        raise AppError(status_code=403, error_code="ACCESS_DENIED", message="You are not a participant in this engagement")

    return success_response(
        _engagement_to_dict(
            engagement,
            participant_count=await engagements_service.count_participants_for_engagement(
                db,
                engagement_id=engagement_id,
            ),
        )
    )


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
        _engagement_to_dict(
            engagement,
            participant_count=await engagements_service.count_participants_for_engagement(
                db,
                engagement_id=engagement_id,
            ),
        )
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


@router.delete("/{engagement_id}")
async def delete_engagement(
    engagement_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    """Permanently delete an engagement and all engagement-scoped data. Users are not deleted."""
    data = await engagements_service.delete_engagement_for_employee(
        db,
        employee=employee,
        engagement_id=engagement_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


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


@router.get("/{engagement_id}/participants")
async def get_engagement_participants_by_id(
    engagement_id: int,
    page: int = 1,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    """Get all distinct users enrolled in a specific engagement by id."""

    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    participants, total = await engagements_service.list_participants_for_engagement_id(
        db,
        employee=employee,
        engagement_id=engagement_id,
        page=page,
        limit=limit,
    )

    return success_response(participants, meta={"page": page, "limit": limit, "total": total})


@router.delete("/{engagement_id}/participants")
async def remove_all_participants_from_engagement(
    engagement_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    data = await engagements_service.remove_all_participants_from_engagement_for_employee(
        db,
        employee=employee,
        engagement_id=engagement_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@router.delete("/{engagement_id}/participants/{user_id}")
async def remove_participant_from_engagement(
    engagement_id: int,
    user_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    data = await engagements_service.remove_participant_from_engagement_for_employee(
        db,
        employee=employee,
        engagement_id=engagement_id,
        user_id=user_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@router.patch("/{engagement_id}/participants/{user_id}")
async def update_participant(
    engagement_id: int,
    user_id: int,
    payload: EngagementParticipantUpdateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    data = await engagements_service.update_participant_for_employee(
        db,
        employee=employee,
        engagement_id=engagement_id,
        user_id=user_id,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@router.post("/{engagement_id}/assign-participants-batch")
async def assign_participants_batch(
    engagement_id: int,
    payload: AssignParticipantsBatchRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    """Enroll users by phone + email and assign assessment instances from Metsights CSV rows."""

    rows = [
        {"metsights_record_id": r.metsights_record_id, "phone": r.phone, "email": r.email}
        for r in payload.rows
    ]
    data = await engagements_service.assign_participants_batch(
        db,
        employee=employee,
        engagement_id=engagement_id,
        rows=rows,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@router.post("/{engagement_id}/create-metsights-profiles")
async def create_metsights_profiles_for_engagement(
    engagement_id: int,
    request: Request,
    payload: CreateMetsightsProfilesRequest = Body(default_factory=CreateMetsightsProfilesRequest),
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    """Create Metsights profiles for participants. Modes: enrol_force, enrol, profile."""

    data = await engagements_service.create_metsights_profiles_for_engagement_participants(
        db,
        employee=employee,
        engagement_id=engagement_id,
        mode=payload.mode,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@router.get("/{engagement_id}/questionnaire-status")
async def get_engagement_questionnaire_status(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    """Per-participant questionnaire draft/submitted status for an engagement."""
    data = await engagements_service.get_questionnaire_status_for_engagement(
        db,
        employee=employee,
        engagement_id=engagement_id,
    )
    return success_response(data)


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
