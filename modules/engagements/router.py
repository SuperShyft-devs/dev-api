"""Engagements HTTP routes."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Body, Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.listing import PageParamsDep
from common.responses import success_response
from core.dependencies import get_current_user
from core.exceptions import AppError
from db.session import get_db
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext

# NOTE: The occupied-slots endpoints are intentionally public (no auth).
from modules.engagement_notifications.schemas import configs_to_api
from modules.engagements.dependencies import (
    get_engagement_feedback_service,
    get_engagements_service,
    get_onboarding_assistants_service,
)
from modules.engagements.engagement_feedback_service import EngagementFeedbackService
from modules.engagements.onboarding_assistants_service import OnboardingAssistantsService
from modules.assessments.dependencies import get_assessment_packages_service
from modules.assessments.packages_service import AssessmentPackagesService
from modules.diagnostics.dependencies import get_diagnostics_service
from modules.diagnostics.service import DiagnosticsService
from modules.engagement_types.service import EngagementTypesService
from modules.experts.dependencies import get_expert_types_service
from modules.experts.service import ExpertTypesService
from modules.notifications.dependencies import get_notifications_service
from modules.notifications.service import NotificationsService
from modules.organizations.dependencies import get_organizations_service
from modules.organizations.service import OrganizationsService
from modules.engagements.schemas import (
    AssignParticipantsBatchRequest,
    ClearParticipantClinicalDataRequest,
    ConsultationConsentRequest,
    CreateMetsightsProfilesRequest,
    EngagementCreateRequest,
    EngagementNotificationOutput,
    EngagementParticipantUpdateRequest,
    ParticipantBloodBookingAdminCreate,
    ParticipantBloodBookingAdminUpdate,
    EngagementRescheduleRequest,
    EngagementStatusUpdateRequest,
    EngagementUpdateRequest,
    SlotCapacityUpdateRequest,
    LoadBloodReportsForParticipantsRequest,
    LoadBioaiReportsForParticipantsRequest,
    RemoveReportsForParticipantsRequest,
    MoveParticipantRequest,
    MoveParticipantsBatchRequest,
    CreatePhleboRequest,
    OnboardingAssistantsAddRequest,
    ResolveHealthiansZoneRequest,
)
from modules.engagements.models import Engagement
from modules.engagements.participant_list_filters import filters_to_meta, parse_participant_list_filters
from modules.engagements.service import EngagementsService
from modules.metsights.dependencies import get_metsights_sync_service
from modules.reports.dependencies import get_reports_service
from modules.reports.service import ReportsService
from modules.metsights.sync_service import MetsightsSyncService

router = APIRouter(prefix="/engagements", tags=["engagements"])


def _get_engagement_types_service() -> EngagementTypesService:
    return EngagementTypesService()


def _consultation_filters_from_query(request: Request) -> dict[str, str]:
    filters: dict[str, str] = {}
    for key, value in request.query_params.multi_items():
        if not key.startswith("consultation_"):
            continue
        if value not in {"yes", "no"}:
            continue
        filters[key.removeprefix("consultation_")] = value
    return filters


def _participant_filters_from_request(
    request: Request,
    *,
    search: str | None = None,
    engagement_date: date | None = None,
    booking_date: date | None = None,
    department: str | None = None,
    has_booking_id: str | None = None,
    reports_ready: str | None = None,
):
    return parse_participant_list_filters(
        search=search,
        engagement_date=engagement_date,
        booking_date=booking_date,
        department=department,
        has_booking_id=has_booking_id,
        reports_ready=reports_ready,
        consultation_filters=_consultation_filters_from_query(request),
    )


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()

    if request.client is None:
        return "unknown"

    return request.client.host


def _engagement_payment_fields(engagement: Engagement) -> dict[str, object]:
    return {
        "payment_type": (
            engagement.payment_type.value
            if getattr(engagement, "payment_type", None) is not None
            and hasattr(engagement.payment_type, "value")
            else (str(engagement.payment_type) if getattr(engagement, "payment_type", None) else "paid_by_group")
        ),
        "auto_booking": bool(engagement.auto_booking) if engagement.auto_booking is not None else False,
    }


def _engagement_to_dict(
    engagement: Engagement,
    *,
    readiness=None,
    participant_count: int | None = None,
    notifications: list[dict] | None = None,
    slot_detail: dict | None = None,
    public_slot_detail: dict | None = None,
) -> dict:
    data = {
        "engagement_id": engagement.engagement_id,
        "engagement_name": engagement.engagement_name,
        "metsights_engagement_id": engagement.metsights_engagement_id,
        "organization_id": engagement.organization_id,
        "camp_no": engagement.camp_no,
        "engagement_code": engagement.engagement_code,
        "engagement_type": engagement.engagement_type,
        "consultations": engagement.consultations,
        "slot_detail": slot_detail,
        "public_slot_detail": public_slot_detail,
        "slot_detail_id": engagement.slot_detail_id,
        "assessment_package_id": engagement.assessment_package_id,
        "diagnostic_package_id": engagement.diagnostic_package_id,
        "diagnostic_package_id_male": engagement.diagnostic_package_id_male,
        "diagnostic_package_id_female": engagement.diagnostic_package_id_female,
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
        "consultation_mode": engagement.consultation_mode.value if engagement.consultation_mode else None,
        "create_profile_on_metsights": engagement.create_profile_on_metsights,
        "enroll_for_fitprint_full": engagement.enroll_for_fitprint_full,
        "load_prev_assessment_questionnaires": engagement.load_prev_assessment_questionnaires,
        "load_prev_questionnaire_category_keys": list(engagement.load_prev_questionnaire_category_keys or [])
        or None,
        "enforce_metsights_pro_gate": bool(engagement.enforce_metsights_pro_gate),
        "notifications": notifications or [],
        **_engagement_payment_fields(engagement),
    }
    if readiness is not None:
        data["readiness"] = readiness.model_dump(mode="json")
    return data


async def _engagement_notifications_payload(
    db: AsyncSession,
    engagement_id: int,
) -> list[dict]:
    from modules.engagement_notifications.repository import EngagementNotificationsRepository
    from modules.notification_events.repository import NotificationEventsRepository

    repo = EngagementNotificationsRepository()
    events_repo = NotificationEventsRepository()
    rows = await repo.list_for_engagement(db, engagement_id)
    events_by_id = {}
    for row in rows:
        if row.notification_event_id not in events_by_id:
            event = await events_repo.get_by_id(db, row.notification_event_id)
            if event:
                events_by_id[row.notification_event_id] = event

    data = []
    for row in rows:
        event = events_by_id.get(row.notification_event_id)
        data.append(
            EngagementNotificationOutput(
                notification_event_id=row.notification_event_id,
                notification_services=configs_to_api(row.notification_services),
                event_code=event.event_code if event else None,
                event_display_name=event.display_name if event else None,
            ).model_dump()
        )
    return data


@router.get("/code/{engagement_code}")
async def get_engagement_by_code(
    engagement_code: str,
    db: AsyncSession = Depends(get_db),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    row = await engagements_service.get_engagement_by_code_public(db, engagement_code=engagement_code)
    if row is None:
        raise AppError(status_code=404, error_code="NOT_FOUND", message="Engagement not found")
    engagement, org_name, org_logo, engagement_type_code = row
    slot_detail = await engagements_service.public_slot_detail_for_engagement(db, engagement)
    return success_response({
        "engagement_name": engagement.engagement_name,
        "organization_name": org_name,
        "organization_logo_url": org_logo,
        "engagement_code": engagement.engagement_code,
        "engagement_type": engagement_type_code,
        "slot_duration": engagement.slot_duration,
        "status": engagement.status,
        "consultations": engagement.consultations,
        "consultation_mode": engagement.consultation_mode.value if engagement.consultation_mode else None,
        "diagnostic_package_id": engagement.diagnostic_package_id,
        "slot_detail": slot_detail,
        **_engagement_payment_fields(engagement),
    })


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


@router.get("/form-bootstrap")
async def get_engagement_form_bootstrap(
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
    packages_service: AssessmentPackagesService = Depends(get_assessment_packages_service),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
    engagement_types_service: EngagementTypesService = Depends(_get_engagement_types_service),
    expert_types_service: ExpertTypesService = Depends(get_expert_types_service),
    notifications_service: NotificationsService = Depends(get_notifications_service),
    organizations_service: OrganizationsService = Depends(get_organizations_service),
):
    payload = await engagements_service.get_form_bootstrap(
        db,
        employee=employee,
        packages_service=packages_service,
        diagnostics_service=diagnostics_service,
        engagement_types_service=engagement_types_service,
        expert_types_service=expert_types_service,
        notifications_service=notifications_service,
        organizations_service=organizations_service,
    )
    return success_response(payload)


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
    pagination: PageParamsDep,
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
    engagements, total, readiness_by_id, counts_by_id = await engagements_service.list_engagements_for_employee(
        db,
        employee=employee,
        page=pagination.page,
        limit=pagination.limit,
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

    slot_details_map = await engagements_service.resolve_slot_details_map(db, engagements)
    filter_options = await engagements_service.get_engagement_filter_options_for_employee(
        db, employee=employee
    )

    data = [
        _engagement_to_dict(
            engagement,
            readiness=readiness_by_id[engagement.engagement_id],
            participant_count=counts_by_id.get(int(engagement.engagement_id), 0),
            slot_detail=(
                slot_details_map.get(int(engagement.slot_detail_id))
                if engagement.slot_detail_id is not None
                else None
            ),
        )
        for engagement in engagements
    ]

    return success_response(
        data,
        meta={
            "page": pagination.page,
            "limit": pagination.limit,
            "total": total,
            "filter_options": filter_options,
        },
    )


@router.get("/data-completeness-summary")
async def list_engagements_data_completeness_summary(
    org_id: int | None = None,
    camp_no: int | None = None,
    status: str | None = None,
    city: str | None = None,
    engagement_type: str | None = None,
    audience: str | None = None,
    search: str | None = None,
    sort_by: str | None = None,
    sort_dir: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    date: date | None = None,
    include_participant_issues: bool = Query(default=False),
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    """Rollup and per-engagement data completeness for filtered engagements.

    Pass ``include_participant_issues=true`` to also return flat participant
    issue rows (missing blood slot, questionnaire, blood report, or Bio-AI report).
    """
    data = await engagements_service.list_engagements_data_completeness_summary(
        db,
        employee=employee,
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
        limit=limit,
        include_participant_issues=include_participant_issues,
    )
    return success_response(data)


@router.get("/{engagement_id}/consultation")
async def get_engagement_consultations_for_user(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    """Return the user's consultation bookings and engagement consultation config."""
    data = await engagements_service.get_consultations_for_user(
        db,
        user_id=current_user.user_id,
        engagement_id=engagement_id,
    )
    return success_response(data)


@router.patch("/{engagement_id}/reschedule")
async def reschedule_blood_collection(
    engagement_id: int,
    payload: EngagementRescheduleRequest,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    """Allow an enrolled user to reschedule their blood-collection date/slot."""
    data = await engagements_service.reschedule_blood_collection_for_user(
        db,
        user_id=current_user.user_id,
        engagement_id=engagement_id,
        payload=payload,
    )
    await db.commit()
    return success_response(data)




async def _engagement_admin_detail_payload(
    db: AsyncSession,
    *,
    engagement: Engagement,
    engagements_service: EngagementsService,
) -> dict:
    engagement_id = int(engagement.engagement_id)
    notifications = await _engagement_notifications_payload(db, engagement_id)
    return _engagement_to_dict(
        engagement,
        participant_count=await engagements_service.count_participants_for_engagement(
            db,
            engagement_id=engagement_id,
        ),
        notifications=notifications,
        slot_detail=await engagements_service.resolve_slot_detail(db, engagement),
        public_slot_detail=await engagements_service.public_slot_detail_for_engagement(db, engagement),
    )


@router.get("/by-code/{engagement_code}")
async def get_engagement_details_by_code(
    engagement_code: str,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    engagement = await engagements_service.get_engagement_by_code_for_employee(
        db,
        employee=employee,
        engagement_code=engagement_code,
    )
    return success_response(
        await _engagement_admin_detail_payload(
            db,
            engagement=engagement,
            engagements_service=engagements_service,
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
        await _engagement_admin_detail_payload(
            db,
            engagement=engagement,
            engagements_service=engagements_service,
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


@router.patch("/{engagement_id}/slot-capacity")
async def update_slot_capacity(
    engagement_id: int,
    payload: SlotCapacityUpdateRequest,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    data = await engagements_service.update_slot_capacity_for_employee(
        db,
        employee=employee,
        engagement_id=engagement_id,
        payload=payload,
    )
    await db.commit()
    return success_response(data)


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


@router.get("/camps/code/{camp_code}/onboarding")
async def get_public_camp_onboarding_by_code(
    camp_code: str,
    db: AsyncSession = Depends(get_db),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    data = await engagements_service.get_public_camp_onboarding_by_code(db, camp_code=camp_code)
    return success_response(data)


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


@router.get("/{engagement_id}/booking-dates")
async def get_engagement_booking_dates(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    """Distinct IST calendar dates when createBooking_v3 succeeded for this engagement."""

    data = await engagements_service.list_booking_dates_for_engagement_id(
        db,
        employee=employee,
        engagement_id=engagement_id,
    )
    return success_response(data)


@router.get("/{engagement_id}/booking-summary")
async def get_engagement_booking_summary(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    """Camp EOD booking summary for this engagement (one countable row per participant)."""

    data = await engagements_service.booking_summary_for_engagement_id(
        db,
        employee=employee,
        engagement_id=engagement_id,
    )
    return success_response(data)


@router.get("/{engagement_id}/participants/filter-options")
async def get_engagement_participant_filter_options(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    data = await engagements_service.participant_filter_options_for_engagement_id(
        db,
        employee=employee,
        engagement_id=engagement_id,
    )
    return success_response(data)


@router.get("/{engagement_id}/participants/stats")
async def get_engagement_participant_stats(
    engagement_id: int,
    request: Request,
    search: str | None = None,
    engagement_date: date | None = None,
    booking_date: date | None = None,
    department: str | None = None,
    has_booking_id: str | None = None,
    reports_ready: str | None = None,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    filters = _participant_filters_from_request(
        request,
        search=search,
        engagement_date=engagement_date,
        booking_date=booking_date,
        department=department,
        has_booking_id=has_booking_id,
        reports_ready=reports_ready,
    )
    data = await engagements_service.participant_stats_for_engagement_id(
        db,
        employee=employee,
        engagement_id=engagement_id,
        filters=filters,
    )
    return success_response(data, meta={"filters": filters_to_meta(filters)})


@router.get("/{engagement_id}/participants/bootstrap")
async def get_engagement_participants_bootstrap(
    engagement_id: int,
    page: int = 1,
    limit: int = 20,
    include_expert_types: bool = False,
    include_roster_progress: bool = True,
    include_questionnaire_maps: bool = True,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
    organizations_service: OrganizationsService = Depends(get_organizations_service),
    expert_types_service: ExpertTypesService = Depends(get_expert_types_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    engagement = await engagements_service.get_engagement_details_for_employee(
        db, employee=employee, engagement_id=engagement_id
    )
    org_id = engagement.organization_id
    organization = None
    if org_id is not None:
        org_row, industry = await organizations_service.get_organization_details_for_employee(
            db,
            employee=employee,
            organization_id=org_id,
        )
        organization = organizations_service.organization_to_details_dict(org_row, industry)

    booking_dates = await engagements_service.list_booking_dates_for_engagement_id(
        db, employee=employee, engagement_id=engagement_id
    )
    filter_options = await engagements_service.participant_filter_options_for_engagement_id(
        db, employee=employee, engagement_id=engagement_id
    )
    participants, total = await engagements_service.list_participants_for_engagement_id(
        db,
        employee=employee,
        engagement_id=engagement_id,
        page=page,
        limit=limit,
        filters=None,
    )
    public_slot_detail = await engagements_service.public_slot_detail_for_engagement(db, engagement)
    payload: dict = {
        "engagement": {
            "engagement_id": engagement.engagement_id,
            "engagement_name": engagement.engagement_name,
            "engagement_code": engagement.engagement_code,
            "organization_id": engagement.organization_id,
            "consultations": engagement.consultations,
            "public_slot_detail": public_slot_detail,
            "blood_collection_type": engagement.blood_collection_type,
            "payment_type": (
                engagement.payment_type.value
                if getattr(engagement, "payment_type", None) is not None
                and hasattr(engagement.payment_type, "value")
                else "paid_by_group"
            ),
            "auto_booking": bool(engagement.auto_booking) if engagement.auto_booking is not None else False,
            "participant_count": total,
        },
        "organization": organization,
        "booking_dates": booking_dates,
        "filter_options": filter_options,
        "participants": participants,
    }
    if include_roster_progress:
        participants, roster_summary, questionnaire_columns = (
            await engagements_service.attach_roster_progress_to_participants(
                db,
                engagement_id=engagement_id,
                participants=participants,
                include_questionnaire_maps=include_questionnaire_maps,
            )
        )
        payload["participants"] = participants
        if include_questionnaire_maps:
            payload["roster_summary"] = roster_summary
            if questionnaire_columns:
                payload["questionnaire_columns"] = questionnaire_columns
    if include_expert_types:
        types = await expert_types_service.list_expert_types(db)
        payload["expert_types"] = [
            {"id": t.id, "type_key": t.type_key, "type": t.type} for t in types
        ]
    return success_response(payload, meta={"page": page, "limit": limit, "total": total})


@router.get("/{engagement_id}/participants/{user_id}/clinical-snapshot")
async def get_engagement_participant_clinical_snapshot(
    engagement_id: int,
    user_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    data = await engagements_service.get_participant_clinical_snapshot_for_engagement(
        db,
        employee=employee,
        engagement_id=engagement_id,
        user_id=user_id,
    )
    return success_response(data)


@router.post("/{engagement_id}/participants/{user_id}/regenerate-bioai-report")
async def regenerate_engagement_participant_bio_ai_report(
    engagement_id: int,
    user_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    data = await engagements_service.regenerate_participant_bio_ai_report_for_engagement(
        db,
        employee=employee,
        engagement_id=engagement_id,
        user_id=user_id,
    )
    return success_response(data)


@router.patch("/{engagement_id}/participants/{user_id}/clinical-data")
async def clear_engagement_participant_clinical_data(
    engagement_id: int,
    user_id: int,
    body: ClearParticipantClinicalDataRequest,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    data = await engagements_service.clear_participant_clinical_data_for_engagement(
        db,
        employee=employee,
        engagement_id=engagement_id,
        user_id=user_id,
        product=body.product,
        field=body.field,
    )
    return success_response(data)


@router.get("/{engagement_id}/participants/ids")
async def get_engagement_participant_ids(
    engagement_id: int,
    request: Request,
    search: str | None = None,
    engagement_date: date | None = None,
    booking_date: date | None = None,
    department: str | None = None,
    has_booking_id: str | None = None,
    reports_ready: str | None = None,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    """User IDs for all participants matching the same filters as the list endpoint."""

    filters = _participant_filters_from_request(
        request,
        search=search,
        engagement_date=engagement_date,
        booking_date=booking_date,
        department=department,
        has_booking_id=has_booking_id,
        reports_ready=reports_ready,
    )
    data = await engagements_service.participant_user_ids_for_engagement_id(
        db,
        employee=employee,
        engagement_id=engagement_id,
        filters=filters,
    )
    return success_response(data, meta={"filters": filters_to_meta(filters)})


@router.post("/{engagement_id}/participants/load-blood-reports")
async def load_blood_reports_for_engagement_participants(
    engagement_id: int,
    payload: LoadBloodReportsForParticipantsRequest,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
    sync_service: MetsightsSyncService = Depends(get_metsights_sync_service),
):
    """Load blood reports for selected participants (same as cron job, no notifications)."""

    data = await engagements_service.load_blood_reports_for_participants(
        db,
        employee=employee,
        engagement_id=engagement_id,
        user_ids=payload.user_ids,
        sync_service=sync_service,
    )
    await db.commit()
    return success_response(data)


@router.post("/{engagement_id}/participants/reload-provider-blood-parameters")
async def reload_provider_blood_parameters_for_engagement_participants(
    engagement_id: int,
    request: Request,
    payload: LoadBloodReportsForParticipantsRequest,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
    reports_service: ReportsService = Depends(get_reports_service),
):
    """Re-fetch Healthians digital values and overwrite stored blood_parameters (reload=1)."""

    data = await engagements_service.reload_provider_blood_parameters_for_participants(
        db,
        employee=employee,
        engagement_id=engagement_id,
        user_ids=payload.user_ids,
        reports_service=reports_service,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@router.post("/{engagement_id}/participants/load-bioai-reports")
async def load_bioai_reports_for_engagement_participants(
    engagement_id: int,
    payload: LoadBioaiReportsForParticipantsRequest,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
    sync_service: MetsightsSyncService = Depends(get_metsights_sync_service),
):
    """Load BioAI reports for selected participants (same as cron job, no notifications)."""

    data = await engagements_service.load_bioai_reports_for_participants(
        db,
        employee=employee,
        engagement_id=engagement_id,
        user_ids=payload.user_ids,
        sync_service=sync_service,
    )
    await db.commit()
    return success_response(data)


@router.post("/{engagement_id}/participants/remove-reports")
async def remove_reports_for_engagement_participants(
    engagement_id: int,
    payload: RemoveReportsForParticipantsRequest,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    """Delete all individual health report rows for selected participants in an engagement."""

    data = await engagements_service.remove_reports_for_participants(
        db,
        employee=employee,
        engagement_id=engagement_id,
        user_ids=payload.user_ids,
    )
    await db.commit()
    return success_response(data)


@router.get("/{engagement_id}/feedback")
async def list_engagement_feedback(
    engagement_id: int,
    page: int = 1,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    feedback_service: EngagementFeedbackService = Depends(get_engagement_feedback_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
    rows, total = await feedback_service.list_for_engagement_admin(
        db,
        employee=employee,
        engagement_id=engagement_id,
        page=page,
        limit=limit,
    )
    return success_response(
        [row.model_dump(mode="json") for row in rows],
        meta={"page": page, "limit": limit, "total": total},
    )


@router.get("/{engagement_id}/participants")
async def get_engagement_participants_by_id(
    engagement_id: int,
    request: Request,
    page: int = 1,
    limit: int = 20,
    search: str | None = None,
    engagement_date: date | None = None,
    booking_date: date | None = None,
    department: str | None = None,
    has_booking_id: str | None = None,
    reports_ready: str | None = None,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    """Get distinct users enrolled in a specific engagement by id."""

    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    filters = _participant_filters_from_request(
        request,
        search=search,
        engagement_date=engagement_date,
        booking_date=booking_date,
        department=department,
        has_booking_id=has_booking_id,
        reports_ready=reports_ready,
    )

    participants, total = await engagements_service.list_participants_for_engagement_id(
        db,
        employee=employee,
        engagement_id=engagement_id,
        page=page,
        limit=limit,
        filters=filters,
    )
    participants, _, _ = await engagements_service.attach_roster_progress_to_participants(
        db,
        engagement_id=engagement_id,
        participants=participants,
        include_questionnaire_maps=False,
    )

    return success_response(
        participants,
        meta={
            "page": page,
            "limit": limit,
            "total": total,
            "filters": filters_to_meta(filters),
        },
    )


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


@router.post("/{engagement_id}/participants/move")
async def move_participants_to_engagement(
    engagement_id: int,
    payload: MoveParticipantsBatchRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    data = await engagements_service.move_participants_to_engagement_for_employee(
        db,
        employee=employee,
        source_engagement_id=engagement_id,
        user_ids=payload.user_ids,
        target_engagement_id=payload.target_engagement_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@router.post("/{engagement_id}/participants/{user_id}/move")
async def move_participant_to_engagement(
    engagement_id: int,
    user_id: int,
    payload: MoveParticipantRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    data = await engagements_service.move_participants_to_engagement_for_employee(
        db,
        employee=employee,
        source_engagement_id=engagement_id,
        user_ids=[user_id],
        target_engagement_id=payload.target_engagement_id,
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


@router.post("/{engagement_id}/participants/{user_id}/blood-bookings")
async def create_participant_blood_booking(
    engagement_id: int,
    user_id: int,
    payload: ParticipantBloodBookingAdminCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    data = await engagements_service.create_participant_blood_booking_for_employee(
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


@router.patch("/{engagement_id}/participants/{user_id}/blood-bookings/{blood_booking_id}")
async def update_participant_blood_booking(
    engagement_id: int,
    user_id: int,
    blood_booking_id: int,
    payload: ParticipantBloodBookingAdminUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    data = await engagements_service.update_participant_blood_booking_for_employee(
        db,
        employee=employee,
        engagement_id=engagement_id,
        user_id=user_id,
        blood_booking_id=blood_booking_id,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@router.delete("/{engagement_id}/participants/{user_id}/blood-bookings/{blood_booking_id}")
async def delete_participant_blood_booking(
    engagement_id: int,
    user_id: int,
    blood_booking_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    data = await engagements_service.delete_participant_blood_booking_for_employee(
        db,
        employee=employee,
        engagement_id=engagement_id,
        user_id=user_id,
        blood_booking_id=blood_booking_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@router.post("/{engagement_id}/consultation/{consultation_id}/consent")
async def update_consultation_consent(
    engagement_id: int,
    consultation_id: int,
    payload: ConsultationConsentRequest,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    data = await engagements_service.update_consultation_consent_for_user(
        db,
        user_id=current_user.user_id,
        engagement_id=engagement_id,
        consultation_id=consultation_id,
        payload=payload,
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



@router.get("/{engagement_id}/data-completeness")
async def get_engagement_data_completeness(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    """Blood/Bio AI/questionnaire completeness counts for enrolled participants."""
    data = await engagements_service.get_data_completeness_for_engagement(
        db,
        employee=employee,
        engagement_id=engagement_id,
    )
    return success_response(data)


@router.get("/{engagement_id}/camp-overview")
async def get_engagement_camp_overview(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    """B2B camp progress + day-wise summary (org engagements only)."""
    from core.exceptions import AppError
    from modules.engagements.camp_overview import build_camp_overview

    engagement = await engagements_service.get_engagement_details_for_employee(
        db, employee=employee, engagement_id=engagement_id
    )
    if engagement.organization_id is None:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message="Camp overview is only available for B2B (organization) engagements",
        )
    slot_detail = await engagements_service.resolve_slot_detail(db, engagement)
    data = await build_camp_overview(db, engagement=engagement, slot_detail=slot_detail)
    return success_response(data)


@router.get("/{engagement_id}/onboarding-assistants")
async def list_onboarding_assistants_for_engagement(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: OnboardingAssistantsService = Depends(get_onboarding_assistants_service),
):
    """Returns all partners assigned to a specific engagement as onboarding assistants."""
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
    """Assigns phlebo/expert partners and/or admin/inferior_admin employees."""
    data = await service.assign_onboarding_assistants_to_engagement(
        db,
        employee=employee,
        engagement_id=engagement_id,
        partner_ids=payload.partner_ids,
        employee_ids=payload.employee_ids,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@router.post("/{engagement_id}/onboarding-assistants/create-phlebo", status_code=201)
async def create_phlebo_onboarding_assistant(
    engagement_id: int,
    payload: CreatePhleboRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: OnboardingAssistantsService = Depends(get_onboarding_assistants_service),
):
    """Creates (or reuses) a phlebo partner and assigns them to an engagement."""
    data = await service.create_and_assign_phlebo(
        db,
        employee=employee,
        engagement_id=engagement_id,
        name=payload.name,
        phone=payload.phone,
        confirm_existing=payload.confirm_existing,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@router.delete("/{engagement_id}/onboarding-assistants/partners/{partner_id}")
async def remove_partner_onboarding_assistant_from_engagement(
    engagement_id: int,
    partner_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: OnboardingAssistantsService = Depends(get_onboarding_assistants_service),
):
    """Removes a partner assignment from an engagement."""
    data = await service.remove_onboarding_assistant_from_engagement(
        db,
        employee=employee,
        engagement_id=engagement_id,
        partner_id=partner_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@router.delete("/{engagement_id}/onboarding-assistants/employees/{employee_id}")
async def remove_employee_onboarding_assistant_from_engagement(
    engagement_id: int,
    employee_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: OnboardingAssistantsService = Depends(get_onboarding_assistants_service),
):
    """Removes a staff-employee assignment from an engagement."""
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


@router.delete("/{engagement_id}/onboarding-assistants/{partner_id}")
async def remove_onboarding_assistant_from_engagement(
    engagement_id: int,
    partner_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: OnboardingAssistantsService = Depends(get_onboarding_assistants_service),
):
    """Legacy remove path — partner assignment only."""
    data = await service.remove_onboarding_assistant_from_engagement(
        db,
        employee=employee,
        engagement_id=engagement_id,
        partner_id=partner_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)
