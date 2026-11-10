"""Users HTTP routes."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Body, Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.listing import sanitize_list_search
from common.masking import mask_email, mask_phone
from common.responses import success_response
from common.validation import ValidationError
from core.dependencies import get_current_user
from core.exceptions import AppError
from core.network import get_client_ip
from core.rate_limit import limiter
from db.session import get_db
from modules.auth.dependencies import get_auth_service
from modules.auth.service import AuthService
from modules.engagements.dependencies import get_engagement_feedback_service
from modules.engagements.engagement_feedback_schemas import EngagementFeedbackSubmitRequest
from modules.engagements.engagement_feedback_service import EngagementFeedbackService
from modules.users.dependencies import get_participant_journey_service, get_users_service
from modules.users.participant_journey_service import ParticipantJourneyService
from modules.employee.dependencies import get_current_employee, get_optional_employee
from modules.employee.service import EmployeeContext
from modules.employee.models import EmployeeRole
from modules.employee.permissions import PermissionAction, context_task_allows, permission_denied
from modules.export_logs.dependencies import get_export_logs_service
from modules.export_logs.schemas import RevealContactRequest
from modules.export_logs.service import ExportLogsService
from modules.metsights.dependencies import get_metsights_sync_service
from modules.metsights.sync_service import MetsightsSyncService
from modules.users.schemas import (
    BookBioAiRequest,
    EmployeeCreateUserRequest,
    EmployeeUpdateUserRequest,
    EngagementOnboardConfirmPaymentRequest,
    EngagementOnboardVerifyPayRequest,
    EngagementUserOnboardRequest,
    FamilyOnboardConfirmPaymentRequest,
    FamilyOnboardStartRequest,
    FamilyOnboardVerifyPayRequest,
    ImportMetsightsProfilesRequest,
    MetsightsSyncRecordsRequest,
    ParticipantJourneyCopyQuestionnairesRequest,
    UpcomingSlotResponse,
    PublicUserOnboardRequest,
    SubProfileCreate,
    SubProfileResponse,
    SubProfileUpdate,
    UnlinkRequest,
    UserPreferencesUpdate,
    UpdateMetsightsProfileIdRequest,
    UpdateMyProfileRequest,
    UserAddressCreate,
    UserAddressUpdate,
    VifcQuickStartRequest,
)
from modules.users.service import UsersService


router = APIRouter(prefix="/users", tags=["users"])


@router.post("/public/family-onboard")
@limiter.limit("3/minute")
async def start_family_onboard(
    payload: FamilyOnboardStartRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    users_service: UsersService = Depends(get_users_service),
):
    result = await users_service.start_family_onboard(
        db,
        payload=payload,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(result.model_dump())


@router.post("/public/family-onboard/verify-pay")
@limiter.limit("5/minute")
async def verify_family_onboard_pay(
    payload: FamilyOnboardVerifyPayRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    users_service: UsersService = Depends(get_users_service),
):
    result = await users_service.verify_family_onboard_pay(
        db,
        payload=payload,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
        client_ip=get_client_ip(request),
    )
    await db.commit()
    return success_response(result)


@router.post("/public/family-onboard/confirm-payment")
@limiter.limit("5/minute")
async def confirm_family_onboard_payment(
    payload: FamilyOnboardConfirmPaymentRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    result = await users_service.confirm_family_onboard_payment(
        db,
        payload=payload,
        caller_user_id=current_user.user_id,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(result)


@router.post("/public/onboard")
@limiter.limit("3/minute")
async def public_onboard_user(
    payload: PublicUserOnboardRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    users_service: UsersService = Depends(get_users_service),
):
    result = await users_service.public_onboard_user(
        db,
        payload=payload,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response(result.model_dump())


@router.post("/public/vifc/quick-start")
@limiter.limit("3/minute")
async def public_vifc_quick_start(
    payload: VifcQuickStartRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    users_service: UsersService = Depends(get_users_service),
):
    result = await users_service.public_vifc_quick_start(
        db,
        payload=payload,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response(result.model_dump())


@router.post("/code/{engagement_code}/onboard")
@limiter.limit("3/minute")
async def onboard_user_for_engagement(
    engagement_code: str,
    payload: EngagementUserOnboardRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    users_service: UsersService = Depends(get_users_service),
):
    result = await users_service.onboard_user_for_engagement(
        db,
        engagement_code=engagement_code,
        payload=payload,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response(result.model_dump())


@router.post("/code/{engagement_code}/onboard/verify-pay")
@limiter.limit("5/minute")
async def verify_engagement_onboard_pay(
    engagement_code: str,
    payload: EngagementOnboardVerifyPayRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    users_service: UsersService = Depends(get_users_service),
):
    result = await users_service.verify_engagement_onboard_pay(
        db,
        engagement_code=engagement_code,
        payload=payload,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
        client_ip=get_client_ip(request),
    )
    await db.commit()
    return success_response(result)


@router.post("/code/{engagement_code}/onboard/confirm-payment")
@limiter.limit("5/minute")
async def confirm_engagement_onboard_payment(
    engagement_code: str,
    payload: EngagementOnboardConfirmPaymentRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    result = await users_service.confirm_engagement_onboard_payment(
        db,
        engagement_code=engagement_code,
        payload=payload,
        caller_user_id=current_user.user_id,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(result)


@router.post("/code/{engagement_code}/onboard/me")
@limiter.limit("3/minute")
async def onboard_and_login_user_for_engagement(
    engagement_code: str,
    payload: EngagementUserOnboardRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    users_service: UsersService = Depends(get_users_service),
    auth_service: AuthService = Depends(get_auth_service),
):
    """B2B onboard into an engagement, then issue session tokens (same shape as verify-otp)."""
    ip_address = get_client_ip(request)
    user_agent = request.headers.get("User-Agent", "unknown")
    endpoint = str(request.url.path)

    result = await users_service.onboard_user_for_engagement(
        db,
        engagement_code=engagement_code,
        payload=payload,
        ip_address=ip_address,
        user_agent=user_agent,
        endpoint=endpoint,
    )
    tokens = await auth_service.issue_tokens_for_user(
        db,
        user_id=result.user_id,
        ip_address=ip_address,
        user_agent=user_agent,
        endpoint=endpoint,
    )
    await db.commit()

    return success_response(
        {
            "user_id": result.user_id,
            "tokens": {
                "access_token": tokens.access_token,
                "refresh_token": tokens.refresh_token,
                "token_type": "bearer",
            },
        }
    )


@router.get("/me")
async def get_me(
    current_user=Depends(get_current_user),
    employee: EmployeeContext | None = Depends(get_optional_employee),
):
    data = {
        "user_id": current_user.user_id,
        "first_name": current_user.first_name,
        "last_name": current_user.last_name,
        "age": current_user.age,
        "phone": current_user.phone,
        "email": current_user.email,
        "profile_photo": current_user.profile_photo,
        "date_of_birth": current_user.date_of_birth,
        "gender": current_user.gender,
        "address": current_user.address,
        "pin_code": current_user.pin_code,
        "city": current_user.city,
        "state": current_user.state,
        "country": current_user.country,
        "referred_by": current_user.referred_by,
        "is_participant": current_user.is_participant,
        "status": current_user.status,
        "created_at": current_user.created_at,
        "updated_at": current_user.updated_at,
    }
    if employee is not None:
        data["employee"] = {
            "employee_id": employee.employee_id,
            "role": employee.role,
        }
        if employee.role == EmployeeRole.inferior_admin:
            task_permissions: dict[str, dict[str, dict[str, bool]]] = {}
            for composite_key, task_grant in employee.task_permissions.items():
                category_key, task_key = composite_key.split(".", 1)
                task_permissions.setdefault(category_key, {})[task_key] = {
                    "can_view": task_grant.can_view,
                    "can_edit": task_grant.can_edit,
                }
            data["employee"]["permissions"] = {
                "version": employee.permissions_version,
                "categories": {
                    key: {
                        "can_view": grant.can_view,
                        "can_edit": grant.can_edit,
                        **(
                            {"tasks": task_permissions[key]}
                            if key in task_permissions
                            else {}
                        ),
                    }
                    for key, grant in sorted(employee.permissions.items())
                },
            }
    return success_response(data)


@router.get("/me/addresses")
async def list_my_addresses(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    rows = await users_service.list_my_addresses(
        db,
        user_id=current_user.user_id,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response([row.model_dump(mode="json") for row in rows])


@router.post("/me/addresses")
async def create_my_address(
    payload: UserAddressCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    row = await users_service.create_my_address(
        db,
        user_id=current_user.user_id,
        payload=payload,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(row.model_dump(mode="json"))


@router.put("/me/addresses/{user_address_id}")
async def update_my_address(
    user_address_id: int,
    payload: UserAddressUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    row = await users_service.update_my_address(
        db,
        user_id=current_user.user_id,
        user_address_id=user_address_id,
        payload=payload,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(row.model_dump(mode="json"))


@router.delete("/me/addresses/{user_address_id}")
async def delete_my_address(
    user_address_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    await users_service.delete_my_address(
        db,
        user_id=current_user.user_id,
        user_address_id=user_address_id,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"deleted": True})


@router.get("/me/upcoming-slot")
async def get_my_upcoming_slot(
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    result: UpcomingSlotResponse = await users_service.get_upcoming_slots(db, user_id=current_user.user_id)
    return success_response(result.model_dump())


@router.get("/me/consultations")
async def list_my_consultations(
    status: Literal["scheduled", "completed", "cancelled"] | None = Query(None),
    page: int = 1,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    data, total = await users_service.list_my_consultations(
        db,
        user_id=current_user.user_id,
        status=status,
        page=page,
        limit=limit,
    )
    return success_response(data, meta={"page": page, "limit": limit, "total": total})


@router.get("/me/appointments")
async def list_my_appointments(
    status: Literal["scheduled", "completed", "cancelled"] | None = Query(None),
    page: int = 1,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    data, total = await users_service.list_my_appointments(
        db,
        user_id=current_user.user_id,
        status=status,
        page=page,
        limit=limit,
    )
    return success_response(data, meta={"page": page, "limit": limit, "total": total})


@router.post("/{user_id}/book-bio-ai")
async def book_bio_ai_for_user(
    user_id: int,
    payload: BookBioAiRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    employee: EmployeeContext | None = Depends(get_optional_employee),
    users_service: UsersService = Depends(get_users_service),
):
    """Bio AI booking for a user (self, family primary booking for a child, or staff on behalf of any user).

    Creates a confirmed booking and runs fulfillment immediately. Does not create Razorpay orders or rows
    in ``orders`` / ``order_bookings`` / ``payments``.

    Response ``data`` matches ``POST /users/public/onboard`` (``user_id``, ``created``, ``is_participant``,
    ``engagement_id``, ``engagement_code``, ``engagement_participant_id``, ``assessment_instance_id``, ``metsights_record_id``).
    For this route ``created`` is always false (the user already exists).
    """

    result = await users_service.book_bio_ai_for_user_without_payment(
        db,
        current_user=current_user,
        target_user_id=user_id,
        employee=employee,
        payload=payload,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    return success_response(result.model_dump())


@router.get("/me/profiles")
async def get_my_profiles(
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    rows = await users_service.get_profiles(db, current_user=current_user)
    data: list[SubProfileResponse] = []
    for row in rows:
        data.append(
            SubProfileResponse(
                user_id=row.user_id,
                first_name=row.first_name or "",
                last_name=row.last_name or "",
                age=row.age,
                date_of_birth=row.date_of_birth,
                gender=row.gender or "",
                relationship=row.relationship or "self",
                phone=row.phone or "",
                email=row.email or "",
                parent_id=row.parent_id,
                status=row.status or "",
            )
        )
    return success_response([item.model_dump() for item in data])


@router.post("/me/profiles")
async def create_my_sub_profile(
    payload: SubProfileCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    row = await users_service.create_sub_profile(
        db,
        current_user=current_user,
        data=payload,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(
        SubProfileResponse(
            user_id=row.user_id,
            first_name=row.first_name or "",
            last_name=row.last_name or "",
            age=row.age,
            date_of_birth=row.date_of_birth,
            gender=row.gender or "",
            relationship=row.relationship or "",
            phone=row.phone or "",
            email=row.email or "",
            parent_id=row.parent_id,
            status=row.status or "",
        ).model_dump()
    )


@router.put("/me/profiles/{user_id}")
async def update_my_sub_profile(
    user_id: int,
    payload: SubProfileUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    row = await users_service.update_sub_profile(
        db,
        current_user=current_user,
        target_user_id=user_id,
        data=payload,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(
        SubProfileResponse(
            user_id=row.user_id,
            first_name=row.first_name or "",
            last_name=row.last_name or "",
            age=row.age,
            date_of_birth=row.date_of_birth,
            gender=row.gender or "",
            relationship=row.relationship or "",
            phone=row.phone or "",
            email=row.email or "",
            parent_id=row.parent_id,
            status=row.status or "",
        ).model_dump()
    )


@router.post("/me/unlink")
async def unlink_my_profile(
    payload: UnlinkRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    row = await users_service.unlink_profile(
        db,
        current_user=current_user,
        data=payload,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(
        {
            "user_id": row.user_id,
            "first_name": row.first_name,
            "last_name": row.last_name,
            "age": row.age,
            "phone": row.phone,
            "email": row.email,
            "profile_photo": row.profile_photo,
            "date_of_birth": row.date_of_birth,
            "gender": row.gender,
            "address": row.address,
            "pin_code": row.pin_code,
            "city": row.city,
            "state": row.state,
            "country": row.country,
            "referred_by": row.referred_by,
            "is_participant": row.is_participant,
            "status": row.status,
            "parent_id": row.parent_id,
            "relationship": row.relationship,
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }
    )


@router.put("/me")
async def update_me(
    payload: UpdateMyProfileRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    updated = await users_service.update_my_profile(
        db,
        user_id=current_user.user_id,
        payload=payload,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response(
        {
            "user_id": updated.user_id,
            "first_name": updated.first_name,
            "last_name": updated.last_name,
            "age": updated.age,
            "phone": updated.phone,
            "email": updated.email,
            "profile_photo": updated.profile_photo,
            "date_of_birth": updated.date_of_birth,
            "gender": updated.gender,
            "address": updated.address,
            "pin_code": updated.pin_code,
            "city": updated.city,
            "state": updated.state,
            "country": updated.country,
            "referred_by": updated.referred_by,
            "is_participant": updated.is_participant,
            "status": updated.status,
            "created_at": updated.created_at,
            "updated_at": updated.updated_at,
        }
    )


@router.get("/me/preferences")
async def get_my_preferences(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    preference = await users_service.get_user_preferences(
        db,
        user_id=current_user.user_id,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )

    await db.commit()
    return success_response(
        {
            "preference_id": preference.preference_id,
            "user_id": preference.user_id,
            "push_enabled": preference.push_enabled,
            "email_enabled": preference.email_enabled,
            "sms_enabled": preference.sms_enabled,
            "access_to_files": preference.access_to_files,
            "store_downloaded_files": preference.store_downloaded_files,
            "diet_preference": preference.diet_preference,
            "allergies": preference.allergies,
            "sports_playlists": preference.sports_playlists,
            "updated_at": preference.updated_at,
        }
    )


@router.put("/me/preferences")
async def update_my_preferences(
    payload: UserPreferencesUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    preference = await users_service.update_user_preferences(
        db,
        user_id=current_user.user_id,
        data=payload,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response(
        {
            "preference_id": preference.preference_id,
            "user_id": preference.user_id,
            "push_enabled": preference.push_enabled,
            "email_enabled": preference.email_enabled,
            "sms_enabled": preference.sms_enabled,
            "access_to_files": preference.access_to_files,
            "store_downloaded_files": preference.store_downloaded_files,
            "diet_preference": preference.diet_preference,
            "allergies": preference.allergies,
            "sports_playlists": preference.sports_playlists,
            "updated_at": preference.updated_at,
        }
    )


@router.get("/me/status")
async def get_my_status(
    current_user=Depends(get_current_user),
):
    status = (current_user.status or "inactive").lower()
    return success_response(
        {
            "user_id": current_user.user_id,
            "status": status,
            "is_active": status == "active",
        }
    )


@router.get("/me/engagement-feedback/pending")
async def get_my_pending_engagement_feedback(
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    feedback_service: EngagementFeedbackService = Depends(get_engagement_feedback_service),
):
    result = await feedback_service.get_pending_for_user(db, user_id=current_user.user_id)
    return success_response(result.model_dump())


@router.get("/me/engagements/{engagement_id}/feedback")
async def get_my_engagement_feedback(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    feedback_service: EngagementFeedbackService = Depends(get_engagement_feedback_service),
):
    result = await feedback_service.get_for_user_engagement(
        db,
        user_id=current_user.user_id,
        engagement_id=engagement_id,
    )
    return success_response(result.model_dump(mode="json"))


@router.post("/me/engagements/{engagement_id}/feedback", status_code=201)
async def submit_my_engagement_feedback(
    engagement_id: int,
    payload: EngagementFeedbackSubmitRequest,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    feedback_service: EngagementFeedbackService = Depends(get_engagement_feedback_service),
):
    result = await feedback_service.submit_for_user_engagement(
        db,
        user_id=current_user.user_id,
        engagement_id=engagement_id,
        payload=payload,
    )
    await db.commit()
    return success_response(result.model_dump(mode="json"))


@router.post("")
@limiter.limit("5/minute")
async def employee_create_user(
    payload: EmployeeCreateUserRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    users_service: UsersService = Depends(get_users_service),
):
    user, created = await users_service.create_user_by_employee(
        db,
        payload=payload,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    if created:
        from modules.notifications.user_registration_notify import notify_user_registered

        await notify_user_registered(db, user=user)
        await db.commit()

    return success_response({"user_id": user.user_id, "created": created})


@router.post("/import-metsights-profiles")
@limiter.limit("5/minute")
async def import_metsights_profiles(
    payload: ImportMetsightsProfilesRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    users_service: UsersService = Depends(get_users_service),
):
    """Import Metsights profiles: upsert user, then one B2C engagement per non-FitPrint record (oldest first)."""

    result = await users_service.import_metsights_profiles_by_employee(
        db,
        employee=employee,
        metsights_profile_ids=payload.metsights_profile_ids,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(result)


@router.get("/stats")
async def employee_get_user_stats(
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    users_service: UsersService = Depends(get_users_service),
):
    stats = await users_service.get_participant_metsights_stats_for_employee(db, employee=employee)
    return success_response(stats)


@router.get("/duplicates")
async def employee_list_duplicate_users(
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    users_service: UsersService = Depends(get_users_service),
):
    groups = await users_service.list_duplicate_phone_users_for_employee(db, employee=employee)
    data = []
    for group in groups:
        data.append(
            {
                "key": "".join(ch for ch in (group[0].phone or "") if ch.isdigit())[-10:],
                "users": [
                    {
                        "user_id": user.user_id,
                        "first_name": user.first_name,
                        "last_name": user.last_name,
                        "phone": mask_phone(user.phone),
                        "email": mask_email(user.email),
                        "status": user.status,
                    }
                    for user in group
                ],
            }
        )
    data.sort(key=lambda g: len(g["users"]), reverse=True)
    return success_response(data)


@router.get("")
async def employee_list_users(
    request: Request,
    page: int = 1,
    limit: int = 20,
    phone: str | None = None,
    email: str | None = None,
    status: str | None = None,
    is_participant: bool | None = None,
    search: str | None = None,
    sort_by: str | None = None,
    sort_dir: str | None = None,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    users_service: UsersService = Depends(get_users_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    try:
        search = sanitize_list_search(search)
    except ValidationError as exc:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message=str(exc)) from exc

    users, total = await users_service.list_users_for_employee(
        db,
        employee=employee,
        page=page,
        limit=limit,
        phone=phone,
        email=email,
        status=status,
        is_participant=is_participant,
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )

    stats = await users_service.get_participant_metsights_counts_for_employee(db, employee=employee)
    protected_user_ids = sorted(await users_service._protected_user_ids(db))

    data = []
    for user in users:
        data.append(
            {
                "user_id": user.user_id,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "age": user.age,
                "phone": mask_phone(user.phone),
                "email": mask_email(user.email),
                "profile_photo": user.profile_photo,
                "date_of_birth": user.date_of_birth,
                "city": user.city,
                "status": user.status,
                "is_participant": user.is_participant,
                "metsights_profile_id": user.metsights_profile_id,
                "created_at": user.created_at,
                "updated_at": user.updated_at,
            }
        )

    return success_response(
        data,
        meta={
            "page": page,
            "limit": limit,
            "total": total,
            "with_metsights_profile": stats.get("with_metsights_profile"),
            "total_participants": stats.get("total_participants"),
            "protected_user_ids": protected_user_ids,
        },
    )


@router.get("/{user_id}/participant-journey/page-bootstrap")
async def employee_get_participant_journey_page_bootstrap(
    user_id: int,
    page: int = 1,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    users_service: UsersService = Depends(get_users_service),
    journey_service: ParticipantJourneyService = Depends(get_participant_journey_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    user = await users_service.get_user_details_for_employee(db, employee=employee, user_id=user_id)
    journey_data, journey_meta = await journey_service.get_summary(
        db,
        employee=employee,
        user_id=user_id,
        page=page,
        limit=limit,
    )
    user_payload = {
        "user_id": user.user_id,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "age": user.age,
        "phone": mask_phone(user.phone),
        "email": mask_email(user.email),
        "profile_photo": user.profile_photo,
        "date_of_birth": user.date_of_birth,
        "gender": user.gender,
        "city": user.city,
        "status": user.status,
        "is_participant": user.is_participant,
        "metsights_profile_id": user.metsights_profile_id,
        "created_at": user.created_at,
        "updated_at": user.updated_at,
    }
    return success_response(
        {"user": user_payload, "journey": journey_data},
        meta=journey_meta,
    )


@router.get("/{user_id}/participant-journey")
async def employee_get_participant_journey_summary(
    user_id: int,
    page: int = 1,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    journey_service: ParticipantJourneyService = Depends(get_participant_journey_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    data, meta = await journey_service.get_summary(
        db,
        employee=employee,
        user_id=user_id,
        page=page,
        limit=limit,
    )
    return success_response(data, meta=meta)


@router.get("/{user_id}/participant-journey/overview")
async def employee_get_participant_journey_overview(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    journey_service: ParticipantJourneyService = Depends(get_participant_journey_service),
):
    data = await journey_service.get_overview(
        db,
        employee=employee,
        user_id=user_id,
    )
    return success_response(data)


@router.post("/{user_id}/metsights/sync-records")
async def sync_user_metsights_completed_records(
    request: Request,
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    employee: EmployeeContext | None = Depends(get_optional_employee),
    sync_service: MetsightsSyncService = Depends(get_metsights_sync_service),
    payload: MetsightsSyncRecordsRequest = Body(default_factory=MetsightsSyncRecordsRequest),
):
    """Import completed Metsights assessments as local assessment instances (self or employee)."""

    if (
        employee is not None
        and employee.role == EmployeeRole.inferior_admin
        and user_id != current_user.user_id
        and not context_task_allows(
            employee, "users", "metsights_sync", PermissionAction.edit
        )
    ):
        raise permission_denied("users", PermissionAction.edit)

    result = await sync_service.sync_completed_metsights_records(
        db,
        target_user_id=user_id,
        current_user_id=current_user.user_id,
        employee_ok=employee is not None,
        engagement_code=payload.engagement_code,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(result)


@router.get("/{user_id}/participant-journey/{assessment_instance_id}")
async def employee_get_participant_journey_detail(
    user_id: int,
    assessment_instance_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    journey_service: ParticipantJourneyService = Depends(get_participant_journey_service),
):
    detail = await journey_service.get_instance_detail(
        db,
        employee=employee,
        user_id=user_id,
        assessment_instance_id=assessment_instance_id,
    )
    return success_response(detail)


@router.post("/{user_id}/participant-journey/{assessment_instance_id}/copy-questionnaires")
async def employee_copy_participant_journey_questionnaires(
    user_id: int,
    assessment_instance_id: int,
    payload: ParticipantJourneyCopyQuestionnairesRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    journey_service: ParticipantJourneyService = Depends(get_participant_journey_service),
):
    data = await journey_service.copy_questionnaires_from_instance(
        db,
        employee=employee,
        user_id=user_id,
        dest_assessment_instance_id=assessment_instance_id,
        source_assessment_instance_id=int(payload.source_assessment_instance_id),
        category_keys=payload.category_keys,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@router.get("/{user_id}")
async def employee_get_user(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    users_service: UsersService = Depends(get_users_service),
):
    user = await users_service.get_user_details_for_employee(db, employee=employee, user_id=user_id)

    return success_response(
        {
            "user_id": user.user_id,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "age": user.age,
            "phone": mask_phone(user.phone),
            "email": mask_email(user.email),
            "profile_photo": user.profile_photo,
            "date_of_birth": user.date_of_birth,
            "gender": user.gender,
            "address": user.address,
            "pin_code": user.pin_code,
            "city": user.city,
            "state": user.state,
            "country": user.country,
            "referred_by": user.referred_by,
            "is_participant": user.is_participant,
            "status": user.status,
            "metsights_profile_id": user.metsights_profile_id,
            "created_at": user.created_at,
            "updated_at": user.updated_at,
        }
    )


@router.post("/{user_id}/reveal-contact")
async def employee_reveal_user_contact(
    user_id: int,
    payload: RevealContactRequest,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    users_service: UsersService = Depends(get_users_service),
    export_logs_service: ExportLogsService = Depends(get_export_logs_service),
):
    user = await users_service.get_user_details_for_employee(db, employee=employee, user_id=user_id)
    await export_logs_service.create_contact_reveal(
        db,
        employee_id=employee.employee_id,
        user_id=user.user_id,
        reason=payload.reason,
    )
    await db.commit()
    return success_response(
        {
            "user_id": user.user_id,
            "phone": user.phone,
            "email": user.email,
        }
    )


@router.put("/{user_id}/metsights-profile-id")
async def employee_update_metsights_profile_id(
    user_id: int,
    payload: UpdateMetsightsProfileIdRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    users_service: UsersService = Depends(get_users_service),
):
    user = await users_service.update_metsights_profile_id_by_employee(
        db,
        employee=employee,
        user_id=user_id,
        metsights_profile_id=payload.metsights_profile_id,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response(
        {
            "user_id": user.user_id,
            "metsights_profile_id": user.metsights_profile_id,
        }
    )


@router.post("/{user_id}/retry-metsights-profile")
async def employee_retry_metsights_profile(
    user_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    users_service: UsersService = Depends(get_users_service),
):
    user = await users_service.retry_metsights_profile_by_employee(
        db,
        employee=employee,
        user_id=user_id,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response(
        {
            "user_id": user.user_id,
            "metsights_profile_id": user.metsights_profile_id,
        }
    )


@router.put("/{user_id}")
async def employee_update_user(
    user_id: int,
    payload: EmployeeUpdateUserRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    users_service: UsersService = Depends(get_users_service),
):
    user = await users_service.update_user_by_employee(
        db,
        employee=employee,
        user_id=user_id,
        payload=payload,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response({"user_id": user.user_id, "status": user.status})


@router.patch("/{user_id}/deactivate")
async def employee_deactivate_user(
    user_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    users_service: UsersService = Depends(get_users_service),
):
    user = await users_service.deactivate_user_by_employee(
        db,
        employee=employee,
        user_id=user_id,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response({"user_id": user.user_id, "status": user.status})


@router.get("/{user_id}/delete-impact")
async def employee_get_user_delete_impact(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    users_service: UsersService = Depends(get_users_service),
):
    """Engagements that would have no participants left after deleting this user (and sub-profiles)."""
    result = await users_service.get_delete_user_impact_by_employee(
        db, employee=employee, user_id=user_id
    )
    return success_response(result)


@router.delete("/{user_id}")
async def employee_delete_user(
    user_id: int,
    request: Request,
    delete_orphan_engagements: bool = Query(
        False,
        description="When true, permanently delete engagements that would have no participants after this user is removed.",
    ),
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    users_service: UsersService = Depends(get_users_service),
):
    result = await users_service.delete_user_by_employee(
        db,
        employee=employee,
        user_id=user_id,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
        delete_orphan_engagements=delete_orphan_engagements,
    )
    await db.commit()
    return success_response(result)
