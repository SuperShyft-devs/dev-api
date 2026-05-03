"""Users HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Body, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.dependencies import get_current_user
from core.exceptions import AppError
from core.network import get_client_ip
from core.rate_limit import limiter
from db.session import get_db
from modules.users.dependencies import get_participant_journey_service, get_users_service
from modules.users.participant_journey_service import ParticipantJourneyService
from modules.employee.dependencies import get_current_employee, get_optional_employee
from modules.employee.service import EmployeeContext
from modules.metsights.dependencies import get_metsights_sync_service
from modules.metsights.sync_service import MetsightsSyncService
from modules.users.schemas import (
    BookBioAiRequest,
    EmployeeCreateUserRequest,
    EmployeeUpdateUserRequest,
    EngagementUserOnboardRequest,
    MetsightsSyncRecordsRequest,
    UpcomingSlotResponse,
    PublicUserOnboardRequest,
    SubProfileCreate,
    SubProfileResponse,
    SubProfileUpdate,
    UnlinkRequest,
    UserPreferencesUpdate,
    UpdateMyProfileRequest,
)
from modules.users.service import UsersService


router = APIRouter(prefix="/users", tags=["users"])


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


@router.get("/me")
async def get_me(
    current_user=Depends(get_current_user),
):
    return success_response(
        {
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
    )


@router.get("/me/upcoming-slot")
async def get_my_upcoming_slot(
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    result: UpcomingSlotResponse = await users_service.get_upcoming_slots(db, user_id=current_user.user_id)
    return success_response(result.model_dump())


@router.post("/me/book-bio-ai")
async def book_bio_ai_for_current_user(
    payload: BookBioAiRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    result = await users_service.book_bio_ai_for_authenticated_user(
        db,
        user=current_user,
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


@router.post("")
@limiter.limit("5/minute")
async def employee_create_user(
    payload: EmployeeCreateUserRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    users_service: UsersService = Depends(get_users_service),
):
    user = await users_service.create_user_by_employee(
        db,
        payload=payload,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response({"user_id": user.user_id})


@router.get("")
async def employee_list_users(
    request: Request,
    page: int = 1,
    limit: int = 20,
    phone: str | None = None,
    email: str | None = None,
    status: str | None = None,
    is_participant: bool | None = None,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    users_service: UsersService = Depends(get_users_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    users, total = await users_service.list_users_for_employee(
        db,
        employee=employee,
        page=page,
        limit=limit,
        phone=phone,
        email=email,
        status=status,
        is_participant=is_participant,
    )

    data = []
    for user in users:
        data.append(
            {
                "user_id": user.user_id,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "age": user.age,
                "phone": user.phone,
                "email": user.email,
                "profile_photo": user.profile_photo,
                "date_of_birth": user.date_of_birth,
                "city": user.city,
                "status": user.status,
                "is_participant": user.is_participant,
                "created_at": user.created_at,
                "updated_at": user.updated_at,
            }
        )

    return success_response(
        data,
        meta={"page": page, "limit": limit, "total": total},
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
            "phone": user.phone,
            "email": user.email,
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
            "created_at": user.created_at,
            "updated_at": user.updated_at,
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


@router.delete("/{user_id}")
async def employee_delete_user(
    user_id: int,
    request: Request,
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
    )
    await db.commit()
    return success_response(result)
