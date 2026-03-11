"""Users HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.dependencies import get_current_user
from db.session import get_db
from modules.users.dependencies import get_users_service
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.users.schemas import (
    EmployeeCreateUserRequest,
    EmployeeUpdateUserRequest,
    EngagementUserOnboardRequest,
    PublicUserOnboardRequest,
    UpdateMyProfileRequest,
)
from modules.users.service import UsersService


router = APIRouter(prefix="/users", tags=["users"])


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()

    if request.client is None:
        return "unknown"

    return request.client.host


@router.post("/public/onboard")
async def public_onboard_user(
    payload: PublicUserOnboardRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    users_service: UsersService = Depends(get_users_service),
):
    result = await users_service.public_onboard_user(
        db,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response(result.model_dump())


@router.post("/code/{engagement_code}/onboard")
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
        ip_address=_client_ip(request),
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
            "phone": current_user.phone,
            "email": current_user.email,
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
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response(
        {
            "user_id": updated.user_id,
            "first_name": updated.first_name,
            "last_name": updated.last_name,
            "phone": updated.phone,
            "email": updated.email,
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
async def employee_create_user(
    payload: EmployeeCreateUserRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    users_service: UsersService = Depends(get_users_service),
):
    user = await users_service.create_user_by_employee(
        db,
        payload=payload,
        ip_address=_client_ip(request),
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
        from core.exceptions import AppError

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
                "phone": user.phone,
                "email": user.email,
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
            "phone": user.phone,
            "email": user.email,
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
        ip_address=_client_ip(request),
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
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response({"user_id": user.user_id, "status": user.status})
