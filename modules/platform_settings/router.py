"""Employee-only routes for platform settings."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.rate_limit import limiter
from db.session import get_db
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.platform_settings.dependencies import get_platform_settings_service
from modules.platform_settings.schemas import (
    B2cOnboardingDefaultsRead,
    B2cOnboardingDefaultsUpdate,
    MetsightsProfilesImportPageRequest,
)
from modules.platform_settings.service import PlatformSettingsService
from modules.users.dependencies import get_users_service
from modules.users.service import UsersService

router = APIRouter(prefix="/platform-settings", tags=["platform-settings"])


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client is None:
        return "unknown"
    return request.client.host


@router.get("/b2c-onboarding")
async def get_b2c_onboarding_defaults(
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: PlatformSettingsService = Depends(get_platform_settings_service),
):
    data = await service.get_b2c_onboarding_defaults(db)
    return success_response(data.model_dump())


@router.patch("/b2c-onboarding")
async def patch_b2c_onboarding_defaults(
    payload: B2cOnboardingDefaultsUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: PlatformSettingsService = Depends(get_platform_settings_service),
):
    data: B2cOnboardingDefaultsRead = await service.update_b2c_onboarding_defaults(
        db,
        employee=employee,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data.model_dump())


@router.get("/metsights-profiles/stats")
async def get_metsights_profiles_stats(
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    users_service: UsersService = Depends(get_users_service),
):
    _ = employee
    data = await users_service.get_metsights_profile_import_stats(db)
    return success_response(data)


@router.post("/metsights-profiles/import-page")
@limiter.limit("120/minute")
async def import_metsights_profiles_page(
    payload: MetsightsProfilesImportPageRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    users_service: UsersService = Depends(get_users_service),
):
    _ = employee
    result = await users_service.import_metsights_profiles_page(db, page=payload.page)
    await db.commit()
    return success_response(result)
