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
    QuestionnaireCategoryProgressRefreshPageRequest,
)
from modules.platform_settings.service import PlatformSettingsService
from modules.questionnaire.dependencies import get_questionnaire_user_service_readonly
from modules.questionnaire.service import QuestionnaireService
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


@router.get("/questionnaire-category-progress/refresh-stats")
async def get_questionnaire_category_progress_refresh_stats(
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    questionnaire_service: QuestionnaireService = Depends(get_questionnaire_user_service_readonly),
):
    _ = employee
    data = await questionnaire_service.get_category_progress_refresh_stats(db)
    return success_response(data)


@router.post("/questionnaire-category-progress/refresh-page")
@limiter.limit("300/minute")
async def refresh_questionnaire_category_progress_page(
    payload: QuestionnaireCategoryProgressRefreshPageRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    questionnaire_service: QuestionnaireService = Depends(get_questionnaire_user_service_readonly),
    platform_service: PlatformSettingsService = Depends(get_platform_settings_service),
):
    """Recompute category progress for one assessment instance (paginated backfill)."""

    result = await questionnaire_service.refresh_category_progress_page(db, offset=payload.offset)
    if not result.get("has_more"):
        await platform_service.log_maintenance_event(
            db,
            employee=employee,
            action="EMPLOYEE_REFRESH_ALL_QUESTIONNAIRE_CATEGORY_PROGRESS",
            endpoint=str(request.url.path),
            ip_address=_client_ip(request),
            user_agent=request.headers.get("User-Agent", "unknown"),
        )
    await db.commit()
    return success_response(result)


@router.post("/questionnaire-category-progress/refresh-all")
@limiter.limit("30/minute")
async def refresh_questionnaire_category_progress_all(
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    questionnaire_service: QuestionnaireService = Depends(get_questionnaire_user_service_readonly),
    platform_service: PlatformSettingsService = Depends(get_platform_settings_service),
):
    """Recompute per-category complete/incomplete for every assessment instance (single long request)."""

    result = await questionnaire_service.refresh_all_category_progress(db)
    await platform_service.log_maintenance_event(
        db,
        employee=employee,
        action="EMPLOYEE_REFRESH_ALL_QUESTIONNAIRE_CATEGORY_PROGRESS",
        endpoint=str(request.url.path),
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
    )
    await db.commit()
    return success_response(result)


@router.post("/metsights-profiles/import-page")
@limiter.limit("300/minute")
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
