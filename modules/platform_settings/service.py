"""Business rules for platform settings."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.assessments.models import AssessmentPackage
from modules.diagnostics.models import DiagnosticPackage
from modules.employee.service import EmployeeContext
from modules.engagements.service import DEFAULT_B2C_DIAGNOSTIC_PACKAGE_ID
from modules.audit.service import AuditService
from modules.platform_settings.repository import PlatformSettingsRepository
from modules.platform_settings.schemas import B2cOnboardingDefaultsRead, B2cOnboardingDefaultsUpdate

_FALLBACK_B2C_ASSESSMENT_PACKAGE_ID = 1


class PlatformSettingsService:
    def __init__(self, repository: PlatformSettingsRepository, audit_service: AuditService | None = None):
        self._repository = repository
        self._audit_service = audit_service

    async def resolve_b2c_default_package_ids(self, db: AsyncSession) -> tuple[int, int]:
        row = await self._repository.get_by_id(db)
        if row is None:
            return _FALLBACK_B2C_ASSESSMENT_PACKAGE_ID, DEFAULT_B2C_DIAGNOSTIC_PACKAGE_ID
        return row.b2c_default_assessment_package_id, row.b2c_default_diagnostic_package_id

    async def ensure_active_b2c_packages(self, db: AsyncSession, assessment_package_id: int, diagnostic_package_id: int) -> None:
        ap = (
            await db.execute(select(AssessmentPackage).where(AssessmentPackage.package_id == assessment_package_id).limit(1))
        ).scalar_one_or_none()
        if ap is None or (ap.status or "").lower() != "active":
            raise AppError(
                status_code=422,
                error_code="INVALID_B2C_ASSESSMENT_PACKAGE",
                message="Assessment package is missing or not active",
            )

        dp = (
            await db.execute(
                select(DiagnosticPackage).where(DiagnosticPackage.diagnostic_package_id == diagnostic_package_id).limit(1)
            )
        ).scalar_one_or_none()
        if dp is None or (dp.status or "").lower() != "active":
            raise AppError(
                status_code=422,
                error_code="INVALID_B2C_DIAGNOSTIC_PACKAGE",
                message="Diagnostic package is missing or not active",
            )

    async def ensure_active_diagnostic_package(self, db: AsyncSession, diagnostic_package_id: int) -> None:
        """Validate a diagnostic package exists and is active (assessment optional)."""

        dp = (
            await db.execute(
                select(DiagnosticPackage).where(DiagnosticPackage.diagnostic_package_id == diagnostic_package_id).limit(1)
            )
        ).scalar_one_or_none()
        if dp is None or (dp.status or "").lower() != "active":
            raise AppError(
                status_code=422,
                error_code="INVALID_B2C_DIAGNOSTIC_PACKAGE",
                message="Diagnostic package is missing or not active",
            )

    async def get_b2c_onboarding_defaults(self, db: AsyncSession) -> B2cOnboardingDefaultsRead:
        a_id, d_id = await self.resolve_b2c_default_package_ids(db)
        return B2cOnboardingDefaultsRead(
            b2c_default_assessment_package_id=a_id,
            b2c_default_diagnostic_package_id=d_id,
        )

    async def update_b2c_onboarding_defaults(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        payload: B2cOnboardingDefaultsUpdate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> B2cOnboardingDefaultsRead:
        await self.ensure_active_b2c_packages(
            db,
            payload.b2c_default_assessment_package_id,
            payload.b2c_default_diagnostic_package_id,
        )
        await self._repository.upsert(
            db,
            assessment_package_id=payload.b2c_default_assessment_package_id,
            diagnostic_package_id=payload.b2c_default_diagnostic_package_id,
            updated_by_user_id=employee.user_id,
        )

        if self._audit_service is not None:
            await self._audit_service.log_event(
                db,
                action="EMPLOYEE_UPDATE_B2C_ONBOARDING_DEFAULTS",
                endpoint=endpoint,
                ip_address=ip_address,
                user_agent=user_agent,
                user_id=employee.user_id,
                session_id=None,
            )

        return await self.get_b2c_onboarding_defaults(db)
