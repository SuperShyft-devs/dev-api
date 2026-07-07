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
from modules.notifications.repository import NotificationsRepository
from modules.platform_settings.repository import PlatformSettingsRepository
from modules.platform_settings.schemas import (
    B2cOnboardingDefaultsRead,
    B2cOnboardingDefaultsUpdate,
    EngagementNotificationDefaultsRead,
    EngagementNotificationDefaultsUpdate,
)

_FALLBACK_B2C_ASSESSMENT_PACKAGE_ID = 1


class PlatformSettingsService:
    def __init__(
        self,
        repository: PlatformSettingsRepository,
        audit_service: AuditService | None = None,
        notifications_repository: NotificationsRepository | None = None,
    ):
        self._repository = repository
        self._audit_service = audit_service
        self._notifications_repository = notifications_repository or NotificationsRepository()

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

    async def _validate_comma_separated_service_keys(self, db: AsyncSession, raw: str | None) -> str | None:
        if not raw:
            return None
        keys = [k.strip() for k in raw.split(",") if k.strip()]
        if not keys:
            return None
        for key in keys:
            svc = await self._notifications_repository.get_service_by_key(db, service_key=key)
            if svc is None:
                raise AppError(
                    status_code=404,
                    error_code="NOTIFICATION_SERVICE_NOT_FOUND",
                    message=f"Notification service '{key}' does not exist",
                )
            if not svc.is_active:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message=f"Notification service '{key}' is not active",
                )
        return ",".join(keys)

    @staticmethod
    def _validate_questionnaire_reminders_disjoint(qr1: str | None, qr2: str | None) -> None:
        keys1 = {k.strip() for k in (qr1 or "").split(",") if k.strip()}
        keys2 = {k.strip() for k in (qr2 or "").split(",") if k.strip()}
        overlap = keys1 & keys2
        if overlap:
            joined = ", ".join(sorted(overlap))
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=(
                    f"Notification service(s) {joined} cannot be used in both "
                    "questionnaire_reminder_1 and questionnaire_reminder_2"
                ),
            )

    async def get_engagement_notification_defaults(self, db: AsyncSession) -> EngagementNotificationDefaultsRead:
        row = await self._repository.get_by_id(db)
        if row is None:
            return EngagementNotificationDefaultsRead()
        return EngagementNotificationDefaultsRead(
            default_onboarding_notification=row.default_onboarding_notification,
            default_pretest_guidelines_notification=row.default_pretest_guidelines_notification,
            default_questionnaire_reminder_1=row.default_questionnaire_reminder_1,
            default_questionnaire_reminder_2=row.default_questionnaire_reminder_2,
            default_blood_report_notification=row.default_blood_report_notification,
            default_bioai_report_notification=row.default_bioai_report_notification,
        )

    async def update_engagement_notification_defaults(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        payload: EngagementNotificationDefaultsUpdate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> EngagementNotificationDefaultsRead:
        onboarding = await self._validate_comma_separated_service_keys(
            db, payload.default_onboarding_notification
        )
        pretest = await self._validate_comma_separated_service_keys(
            db, payload.default_pretest_guidelines_notification
        )
        qr1 = await self._validate_comma_separated_service_keys(db, payload.default_questionnaire_reminder_1)
        qr2 = await self._validate_comma_separated_service_keys(db, payload.default_questionnaire_reminder_2)
        blood = await self._validate_comma_separated_service_keys(db, payload.default_blood_report_notification)
        bioai = await self._validate_comma_separated_service_keys(db, payload.default_bioai_report_notification)
        self._validate_questionnaire_reminders_disjoint(qr1, qr2)

        a_id, d_id = await self.resolve_b2c_default_package_ids(db)
        await self._repository.upsert_notification_defaults(
            db,
            default_onboarding_notification=onboarding,
            default_pretest_guidelines_notification=pretest,
            default_questionnaire_reminder_1=qr1,
            default_questionnaire_reminder_2=qr2,
            default_blood_report_notification=blood,
            default_bioai_report_notification=bioai,
            updated_by_user_id=employee.user_id,
            assessment_package_id=a_id,
            diagnostic_package_id=d_id,
        )

        if self._audit_service is not None:
            await self._audit_service.log_event(
                db,
                action="EMPLOYEE_UPDATE_ENGAGEMENT_NOTIFICATION_DEFAULTS",
                endpoint=endpoint,
                ip_address=ip_address,
                user_agent=user_agent,
                user_id=employee.user_id,
                session_id=None,
            )

        return await self.get_engagement_notification_defaults(db)
