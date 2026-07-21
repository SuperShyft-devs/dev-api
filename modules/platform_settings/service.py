"""Business rules for platform settings."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.assessments.models import AssessmentPackage
from modules.diagnostics.models import DiagnosticPackage
from modules.employee.access_control import ONBOARDING_ASSISTANT_ASSIGNEE_ROLES
from modules.employee.models import EmployeeRole
from modules.employee.repository import EmployeeRepository
from modules.employee.service import EmployeeContext
from modules.engagements.models import BloodCollectionType, EngagementKind
from modules.engagements.service import DEFAULT_B2C_DIAGNOSTIC_PACKAGE_ID
from modules.audit.service import AuditService
from modules.notifications.repository import NotificationsRepository
from modules.platform_settings.repository import (
    PlatformSettingsRepository,
    parse_comma_separated_employee_ids,
    serialize_comma_separated_employee_ids,
)
from modules.platform_settings.schemas import (
    B2cOnboardingDefaultsRead,
    B2cOnboardingDefaultsUpdate,
    DefaultOnboardingAssistantItem,
    DefaultOnboardingAssistantsRead,
    DefaultOnboardingAssistantsUpdate,
    EngagementNotificationDefaultsRead,
    EngagementNotificationDefaultsUpdate,
    SupportQueryNotificationRead,
    SupportQueryNotificationUpdate,
)

_FALLBACK_B2C_ASSESSMENT_PACKAGE_ID = 1
_FALLBACK_B2C_ENGAGEMENT_TYPE = EngagementKind.bio_ai
_FALLBACK_B2C_BLOOD_COLLECTION_TYPE: BloodCollectionType | None = None
_FALLBACK_B2C_CREATE_PROFILE_ON_METSIGHTS = True
_FALLBACK_B2C_ENROLL_FOR_FITPRINT_FULL = False


class PlatformSettingsService:
    def __init__(
        self,
        repository: PlatformSettingsRepository,
        audit_service: AuditService | None = None,
        notifications_repository: NotificationsRepository | None = None,
        employee_repository: EmployeeRepository | None = None,
    ):
        self._repository = repository
        self._audit_service = audit_service
        self._notifications_repository = notifications_repository or NotificationsRepository()
        self._employee_repository = employee_repository or EmployeeRepository()

    async def resolve_b2c_default_package_ids(self, db: AsyncSession) -> tuple[int, int]:
        row = await self._repository.get_by_id(db)
        if row is None:
            return _FALLBACK_B2C_ASSESSMENT_PACKAGE_ID, DEFAULT_B2C_DIAGNOSTIC_PACKAGE_ID
        return row.b2c_default_assessment_package_id, row.b2c_default_diagnostic_package_id

    async def resolve_b2c_onboarding_defaults(self, db: AsyncSession) -> B2cOnboardingDefaultsRead:
        row = await self._repository.get_by_id(db)
        if row is None:
            return B2cOnboardingDefaultsRead(
                b2c_default_assessment_package_id=_FALLBACK_B2C_ASSESSMENT_PACKAGE_ID,
                b2c_default_diagnostic_package_id=DEFAULT_B2C_DIAGNOSTIC_PACKAGE_ID,
                b2c_default_engagement_type=_FALLBACK_B2C_ENGAGEMENT_TYPE,
                b2c_default_blood_collection_type=_FALLBACK_B2C_BLOOD_COLLECTION_TYPE,
                b2c_default_create_profile_on_metsights=_FALLBACK_B2C_CREATE_PROFILE_ON_METSIGHTS,
                b2c_default_enroll_for_fitprint_full=_FALLBACK_B2C_ENROLL_FOR_FITPRINT_FULL,
            )
        return B2cOnboardingDefaultsRead(
            b2c_default_assessment_package_id=row.b2c_default_assessment_package_id,
            b2c_default_diagnostic_package_id=row.b2c_default_diagnostic_package_id,
            b2c_default_engagement_type=row.b2c_default_engagement_type,
            b2c_default_blood_collection_type=row.b2c_default_blood_collection_type,
            b2c_default_create_profile_on_metsights=bool(row.b2c_default_create_profile_on_metsights),
            b2c_default_enroll_for_fitprint_full=bool(row.b2c_default_enroll_for_fitprint_full),
        )

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
        return await self.resolve_b2c_onboarding_defaults(db)

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
        if payload.b2c_default_enroll_for_fitprint_full and not payload.b2c_default_create_profile_on_metsights:
            raise AppError(
                status_code=422,
                error_code="INVALID_B2C_ONBOARDING_DEFAULTS",
                message="FitPrint Full enrollment requires Metsights profile creation",
            )
        await self._repository.upsert(
            db,
            assessment_package_id=payload.b2c_default_assessment_package_id,
            diagnostic_package_id=payload.b2c_default_diagnostic_package_id,
            engagement_type=payload.b2c_default_engagement_type,
            blood_collection_type=payload.b2c_default_blood_collection_type,
            create_profile_on_metsights=payload.b2c_default_create_profile_on_metsights,
            enroll_for_fitprint_full=payload.b2c_default_enroll_for_fitprint_full,
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
            default_notify_users_for_consultation=row.default_notify_users_for_consultation,
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
        consultation = await self._validate_comma_separated_service_keys(
            db, payload.default_notify_users_for_consultation
        )
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
            default_notify_users_for_consultation=consultation,
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

    async def _validate_default_onboarding_assistant_ids(
        self,
        db: AsyncSession,
        employee_ids: list[int],
    ) -> list[int]:
        normalized: list[int] = []
        seen: set[int] = set()
        for raw in employee_ids:
            if not isinstance(raw, int) or raw <= 0 or raw in seen:
                continue
            seen.add(raw)
            row = await self._employee_repository.get_by_id_with_user_names(db, raw)
            if row is None:
                raise AppError(
                    status_code=404,
                    error_code="EMPLOYEE_NOT_FOUND",
                    message=f"Employee with ID {raw} does not exist",
                )
            emp, _first_name, _last_name = row
            if (emp.status or "").lower() != "active":
                raise AppError(
                    status_code=422,
                    error_code="INVALID_ONBOARDING_ASSISTANT",
                    message=f"Employee {raw} is not active",
                )
            if emp.role not in ONBOARDING_ASSISTANT_ASSIGNEE_ROLES:
                raise AppError(
                    status_code=422,
                    error_code="INVALID_ONBOARDING_ASSISTANT",
                    message=f"Employee {raw} cannot be assigned as an onboarding assistant",
                )
            normalized.append(raw)
        return normalized

    async def _build_default_onboarding_assistants_read(
        self,
        db: AsyncSession,
        employee_ids: list[int],
    ) -> DefaultOnboardingAssistantsRead:
        assistants: list[DefaultOnboardingAssistantItem] = []
        for emp_id in employee_ids:
            row = await self._employee_repository.get_by_id_with_user_names(db, emp_id)
            if row is None:
                continue
            emp, first_name, last_name = row
            assistants.append(
                DefaultOnboardingAssistantItem(
                    employee_id=emp.employee_id,
                    user_id=emp.user_id,
                    role=emp.role.value if isinstance(emp.role, EmployeeRole) else str(emp.role),
                    status=emp.status,
                    first_name=first_name,
                    last_name=last_name,
                )
            )
        return DefaultOnboardingAssistantsRead(employee_ids=employee_ids, assistants=assistants)

    async def get_default_onboarding_assistants(self, db: AsyncSession) -> DefaultOnboardingAssistantsRead:
        row = await self._repository.get_by_id(db)
        employee_ids = parse_comma_separated_employee_ids(
            row.default_onboarding_assistant_employee_ids if row else None
        )
        return await self._build_default_onboarding_assistants_read(db, employee_ids)

    async def update_default_onboarding_assistants(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        payload: DefaultOnboardingAssistantsUpdate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> DefaultOnboardingAssistantsRead:
        normalized_ids = await self._validate_default_onboarding_assistant_ids(db, payload.employee_ids)
        serialized = serialize_comma_separated_employee_ids(normalized_ids)
        a_id, d_id = await self.resolve_b2c_default_package_ids(db)
        await self._repository.upsert_default_onboarding_assistants(
            db,
            default_onboarding_assistant_employee_ids=serialized,
            updated_by_user_id=employee.user_id,
            assessment_package_id=a_id,
            diagnostic_package_id=d_id,
        )

        if self._audit_service is not None:
            await self._audit_service.log_event(
                db,
                action="EMPLOYEE_UPDATE_DEFAULT_ONBOARDING_ASSISTANTS",
                endpoint=endpoint,
                ip_address=ip_address,
                user_agent=user_agent,
                user_id=employee.user_id,
                session_id=None,
            )

        return await self._build_default_onboarding_assistants_read(db, normalized_ids)

    async def get_support_query_notification(self, db: AsyncSession) -> SupportQueryNotificationRead:
        row = await self._repository.get_by_id(db)
        if row is None:
            return SupportQueryNotificationRead()
        return SupportQueryNotificationRead(
            default_support_query_notification=row.default_support_query_notification,
        )

    async def update_support_query_notification(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        payload: SupportQueryNotificationUpdate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> SupportQueryNotificationRead:
        keys = await self._validate_comma_separated_service_keys(
            db, payload.default_support_query_notification
        )
        a_id, d_id = await self.resolve_b2c_default_package_ids(db)
        await self._repository.upsert_support_query_notification(
            db,
            default_support_query_notification=keys,
            updated_by_user_id=employee.user_id,
            assessment_package_id=a_id,
            diagnostic_package_id=d_id,
        )

        if self._audit_service is not None:
            await self._audit_service.log_event(
                db,
                action="EMPLOYEE_UPDATE_SUPPORT_QUERY_NOTIFICATION",
                endpoint=endpoint,
                ip_address=ip_address,
                user_agent=user_agent,
                user_id=employee.user_id,
                session_id=None,
            )

        return await self.get_support_query_notification(db)
