"""Business rules for platform settings."""

from __future__ import annotations

from typing import Any

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
    B2cOnboardingTypeDefaults,
    DefaultOnboardingAssistantItem,
    DefaultOnboardingAssistantsRead,
    DefaultOnboardingAssistantsUpdate,
    EngagementNotificationDefaultsRead,
    EngagementNotificationDefaultsUpdate,
    SupportQueryNotificationRead,
    SupportQueryNotificationUpdate,
)

_FALLBACK_B2C_ASSESSMENT_PACKAGE_ID = 1
_FALLBACK_B2C_BLOOD_COLLECTION_TYPE: BloodCollectionType | None = None
_FALLBACK_B2C_CREATE_PROFILE_ON_METSIGHTS = True
_FALLBACK_B2C_ENROLL_FOR_FITPRINT_FULL = False


def _fallback_type_defaults() -> B2cOnboardingTypeDefaults:
    return B2cOnboardingTypeDefaults(
        assessment_package_id=_FALLBACK_B2C_ASSESSMENT_PACKAGE_ID,
        diagnostic_package_id=DEFAULT_B2C_DIAGNOSTIC_PACKAGE_ID,
        blood_collection_type=_FALLBACK_B2C_BLOOD_COLLECTION_TYPE,
        create_profile_on_metsights=_FALLBACK_B2C_CREATE_PROFILE_ON_METSIGHTS,
        enroll_for_fitprint_full=_FALLBACK_B2C_ENROLL_FOR_FITPRINT_FULL,
    )


def _parse_type_defaults(raw: Any) -> B2cOnboardingTypeDefaults | None:
    if not isinstance(raw, dict):
        return None
    try:
        blood = raw.get("blood_collection_type")
        if blood is not None and not isinstance(blood, BloodCollectionType):
            blood = BloodCollectionType(blood)
        raw_diag = raw.get("diagnostic_package_id")
        diagnostic_package_id = None if raw_diag is None else int(raw_diag)
        return B2cOnboardingTypeDefaults(
            assessment_package_id=int(raw["assessment_package_id"]),
            diagnostic_package_id=diagnostic_package_id,
            blood_collection_type=blood,
            create_profile_on_metsights=bool(raw.get("create_profile_on_metsights", True)),
            enroll_for_fitprint_full=bool(raw.get("enroll_for_fitprint_full", False)),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _type_defaults_to_dict(defaults: B2cOnboardingTypeDefaults) -> dict[str, Any]:
    blood = defaults.blood_collection_type
    return {
        "assessment_package_id": defaults.assessment_package_id,
        "diagnostic_package_id": defaults.diagnostic_package_id,
        "blood_collection_type": blood.value if isinstance(blood, BloodCollectionType) else blood,
        "create_profile_on_metsights": defaults.create_profile_on_metsights,
        "enroll_for_fitprint_full": defaults.enroll_for_fitprint_full,
    }


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

    def _legacy_flat_as_type_defaults(self, row) -> B2cOnboardingTypeDefaults:
        blood = row.b2c_default_blood_collection_type
        if blood is not None and not isinstance(blood, BloodCollectionType):
            try:
                blood = BloodCollectionType(blood)
            except ValueError:
                blood = None
        return B2cOnboardingTypeDefaults(
            assessment_package_id=int(row.b2c_default_assessment_package_id),
            diagnostic_package_id=int(row.b2c_default_diagnostic_package_id),
            blood_collection_type=blood,
            create_profile_on_metsights=bool(row.b2c_default_create_profile_on_metsights),
            enroll_for_fitprint_full=bool(row.b2c_default_enroll_for_fitprint_full),
        )

    async def _list_active_engagement_type_codes(self, db: AsyncSession) -> list[str]:
        from modules.engagements.models import EngagementType

        result = await db.execute(
            select(EngagementType.code)
            .where(EngagementType.is_active.is_(True))
            .order_by(EngagementType.id.asc())
        )
        codes = [str(code) for code in result.scalars().all()]
        if codes:
            return codes
        # Fallback if engagement_types has not been seeded yet.
        return [kind.value for kind in EngagementKind]

    async def _build_defaults_map(
        self,
        db: AsyncSession,
        row,
    ) -> dict[str, B2cOnboardingTypeDefaults]:
        fallback = _fallback_type_defaults()
        legacy = self._legacy_flat_as_type_defaults(row) if row is not None else fallback
        raw_map = getattr(row, "b2c_onboarding_by_engagement_type", None) if row is not None else None
        result: dict[str, B2cOnboardingTypeDefaults] = {}
        for code in await self._list_active_engagement_type_codes(db):
            parsed = None
            if isinstance(raw_map, dict):
                parsed = _parse_type_defaults(raw_map.get(code))
            result[code] = parsed or legacy
        return result

    def _resolve_type_defaults_from_map(
        self,
        defaults_map: dict[str, B2cOnboardingTypeDefaults],
        engagement_type_code: str,
        *,
        raw_map: dict | None = None,
    ) -> B2cOnboardingTypeDefaults:
        if engagement_type_code in defaults_map:
            return defaults_map[engagement_type_code]
        if isinstance(raw_map, dict):
            parsed = _parse_type_defaults(raw_map.get(engagement_type_code))
            if parsed is not None:
                return parsed
        if "bio_ai" in defaults_map:
            return defaults_map["bio_ai"]
        return _fallback_type_defaults()

    async def resolve_b2c_default_package_ids(self, db: AsyncSession) -> tuple[int, int]:
        defaults = await self.resolve_b2c_onboarding_defaults(db, EngagementKind.bio_ai)
        diagnostic_id = defaults.diagnostic_package_id or DEFAULT_B2C_DIAGNOSTIC_PACKAGE_ID
        return defaults.assessment_package_id, diagnostic_id

    async def resolve_b2c_onboarding_defaults(
        self,
        db: AsyncSession,
        engagement_type: EngagementKind | str = EngagementKind.bio_ai,
    ) -> B2cOnboardingTypeDefaults:
        if isinstance(engagement_type, EngagementKind):
            code = engagement_type.value
        else:
            code = (engagement_type or "").strip() or EngagementKind.bio_ai.value
        row = await self._repository.get_by_id(db)
        defaults_map = await self._build_defaults_map(db, row)
        raw_map = getattr(row, "b2c_onboarding_by_engagement_type", None) if row is not None else None
        return self._resolve_type_defaults_from_map(defaults_map, code, raw_map=raw_map)

    async def ensure_active_b2c_packages(
        self,
        db: AsyncSession,
        assessment_package_id: int,
        diagnostic_package_id: int | None,
    ) -> None:
        ap = (
            await db.execute(select(AssessmentPackage).where(AssessmentPackage.package_id == assessment_package_id).limit(1))
        ).scalar_one_or_none()
        if ap is None or (ap.status or "").lower() != "active":
            raise AppError(
                status_code=422,
                error_code="INVALID_B2C_ASSESSMENT_PACKAGE",
                message="Assessment package is missing or not active",
            )

        if diagnostic_package_id is None:
            return

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
        row = await self._repository.get_by_id(db)
        return B2cOnboardingDefaultsRead(defaults_by_engagement_type=await self._build_defaults_map(db, row))

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
        provided = {str(code).strip(): entry for code, entry in payload.defaults_by_engagement_type.items()}
        if not provided:
            raise AppError(
                status_code=422,
                error_code="INVALID_B2C_ONBOARDING_DEFAULTS",
                message="defaults_by_engagement_type must include at least one engagement type",
            )

        active_codes = await self._list_active_engagement_type_codes(db)
        active_set = set(active_codes)
        unknown = sorted(code for code in provided if code not in active_set)
        if unknown:
            raise AppError(
                status_code=422,
                error_code="INVALID_ENGAGEMENT_TYPE",
                message=(
                    "defaults_by_engagement_type keys must be active engagement_types.code; "
                    f"unknown or inactive: {', '.join(unknown)}"
                ),
            )

        existing_row = await self._repository.get_by_id(db)
        existing_map = await self._build_defaults_map(db, existing_row)
        complete: dict[str, B2cOnboardingTypeDefaults] = {}
        for code in active_codes:
            complete[code] = provided.get(code) or existing_map.get(code) or _fallback_type_defaults()

        for code, entry in complete.items():
            await self.ensure_active_b2c_packages(db, entry.assessment_package_id, entry.diagnostic_package_id)
            if entry.enroll_for_fitprint_full and not entry.create_profile_on_metsights:
                raise AppError(
                    status_code=422,
                    error_code="INVALID_B2C_ONBOARDING_DEFAULTS",
                    message=(
                        f"FitPrint Full enrollment requires Metsights profile creation "
                        f"(engagement_type={code})"
                    ),
                )

        serialized = {code: _type_defaults_to_dict(entry) for code, entry in complete.items()}
        # Preserve configs for inactive codes so reactivating a type keeps settings.
        existing_raw = getattr(existing_row, "b2c_onboarding_by_engagement_type", None) if existing_row else None
        if isinstance(existing_raw, dict):
            for code, raw_entry in existing_raw.items():
                key = str(code)
                if key not in serialized and isinstance(raw_entry, dict):
                    serialized[key] = raw_entry

        bio_ai = complete.get("bio_ai") or next(iter(complete.values()))
        # Legacy flat column is NOT NULL; keep a concrete fallback when bio_ai has no diagnostic.
        mirrored_diagnostic_id = bio_ai.diagnostic_package_id or DEFAULT_B2C_DIAGNOSTIC_PACKAGE_ID
        await self._repository.upsert_b2c_onboarding_by_engagement_type(
            db,
            defaults_by_engagement_type=serialized,
            assessment_package_id=bio_ai.assessment_package_id,
            diagnostic_package_id=mirrored_diagnostic_id,
            blood_collection_type=bio_ai.blood_collection_type,
            create_profile_on_metsights=bio_ai.create_profile_on_metsights,
            enroll_for_fitprint_full=bio_ai.enroll_for_fitprint_full,
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
        return EngagementNotificationDefaultsRead()

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
        return EngagementNotificationDefaultsRead()

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
