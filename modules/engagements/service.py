"""Engagements service.

Business rules:
- Engagement creation
- Enrolling users by creating `engagement_participants`
- Updating `participant_count`
"""

from __future__ import annotations

import secrets
import string
from datetime import date, datetime, time, timezone
from typing import TYPE_CHECKING, Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.assessments.repository import AssessmentsRepository
from modules.assessments.service import AssessmentsService
from modules.audit.service import AuditService
from modules.checklists.schemas import ChecklistReadiness
from modules.employee.service import EmployeeContext
from modules.engagements.constants import DEFAULT_ENGAGEMENT_NOTIFICATION_SERVICE_KEY
from modules.engagements.models import Engagement, EngagementKind, EngagementParticipant, OnboardingAssistantAssignment
from modules.engagements.repository import EngagementsRepository
from modules.engagements.schemas import EngagementCreateRequest, EngagementUpdateRequest
from modules.notifications.repository import NotificationsRepository
from modules.organizations.repository import OrganizationsRepository
from modules.questionnaire.repository import QuestionnaireRepository
from modules.reports.repository import ReportsRepository
from modules.users.models import User
from modules.users.repository import UsersRepository

if TYPE_CHECKING:
    from modules.checklists.service import ChecklistsService
    from modules.metsights.service import MetsightsService
    from modules.notifications.service import NotificationsService


def _generate_engagement_code(length: int = 8) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


_ALLOWED_ENGAGEMENT_STATUS = {"active", "inactive", "archived"}
DEFAULT_B2C_DIAGNOSTIC_PACKAGE_ID = 1
_B2C_DEFAULT_ONBOARDING_ASSISTANT_EMPLOYEE_IDS = (1, 8)


def _normalize_status(value: str | None) -> str:
    return (value or "").strip().lower()


def _resolve_notification_service_key(raw: str | None) -> str:
    key = (raw or "").strip()
    return key or DEFAULT_ENGAGEMENT_NOTIFICATION_SERVICE_KEY


def _phone_lookup_candidates(phone: str) -> list[str]:
    """Build ordered unique phone strings to match stored user.phone values."""

    raw = (phone or "").strip()
    if not raw:
        return []

    stripped = raw.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    digits = "".join(ch for ch in raw if ch.isdigit())

    ordered: list[str] = []
    for value in (raw, stripped):
        if value and value not in ordered:
            ordered.append(value)

    if len(digits) == 10:
        for value in (digits, f"+91{digits}", f"91{digits}"):
            if value not in ordered:
                ordered.append(value)
    elif len(digits) == 12 and digits.startswith("91"):
        base10 = digits[2:]
        for value in (base10, f"+91{base10}", f"91{base10}", f"+{digits}"):
            if value not in ordered:
                ordered.append(value)
    elif len(digits) == 11 and digits.startswith("0"):
        base10 = digits[1:]
        for value in (base10, f"+91{base10}", f"91{base10}"):
            if value not in ordered:
                ordered.append(value)
    elif digits.startswith("+") or (digits and len(digits) > 10):
        plus = f"+{digits}" if not digits.startswith("+") else digits
        if plus not in ordered:
            ordered.append(plus)

    return ordered


def _normalize_phone_for_metsights(raw: str | None) -> str | None:
    value = (raw or "").strip().replace(" ", "").replace("-", "")
    if not value:
        return None
    if value.startswith("+"):
        return value
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) == 10:
        return f"+91{digits}"
    return f"+{digits}" if digits else None


def _to_metsights_gender(raw: str | None) -> str | None:
    v = (raw or "").strip()
    if not v:
        return None
    if v in {"1", "2"}:
        return v
    lowered = v.lower()
    if lowered.startswith("m"):
        return "1"
    if lowered.startswith("f"):
        return "2"
    return None


def _participant_enrollment_to_dict(row: tuple) -> dict[str, Any]:
    (
        engagement_participant_id,
        engagement_id,
        user_id,
        first_name,
        last_name,
        phone,
        email,
        address,
        pin_code,
        city,
        state,
        country,
        status,
        slot_start_time,
        engagement_date,
        participants_employee_id,
        participant_department,
        participant_blood_group,
        want_doctor_consultation,
        want_nutritionist_consultation,
        want_doctor_and_nutritionist_consultation,
        is_profile_created_on_metsights,
        is_primary_record_id_synced,
        is_fitprint_record_id_synced,
    ) = row
    return {
        "engagement_participant_id": engagement_participant_id,
        "engagement_id": engagement_id,
        "user_id": user_id,
        "first_name": first_name,
        "last_name": last_name,
        "phone": phone,
        "email": email,
        "address": address,
        "pin_code": pin_code,
        "city": city,
        "state": state,
        "country": country,
        "status": status,
        "slot_start_time": slot_start_time.isoformat() if slot_start_time is not None else None,
        "engagement_date": engagement_date.isoformat() if engagement_date is not None else None,
        "participants_employee_id": participants_employee_id,
        "participant_department": participant_department,
        "participant_blood_group": participant_blood_group,
        "want_doctor_consultation": want_doctor_consultation,
        "want_nutritionist_consultation": want_nutritionist_consultation,
        "want_doctor_and_nutritionist_consultation": want_doctor_and_nutritionist_consultation,
        "is_profile_created_on_metsights": is_profile_created_on_metsights,
        "is_primary_record_id_synced": is_primary_record_id_synced,
        "is_fitprint_record_id_synced": is_fitprint_record_id_synced,
    }


class EngagementsService:
    def __init__(
        self,
        repository: EngagementsRepository,
        audit_service: AuditService | None = None,
        organizations_repository: OrganizationsRepository | None = None,
        users_repository: UsersRepository | None = None,
        assessments_service: AssessmentsService | None = None,
        metsights_service: MetsightsService | None = None,
        notifications_repository: NotificationsRepository | None = None,
        notifications_service: "NotificationsService | None" = None,
    ):
        self._repository = repository
        self._audit_service = audit_service
        self._organizations_repository = organizations_repository
        self._users_repository = users_repository or UsersRepository()
        self._assessments_service = assessments_service
        self._metsights_service = metsights_service
        self._notifications_repository = notifications_repository or NotificationsRepository()
        self._notifications_service = notifications_service
        self._assessments_repository = AssessmentsRepository()
        self._questionnaire_repository = QuestionnaireRepository()
        self._reports_repository = ReportsRepository()
        self._checklists_service = None

    def lazy_checklists_service(self) -> ChecklistsService:
        if self._checklists_service is None:
            from modules.checklists.repository import ChecklistsRepository
            from modules.checklists.service import ChecklistsService

            self._checklists_service = ChecklistsService(
                ChecklistsRepository(),
                self._require_audit_service(),
                engagements_service=self,
            )
        return self._checklists_service

    def _group_slots_by_date(self, rows: list[tuple]) -> dict[str, list[str]]:
        """Group (date, time) rows into a {"YYYY-MM-DD": ["HH:MM:SS", ...]} mapping."""

        grouped: dict[str, list[str]] = {}
        for engagement_date, slot_start_time in rows:
            date_key = engagement_date.isoformat()
            grouped.setdefault(date_key, []).append(slot_start_time.isoformat())
        return grouped

    def _ensure_employee_access(self, employee: EmployeeContext | None) -> None:
        if employee is None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

    def _require_audit_service(self) -> AuditService:
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        return self._audit_service

    async def _validate_notification_service_key(self, db: AsyncSession, raw: str | None) -> str:
        service_key = _resolve_notification_service_key(raw)
        svc = await self._notifications_repository.get_service_by_key(db, service_key=service_key)
        if svc is None:
            raise AppError(
                status_code=404,
                error_code="NOTIFICATION_SERVICE_NOT_FOUND",
                message=f"Notification service '{service_key}' does not exist",
            )
        if not svc.is_active:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"Notification service '{service_key}' is not active",
            )
        return service_key

    async def get_by_code(self, db: AsyncSession, engagement_code: str) -> Engagement | None:
        return await self._repository.get_engagement_by_code(db, engagement_code)

    async def get_occupied_slots_for_engagement_code(
        self,
        db: AsyncSession,
        *,
        engagement_code: str,
    ) -> dict[str, list[str]]:
        """Return occupied slots for one engagement.

        Output is grouped by date.
        """

        code = (engagement_code or "").strip()
        if not code:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        engagement = await self._repository.get_engagement_by_code(db, code)
        if engagement is None:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")

        rows = await self._repository.list_occupied_slots_by_engagement_id(db, engagement_id=engagement.engagement_id)
        return self._group_slots_by_date(rows)

    async def get_occupied_slots_for_active_b2c_engagements(
        self,
        db: AsyncSession,
    ) -> dict[str, list[str]]:
        """Return occupied slots for all active B2C engagements.

        Public (no-auth) use-case.
        Output is grouped by date.
        """

        rows = await self._repository.list_occupied_slots_for_active_b2c_engagements(db)
        return self._group_slots_by_date(rows)

    async def get_by_id(self, db: AsyncSession, engagement_id: int) -> Engagement | None:
        return await self._repository.get_engagement_by_id(db, engagement_id)

    async def create_b2b_engagement(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        payload: EngagementCreateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Engagement:
        self._ensure_employee_access(employee)

        if payload.start_date > payload.end_date:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        # Validate that the organization exists
        if self._organizations_repository is not None:
            organization = await self._organizations_repository.get_by_id(db, payload.organization_id)
            if organization is None:
                raise AppError(
                    status_code=404,
                    error_code="ORGANIZATION_NOT_FOUND",
                    message=f"Organization with ID {payload.organization_id} does not exist",
                )

        diagnostic_package_id = payload.diagnostic_package_id

        # Use provided engagement_code or generate a unique one.
        if payload.engagement_code:
            code = payload.engagement_code.strip()
            if not code:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Engagement code cannot be empty")
            # Check if the provided code is already in use
            existing = await self._repository.get_engagement_by_code(db, code)
            if existing is not None:
                raise AppError(status_code=409, error_code="DUPLICATE_ENGAGEMENT_CODE", message="Engagement code already exists")
        else:
            # Generate a unique engagement_code.
            for _ in range(10):
                code = _generate_engagement_code()
                existing = await self._repository.get_engagement_by_code(db, code)
                if existing is None:
                    break
            else:
                raise AppError(status_code=500, error_code="INTERNAL_ERROR", message="An unexpected error occurred")

        notification_service_key = await self._validate_notification_service_key(
            db, payload.notification_service_key
        )

        engagement = Engagement(
            engagement_name=payload.engagement_name,
            metsights_engagement_id=payload.metsights_engagement_id,
            organization_id=payload.organization_id,
            engagement_code=code,
            engagement_type=payload.engagement_type,
            assessment_package_id=payload.assessment_package_id,
            diagnostic_package_id=diagnostic_package_id,
            city=payload.city,
            address=payload.address,
            pincode=payload.pincode,
            slot_duration=payload.slot_duration,
            start_date=payload.start_date,
            end_date=payload.end_date,
            status="active",
            participant_count=0,
            create_profile_on_metsights=payload.create_profile_on_metsights,
            enroll_for_fitprint_full=payload.enroll_for_fitprint_full,
            notification_service_key=notification_service_key,
        )

        engagement = await self._repository.create_engagement(db, engagement)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_CREATE_ENGAGEMENT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return engagement

    async def list_engagements_for_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        page: int,
        limit: int,
        organization_id: int | None,
        status: str | None,
        city: str | None,
        on_date,
        search: str | None = None,
        engagement_type: str | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
    ) -> tuple[list[Engagement], int, dict[int, ChecklistReadiness]]:
        self._ensure_employee_access(employee)

        status_value = None
        if status is not None:
            normalized = _normalize_status(status)
            if normalized not in _ALLOWED_ENGAGEMENT_STATUS:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            status_value = normalized

        engagements = await self._repository.list_engagements(
            db,
            page=page,
            limit=limit,
            organization_id=organization_id,
            status=status_value,
            city=city,
            on_date=on_date,
            search=search,
            engagement_type=engagement_type,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )

        total = await self._repository.count_engagements(
            db,
            organization_id=organization_id,
            status=status_value,
            city=city,
            on_date=on_date,
            search=search,
            engagement_type=engagement_type,
        )

        counts_by_id = await self._repository.count_distinct_participants_by_engagement_ids(
            db,
            engagement_ids=[int(row.engagement_id) for row in engagements],
        )
        for row in engagements:
            row.participant_count = counts_by_id.get(int(row.engagement_id), 0)

        checklists = self.lazy_checklists_service()
        readiness_by_id: dict[int, ChecklistReadiness] = {}
        for row in engagements:
            readiness_by_id[row.engagement_id] = await checklists.get_engagement_readiness(db, row.engagement_id)

        return engagements, total, readiness_by_id

    async def get_engagement_filter_options_for_employee(self, db: AsyncSession, *, employee: EmployeeContext) -> dict:
        self._ensure_employee_access(employee)
        types, cities = await self._repository.list_distinct_engagement_types_and_cities(db)
        return {"engagement_types": types, "cities": cities}

    async def get_engagement_details_for_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
    ) -> Engagement:
        self._ensure_employee_access(employee)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")

        await self._refresh_participant_count(db, engagement)
        return engagement

    async def _refresh_participant_count(self, db: AsyncSession, engagement: Engagement) -> None:
        engagement.participant_count = await self._repository.count_distinct_participants_for_engagement(
            db,
            engagement_id=int(engagement.engagement_id),
        )
        await self._repository.update_engagement(db, engagement)

    async def update_engagement_for_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        payload: EngagementUpdateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Engagement:
        self._ensure_employee_access(employee)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")

        if payload.start_date > payload.end_date:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        # Validate that the organization exists when one is provided (B2C engagements have no org).
        if payload.organization_id is not None and self._organizations_repository is not None:
            organization = await self._organizations_repository.get_by_id(db, payload.organization_id)
            if organization is None:
                raise AppError(
                    status_code=404,
                    error_code="ORGANIZATION_NOT_FOUND",
                    message=f"Organization with ID {payload.organization_id} does not exist",
                )

        code = (payload.engagement_code or "").strip()
        if not code:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Engagement code cannot be empty")
        existing_code = await self._repository.get_engagement_by_code(db, code)
        if existing_code is not None and int(existing_code.engagement_id) != int(engagement.engagement_id):
            raise AppError(
                status_code=409,
                error_code="DUPLICATE_ENGAGEMENT_CODE",
                message="Engagement code already exists",
            )

        engagement.engagement_name = payload.engagement_name
        engagement.engagement_code = code
        engagement.organization_id = payload.organization_id
        engagement.engagement_type = payload.engagement_type
        engagement.assessment_package_id = payload.assessment_package_id
        engagement.diagnostic_package_id = payload.diagnostic_package_id
        engagement.city = payload.city
        engagement.address = payload.address
        engagement.pincode = payload.pincode
        engagement.slot_duration = payload.slot_duration
        engagement.start_date = payload.start_date
        engagement.end_date = payload.end_date
        engagement.metsights_engagement_id = payload.metsights_engagement_id
        engagement.create_profile_on_metsights = payload.create_profile_on_metsights
        engagement.enroll_for_fitprint_full = payload.enroll_for_fitprint_full
        notif_key_raw = payload.notification_service_key
        if notif_key_raw is None:
            notif_key_raw = engagement.notification_service_key
        engagement.notification_service_key = await self._validate_notification_service_key(
            db, notif_key_raw
        )

        engagement = await self._repository.update_engagement(db, engagement)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_UPDATE_ENGAGEMENT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return engagement

    async def change_engagement_status_for_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        status: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Engagement:
        self._ensure_employee_access(employee)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")

        normalized = _normalize_status(status)
        if normalized not in _ALLOWED_ENGAGEMENT_STATUS:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        engagement.status = normalized
        engagement = await self._repository.update_engagement(db, engagement)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_UPDATE_ENGAGEMENT_STATUS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return engagement

    async def create_b2c_engagement(
        self,
        db: AsyncSession,
        *,
        user_first_name: str | None,
        engagement_date: date,
        city: str | None,
        assessment_package_id: int | None,
        diagnostic_package_id: int | None = None,
        engagement_type: EngagementKind = EngagementKind.bio_ai,
        address: str | None = None,
        pincode: str | None = None,
        create_profile_on_metsights: bool = False,
        enroll_for_fitprint_full: bool = False,
    ) -> Engagement:
        """Create a B2C engagement with no onboarding assistants assigned by default.

        B2C engagements are created automatically during public user onboarding.
        Onboarding assistant assignments can be added later if needed.
        """
        name_part = (user_first_name or "user").strip() or "user"
        engagement_name = f"{name_part}-{engagement_date.isoformat()}"

        engagement = Engagement(
            engagement_name=engagement_name,
            metsights_engagement_id=None,
            organization_id=None,
            engagement_code=_generate_engagement_code(),
            engagement_type=engagement_type,
            assessment_package_id=assessment_package_id,
            diagnostic_package_id=diagnostic_package_id,
            city=city,
            address=address,
            pincode=pincode,
            slot_duration=20,
            start_date=engagement_date,
            end_date=engagement_date,
            status="active",
            participant_count=0,
            create_profile_on_metsights=create_profile_on_metsights,
            enroll_for_fitprint_full=enroll_for_fitprint_full,
            notification_service_key=DEFAULT_ENGAGEMENT_NOTIFICATION_SERVICE_KEY,
        )
        engagement = await self._repository.create_engagement(db, engagement)

        for emp_id in _B2C_DEFAULT_ONBOARDING_ASSISTANT_EMPLOYEE_IDS:
            assignment = OnboardingAssistantAssignment(
                engagement_id=engagement.engagement_id,
                employee_id=emp_id,
            )
            await self._repository.create_onboarding_assistant_assignment(db, assignment)

        return engagement

    async def list_onboarding_assistant_user_ids(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
    ) -> list[int]:
        """Return user_ids of all onboarding assistants for an engagement."""
        return await self._repository.list_onboarding_assistant_user_ids(
            db, engagement_id=engagement_id
        )

    async def notify_onboarding_assistants_after_enrollment(
        self,
        db: AsyncSession,
        *,
        engagement: Engagement,
        user: User,
        source: str,
        collection_date: str | None = None,
        collection_time: str | None = None,
    ) -> None:
        if self._notifications_service is None:
            return

        from modules.notifications.onboarding_notify import (
            notify_onboarding_assistants_on_enrollment,
            participant_details_from_user,
        )

        participant_details = participant_details_from_user(
            user,
            source=source,
            participant_user_id=int(user.user_id),
            collection_date=collection_date,
            collection_time=collection_time,
        )
        await notify_onboarding_assistants_on_enrollment(
            db,
            notifications_service=self._notifications_service,
            notifications_repository=self._notifications_repository,
            engagements_repository=self._repository,
            engagement=engagement,
            participant_user_id=int(user.user_id),
            participant_details=participant_details,
        )

    async def user_has_slot_for_engagement(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
    ) -> bool:
        return await self._repository.has_participant_for_user_engagement(
            db, user_id=user_id, engagement_id=engagement_id
        )

    async def enroll_user_in_engagement(
        self,
        db: AsyncSession,
        *,
        engagement: Engagement,
        user_id: int,
        engagement_date: date,
        slot_start_time: time,
        increment_participant_count: bool = False,
        participants_employee_id: str | None = None,
        participant_department: str | None = None,
        participant_blood_group: str | None = None,
        want_doctor_consultation: bool | None = None,
        want_nutritionist_consultation: bool | None = None,
        want_doctor_and_nutritionist_consultation: bool | None = None,
        is_profile_created_on_metsights: bool = False,
        is_primary_record_id_synced: bool = False,
        is_fitprint_record_id_synced: bool = False,
    ) -> EngagementParticipant:
        if (engagement.status or "").lower() != "active":
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Engagement is no longer active")

        participant = EngagementParticipant(
            engagement_id=engagement.engagement_id,
            user_id=user_id,
            engagement_date=engagement_date,
            slot_start_time=slot_start_time,
            participants_employee_id=participants_employee_id,
            participant_department=participant_department,
            participant_blood_group=participant_blood_group,
            want_doctor_consultation=want_doctor_consultation,
            want_nutritionist_consultation=want_nutritionist_consultation,
            want_doctor_and_nutritionist_consultation=want_doctor_and_nutritionist_consultation,
            is_profile_created_on_metsights=is_profile_created_on_metsights,
            is_primary_record_id_synced=is_primary_record_id_synced,
            is_fitprint_record_id_synced=is_fitprint_record_id_synced,
        )
        created = await self._repository.create_participant(db, participant)
        await self._refresh_participant_count(db, engagement)
        return created

    async def update_participant_sync_flags(
        self,
        db: AsyncSession,
        *,
        participant: EngagementParticipant,
        is_profile_created_on_metsights: bool | None = None,
        is_primary_record_id_synced: bool | None = None,
        is_fitprint_record_id_synced: bool | None = None,
    ) -> EngagementParticipant:
        if is_profile_created_on_metsights is not None:
            participant.is_profile_created_on_metsights = is_profile_created_on_metsights
        if is_primary_record_id_synced is not None:
            participant.is_primary_record_id_synced = is_primary_record_id_synced
        if is_fitprint_record_id_synced is not None:
            participant.is_fitprint_record_id_synced = is_fitprint_record_id_synced
        return await self._repository.update_participant(db, participant)

    async def list_participants_for_engagement_code(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_code: str,
        page: int,
        limit: int,
    ) -> tuple[list[dict], int]:
        """Fetch participant enrollment rows for a specific engagement by code.
        
        This endpoint is for employees only.
        """
        self._ensure_employee_access(employee)

        # Validate engagement exists
        engagement = await self._repository.get_engagement_by_code(db, engagement_code)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        # Fetch participants with pagination
        participants = await self._repository.list_participants_by_engagement_code(
            db,
            engagement_code=engagement_code,
            engagement_id=int(engagement.engagement_id),
            page=page,
            limit=limit,
        )

        # Count total participant enrollment rows
        total = await self._repository.count_participants_by_engagement_code(
            db,
            engagement_code=engagement_code,
            engagement_id=int(engagement.engagement_id),
        )

        result = [_participant_enrollment_to_dict(row) for row in participants]
        return result, total

    async def list_participants_for_engagement_id(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        page: int,
        limit: int,
    ) -> tuple[list[dict], int]:
        """Fetch participant enrollment rows for a specific engagement by id."""

        self._ensure_employee_access(employee)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        participants = await self._repository.list_participants_by_engagement_id(
            db,
            engagement_id=engagement_id,
            page=page,
            limit=limit,
        )
        total = await self._repository.count_distinct_participants_for_engagement(
            db,
            engagement_id=engagement_id,
        )

        result = [_participant_enrollment_to_dict(row) for row in participants]
        return result, total

    async def list_participants_for_b2c_engagements(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        page: int,
        limit: int,
    ) -> tuple[list[dict], int]:
        """Fetch participant enrollment rows in all B2C engagements.
        
        This endpoint is for employees only.
        B2C engagements are engagements with no organization_id.
        """
        self._ensure_employee_access(employee)

        # Fetch participants with pagination
        participants = await self._repository.list_participants_for_b2c_engagements(
            db,
            page=page,
            limit=limit,
        )

        # Count total participant enrollment rows
        total = await self._repository.count_participants_for_b2c_engagements(db)

        result = [_participant_enrollment_to_dict(row) for row in participants]
        return result, total

    async def _detach_assessment_instance_references(
        self,
        db: AsyncSession,
        *,
        assessment_instance_ids: list[int],
    ) -> None:
        """Clear FK references so assessment instances can be deleted."""

        if not assessment_instance_ids:
            return

        from sqlalchemy import update

        from modules.notifications.models import Notification
        from modules.reports.models import ReportsUserSyncState

        await db.execute(
            update(Notification)
            .where(Notification.assessment_instance_id.in_(assessment_instance_ids))
            .values(assessment_instance_id=None)
        )
        await db.execute(
            update(ReportsUserSyncState)
            .where(ReportsUserSyncState.last_synced_assessment_instance_id.in_(assessment_instance_ids))
            .values(last_synced_assessment_instance_id=None)
        )

    async def _purge_user_engagement_data(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
    ) -> dict[str, int]:
        instances = await self._assessments_repository.list_instances_for_user_engagement(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
        )
        instance_ids = [int(i.assessment_instance_id) for i in instances]
        await self._detach_assessment_instance_references(db, assessment_instance_ids=instance_ids)

        deleted_reports = 0
        deleted_questionnaire_responses = 0
        deleted_category_progress = 0
        deleted_instances = 0

        for instance in instances:
            purge = await self._purge_assessment_instance(
                db,
                assessment_instance_id=int(instance.assessment_instance_id),
            )
            deleted_reports += purge["deleted_reports"]
            deleted_questionnaire_responses += purge["deleted_questionnaire_responses"]
            deleted_category_progress += purge["deleted_category_progress_rows"]
            deleted_instances += purge["deleted_assessment_instances"]

        deleted_participants = await self._repository.delete_participants_for_user_engagement(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
        )

        return {
            "deleted_engagement_participants": deleted_participants,
            "deleted_assessment_instances": deleted_instances,
            "deleted_questionnaire_responses": deleted_questionnaire_responses,
            "deleted_reports": deleted_reports,
            "deleted_category_progress_rows": deleted_category_progress,
        }

    async def _purge_assessment_instance(self, db: AsyncSession, *, assessment_instance_id: int) -> dict[str, int]:
        instance_id = int(assessment_instance_id)
        deleted_reports = await self._reports_repository.delete_individual_reports_for_instance(
            db,
            assessment_instance_id=instance_id,
        )
        deleted_questionnaire_responses = await self._questionnaire_repository.delete_responses_for_instance(
            db,
            assessment_instance_id=instance_id,
        )
        deleted_category_progress = await self._assessments_repository.delete_category_progress_for_instance(
            db,
            assessment_instance_id=instance_id,
        )
        deleted_instances = await self._assessments_repository.delete_instance(
            db,
            assessment_instance_id=instance_id,
        )
        return {
            "deleted_assessment_instances": deleted_instances,
            "deleted_questionnaire_responses": deleted_questionnaire_responses,
            "deleted_reports": deleted_reports,
            "deleted_category_progress_rows": deleted_category_progress,
        }

    async def _purge_engagement_scoped_data(self, db: AsyncSession, *, engagement_id: int) -> dict[str, int]:
        """Delete all data tied to an engagement. Does not delete users."""

        from sqlalchemy import delete, update

        from modules.checklists.models import EngagementChecklist
        from modules.notifications.models import Notification
        from modules.reports.models import OrganizationHealthReport

        instances = await self._assessments_repository.list_all_instances_for_engagement(
            db,
            engagement_id=engagement_id,
        )
        instance_ids = [int(i.assessment_instance_id) for i in instances]

        await db.execute(
            update(Notification)
            .where(Notification.engagement_id == engagement_id)
            .values(engagement_id=None)
        )
        await self._detach_assessment_instance_references(db, assessment_instance_ids=instance_ids)

        totals = {
            "deleted_engagement_participants": 0,
            "deleted_assessment_instances": 0,
            "deleted_questionnaire_responses": 0,
            "deleted_reports": 0,
            "deleted_category_progress_rows": 0,
            "deleted_organization_health_reports": 0,
            "deleted_onboarding_assistant_assignments": 0,
            "deleted_engagement_checklists": 0,
        }

        for instance in instances:
            purge = await self._purge_assessment_instance(
                db,
                assessment_instance_id=int(instance.assessment_instance_id),
            )
            totals["deleted_assessment_instances"] += purge["deleted_assessment_instances"]
            totals["deleted_questionnaire_responses"] += purge["deleted_questionnaire_responses"]
            totals["deleted_reports"] += purge["deleted_reports"]
            totals["deleted_category_progress_rows"] += purge["deleted_category_progress_rows"]

        totals["deleted_engagement_participants"] = await self._repository.delete_all_participants_for_engagement(
            db,
            engagement_id=engagement_id,
        )

        org_report_result = await db.execute(
            delete(OrganizationHealthReport).where(OrganizationHealthReport.engagement_id == engagement_id)
        )
        totals["deleted_organization_health_reports"] = int(org_report_result.rowcount or 0)

        totals["deleted_onboarding_assistant_assignments"] = (
            await self._repository.delete_all_onboarding_assignments_for_engagement(
                db,
                engagement_id=engagement_id,
            )
        )

        checklist_result = await db.execute(
            delete(EngagementChecklist).where(EngagementChecklist.engagement_id == engagement_id)
        )
        totals["deleted_engagement_checklists"] = int(checklist_result.rowcount or 0)

        return totals

    async def delete_engagement_for_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        """Permanently delete an engagement and all engagement-scoped data. Users are not deleted."""

        self._ensure_employee_access(employee)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")

        purge = await self._purge_engagement_scoped_data(db, engagement_id=engagement_id)

        deleted = await self._repository.delete_engagement_by_id(db, engagement_id=engagement_id)
        if not deleted:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_DELETE_ENGAGEMENT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return {
            "engagement_id": engagement_id,
            "engagement_code": engagement.engagement_code,
            "engagement_name": engagement.engagement_name,
            **purge,
        }

    async def remove_participant_from_engagement_for_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        self._ensure_employee_access(employee)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")

        participant = await self._repository.get_participant_for_user_engagement(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
        )
        if participant is None:
            raise AppError(status_code=404, error_code="PARTICIPANT_NOT_FOUND", message="Participant does not exist")

        purge = await self._purge_user_engagement_data(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
        )
        engagement.participant_count = await self._repository.count_distinct_participants_for_engagement(
            db,
            engagement_id=engagement_id,
        )
        await self._repository.update_engagement(db, engagement)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_DELETE_ENGAGEMENT_PARTICIPANT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return {
            "engagement_id": engagement_id,
            "user_id": user_id,
            **purge,
        }

    async def remove_all_participants_from_engagement_for_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        """Remove every enrolled user from an engagement (same purge as single delete)."""

        self._ensure_employee_access(employee)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")

        user_ids = await self._repository.list_distinct_participant_ids_for_engagement(
            db,
            engagement_id=engagement_id,
        )

        totals = {
            "deleted_users": 0,
            "deleted_engagement_participants": 0,
            "deleted_assessment_instances": 0,
            "deleted_questionnaire_responses": 0,
            "deleted_reports": 0,
            "deleted_category_progress_rows": 0,
        }

        for user_id in user_ids:
            participant = await self._repository.get_participant_for_user_engagement(
                db,
                user_id=user_id,
                engagement_id=engagement_id,
            )
            if participant is None:
                continue

            purge = await self._purge_user_engagement_data(
                db,
                user_id=user_id,
                engagement_id=engagement_id,
            )
            totals["deleted_users"] += 1
            totals["deleted_engagement_participants"] += purge["deleted_engagement_participants"]
            totals["deleted_assessment_instances"] += purge["deleted_assessment_instances"]
            totals["deleted_questionnaire_responses"] += purge["deleted_questionnaire_responses"]
            totals["deleted_reports"] += purge["deleted_reports"]
            totals["deleted_category_progress_rows"] += purge["deleted_category_progress_rows"]

        engagement.participant_count = await self._repository.count_distinct_participants_for_engagement(
            db,
            engagement_id=engagement_id,
        )
        await self._repository.update_engagement(db, engagement)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_DELETE_ALL_ENGAGEMENT_PARTICIPANTS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return {
            "engagement_id": engagement_id,
            **totals,
        }

    async def get_questionnaire_status_for_engagement(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
    ) -> dict:
        """Return per-participant questionnaire completion status for an engagement.

        Groups by user (participant) — a user may have multiple assessment instances
        (one per package). The participant's overall state is the "best" across instances:
          submitted > drafted > not_started

        The questionnaire lifecycle per assessment instance is:
        - NOT STARTED: no questionnaire_responses rows exist
        - DRAFTED (filled): responses exist with submitted_at = NULL (user saved progress)
        - SUBMITTED: assessment instance status = "completed" (all responses got submitted_at set)
        """
        self._ensure_employee_access(employee)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        from modules.assessments.models import AssessmentInstance, AssessmentPackage
        from modules.users.models import User
        from sqlalchemy import select

        instances_query = (
            select(
                AssessmentInstance.assessment_instance_id,
                AssessmentInstance.user_id,
                AssessmentInstance.status,
                AssessmentInstance.package_id,
                AssessmentInstance.completed_at,
                User.first_name,
                User.last_name,
                User.phone,
                User.email,
                AssessmentPackage.package_code,
                AssessmentPackage.display_name.label("package_display_name"),
            )
            .join(User, User.user_id == AssessmentInstance.user_id)
            .outerjoin(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
            .where(AssessmentInstance.engagement_id == engagement_id)
            .order_by(User.first_name.asc(), User.last_name.asc(), AssessmentInstance.assessment_instance_id.asc())
        )
        result = await db.execute(instances_query)
        instance_rows = result.all()

        if not instance_rows:
            return {"summary": {"drafted": 0, "submitted": 0, "not_started": 0}, "participants": []}

        instance_ids = [row.assessment_instance_id for row in instance_rows]
        responses = await self._questionnaire_repository.list_responses_for_instances(
            db, assessment_instance_ids=instance_ids
        )

        response_counts: dict[int, int] = {}
        for resp in responses:
            inst_id = int(resp.assessment_instance_id)
            response_counts[inst_id] = response_counts.get(inst_id, 0) + 1

        # Group instances by user_id to determine per-participant state
        user_data: dict[int, dict] = {}
        for row in instance_rows:
            uid = int(row.user_id)
            resp_count = response_counts.get(row.assessment_instance_id, 0)
            is_completed = (row.status or "").lower() == "completed"

            if uid not in user_data:
                user_data[uid] = {
                    "user_id": uid,
                    "first_name": row.first_name,
                    "last_name": row.last_name,
                    "phone": row.phone,
                    "email": row.email,
                    "has_submitted": False,
                    "has_drafted": False,
                    "total_responses": 0,
                    "packages": [],
                }

            entry = user_data[uid]
            entry["total_responses"] += resp_count
            entry["packages"].append({
                "package_code": row.package_code,
                "package_display_name": row.package_display_name,
                "questionnaire_state": (
                    "submitted" if is_completed
                    else "drafted" if resp_count > 0
                    else "not_started"
                ),
                "responses_count": resp_count,
            })

            if is_completed:
                entry["has_submitted"] = True
            elif resp_count > 0:
                entry["has_drafted"] = True

        participants: list[dict] = []
        summary_drafted = 0
        summary_submitted = 0
        summary_not_started = 0

        for entry in user_data.values():
            if entry["has_submitted"]:
                state = "submitted"
                summary_submitted += 1
            elif entry["has_drafted"]:
                state = "drafted"
                summary_drafted += 1
            else:
                state = "not_started"
                summary_not_started += 1

            participants.append({
                "user_id": entry["user_id"],
                "first_name": entry["first_name"],
                "last_name": entry["last_name"],
                "phone": entry["phone"],
                "email": entry["email"],
                "questionnaire_state": state,
                "total_responses": entry["total_responses"],
                "packages": entry["packages"],
            })

        return {
            "summary": {
                "drafted": summary_drafted,
                "submitted": summary_submitted,
                "not_started": summary_not_started,
            },
            "participants": participants,
        }

    def _build_phone_lookup_index(self, users: list[User]) -> dict[str, list[User]]:
        index: dict[str, list[User]] = {}
        for user in users:
            phone_key = (user.phone or "").strip()
            if not phone_key:
                continue
            bucket = index.setdefault(phone_key, [])
            if not any(int(row.user_id) == int(user.user_id) for row in bucket):
                bucket.append(user)
        return index

    def _resolve_user_by_phone_from_index(self, phone: str, phone_index: dict[str, list[User]]) -> User | None:
        rows_by_user_id: dict[int, User] = {}
        for candidate in _phone_lookup_candidates(phone):
            for user in phone_index.get(candidate, []):
                rows_by_user_id[int(user.user_id)] = user

        rows = list(rows_by_user_id.values())
        if not rows:
            return None

        primaries = [u for u in rows if u.parent_id is None]
        if len(primaries) > 1:
            raise AppError(
                status_code=409,
                error_code="AMBIGUOUS_PHONE",
                message="Multiple accounts match this phone number",
            )
        if len(primaries) == 1:
            return primaries[0]

        subs = [u for u in rows if u.parent_id is not None]
        if len(subs) > 1:
            raise AppError(
                status_code=409,
                error_code="AMBIGUOUS_PHONE",
                message="Multiple accounts match this phone number",
            )
        return subs[0] if subs else None

    async def _preload_phone_lookup_index(self, db: AsyncSession, raw_phones: list[str]) -> dict[str, list[User]]:
        candidates: list[str] = []
        for phone in raw_phones:
            candidates.extend(_phone_lookup_candidates(phone))
        unique = list(dict.fromkeys(c for c in candidates if c))
        if not unique:
            return {}
        users = await self._users_repository.list_users_by_phones(db, unique)
        return self._build_phone_lookup_index(users)

    async def _resolve_user_by_phone(self, db: AsyncSession, phone: str) -> User | None:
        phone_index = await self._preload_phone_lookup_index(db, [phone])
        return self._resolve_user_by_phone_from_index(phone, phone_index)

    def _users_matching_phone_from_index(self, phone: str, phone_index: dict[str, list[User]]) -> list[User]:
        rows_by_user_id: dict[int, User] = {}
        for candidate in _phone_lookup_candidates(phone):
            for user in phone_index.get(candidate, []):
                rows_by_user_id[int(user.user_id)] = user
        return list(rows_by_user_id.values())

    def _resolve_user_by_phone_and_email_from_index(
        self,
        phone: str,
        email: str | None,
        phone_index: dict[str, list[User]],
    ) -> User | None:
        email_norm = (email or "").strip().lower()
        if email_norm:
            matched = [
                u
                for u in self._users_matching_phone_from_index(phone, phone_index)
                if (u.email or "").strip().lower() == email_norm
            ]
            if not matched:
                return None
            if len(matched) > 1:
                raise AppError(
                    status_code=409,
                    error_code="AMBIGUOUS_PHONE",
                    message="Multiple accounts match this phone and email",
                )
            return matched[0]

        return self._resolve_user_by_phone_from_index(phone, phone_index)

    @staticmethod
    def _metsights_records_list_payload(data: Any) -> list[dict[str, Any]]:
        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]
        if isinstance(data, dict):
            inner = data.get("results")
            if isinstance(inner, list):
                return [r for r in inner if isinstance(r, dict)]
        return []

    @staticmethod
    def _parse_metsights_datetime(raw: Any) -> datetime | None:
        value = (str(raw).strip() if raw is not None else "")
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed

    async def _load_metsights_record_created_at_index(
        self,
        profile_id: str,
        *,
        cache: dict[str, dict[str, datetime]],
    ) -> dict[str, datetime]:
        pid = (profile_id or "").strip()
        if not pid:
            return {}
        if pid in cache:
            return cache[pid]

        if self._metsights_service is None:
            raise RuntimeError("Metsights service is required")

        raw_records = await self._metsights_service.list_profile_records(profile_id=pid)
        rows = self._metsights_records_list_payload(raw_records)
        index: dict[str, datetime] = {}
        for row in rows:
            mrid = str(row.get("id") or "").strip()
            if not mrid:
                continue
            created_at = self._parse_metsights_datetime(row.get("created_at"))
            if created_at is not None:
                index[mrid] = created_at
        cache[pid] = index
        return index

    async def _resolve_user_for_metsights_record_by_phone(
        self,
        *,
        phone: str,
        metsights_record_id: str,
        phone_index: dict[str, list[User]],
        metsights_record_created_at_cache: dict[str, dict[str, datetime]],
    ) -> tuple[User | None, datetime | None, str | None]:
        """Find a user among all accounts sharing *phone* whose Metsights profile contains *metsights_record_id*."""

        mrid = (metsights_record_id or "").strip()
        if not mrid:
            return None, None, "missing_record_id"

        candidates = self._users_matching_phone_from_index(phone, phone_index)
        if not candidates:
            return None, None, "user_not_found"

        matched: list[tuple[User, datetime]] = []
        seen_profile_ids: set[str] = set()
        for user in sorted(candidates, key=lambda u: int(u.user_id)):
            profile_id = (user.metsights_profile_id or "").strip()
            if not profile_id or profile_id in seen_profile_ids:
                continue
            seen_profile_ids.add(profile_id)
            record_index = await self._load_metsights_record_created_at_index(
                profile_id,
                cache=metsights_record_created_at_cache,
            )
            assigned_at = record_index.get(mrid)
            if assigned_at is not None:
                matched.append((user, assigned_at))

        if not matched:
            return None, None, "metsights_record_not_found"

        user, assigned_at = matched[0]
        return user, assigned_at, None

    async def assign_participants_batch(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        rows: list[dict[str, str]],
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict[str, Any]:
        """Enroll users by phone and assign assessment instances keyed by Metsights record id.

        For each CSV row, every user account with the same phone is checked against Metsights
        (``GET /profiles/{profile_id}/records/``). The user whose profile contains the record id
        from the CSV is enrolled and assigned. Email is optional; when absent, resolution uses only
        phone + Metsights record id. Database changes for a row are rolled back on failure.
        """

        self._ensure_employee_access(employee)

        if self._assessments_service is None:
            raise RuntimeError("Assessments service is required")
        if self._metsights_service is None:
            raise RuntimeError("Metsights service is required")

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")

        if (engagement.status or "").lower() != "active":
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Engagement is no longer active")

        package_id = engagement.assessment_package_id
        if package_id is None:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Engagement has no assessment package configured",
            )

        engagement_date = engagement.start_date or date.today()
        default_slot = time(10, 0)
        results: list[dict[str, Any]] = []

        mrids = [(row.get("metsights_record_id") or "").strip() for row in rows]
        phones = [(row.get("phone") or "").strip() for row in rows if (row.get("phone") or "").strip()]

        existing_by_mrid = await self._assessments_service.get_instances_by_metsights_record_ids(db, mrids)
        phone_index = await self._preload_phone_lookup_index(db, phones)
        metsights_record_created_at_cache: dict[str, dict[str, datetime]] = {}

        candidate_user_ids: list[int] = []
        seen_user_ids: set[int] = set()
        for row in rows:
            phone_raw = (row.get("phone") or "").strip()
            if not phone_raw:
                continue
            for user in self._users_matching_phone_from_index(phone_raw, phone_index):
                uid = int(user.user_id)
                if uid not in seen_user_ids:
                    seen_user_ids.add(uid)
                    candidate_user_ids.append(uid)

        enrolled_user_ids = await self._repository.list_enrolled_user_ids_for_engagement(
            db,
            engagement_id=engagement.engagement_id,
            user_ids=candidate_user_ids,
        )
        participants_by_user = await self._repository.get_participants_map_for_engagement(
            db,
            engagement_id=engagement.engagement_id,
            user_ids=candidate_user_ids,
        )

        for row in rows:
            mrid = (row.get("metsights_record_id") or "").strip()
            phone_raw = (row.get("phone") or "").strip()
            email_raw = (row.get("email") or "").strip()
            base: dict[str, Any] = {
                "metsights_record_id": mrid or row.get("metsights_record_id", ""),
                "phone": phone_raw or row.get("phone", ""),
                "email": email_raw or row.get("email", ""),
                "status": "error",
                "reason": None,
                "user_id": None,
                "assessment_instance_id": None,
                "newly_enrolled": None,
            }

            if not mrid:
                base["status"] = "skipped"
                base["reason"] = "missing_record_id"
                results.append(base)
                continue

            if not phone_raw:
                base["status"] = "skipped"
                base["reason"] = "missing_phone"
                results.append(base)
                continue

            base["metsights_record_id"] = mrid
            base["phone"] = phone_raw
            base["email"] = email_raw

            existing_inst = existing_by_mrid.get(mrid)
            if existing_inst is not None:
                base["status"] = "skipped"
                base["reason"] = "already_assigned"
                base["assessment_instance_id"] = int(existing_inst.assessment_instance_id)
                base["user_id"] = int(existing_inst.user_id)
                results.append(base)
                continue

            user, assigned_at, resolve_reason = await self._resolve_user_for_metsights_record_by_phone(
                phone=phone_raw,
                metsights_record_id=mrid,
                phone_index=phone_index,
                metsights_record_created_at_cache=metsights_record_created_at_cache,
            )
            if user is None or assigned_at is None:
                if resolve_reason == "user_not_found":
                    base["status"] = "skipped"
                else:
                    base["status"] = "error"
                base["reason"] = resolve_reason or "metsights_record_not_found"
                results.append(base)
                continue

            base["user_id"] = int(user.user_id)
            if not email_raw:
                base["email"] = (user.email or "").strip()
            newly_enrolled = False
            user_id = int(user.user_id)
            profile_on_metsights = bool((user.metsights_profile_id or "").strip())

            try:
                async with db.begin_nested():
                    if user_id not in enrolled_user_ids:
                        await self.enroll_user_in_engagement(
                            db,
                            engagement=engagement,
                            user_id=user_id,
                            engagement_date=engagement_date,
                            slot_start_time=default_slot,
                            increment_participant_count=True,
                            is_profile_created_on_metsights=profile_on_metsights,
                            is_primary_record_id_synced=False,
                        )
                        newly_enrolled = True
                        enrolled_user_ids.add(user_id)

                    if newly_enrolled:
                        await self.notify_onboarding_assistants_after_enrollment(
                            db,
                            engagement=engagement,
                            user=user,
                            source=engagement.engagement_code or "assign-participants",
                            collection_date=engagement_date.isoformat(),
                            collection_time=default_slot.isoformat(),
                        )

                    instance = await self._assessments_service.create_instance_for_metsights_record(
                        db,
                        user_id=user_id,
                        engagement_id=engagement.engagement_id,
                        package_id=int(package_id),
                        metsights_record_id=mrid,
                        metsights_is_complete=False,
                        ip_address=ip_address,
                        user_agent=user_agent,
                        endpoint=endpoint,
                        assigned_at=assigned_at,
                    )
                    existing_by_mrid[mrid] = instance

                    participant = participants_by_user.get(user_id)
                    if participant is None:
                        participant = await self._repository.get_participant_for_user_engagement(
                            db,
                            user_id=user_id,
                            engagement_id=engagement.engagement_id,
                        )
                        if participant is not None:
                            participants_by_user[user_id] = participant

                    if participant is not None:
                        await self.update_participant_sync_flags(
                            db,
                            participant=participant,
                            is_profile_created_on_metsights=(
                                True if profile_on_metsights else None
                            ),
                            is_primary_record_id_synced=True,
                        )

                base["status"] = "assigned"
                base["assessment_instance_id"] = int(instance.assessment_instance_id)
                base["newly_enrolled"] = newly_enrolled
                if not newly_enrolled:
                    base["reason"] = "already_enrolled"
            except AppError as exc:
                base["status"] = "error"
                base["reason"] = exc.message or exc.error_code or "assignment_failed"
            except IntegrityError:
                base["status"] = "error"
                base["reason"] = "database_constraint_violation"
            except Exception as exc:
                base["status"] = "error"
                base["reason"] = str(exc)

            results.append(base)

        await self._refresh_participant_count(db, engagement)
        return {"results": results}

    async def create_metsights_profiles_for_engagement_participants(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict[str, Any]:
        """Create regular Metsights profiles (POST /profiles/) for enrolled participants.

        Skips users who already have ``metsights_profile_id``. Does not use engagement registration.
        """

        _ = ip_address, user_agent, endpoint
        self._ensure_employee_access(employee)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")

        if self._metsights_service is None:
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message="Metsights integration is not configured",
            )

        user_ids = await self._repository.list_distinct_participant_ids_for_engagement(
            db,
            engagement_id=engagement_id,
        )

        created = 0
        skipped = 0
        failed = 0
        results: list[dict[str, Any]] = []

        for user_id in user_ids:
            base: dict[str, Any] = {"user_id": user_id, "status": "pending", "metsights_profile_id": None, "reason": None}
            user = await self._users_repository.get_user_by_id(db, user_id)
            if user is None:
                base["status"] = "error"
                base["reason"] = "user_not_found"
                failed += 1
                results.append(base)
                continue

            existing_profile_id = (user.metsights_profile_id or "").strip()
            if existing_profile_id:
                base["status"] = "skipped"
                base["reason"] = "already_has_metsights_profile_id"
                base["metsights_profile_id"] = existing_profile_id
                skipped += 1
                results.append(base)
                continue

            first_name = (user.first_name or "").strip()
            last_name = (user.last_name or "").strip()
            gender = _to_metsights_gender(user.gender)
            phone = _normalize_phone_for_metsights(user.phone) or (user.phone or "").strip()
            dob = user.date_of_birth.isoformat() if user.date_of_birth is not None else None
            email = (user.email or "").strip() if user.email else None

            if not first_name or not last_name or not phone or gender is None:
                base["status"] = "error"
                base["reason"] = "missing_required_user_fields"
                failed += 1
                results.append(base)
                continue

            if not dob and user.age is None:
                base["status"] = "error"
                base["reason"] = "missing_date_of_birth_or_age"
                failed += 1
                results.append(base)
                continue

            profile_id: str | None = None
            candidate_phones = [phone]
            raw_phone = (user.phone or "").strip()
            if raw_phone and raw_phone not in candidate_phones:
                candidate_phones.append(raw_phone)

            last_error: str | None = None
            for candidate_phone in candidate_phones:
                try:
                    profile_id = await self._metsights_service.get_or_create_profile_id(
                        first_name=first_name,
                        last_name=last_name,
                        phone=candidate_phone,
                        email=email,
                        gender=gender,
                        date_of_birth=dob,
                        age=user.age,
                    )
                    if profile_id:
                        break
                except AppError as exc:
                    last_error = exc.message or exc.error_code or "metsights_error"
                except Exception as exc:
                    last_error = str(exc)

            if not profile_id:
                base["status"] = "error"
                base["reason"] = last_error or "metsights_profile_creation_failed"
                failed += 1
                results.append(base)
                continue

            await self._users_repository.update_user_partial(
                db,
                user_id,
                {"metsights_profile_id": profile_id},
            )

            participant = await self._repository.get_participant_for_user_engagement(
                db,
                user_id=user_id,
                engagement_id=engagement_id,
            )
            if participant is not None:
                await self.update_participant_sync_flags(
                    db,
                    participant=participant,
                    is_profile_created_on_metsights=True,
                )

            base["status"] = "created"
            base["metsights_profile_id"] = profile_id
            created += 1
            results.append(base)

        return {
            "engagement_id": engagement_id,
            "total": len(user_ids),
            "created": created,
            "skipped": skipped,
            "failed": failed,
            "results": results,
        }
