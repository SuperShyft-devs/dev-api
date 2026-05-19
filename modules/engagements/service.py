"""Engagements service.

Business rules:
- Engagement creation
- Enrolling users by creating `engagement_participants`
- Updating `participant_count`
"""

from __future__ import annotations

import secrets
import string
from datetime import date, time
from typing import TYPE_CHECKING, Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.assessments.repository import AssessmentsRepository
from modules.assessments.service import AssessmentsService
from modules.audit.service import AuditService
from modules.checklists.schemas import ChecklistReadiness
from modules.employee.service import EmployeeContext
from modules.engagements.models import Engagement, EngagementKind, EngagementParticipant, OnboardingAssistantAssignment
from modules.engagements.repository import EngagementsRepository
from modules.engagements.schemas import EngagementCreateRequest, EngagementUpdateRequest
from modules.organizations.repository import OrganizationsRepository
from modules.questionnaire.repository import QuestionnaireRepository
from modules.reports.repository import ReportsRepository
from modules.users.models import User
from modules.users.repository import UsersRepository

if TYPE_CHECKING:
    from modules.checklists.service import ChecklistsService


def _generate_engagement_code(length: int = 8) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


_ALLOWED_ENGAGEMENT_STATUS = {"active", "inactive", "archived"}
DEFAULT_B2C_DIAGNOSTIC_PACKAGE_ID = 1
_B2C_DEFAULT_ONBOARDING_ASSISTANT_EMPLOYEE_IDS = (1, 8)


def _normalize_status(value: str | None) -> str:
    return (value or "").strip().lower()


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


class EngagementsService:
    def __init__(
        self,
        repository: EngagementsRepository,
        audit_service: AuditService | None = None,
        organizations_repository: OrganizationsRepository | None = None,
        users_repository: UsersRepository | None = None,
        assessments_service: AssessmentsService | None = None,
    ):
        self._repository = repository
        self._audit_service = audit_service
        self._organizations_repository = organizations_repository
        self._users_repository = users_repository or UsersRepository()
        self._assessments_service = assessments_service
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
        )

        total = await self._repository.count_engagements(
            db,
            organization_id=organization_id,
            status=status_value,
            city=city,
            on_date=on_date,
        )

        checklists = self.lazy_checklists_service()
        readiness_by_id: dict[int, ChecklistReadiness] = {}
        for row in engagements:
            readiness_by_id[row.engagement_id] = await checklists.get_engagement_readiness(db, row.engagement_id)

        return engagements, total, readiness_by_id

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

        return engagement

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

        # Validate that the organization exists
        if self._organizations_repository is not None:
            organization = await self._organizations_repository.get_by_id(db, payload.organization_id)
            if organization is None:
                raise AppError(
                    status_code=404,
                    error_code="ORGANIZATION_NOT_FOUND",
                    message=f"Organization with ID {payload.organization_id} does not exist",
                )

        engagement.engagement_name = payload.engagement_name
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

        if increment_participant_count:
            engagement.participant_count = int(engagement.participant_count or 0) + 1
            await self._repository.update_engagement(db, engagement)

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
        return await self._repository.create_participant(db, participant)

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
            page=page,
            limit=limit,
        )

        # Count total participant enrollment rows
        total = await self._repository.count_participants_by_engagement_code(
            db,
            engagement_code=engagement_code,
        )

        # Transform tuple results to dictionary format
        result = []
        for row in participants:
            (
                engagement_participant_id,
                engagement_id,
                user_id,
                first_name,
                last_name,
                phone,
                email,
                city,
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
            result.append({
                "engagement_participant_id": engagement_participant_id,
                "engagement_id": engagement_id,
                "user_id": user_id,
                "first_name": first_name,
                "last_name": last_name,
                "phone": phone,
                "email": email,
                "city": city,
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
            })

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

        # Transform tuple results to dictionary format
        result = []
        for row in participants:
            (
                engagement_participant_id,
                engagement_id,
                user_id,
                first_name,
                last_name,
                phone,
                email,
                city,
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
            result.append({
                "engagement_participant_id": engagement_participant_id,
                "engagement_id": engagement_id,
                "user_id": user_id,
                "first_name": first_name,
                "last_name": last_name,
                "phone": phone,
                "email": email,
                "city": city,
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
            })

        return result, total

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

        instances = await self._assessments_repository.list_instances_for_user_engagement(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
        )

        deleted_reports = 0
        deleted_questionnaire_responses = 0
        deleted_category_progress = 0
        deleted_instances = 0

        for instance in instances:
            instance_id = int(instance.assessment_instance_id)
            deleted_reports += await self._reports_repository.delete_individual_reports_for_instance(
                db,
                assessment_instance_id=instance_id,
            )
            deleted_questionnaire_responses += await self._questionnaire_repository.delete_responses_for_instance(
                db,
                assessment_instance_id=instance_id,
            )
            deleted_category_progress += await self._assessments_repository.delete_category_progress_for_instance(
                db,
                assessment_instance_id=instance_id,
            )
            deleted_instances += await self._assessments_repository.delete_instance(
                db,
                assessment_instance_id=instance_id,
            )

        deleted_participants = await self._repository.delete_participants_for_user_engagement(
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
            "deleted_engagement_participants": deleted_participants,
            "deleted_assessment_instances": deleted_instances,
            "deleted_questionnaire_responses": deleted_questionnaire_responses,
            "deleted_reports": deleted_reports,
            "deleted_category_progress_rows": deleted_category_progress,
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
        """Enroll users by phone and assign assessment instances keyed by Metsights record id."""

        self._ensure_employee_access(employee)

        if self._assessments_service is None:
            raise RuntimeError("Assessments service is required")

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

        resolved_users: dict[str, User | None] = {}
        ambiguous_phones: set[str] = set()
        for phone_raw in phones:
            if phone_raw in resolved_users or phone_raw in ambiguous_phones:
                continue
            try:
                resolved_users[phone_raw] = self._resolve_user_by_phone_from_index(phone_raw, phone_index)
            except AppError:
                ambiguous_phones.add(phone_raw)

        candidate_user_ids = [
            int(user.user_id) for user in resolved_users.values() if user is not None
        ]
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
            base: dict[str, Any] = {
                "metsights_record_id": mrid or row.get("metsights_record_id", ""),
                "phone": phone_raw or row.get("phone", ""),
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

            existing_inst = existing_by_mrid.get(mrid)
            if existing_inst is not None:
                base["status"] = "skipped"
                base["reason"] = "already_assigned"
                base["assessment_instance_id"] = int(existing_inst.assessment_instance_id)
                base["user_id"] = int(existing_inst.user_id)
                results.append(base)
                continue

            if phone_raw in ambiguous_phones:
                base["status"] = "error"
                base["reason"] = "ambiguous_phone"
                results.append(base)
                continue

            user = resolved_users.get(phone_raw)
            if user is None:
                base["status"] = "skipped"
                base["reason"] = "user_not_found"
                results.append(base)
                continue

            base["user_id"] = int(user.user_id)
            newly_enrolled = False
            user_id = int(user.user_id)

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
                            is_primary_record_id_synced=False,
                        )
                        newly_enrolled = True
                        enrolled_user_ids.add(user_id)

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

        return {"results": results}
