"""Engagements service.

Business rules:
- Engagement creation
- Enrolling users by creating `engagement_participants`
"""

from __future__ import annotations

import asyncio
import copy
import logging
import secrets
import string
from datetime import date, datetime, time, timedelta, timezone
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from common.masking import mask_email, mask_phone
from common.phone import phone_lookup_candidates as _phone_lookup_candidates
from common.schedule_cutoff import (
    MSG_BLOOD_COLLECTION_RESCHEDULE,
    ensure_schedule_change_allowed,
)
from core.config import settings
from core.exceptions import AppError
from modules.assessments.essentials_vitals import (
    category_optional_for_essentials_completion,
    is_metsights_essentials,
)
from modules.assessments.repository import AssessmentsRepository
from modules.assessments.service import AssessmentsService
from modules.audit.service import AuditService
from modules.checklists.schemas import ChecklistReadiness
from modules.employee.access_control import (
    ONBOARDING_ASSISTANT_ASSIGNEE_EMPLOYEE_ROLES,
    ensure_admin,
)
from modules.employee.models import EmployeeRole
from modules.employee.repository import EmployeeRepository
from modules.employee.service import EmployeeContext
from modules.engagements.blood_booking_id_validation import (
    booking_id_integrity_app_error,
    ensure_active_booking_id_available,
)
from modules.engagements.camp_no import compute_camp_no
from modules.engagements.consultation_booking_validation import (
    effective_consultation_mode,
)
from modules.engagements.enums import EngagementPaymentType
from modules.engagements.models import BloodCollectionType, ConsultationMode, Engagement, EngagementParticipant, EngagementStatus, OnboardingAssistantAssignment
from modules.partners.models import PartnerRole
from modules.partners.role_utils import partner_role_value
from modules.partners.repository import PartnersRepository
from modules.engagements.participant_list_filters import (
    ParticipantListFilters,
    filters_are_active,
    parse_participant_list_filters,
)
from modules.engagements.metsights_pro_validation import ensure_metsights_pro_allowed_for_engagement
from modules.engagements.repository import EngagementsRepository
from modules.engagements.slot_availability import (
    build_cabin_slot_rows,
    build_public_slot_detail,
    capacity_for_slot,
    coerce_time,
    format_hhmm,
    generate_slot_starts,
    locate_cabin,
    occupancy_map_from_rows,
    require_available_blood_collection_slot,
    require_available_consultation_slot,
    section_is_enabled,
    slot_detail_is_configured,
    slot_unavailable,
)
from modules.engagements.slot_detail_dates import _blood_collection_section
from modules.engagements.slot_info_repository import EngagementSlotInfoRepository
from modules.experts.consultation_bookings_repository import ConsultationBookingsRepository
from modules.experts.consultations import bookings_to_consultations_map, empty_consent, normalize_consultations_map, normalize_consent
from modules.engagements.schemas import (
    EngagementCreateRequest,
    EngagementParticipantUpdateRequest,
    EngagementRescheduleRequest,
    EngagementUpdateRequest,
    ConsultationConsentRequest,
    ResolveHealthiansZoneRequest,
    ResolveHealthiansZoneResponse,
    SlotCapacityUpdateRequest,
    SlotDetail,
)
from modules.experts.repository import ExpertTypesRepository
from modules.experts.service import ExpertTypesService
from modules.notifications.repository import NotificationsRepository
from modules.organizations.repository import OrganizationsRepository
from modules.platform_settings.repository import PlatformSettingsRepository
from modules.organizations.service import validate_participant_department_for_organization
from modules.questionnaire.repository import QuestionnaireRepository
from modules.reports.repository import ReportsRepository
from modules.metsights.service import outbound_last_name
from modules.users.models import User
from modules.users.repository import UsersRepository

if TYPE_CHECKING:
    from modules.checklists.service import ChecklistsService
    from modules.metsights.service import MetsightsService
    from modules.metsights.sync_service import MetsightsSyncService
    from modules.notifications.service import NotificationsService


logger = logging.getLogger(__name__)


def _consultations_enabled(consultations: dict[str, bool] | None) -> bool:
    if not consultations:
        return False
    return any(value is True for value in consultations.values())


def _prepare_slot_detail_for_consultation_mode(
    slot_detail: SlotDetail | None,
    consultation_mode: ConsultationMode | None,
) -> SlotDetail | None:
    if slot_detail is None:
        return None
    if consultation_mode != ConsultationMode.online:
        return slot_detail
    dumped = slot_detail.model_dump(mode="json", exclude_none=True)
    dumped.pop("consultation", None)
    if not dumped:
        return None
    return SlotDetail.model_validate(dumped)


def _generate_engagement_code(length: int = 8) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


_ALLOWED_ENGAGEMENT_STATUS = {"draft", "scheduled", "running", "completed", "cancelled"}
DEFAULT_B2C_DIAGNOSTIC_PACKAGE_ID = 1


def _normalize_status(value: str | None) -> str:
    return (value or "").strip().lower()


def _engagement_consultations_fingerprint(raw: Any) -> dict[str, bool]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, bool] = {}
    for key, value in raw.items():
        if isinstance(value, dict):
            out[str(key)] = bool(value.get("want"))
        else:
            out[str(key)] = bool(value)
    return out


def _blood_collection_type_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, BloodCollectionType):
        return value.value
    if hasattr(value, "value"):
        return str(value.value)
    return str(value)


def _resolve_auto_booking(
    *,
    auto_booking: bool | None,
    blood_collection_type: BloodCollectionType | str | None,
) -> bool:
    """Camp collection cannot use auto_booking (partner home-collection flow)."""
    if bool(auto_booking) and _blood_collection_type_value(blood_collection_type) == BloodCollectionType.camp_collection.value:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message="Auto booking is not available when blood collection mode is camp collection",
        )
    return bool(auto_booking)


def _participant_move_mismatch_reasons(source: Engagement, target: Engagement) -> list[str]:
    mismatches: list[str] = []
    if bool(source.create_profile_on_metsights) != bool(target.create_profile_on_metsights):
        mismatches.append("Create Profile on Metsights")
    if source.assessment_package_id != target.assessment_package_id:
        mismatches.append("Assessment package")
    if source.diagnostic_package_id != target.diagnostic_package_id:
        mismatches.append("diagnostic_package_id")
    if source.diagnostic_package_id_male != target.diagnostic_package_id_male:
        mismatches.append("diagnostic_package_id_male")
    if source.diagnostic_package_id_female != target.diagnostic_package_id_female:
        mismatches.append("diagnostic_package_id_female")
    if source.engagement_type != target.engagement_type:
        mismatches.append("Engagement Type")
    if _blood_collection_type_value(source.blood_collection_type) != _blood_collection_type_value(
        target.blood_collection_type
    ):
        mismatches.append("Blood Collection Type")
    if _engagement_consultations_fingerprint(source.consultations) != _engagement_consultations_fingerprint(
        target.consultations
    ):
        mismatches.append("Consultations")
    if bool(source.enroll_for_fitprint_full) != bool(target.enroll_for_fitprint_full):
        mismatches.append("Enroll for FitPrint Full")
    if bool(source.load_prev_assessment_questionnaires) != bool(target.load_prev_assessment_questionnaires):
        mismatches.append("Load previous assessment questionnaires")
    source_keys = sorted((source.load_prev_questionnaire_category_keys or []) or [])
    target_keys = sorted((target.load_prev_questionnaire_category_keys or []) or [])
    if source_keys != target_keys:
        mismatches.append("Load previous questionnaire categories")
    return mismatches


def _normalize_load_prev_category_keys(keys: list[str] | None) -> list[str] | None:
    if keys is None:
        return None
    cleaned = sorted({str(k).strip() for k in keys if str(k).strip()})
    return cleaned or None


def _cannot_move_participant_message(reason: str) -> str:
    return f"Cannot move this participant. {reason}"


def _parse_status_filter(status: str | None) -> list[str] | None:
    if status is None:
        return None
    raw_values = [part.strip() for part in status.split(",") if part.strip()]
    if not raw_values:
        return None
    normalized_values: list[str] = []
    for raw in raw_values:
        normalized = _normalize_status(raw)
        if normalized not in _ALLOWED_ENGAGEMENT_STATUS:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        normalized_values.append(normalized)
    return normalized_values


def _participant_display_name(first_name, last_name, user_id: int) -> str:
    parts = [str(first_name or "").strip(), str(last_name or "").strip()]
    name = " ".join(part for part in parts if part)
    return name or f"User #{user_id}"


def _participant_issues_from_completeness(engagement, participants: list[dict]) -> list[dict]:
    """Flatten missing blood slot, questionnaire, blood report, and Bio-AI report issues."""
    issues: list[dict] = []
    engagement_id = int(engagement.engagement_id)
    for participant in participants:
        user_id = int(participant["user_id"])
        base = {
            "user_id": user_id,
            "participant_name": _participant_display_name(
                participant.get("first_name"),
                participant.get("last_name"),
                user_id,
            ),
            "engagement_id": engagement_id,
            "engagement_name": engagement.engagement_name,
            "engagement_code": engagement.engagement_code,
        }
        if not participant.get("has_booking_id"):
            issues.append({**base, "issue_type": "missing_blood_slot", "status": "missing"})
        if participant.get("questionnaire_state") != "filled":
            issues.append(
                {
                    **base,
                    "issue_type": "missing_questionnaire",
                    "status": participant.get("questionnaire_state") or "not_started",
                }
            )
        if not participant.get("has_blood_report"):
            issues.append({**base, "issue_type": "missing_blood_report", "status": "missing"})
        if not participant.get("has_bio_ai_report"):
            issues.append({**base, "issue_type": "missing_bio_ai_report", "status": "missing"})
    return issues


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


def _participant_enrollment_to_dict(
    row: tuple,
    *,
    consultations: dict[str, Any] | None = None,
    sync_flags: dict[str, bool] | None = None,
) -> dict[str, Any]:
    (
        engagement_participant_id,
        engagement_id,
        user_id,
        first_name,
        last_name,
        phone,
        email,
        age,
        address,
        pin_code,
        city,
        state,
        country,
        status,
        slot_start_time,
        engagement_date,
        blood_collection_cabin,
        participants_employee_id,
        participant_department,
        participant_blood_group,
        _consultation_booking_ids,
        metsights_profile_id,
        barcode,
        booking_id,
        blood_collection_time_slot_id,
        booked_by_user_id,
        paid,
        booker_message,
        booker_relation,
    ) = row
    flags = sync_flags or {
        "is_profile_created_on_metsights": bool((metsights_profile_id or "").strip()),
        "is_primary_record_id_synced": False,
        "is_fitprint_record_id_synced": False,
    }
    return {
        "engagement_participant_id": engagement_participant_id,
        "engagement_id": engagement_id,
        "user_id": user_id,
        "first_name": first_name,
        "last_name": last_name,
        "phone": mask_phone(phone),
        "email": mask_email(email),
        "age": age,
        "address": address,
        "pin_code": pin_code,
        "city": city,
        "state": state,
        "country": country,
        "status": status,
        "slot_start_time": slot_start_time.isoformat() if slot_start_time is not None else None,
        "engagement_date": engagement_date.isoformat() if engagement_date is not None else None,
        "blood_collection_cabin": blood_collection_cabin,
        "participants_employee_id": participants_employee_id,
        "participant_department": participant_department,
        "participant_blood_group": participant_blood_group,
        "consultations": consultations or {},
        "is_profile_created_on_metsights": flags["is_profile_created_on_metsights"],
        "is_primary_record_id_synced": flags["is_primary_record_id_synced"],
        "is_fitprint_record_id_synced": flags["is_fitprint_record_id_synced"],
        "barcode": barcode,
        "booking_id": booking_id,
        "blood_collection_time_slot_id": blood_collection_time_slot_id,
        "booked_by_user_id": booked_by_user_id,
        "paid": bool(paid) if paid is not None else True,
        "booker_message": booker_message,
        "booker_relation": booker_relation,
    }


def _ist_today() -> date:
    return datetime.now(ZoneInfo("Asia/Kolkata")).date()


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
        consultation_bookings_repository: ConsultationBookingsRepository | None = None,
        expert_types_service: ExpertTypesService | None = None,
        slot_info_repository: EngagementSlotInfoRepository | None = None,
        camp_onboarding_payload_service: Any | None = None,
    ):
        self._repository = repository
        self._audit_service = audit_service
        self._organizations_repository = organizations_repository
        self._users_repository = users_repository or UsersRepository()
        self._assessments_service = assessments_service
        self._metsights_service = metsights_service
        self._notifications_repository = notifications_repository or NotificationsRepository()
        self._notifications_service = notifications_service
        self._platform_settings_repository = PlatformSettingsRepository()
        self._partners_repository = PartnersRepository()
        self._employee_repository = EmployeeRepository()
        self._assessments_repository = AssessmentsRepository()
        self._questionnaire_repository = QuestionnaireRepository()
        self._reports_repository = ReportsRepository()
        self._consultation_bookings = consultation_bookings_repository or ConsultationBookingsRepository()
        self._expert_types_service = expert_types_service or ExpertTypesService(repository=ExpertTypesRepository())
        self._slot_info_repository = slot_info_repository or EngagementSlotInfoRepository()
        self._checklists_service = None
        self._camp_onboarding_payload_service = camp_onboarding_payload_service

    async def resolve_slot_detail(self, db: AsyncSession, engagement: Engagement) -> dict[str, Any] | None:
        if engagement.slot_detail_id is None:
            return None
        return await self._slot_info_repository.get_by_id(db, int(engagement.slot_detail_id))

    async def resolve_slot_details_map(
        self,
        db: AsyncSession,
        engagements: list[Engagement],
    ) -> dict[int, dict[str, Any]]:
        ids = list({int(e.slot_detail_id) for e in engagements if e.slot_detail_id is not None})
        return await self._slot_info_repository.get_by_ids(db, ids)

    async def _persist_slot_detail(
        self,
        db: AsyncSession,
        *,
        organization_id: int | None,
        city: str | None,
        start_date: date,
        end_date: date,
        slot_detail: SlotDetail | None,
        existing_slot_detail_id: int | None = None,
        exclude_engagement_id: int | None = None,
    ) -> int | None:
        if slot_detail is None:
            return None
        data = slot_detail.model_dump(mode="json", exclude_none=True)
        if not slot_detail_is_configured(data):
            return None

        shared_id = await self._slot_info_repository.find_shared_slot_detail_id(
            db,
            organization_id=organization_id,
            city=city,
            start_date=start_date,
            end_date=end_date,
            exclude_engagement_id=exclude_engagement_id,
        )
        if shared_id is not None:
            await self._slot_info_repository.update(db, shared_id, data)
            return shared_id

        if existing_slot_detail_id is not None:
            await self._slot_info_repository.update(db, existing_slot_detail_id, data)
            return existing_slot_detail_id

        return await self._slot_info_repository.create(db, data)

    async def _participant_rows_to_dicts(
        self,
        db: AsyncSession,
        rows: list[tuple],
        *,
        engagement: Engagement | None = None,
    ) -> list[dict[str, Any]]:
        from modules.engagements.blood_bookings_repository import (
            BloodBookingsRepository,
            blood_bookings_to_api_list,
        )
        from modules.engagements.participant_sync_status import (
            load_engagement_sync_user_sets,
            participant_sync_flags,
        )

        all_booking_ids: list[int] = []
        ep_ids: list[int] = []
        user_ids: list[int] = []
        engagement_id: int | None = None
        for row in rows:
            ep_ids.append(int(row[0]))
            if engagement_id is None:
                engagement_id = int(row[1])
            user_ids.append(int(row[2]))
            ids = row[20] or []
            all_booking_ids.extend(int(i) for i in ids)

        sync_sets = None
        if engagement is None and engagement_id is not None:
            engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is not None and user_ids:
            sync_sets = await load_engagement_sync_user_sets(
                db, engagement=engagement, user_ids=list(dict.fromkeys(user_ids))
            )

        bookings = await self._consultation_bookings.get_by_ids(db, list(dict.fromkeys(all_booking_ids)))
        bookings_by_id = {booking.consultation_id: booking for booking in bookings}

        blood_repo = BloodBookingsRepository()
        blood_by_ep: dict[int, list] = {}
        for ep_id in ep_ids:
            blood_by_ep[ep_id] = await blood_repo.list_for_participant(db, engagement_participant_id=ep_id)

        result: list[dict[str, Any]] = []
        for row in rows:
            ids = row[20] or []
            participant_bookings = [bookings_by_id[i] for i in ids if i in bookings_by_id]
            consultations = bookings_to_consultations_map(participant_bookings)
            uid = int(row[2])
            flags = (
                participant_sync_flags(
                    user_id=uid,
                    metsights_profile_id=row[21],
                    sync_sets=sync_sets,
                )
                if sync_sets is not None
                else None
            )
            participant = _participant_enrollment_to_dict(row, consultations=consultations, sync_flags=flags)
            participant["blood_test_complete"] = None
            participant["blood_bookings"] = blood_bookings_to_api_list(
                blood_by_ep.get(int(row[0]), [])
            )
            result.append(participant)
        return result

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

    async def _validate_consultation_expert_types(
        self,
        db: AsyncSession,
        slot_detail: SlotDetail | None,
    ) -> None:
        if slot_detail is None or not slot_detail.consultation:
            return
        seen: set[str] = set()
        for date_entry in slot_detail.consultation.dates.values():
            for cabin in date_entry.cabins:
                key = cabin.expert_type.strip()
                if not key or key in seen:
                    continue
                seen.add(key)
                await self._expert_types_service.validate_type_key(db, key)

    def _group_slots_by_date(self, rows: list[tuple]) -> dict[str, list[str]]:
        """Group (date, time) rows into a {"YYYY-MM-DD": ["HH:MM:SS", ...]} mapping."""

        grouped: dict[str, list[str]] = {}
        for engagement_date, slot_start_time in rows:
            date_key = engagement_date.isoformat()
            grouped.setdefault(date_key, []).append(slot_start_time.isoformat())
        return grouped

    def _require_audit_service(self) -> AuditService:
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        return self._audit_service

    async def _validate_comma_separated_service_keys(self, db: AsyncSession, raw: str | None) -> str | None:
        """Validate a comma-separated list of service keys. Returns the cleaned string or None."""
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

    async def count_participants_for_engagement(self, db: AsyncSession, *, engagement_id: int) -> int:
        return await self._repository.count_distinct_participants_for_engagement(
            db,
            engagement_id=engagement_id,
        )

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
        """Return occupied slots for all running B2C engagements.

        Public (no-auth) use-case.
        Output is grouped by date.
        """

        rows = await self._repository.list_occupied_slots_for_active_b2c_engagements(db)
        return self._group_slots_by_date(rows)

    async def get_by_id(self, db: AsyncSession, engagement_id: int) -> Engagement | None:
        return await self._repository.get_engagement_by_id(db, engagement_id)

    async def _reject_diagnostic_packages_without_external_code(
        self,
        db: AsyncSession,
        *,
        package_ids: list[int | None],
    ) -> None:
        ids = sorted({int(package_id) for package_id in package_ids if package_id})
        if not ids:
            return
        from modules.diagnostics.models import DiagnosticPackage

        result = await db.execute(
            select(DiagnosticPackage).where(DiagnosticPackage.diagnostic_package_id.in_(ids))
        )
        for pkg in result.scalars().all():
            if not (pkg.external_package_code or "").strip():
                raise AppError(
                    status_code=400,
                    error_code="DIAGNOSTIC_PACKAGE_EXTERNAL_CODE_REQUIRED",
                    message=f"Diagnostic package {pkg.package_name} has no external package code",
                )

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
        ensure_admin(employee)

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

        await self._reject_diagnostic_packages_without_external_code(
            db,
            package_ids=[
                payload.diagnostic_package_id,
                payload.diagnostic_package_id_male,
                payload.diagnostic_package_id_female,
            ],
        )

        diagnostic_package_id = payload.diagnostic_package_id
        prepared_slot_detail = _prepare_slot_detail_for_consultation_mode(
            payload.slot_detail,
            payload.consultation_mode,
        )
        await self._validate_consultation_expert_types(db, prepared_slot_detail)

        await ensure_metsights_pro_allowed_for_engagement(
            db,
            engagement_type_id=int(payload.engagement_type),
            assessment_package_id=payload.assessment_package_id,
            diagnostic_package_id=payload.diagnostic_package_id,
            diagnostic_package_id_male=payload.diagnostic_package_id_male,
            diagnostic_package_id_female=payload.diagnostic_package_id_female,
        )

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

        initial_status = "running" if payload.start_date <= date.today() else "scheduled"

        slot_detail_id = await self._persist_slot_detail(
            db,
            organization_id=payload.organization_id,
            city=payload.city,
            start_date=payload.start_date,
            end_date=payload.end_date,
            slot_detail=prepared_slot_detail,
        )

        engagement = Engagement(
            engagement_name=payload.engagement_name,
            metsights_engagement_id=payload.metsights_engagement_id,
            organization_id=payload.organization_id,
            camp_no=compute_camp_no(payload.organization_id, payload.start_date),
            engagement_code=code,
            engagement_type=payload.engagement_type,
            consultations=payload.consultations,
            slot_detail_id=slot_detail_id,
            assessment_package_id=payload.assessment_package_id,
            diagnostic_package_id=diagnostic_package_id,
            diagnostic_package_id_male=payload.diagnostic_package_id_male,
            diagnostic_package_id_female=payload.diagnostic_package_id_female,
            city=payload.city,
            address=payload.address,
            sub_locality=payload.sub_locality,
            landmark=payload.landmark,
            pincode=payload.pincode,
            state=payload.state,
            country=payload.country,
            latitude=payload.latitude,
            longitude=payload.longitude,
            slot_duration=payload.slot_duration,
            start_date=payload.start_date,
            end_date=payload.end_date,
            status=initial_status,
            healthians_zone_id=payload.healthians_zone_id,
            external_camp_id=payload.external_camp_id,
            blood_collection_type=payload.blood_collection_type,
            consultation_mode=payload.consultation_mode,
            create_profile_on_metsights=payload.create_profile_on_metsights,
            enroll_for_fitprint_full=payload.enroll_for_fitprint_full,
            load_prev_assessment_questionnaires=payload.load_prev_assessment_questionnaires,
            load_prev_questionnaire_category_keys=_normalize_load_prev_category_keys(
                payload.load_prev_questionnaire_category_keys
            ),
            payment_type=payload.payment_type,
            auto_booking=_resolve_auto_booking(
                auto_booking=payload.auto_booking,
                blood_collection_type=payload.blood_collection_type,
            ),
        )

        engagement = await self._repository.create_engagement(db, engagement)

        from modules.engagement_notifications.repository import EngagementNotificationsRepository
        from modules.engagement_notifications.service_config import serialize_for_db
        en_repo = EngagementNotificationsRepository()
        if payload.notifications is not None:
            items = [
                {
                    "notification_event_id": n.notification_event_id,
                    "notification_services": serialize_for_db(n.notification_services),
                }
                for n in payload.notifications
            ]
            await en_repo.upsert_for_engagement(db, engagement.engagement_id, items)
        else:
            await en_repo.populate_from_defaults(
                db, engagement_id=engagement.engagement_id, engagement_type_id=payload.engagement_type
            )

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_CREATE_ENGAGEMENT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )

        await self._assign_default_onboarding_assistants(
            db,
            engagement_id=engagement.engagement_id,
            organization_id=engagement.organization_id,
        )

        if self._camp_onboarding_payload_service is not None and engagement.camp_no is not None:
            await self._camp_onboarding_payload_service.ensure_row_for_camp_no(
                db,
                camp_no=int(engagement.camp_no),
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
        camp_no: int | None,
        status: str | None,
        city: str | None,
        on_date,
        search: str | None = None,
        engagement_type: str | None = None,
        audience: str | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
        include_readiness: bool = True,
    ) -> tuple[list[Engagement], int, dict[int, ChecklistReadiness]]:
        ensure_admin(employee)

        status_values = _parse_status_filter(status)

        audience_value = None
        if audience is not None:
            normalized_audience = audience.strip().lower()
            if normalized_audience not in {"b2b", "b2c"}:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            audience_value = normalized_audience

        engagements = await self._repository.list_engagements(
            db,
            page=page,
            limit=limit,
            organization_id=organization_id,
            camp_no=camp_no,
            statuses=status_values,
            city=city,
            on_date=on_date,
            search=search,
            engagement_type=engagement_type,
            audience=audience_value,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )

        total = await self._repository.count_engagements(
            db,
            organization_id=organization_id,
            camp_no=camp_no,
            statuses=status_values,
            city=city,
            on_date=on_date,
            search=search,
            engagement_type=engagement_type,
            audience=audience_value,
        )

        counts_by_id = await self._repository.count_distinct_participants_by_engagement_ids(
            db,
            engagement_ids=[int(row.engagement_id) for row in engagements],
        )

        readiness_by_id: dict[int, ChecklistReadiness] = {}
        if include_readiness:
            checklists = self.lazy_checklists_service()
            for row in engagements:
                readiness_by_id[row.engagement_id] = await checklists.get_engagement_readiness(
                    db, row.engagement_id
                )

        return engagements, total, readiness_by_id, counts_by_id

    async def get_engagement_filter_options_for_employee(self, db: AsyncSession, *, employee: EmployeeContext) -> dict:
        ensure_admin(employee)
        types, cities = await self._repository.list_distinct_engagement_types_and_cities(db)
        return {"engagement_types": types, "cities": cities}

    async def get_form_bootstrap(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        packages_service: Any,
        diagnostics_service: Any,
        engagement_types_service: Any,
        expert_types_service: Any,
        notifications_service: Any,
        organizations_service: Any,
    ) -> dict[str, Any]:
        (
            engagement_types,
            org_result,
            assessment_result,
            camp_packages_result,
            public_packages_result,
            notification_services,
            expert_types,
        ) = await asyncio.gather(
            engagement_types_service.list_all(db, is_active=True),
            organizations_service.list_organizations_for_employee(
                db,
                employee=employee,
                page=1,
                limit=100,
                status=None,
                organization_type=None,
                bd_employee_id=None,
                search=None,
                city=None,
                country=None,
                industry_key=None,
                sort_by=None,
                sort_dir=None,
            ),
            packages_service.list_packages(db, page=1, limit=100, status="active"),
            diagnostics_service.get_packages(
                db,
                gender=None,
                tag=None,
                active_only=True,
                list_type="public_package",
                package_for="camp",
            ),
            diagnostics_service.get_packages(
                db,
                gender=None,
                tag=None,
                active_only=True,
                list_type="public_package",
                package_for="public",
            ),
            notifications_service.list_services(db),
            expert_types_service.list_expert_types(db),
        )
        camp_packages, _, _ = camp_packages_result
        public_packages, _, _ = public_packages_result
        org_rows, _org_total = org_result
        assessment_rows, _ = assessment_result
        organizations = [
            {
                "organization_id": organization.organization_id,
                "name": organization.name,
                "organization_type": organization.organization_type,
                "logo": organization.logo,
                "city": organization.city,
                "status": organization.status,
            }
            for organization, _industry in org_rows
        ]
        diagnostic_by_id = {p.diagnostic_package_id: p for p in camp_packages + public_packages}
        bootstrap_packages = await diagnostics_service.diagnostic_packages_for_form_bootstrap(
            db, list(diagnostic_by_id.values())
        )
        return {
            "engagement_types": [item.model_dump(mode="json") for item in engagement_types],
            "organizations": organizations,
            "assessment_packages": [
                {
                    "package_id": p.package_id,
                    "package_code": p.package_code,
                    "display_name": p.display_name,
                    "assessment_type_code": p.assessment_type_code,
                    "subscription_id": p.subscription_id,
                    "status": p.status,
                }
                for p in assessment_rows
            ],
            "diagnostic_packages": bootstrap_packages,
            "notification_services": [
                {
                    "service_key": svc.service_key,
                    "display_name": svc.display_name,
                    "is_active": svc.is_active,
                }
                for svc in notification_services
            ],
            "expert_types": [
                {"id": row.id, "type_key": row.type_key, "type": row.type} for row in expert_types
            ],
        }

    async def resolve_healthians_zone_for_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        payload: ResolveHealthiansZoneRequest,
    ) -> ResolveHealthiansZoneResponse:
        from modules.diagnostics.healthians import client as healthians_client
        from modules.diagnostics.healthians.sync_log import (
            finalize_healthians_sync_log_isolated,
            persist_healthians_sync_log_isolated,
        )
        from modules.diagnostics.models import DiagnosticPackage

        ensure_admin(employee)

        result = await db.execute(
            select(DiagnosticPackage).where(
                DiagnosticPackage.diagnostic_package_id == payload.diagnostic_package_id
            )
        )
        pkg = result.scalar_one_or_none()
        if pkg is None:
            raise AppError(
                status_code=404,
                error_code="PACKAGE_NOT_FOUND",
                message=f"Diagnostic package {payload.diagnostic_package_id} not found",
            )

        provider = (pkg.diagnostic_provider or "").strip().lower()
        if provider != "healthians":
            return ResolveHealthiansZoneResponse(
                serviceable=False,
                zone_id=None,
                message="Zone auto-fill applies only to Healthians diagnostic packages.",
            )

        lat = str(payload.latitude)
        lng = str(payload.longitude)
        zipcode = payload.pincode.strip()
        serviceability_url = f"{settings.HEALTHIANS_BASE_URL}/toast4health/checkServiceabilityByLocation_v2"
        serviceability_payload = {
            "lat": lat,
            "long": lng,
            "zipcode": zipcode,
            "is_ppmc_booking": 0,
        }

        serviceability_log_id = await persist_healthians_sync_log_isolated(
            engagement_id=None,
            user_id=None,
            provider="healthians",
            api_url=serviceability_url,
            request_payload=serviceability_payload,
            status="pending",
        )

        try:
            access_token = await healthians_client.get_access_token()
            serviceability_response = await healthians_client.check_serviceability_by_location_v2(
                access_token,
                lat=lat,
                long=lng,
                zipcode=zipcode,
                is_ppmc_booking=0,
            )
            await finalize_healthians_sync_log_isolated(
                sync_log_id=serviceability_log_id,
                status="success" if serviceability_response.get("status") else "failed",
                response_payload=serviceability_response,
            )
        except Exception as exc:
            logger.exception("Healthians serviceability check failed for engagement zone resolution")
            await finalize_healthians_sync_log_isolated(
                sync_log_id=serviceability_log_id,
                status="failed",
                error_message=str(exc),
            )
            raise AppError(
                status_code=502,
                error_code="HEALTHIANS_SERVICEABILITY_FAILED",
                message=str(exc),
            ) from exc

        if not serviceability_response.get("status"):
            message = serviceability_response.get("message") or "Location is not serviceable"
            return ResolveHealthiansZoneResponse(
                serviceable=False,
                zone_id=None,
                message=message,
            )

        zone_data = serviceability_response.get("data") or {}
        zone_id_raw = zone_data.get("zone_id")
        if zone_id_raw is None:
            return ResolveHealthiansZoneResponse(
                serviceable=False,
                zone_id=None,
                message="Serviceability response did not include zone_id",
            )

        return ResolveHealthiansZoneResponse(
            serviceable=True,
            zone_id=str(zone_id_raw),
            message="Zone ID auto-filled from Healthians.",
        )

    async def get_engagement_by_code_public(self, db: AsyncSession, *, engagement_code: str):
        return await self._repository.get_engagement_with_org_by_code(db, engagement_code)

    async def get_public_camp_onboarding_by_code(
        self,
        db: AsyncSession,
        *,
        camp_code: str,
    ) -> dict[str, Any]:
        if self._camp_onboarding_payload_service is None:
            raise AppError(
                status_code=404,
                error_code="CAMP_NOT_FOUND",
                message="Camp does not exist",
            )
        row = await self._camp_onboarding_payload_service.get_row_by_camp_code(
            db,
            camp_code=camp_code,
        )
        if row is None:
            raise AppError(
                status_code=404,
                error_code="CAMP_NOT_FOUND",
                message="Camp does not exist",
            )
        camp_no = int(row.camp_no)
        engagements = await self._repository.list_engagement_codes_and_cities_by_camp_no(
            db,
            camp_no=camp_no,
        )
        if not engagements:
            raise AppError(
                status_code=404,
                error_code="CAMP_NOT_FOUND",
                message="Camp does not exist",
            )
        organization_name = await self._repository.get_organization_name_by_camp_no(
            db,
            camp_no=camp_no,
        )
        return await self._camp_onboarding_payload_service.public_onboarding_for_camp(
            db,
            camp_no=camp_no,
            engagements=engagements,
            organization_name=organization_name,
        )

    async def public_slot_detail_for_engagement(self, db: AsyncSession, engagement: Engagement):
        slot_detail = await self.resolve_slot_detail(db, engagement)
        if not slot_detail_is_configured(slot_detail):
            return None
        shared_slot_detail_id = (
            int(engagement.slot_detail_id) if engagement.slot_detail_id is not None else None
        )
        rows = await self._repository.list_cabin_slot_occupancy(
            db,
            engagement_id=int(engagement.engagement_id),
            slot_detail_id=shared_slot_detail_id,
        )
        consultation_rows = await self._consultation_bookings.list_consultation_cabin_slot_occupancy(
            db,
            engagement_id=int(engagement.engagement_id),
            slot_detail_id=shared_slot_detail_id,
        )
        return build_public_slot_detail(
            slot_detail,
            occupancy_map_from_rows(rows),
            occupancy_map_from_rows(consultation_rows),
        )

    async def update_slot_capacity_for_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        payload: SlotCapacityUpdateRequest,
    ) -> dict[str, Any]:
        """Merge one cabin slot override into the stored JSON without replacing the rest."""
        ensure_admin(employee)
        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")
        if engagement.slot_detail_id is None:
            raise AppError(
                status_code=400,
                error_code="SLOT_DETAIL_NOT_CONFIGURED",
                message="This engagement has no slot schedule",
            )

        stored = await self.resolve_slot_detail(db, engagement)
        if not isinstance(stored, dict):
            raise AppError(
                status_code=400,
                error_code="SLOT_DETAIL_NOT_CONFIGURED",
                message="This engagement has no slot schedule",
            )

        slot_detail = copy.deepcopy(stored)
        cabin = locate_cabin(
            slot_detail,
            section=payload.section,
            date_key=payload.date,
            cabin_key=payload.cabin_key,
        )
        if cabin is None:
            raise AppError(status_code=404, error_code="CABIN_NOT_FOUND", message="Cabin does not exist")

        try:
            slot_date = date.fromisoformat(payload.date)
        except ValueError as exc:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid date") from exc

        starts = generate_slot_starts(
            cabin.get("start_time"),
            cabin.get("end_time"),
            int(cabin.get("slot_duration") or 0),
            cabin.get("breaks") or [],
        )
        slot_time = coerce_time(payload.slot)
        if slot_time is None or slot_time not in starts:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="That time is not a slot for this cabin",
            )

        try:
            base_capacity = int(cabin.get("capacity_per_slot") or 0)
        except (TypeError, ValueError) as exc:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Cabin capacity is invalid") from exc

        shared_slot_detail_id = int(engagement.slot_detail_id)
        if payload.section == "consultation":
            count = await self._consultation_bookings.count_cabin_slot_bookings(
                db,
                engagement_id=int(engagement.engagement_id),
                consultation_cabin=payload.cabin_key,
                consultation_date=slot_date,
                consultation_slot=format_hhmm(slot_time),
                slot_detail_id=shared_slot_detail_id,
            )
        else:
            count = await self._repository.count_cabin_slot_participants(
                db,
                engagement_id=int(engagement.engagement_id),
                blood_collection_cabin=payload.cabin_key,
                engagement_date=slot_date,
                slot_start_time=time(slot_time.hour, slot_time.minute),
                slot_detail_id=shared_slot_detail_id,
            )
        if payload.capacity < count:
            raise AppError(
                status_code=400,
                error_code="SLOT_CAPACITY_BELOW_BOOKINGS",
                message="Capacity cannot be lower than people already booked in this slot",
            )

        raw_overrides = cabin.get("slot_capacity_overrides")
        overrides = dict(raw_overrides) if isinstance(raw_overrides, dict) else {}
        slot_key = format_hhmm(slot_time)
        if payload.capacity == base_capacity:
            overrides.pop(slot_key, None)
        else:
            overrides[slot_key] = payload.capacity
        cabin["slot_capacity_overrides"] = overrides

        await self._slot_info_repository.update(db, shared_slot_detail_id, slot_detail)

        if payload.section == "consultation":
            occupancy_rows = await self._consultation_bookings.list_consultation_cabin_slot_occupancy(
                db,
                engagement_id=int(engagement.engagement_id),
                slot_detail_id=shared_slot_detail_id,
            )
        else:
            occupancy_rows = await self._repository.list_cabin_slot_occupancy(
                db,
                engagement_id=int(engagement.engagement_id),
                slot_detail_id=shared_slot_detail_id,
            )
        return {
            "section": payload.section,
            "date": payload.date,
            "cabin": cabin,
            "slots": build_cabin_slot_rows(
                cabin,
                slot_date=slot_date,
                occupancy=occupancy_map_from_rows(occupancy_rows),
            ),
        }

    async def _validate_blood_collection_slot_capacity(
        self,
        db: AsyncSession,
        *,
        engagement: Engagement,
        collection_date: date,
        cabin_key: str | None,
        slot_time: time,
        exclude_engagement_participant_id: int | None = None,
    ) -> str | None:
        normalized_cabin = (cabin_key or "").strip() or None
        slot_detail = await self.resolve_slot_detail(db, engagement)
        if not slot_detail_is_configured(slot_detail):
            return normalized_cabin
        if not isinstance(slot_detail, dict) or _blood_collection_section(slot_detail) is None:
            return normalized_cabin

        cabin = require_available_blood_collection_slot(
            slot_detail,
            collection_date=collection_date,
            cabin_key=normalized_cabin,
            slot_time=slot_time,
        )
        persisted_cabin = (cabin.get("cabin_key") or "").strip()
        count = await self._repository.count_cabin_slot_participants(
            db,
            engagement_id=int(engagement.engagement_id),
            blood_collection_cabin=persisted_cabin,
            engagement_date=collection_date,
            slot_start_time=time(slot_time.hour, slot_time.minute),
            slot_detail_id=(
                int(engagement.slot_detail_id) if engagement.slot_detail_id is not None else None
            ),
            exclude_engagement_participant_id=exclude_engagement_participant_id,
        )
        capacity = capacity_for_slot(cabin, slot_time)
        if count >= capacity:
            raise slot_unavailable()
        return persisted_cabin

    async def validate_blood_collection_slot_for_onboard(
        self,
        db: AsyncSession,
        *,
        engagement: Engagement,
        collection_date: date,
        cabin_key: str | None,
        slot_time: time,
    ) -> str | None:
        return await self._validate_blood_collection_slot_capacity(
            db,
            engagement=engagement,
            collection_date=collection_date,
            cabin_key=cabin_key,
            slot_time=slot_time,
        )

    async def validate_consultation_slots_for_onboard(
        self,
        db: AsyncSession,
        *,
        engagement: Engagement,
        consultations: dict[str, Any] | None,
    ) -> dict[str, Any]:
        normalized = normalize_consultations_map(consultations)
        slot_detail = await self.resolve_slot_detail(db, engagement)
        if not slot_detail_is_configured(slot_detail):
            return normalized
        section = None
        if isinstance(slot_detail, dict):
            section = slot_detail.get("consultation")
        if not isinstance(section, dict) or not section:
            return normalized
        if not section_is_enabled(section):
            return normalized

        shared_slot_detail_id = (
            int(engagement.slot_detail_id) if engagement.slot_detail_id is not None else None
        )
        for expert_type, pref in normalized.items():
            if pref.get("want") is not True:
                continue
            date_val = pref.get("date")
            cabin_val = (pref.get("cabin") or "").strip() or None
            slot_val = pref.get("slot")
            if not date_val and not cabin_val and not slot_val:
                continue
            if not date_val or not cabin_val or not slot_val:
                raise slot_unavailable()
            try:
                consultation_date = date.fromisoformat(str(date_val)[:10])
            except ValueError as exc:
                raise slot_unavailable() from exc
            slot_time = coerce_time(slot_val)
            if slot_time is None:
                raise slot_unavailable()
            cabin = require_available_consultation_slot(
                slot_detail,
                expert_type=str(expert_type),
                consultation_date=consultation_date,
                cabin_key=cabin_val,
                slot_time=slot_time,
            )
            persisted_cabin = (cabin.get("cabin_key") or "").strip()
            slot_hhmm = format_hhmm(slot_time)
            count = await self._consultation_bookings.count_cabin_slot_bookings(
                db,
                engagement_id=int(engagement.engagement_id),
                consultation_cabin=persisted_cabin,
                consultation_date=consultation_date,
                consultation_slot=slot_hhmm,
                slot_detail_id=shared_slot_detail_id,
            )
            capacity = capacity_for_slot(cabin, slot_time)
            if count >= capacity:
                raise slot_unavailable()
            pref["cabin"] = persisted_cabin
            pref["slot"] = slot_hhmm
            pref["date"] = consultation_date.isoformat()
        return normalized

    async def get_engagement_details_for_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
    ) -> Engagement:
        ensure_admin(employee)

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
        ensure_admin(employee)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")

        if payload.start_date > payload.end_date:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        await ensure_metsights_pro_allowed_for_engagement(
            db,
            engagement_type_id=int(payload.engagement_type),
            assessment_package_id=payload.assessment_package_id,
            diagnostic_package_id=payload.diagnostic_package_id,
            diagnostic_package_id_male=payload.diagnostic_package_id_male,
            diagnostic_package_id_female=payload.diagnostic_package_id_female,
        )

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

        update_fields = payload.model_dump(exclude_unset=True)

        engagement.engagement_name = payload.engagement_name
        engagement.engagement_code = code
        engagement.organization_id = payload.organization_id
        engagement.engagement_type = payload.engagement_type
        # Callers that never send the key keep whatever is already configured.
        if "consultations" in update_fields:
            engagement.consultations = payload.consultations
        consultation_mode = payload.consultation_mode
        if "consultation_mode" in update_fields:
            engagement.consultation_mode = consultation_mode
        elif "consultations" in update_fields and _consultations_enabled(payload.consultations):
            engagement.consultation_mode = consultation_mode
        effective_mode = engagement.consultation_mode
        if "slot_detail" in update_fields:
            prepared_slot_detail = _prepare_slot_detail_for_consultation_mode(
                payload.slot_detail,
                effective_mode,
            )
            await self._validate_consultation_expert_types(db, prepared_slot_detail)
            if prepared_slot_detail is None:
                engagement.slot_detail_id = None
            else:
                engagement.slot_detail_id = await self._persist_slot_detail(
                    db,
                    organization_id=payload.organization_id,
                    city=payload.city,
                    start_date=payload.start_date,
                    end_date=payload.end_date,
                    slot_detail=prepared_slot_detail,
                    existing_slot_detail_id=engagement.slot_detail_id,
                    exclude_engagement_id=int(engagement.engagement_id),
                )
        elif "consultation_mode" in update_fields and effective_mode == ConsultationMode.online:
            existing = await self.resolve_slot_detail(db, engagement)
            if isinstance(existing, dict) and existing.get("consultation"):
                slot_detail_data = dict(existing)
                slot_detail_data.pop("consultation", None)
                if not slot_detail_is_configured(slot_detail_data):
                    engagement.slot_detail_id = None
                else:
                    prepared = SlotDetail.model_validate(slot_detail_data)
                    engagement.slot_detail_id = await self._persist_slot_detail(
                        db,
                        organization_id=payload.organization_id,
                        city=payload.city,
                        start_date=payload.start_date,
                        end_date=payload.end_date,
                        slot_detail=prepared,
                        existing_slot_detail_id=engagement.slot_detail_id,
                        exclude_engagement_id=int(engagement.engagement_id),
                    )
        engagement.assessment_package_id = payload.assessment_package_id
        engagement.diagnostic_package_id = payload.diagnostic_package_id
        engagement.diagnostic_package_id_male = payload.diagnostic_package_id_male
        engagement.diagnostic_package_id_female = payload.diagnostic_package_id_female
        engagement.city = payload.city
        engagement.address = payload.address
        engagement.sub_locality = payload.sub_locality
        engagement.landmark = payload.landmark
        engagement.pincode = payload.pincode
        engagement.state = payload.state
        engagement.country = payload.country
        engagement.latitude = payload.latitude
        engagement.longitude = payload.longitude
        engagement.slot_duration = payload.slot_duration
        engagement.start_date = payload.start_date
        engagement.end_date = payload.end_date
        if "camp_no" in update_fields:
            engagement.camp_no = update_fields["camp_no"]
        else:
            engagement.camp_no = compute_camp_no(payload.organization_id, payload.start_date)
        engagement.healthians_zone_id = payload.healthians_zone_id
        engagement.external_camp_id = payload.external_camp_id
        engagement.blood_collection_type = payload.blood_collection_type
        engagement.metsights_engagement_id = payload.metsights_engagement_id
        engagement.create_profile_on_metsights = payload.create_profile_on_metsights
        engagement.enroll_for_fitprint_full = payload.enroll_for_fitprint_full
        engagement.load_prev_assessment_questionnaires = payload.load_prev_assessment_questionnaires
        engagement.load_prev_questionnaire_category_keys = _normalize_load_prev_category_keys(
            payload.load_prev_questionnaire_category_keys
        )
        engagement.payment_type = payload.payment_type
        engagement.auto_booking = _resolve_auto_booking(
            auto_booking=payload.auto_booking,
            blood_collection_type=payload.blood_collection_type,
        )

        engagement = await self._repository.update_engagement(db, engagement)

        if payload.notifications is not None:
            from modules.engagement_notifications.repository import EngagementNotificationsRepository
            from modules.engagement_notifications.service_config import serialize_for_db
            en_repo = EngagementNotificationsRepository()
            items = [
                {
                    "notification_event_id": n.notification_event_id,
                    "notification_services": serialize_for_db(n.notification_services),
                }
                for n in payload.notifications
            ]
            await en_repo.upsert_for_engagement(db, engagement.engagement_id, items)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_UPDATE_ENGAGEMENT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )

        if self._camp_onboarding_payload_service is not None and engagement.camp_no is not None:
            await self._camp_onboarding_payload_service.ensure_row_for_camp_no(
                db,
                camp_no=int(engagement.camp_no),
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
        ensure_admin(employee)

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
            user_id=None,
            session_id=None,
        )

        return engagement

    async def transition_engagement_statuses(
        self,
        db: AsyncSession,
        *,
        as_of: date | None = None,
        dry_run: bool = False,
        endpoint: str = "cron:transition-engagement-statuses",
        ip_address: str = "127.0.0.1",
        user_agent: str = "transition-engagement-statuses-job",
    ) -> dict[str, int | str | bool]:
        """Transition engagement statuses:
        - scheduled -> running when start_date <= as_of
        - running -> completed when end_date < as_of
        """
        effective_as_of = as_of or date.today()

        if dry_run:
            activated_count = await self._repository.count_scheduled_engagements_past_start_date(
                db,
                as_of=effective_as_of,
            )
            completed_count = await self._repository.count_running_engagements_past_end_date(
                db,
                as_of=effective_as_of,
            )
        else:
            activated_count = await self._repository.bulk_activate_scheduled_engagements(
                db,
                as_of=effective_as_of,
            )
            completed_count = await self._repository.bulk_complete_expired_engagements(
                db,
                as_of=effective_as_of,
            )
            if (activated_count or completed_count) and self._audit_service is not None:
                await self._audit_service.log_event(
                    db,
                    action="SYSTEM_TRANSITION_ENGAGEMENT_STATUSES",
                    endpoint=endpoint,
                    ip_address=ip_address,
                    user_agent=user_agent,
                    user_id=None,
                    session_id=None,
                )

        return {
            "as_of": effective_as_of.isoformat(),
            "activated_count": activated_count,
            "completed_count": completed_count,
            "dry_run": dry_run,
        }

    async def create_b2c_engagement(
        self,
        db: AsyncSession,
        *,
        user_first_name: str | None,
        engagement_date: date,
        city: str | None,
        assessment_package_id: int | None,
        diagnostic_package_id: int | None = None,
        engagement_type: int | None = None,
        blood_collection_type: BloodCollectionType | None = None,
        consultations: dict | None = None,
        address: str | None = None,
        sub_locality: str | None = None,
        landmark: str | None = None,
        pincode: str | None = None,
        state: str | None = None,
        country: str | None = None,
        latitude: float | None = None,
        longitude: float | None = None,
        create_profile_on_metsights: bool = False,
        enroll_for_fitprint_full: bool = False,
        load_prev_assessment_questionnaires: bool = False,
        load_prev_questionnaire_category_keys: list[str] | None = None,
        slot_duration: int = 20,
        consultation_mode: ConsultationMode | None = None,
        payment_type: EngagementPaymentType | None = None,
        auto_booking: bool = False,
    ) -> Engagement:
        """Create a B2C engagement and auto-assign default onboarding assistants from platform settings.

        B2C engagements are created automatically during public user onboarding.
        Default assistants come from platform_settings.default_onboarding_assistant_employee_ids.

        When location fields are incomplete and an address (or address parts) is
        available, missing fields are filled via the configured geocoding provider
        (with fallback). Geocode failures are ignored so engagement creation is
        never blocked.
        """
        from modules.geocoding.client import enrich_location_fields
        from modules.platform_settings.repository import PlatformSettingsRepository

        name_part = (user_first_name or "user").strip() or "user"
        engagement_name = f"{name_part}-{engagement_date.isoformat()}"

        primary = await PlatformSettingsRepository().resolve_geocoding_provider(db)
        location = await enrich_location_fields(
            address=address,
            sub_locality=sub_locality,
            landmark=landmark,
            city=city,
            pincode=pincode,
            state=state,
            country=country,
            latitude=latitude,
            longitude=longitude,
            primary=primary,
        )

        engagement = Engagement(
            engagement_name=engagement_name,
            metsights_engagement_id=None,
            organization_id=None,
            camp_no=None,
            engagement_code=_generate_engagement_code(),
            engagement_type=engagement_type,
            consultations=consultations,
            assessment_package_id=assessment_package_id,
            diagnostic_package_id=diagnostic_package_id,
            blood_collection_type=blood_collection_type,
            city=location.get("city"),
            address=location.get("address"),
            sub_locality=location.get("sub_locality"),
            landmark=location.get("landmark"),
            pincode=location.get("pincode"),
            state=location.get("state"),
            country=location.get("country"),
            latitude=location.get("latitude"),
            longitude=location.get("longitude"),
            slot_duration=slot_duration,
            start_date=engagement_date,
            end_date=engagement_date,
            status="running" if engagement_date <= date.today() else "scheduled",
            consultation_mode=(
                consultation_mode
                if consultation_mode is not None
                else (
                    ConsultationMode.online if _consultations_enabled(consultations) else None
                )
            ),
            create_profile_on_metsights=create_profile_on_metsights,
            enroll_for_fitprint_full=enroll_for_fitprint_full,
            load_prev_assessment_questionnaires=load_prev_assessment_questionnaires,
            load_prev_questionnaire_category_keys=_normalize_load_prev_category_keys(
                load_prev_questionnaire_category_keys
            ),
            payment_type=payment_type or EngagementPaymentType.paid_by_patient,
            auto_booking=_resolve_auto_booking(
                auto_booking=auto_booking,
                blood_collection_type=blood_collection_type,
            ),
        )
        engagement = await self._repository.create_engagement(db, engagement)

        from modules.engagement_notifications.repository import EngagementNotificationsRepository
        en_repo = EngagementNotificationsRepository()
        if engagement_type:
            await en_repo.populate_from_defaults(
                db, engagement_id=engagement.engagement_id, engagement_type_id=engagement_type
            )

        await self._assign_default_onboarding_assistants(
            db,
            engagement_id=engagement.engagement_id,
            organization_id=None,
        )

        return engagement

    async def apply_b2c_defaults_and_notify_after_booking(
        self,
        db: AsyncSession,
        *,
        engagement: Engagement,
        user: User,
        collection_date: date | None,
        collection_time: time | None = None,
    ) -> None:
        """After Healthians /book finalize: dates, platform notification defaults, default OAs, notify.

        start_date = blood collection date; end_date = start_date + 3 days.
        """
        if collection_date is not None:
            engagement.start_date = collection_date
            engagement.end_date = collection_date + timedelta(days=3)
            name_part = (user.first_name or "user").strip() or "user"
            current_name = (engagement.engagement_name or "").strip()
            if current_name.endswith("-draft"):
                engagement.engagement_name = f"{name_part}-{collection_date.isoformat()}"

        from modules.engagement_notifications.repository import EngagementNotificationsRepository
        en_repo = EngagementNotificationsRepository()
        if engagement.engagement_type:
            await en_repo.populate_from_defaults(
                db, engagement_id=engagement.engagement_id, engagement_type_id=engagement.engagement_type
            )

        await self._assign_default_onboarding_assistants(
            db,
            engagement_id=engagement.engagement_id,
            organization_id=None,
        )

        collection_date_str = collection_date.isoformat() if collection_date is not None else None
        collection_time_str = collection_time.isoformat() if collection_time is not None else None
        await self.notify_onboarding_assistants_after_enrollment(
            db,
            engagement=engagement,
            user=user,
            source=engagement.engagement_code or "book-finalize",
            collection_date=collection_date_str,
            collection_time=collection_time_str,
        )

    async def _resolve_default_onboarding_assistant_employee_ids(self, db: AsyncSession) -> list[int]:
        return await self._platform_settings_repository.resolve_default_onboarding_assistant_employee_ids(db)

    async def _assign_default_onboarding_assistants(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        organization_id: int | None,
    ) -> None:
        _ = organization_id
        employee_ids = await self._resolve_default_onboarding_assistant_employee_ids(db)
        if not employee_ids:
            return

        for employee_id in employee_ids:
            emp = await self._employee_repository.get_by_id(db, employee_id)
            if emp is None:
                logger.warning("Skipping default onboarding assistant %s: employee not found", employee_id)
                continue
            if (emp.status or "").lower() != "active":
                logger.warning("Skipping default onboarding assistant %s: employee not active", employee_id)
                continue
            role = emp.role if isinstance(emp.role, EmployeeRole) else EmployeeRole(str(emp.role))
            if role not in ONBOARDING_ASSISTANT_ASSIGNEE_EMPLOYEE_ROLES:
                logger.warning("Skipping default onboarding assistant %s: invalid role", employee_id)
                continue

            existing = await self._repository.get_onboarding_assistant_assignment(
                db,
                engagement_id=engagement_id,
                employee_id=employee_id,
            )
            if existing is not None:
                continue

            assignment = OnboardingAssistantAssignment(
                engagement_id=engagement_id,
                partner_id=None,
                employee_id=employee_id,
            )
            await self._repository.create_onboarding_assistant_assignment(db, assignment)

    async def list_onboarding_assistant_user_ids(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
    ) -> list[int]:
        """Deprecated: assistants are partners. Prefer list_onboarding_assistant_partner_contacts."""
        return await self._repository.list_onboarding_assistant_user_ids(
            db, engagement_id=engagement_id
        )

    async def list_onboarding_assistant_partner_contacts(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        roles: frozenset[str] | None = None,
    ) -> list[dict]:
        return await self._repository.list_onboarding_assistant_partner_contacts(
            db, engagement_id=engagement_id, roles=roles
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

    async def user_has_prior_engagement_participation(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        exclude_engagement_id: int,
    ) -> bool:
        return await self._repository.user_has_prior_engagement_participation(
            db,
            user_id=user_id,
            exclude_engagement_id=exclude_engagement_id,
        )

    async def enroll_user_in_engagement(
        self,
        db: AsyncSession,
        *,
        engagement: Engagement,
        user_id: int,
        engagement_date: date | None,
        slot_start_time: time | None,
        participants_employee_id: str | None = None,
        participant_department: str | None = None,
        participant_blood_group: str | None = None,
        blood_collection_cabin: str | None = None,
        consultations: dict | None = None,
        booked_by_user_id: int | None = None,
        paid: bool | None = None,
        booker_message: str | None = None,
        booker_relation: str | None = None,
        blood_collection_time_slot_id: str | None = None,
    ) -> EngagementParticipant:
        if (engagement.status or "").lower() not in ("scheduled", "running", "draft"):
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Engagement is not accepting enrollments")

        existing_participant = await self._repository.get_participant_for_user_engagement(
            db, user_id=user_id, engagement_id=int(engagement.engagement_id),
        )
        if existing_participant is not None:
            raise AppError(status_code=409, error_code="ALREADY_ENROLLED", message="User is already enrolled in this engagement")

        camp_no = engagement.camp_no
        if camp_no is not None:
            same_camp_participant = await self._repository.get_participant_for_user_camp_no(
                db,
                user_id=user_id,
                camp_no=int(camp_no),
                exclude_engagement_id=int(engagement.engagement_id),
            )
            if same_camp_participant is not None:
                raise AppError(
                    status_code=409,
                    error_code="ALREADY_ENROLLED_SAME_CAMP",
                    message="User is already enrolled in another engagement for this camp",
                )

        from modules.engagements.participant_eligibility import default_paid_for_enrollment

        booked_by = booked_by_user_id if booked_by_user_id is not None else user_id
        resolved_paid = paid if paid is not None else default_paid_for_enrollment(engagement)
        participant = EngagementParticipant(
            engagement_id=engagement.engagement_id,
            user_id=user_id,
            booked_by_user_id=booked_by,
            participants_employee_id=participants_employee_id,
            participant_department=participant_department,
            participant_blood_group=participant_blood_group,
            paid=resolved_paid,
            booker_message=(booker_message or "").strip() or None,
            booker_relation=(booker_relation or "").strip() or None,
        )
        created = await self._repository.create_participant(db, participant)
        cabin = (blood_collection_cabin or "").strip() or None
        slot_id = (blood_collection_time_slot_id or "").strip() or None
        if (
            engagement_date is not None
            or slot_start_time is not None
            or cabin is not None
            or slot_id is not None
        ):
            from modules.engagements.blood_bookings_access import apply_schedule

            await apply_schedule(
                db,
                created,
                engagement_date=engagement_date,
                slot_start_time=slot_start_time,
                blood_collection_cabin=cabin,
                blood_collection_time_slot_id=slot_id,
            )
        if consultations:
            await self._consultation_bookings.sync_from_want_map(db, created, consultations)
        return created

    async def get_participant_for_user_engagement(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
    ) -> EngagementParticipant | None:
        return await self._repository.get_participant_for_user_engagement(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
        )

    async def ensure_assessment_instances_for_participant(
        self,
        db: AsyncSession,
        *,
        engagement: Engagement,
        user_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> list[Any]:
        """Idempotently assign engagement assessment package(s) for a participant.

        Console enrollment / code-onboard can commit the participant row before Healthians
        booking succeeds. Journey pages read assessment_instances, so assignment must not
        wait on booking success.
        """
        if self._assessments_service is None:
            raise RuntimeError("Assessments service is required")

        participant = await self.get_participant_for_user_engagement(
            db,
            user_id=user_id,
            engagement_id=int(engagement.engagement_id),
        )
        if participant is not None:
            from modules.engagements.participant_eligibility import ensure_participant_paid

            ensure_participant_paid(participant)

        instances: list[Any] = []
        package_id = engagement.assessment_package_id
        if package_id is not None:
            instances.append(
                await self._assessments_service.ensure_instance_assigned(
                    db,
                    user_id=user_id,
                    engagement_id=int(engagement.engagement_id),
                    package_id=int(package_id),
                    ip_address=ip_address,
                    user_agent=user_agent,
                    endpoint=endpoint,
                )
            )

        if bool(engagement.enroll_for_fitprint_full):
            fitprint_package = await self._assessments_service.get_package_by_assessment_type_code(
                db,
                assessment_type_code="7",
            )
            if fitprint_package is not None:
                instances.append(
                    await self._assessments_service.ensure_instance_assigned(
                        db,
                        user_id=user_id,
                        engagement_id=int(engagement.engagement_id),
                        package_id=int(fitprint_package.package_id),
                        ip_address=ip_address,
                        user_agent=user_agent,
                        endpoint=endpoint,
                    )
                )
        return instances

    async def resolve_participant_department_for_engagement(
        self,
        db: AsyncSession,
        *,
        engagement: Engagement,
        participant_department: str | None,
    ) -> str | None:
        value = (participant_department or "").strip()
        if not value:
            return None
        if engagement.organization_id is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Department requires an organization engagement",
            )
        if self._organizations_repository is None:
            raise RuntimeError("Organizations repository is required")
        organization = await self._organizations_repository.get_by_id(db, engagement.organization_id)
        return validate_participant_department_for_organization(organization, value)

    async def update_participant_for_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_id: int,
        payload: EngagementParticipantUpdateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        ensure_admin(employee)

        updates = payload.model_dump(exclude_unset=True)
        if not updates:
            raise AppError(
                status_code=400,
                error_code="PARTICIPANT_UPDATE_EMPTY",
                message="At least one participant field must be provided",
            )

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

        response: dict = {"engagement_id": engagement_id, "user_id": user_id}

        if "participant_department" in updates:
            participant_department = updates["participant_department"]
            normalized: str | None
            if participant_department is None:
                normalized = None
            else:
                normalized = await self.resolve_participant_department_for_engagement(
                    db,
                    engagement=engagement,
                    participant_department=participant_department,
                )
            participant.participant_department = normalized
            response["participant_department"] = normalized

        if "consultations" in updates:
            normalized_consultations = normalize_consultations_map(updates["consultations"])
            await self._consultation_bookings.sync_from_want_map(db, participant, normalized_consultations)
            bookings = await self._consultation_bookings.get_for_participant(
                db,
                participant.engagement_participant_id,
            )
            response["consultations"] = bookings_to_consultations_map(bookings)

        from modules.engagements.blood_bookings_access import (
            apply_schedule,
            get_current_row,
            get_or_create_current,
            read_schedule_from_row,
        )

        if "booking_id" in updates:
            raw_booking_id = updates["booking_id"]
            if raw_booking_id is None:
                normalized_booking_id = None
            else:
                stripped = str(raw_booking_id).strip()
                normalized_booking_id = stripped or None
            row = await get_or_create_current(db, participant)
            await ensure_active_booking_id_available(
                db,
                booking_id=normalized_booking_id,
                exclude_pbb_id=int(row.id),
            )
            row.booking_id = normalized_booking_id
            db.add(row)
            try:
                await db.flush()
            except IntegrityError as exc:
                mapped = booking_id_integrity_app_error(exc)
                if mapped is not None:
                    raise mapped from exc
                raise
            response["booking_id"] = normalized_booking_id

        schedule_fields = {"engagement_date", "slot_start_time", "blood_collection_cabin"}
        if updates.keys() & schedule_fields:
            if _blood_collection_type_value(engagement.blood_collection_type) == BloodCollectionType.home_collection.value:
                raise AppError(
                    status_code=400,
                    error_code="SCHEDULE_UPDATE_NOT_ALLOWED",
                    message="Schedule fields cannot be updated for home collection engagements",
                )

            slot_detail = await self.resolve_slot_detail(db, engagement)
            if not slot_detail_is_configured(slot_detail):
                raise AppError(
                    status_code=400,
                    error_code="SCHEDULE_UPDATE_NOT_CONFIGURED",
                    message="Schedule editing requires slot_detail configuration",
                )

            sched = read_schedule_from_row(await get_current_row(db, participant))
            new_date = updates.get("engagement_date", sched["engagement_date"])
            raw_slot = updates.get("slot_start_time", sched["slot_start_time"])
            new_slot = coerce_time(raw_slot) if not isinstance(raw_slot, time) else raw_slot
            new_cabin_raw = updates.get("blood_collection_cabin", sched["blood_collection_cabin"])

            if new_date is None or new_slot is None:
                raise AppError(
                    status_code=400,
                    error_code="SCHEDULE_UPDATE_INCOMPLETE",
                    message="engagement_date and slot_start_time are required",
                )

            persisted_cabin = await self._validate_blood_collection_slot_capacity(
                db,
                engagement=engagement,
                collection_date=new_date,
                cabin_key=new_cabin_raw,
                slot_time=new_slot,
                exclude_engagement_participant_id=int(participant.engagement_participant_id),
            )

            normalized_slot = time(new_slot.hour, new_slot.minute)
            await apply_schedule(
                db,
                participant,
                engagement_date=new_date,
                slot_start_time=normalized_slot,
                blood_collection_cabin=persisted_cabin,
            )
            response["engagement_date"] = new_date.isoformat()
            response["slot_start_time"] = normalized_slot.isoformat()
            response["blood_collection_cabin"] = persisted_cabin

        await self._repository.update_participant(db, participant)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_UPDATE_PARTICIPANT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )

        return response

    async def _participant_for_employee_admin(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_id: int,
    ):
        ensure_admin(employee)
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
        return participant

    async def create_participant_blood_booking_for_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_id: int,
        payload,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        from modules.engagements.blood_bookings_repository import (
            BloodBookingsRepository,
            blood_bookings_to_api_list,
        )
        from modules.engagements.models import ParticipantBloodBooking

        participant = await self._participant_for_employee_admin(
            db, employee=employee, engagement_id=engagement_id, user_id=user_id
        )
        data = payload.model_dump()
        booking_id = (data.get("booking_id") or "").strip() or None
        await ensure_active_booking_id_available(db, booking_id=booking_id, exclude_pbb_id=None)
        row = ParticipantBloodBooking(
            engagement_participant_id=participant.engagement_participant_id,
            relation=data["relation"],
            status=data["status"],
            booking_id=booking_id,
            barcode=(data.get("barcode") or "").strip() or None,
            collection_date=data.get("collection_date"),
            collection_time=data.get("collection_time"),
            collection_cabin=(data.get("collection_cabin") or "").strip() or None,
            collection_time_slot_id=(data.get("collection_time_slot_id") or "").strip() or None,
            parent_booking_id=(data.get("parent_booking_id") or "").strip() or None,
            collected_at=data.get("collected_at"),
        )
        db.add(row)
        try:
            await db.flush()
        except IntegrityError as exc:
            mapped = booking_id_integrity_app_error(exc)
            if mapped is not None:
                raise mapped from exc
            raise
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_CREATE_BLOOD_BOOKING",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )
        return blood_bookings_to_api_list([row])[0]

    async def update_participant_blood_booking_for_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_id: int,
        blood_booking_id: int,
        payload,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        from modules.engagements.blood_bookings_repository import (
            BloodBookingsRepository,
            blood_bookings_to_api_list,
        )

        participant = await self._participant_for_employee_admin(
            db, employee=employee, engagement_id=engagement_id, user_id=user_id
        )
        repo = BloodBookingsRepository()
        row = await repo.get_by_id(db, blood_booking_id=blood_booking_id)
        if row is None or row.engagement_participant_id != participant.engagement_participant_id:
            raise AppError(status_code=404, error_code="BLOOD_BOOKING_NOT_FOUND", message="Collection not found")

        updates = payload.model_dump(exclude_unset=True)
        if "booking_id" in updates:
            bid = (updates["booking_id"] or "").strip() or None
            await ensure_active_booking_id_available(
                db,
                booking_id=bid,
                exclude_pbb_id=int(row.id),
            )
            row.booking_id = bid
        if "collected_at" in updates:
            row.collected_at = updates["collected_at"]
        for field in (
            "relation",
            "status",
            "barcode",
            "collection_date",
            "collection_time",
            "collection_cabin",
            "collection_time_slot_id",
            "parent_booking_id",
        ):
            if field in updates:
                val = updates[field]
                if field in ("barcode", "collection_cabin", "collection_time_slot_id", "parent_booking_id"):
                    val = (val or "").strip() or None if val is not None else None
                setattr(row, field, val)
        db.add(row)
        try:
            await db.flush()
        except IntegrityError as exc:
            mapped = booking_id_integrity_app_error(exc)
            if mapped is not None:
                raise mapped from exc
            raise
        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_UPDATE_BLOOD_BOOKING",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )
        return blood_bookings_to_api_list([row])[0]

    async def delete_participant_blood_booking_for_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_id: int,
        blood_booking_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        from modules.engagements.blood_booking_enums import BloodBookingStatus
        from modules.engagements.blood_bookings_repository import BloodBookingsRepository

        participant = await self._participant_for_employee_admin(
            db, employee=employee, engagement_id=engagement_id, user_id=user_id
        )
        repo = BloodBookingsRepository()
        row = await repo.get_by_id(db, blood_booking_id=blood_booking_id)
        if row is None or row.engagement_participant_id != participant.engagement_participant_id:
            raise AppError(status_code=404, error_code="BLOOD_BOOKING_NOT_FOUND", message="Collection not found")

        if row.blood_parameters is not None or (row.diagnostic_report_url or "").strip():
            row.status = BloodBookingStatus.cancelled.value
            row.booking_id = None
            db.add(row)
            await db.flush()
            action = "cancelled"
        else:
            await db.delete(row)
            await db.flush()
            action = "deleted"

        await self._require_audit_service().log_event(
            db,
            action="EMPLOYEE_DELETE_BLOOD_BOOKING",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )
        return {"blood_booking_id": blood_booking_id, "result": action}

    async def reschedule_blood_collection_for_user(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
        payload: EngagementRescheduleRequest,
    ) -> dict[str, Any]:
        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")

        participant = await self._repository.get_participant_for_user_engagement(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
        )
        if participant is None:
            raise AppError(
                status_code=403,
                error_code="ACCESS_DENIED",
                message="You are not a participant in this engagement",
            )

        if _blood_collection_type_value(engagement.blood_collection_type) == BloodCollectionType.home_collection.value:
            raise AppError(
                status_code=400,
                error_code="SCHEDULE_UPDATE_NOT_ALLOWED",
                message="Schedule fields cannot be updated for home collection engagements",
            )

        from modules.engagements.blood_bookings_access import apply_schedule, get_current_row, read_schedule_from_row

        sched = read_schedule_from_row(await get_current_row(db, participant))
        ensure_schedule_change_allowed(
            sched["engagement_date"],
            sched["slot_start_time"],
            message=MSG_BLOOD_COLLECTION_RESCHEDULE,
        )

        slot_time = coerce_time(payload.blood_collection_time_slot)
        if slot_time is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="blood_collection_time_slot must be a valid HH:MM time",
            )
        normalized_slot = time(slot_time.hour, slot_time.minute)
        collection_date = payload.blood_collection_date
        cabin_key = (payload.blood_collection_cabin or "").strip() or None

        if cabin_key is not None:
            slot_detail = await self.resolve_slot_detail(db, engagement)
            if slot_detail_is_configured(slot_detail):
                cabin_key = await self._validate_blood_collection_slot_capacity(
                    db,
                    engagement=engagement,
                    collection_date=collection_date,
                    cabin_key=cabin_key,
                    slot_time=normalized_slot,
                    exclude_engagement_participant_id=int(participant.engagement_participant_id),
                )
            await apply_schedule(
                db,
                participant,
                engagement_date=collection_date,
                slot_start_time=normalized_slot,
                blood_collection_cabin=cabin_key,
            )
        else:
            if engagement.start_date is None or engagement.end_date is None:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message="Engagement start_date and end_date are required for date-only reschedule",
                )
            if not (engagement.start_date <= collection_date <= engagement.end_date):
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message="blood_collection_date must be within the engagement start_date and end_date",
                )
            await apply_schedule(
                db,
                participant,
                engagement_date=collection_date,
                slot_start_time=normalized_slot,
            )

        await self._repository.update_participant(db, participant)
        final_sched = read_schedule_from_row(await get_current_row(db, participant))

        return {
            "engagement_id": engagement_id,
            "user_id": user_id,
            "engagement_date": collection_date.isoformat(),
            "slot_start_time": normalized_slot.isoformat(),
            "blood_collection_cabin": final_sched["blood_collection_cabin"],
        }

    async def update_consultation_consent_for_user(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
        consultation_id: int,
        payload: ConsultationConsentRequest,
    ) -> dict[str, Any]:
        participant = await self._repository.get_participant_for_user_engagement(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
        )
        if participant is None:
            raise AppError(
                status_code=403,
                error_code="ACCESS_DENIED",
                message="You are not a participant in this engagement",
            )

        booking_ids = participant.consultation_booking_ids or []
        if consultation_id not in booking_ids:
            raise AppError(
                status_code=404,
                error_code="NOT_FOUND",
                message="Consultation booking not found for this participant",
            )

        booking = await self._consultation_bookings.get_by_id(db, consultation_id)
        if booking is None or booking.engagement_participant_id != participant.engagement_participant_id:
            raise AppError(
                status_code=404,
                error_code="NOT_FOUND",
                message="Consultation booking not found for this participant",
            )

        merged = empty_consent()
        merged.update(normalize_consent(booking.consent))
        updates = payload.model_dump(exclude_unset=True)
        merged.update({key: bool(value) for key, value in updates.items()})
        booking.consent = merged
        await db.flush()

        return {
            "consultation_id": booking.consultation_id,
            "engagement_id": engagement_id,
            "consent": merged,
        }

    async def get_consultations_for_user(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
    ) -> dict[str, Any]:
        participant = await self._repository.get_participant_for_user_engagement(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
        )
        if participant is None:
            raise AppError(
                status_code=403,
                error_code="ACCESS_DENIED",
                message="You are not a participant in this engagement",
            )

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        bookings = await self._consultation_bookings.get_for_participant(
            db,
            participant.engagement_participant_id,
        )
        my_consultations: list[dict[str, Any]] = []
        for booking in bookings:
            date_val = booking.consultation_date.isoformat() if booking.consultation_date else None
            attachments = booking.attachments
            if attachments is not None and not isinstance(attachments, list):
                attachments = list(attachments)
            my_consultations.append(
                {
                    "consultation_id": booking.consultation_id,
                    "expert_type": booking.expert_type,
                    "want": bool(booking.want),
                    "expert_id": booking.expert_id,
                    "date": date_val,
                    "cabin": booking.consultation_cabin,
                    "slot": booking.consultation_slot,
                    "done": bool(booking.done),
                    "consultation_summary": booking.consultation_summary,
                    "attachments": attachments,
                }
            )

        engagement_consultations = engagement.consultations
        if not isinstance(engagement_consultations, dict):
            engagement_consultations = {}

        from modules.reports.blood_booking_reports import get_current_report_root
        from modules.reports.blood_report_archival import is_persistable_blood_report_url
        from modules.reports.models import IndividualHealthReport

        blood_root = await get_current_report_root(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
        )
        blood_report_generated = bool(
            blood_root
            and is_persistable_blood_report_url((blood_root.diagnostic_report_url or "").strip())
        )

        ihr_result = await db.execute(
            select(IndividualHealthReport.report_url).where(
                IndividualHealthReport.user_id == user_id,
                IndividualHealthReport.engagement_id == engagement_id,
            )
        )
        bio_ai_generated = any(bool((url or "").strip()) for (url,) in ihr_result.all())

        return {
            "engagement_id": engagement_id,
            "engagement_code": engagement.engagement_code,
            "engagement_status": engagement.status,
            "consultation_mode": effective_consultation_mode(engagement).value,
            "my_consultations": my_consultations,
            "consultations": engagement_consultations,
            "blood_report_generated": blood_report_generated,
            "bio_ai_generated": bio_ai_generated,
        }

    async def update_participant_department_for_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_id: int,
        participant_department: str | None,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        return await self.update_participant_for_employee(
            db,
            employee=employee,
            engagement_id=engagement_id,
            user_id=user_id,
            payload=EngagementParticipantUpdateRequest(participant_department=participant_department),
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
        )

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
        ensure_admin(employee)

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

        result = await self._participant_rows_to_dicts(db, participants, engagement=engagement)
        return result, total

    async def _resolve_participant_list_filters(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        filters: ParticipantListFilters,
    ) -> ParticipantListFilters:
        if filters.booking_date is None:
            return filters

        audit = self._require_audit_service()
        booking_dates = await audit.list_create_booking_dates_for_engagement(
            db,
            engagement_id=engagement_id,
        )
        user_ids = {
            int(user_id)
            for user_id in booking_dates.get("user_ids_by_date", {}).get(
                filters.booking_date.isoformat(),
                [],
            )
        }
        return ParticipantListFilters(
            search=filters.search,
            engagement_date=filters.engagement_date,
            booking_date=filters.booking_date,
            department=filters.department,
            has_booking_id=filters.has_booking_id,
            reports_ready=filters.reports_ready,
            booking_date_user_ids=user_ids,
            consultation_filters=filters.consultation_filters,
        )

    async def _ensure_engagement_exists(self, db: AsyncSession, engagement_id: int) -> Engagement:
        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )
        return engagement

    async def list_participants_for_engagement_id(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        page: int,
        limit: int,
        filters: ParticipantListFilters | None = None,
    ) -> tuple[list[dict], int]:
        """Fetch participant enrollment rows for a specific engagement by id."""

        ensure_admin(employee)
        await self._ensure_engagement_exists(db, engagement_id)

        resolved_filters = (
            await self._resolve_participant_list_filters(
                db,
                engagement_id=engagement_id,
                filters=filters,
            )
            if filters is not None
            else None
        )

        participants = await self._repository.list_participants_by_engagement_id(
            db,
            engagement_id=engagement_id,
            page=page,
            limit=limit,
            filters=resolved_filters,
        )
        total = await self._repository.count_distinct_participants_for_engagement(
            db,
            engagement_id=engagement_id,
            filters=resolved_filters,
        )

        engagement = await self._ensure_engagement_exists(db, engagement_id)
        result = await self._participant_rows_to_dicts(db, participants, engagement=engagement)
        return result, total

    async def attach_roster_progress_to_participants(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        participants: list[dict[str, Any]],
        include_questionnaire_maps: bool = True,
    ) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any] | None]:
        """Merge data-completeness and questionnaire category status into participant rows."""
        from modules.engagements.participant_report_flags import (
            default_participant_report_flags,
            engagement_report_flags_by_user,
            merge_report_flags_into_participant_row,
        )

        summary_payload = await self._build_engagement_data_completeness_payload(
            db,
            engagement_id=engagement_id,
            include_participants=False,
        )
        summary = summary_payload.get("summary") or {}

        if not participants:
            columns = (
                await self._resolve_engagement_questionnaire_column_defs(
                    db, engagement_id=engagement_id
                )
                if include_questionnaire_maps
                else None
            )
            return participants, summary, columns

        questionnaire_columns: dict[str, Any] | None = None
        by_user_q: dict[int, dict[str, Any]] = {}

        if include_questionnaire_maps:
            completeness = await self._build_engagement_data_completeness_payload(
                db,
                engagement_id=engagement_id,
                include_participants=True,
            )
            summary = completeness.get("summary") or summary
            q_status = await self._build_engagement_questionnaire_status(
                db, engagement_id=engagement_id
            )
            questionnaire_columns = q_status.get("questionnaire_columns")
            by_user_complete = {
                int(row["user_id"]): row for row in completeness.get("participants") or []
            }
            by_user_q = {int(row["user_id"]): row for row in q_status.get("participants") or []}
        else:
            flags_by_user = await engagement_report_flags_by_user(db, engagement_id=engagement_id)
            by_user_complete = flags_by_user

        default_flags = default_participant_report_flags()
        enriched: list[dict[str, Any]] = []
        for participant in participants:
            row = dict(participant)
            uid = int(row["user_id"])
            complete = by_user_complete.get(uid)
            if include_questionnaire_maps and complete:
                row.update(
                    {
                        "has_booking_id": complete.get("has_booking_id"),
                        "has_blood_report": complete.get("has_blood_report"),
                        "has_blood_values": complete.get("has_blood_values"),
                        "has_bio_ai_report": complete.get("has_bio_ai_report"),
                        "has_bio_ai_values": complete.get("has_bio_ai_values"),
                        "has_bio_ai_json": complete.get("has_bio_ai_json"),
                        "has_fitprint_report": complete.get("has_fitprint_report"),
                        "has_fitprint_values": complete.get("has_fitprint_values"),
                        "blood_report_url": complete.get("blood_report_url"),
                        "bio_ai_report_url": complete.get("bio_ai_report_url"),
                        "fitprint_report_url": complete.get("fitprint_report_url"),
                        "questionnaire_state": complete.get("questionnaire_state"),
                    }
                )
            elif not include_questionnaire_maps:
                merge_report_flags_into_participant_row(
                    row, complete if complete is not None else default_flags
                )
            q_row = by_user_q.get(uid)
            if q_row:
                nested = q_row.get("questionnaire_categories")
                if nested:
                    row["questionnaire_categories"] = nested
                else:
                    row["questionnaire_categories"] = {
                        "metsights": q_row.get("categories") or {},
                        "supershyft": {},
                    }
                if not row.get("questionnaire_state"):
                    row["questionnaire_state"] = q_row.get("questionnaire_state")
            enriched.append(row)

        return enriched, summary, questionnaire_columns

    async def get_participant_clinical_snapshot_for_engagement(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_id: int,
    ) -> dict[str, Any]:
        """Blood, Bio AI, and questionnaire status for one enrolled participant."""
        ensure_admin(employee)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        ep = await self._repository.get_participant_for_user_engagement(
            db, engagement_id=engagement_id, user_id=user_id
        )
        if ep is None:
            raise AppError(
                status_code=404,
                error_code="PARTICIPANT_NOT_FOUND",
                message="Participant is not enrolled in this engagement",
            )

        from modules.engagements.blood_bookings_repository import (
            BloodBookingsRepository,
            blood_bookings_to_api_list,
        )

        completeness = await self._build_engagement_data_completeness_payload(
            db,
            engagement_id=engagement_id,
            include_participants=True,
        )
        complete_row = next(
            (row for row in completeness.get("participants") or [] if int(row["user_id"]) == user_id),
            None,
        )
        q_status = await self._build_engagement_questionnaire_status(db, engagement_id=engagement_id)
        q_row = next(
            (row for row in q_status.get("participants") or [] if int(row["user_id"]) == user_id),
            None,
        )
        blood_repo = BloodBookingsRepository()
        blood_rows = await blood_repo.list_for_participant(
            db, engagement_participant_id=int(ep.engagement_participant_id)
        )
        blood_bookings = blood_bookings_to_api_list(blood_rows)

        questionnaire_categories = {"metsights": {}, "supershyft": {}}
        questionnaire_state = "not_started"
        if q_row:
            questionnaire_state = q_row.get("questionnaire_state") or questionnaire_state
            nested = q_row.get("questionnaire_categories")
            if nested:
                questionnaire_categories = nested
            else:
                questionnaire_categories = {
                    "metsights": q_row.get("categories") or {},
                    "supershyft": {},
                }

        blood_flags = complete_row or {}
        blood_report_url = blood_flags.get("blood_report_url")
        if not (blood_report_url and str(blood_report_url).strip()):
            for bb in blood_bookings:
                candidate = str(bb.get("diagnostic_report_url") or "").strip()
                if candidate:
                    blood_report_url = candidate
                    break

        from modules.engagements.participant_report_flags import engagement_user_scoped_report_payloads

        bio_ai_reports, fitprint_reports = await engagement_user_scoped_report_payloads(
            db,
            engagement_id=engagement_id,
            user_id=user_id,
        )

        from modules.reports.blood_parameters_schemas import (
            has_usable_provider_blood_parameters,
            is_empty_blood_parameters,
        )

        blood_report_raw: dict | list | None = None
        for bb_row in blood_rows:
            raw = bb_row.blood_report_raw
            if not is_empty_blood_parameters(raw):
                blood_report_raw = raw if isinstance(raw, (dict, list)) else None
                break

        blood_parameters_payload: dict | list | None = None
        for bb_row in blood_rows:
            params = bb_row.blood_parameters
            if has_usable_provider_blood_parameters(params):
                blood_parameters_payload = params if isinstance(params, (dict, list)) else None
                break

        return {
            "engagement_id": engagement_id,
            "user_id": user_id,
            "blood": {
                "has_blood_report": bool(blood_flags.get("has_blood_report")),
                "has_blood_report_raw": bool(blood_flags.get("has_blood_report_raw")),
                "has_blood_values": bool(blood_flags.get("has_blood_values")),
                "blood_report_url": blood_report_url,
                "blood_report_raw": blood_report_raw,
                "blood_parameters": blood_parameters_payload,
                "blood_bookings": blood_bookings,
            },
            "bio_ai": {
                "has_bio_ai_report": bool(blood_flags.get("has_bio_ai_report")),
                "has_bio_ai_values": bool(blood_flags.get("has_bio_ai_values")),
                "has_bio_ai_json": bool(blood_flags.get("has_bio_ai_json")),
                "bio_ai_report_url": blood_flags.get("bio_ai_report_url"),
                "reports": bio_ai_reports,
            },
            "fitprint": {
                "has_fitprint_report": bool(blood_flags.get("has_fitprint_report")),
                "has_fitprint_values": bool(blood_flags.get("has_fitprint_values")),
                "fitprint_report_url": blood_flags.get("fitprint_report_url"),
                "reports": fitprint_reports,
            },
            "questionnaires": {
                "questionnaire_state": questionnaire_state,
                "categories": questionnaire_categories,
            },
            "questionnaire_columns": q_status.get("questionnaire_columns"),
        }

    async def clear_participant_clinical_data_for_engagement(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_id: int,
        product: str,
        field: str,
    ) -> dict[str, Any]:
        """Admin: null report URL or values for blood / Bio AI / FitPrint."""
        ensure_admin(employee)

        ep = await self._repository.get_participant_for_user_engagement(
            db, engagement_id=engagement_id, user_id=user_id
        )
        if ep is None:
            raise AppError(
                status_code=404,
                error_code="PARTICIPANT_NOT_FOUND",
                message="Participant is not enrolled in this engagement",
            )

        from modules.engagements.clinical_data_clear import clear_participant_clinical_data

        updated = await clear_participant_clinical_data(
            db,
            engagement_id=engagement_id,
            engagement_participant_id=int(ep.engagement_participant_id),
            user_id=user_id,
            product=product,  # type: ignore[arg-type]
            field=field,  # type: ignore[arg-type]
        )
        await db.commit()

        snapshot = await self.get_participant_clinical_snapshot_for_engagement(
            db,
            employee=employee,
            engagement_id=engagement_id,
            user_id=user_id,
        )
        snapshot["cleared_rows"] = updated
        return snapshot

    async def regenerate_participant_bio_ai_report_for_engagement(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_id: int,
    ) -> dict[str, Any]:
        """Admin: refresh MetSights JSON and regenerate Bio AI PDF at the same slug."""
        ensure_admin(employee)

        ep = await self._repository.get_participant_for_user_engagement(
            db, engagement_id=engagement_id, user_id=user_id
        )
        if ep is None:
            raise AppError(
                status_code=404,
                error_code="PARTICIPANT_NOT_FOUND",
                message="Participant is not enrolled in this engagement",
            )
        if self._metsights_service is None:
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message="Metsights integration is not configured",
            )

        from modules.engagements.regenerate_participant_bio_ai import (
            regenerate_participant_bio_ai_report,
        )

        regen_result = await regenerate_participant_bio_ai_report(
            db,
            metsights_service=self._metsights_service,
            engagement_id=engagement_id,
            user_id=user_id,
        )
        await db.commit()

        snapshot = await self.get_participant_clinical_snapshot_for_engagement(
            db,
            employee=employee,
            engagement_id=engagement_id,
            user_id=user_id,
        )
        snapshot["regenerate"] = regen_result
        return snapshot

    async def participant_stats_for_engagement_id(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        filters: ParticipantListFilters | None = None,
    ) -> dict[str, Any]:
        """Return blood-test completion counts using live Healthians lookups."""

        from modules.engagements.blood_test_complete_resolver import resolve_blood_test_complete_flags

        ensure_admin(employee)
        await self._ensure_engagement_exists(db, engagement_id)

        resolved_filters = (
            await self._resolve_participant_list_filters(
                db,
                engagement_id=engagement_id,
                filters=filters,
            )
            if filters is not None
            else ParticipantListFilters()
        )

        filtered_rows = await self._repository.list_all_participants_by_engagement_id(
            db,
            engagement_id=engagement_id,
            filters=resolved_filters,
        )
        filtered_flags = await resolve_blood_test_complete_flags(db, filtered_rows)
        filtered_complete = sum(1 for ready in filtered_flags.values() if ready)
        not_ready_names = [
            " ".join(part for part in (row[3], row[4]) if part).strip() or "—"
            for row in filtered_rows
            if not filtered_flags.get((int(row[2]), int(row[1])), False)
        ]

        overall_total = len(filtered_rows)
        overall_complete = filtered_complete
        if filters is not None and filters_are_active(resolved_filters):
            overall_rows = await self._repository.list_all_participants_by_engagement_id(
                db,
                engagement_id=engagement_id,
                filters=None,
            )
            overall_flags = await resolve_blood_test_complete_flags(db, overall_rows)
            overall_total = len(overall_rows)
            overall_complete = sum(1 for ready in overall_flags.values() if ready)

        return {
            "filtered_total": len(filtered_rows),
            "filtered_blood_test_complete": filtered_complete,
            "overall_total": overall_total,
            "overall_blood_test_complete": overall_complete,
            "not_ready_names": not_ready_names[:100],
        }

    async def participant_user_ids_for_engagement_id(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        filters: ParticipantListFilters | None = None,
    ) -> dict[str, Any]:
        """Return user_ids for all participants matching list filters (no pagination)."""

        ensure_admin(employee)
        await self._ensure_engagement_exists(db, engagement_id)

        resolved_filters = (
            await self._resolve_participant_list_filters(
                db,
                engagement_id=engagement_id,
                filters=filters,
            )
            if filters is not None
            else None
        )

        user_ids = await self._repository.list_participant_user_ids_by_engagement_id(
            db,
            engagement_id=engagement_id,
            filters=resolved_filters,
        )
        return {"user_ids": user_ids, "total": len(user_ids)}

    async def participant_filter_options_for_engagement_id(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
    ) -> dict[str, Any]:
        ensure_admin(employee)
        await self._ensure_engagement_exists(db, engagement_id)
        engagement_dates = await self._repository.list_distinct_engagement_dates_for_engagement(
            db,
            engagement_id=engagement_id,
        )
        return {"engagement_dates": engagement_dates}

    async def load_blood_reports_for_participants(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_ids: list[int],
        sync_service: "MetsightsSyncService",
    ) -> dict[str, Any]:
        """Run load_blood_reports for selected participants without sending notifications."""

        from modules.notifications.load_blood_reports import load_blood_reports

        ensure_admin(employee)
        await self._ensure_engagement_exists(db, engagement_id)

        unique_user_ids = list(dict.fromkeys(int(user_id) for user_id in user_ids))
        enrolled = await self._repository.get_participants_map_for_engagement(
            db,
            engagement_id=engagement_id,
            user_ids=unique_user_ids,
        )
        missing = [user_id for user_id in unique_user_ids if user_id not in enrolled]
        if missing:
            raise AppError(
                status_code=422,
                error_code="INVALID_INPUT",
                message=(
                    "These participants are not in the engagement: "
                    + ", ".join(str(user_id) for user_id in missing)
                ),
            )

        if self._metsights_service is None or self._assessments_service is None:
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message="Metsights integration is not configured",
            )

        notifications_service = self._notifications_service
        if notifications_service is None:
            from modules.notifications.dependencies import get_notifications_service

            notifications_service = get_notifications_service()

        return await load_blood_reports(
            db,
            metsights_service=self._metsights_service,
            notifications_service=notifications_service,
            assessments_service=self._assessments_service,
            sync_service=sync_service,
            engagement_id=engagement_id,
            user_ids=set(unique_user_ids),
            send_notifications=False,
            all_engagements=True,
            ignore_engagement_date=True,
        )

    async def reload_provider_blood_parameters_for_participants(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_ids: list[int],
        reports_service: Any,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict[str, Any]:
        """Re-fetch Healthians digital values and persist grouped blood_parameters (reload=1)."""

        from modules.reports.blood_booking_reports import get_current_report_root, merged_blood_parameters_blob
        from modules.reports.blood_parameters_schemas import has_usable_provider_blood_parameters

        ensure_admin(employee)
        await self._ensure_engagement_exists(db, engagement_id)

        unique_user_ids = list(dict.fromkeys(int(user_id) for user_id in user_ids))
        enrolled = await self._repository.get_participants_map_for_engagement(
            db,
            engagement_id=engagement_id,
            user_ids=unique_user_ids,
        )
        missing = [user_id for user_id in unique_user_ids if user_id not in enrolled]
        if missing:
            raise AppError(
                status_code=422,
                error_code="INVALID_INPUT",
                message=(
                    "These participants are not in the engagement: "
                    + ", ".join(str(user_id) for user_id in missing)
                ),
            )

        reloaded = 0
        skipped = 0
        failed = 0
        details: list[dict[str, Any]] = []

        for user_id in unique_user_ids:
            instances = await self._assessments_repository.list_instances_for_user_engagement(
                db,
                user_id=user_id,
                engagement_id=engagement_id,
            )
            if not instances:
                skipped += 1
                details.append(
                    {
                        "user_id": user_id,
                        "engagement_id": engagement_id,
                        "action": "skipped",
                        "reason": "no assessment instance for engagement",
                    }
                )
                continue

            assessment_instance = instances[0]
            for inst in instances:
                if (inst.metsights_record_id or "").strip():
                    assessment_instance = inst
                    break

            record_id = (assessment_instance.metsights_record_id or "").strip()
            if not record_id:
                skipped += 1
                details.append(
                    {
                        "user_id": user_id,
                        "engagement_id": engagement_id,
                        "action": "skipped",
                        "reason": "assessment has no Metsights record id",
                    }
                )
                continue

            user = await self._users_repository.get_user_by_id(db, user_id)
            if user is None:
                skipped += 1
                details.append(
                    {
                        "user_id": user_id,
                        "engagement_id": engagement_id,
                        "action": "skipped",
                        "reason": "user not found",
                    }
                )
                continue

            first_name = getattr(user, "first_name", "") or ""
            last_name = getattr(user, "last_name", "") or ""
            gender = getattr(user, "gender", None)

            try:
                await reports_service.get_blood_parameters_for_user(
                    db,
                    assessment_id=int(assessment_instance.assessment_instance_id),
                    user_id=user_id,
                    user_gender=gender,
                    user_first_name=first_name,
                    user_last_name=last_name,
                    load_from="provider",
                    reload=1,
                    ip_address=ip_address,
                    user_agent=user_agent,
                    endpoint=endpoint,
                )
                await db.flush()
                blood_root = await get_current_report_root(
                    db,
                    user_id=user_id,
                    engagement_id=engagement_id,
                )
                if has_usable_provider_blood_parameters(merged_blood_parameters_blob(blood_root)):
                    reloaded += 1
                    details.append(
                        {
                            "user_id": user_id,
                            "engagement_id": engagement_id,
                            "action": "reloaded",
                            "reason": "provider blood parameters refreshed",
                        }
                    )
                else:
                    skipped += 1
                    details.append(
                        {
                            "user_id": user_id,
                            "engagement_id": engagement_id,
                            "action": "skipped",
                            "reason": "provider blood data unavailable after reload",
                        }
                    )
            except AppError as exc:
                if exc.status_code in {404, 422}:
                    skipped += 1
                    action = "skipped"
                else:
                    failed += 1
                    action = "failed"
                details.append(
                    {
                        "user_id": user_id,
                        "engagement_id": engagement_id,
                        "action": action,
                        "reason": (exc.message or exc.error_code)[:200],
                    }
                )
            except Exception as exc:
                failed += 1
                details.append(
                    {
                        "user_id": user_id,
                        "engagement_id": engagement_id,
                        "action": "failed",
                        "reason": str(exc)[:200],
                    }
                )

        return {
            "engagement_id": engagement_id,
            "reloaded": reloaded,
            "skipped": skipped,
            "failed": failed,
            "details": details,
        }

    async def load_bioai_reports_for_participants(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_ids: list[int],
        sync_service: "MetsightsSyncService",
    ) -> dict[str, Any]:
        """Run load_bioai_reports for selected participants without sending notifications."""

        from modules.notifications.load_bioai_reports import load_bioai_reports

        ensure_admin(employee)
        await self._ensure_engagement_exists(db, engagement_id)

        unique_user_ids = list(dict.fromkeys(int(user_id) for user_id in user_ids))
        enrolled = await self._repository.get_participants_map_for_engagement(
            db,
            engagement_id=engagement_id,
            user_ids=unique_user_ids,
        )
        missing = [user_id for user_id in unique_user_ids if user_id not in enrolled]
        if missing:
            raise AppError(
                status_code=422,
                error_code="INVALID_INPUT",
                message=(
                    "These participants are not in the engagement: "
                    + ", ".join(str(user_id) for user_id in missing)
                ),
            )

        if self._metsights_service is None or self._assessments_service is None:
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message="Metsights integration is not configured",
            )

        notifications_service = self._notifications_service
        if notifications_service is None:
            from modules.notifications.dependencies import get_notifications_service

            notifications_service = get_notifications_service()

        return await load_bioai_reports(
            db,
            metsights_service=self._metsights_service,
            notifications_service=notifications_service,
            assessments_service=self._assessments_service,
            sync_service=sync_service,
            engagement_id=engagement_id,
            user_ids=set(unique_user_ids),
            send_notifications=False,
            all_engagements=True,
            ignore_engagement_date=True,
        )

    async def remove_reports_for_participants(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        user_ids: list[int],
    ) -> dict[str, Any]:
        """Hard-delete all individual_health_report rows for selected participants in an engagement."""

        ensure_admin(employee)
        await self._ensure_engagement_exists(db, engagement_id)

        unique_user_ids = list(dict.fromkeys(int(user_id) for user_id in user_ids))
        enrolled = await self._repository.get_participants_map_for_engagement(
            db,
            engagement_id=engagement_id,
            user_ids=unique_user_ids,
        )
        missing = [user_id for user_id in unique_user_ids if user_id not in enrolled]
        if missing:
            raise AppError(
                status_code=422,
                error_code="INVALID_INPUT",
                message=(
                    "These participants are not in the engagement: "
                    + ", ".join(str(user_id) for user_id in missing)
                ),
            )

        from modules.reports.blood_booking_reports import clear_blood_report_data_for_participant

        total_removed = 0
        users_with_no_reports = 0
        for user_id in unique_user_ids:
            deleted = await self._reports_repository.delete_individual_reports_for_engagement(
                db,
                user_id=user_id,
                engagement_id=engagement_id,
            )
            total_removed += deleted
            participant = enrolled.get(user_id)
            if participant is not None:
                await clear_blood_report_data_for_participant(
                    db,
                    engagement_participant_id=participant.engagement_participant_id,
                )
            if deleted == 0:
                users_with_no_reports += 1

        return {
            "removed": total_removed,
            "users_processed": len(unique_user_ids),
            "users_with_no_reports": users_with_no_reports,
        }

    async def list_booking_dates_for_engagement_id(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
    ) -> dict[str, Any]:
        """Distinct IST booking dates from successful createBooking_v3 sync logs."""

        ensure_admin(employee)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        audit = self._require_audit_service()
        return await audit.list_create_booking_dates_for_engagement(
            db,
            engagement_id=engagement_id,
        )

    async def booking_summary_for_engagement_id(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
    ) -> dict[str, Any]:
        """Camp EOD booking counts for one engagement (no double-count of superseded/resample)."""

        ensure_admin(employee)
        await self._ensure_engagement_exists(db, engagement_id)

        from modules.engagements.blood_bookings_repository import BloodBookingsRepository

        today = _ist_today()
        return await BloodBookingsRepository().booking_summary_for_engagement(
            db,
            engagement_id=engagement_id,
            today=today,
        )

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
        ensure_admin(employee)

        # Fetch participants with pagination
        participants = await self._repository.list_participants_for_b2c_engagements(
            db,
            page=page,
            limit=limit,
        )

        # Count total participant enrollment rows
        total = await self._repository.count_participants_for_b2c_engagements(db)

        result = await self._participant_rows_to_dicts(db, participants)
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
        await self._detach_assessment_instance_references(db, assessment_instance_ids=[instance_id])
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
        from modules.engagements.models import EngagementNotification
        from modules.notifications.models import Notification

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

        notification_result = await db.execute(
            delete(EngagementNotification).where(EngagementNotification.engagement_id == engagement_id)
        )
        totals["deleted_engagement_notifications"] = int(notification_result.rowcount or 0)

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

        ensure_admin(employee)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")

        camp_no = engagement.camp_no

        purge = await self._purge_engagement_scoped_data(db, engagement_id=engagement_id)

        deleted = await self._repository.delete_engagement_by_id(db, engagement_id=engagement_id)
        if not deleted:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")

        from modules.reports.camp_reports_repository import CampReportsRepository

        camp_reports_repo = CampReportsRepository()
        deleted_camp_reports = 0
        if camp_no is not None:
            remaining = await camp_reports_repo.count_engagements_for_camp_no(db, camp_no=camp_no)
            if remaining == 0:
                deleted_camp_reports = await camp_reports_repo.delete_all_for_camp_no(db, camp_no=camp_no)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_DELETE_ENGAGEMENT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )

        return {
            "engagement_id": engagement_id,
            "engagement_code": engagement.engagement_code,
            "engagement_name": engagement.engagement_name,
            "deleted_camp_reports": deleted_camp_reports,
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
        ensure_admin(employee)

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

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_DELETE_ENGAGEMENT_PARTICIPANT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )

        return {
            "engagement_id": engagement_id,
            "user_id": user_id,
            **purge,
        }

    async def move_participants_to_engagement_for_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        source_engagement_id: int,
        user_ids: list[int],
        target_engagement_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        ensure_admin(employee)

        normalized_user_ids = sorted({int(user_id) for user_id in user_ids})
        if not normalized_user_ids:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=_cannot_move_participant_message("Select at least one participant."),
            )

        if int(source_engagement_id) == int(target_engagement_id):
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=_cannot_move_participant_message(
                    "Source and target engagement are the same."
                ),
            )

        source = await self._repository.get_engagement_by_id(db, source_engagement_id)
        if source is None:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")

        target = await self._repository.get_engagement_by_id(db, target_engagement_id)
        if target is None:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")

        mismatch_reasons = _participant_move_mismatch_reasons(source, target)
        if mismatch_reasons:
            raise AppError(
                status_code=400,
                error_code="INCOMPATIBLE_ENGAGEMENT",
                message=_cannot_move_participant_message(
                    "These settings do not match: " + ", ".join(mismatch_reasons) + "."
                ),
            )

        missing_on_source: list[int] = []
        already_on_target: list[int] = []
        already_on_same_camp: list[int] = []
        target_camp_no = target.camp_no
        for user_id in normalized_user_ids:
            on_source = await self._repository.has_participant_for_user_engagement(
                db,
                user_id=user_id,
                engagement_id=source_engagement_id,
            )
            if not on_source:
                missing_on_source.append(user_id)
                continue
            on_target = await self._repository.has_participant_for_user_engagement(
                db,
                user_id=user_id,
                engagement_id=target_engagement_id,
            )
            if on_target:
                already_on_target.append(user_id)
                continue
            if target_camp_no is not None:
                same_camp = await self._repository.get_participant_for_user_camp_no(
                    db,
                    user_id=user_id,
                    camp_no=int(target_camp_no),
                    exclude_engagement_id=int(source_engagement_id),
                )
                if same_camp is not None:
                    already_on_same_camp.append(user_id)

        if missing_on_source:
            raise AppError(
                status_code=404,
                error_code="PARTICIPANT_NOT_FOUND",
                message=_cannot_move_participant_message(
                    "These participants are not in the source engagement: "
                    + ", ".join(str(uid) for uid in missing_on_source)
                    + "."
                ),
            )
        if already_on_target:
            raise AppError(
                status_code=409,
                error_code="ALREADY_ENROLLED",
                message=_cannot_move_participant_message(
                    "These participants are already enrolled in the target engagement: "
                    + ", ".join(str(uid) for uid in already_on_target)
                    + "."
                ),
            )
        if already_on_same_camp:
            raise AppError(
                status_code=409,
                error_code="ALREADY_ENROLLED_SAME_CAMP",
                message=_cannot_move_participant_message(
                    "These participants are already enrolled in another engagement for this camp: "
                    + ", ".join(str(uid) for uid in already_on_same_camp)
                    + "."
                ),
            )

        from sqlalchemy import cast, or_, update

        from modules.assessments.models import AssessmentInstance
        from modules.audit.models import IntegrationSyncLog
        from modules.notifications.models import Notification
        from modules.reports.models import IndividualHealthReport

        # Update only engagement_id via SQL so we do not SELECT columns that may
        # not exist yet on older DBs (e.g. home-collection address fields).
        move_result = await db.execute(
            update(EngagementParticipant)
            .where(EngagementParticipant.user_id.in_(normalized_user_ids))
            .where(EngagementParticipant.engagement_id == source_engagement_id)
            .values(engagement_id=target_engagement_id)
        )
        if int(move_result.rowcount or 0) < len(normalized_user_ids):
            raise AppError(status_code=404, error_code="PARTICIPANT_NOT_FOUND", message="Participant does not exist")

        await db.execute(
            update(AssessmentInstance)
            .where(AssessmentInstance.user_id.in_(normalized_user_ids))
            .where(AssessmentInstance.engagement_id == source_engagement_id)
            .values(engagement_id=target_engagement_id)
        )
        await db.execute(
            update(IndividualHealthReport)
            .where(IndividualHealthReport.user_id.in_(normalized_user_ids))
            .where(IndividualHealthReport.engagement_id == source_engagement_id)
            .values(engagement_id=target_engagement_id)
        )
        await db.execute(
            update(IntegrationSyncLog)
            .where(IntegrationSyncLog.user_id.in_(normalized_user_ids))
            .where(IntegrationSyncLog.engagement_id == source_engagement_id)
            .values(engagement_id=target_engagement_id)
        )

        from sqlalchemy.dialects.postgresql import JSONB

        notification_user_filters = [
            cast(Notification.user, JSONB).contains({"user_ids": [user_id]})
            for user_id in normalized_user_ids
        ]
        await db.execute(
            update(Notification)
            .where(Notification.engagement_id == source_engagement_id)
            .where(
                or_(
                    Notification.triggered_by_user_id.in_(normalized_user_ids),
                    *notification_user_filters,
                )
            )
            .values(engagement_id=target_engagement_id)
        )

        remaining = await self._repository.count_distinct_participants_for_engagement(
            db,
            engagement_id=source_engagement_id,
        )

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_MOVE_ENGAGEMENT_PARTICIPANTS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )

        return {
            "source_engagement_id": source_engagement_id,
            "target_engagement_id": target_engagement_id,
            "user_ids": normalized_user_ids,
            "moved_count": len(normalized_user_ids),
            "source_remaining_participant_count": int(remaining),
        }

    async def move_participant_to_engagement_for_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        source_engagement_id: int,
        user_id: int,
        target_engagement_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        return await self.move_participants_to_engagement_for_employee(
            db,
            employee=employee,
            source_engagement_id=source_engagement_id,
            user_ids=[user_id],
            target_engagement_id=target_engagement_id,
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
        )

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

        ensure_admin(employee)

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

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_DELETE_ALL_ENGAGEMENT_PARTICIPANTS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
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

        Summary cards: filled / partially_filled / not_started (sum = total enrolled).
        Per-participant: 4 metsights category columns with status/is_submitted/has_responses/assigned.
        """
        ensure_admin(employee)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        return await self._build_engagement_questionnaire_status(db, engagement_id=engagement_id)

    async def get_data_completeness_for_engagement(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
    ) -> dict:
        """Return blood/Bio AI/questionnaire completeness for enrolled participants."""
        ensure_admin(employee)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        return await self._build_engagement_data_completeness_payload(
            db,
            engagement_id=engagement_id,
            include_participants=True,
        )

    async def get_engagement_by_code_for_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_code: str,
    ) -> Engagement:
        ensure_admin(employee)
        code = (engagement_code or "").strip()
        if not code:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Engagement code is required")
        engagement = await self._repository.get_engagement_by_code(db, code)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )
        return engagement

    async def list_dashboard_participant_issues_block(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        limit: int = 20,
    ) -> tuple[list[dict], dict | None]:
        """Fast dashboard rollup + sample issues for running/scheduled engagements."""
        from modules.engagements.blood_bookings_repository import current_blood_booking_subquery
        from modules.engagements.models import EngagementParticipant
        from modules.reports.blood_parameters_schemas import has_usable_provider_blood_parameters
        from modules.reports.blood_report_archival import is_persistable_blood_report_url
        from modules.reports.camp_reports_repository import _coerce_reports_dict
        from modules.reports.models import IndividualHealthReport
        from modules.users.models import User

        ensure_admin(employee)
        if limit < 1 or limit > 500:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        engagements = await self._repository.list_engagements_for_filters(
            db,
            limit=limit,
            organization_id=None,
            camp_no=None,
            statuses=["running", "scheduled"],
            city=None,
            on_date=None,
            search=None,
            engagement_type=None,
            audience=None,
            sort_by=None,
            sort_dir=None,
        )
        if not engagements:
            return [], {
                "participants_in_scope": 0,
                "engagements_in_scope": 0,
                "limit_reached": False,
                "missing_blood_slot": 0,
                "missing_questionnaire": 0,
                "missing_blood_report": 0,
                "missing_bio_ai_report": 0,
            }

        engagement_ids = [int(e.engagement_id) for e in engagements]
        curr_pbb = current_blood_booking_subquery()

        enrolled_rows = (
            await db.execute(
                select(
                    EngagementParticipant.engagement_id,
                    User.user_id,
                    User.first_name,
                    User.last_name,
                    curr_pbb.c.booking_id,
                )
                .select_from(EngagementParticipant)
                .join(User, User.user_id == EngagementParticipant.user_id)
                .outerjoin(
                    curr_pbb,
                    curr_pbb.c.engagement_participant_id
                    == EngagementParticipant.engagement_participant_id,
                )
                .where(EngagementParticipant.engagement_id.in_(engagement_ids))
                .order_by(EngagementParticipant.engagement_id.asc(), User.user_id.asc())
            )
        ).all()

        from modules.assessments.models import AssessmentInstance, AssessmentPackage
        from modules.engagements.participant_report_flags import _apply_ihr_row

        ihr_flags: dict[tuple[int, int], dict[str, bool]] = {}
        ihr_result = await db.execute(
            select(
                IndividualHealthReport.engagement_id,
                IndividualHealthReport.user_id,
                IndividualHealthReport.report_url,
                IndividualHealthReport.reports,
                AssessmentPackage.assessment_type_code,
            )
            .select_from(IndividualHealthReport)
            .outerjoin(
                AssessmentInstance,
                AssessmentInstance.assessment_instance_id
                == IndividualHealthReport.assessment_instance_id,
            )
            .outerjoin(
                AssessmentPackage,
                AssessmentPackage.package_id == AssessmentInstance.package_id,
            )
            .where(IndividualHealthReport.engagement_id.in_(engagement_ids))
        )
        for row in ihr_result.all():
            key = (int(row.engagement_id), int(row.user_id))
            flags = ihr_flags.setdefault(
                key,
                {
                    "has_blood_report": False,
                    "has_blood_values": False,
                    "has_bio_ai_report": False,
                    "has_bio_ai_json": False,
                },
            )
            _apply_ihr_row(
                flags,
                report_url=row.report_url,
                reports=row.reports,
                assessment_type_code=row.assessment_type_code,
            )

        blood_result = await db.execute(
            select(
                EngagementParticipant.engagement_id,
                EngagementParticipant.user_id,
                curr_pbb.c.diagnostic_report_url,
                curr_pbb.c.blood_parameters,
            )
            .select_from(EngagementParticipant)
            .join(
                curr_pbb,
                curr_pbb.c.engagement_participant_id
                == EngagementParticipant.engagement_participant_id,
            )
            .where(EngagementParticipant.engagement_id.in_(engagement_ids))
        )
        for row in blood_result.all():
            key = (int(row.engagement_id), int(row.user_id))
            flags = ihr_flags.setdefault(
                key,
                {
                    "has_blood_report": False,
                    "has_blood_values": False,
                    "has_bio_ai_report": False,
                    "has_bio_ai_json": False,
                },
            )
            if row.diagnostic_report_url and is_persistable_blood_report_url(
                str(row.diagnostic_report_url).strip()
            ):
                flags["has_blood_report"] = True
            if has_usable_provider_blood_parameters(row.blood_parameters):
                flags["has_blood_values"] = True

        default_flags = {
            "has_blood_report": False,
            "has_blood_values": False,
            "has_bio_ai_report": False,
            "has_bio_ai_json": False,
        }

        q_state_by_engagement_user: dict[tuple[int, int], str] = {}
        for engagement in engagements:
            q_status = await self._build_engagement_questionnaire_status(
                db, engagement_id=int(engagement.engagement_id)
            )
            for participant in q_status["participants"]:
                q_state_by_engagement_user[
                    (int(engagement.engagement_id), int(participant["user_id"]))
                ] = participant["questionnaire_state"]

        rollup = {
            "total_participants": 0,
            "with_booking_id": 0,
            "blood_report": 0,
            "bio_ai_report": 0,
            "questionnaire_not_started": 0,
            "questionnaire_partially_filled": 0,
        }
        participant_issues: list[dict] = []
        engagement_by_id = {int(e.engagement_id): e for e in engagements}
        seen: set[tuple[int, int]] = set()

        for row in enrolled_rows:
            engagement_id = int(row.engagement_id)
            user_id = int(row.user_id)
            key = (engagement_id, user_id)
            if key in seen:
                continue
            seen.add(key)

            flags = ihr_flags.get(key, default_flags)
            q_state = q_state_by_engagement_user.get(key, "not_started")
            has_booking_id = bool(row.booking_id and str(row.booking_id).strip())
            engagement = engagement_by_id[engagement_id]

            participant = {
                "user_id": user_id,
                "first_name": row.first_name,
                "last_name": row.last_name,
                "has_booking_id": has_booking_id,
                "has_blood_report": flags["has_blood_report"],
                "has_bio_ai_report": flags["has_bio_ai_report"],
                "questionnaire_state": q_state,
            }
            participant_issues.extend(_participant_issues_from_completeness(engagement, [participant]))

            rollup["total_participants"] += 1
            if has_booking_id:
                rollup["with_booking_id"] += 1
            if flags["has_blood_report"]:
                rollup["blood_report"] += 1
            if flags["has_bio_ai_report"]:
                rollup["bio_ai_report"] += 1
            if q_state == "partially_filled":
                rollup["questionnaire_partially_filled"] += 1
            elif q_state != "filled":
                rollup["questionnaire_not_started"] += 1

        participants_in_scope = rollup["total_participants"]
        engagements_in_scope = len(engagements)
        questionnaire_missing = rollup["questionnaire_not_started"] + rollup["questionnaire_partially_filled"]
        summary = {
            "participants_in_scope": participants_in_scope,
            "engagements_in_scope": engagements_in_scope,
            "limit_reached": engagements_in_scope >= limit,
            "missing_blood_slot": max(0, participants_in_scope - rollup["with_booking_id"]),
            "missing_questionnaire": questionnaire_missing,
            "missing_blood_report": max(0, participants_in_scope - rollup["blood_report"]),
            "missing_bio_ai_report": max(0, participants_in_scope - rollup["bio_ai_report"]),
        }
        return participant_issues, summary

    async def list_engagements_data_completeness_summary(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        organization_id: int | None,
        camp_no: int | None,
        status: str | None,
        city: str | None,
        on_date,
        search: str | None = None,
        engagement_type: str | None = None,
        audience: str | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
        limit: int = 100,
        include_participant_issues: bool = False,
    ) -> dict:
        """Return rollup + per-engagement completeness summaries for filtered engagements.

        When ``include_participant_issues`` is true, also return a flat
        ``participant_issues`` list for missing blood slots, questionnaires,
        blood reports, and Bio-AI reports.
        """
        ensure_admin(employee)

        if limit < 1 or limit > 500:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        status_values = _parse_status_filter(status)

        audience_value = None
        if audience is not None:
            normalized_audience = audience.strip().lower()
            if normalized_audience not in {"b2b", "b2c"}:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            audience_value = normalized_audience

        engagements = await self._repository.list_engagements_for_filters(
            db,
            limit=limit,
            organization_id=organization_id,
            camp_no=camp_no,
            statuses=status_values,
            city=city,
            on_date=on_date,
            search=search,
            engagement_type=engagement_type,
            audience=audience_value,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )

        rollup = {
            "engagement_count": 0,
            "total_participants": 0,
            "with_booking_id": 0,
            "blood_report": 0,
            "blood_values": 0,
            "bio_ai_report": 0,
            "bio_ai_json": 0,
            "bio_ai_values": 0,
            "fitprint_report": 0,
            "fitprint_values": 0,
            "questionnaire_filled": 0,
            "questionnaire_partially_filled": 0,
            "questionnaire_not_started": 0,
        }
        engagement_rows: list[dict] = []
        participant_issues: list[dict] = []

        if include_participant_issues:
            payloads = await asyncio.gather(
                *[
                    self._build_engagement_data_completeness_payload(
                        db,
                        engagement_id=int(engagement.engagement_id),
                        include_participants=True,
                    )
                    for engagement in engagements
                ]
            )
            for engagement, payload in zip(engagements, payloads, strict=True):
                summary = payload["summary"]
                participant_issues.extend(
                    _participant_issues_from_completeness(engagement, payload.get("participants") or [])
                )
                engagement_rows.append(
                    {
                        "engagement_id": int(engagement.engagement_id),
                        "engagement_name": engagement.engagement_name,
                        "engagement_code": engagement.engagement_code,
                        "city": engagement.city,
                        "status": engagement.status,
                        "organization_id": engagement.organization_id,
                        "summary": summary,
                    }
                )
                rollup["total_participants"] += summary["total_participants"]
                rollup["with_booking_id"] += summary["with_booking_id"]
                rollup["blood_report"] += summary["blood_report"]
                rollup["blood_values"] += summary["blood_values"]
                rollup["bio_ai_report"] += summary["bio_ai_report"]
                rollup["bio_ai_json"] += summary["bio_ai_json"]
                rollup["bio_ai_values"] += summary.get("bio_ai_values", summary["bio_ai_json"])
                rollup["fitprint_report"] += summary.get("fitprint_report", 0)
                rollup["fitprint_values"] += summary.get("fitprint_values", 0)
                rollup["questionnaire_filled"] += summary["questionnaire_filled"]
                rollup["questionnaire_partially_filled"] += summary["questionnaire_partially_filled"]
                rollup["questionnaire_not_started"] += summary["questionnaire_not_started"]
        else:
            summaries = await asyncio.gather(
                *[
                    self._summarize_engagement_data_completeness(
                        db,
                        engagement_id=int(engagement.engagement_id),
                    )
                    for engagement in engagements
                ]
            )
            for engagement, summary in zip(engagements, summaries, strict=True):
                engagement_rows.append(
                    {
                        "engagement_id": int(engagement.engagement_id),
                        "engagement_name": engagement.engagement_name,
                        "engagement_code": engagement.engagement_code,
                        "city": engagement.city,
                        "status": engagement.status,
                        "organization_id": engagement.organization_id,
                        "summary": summary,
                    }
                )
                rollup["total_participants"] += summary["total_participants"]
                rollup["with_booking_id"] += summary["with_booking_id"]
                rollup["blood_report"] += summary["blood_report"]
                rollup["blood_values"] += summary["blood_values"]
                rollup["bio_ai_report"] += summary["bio_ai_report"]
                rollup["bio_ai_json"] += summary["bio_ai_json"]
                rollup["bio_ai_values"] += summary.get("bio_ai_values", summary["bio_ai_json"])
                rollup["fitprint_report"] += summary.get("fitprint_report", 0)
                rollup["fitprint_values"] += summary.get("fitprint_values", 0)
                rollup["questionnaire_filled"] += summary["questionnaire_filled"]
                rollup["questionnaire_partially_filled"] += summary["questionnaire_partially_filled"]
                rollup["questionnaire_not_started"] += summary["questionnaire_not_started"]

        rollup["engagement_count"] = len(engagement_rows)
        result: dict = {"rollup": rollup, "engagements": engagement_rows}
        if include_participant_issues:
            result["participant_issues"] = participant_issues
        return result

    async def _summarize_engagement_data_completeness(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
    ) -> dict:
        payload = await self._build_engagement_data_completeness_payload(
            db,
            engagement_id=engagement_id,
            include_participants=False,
        )
        return payload["summary"]

    async def _build_engagement_data_completeness_payload(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        include_participants: bool,
    ) -> dict:
        from modules.engagements.blood_bookings_repository import current_blood_booking_subquery
        from modules.engagements.models import EngagementParticipant
        from modules.users.models import User

        curr_pbb = current_blood_booking_subquery()

        enrolled_query = (
            select(
                User.user_id,
                User.first_name,
                User.last_name,
                User.phone,
                User.email,
                curr_pbb.c.booking_id,
            )
            .select_from(EngagementParticipant)
            .join(User, User.user_id == EngagementParticipant.user_id)
            .outerjoin(
                curr_pbb,
                curr_pbb.c.engagement_participant_id
                == EngagementParticipant.engagement_participant_id,
            )
            .where(EngagementParticipant.engagement_id == engagement_id)
            .order_by(User.first_name.asc(), User.last_name.asc(), User.user_id.asc())
        )
        enrolled_result = await db.execute(enrolled_query)
        enrolled_rows = enrolled_result.all()

        q_status = await self._build_engagement_questionnaire_status(db, engagement_id=engagement_id)
        q_state_by_user: dict[int, str] = {
            int(participant["user_id"]): participant["questionnaire_state"]
            for participant in q_status["participants"]
        }

        from modules.engagements.participant_report_flags import (
            default_participant_report_flags,
            engagement_report_flags_by_user,
        )

        ihr_flags = await engagement_report_flags_by_user(db, engagement_id=engagement_id)
        default_flags = default_participant_report_flags()

        participants: list[dict] = []
        summary = {
            "total_participants": 0,
            "with_booking_id": 0,
            "blood_report": 0,
            "blood_values": 0,
            "bio_ai_report": 0,
            "bio_ai_json": 0,
            "bio_ai_values": 0,
            "fitprint_report": 0,
            "fitprint_values": 0,
            "questionnaire_filled": 0,
            "questionnaire_partially_filled": 0,
            "questionnaire_not_started": 0,
        }

        seen_users: set[int] = set()
        for row in enrolled_rows:
            uid = int(row.user_id)
            if uid in seen_users:
                continue
            seen_users.add(uid)

            flags = ihr_flags.get(uid, default_flags)
            q_state = q_state_by_user.get(uid, "not_started")
            has_booking_id = bool(row.booking_id and str(row.booking_id).strip())

            if include_participants:
                participants.append(
                    {
                        "user_id": uid,
                        "first_name": row.first_name,
                        "last_name": row.last_name,
                        "phone": mask_phone(row.phone),
                        "email": mask_email(row.email),
                        "has_booking_id": has_booking_id,
                        "has_blood_report": flags["has_blood_report"],
                        "has_blood_report_raw": flags["has_blood_report_raw"],
                        "has_blood_values": flags["has_blood_values"],
                        "has_bio_ai_report": flags["has_bio_ai_report"],
                        "has_bio_ai_values": flags["has_bio_ai_values"],
                        "has_bio_ai_json": flags["has_bio_ai_json"],
                        "has_fitprint_report": flags["has_fitprint_report"],
                        "has_fitprint_values": flags["has_fitprint_values"],
                        "blood_report_url": flags.get("blood_report_url"),
                        "bio_ai_report_url": flags.get("bio_ai_report_url"),
                        "fitprint_report_url": flags.get("fitprint_report_url"),
                        "questionnaire_state": q_state,
                    }
                )

            summary["total_participants"] += 1
            if has_booking_id:
                summary["with_booking_id"] += 1
            if flags["has_blood_report"]:
                summary["blood_report"] += 1
            if flags["has_blood_values"]:
                summary["blood_values"] += 1
            if flags["has_bio_ai_report"]:
                summary["bio_ai_report"] += 1
            if flags["has_bio_ai_values"]:
                summary["bio_ai_values"] += 1
                summary["bio_ai_json"] += 1
            if flags["has_fitprint_report"]:
                summary["fitprint_report"] += 1
            if flags["has_fitprint_values"]:
                summary["fitprint_values"] += 1
            if q_state == "filled":
                summary["questionnaire_filled"] += 1
            elif q_state == "partially_filled":
                summary["questionnaire_partially_filled"] += 1
            else:
                summary["questionnaire_not_started"] += 1

        if include_participants:
            return {"summary": summary, "participants": participants}
        return {"summary": summary}

    async def _resolve_engagement_questionnaire_column_defs(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
    ) -> dict[str, list[dict[str, str]]]:
        """Assigned questionnaire categories for roster columns (MetSights + SuperShyft)."""
        from modules.assessments.models import AssessmentPackageCategory
        from modules.questionnaire.models import QuestionnaireCategory
        from sqlalchemy import select

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            return {"metsights": [], "supershyft": []}

        package_ids: list[int] = []
        if engagement.assessment_package_id is not None:
            package_ids.append(int(engagement.assessment_package_id))
        if bool(engagement.enroll_for_fitprint_full) and self._assessments_service is not None:
            fitprint = await self._assessments_service.get_package_by_assessment_type_code(
                db, assessment_type_code="7"
            )
            if fitprint is not None:
                package_ids.append(int(fitprint.package_id))
        package_ids = list(dict.fromkeys(package_ids))
        if not package_ids:
            return {"metsights": [], "supershyft": []}

        result = await db.execute(
            select(
                AssessmentPackageCategory.package_id,
                AssessmentPackageCategory.display_order,
                AssessmentPackageCategory.id,
                QuestionnaireCategory.category_key,
                QuestionnaireCategory.category_of,
                QuestionnaireCategory.display_name,
            )
            .join(
                QuestionnaireCategory,
                QuestionnaireCategory.category_id == AssessmentPackageCategory.category_id,
            )
            .where(AssessmentPackageCategory.package_id.in_(package_ids))
            .where(QuestionnaireCategory.category_of.in_(("metsights", "supershyft")))
            .order_by(
                AssessmentPackageCategory.package_id.asc(),
                AssessmentPackageCategory.display_order.asc().nulls_last(),
                AssessmentPackageCategory.id.asc(),
            )
        )
        seen_m: set[str] = set()
        seen_s: set[str] = set()
        metsights: list[dict[str, str]] = []
        supershyft: list[dict[str, str]] = []
        for row in result.all():
            key = (row.category_key or "").strip()
            if not key:
                continue
            cat_of = (row.category_of or "supershyft").strip().lower()
            label = (row.display_name or key).strip()
            col = {"key": key, "label": label, "category_of": cat_of}
            if cat_of == "metsights":
                if key in seen_m:
                    continue
                seen_m.add(key)
                metsights.append(col)
            else:
                if key in seen_s:
                    continue
                seen_s.add(key)
                supershyft.append(col)
        return {"metsights": metsights, "supershyft": supershyft}

    async def _build_engagement_questionnaire_status(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
    ) -> dict:
        """Build questionnaire status payload shared by admin completeness endpoints."""
        from modules.assessments.models import (
            AssessmentCategoryProgress,
            AssessmentInstance,
            AssessmentPackage,
            AssessmentPackageCategory,
        )
        from modules.questionnaire.models import QuestionnaireCategory, QuestionnaireResponse
        from modules.users.models import User
        from sqlalchemy import select, func

        questionnaire_columns = await self._resolve_engagement_questionnaire_column_defs(
            db, engagement_id=engagement_id
        )
        column_keys_metsights = [c["key"] for c in questionnaire_columns.get("metsights") or []]
        column_keys_supershyft = [c["key"] for c in questionnaire_columns.get("supershyft") or []]
        all_category_keys = list(dict.fromkeys(column_keys_metsights + column_keys_supershyft))

        instances_query = (
            select(
                AssessmentInstance.assessment_instance_id,
                AssessmentInstance.user_id,
                AssessmentInstance.status,
                AssessmentInstance.package_id,
                User.first_name,
                User.last_name,
                User.phone,
                User.email,
            )
            .join(User, User.user_id == AssessmentInstance.user_id)
            .where(AssessmentInstance.engagement_id == engagement_id)
            .order_by(User.first_name.asc(), User.last_name.asc(), AssessmentInstance.assessment_instance_id.asc())
        )
        result = await db.execute(instances_query)
        instance_rows = result.all()

        if not instance_rows:
            return {
                "summary": {"filled": 0, "partially_filled": 0, "not_started": 0},
                "participants": [],
                "questionnaire_columns": questionnaire_columns,
            }

        instance_ids = [row.assessment_instance_id for row in instance_rows]

        # Get all category progress rows for these instances
        progress_query = (
            select(
                AssessmentCategoryProgress.assessment_instance_id,
                AssessmentCategoryProgress.category_id,
                AssessmentCategoryProgress.status,
                AssessmentCategoryProgress.is_submitted,
                QuestionnaireCategory.category_key,
                QuestionnaireCategory.category_of,
            )
            .join(QuestionnaireCategory, QuestionnaireCategory.category_id == AssessmentCategoryProgress.category_id)
            .where(AssessmentCategoryProgress.assessment_instance_id.in_(instance_ids))
        )
        progress_result = await db.execute(progress_query)
        progress_rows = progress_result.all()

        # Build progress map: instance_id -> {category_key -> {status, is_submitted}}
        progress_map: dict[int, dict[str, dict]] = {}
        for p in progress_rows:
            iid = int(p.assessment_instance_id)
            ck = p.category_key
            if ck is None:
                continue
            progress_map.setdefault(iid, {})[ck] = {
                "status": p.status,
                "is_submitted": bool(p.is_submitted),
            }

        # Count responses per instance per category for has_responses
        resp_count_query = (
            select(
                QuestionnaireResponse.assessment_instance_id,
                func.count(QuestionnaireResponse.response_id),
            )
            .where(QuestionnaireResponse.assessment_instance_id.in_(instance_ids))
            .group_by(QuestionnaireResponse.assessment_instance_id)
        )
        resp_result = await db.execute(resp_count_query)
        resp_counts: dict[int, int] = {int(r[0]): int(r[1]) for r in resp_result.all()}

        # Determine which metsights categories each package has
        package_ids = list({int(r.package_id) for r in instance_rows})
        pkg_cat_query = (
            select(
                AssessmentPackageCategory.package_id,
                QuestionnaireCategory.category_key,
                QuestionnaireCategory.category_of,
            )
            .join(QuestionnaireCategory, QuestionnaireCategory.category_id == AssessmentPackageCategory.category_id)
            .where(AssessmentPackageCategory.package_id.in_(package_ids))
            .where(QuestionnaireCategory.category_of.in_(("metsights", "supershyft")))
        )
        pkg_cat_result = await db.execute(pkg_cat_query)
        pkg_categories: dict[int, set[tuple[str, str]]] = {}
        for pc in pkg_cat_result.all():
            key = (pc.category_key or "").strip()
            cat_of = (pc.category_of or "supershyft").strip().lower()
            if key:
                pkg_categories.setdefault(int(pc.package_id), set()).add((key, cat_of))

        pkg_type_query = select(
            AssessmentPackage.package_id,
            AssessmentPackage.assessment_type_code,
            AssessmentPackage.package_code,
        ).where(AssessmentPackage.package_id.in_(package_ids))
        pkg_type_rows = await db.execute(pkg_type_query)
        pkg_essentials: dict[int, bool] = {}
        for prow in pkg_type_rows.all():
            pkg_essentials[int(prow.package_id)] = is_metsights_essentials(
                type_code=prow.assessment_type_code,
                package_code=prow.package_code,
            )

        # Also get per-category response counts using category_ids array overlap
        cat_resp_map: dict[int, dict[str, bool]] = {}  # instance_id -> category_key -> has_responses
        for iid in instance_ids:
            cat_resp_map[int(iid)] = {}

        cat_key_to_id: dict[tuple[str, str], int] = {}
        if not all_category_keys:
            return {
                "summary": {"filled": 0, "partially_filled": 0, "not_started": 0},
                "participants": [],
                "questionnaire_columns": questionnaire_columns,
            }

        cat_rows = await db.execute(
            select(
                QuestionnaireCategory.category_id,
                QuestionnaireCategory.category_key,
                QuestionnaireCategory.category_of,
            )
            .where(QuestionnaireCategory.category_key.in_(all_category_keys))
            .where(QuestionnaireCategory.category_of.in_(("metsights", "supershyft")))
        )
        for row in cat_rows.all():
            key = (row.category_key or "").strip()
            cat_of = (row.category_of or "supershyft").strip().lower()
            if key:
                cat_key_to_id[(key, cat_of)] = int(row.category_id)

        # Check if responses exist per category per instance
        for (ck, _cat_of), cid in cat_key_to_id.items():
            has_resp_query = (
                select(
                    QuestionnaireResponse.assessment_instance_id,
                    func.count(QuestionnaireResponse.response_id),
                )
                .where(QuestionnaireResponse.assessment_instance_id.in_(instance_ids))
                .where(QuestionnaireResponse.category_ids.any(cid))
                .group_by(QuestionnaireResponse.assessment_instance_id)
            )
            has_resp_result = await db.execute(has_resp_query)
            for r in has_resp_result.all():
                cat_resp_map.setdefault(int(r[0]), {})[ck] = int(r[1]) > 0

        # Unanswered required questions per instance/category (for partial hover tooltips)
        from modules.questionnaire.models import QuestionnaireCategoryQuestion, QuestionnaireDefinition

        unanswered_map: dict[int, dict[str, list[dict]]] = {}
        for (ck, _cat_of), cid in cat_key_to_id.items():
            unanswered_query = (
                select(
                    AssessmentInstance.assessment_instance_id,
                    QuestionnaireDefinition.question_id,
                    QuestionnaireDefinition.question_text,
                )
                .select_from(AssessmentInstance)
                .join(
                    AssessmentPackageCategory,
                    AssessmentPackageCategory.package_id == AssessmentInstance.package_id,
                )
                .join(
                    QuestionnaireCategoryQuestion,
                    QuestionnaireCategoryQuestion.category_id == AssessmentPackageCategory.category_id,
                )
                .join(
                    QuestionnaireDefinition,
                    QuestionnaireDefinition.question_id == QuestionnaireCategoryQuestion.question_id,
                )
                .outerjoin(
                    QuestionnaireResponse,
                    (QuestionnaireResponse.assessment_instance_id == AssessmentInstance.assessment_instance_id)
                    & (QuestionnaireResponse.question_id == QuestionnaireDefinition.question_id)
                    & (QuestionnaireResponse.category_ids.any(cid)),
                )
                .where(AssessmentInstance.assessment_instance_id.in_(instance_ids))
                .where(AssessmentPackageCategory.category_id == cid)
                .where(QuestionnaireDefinition.is_required.is_(True))
                .where(QuestionnaireResponse.response_id.is_(None))
                .order_by(AssessmentInstance.assessment_instance_id.asc(), QuestionnaireDefinition.question_id.asc())
            )
            unanswered_result = await db.execute(unanswered_query)
            for r in unanswered_result.all():
                unanswered_map.setdefault(int(r.assessment_instance_id), {}).setdefault(ck, []).append(
                    {
                        "question_id": int(r.question_id),
                        "question_text": r.question_text,
                    }
                )

        # Group by user
        user_data: dict[int, dict] = {}
        for row in instance_rows:
            uid = int(row.user_id)
            iid = int(row.assessment_instance_id)
            pid = int(row.package_id)
            assigned_cats = pkg_categories.get(pid, set())

            if uid not in user_data:
                user_data[uid] = {
                    "user_id": uid,
                    "first_name": row.first_name,
                    "last_name": row.last_name,
                    "phone": mask_phone(row.phone),
                    "email": mask_email(row.email),
                    "categories": {},
                    "categories_by_of": {"metsights": {}, "supershyft": {}},
                    "has_any_responses": False,
                    "all_assigned_complete": True,
                    "assigned_cats": set(),
                }

            entry = user_data[uid]
            entry["assigned_cats"].update(assigned_cats)
            total_resp = resp_counts.get(iid, 0)
            if total_resp > 0:
                entry["has_any_responses"] = True

            inst_progress = progress_map.get(iid, {})

            for ck, cat_of in assigned_cats:
                assigned = True
                prog = inst_progress.get(ck, {})
                has_resp = cat_resp_map.get(iid, {}).get(ck, False)
                cat_status = prog.get("status", "incomplete")
                cat_submitted = prog.get("is_submitted", False)
                unanswered = unanswered_map.get(iid, {}).get(ck, []) if assigned else []

                if assigned and cat_status != "complete" and has_resp and not unanswered:
                    cat_status = "complete"

                bucket = entry["categories_by_of"].setdefault(cat_of, {})
                existing = bucket.get(ck)
                cat_payload = {
                    "status": cat_status,
                    "is_submitted": cat_submitted,
                    "has_responses": has_resp,
                    "assigned": assigned,
                    "unanswered_required": unanswered if cat_status != "complete" else [],
                }
                if existing is None or (assigned and not existing.get("assigned")):
                    bucket[ck] = cat_payload
                elif assigned:
                    if cat_status == "complete":
                        existing["status"] = "complete"
                        existing["unanswered_required"] = []
                    if cat_submitted:
                        existing["is_submitted"] = True
                    if has_resp:
                        existing["has_responses"] = True
                    if unanswered and existing.get("status") != "complete":
                        prev = existing.get("unanswered_required") or unanswered
                        if len(unanswered) <= len(prev):
                            existing["unanswered_required"] = unanswered

                if cat_of == "metsights":
                    existing_flat = entry["categories"].get(ck)
                    if existing_flat is None or (assigned and not existing_flat.get("assigned")):
                        entry["categories"][ck] = dict(bucket[ck])
                    elif assigned:
                        if cat_status == "complete":
                            existing_flat["status"] = "complete"
                        if cat_submitted:
                            existing_flat["is_submitted"] = True
                        if has_resp:
                            existing_flat["has_responses"] = True

                if assigned and cat_status != "complete":
                    if cat_of == "metsights" and pkg_essentials.get(pid) and category_optional_for_essentials_completion(ck):
                        pass
                    else:
                        entry["all_assigned_complete"] = False

        participants: list[dict] = []
        summary_filled = 0
        summary_partial = 0
        summary_not_started = 0

        for entry in user_data.values():
            assigned_cats = entry["assigned_cats"]
            if not assigned_cats:
                summary_not_started += 1
                state = "not_started"
            elif entry["all_assigned_complete"]:
                summary_filled += 1
                state = "filled"
            elif entry["has_any_responses"]:
                summary_partial += 1
                state = "partially_filled"
            else:
                summary_not_started += 1
                state = "not_started"

            def _default_cat(key: str, cat_of: str, is_assigned: bool) -> dict:
                return {
                    "status": "incomplete",
                    "is_submitted": False,
                    "has_responses": False,
                    "assigned": is_assigned,
                    "unanswered_required": [],
                }

            assigned_keys_m = {(k, o) for k, o in assigned_cats if o == "metsights"}
            assigned_keys_s = {(k, o) for k, o in assigned_cats if o == "supershyft"}
            cats_metsights: dict[str, dict] = {}
            for col in questionnaire_columns.get("metsights") or []:
                ck = col["key"]
                cats_metsights[ck] = entry["categories_by_of"].get("metsights", {}).get(
                    ck,
                    _default_cat(ck, "metsights", (ck, "metsights") in assigned_keys_m),
                )
            cats_supershyft: dict[str, dict] = {}
            for col in questionnaire_columns.get("supershyft") or []:
                ck = col["key"]
                cats_supershyft[ck] = entry["categories_by_of"].get("supershyft", {}).get(
                    ck,
                    _default_cat(ck, "supershyft", (ck, "supershyft") in assigned_keys_s),
                )

            participants.append({
                "user_id": entry["user_id"],
                "first_name": entry["first_name"],
                "last_name": entry["last_name"],
                "phone": entry["phone"],
                "email": entry["email"],
                "questionnaire_state": state,
                "categories": cats_metsights,
                "questionnaire_categories": {
                    "metsights": cats_metsights,
                    "supershyft": cats_supershyft,
                },
            })

        return {
            "summary": {
                "filled": summary_filled,
                "partially_filled": summary_partial,
                "not_started": summary_not_started,
            },
            "participants": participants,
            "questionnaire_columns": questionnaire_columns,
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

        ensure_admin(employee)

        if self._assessments_service is None:
            raise RuntimeError("Assessments service is required")
        if self._metsights_service is None:
            raise RuntimeError("Metsights service is required")

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")

        if (engagement.status or "").lower() != "running":
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Engagement is not running")

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
                            slot_start_time=default_slot
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

        for item in results:
            item["phone"] = mask_phone(item.get("phone"))
            item["email"] = mask_email(item.get("email"))

        return {"results": results}

    async def create_metsights_profiles_for_engagement_participants(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        mode: str = "profile",
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict[str, Any]:
        """Create Metsights profiles for enrolled participants.

        Modes:
        - ``enrol_force``: Create a blank profile via ``POST /external/profiles/`` for every
          participant, even if they already have ``metsights_profile_id``.
        - ``enrol``: Same create call, only for participants without ``metsights_profile_id``.
        - ``profile``: Create standalone profiles via ``POST /external/profiles/`` for
          participants without ``metsights_profile_id``.
          Names and phone numbers are not sent. The external API has no engagement register.
        """

        _ = ip_address, user_agent, endpoint
        ensure_admin(employee)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")

        if self._metsights_service is None:
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message="Metsights integration is not configured",
            )

        metsights_engagement_id = (engagement.metsights_engagement_id or "").strip()
        if mode in ("enrol_force", "enrol") and not metsights_engagement_id:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Metsights Engagement ID is required for enrol mode",
            )

        user_ids = await self._repository.list_distinct_participant_ids_for_engagement(
            db,
            engagement_id=engagement_id,
        )

        skip_existing = mode != "enrol_force"

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
            if skip_existing and existing_profile_id:
                base["status"] = "skipped"
                base["reason"] = "already_has_metsights_profile_id"
                base["metsights_profile_id"] = existing_profile_id
                skipped += 1
                results.append(base)
                continue

            first_name = (user.first_name or "").strip()
            last_name = outbound_last_name(user.last_name)
            gender = _to_metsights_gender(user.gender)
            phone = _normalize_phone_for_metsights(user.phone) or (user.phone or "").strip()
            dob = user.date_of_birth.isoformat() if user.date_of_birth is not None else None
            email = (user.email or "").strip() if user.email else None

            if not first_name or not phone or gender is None:
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
            last_error: str | None = None

            if mode in ("enrol_force", "enrol"):
                try:
                    profile_id = await self._metsights_service.create_profile_for_engagement(
                        engagement_id=metsights_engagement_id,
                        first_name=first_name,
                        last_name=last_name,
                        phone=phone,
                        email=email,
                        gender=gender,
                        date_of_birth=dob,
                        age=user.age,
                    )
                except AppError as exc:
                    last_error = exc.message or exc.error_code or "metsights_error"
                except Exception as exc:
                    last_error = str(exc)
            else:
                candidate_phones = [phone]
                raw_phone = (user.phone or "").strip()
                if raw_phone and raw_phone not in candidate_phones:
                    candidate_phones.append(raw_phone)

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
