"""Engagements service.

Business rules:
- Engagement creation
- Enrolling users by creating `engagement_participants`
"""

from __future__ import annotations

import logging
import secrets
import string
from datetime import date, datetime, time, timedelta, timezone
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from common.phone import phone_lookup_candidates as _phone_lookup_candidates
from core.config import settings
from core.exceptions import AppError
from modules.assessments.repository import AssessmentsRepository
from modules.assessments.service import AssessmentsService
from modules.audit.service import AuditService
from modules.checklists.schemas import ChecklistReadiness
from modules.employee.access_control import (
    ONBOARDING_ASSISTANT_ASSIGNEE_ROLES,
    ensure_admin,
    ensure_org_manager_assignable_to_engagement,
)
from modules.employee.models import EmployeeRole
from modules.employee.repository import EmployeeRepository
from modules.employee.service import EmployeeContext
from modules.engagements.camp_no import compute_camp_no
from modules.engagements.models import BloodCollectionType, Engagement, EngagementParticipant, EngagementStatus, OnboardingAssistantAssignment
from modules.engagements.repository import EngagementsRepository
from modules.engagements.slot_availability import (
    build_public_slot_detail,
    coerce_time,
    format_hhmm,
    occupancy_map_from_rows,
    require_available_blood_collection_slot,
    require_available_consultation_slot,
    slot_detail_is_configured,
    slot_unavailable,
)
from modules.experts.consultation_bookings_repository import ConsultationBookingsRepository
from modules.experts.consultations import bookings_to_consultations_map, empty_consent, normalize_consultations_map, normalize_consent
from modules.engagements.schemas import (
    EngagementCreateRequest,
    EngagementParticipantUpdateRequest,
    EngagementUpdateRequest,
    ConsultationConsentRequest,
    ResolveHealthiansZoneRequest,
    ResolveHealthiansZoneResponse,
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
from modules.users.models import User
from modules.users.repository import UsersRepository

if TYPE_CHECKING:
    from modules.checklists.service import ChecklistsService
    from modules.metsights.service import MetsightsService
    from modules.notifications.service import NotificationsService


logger = logging.getLogger(__name__)


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


def _participant_move_mismatch_reasons(source: Engagement, target: Engagement) -> list[str]:
    mismatches: list[str] = []
    if bool(source.create_profile_on_metsights) != bool(target.create_profile_on_metsights):
        mismatches.append("Create Profile on Metsights")
    if source.assessment_package_id != target.assessment_package_id:
        mismatches.append("Assessment package")
    if source.diagnostic_package_id != target.diagnostic_package_id:
        mismatches.append("diagnostic_package_id")
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
    return mismatches


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


def _parse_comma_separated_keys(raw: str | None) -> set[str]:
    return {k.strip() for k in (raw or "").split(",") if k.strip()}


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


def _participant_enrollment_to_dict(row: tuple, *, consultations: dict[str, Any] | None = None) -> dict[str, Any]:
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
        participants_employee_id,
        participant_department,
        participant_blood_group,
        _consultation_booking_ids,
        is_profile_created_on_metsights,
        is_primary_record_id_synced,
        is_fitprint_record_id_synced,
        barcode,
        booking_id,
        blood_collection_time_slot_id,
        booked_by_user_id,
    ) = row
    return {
        "engagement_participant_id": engagement_participant_id,
        "engagement_id": engagement_id,
        "user_id": user_id,
        "first_name": first_name,
        "last_name": last_name,
        "phone": phone,
        "email": email,
        "age": age,
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
        "consultations": consultations or {},
        "is_profile_created_on_metsights": is_profile_created_on_metsights,
        "is_primary_record_id_synced": is_primary_record_id_synced,
        "is_fitprint_record_id_synced": is_fitprint_record_id_synced,
        "barcode": barcode,
        "booking_id": booking_id,
        "blood_collection_time_slot_id": blood_collection_time_slot_id,
        "booked_by_user_id": booked_by_user_id,
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
        consultation_bookings_repository: ConsultationBookingsRepository | None = None,
        expert_types_service: ExpertTypesService | None = None,
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
        self._employee_repository = EmployeeRepository()
        self._assessments_repository = AssessmentsRepository()
        self._questionnaire_repository = QuestionnaireRepository()
        self._reports_repository = ReportsRepository()
        self._consultation_bookings = consultation_bookings_repository or ConsultationBookingsRepository()
        self._expert_types_service = expert_types_service or ExpertTypesService(repository=ExpertTypesRepository())
        self._checklists_service = None

    async def _participant_rows_to_dicts(self, db: AsyncSession, rows: list[tuple]) -> list[dict[str, Any]]:
        all_booking_ids: list[int] = []
        for row in rows:
            ids = row[19] or []
            all_booking_ids.extend(int(i) for i in ids)

        bookings = await self._consultation_bookings.get_by_ids(db, list(dict.fromkeys(all_booking_ids)))
        bookings_by_id = {booking.consultation_id: booking for booking in bookings}

        result: list[dict[str, Any]] = []
        for row in rows:
            ids = row[19] or []
            participant_bookings = [bookings_by_id[i] for i in ids if i in bookings_by_id]
            consultations = bookings_to_consultations_map(participant_bookings)
            result.append(_participant_enrollment_to_dict(row, consultations=consultations))
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
        for cabins in slot_detail.consultation.values():
            for cabin in cabins:
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

    @staticmethod
    def _validate_questionnaire_reminders_disjoint(qr1: str | None, qr2: str | None) -> None:
        overlap = _parse_comma_separated_keys(qr1) & _parse_comma_separated_keys(qr2)
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

        diagnostic_package_id = payload.diagnostic_package_id
        await self._validate_consultation_expert_types(db, payload.slot_detail)

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

        engagement = Engagement(
            engagement_name=payload.engagement_name,
            metsights_engagement_id=payload.metsights_engagement_id,
            organization_id=payload.organization_id,
            camp_no=compute_camp_no(payload.organization_id, payload.start_date),
            engagement_code=code,
            engagement_type=payload.engagement_type,
            consultations=payload.consultations,
            slot_detail=payload.slot_detail.model_dump(mode="json", exclude_none=True) if payload.slot_detail is not None else None,
            assessment_package_id=payload.assessment_package_id,
            diagnostic_package_id=diagnostic_package_id,
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
            create_profile_on_metsights=payload.create_profile_on_metsights,
            enroll_for_fitprint_full=payload.enroll_for_fitprint_full,
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
            user_id=employee.user_id,
            session_id=None,
        )

        await self._assign_default_onboarding_assistants(
            db,
            engagement_id=engagement.engagement_id,
            organization_id=engagement.organization_id,
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

        checklists = self.lazy_checklists_service()
        readiness_by_id: dict[int, ChecklistReadiness] = {}
        for row in engagements:
            readiness_by_id[row.engagement_id] = await checklists.get_engagement_readiness(db, row.engagement_id)

        return engagements, total, readiness_by_id, counts_by_id

    async def get_engagement_filter_options_for_employee(self, db: AsyncSession, *, employee: EmployeeContext) -> dict:
        ensure_admin(employee)
        types, cities = await self._repository.list_distinct_engagement_types_and_cities(db)
        return {"engagement_types": types, "cities": cities}

    async def resolve_healthians_zone_for_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        payload: ResolveHealthiansZoneRequest,
    ) -> ResolveHealthiansZoneResponse:
        from modules.diagnostics.healthians import client as healthians_client
        from modules.diagnostics.healthians.sync_log import finalize_healthians_sync_log, log_healthians_call
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

        serviceability_log = await log_healthians_call(
            db,
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
            await finalize_healthians_sync_log(
                db,
                sync_log_id=serviceability_log.sync_log_id,
                status="success" if serviceability_response.get("status") else "failed",
                response_payload=serviceability_response,
            )
        except Exception as exc:
            logger.exception("Healthians serviceability check failed for engagement zone resolution")
            await finalize_healthians_sync_log(
                db,
                sync_log_id=serviceability_log.sync_log_id,
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

    async def public_slot_detail_for_engagement(self, db: AsyncSession, engagement: Engagement):
        if not slot_detail_is_configured(engagement.slot_detail):
            return None
        rows = await self._repository.list_cabin_slot_occupancy(
            db,
            engagement_id=int(engagement.engagement_id),
        )
        consultation_rows = await self._consultation_bookings.list_consultation_cabin_slot_occupancy(
            db,
            engagement_id=int(engagement.engagement_id),
        )
        return build_public_slot_detail(
            engagement.slot_detail,
            occupancy_map_from_rows(rows),
            occupancy_map_from_rows(consultation_rows),
        )

    async def validate_blood_collection_slot_for_onboard(
        self,
        db: AsyncSession,
        *,
        engagement: Engagement,
        collection_date: date,
        cabin_key: str | None,
        slot_time: time,
    ) -> str | None:
        normalized_cabin = (cabin_key or "").strip() or None
        if not slot_detail_is_configured(engagement.slot_detail):
            return normalized_cabin

        cabin = require_available_blood_collection_slot(
            engagement.slot_detail,
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
        )
        capacity = int(cabin.get("capacity_per_slot") or 0)
        if count >= capacity:
            raise slot_unavailable()
        return persisted_cabin

    async def validate_consultation_slots_for_onboard(
        self,
        db: AsyncSession,
        *,
        engagement: Engagement,
        consultations: dict[str, Any] | None,
    ) -> dict[str, Any]:
        normalized = normalize_consultations_map(consultations)
        if not slot_detail_is_configured(engagement.slot_detail):
            return normalized
        section = None
        if isinstance(engagement.slot_detail, dict):
            section = engagement.slot_detail.get("consultation")
        if not isinstance(section, dict) or not section:
            return normalized

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
                engagement.slot_detail,
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
            )
            capacity = int(cabin.get("capacity_per_slot") or 0)
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
        if "slot_detail" in update_fields:
            await self._validate_consultation_expert_types(db, payload.slot_detail)
            engagement.slot_detail = (
                payload.slot_detail.model_dump(mode="json", exclude_none=True)
                if payload.slot_detail is not None
                else None
            )
        engagement.assessment_package_id = payload.assessment_package_id
        engagement.diagnostic_package_id = payload.diagnostic_package_id
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
            user_id=employee.user_id,
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
    ) -> Engagement:
        """Create a B2C engagement and auto-assign default onboarding assistants from platform settings.

        B2C engagements are created automatically during public user onboarding.
        Default assistants come from platform_settings.default_onboarding_assistant_employee_ids.

        When location fields are incomplete and an address (or address parts) is
        available, missing fields are filled via Nominatim. Geocode failures are
        ignored so engagement creation is never blocked.
        """
        from modules.geocoding.client import enrich_location_fields

        name_part = (user_first_name or "user").strip() or "user"
        engagement_name = f"{name_part}-{engagement_date.isoformat()}"

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
            slot_duration=20,
            start_date=engagement_date,
            end_date=engagement_date,
            status="running" if engagement_date <= date.today() else "scheduled",
            create_profile_on_metsights=create_profile_on_metsights,
            enroll_for_fitprint_full=enroll_for_fitprint_full,
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
        employee_ids = await self._resolve_default_onboarding_assistant_employee_ids(db)
        if not employee_ids:
            return

        for emp_id in employee_ids:
            row = await self._employee_repository.get_by_id(db, emp_id)
            if row is None:
                logger.warning("Skipping default onboarding assistant %s: employee not found", emp_id)
                continue
            if (row.status or "").lower() != "active":
                logger.warning("Skipping default onboarding assistant %s: employee not active", emp_id)
                continue
            if row.role not in ONBOARDING_ASSISTANT_ASSIGNEE_ROLES:
                logger.warning("Skipping default onboarding assistant %s: invalid role", emp_id)
                continue
            if organization_id is None and row.role == EmployeeRole.organization_manager:
                logger.info(
                    "Skipping default onboarding assistant %s: organization_manager on B2C engagement",
                    emp_id,
                )
                continue

            try:
                await ensure_org_manager_assignable_to_engagement(
                    db,
                    assignee_user_id=row.user_id,
                    assignee_role=row.role,
                    engagement_id=engagement_id,
                    repository=self._repository,
                    organizations_repository=self._organizations_repository,
                )
            except AppError as exc:
                logger.info(
                    "Skipping default onboarding assistant %s: %s",
                    emp_id,
                    exc.message,
                )
                continue

            existing = await self._repository.get_onboarding_assistant_assignment(
                db,
                engagement_id=engagement_id,
                employee_id=emp_id,
            )
            if existing is not None:
                continue

            assignment = OnboardingAssistantAssignment(
                engagement_id=engagement_id,
                employee_id=emp_id,
            )
            await self._repository.create_onboarding_assistant_assignment(db, assignment)

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
        engagement_date: date | None,
        slot_start_time: time | None,
        participants_employee_id: str | None = None,
        participant_department: str | None = None,
        participant_blood_group: str | None = None,
        blood_collection_cabin: str | None = None,
        consultations: dict | None = None,
        is_profile_created_on_metsights: bool = False,
        is_primary_record_id_synced: bool = False,
        is_fitprint_record_id_synced: bool = False,
        booked_by_user_id: int | None = None,
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

        booked_by = booked_by_user_id if booked_by_user_id is not None else user_id
        participant = EngagementParticipant(
            engagement_id=engagement.engagement_id,
            user_id=user_id,
            booked_by_user_id=booked_by,
            engagement_date=engagement_date,
            slot_start_time=slot_start_time,
            participants_employee_id=participants_employee_id,
            participant_department=participant_department,
            participant_blood_group=participant_blood_group,
            blood_collection_cabin=(blood_collection_cabin or "").strip() or None,
            is_profile_created_on_metsights=is_profile_created_on_metsights,
            is_primary_record_id_synced=is_primary_record_id_synced,
            is_fitprint_record_id_synced=is_fitprint_record_id_synced,
        )
        created = await self._repository.create_participant(db, participant)
        if consultations:
            await self._consultation_bookings.sync_from_want_map(db, created, consultations)
        return created

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

        if "booking_id" in updates:
            raw_booking_id = updates["booking_id"]
            if raw_booking_id is None:
                normalized_booking_id = None
            else:
                stripped = str(raw_booking_id).strip()
                normalized_booking_id = stripped or None
            participant.booking_id = normalized_booking_id
            response["booking_id"] = normalized_booking_id

        await self._repository.update_participant(db, participant)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_UPDATE_PARTICIPANT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return response

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

        return {
            "engagement_id": engagement_id,
            "user_id": user_id,
            "my_consultations": my_consultations,
            "consultations": engagement_consultations,
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

        result = await self._participant_rows_to_dicts(db, participants)
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

        ensure_admin(employee)

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

        result = await self._participant_rows_to_dicts(db, participants)
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
            user_id=employee.user_id,
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
            user_id=employee.user_id,
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
            user_id=employee.user_id,
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

        from modules.assessments.models import (
            AssessmentCategoryProgress,
            AssessmentInstance,
            AssessmentPackage,
            AssessmentPackageCategory,
        )
        from modules.questionnaire.models import QuestionnaireCategory, QuestionnaireResponse
        from modules.users.models import User
        from sqlalchemy import select, func

        _METSIGHTS_CATEGORY_KEYS = [
            "physical-measurement",
            "diet-lifestyle-parameters",
            "vitals",
            "fitness-parameters",
        ]

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
            )
            .join(QuestionnaireCategory, QuestionnaireCategory.category_id == AssessmentPackageCategory.category_id)
            .where(AssessmentPackageCategory.package_id.in_(package_ids))
            .where(QuestionnaireCategory.category_of == "metsights")
        )
        pkg_cat_result = await db.execute(pkg_cat_query)
        pkg_categories: dict[int, set[str]] = {}
        for pc in pkg_cat_result.all():
            pkg_categories.setdefault(int(pc.package_id), set()).add(pc.category_key)

        # Also get per-category response counts using category_ids array overlap
        cat_resp_map: dict[int, dict[str, bool]] = {}  # instance_id -> category_key -> has_responses
        for iid in instance_ids:
            cat_resp_map[int(iid)] = {}

        # Query category_id for each metsights category key
        cat_key_to_id: dict[str, int] = {}
        for ck in _METSIGHTS_CATEGORY_KEYS:
            cat_row = await db.execute(
                select(QuestionnaireCategory.category_id)
                .where(QuestionnaireCategory.category_key == ck)
                .where(QuestionnaireCategory.category_of == "metsights")
                .limit(1)
            )
            cid = cat_row.scalar_one_or_none()
            if cid is not None:
                cat_key_to_id[ck] = int(cid)

        # Check if responses exist per category per instance
        for ck, cid in cat_key_to_id.items():
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
        for ck, cid in cat_key_to_id.items():
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
                    "phone": row.phone,
                    "email": row.email,
                    "categories": {},
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

            for ck in _METSIGHTS_CATEGORY_KEYS:
                assigned = ck in assigned_cats
                prog = inst_progress.get(ck, {})
                has_resp = cat_resp_map.get(iid, {}).get(ck, False)
                cat_status = prog.get("status", "incomplete")
                cat_submitted = prog.get("is_submitted", False)
                unanswered = unanswered_map.get(iid, {}).get(ck, []) if assigned else []

                # Infer complete from required-question coverage when progress row is missing/stale
                if assigned and cat_status != "complete" and has_resp and not unanswered:
                    cat_status = "complete"

                existing = entry["categories"].get(ck)
                if existing is None or (assigned and not existing.get("assigned")):
                    entry["categories"][ck] = {
                        "status": cat_status,
                        "is_submitted": cat_submitted,
                        "has_responses": has_resp,
                        "assigned": assigned,
                        "unanswered_required": unanswered if cat_status != "complete" else [],
                    }
                elif assigned:
                    # Merge: take best status
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

                if assigned and cat_status != "complete":
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

            # Fill default for unset categories
            cats_output = {}
            for ck in _METSIGHTS_CATEGORY_KEYS:
                cats_output[ck] = entry["categories"].get(ck, {
                    "status": "incomplete",
                    "is_submitted": False,
                    "has_responses": False,
                    "assigned": ck in assigned_cats,
                    "unanswered_required": [],
                })

            participants.append({
                "user_id": entry["user_id"],
                "first_name": entry["first_name"],
                "last_name": entry["last_name"],
                "phone": entry["phone"],
                "email": entry["email"],
                "questionnaire_state": state,
                "categories": cats_output,
            })

        return {
            "summary": {
                "filled": summary_filled,
                "partially_filled": summary_partial,
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
                            slot_start_time=default_slot,
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
        - ``enrol_force``: Register ALL participants via ``POST /engagements/{id}/register/``
          (even if they already have ``metsights_profile_id``). Requires ``metsights_engagement_id``.
        - ``enrol``: Register only participants without ``metsights_profile_id`` via engagement
          registration. Requires ``metsights_engagement_id``.
        - ``profile``: Create standalone profiles via ``POST /profiles/`` for participants
          without ``metsights_profile_id``.
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
