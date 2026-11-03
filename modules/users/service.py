"""Users service.

This module owns user business rules.
Auth may only use it for existence checks.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional
from uuid import uuid4

if TYPE_CHECKING:
    from modules.employee.service import EmployeeContext

import logging
import random
from datetime import date, datetime, time, timedelta, timezone

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from core.exceptions import AppError
from db.transaction import release_request_transaction
from modules.employee.models import Employee
from modules.audit.service import AuditService
from modules.metsights.service import MetsightsService, outbound_last_name
from modules.metsights.integration_logging import MetsightsSyncContext
from modules.metsights.sync_service import (
    _normalize_metsights_type_code,
    _parse_iso_date,
    _resolve_active_diagnostic_package_id,
)
from modules.platform_settings.b2c_onboard_apply import (
    b2c_engagement_create_kwargs_from_defaults,
    resolve_b2c_diagnostic_package_id,
)
from modules.platform_settings.service import PlatformSettingsService
from modules.users.models import User, UserAddress, UserPreference
from modules.users.repository import UsersRepository
from modules.engagements.blood_bookings_access import current_booking_id
from modules.engagements.models import BloodCollectionType, EngagementKind, EngagementParticipant
from modules.engagements.repository import EngagementsRepository
from modules.engagements.slot_availability import find_active_cabin
from modules.diagnostics.repository import DiagnosticsRepository
from common.masking import looks_masked
from common.phone import phone_lookup_candidates as _phone_lookup_candidates
from modules.users.address_utils import (
    MAX_USER_ADDRESSES,
    compose_address,
    snapshot_from_user,
    user_has_location,
)
from modules.users.schemas import (
    BookBioAiBatchRequest,
    BookBioAiRequest,
    BookBloodTestBatchRequest,
    BookingPaymentResponse,
    EmployeeCreateUserRequest,
    EmployeeUpdateUserRequest,
    EngagementOnboardConfirmPaymentRequest,
    EngagementOnboardVerifyPayRequest,
    EngagementUserOnboardRequest,
    FamilyOnboardConfirmPaymentRequest,
    FamilyOnboardMemberPayload,
    FamilyOnboardMemberResponse,
    FamilyOnboardStartRequest,
    FamilyOnboardStartResponse,
    FamilyOnboardVerifyPayRequest,
    UpcomingSlotResponse,
    PublicUserOnboardRequest,
    SubProfileCreate,
    SubProfileUpdate,
    UnlinkRequest,
    UserAddressCreate,
    UserAddressResponse,
    UserAddressUpdate,
    UserPreferencesUpdate,
    UpdateMyProfileRequest,
    UserOnboardResponse,
    VifcQuickStartRequest,
    VifcQuickStartResponse,
)

logger = logging.getLogger(__name__)
_ALWAYS_ACTIVE_EMPLOYEE_ID = 1

def _normalize_import_phone(raw: str | None) -> str:
    s = (raw or "").strip().replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    return s


def _integrity_error_reason(exc: IntegrityError) -> str:
    orig = getattr(exc, "orig", None)
    if orig is not None:
        text = str(orig)
        if "uq_users_email_global" in text or "email" in text.lower():
            return "Email already belongs to another user"
        if "phone" in text.lower():
            return "Phone already belongs to another user"
        if len(text) <= 200:
            return text
    return "Database constraint violation"


_diagnostics_repo = DiagnosticsRepository()


async def _resolve_engagement_type_id(db: AsyncSession, code: str) -> int | None:
    from modules.engagements.models import EngagementType
    from sqlalchemy import select as _select
    result = await db.execute(_select(EngagementType.id).where(EngagementType.code == code))
    return result.scalar_one_or_none()


async def _require_active_engagement_type_code(db: AsyncSession, code: str):
    """Resolve an active engagement_types row by code, or raise 422."""
    from modules.engagements.models import EngagementType
    from sqlalchemy import select as _select

    normalized = (code or "").strip()
    if not normalized:
        raise AppError(
            status_code=422,
            error_code="INVALID_ENGAGEMENT_TYPE",
            message="engagement_type is required and must match engagement_types.code",
        )
    result = await db.execute(
        _select(EngagementType).where(EngagementType.code == normalized)
    )
    row = result.scalar_one_or_none()
    if row is None or not row.is_active:
        raise AppError(
            status_code=422,
            error_code="INVALID_ENGAGEMENT_TYPE",
            message=f"engagement_type must be an active engagement_types.code; got '{normalized}'",
        )
    return row


async def _resolve_onboard_engagement_type_code(
    platform_settings: PlatformSettingsService,
    db: AsyncSession,
    explicit: str | None,
) -> str:
    code = (explicit or "").strip()
    if code:
        return code
    return await platform_settings.resolve_default_engagement_type_code(db)


def _validate_onboard_blood_fields(
    engagement_type_code: str,
    *,
    blood_collection_date: date | None,
    blood_collection_time_slot: str | None,
) -> None:
    if engagement_type_code.strip().lower() == "vifc":
        return
    if blood_collection_date is None:
        raise AppError(
            status_code=422,
            error_code="INVALID_INPUT",
            message="blood_collection_date is required",
        )
    slot = (blood_collection_time_slot or "").strip()
    if not slot:
        raise AppError(
            status_code=422,
            error_code="INVALID_INPUT",
            message="blood_collection_time_slot is required",
        )


_WITH_CONSULTATION_CODE_MAP = {
    "bio_ai": "bio_ai_with_consultation",
    "blood_test": "blood_test_with_consultation",
}


async def _resolve_consultation_from_pkg(
    db: AsyncSession,
    diagnostic_package_id: int | None,
    base_type_code: str,
) -> tuple[int | None, dict[str, bool] | None]:
    base_type_id = await _resolve_engagement_type_id(db, base_type_code)
    if diagnostic_package_id is None:
        return base_type_id, None
    pkg = await _diagnostics_repo.get_package_by_id_basic(db, package_id=diagnostic_package_id)
    if pkg is None:
        return base_type_id, None
    cc = pkg.complementary_consultation
    if not cc or not isinstance(cc, dict):
        return base_type_id, None
    has_any = any(v is True for v in cc.values())
    if not has_any:
        return base_type_id, None
    new_code = _WITH_CONSULTATION_CODE_MAP.get(base_type_code, base_type_code)
    new_type_id = await _resolve_engagement_type_id(db, new_code)
    return new_type_id or base_type_id, cc


def _validate_requested_consultations(
    requested: dict | None,
    allowed: dict[str, bool] | None,
) -> dict:
    """Reject want=true consultation flags unless enabled on the engagement."""
    from modules.experts.consultations import validate_requested_consultations

    return validate_requested_consultations(requested, allowed)

class UsersService:
    def _normalize_phone_for_metsights(self, raw: str | None) -> str | None:
        value = (raw or "").strip().replace(" ", "").replace("-", "")
        if not value:
            return None
        if value.startswith("+"):
            return value
        digits = "".join(ch for ch in value if ch.isdigit())
        if len(digits) == 10:
            return f"+91{digits}"
        return f"+{digits}" if digits else None

    async def _protected_user_ids(self, db: AsyncSession) -> set[int]:
        from modules.employee.repository import EmployeeRepository

        employee_row = await EmployeeRepository().get_by_id(db, _ALWAYS_ACTIVE_EMPLOYEE_ID)
        if employee_row is None:
            return set()
        phone = (employee_row.phone or "").strip()
        if not phone:
            return set()
        user = await self._repository.get_user_by_phone(db, phone)
        if user is None:
            normalized = self._normalize_phone_for_metsights(phone)
            if normalized and normalized != phone:
                user = await self._repository.get_user_by_phone(db, normalized)
        if user is None:
            return set()
        return {int(user.user_id)}

    async def _is_protected_employee_user(self, db: AsyncSession, user_id: int) -> bool:
        protected = await self._protected_user_ids(db)
        return int(user_id) in protected

    """Users service layer."""

    def _ensure_employee_access(self, employee) -> None:
        """Ensure the caller is an authenticated employee.

        This check is defensive.
        Routers should already enforce employee identity.
        """

        if employee is None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

    def __init__(
        self,
        repository: UsersRepository,
        audit_service: AuditService | None = None,
        engagements_service=None,
        assessments_service=None,
        platform_settings_service: PlatformSettingsService | None = None,
        metsights_service: MetsightsService | None = None,
        payments_service=None,
        notifications_service=None,
        questionnaire_service=None,
        aurae_service=None,
    ):
        self._repository = repository
        self._audit_service = audit_service
        self._engagements_service = engagements_service
        self._assessments_service = assessments_service
        self._platform_settings_service = platform_settings_service
        self._metsights_service = metsights_service
        self._payments_service = payments_service
        self._notifications_service = notifications_service
        self._questionnaire_service = questionnaire_service
        self._aurae_service = aurae_service

    async def get_existing_user_by_phone(self, db: AsyncSession, phone: str) -> Optional[User]:
        return await self._repository.get_user_by_phone(db, phone)

    async def resolve_user_by_phone(self, db: AsyncSession, phone: str) -> User | None:
        """Resolve a user by phone, matching +91 / 10-digit / E.164 variants."""
        return await self._resolve_user_by_phone_for_import(db, phone)

    async def _has_phone_conflict(
        self,
        db: AsyncSession,
        *,
        phone: str,
        exclude_user_id: int | None = None,
    ) -> bool:
        """True when another account already uses this phone (incl. +91 / 10-digit variants)."""

        candidates = _phone_lookup_candidates(phone)
        if not candidates:
            return False
        users = await self._repository.list_users_by_phones(db, candidates)
        if exclude_user_id is None:
            return bool(users)
        return any(int(u.user_id) != int(exclude_user_id) for u in users)

    async def get_user_by_id(self, db: AsyncSession, user_id: int) -> Optional[User]:
        return await self._repository.get_user_by_id(db, user_id)

    async def list_my_consultations(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        status: str | None,
        page: int,
        limit: int,
    ) -> tuple[list[dict], int]:
        from modules.engagements.consultation_booking_validation import effective_consultation_mode
        from modules.experts.consultations import derive_user_consultation_status

        total = await self._repository.count_user_consultations(
            db, user_id=user_id, status=status
        )
        rows = await self._repository.list_user_consultations(
            db,
            user_id=user_id,
            status=status,
            page=page,
            limit=limit,
        )
        data: list[dict] = []
        for booking, engagement, engagement_type_row in rows:
            eng_status = engagement.status
            if hasattr(eng_status, "value"):
                eng_status_str = eng_status.value
            else:
                eng_status_str = str(eng_status) if eng_status is not None else None

            list_status = derive_user_consultation_status(
                done=bool(booking.done),
                want=bool(booking.want),
                engagement_status=engagement.status,
            )
            date_val = (
                booking.consultation_date.isoformat() if booking.consultation_date else None
            )
            data.append(
                {
                    "consultation_id": booking.consultation_id,
                    "status": list_status,
                    "engagement_id": engagement.engagement_id,
                    "engagement_type": (
                        engagement_type_row.code if engagement_type_row is not None else None
                    ),
                    "consultation_mode": effective_consultation_mode(engagement).value,
                    "expert_type": booking.expert_type,
                    "expert_id": booking.expert_id,
                    "date": date_val,
                    "slot": booking.consultation_slot,
                    "want": bool(booking.want),
                    "done": bool(booking.done),
                    "engagement_status": eng_status_str,
                }
            )
        return data, total

    async def list_my_appointments(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        status: str | None,
        page: int,
        limit: int,
    ) -> tuple[list[dict], int]:
        from modules.engagements.consultation_booking_validation import effective_consultation_mode
        from modules.users.appointments import (
            derive_user_blood_appointment_status,
            derive_user_consultation_appointment_status,
        )

        consult_rows = await self._repository.list_user_consultations_for_appointments(
            db, user_id=user_id, status=status
        )
        blood_rows = await self._repository.list_user_blood_bookings_for_appointments(
            db, user_id=user_id, status=status
        )

        merged: list[dict] = []

        for booking, engagement, engagement_type_row in consult_rows:
            eng_status = engagement.status
            if hasattr(eng_status, "value"):
                eng_status_str = eng_status.value
            else:
                eng_status_str = str(eng_status) if eng_status is not None else None

            want_val = booking.want
            list_status = derive_user_consultation_appointment_status(
                done=bool(booking.done),
                want=want_val if want_val is None else bool(want_val),
                engagement_status=engagement.status,
            )
            if want_val is None:
                want_out = None
            else:
                want_out = bool(want_val)

            date_val = (
                booking.consultation_date.isoformat() if booking.consultation_date else None
            )
            merged.append(
                {
                    "appointment_type": "consultation",
                    "consultation_id": booking.consultation_id,
                    "status": list_status,
                    "engagement_id": engagement.engagement_id,
                    "engagement_type": (
                        engagement_type_row.code if engagement_type_row is not None else None
                    ),
                    "consultation_mode": effective_consultation_mode(engagement).value,
                    "expert_type": booking.expert_type,
                    "expert_id": booking.expert_id,
                    "date": date_val,
                    "slot": booking.consultation_slot,
                    "want": want_out,
                    "done": bool(booking.done),
                    "engagement_status": eng_status_str,
                    "_sort_date": booking.consultation_date,
                    "_sort_slot": booking.consultation_slot or "",
                    "_sort_id": booking.consultation_id,
                }
            )

        for blood_row, engagement, engagement_type_row in blood_rows:
            eng_status = engagement.status
            if hasattr(eng_status, "value"):
                eng_status_str = eng_status.value
            else:
                eng_status_str = str(eng_status) if eng_status is not None else None

            st = blood_row.status
            st_str = st.value if hasattr(st, "value") else str(st)
            rel = blood_row.relation
            rel_str = rel.value if hasattr(rel, "value") else str(rel)

            list_status = derive_user_blood_appointment_status(
                blood_row_status=st_str,
                engagement_status=engagement.status,
                collected_at=blood_row.collected_at,
            )

            slot = blood_row.collection_time
            bct = engagement.blood_collection_type
            if bct is not None and hasattr(bct, "value"):
                bct_str = bct.value
            else:
                bct_str = str(bct) if bct is not None else None

            merged.append(
                {
                    "appointment_type": "blood_test",
                    "blood_booking_id": blood_row.id,
                    "status": list_status,
                    "engagement_id": engagement.engagement_id,
                    "engagement_type": (
                        engagement_type_row.code if engagement_type_row is not None else None
                    ),
                    "date": (
                        blood_row.collection_date.isoformat()
                        if blood_row.collection_date
                        else None
                    ),
                    "slot": slot.isoformat() if slot is not None else None,
                    "cabin": blood_row.collection_cabin,
                    "blood_collection_type": bct_str,
                    "booking_id": blood_row.booking_id,
                    "relation": rel_str,
                    "engagement_status": eng_status_str,
                    "_sort_date": blood_row.collection_date,
                    "_sort_slot": slot.isoformat() if slot is not None else "",
                    "_sort_id": blood_row.id,
                }
            )

        def _sort_key(item: dict) -> tuple:
            d = item.get("_sort_date")
            return (
                d is None,
                d or date.min,
                item.get("_sort_slot") or "",
                item.get("appointment_type") or "",
                item.get("_sort_id") or 0,
            )

        merged.sort(key=_sort_key, reverse=True)

        total = len(merged)
        offset = (page - 1) * limit
        page_items = merged[offset : offset + limit]
        for item in page_items:
            item.pop("_sort_date", None)
            item.pop("_sort_slot", None)
            item.pop("_sort_id", None)

        return page_items, total

    async def get_upcoming_slots(self, db: AsyncSession, *, user_id: int) -> UpcomingSlotResponse:
        rows = await self._repository.get_upcoming_slots(db, user_id)
        if not rows:
            return UpcomingSlotResponse(has_scheduled_slot=False, slots=[])

        slots = []
        for row in rows:
            if row.engagement_date is None or row.slot_start_time is None:
                continue

            is_b2b = row.organization_id is not None
            engagement_type = "b2b" if is_b2b else "b2c"

            cabin_key = (row.blood_collection_cabin or "").strip() or None
            slot_detail = row.slot_detail if isinstance(row.slot_detail, dict) else None

            slot_duration_minutes = int(row.slot_duration or 0)
            if cabin_key and slot_detail:
                cabin = find_active_cabin(
                    slot_detail,
                    section="blood_collection",
                    date_key=row.engagement_date.isoformat(),
                    cabin_key=cabin_key,
                )
                if cabin is not None:
                    cabin_duration = int(cabin.get("slot_duration") or 0)
                    if cabin_duration > 0:
                        slot_duration_minutes = cabin_duration

            slot_start_dt = datetime.combine(row.engagement_date, row.slot_start_time)
            slot_end_dt = slot_start_dt + timedelta(minutes=slot_duration_minutes)

            from modules.engagements.participant_location import (
                resolve_upcoming_slot_location_display,
                resolve_upcoming_slot_location_type,
            )

            participant_count = int(getattr(row, "participant_count", 0) or 0)
            location_display = resolve_upcoming_slot_location_display(
                blood_collection_type=getattr(row, "blood_collection_type", None),
                participant_address=getattr(row, "participant_address", None),
                engagement_address=getattr(row, "engagement_address", None),
                engagement_city=getattr(row, "engagement_city", None),
                user_address=getattr(row, "user_address", None),
                user_city=getattr(row, "user_city", None),
                organization_id=row.organization_id,
                participant_count=participant_count,
            )
            location_type = resolve_upcoming_slot_location_type(
                blood_collection_type=getattr(row, "blood_collection_type", None),
                organization_id=row.organization_id,
            )

            slots.append(
                {
                    "engagement": {
                        "engagement_type": engagement_type,
                        "organization_name": row.organization_name if is_b2b else None,
                    },
                    "slot": {
                        "slot_start_time": slot_start_dt.strftime("%I:%M %p").lstrip("0"),
                        "slot_end_time": slot_end_dt.strftime("%I:%M %p").lstrip("0"),
                        "engagement_date": row.engagement_date,
                        "is_booked": row.booking_id is not None,
                        "cabin": cabin_key,
                    },
                    "location": {
                        "type": location_type,
                        "display": location_display,
                    },
                }
            )

        return UpcomingSlotResponse(has_scheduled_slot=bool(slots), slots=slots)

    async def get_profiles(self, db: AsyncSession, *, current_user: User) -> list[User]:
        if current_user.parent_id is not None:
            return await self._repository.get_profiles_as_sub(db, current_user.parent_id)
        return await self._repository.get_profiles_as_primary(db, current_user.user_id)

    async def create_sub_profile(
        self,
        db: AsyncSession,
        *,
        current_user: User,
        data: SubProfileCreate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> User:
        if current_user.parent_id is not None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="Sub-profiles cannot create additional profiles",
            )

        phone_to_use = data.phone or current_user.phone
        if data.phone is not None:
            if await self._has_phone_conflict(
                db,
                phone=data.phone,
                exclude_user_id=int(current_user.user_id),
            ):
                raise AppError(status_code=409, error_code="CONFLICT", message="User already exists")

        email_to_use = str(data.email) if data.email is not None else None
        if email_to_use is not None:
            existing_email = await self._repository.get_user_by_email(db, email_to_use)
            if existing_email is not None:
                raise AppError(status_code=409, error_code="CONFLICT", message="User already exists")
        else:
            if not current_user.email or "@" not in current_user.email:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

            local_part, domain = current_user.email.split("@", 1)
            for _ in range(200):
                suffix = random.randint(1000, 9999)
                candidate = f"{local_part}+{suffix}@{domain}"
                existing = await self._repository.get_user_by_email(db, candidate)
                if existing is None:
                    email_to_use = candidate
                    break

        if email_to_use is None:
            raise AppError(status_code=409, error_code="CONFLICT", message="Unable to create profile")

        created = await self._repository.create_sub_profile(
            db,
            current_user,
            {
                "first_name": data.first_name,
                "last_name": data.last_name,
                "age": data.age,
                "date_of_birth": data.date_of_birth,
                "gender": data.gender,
                "relationship": data.relationship,
                "city": data.city,
                "phone": phone_to_use,
                "email": email_to_use,
            },
        )
        if created is None:
            raise AppError(status_code=409, error_code="CONFLICT", message="Unable to create profile")

        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        await self._audit_service.log_event(
            db,
            action="USER_CREATE_SUB_PROFILE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=current_user.user_id,
            session_id=None,
        )

        return created

    async def update_sub_profile(
        self,
        db: AsyncSession,
        *,
        current_user: User,
        target_user_id: int,
        data: SubProfileUpdate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> User:
        if current_user.parent_id is not None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

        target = await self._repository.get_user_by_id(db, target_user_id)
        if target is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")
        if target.parent_id != current_user.user_id:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

        partial = data.model_dump(exclude_unset=True)
        if "email" in partial and partial["email"] is not None:
            new_email = str(partial["email"])
            existing_email = await self._repository.get_user_by_email(db, new_email)
            if existing_email is not None and existing_email.user_id != target_user_id:
                raise AppError(status_code=409, error_code="CONFLICT", message="User already exists")
            partial["email"] = new_email

        updated = await self._repository.update_user_partial(db, target_user_id, partial)
        if updated is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        await self._audit_service.log_event(
            db,
            action="USER_UPDATE_SUB_PROFILE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=current_user.user_id,
            session_id=None,
        )
        return updated

    async def _unlink_subject_from_parent(
        self,
        db: AsyncSession,
        *,
        subject: User,
        parent_user: User,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> User:
        if subject.phone == parent_user.phone:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Cannot unlink without an independent phone number. Update your phone number first before unlinking.",
            )

        updated = await self._repository.update_user_partial(
            db,
            subject.user_id,
            {
                "parent_id": None,
                "relationship": "self",
            },
        )
        if updated is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        await self._audit_service.log_event(
            db,
            action="USER_UNLINK_PROFILE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=subject.user_id,
            session_id=None,
        )

        return updated

    async def unlink_profile(
        self,
        db: AsyncSession,
        *,
        current_user: User,
        data: UnlinkRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> User:
        if current_user.parent_id is not None:
            if data.child_user_id is not None:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message="Cannot specify child_user_id when unlinking your own profile",
                )
            parent_user = await self._repository.get_user_by_id(db, current_user.parent_id)
            if parent_user is None:
                raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")
            return await self._unlink_subject_from_parent(
                db,
                subject=current_user,
                parent_user=parent_user,
                ip_address=ip_address,
                user_agent=user_agent,
                endpoint=endpoint,
            )

        if data.child_user_id is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Specify child_user_id to unlink a sub-profile from your account",
            )

        child = await self._repository.get_user_by_id(db, data.child_user_id)
        if child is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")
        if child.parent_id != current_user.user_id:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

        return await self._unlink_subject_from_parent(
            db,
            subject=child,
            parent_user=current_user,
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
        )

    async def get_user_preferences(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> UserPreference:
        preferences = await self._repository.get_preferences(db, user_id)
        if preferences is not None:
            return preferences

        created = await self._repository.upsert_preferences(db, user_id, data={})

        if self._audit_service is None:
            raise RuntimeError("Audit service is required")

        await self._audit_service.log_event(
            db,
            action="USER_INIT_PREFERENCES",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )
        return created

    async def update_user_preferences(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        data: UserPreferencesUpdate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> UserPreference:
        updated = await self._repository.upsert_preferences(db, user_id, data=data.model_dump(exclude_unset=True))

        if self._audit_service is None:
            raise RuntimeError("Audit service is required")

        await self._audit_service.log_event(
            db,
            action="USER_UPDATE_PREFERENCES",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )
        return updated

    async def update_my_profile(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        payload: UpdateMyProfileRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> User:
        user = await self._repository.get_user_by_id(db, user_id)
        if user is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

        payload_to_apply = payload
        partial = payload.model_dump(exclude_unset=True)
        if "phone" in partial and partial["phone"] is not None:
            new_phone = partial["phone"].strip()
            if new_phone != (user.phone or "").strip():
                if await self._has_phone_conflict(
                    db,
                    phone=new_phone,
                    exclude_user_id=int(user.user_id),
                ):
                    raise AppError(status_code=409, error_code="CONFLICT", message="User already exists")
            payload_to_apply = payload.model_copy(update={"phone": new_phone})

        updated = await self._repository.update_user_profile(db, user=user, payload=payload_to_apply)

        location_keys = {"address", "pin_code", "city", "state"}
        if location_keys.intersection(partial.keys()):
            await self._sync_default_address_from_user(db, updated)

        if self._audit_service is None:
            raise RuntimeError("Audit service is required")

        await self._audit_service.log_event(
            db,
            action="USER_UPDATE_PROFILE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user.user_id,
            session_id=None,
        )

        return updated

    def _to_address_response(self, row: UserAddress) -> UserAddressResponse:
        return UserAddressResponse(
            user_address_id=row.user_address_id,
            user_id=row.user_id,
            address_line1=row.address_line1,
            address_line2=row.address_line2,
            landmark=row.landmark,
            city=row.city,
            state=row.state,
            pincode=row.pincode,
            address=row.address,
            is_default=bool(row.is_default),
            created_at=row.created_at,
            updated_at=row.updated_at,
        )

    def _apply_address_fields(self, row: UserAddress, data: dict) -> None:
        for field_name, value in data.items():
            if field_name == "is_default":
                continue
            setattr(row, field_name, value)
        row.address = compose_address(row.address_line1, row.address_line2, row.landmark)
        row.updated_at = datetime.now(timezone.utc)

    async def _copy_address_to_user(self, db: AsyncSession, *, user_id: int, row: UserAddress | None) -> None:
        user = await self._repository.get_user_by_id(db, user_id)
        if user is None:
            return
        if row is None:
            user.address = None
            user.city = None
            user.state = None
            user.pin_code = None
        else:
            user.address = row.address
            user.city = row.city
            user.state = row.state
            user.pin_code = row.pincode
        user.updated_at = datetime.now(timezone.utc)
        db.add(user)
        await db.flush()

    async def _sync_default_address_from_user(self, db: AsyncSession, user: User) -> None:
        if not user_has_location(user):
            return

        snapshot = snapshot_from_user(user)
        default = await self._repository.get_default_address(db, int(user.user_id))
        if default is None:
            default = await self._repository.get_oldest_address(db, int(user.user_id))

        if default is None:
            count = await self._repository.count_addresses_for_user(db, int(user.user_id))
            if count >= MAX_USER_ADDRESSES:
                return
            row = UserAddress(
                user_id=int(user.user_id),
                is_default=True,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
                **snapshot,
            )
            await self._repository.create_address(db, row)
            return

        self._apply_address_fields(default, snapshot)
        if not default.is_default:
            await self._repository.clear_default_addresses(
                db, int(user.user_id), exclude_id=int(default.user_address_id)
            )
            default.is_default = True
        db.add(default)
        await db.flush()

    async def list_my_addresses(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> list[UserAddressResponse]:
        rows = await self._repository.list_addresses_for_user(db, user_id)
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        await self._audit_service.log_event(
            db,
            action="USER_LIST_ADDRESSES",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )
        return [self._to_address_response(row) for row in rows]

    async def create_my_address(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        payload: UserAddressCreate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> UserAddressResponse:
        count = await self._repository.count_addresses_for_user(db, user_id)
        if count >= MAX_USER_ADDRESSES:
            raise AppError(
                status_code=422,
                error_code="ADDRESS_LIMIT_REACHED",
                message="You can save up to 3 addresses",
            )

        make_default = bool(payload.is_default) or count == 0
        now = datetime.now(timezone.utc)
        row = UserAddress(
            user_id=int(user_id),
            address_line1=payload.address_line1,
            address_line2=payload.address_line2,
            landmark=payload.landmark,
            city=payload.city,
            state=payload.state,
            pincode=payload.pincode,
            address=compose_address(payload.address_line1, payload.address_line2, payload.landmark),
            is_default=make_default,
            created_at=now,
            updated_at=now,
        )
        if make_default:
            await self._repository.clear_default_addresses(db, user_id)
        row = await self._repository.create_address(db, row)
        if make_default:
            await self._copy_address_to_user(db, user_id=user_id, row=row)

        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        await self._audit_service.log_event(
            db,
            action="USER_CREATE_ADDRESS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )
        return self._to_address_response(row)

    async def update_my_address(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        user_address_id: int,
        payload: UserAddressUpdate,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> UserAddressResponse:
        row = await self._repository.get_address_by_id(db, user_address_id)
        if row is None or int(row.user_id) != int(user_id):
            raise AppError(status_code=404, error_code="ADDRESS_NOT_FOUND", message="Address does not exist")

        data = payload.model_dump(exclude_unset=True)
        make_default = data.pop("is_default", None)
        self._apply_address_fields(row, data)

        if make_default is True or (make_default is None and row.is_default):
            if make_default is True:
                await self._repository.clear_default_addresses(
                    db, user_id, exclude_id=int(row.user_address_id)
                )
                row.is_default = True
            db.add(row)
            await db.flush()
            if row.is_default:
                await self._copy_address_to_user(db, user_id=user_id, row=row)
        else:
            db.add(row)
            await db.flush()

        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        await self._audit_service.log_event(
            db,
            action="USER_UPDATE_ADDRESS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )
        return self._to_address_response(row)

    async def delete_my_address(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        user_address_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        row = await self._repository.get_address_by_id(db, user_address_id)
        if row is None or int(row.user_id) != int(user_id):
            raise AppError(status_code=404, error_code="ADDRESS_NOT_FOUND", message="Address does not exist")

        was_default = bool(row.is_default)
        await self._repository.delete_address(db, row)

        remaining = await self._repository.list_addresses_for_user(db, user_id)
        if not remaining:
            await self._copy_address_to_user(db, user_id=user_id, row=None)
        elif was_default:
            next_default = remaining[0]
            await self._repository.clear_default_addresses(
                db, user_id, exclude_id=int(next_default.user_address_id)
            )
            next_default.is_default = True
            next_default.updated_at = datetime.now(timezone.utc)
            db.add(next_default)
            await db.flush()
            await self._copy_address_to_user(db, user_id=user_id, row=next_default)

        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        await self._audit_service.log_event(
            db,
            action="USER_DELETE_ADDRESS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )

    def _parse_time_slot(self, slot: str) -> time:
        """Parse a time slot string.

        Expected formats:
        - "HH:MM"
        - "HH:MM:SS"

        We only store the start time in `engagement_participants.slot_start_time`.
        """

        value = (slot or "").strip()
        if not value:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        try:
            parts = value.split(":")
            if len(parts) == 2:
                return time(hour=int(parts[0]), minute=int(parts[1]))
            if len(parts) == 3:
                return time(hour=int(parts[0]), minute=int(parts[1]), second=int(parts[2]))
        except Exception as exc:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request") from exc

        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    async def _get_user_bookable_by_primary(
        self,
        db: AsyncSession,
        *,
        primary_user_id: int,
        target_user_id: int,
    ) -> User:
        """Return the target user row if the primary account may book on their behalf."""

        if target_user_id == primary_user_id:
            row = await self._repository.get_user_by_id(db, primary_user_id)
            if row is None:
                raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")
            return row

        row = await self._repository.get_user_by_id(db, target_user_id)
        if row is None or row.parent_id != primary_user_id:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to book for this user",
            )
        return row

    async def _create_bookings_for_batch(
        self,
        db: AsyncSession,
        *,
        actor: User,
        members: list,
        booking_type: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> BookingPaymentResponse:
        """Pre-payment phase: validate members, create Booking rows, create Razorpay order."""
        from modules.payments.services import PaymentsService

        if actor.parent_id is not None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="Batch booking is only available for primary accounts",
            )

        payments_service = self._payments_service
        if payments_service is None:
            payments_service = PaymentsService()

        validated_items: list[tuple[int, str, int]] = []
        metadata_by_user: dict[int, dict] = {}
        seen_user_ids: set[int] = set()

        for m in members:
            if m.user_id in seen_user_ids:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message="Duplicate user_id in members",
                )
            seen_user_ids.add(m.user_id)
            target = await self._get_user_bookable_by_primary(
                db, primary_user_id=actor.user_id, target_user_id=m.user_id
            )
            validated_items.append((target.user_id, "diagnostic_package", m.diagnostic_package_id))
            metadata_by_user[target.user_id] = {
                "address": m.address,
                "pincode": m.pincode,
                "city": m.city,
                "blood_collection_date": m.blood_collection_date.isoformat(),
                "blood_collection_time_slot": m.blood_collection_time_slot,
                "diagnostic_package_id": m.diagnostic_package_id,
            }

        result = await payments_service.create_order(
            db,
            payer_user_id=actor.user_id,
            items=validated_items,
            authenticated_user_id=actor.user_id,
            booking_type=booking_type,
            metadata_by_user=metadata_by_user,
        )

        err = result.get("_error")
        if err:
            code, msg = err
            raise AppError(status_code=code, error_code="PAYMENT_ERROR", message=msg)

        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        action = "USER_BOOK_BIO_AI" if booking_type == "bio_ai" else "USER_BOOK_BLOOD_TEST"
        await self._audit_service.log_event(
            db,
            action=action,
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=actor.user_id,
            session_id=None,
        )

        return BookingPaymentResponse(
            booking_ids=result["booking_ids"],
            booking_id=result["booking_id"],
            razorpay_order_id=result["razorpay_order_id"],
            amount_paise=result["amount_paise"],
            amount_rupees=result["amount_rupees"],
            currency=result["currency"],
            key_id=result["key_id"],
        )

    async def _create_bio_ai_bookings_without_payment(
        self,
        db: AsyncSession,
        *,
        members: list,
        ip_address: str,
        user_agent: str,
        endpoint: str,
        audit_user_id: int,
        audit_action: str,
    ) -> UserOnboardResponse:
        """Create confirmed ``bookings`` rows, increment diagnostic counts, run Bio AI fulfillment — no Razorpay or ``orders``."""

        from modules.diagnostics.models import DiagnosticPackage
        from modules.payments.models import Booking
        from modules.payments.services import _resolve_line_pricing

        onboard: UserOnboardResponse | None = None

        try:
            for m in members:
                row = await self._repository.get_user_by_id(db, m.user_id)
                if row is None:
                    raise AppError(status_code=400, error_code="INVALID_INPUT", message="User not found for line")

                resolved = await _resolve_line_pricing(
                    db, entity_type="diagnostic_package", entity_id=m.diagnostic_package_id
                )
                if isinstance(resolved, dict) and resolved.get("_error"):
                    code, msg = resolved["_error"]
                    raise AppError(status_code=code, error_code="PAYMENT_ERROR", message=msg)
                line_paise, entity_name = resolved

                meta = {
                    "address": m.address,
                    "pincode": m.pincode,
                    "city": m.city,
                    "blood_collection_date": m.blood_collection_date.isoformat(),
                    "blood_collection_time_slot": m.blood_collection_time_slot,
                    "diagnostic_package_id": m.diagnostic_package_id,
                }

                booking = Booking(
                    user_id=m.user_id,
                    entity_type="diagnostic_package",
                    entity_id=m.diagnostic_package_id,
                    entity_name=entity_name,
                    booking_type="bio_ai",
                    metadata_=meta,
                    amount_paise=line_paise,
                    currency="INR",
                    status="confirmed",
                )
                db.add(booking)
                await db.flush()

                await db.execute(
                    update(DiagnosticPackage)
                    .where(DiagnosticPackage.diagnostic_package_id == booking.entity_id)
                    .values(bookings_count=DiagnosticPackage.bookings_count + 1)
                )

                onboard = await self.fulfill_bio_ai_booking(
                    db, booking=booking, booked_by_user_id=audit_user_id
                )
                if onboard is None:
                    raise AppError(
                        status_code=500,
                        error_code="FULFILLMENT_FAILED",
                        message="Bio AI fulfillment could not complete",
                    )

            if self._audit_service is None:
                raise RuntimeError("Audit service is required")
            await self._audit_service.log_event(
                db,
                action=audit_action,
                endpoint=endpoint,
                ip_address=ip_address,
                user_agent=user_agent,
                user_id=audit_user_id,
                session_id=None,
            )

            await db.commit()
        except Exception:
            await db.rollback()
            raise

        assert onboard is not None
        return onboard

    async def book_bio_ai_batch_for_primary(
        self,
        db: AsyncSession,
        *,
        actor: User,
        payload: BookBioAiBatchRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> BookingPaymentResponse:
        return await self._create_bookings_for_batch(
            db,
            actor=actor,
            members=payload.members,
            booking_type="bio_ai",
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
        )

    async def book_blood_test_batch_for_primary(
        self,
        db: AsyncSession,
        *,
        actor: User,
        payload: BookBloodTestBatchRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> BookingPaymentResponse:
        return await self._create_bookings_for_batch(
            db,
            actor=actor,
            members=payload.members,
            booking_type="blood_test",
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
        )

    async def fulfill_bio_ai_booking(
        self,
        db: AsyncSession,
        *,
        booking,
        booked_by_user_id: int | None = None,
    ) -> UserOnboardResponse | None:
        """Post-payment fulfillment for a bio_ai booking.

        Returns the same shape as public B2C onboard when successful; ``None`` if the booking user is missing.
        """
        if self._engagements_service is None:
            raise RuntimeError("Engagements service is required")
        if self._platform_settings_service is None:
            raise RuntimeError("Platform settings service is required")
        if self._assessments_service is None:
            raise RuntimeError("Assessments service is required")

        meta = booking.metadata_ or {}
        user = await self._repository.get_user_by_id(db, booking.user_id)
        if user is None:
            logger.warning("fulfill_bio_ai_booking: user_id=%s not found", booking.user_id)
            return None

        if not user.is_participant:
            await self._repository.update_user_partial(db, user.user_id, {"is_participant": True})
            user.is_participant = True

        assessment_package_id, default_diagnostic_id = (
            await self._platform_settings_service.resolve_b2c_default_package_ids(db)
        )
        diagnostic_package_id = meta.get("diagnostic_package_id") or default_diagnostic_id
        await self._platform_settings_service.ensure_active_b2c_packages(db, assessment_package_id, diagnostic_package_id)

        eng_city = (meta.get("city") or "").strip() or user.city
        resolved_type_id, resolved_consultations = await _resolve_consultation_from_pkg(
            db, diagnostic_package_id, "bio_ai",
        )
        engagement = await self._engagements_service.create_b2c_engagement(
            db,
            user_first_name=user.first_name,
            engagement_date=date.fromisoformat(meta["blood_collection_date"]),
            city=eng_city,
            assessment_package_id=assessment_package_id,
            diagnostic_package_id=diagnostic_package_id,
            engagement_type=resolved_type_id,
            consultations=resolved_consultations,
            address=meta.get("address"),
            sub_locality=meta.get("sub_locality"),
            landmark=meta.get("landmark"),
            pincode=meta.get("pincode"),
            state=meta.get("state"),
            country=meta.get("country"),
            latitude=meta.get("latitude"),
            longitude=meta.get("longitude"),
            create_profile_on_metsights=True,
            enroll_for_fitprint_full=True,
        )

        slot_start = self._parse_time_slot(meta["blood_collection_time_slot"])
        booked_by = booked_by_user_id if booked_by_user_id is not None else user.user_id
        time_slot = await self._engagements_service.enroll_user_in_engagement(
            db,
            engagement=engagement,
            user_id=user.user_id,
            engagement_date=date.fromisoformat(meta["blood_collection_date"]),
            slot_start_time=slot_start,
            booked_by_user_id=booked_by,
        )

        await self._ensure_metsights_profile_id(db, user=user)
        fresh_user = await self._repository.get_user_by_id(db, user.user_id)
        profile_id = (fresh_user.metsights_profile_id or "").strip() if fresh_user else ""

        ip_address = "system"
        user_agent = "post-payment-fulfillment"
        endpoint = "/payments/verify"

        if profile_id and self._metsights_service is not None:
            try:
                await release_request_transaction(db)
                fitprint_package = await self._assessments_service.get_package_by_assessment_type_code(
                    db,
                    assessment_type_code="7",
                )
                fitprint_sub = (
                    (getattr(fitprint_package, "subscription_id", None) or "").strip() if fitprint_package else ""
                )
                if not fitprint_sub:
                    raise RuntimeError("FitPrint package is missing MetSights subscription_id")
                fitprint_record_id = await self._metsights_service.create_record_for_profile(
                    profile_id=profile_id,
                    subscription_id=fitprint_sub,
                )
                if fitprint_package is not None and fitprint_record_id:
                    await self._assessments_service.ensure_instance_assigned(
                        db,
                        user_id=user.user_id,
                        engagement_id=engagement.engagement_id,
                        package_id=int(fitprint_package.package_id),
                        ip_address=ip_address,
                        user_agent=user_agent,
                        endpoint=endpoint,
                        metsights_record_id=fitprint_record_id,
                    )
            except Exception as exc:
                logger.warning(
                    "Metsights FitPrint record creation failed for user_id=%s: %s",
                    user.user_id, str(exc),
                )

        metsights_record_id: str | None = None
        if profile_id and self._metsights_service is not None:
            try:
                package = await self._assessments_service.get_package_by_id(db, assessment_package_id)
                subscription_id = (getattr(package, "subscription_id", None) or "").strip() if package else ""
                if subscription_id:
                    await release_request_transaction(db)
                    metsights_record_id = await self._metsights_service.create_record_for_profile(
                        profile_id=profile_id,
                        subscription_id=subscription_id,
                    )
            except Exception as exc:
                logger.warning(
                    "Metsights primary record creation failed for user_id=%s: %s",
                    user.user_id, str(exc),
                )

        ap_id = engagement.assessment_package_id
        primary_instance = None
        if ap_id is not None:
            primary_instance = await self._assessments_service.ensure_instance_assigned(
                db,
                user_id=user.user_id,
                engagement_id=engagement.engagement_id,
                package_id=int(ap_id),
                ip_address=ip_address,
                user_agent=user_agent,
                endpoint=endpoint,
                metsights_record_id=metsights_record_id,
            )

        assessment_instance_id = (
            int(primary_instance.assessment_instance_id) if primary_instance is not None else None
        )
        mid_out: str | None = None
        if primary_instance is not None:
            mid_out = (primary_instance.metsights_record_id or "").strip() or None
        if not mid_out and metsights_record_id:
            mid_out = (metsights_record_id or "").strip() or None

        await self._notify_onboarding_assistants_for_user(
            db,
            engagement=engagement,
            user=user,
            source=engagement.engagement_code or "payment-fulfillment",
            collection_date=str(meta.get("blood_collection_date") or ""),
            collection_time=str(meta.get("blood_collection_time_slot") or ""),
        )

        return UserOnboardResponse(
            user_id=user.user_id,
            created=False,
            is_participant=True,
            engagement_id=int(engagement.engagement_id),
            engagement_code=engagement.engagement_code,
            engagement_participant_id=int(time_slot.engagement_participant_id),
            assessment_instance_id=assessment_instance_id,
            metsights_record_id=mid_out,
        )

    async def fulfill_blood_test_booking(
        self,
        db: AsyncSession,
        *,
        booking,
        booked_by_user_id: int | None = None,
    ) -> dict[str, int | str] | None:
        """Post-payment fulfillment for a blood_test (diagnostic-only) booking."""
        if self._engagements_service is None:
            raise RuntimeError("Engagements service is required")
        if self._platform_settings_service is None:
            raise RuntimeError("Platform settings service is required")

        meta = booking.metadata_ or {}

        if not meta.get("blood_collection_date") or not meta.get("blood_collection_time_slot"):
            logger.warning(
                "fulfill_blood_test_booking: booking_id=%s is missing blood_collection_date "
                "or blood_collection_time_slot in metadata",
                getattr(booking, "booking_id", "unknown"),
            )
            return None

        user = await self._repository.get_user_by_id(db, booking.user_id)
        if user is None:
            logger.warning("fulfill_blood_test_booking: user_id=%s not found", booking.user_id)
            return None

        if not user.is_participant:
            await self._repository.update_user_partial(db, user.user_id, {"is_participant": True})
            user.is_participant = True

        diagnostic_package_id = meta.get("diagnostic_package_id") or booking.entity_id
        await self._platform_settings_service.ensure_active_diagnostic_package(db, diagnostic_package_id)

        resolved_type_id, resolved_consultations = await _resolve_consultation_from_pkg(
            db, diagnostic_package_id, "blood_test",
        )
        engagement = await self._engagements_service.create_b2c_engagement(
            db,
            user_first_name=user.first_name,
            engagement_date=date.fromisoformat(meta["blood_collection_date"]),
            city=meta.get("city") or user.city,
            assessment_package_id=None,
            diagnostic_package_id=diagnostic_package_id,
            engagement_type=resolved_type_id,
            consultations=resolved_consultations,
            address=meta.get("address"),
            sub_locality=meta.get("sub_locality"),
            landmark=meta.get("landmark"),
            pincode=meta.get("pincode"),
            state=meta.get("state"),
            country=meta.get("country"),
            latitude=meta.get("latitude"),
            longitude=meta.get("longitude"),
            create_profile_on_metsights=False,
            enroll_for_fitprint_full=False,
        )

        slot_start = self._parse_time_slot(meta["blood_collection_time_slot"])
        booked_by = booked_by_user_id if booked_by_user_id is not None else user.user_id
        time_slot = await self._engagements_service.enroll_user_in_engagement(
            db,
            engagement=engagement,
            user_id=user.user_id,
            engagement_date=date.fromisoformat(meta["blood_collection_date"]),
            slot_start_time=slot_start,
            booked_by_user_id=booked_by,
        )

        await self._notify_onboarding_assistants_for_user(
            db,
            engagement=engagement,
            user=user,
            source=engagement.engagement_code or "payment-fulfillment",
            collection_date=str(meta.get("blood_collection_date") or ""),
            collection_time=str(meta.get("blood_collection_time_slot") or ""),
        )

        return {
            "engagement_id": int(engagement.engagement_id),
            "engagement_participant_id": int(time_slot.engagement_participant_id),
            "booking_type": "blood_test",
        }

    def _create_metsights_profile(self, user: User) -> None:
        """Legacy placeholder.

        This module does not call external services during onboarding.
        """

        _ = user
        return None

    def _to_metsights_gender(self, raw: str | None) -> str | None:
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

    async def _ensure_metsights_profile_id(
        self,
        db: AsyncSession,
        *,
        user: User,
        sync_context: MetsightsSyncContext | None = None,
    ) -> None:
        if user.metsights_profile_id:
            return
        if self._metsights_service is None:
            return

        first_name = (user.first_name or "").strip()
        last_name = outbound_last_name(user.last_name)
        phone = (user.phone or "").strip()
        gender = self._to_metsights_gender(user.gender)

        if not first_name or not phone or gender is None:
            return

        dob = user.date_of_birth.isoformat() if user.date_of_birth is not None else None
        email = (user.email or "").strip() if user.email else None
        normalized_phone = self._normalize_phone_for_metsights(phone)
        candidate_phones = [phone]
        if normalized_phone and normalized_phone != phone:
            candidate_phones.insert(0, normalized_phone)

        profile_id: str | None = None
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
                    sync_context=sync_context,
                )
                if profile_id:
                    break
            except AppError as exc:
                logger.warning(
                    "Metsights profile sync failed for user_id=%s phone=%s: %s",
                    user.user_id,
                    candidate_phone,
                    exc.message,
                )
            except Exception as exc:
                logger.warning(
                    "Metsights profile sync crashed for user_id=%s phone=%s: %s",
                    user.user_id,
                    candidate_phone,
                    str(exc),
                )

        if not profile_id:
            # Non-blocking: user creation/onboarding should not fail on external integration.
            return

        await self._repository.update_user_partial(db, user.user_id, {"metsights_profile_id": profile_id})

    async def _create_metsights_profile_for_engagement(
        self,
        db: AsyncSession,
        *,
        user: User,
        engagement,
        sync_context: MetsightsSyncContext | None = None,
    ) -> str | None:
        if self._metsights_service is None:
            return None
        if not bool(engagement.create_profile_on_metsights):
            return (user.metsights_profile_id or "").strip() or None

        engagement_metsights_id = (engagement.metsights_engagement_id or "").strip()
        first_name = (user.first_name or "").strip()
        last_name = outbound_last_name(user.last_name)
        phone = self._normalize_phone_for_metsights(user.phone)
        gender = self._to_metsights_gender(user.gender)
        if not first_name or not phone or gender is None:
            logger.warning(
                "Metsights profile creation skipped for user_id=%s engagement_id=%s: missing required user fields",
                user.user_id,
                engagement.engagement_id,
            )
            return (user.metsights_profile_id or "").strip() or None

        dob = user.date_of_birth.isoformat() if user.date_of_birth is not None else None
        email = (user.email or "").strip() if user.email else None

        if engagement_metsights_id:
            profile_id = await self._metsights_service.create_profile_for_engagement(
                engagement_id=engagement_metsights_id,
                first_name=first_name,
                last_name=last_name,
                phone=phone,
                email=email,
                gender=gender,
                date_of_birth=dob,
                age=user.age,
                sync_context=sync_context,
            )
        else:
            profile_id = await self._metsights_service.get_or_create_profile_id(
                first_name=first_name,
                last_name=last_name,
                phone=phone,
                email=email,
                gender=gender,
                date_of_birth=dob,
                age=user.age,
                sync_context=sync_context,
            )

        await self._repository.update_user_partial(db, user.user_id, {"metsights_profile_id": profile_id})
        user.metsights_profile_id = profile_id
        return profile_id

    async def create_user_by_employee(
        self,
        db: AsyncSession,
        *,
        employee=None,
        payload: EmployeeCreateUserRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> tuple[User, bool]:
        existing_by_phone = await self.resolve_user_by_phone(db, payload.phone)
        if existing_by_phone is not None:
            return existing_by_phone, False

        if payload.email is not None:
            existing_email = await self._repository.get_user_by_email(db, str(payload.email))
            if existing_email is not None:
                raise AppError(
                    status_code=409,
                    error_code="CONFLICT",
                    message="User already exists",
                    details={"user_id": int(existing_email.user_id)},
                )

        user = User(
            first_name=payload.first_name,
            last_name=payload.last_name,
            age=payload.age,
            phone=payload.phone,
            email=str(payload.email) if payload.email is not None else None,
            profile_photo=payload.profile_photo,
            date_of_birth=payload.date_of_birth,
            gender=payload.gender,
            address=payload.address,
            pin_code=payload.pin_code,
            city=payload.city,
            state=payload.state,
            country=payload.country,
            referred_by=payload.referred_by,
            is_participant=payload.is_participant,
            status=(payload.status or "active"),
        )

        created = await self._repository.create_user(db, user)
        await self._ensure_metsights_profile_id(db, user=created)

        if self._audit_service is None:
            raise RuntimeError("Audit service is required")

        actor_user_id = None
        action = "EMPLOYEE_CREATE_USER" if employee is not None else "PUBLIC_CREATE_USER"
        await self._audit_service.log_event(
            db,
            action=action,
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=actor_user_id,
            session_id=None,
        )

        return created, True

    async def list_superclub_playlists_for_employee(
        self,
        db: AsyncSession,
        *,
        employee,
        page: int,
        limit: int,
        search: str | None = None,
        sport_ids: list[str] | None = None,
        other_selected: bool | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
    ) -> tuple[list[tuple[User, UserPreference]], int, dict]:
        from modules.users.superclub_catalog import (
            SUPERCLUB_SPORT_CATALOG,
            has_submitted_playlist,
            normalize_sports_playlists,
        )

        self._ensure_employee_access(employee)

        rows = await self._repository.list_superclub_playlists(
            db,
            page=page,
            limit=limit,
            search=search,
            sport_ids=sport_ids,
            other_selected=other_selected,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
        total = await self._repository.count_superclub_playlists(
            db,
            search=search,
            sport_ids=sport_ids,
            other_selected=other_selected,
        )

        facet_payloads = await self._repository.list_superclub_playlist_payloads_for_search(
            db, search=search
        )
        sport_counts: dict[str, int] = {}
        other_count = 0
        total_sport_picks = 0
        playlists_submitted = 0
        for raw in facet_payloads:
            normalized = normalize_sports_playlists(raw)
            if not has_submitted_playlist(normalized):
                continue
            playlists_submitted += 1
            if normalized["otherSelected"]:
                other_count += 1
                sport_counts["other"] = sport_counts.get("other", 0) + 1
            for sport_id in normalized["sportIds"]:
                sport_counts[str(sport_id)] = sport_counts.get(str(sport_id), 0) + 1
                total_sport_picks += 1

        avg_sports = round(total_sport_picks / playlists_submitted, 1) if playlists_submitted else 0.0
        meta = {
            "playlists_submitted": playlists_submitted,
            "other_count": other_count,
            "avg_sports": avg_sports,
            "sport_catalog": SUPERCLUB_SPORT_CATALOG,
            "sport_counts": sport_counts,
        }
        return rows, total, meta

    async def list_users_for_employee(
        self,
        db: AsyncSession,
        *,
        employee,
        page: int,
        limit: int,
        phone: str | None,
        email: str | None,
        status: str | None,
        is_participant: bool | None,
        search: str | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
    ) -> tuple[list[User], int]:
        self._ensure_employee_access(employee)

        users = await self._repository.list_users(
            db,
            page=page,
            limit=limit,
            phone=phone,
            email=email,
            status=status,
            is_participant=is_participant,
            search=search,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )

        total = await self._repository.count_users(
            db,
            phone=phone,
            email=email,
            status=status,
            is_participant=is_participant,
            search=search,
        )

        return users, total

    async def get_participant_metsights_counts_for_employee(self, db: AsyncSession, *, employee) -> dict:
        self._ensure_employee_access(employee)
        with_profile, total_participants = await self._repository.count_participant_metsights_stats(db)
        return {
            "with_metsights_profile": with_profile,
            "total_participants": total_participants,
        }

    async def get_participant_metsights_stats_for_employee(self, db: AsyncSession, *, employee) -> dict:
        counts = await self.get_participant_metsights_counts_for_employee(db, employee=employee)
        year_counts = await self._repository.count_users_created_by_year(db)
        yearly_totals: list[dict] = []
        if year_counts:
            by_year = dict(year_counts)
            running = 0
            for year in range(min(by_year), max(by_year) + 1):
                added = by_year.get(year, 0)
                running += added
                yearly_totals.append({"year": year, "new_users": added, "total_users": running})
        return {**counts, "yearly_totals": yearly_totals}

    async def list_duplicate_phone_users_for_employee(self, db: AsyncSession, *, employee) -> list[list[User]]:
        self._ensure_employee_access(employee)
        return await self._repository.list_duplicate_phone_groups(db)

    async def get_user_details_for_employee(self, db: AsyncSession, *, employee, user_id: int) -> User:
        self._ensure_employee_access(employee)

        user = await self._repository.get_user_by_id(db, user_id)
        if user is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

        return user

    async def update_user_by_employee(
        self,
        db: AsyncSession,
        *,
        employee,
        user_id: int,
        payload: EmployeeUpdateUserRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> User:
        self._ensure_employee_access(employee)

        user = await self._repository.get_user_by_id(db, user_id)
        if user is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

        if await self._is_protected_employee_user(db, user_id):
            if (payload.status or "").strip().lower() != "active":
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="User must remain active")

        phone_in = (payload.phone or "").strip()
        email_in = str(payload.email).strip() if payload.email is not None else None
        if looks_masked(phone_in):
            phone_in = (user.phone or "").strip()
        if email_in is not None and looks_masked(email_in):
            email_in = (user.email or "").strip() or None

        if phone_in != (user.phone or "").strip():
            if await self._has_phone_conflict(db, phone=phone_in, exclude_user_id=int(user_id)):
                raise AppError(status_code=409, error_code="CONFLICT", message="User already exists")

        if email_in is not None:
            existing_email = await self._repository.get_user_by_email(db, email_in)
            if existing_email is not None and existing_email.user_id != user_id:
                raise AppError(status_code=409, error_code="CONFLICT", message="User already exists")

        data = payload.model_dump()
        data["phone"] = phone_in
        data["email"] = email_in

        updated = await self._repository.update_user_full(db, user=user, data=data)
        await self._sync_default_address_from_user(db, updated)

        if self._audit_service is None:
            raise RuntimeError("Audit service is required")

        await self._audit_service.log_event(
            db,
            action="EMPLOYEE_UPDATE_USER",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )

        return updated

    async def update_metsights_profile_id_by_employee(
        self,
        db: AsyncSession,
        *,
        employee,
        user_id: int,
        metsights_profile_id: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> User:
        self._ensure_employee_access(employee)

        user = await self._repository.get_user_by_id(db, user_id)
        if user is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

        normalized = (metsights_profile_id or "").strip()
        value: str | None = normalized if normalized else None

        if value is not None:
            existing = await self._repository.get_user_by_metsights_profile_id(db, value)
            if existing is not None and int(existing.user_id) != int(user_id):
                raise AppError(
                    status_code=409,
                    error_code="CONFLICT",
                    message="Another user already has this metsights_profile_id",
                )

        updated = await self._repository.update_user_partial(
            db, user_id, {"metsights_profile_id": value}
        )
        if updated is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

        if self._audit_service is None:
            raise RuntimeError("Audit service is required")

        await self._audit_service.log_event(
            db,
            action="EMPLOYEE_UPDATE_USER_METSIGHTS_PROFILE_ID",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )

        return updated

    async def retry_metsights_profile_by_employee(
        self,
        db: AsyncSession,
        *,
        employee,
        user_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> User:
        self._ensure_employee_access(employee)

        user = await self._repository.get_user_by_id(db, user_id)
        if user is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

        existing_id = (user.metsights_profile_id or "").strip()
        if existing_id:
            return user

        if self._metsights_service is None:
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message="Metsights integration is not configured",
            )

        first_name = (user.first_name or "").strip()
        last_name = outbound_last_name(user.last_name)
        phone = (user.phone or "").strip()
        gender = self._to_metsights_gender(user.gender)
        if not first_name or not phone or gender is None:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="User must have first name, phone, and gender before creating a Metsights profile",
            )

        dob = user.date_of_birth.isoformat() if user.date_of_birth is not None else None
        if not dob and user.age is None:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="User must have date of birth or age before creating a Metsights profile",
            )

        email = (user.email or "").strip() if user.email else None
        normalized_phone = self._normalize_phone_for_metsights(phone)
        candidate_phones = [phone]
        if normalized_phone and normalized_phone != phone:
            candidate_phones.insert(0, normalized_phone)

        profile_id: str | None = None
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
                raise
            except Exception as exc:
                last_error = str(exc)

        if not profile_id:
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message=last_error or "Metsights profile creation failed",
            )

        updated = await self._repository.update_user_partial(
            db, user_id, {"metsights_profile_id": profile_id}
        )
        if updated is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

        if self._audit_service is None:
            raise RuntimeError("Audit service is required")

        await self._audit_service.log_event(
            db,
            action="EMPLOYEE_RETRY_METSIGHTS_PROFILE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )

        return updated

    async def deactivate_user_by_employee(
        self,
        db: AsyncSession,
        *,
        employee,
        user_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> User:
        self._ensure_employee_access(employee)

        user = await self._repository.get_user_by_id(db, user_id)
        if user is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

        if await self._is_protected_employee_user(db, user_id):
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="User must remain active")

        updated = await self._repository.update_user_full(db, user=user, data={"status": "inactive"})

        if self._audit_service is None:
            raise RuntimeError("Audit service is required")

        await self._audit_service.log_event(
            db,
            action="EMPLOYEE_DEACTIVATE_USER",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )

        return updated

    async def _resolve_user_ids_for_employee_delete(
        self,
        db: AsyncSession,
        *,
        employee,
        user_id: int,
    ) -> list[int]:
        self._ensure_employee_access(employee)

        user = await self._repository.get_user_by_id(db, user_id)
        if user is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

        user_ids_to_delete = await self._repository.list_descendant_user_ids(db, user_id)

        for candidate_user_id in user_ids_to_delete:
            if await self._is_protected_employee_user(db, candidate_user_id):
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="User cannot be deleted")

        return user_ids_to_delete

    async def get_delete_user_impact_by_employee(
        self,
        db: AsyncSession,
        *,
        employee,
        user_id: int,
    ) -> dict:
        user_ids_to_delete = await self._resolve_user_ids_for_employee_delete(
            db, employee=employee, user_id=user_id
        )
        engagements = await self._repository.list_engagements_fully_owned_by_users(
            db, user_ids_to_delete
        )
        return {
            "engagements_to_orphan": [
                {
                    "engagement_id": e.engagement_id,
                    "engagement_code": e.engagement_code,
                    "engagement_name": e.engagement_name,
                }
                for e in engagements
            ]
        }

    async def delete_user_by_employee(
        self,
        db: AsyncSession,
        *,
        employee,
        user_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
        delete_orphan_engagements: bool = False,
    ) -> dict:
        user_ids_to_delete = await self._resolve_user_ids_for_employee_delete(
            db, employee=employee, user_id=user_id
        )

        await self._repository.delete_user_related_data(
            db,
            user_ids_to_delete,
            delete_orphan_engagements=delete_orphan_engagements,
        )
        deleted_count = await self._repository.delete_users_by_ids(db, user_ids_to_delete)

        if self._audit_service is None:
            raise RuntimeError("Audit service is required")

        await self._audit_service.log_event(
            db,
            action="EMPLOYEE_DELETE_USER",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )

        return {"deleted_user_id": user_id, "deleted_user_count": deleted_count}

    async def _allocate_plus_alias_email(self, db: AsyncSession, base_email: str) -> str:
        """Allocate a free `{local}+{1000-9999}@{domain}` alias derived from *base_email*."""
        if not base_email or "@" not in base_email:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        local_part, domain = base_email.split("@", 1)
        for _ in range(200):
            suffix = random.randint(1000, 9999)
            candidate = f"{local_part}+{suffix}@{domain}"
            existing = await self._repository.get_user_by_email(db, candidate)
            if existing is None:
                return candidate

        raise AppError(status_code=409, error_code="CONFLICT", message="Unable to create profile")

    async def _apply_email_if_different(
        self,
        db: AsyncSession,
        user: User,
        email: str | None,
    ) -> User:
        """Set *user*'s email when *email* differs (case-insensitive). Family-onboard only."""

        candidate = (email or "").strip()
        if not candidate or "@" not in candidate:
            return user

        existing = (user.email or "").strip()
        if existing.lower() == candidate.lower():
            return user

        owner = await self._repository.get_user_by_email(db, candidate)
        if owner is not None and int(owner.user_id) != int(user.user_id):
            if owner.parent_id is not None and int(owner.parent_id) == int(user.user_id):
                try:
                    alias = await self._allocate_plus_alias_email(db, candidate)
                    await self._repository.update_user_partial(
                        db, int(owner.user_id), {"email": alias}
                    )
                except IntegrityError as exc:
                    raise AppError(
                        status_code=409,
                        error_code="CONFLICT",
                        message=_integrity_error_reason(exc),
                    ) from exc
            else:
                raise AppError(
                    status_code=409,
                    error_code="CONFLICT",
                    message="Email already belongs to another user",
                )

        try:
            updated = await self._repository.update_user_partial(
                db, int(user.user_id), {"email": candidate}
            )
        except IntegrityError as exc:
            raise AppError(
                status_code=409,
                error_code="CONFLICT",
                message=_integrity_error_reason(exc),
            ) from exc
        return updated if updated is not None else user

    async def _get_or_create_user_for_onboarding(
        self,
        db: AsyncSession,
        *,
        phone: str,
        email: str | None,
        patch_data: dict,
        create_data: dict,
    ) -> tuple[User, bool]:
        existing = await self.resolve_user_by_phone(db, phone)
        if existing is None and email is not None:
            existing = await self._repository.get_user_by_email(db, email)

        if existing is None:
            user = User(**create_data)
            user = await self._repository.create_user(db, user)
            return user, True

        user = await self._repository.patch_missing_fields(db, user=existing, data=patch_data)
        return user, False

    async def _resolve_blood_cabin_name_for_notify(
        self,
        db: AsyncSession,
        *,
        engagement,
        collection_date: date | str | None,
        cabin_key: str | None,
    ) -> str | None:
        if self._engagements_service is None:
            return (cabin_key or "").strip() or None
        key = (cabin_key or "").strip() or None
        if not key or collection_date is None:
            return key
        try:
            parsed_date = (
                collection_date
                if isinstance(collection_date, date)
                else date.fromisoformat(str(collection_date).strip())
            )
        except ValueError:
            return key
        from modules.engagements.slot_availability import resolve_blood_collection_cabin_display_name

        slot_detail = await self._engagements_service.resolve_slot_detail(db, engagement)
        return resolve_blood_collection_cabin_display_name(
            slot_detail,
            collection_date=parsed_date,
            cabin_key=key,
        )

    @staticmethod
    def _participant_details_from_onboard_payload(
        payload: PublicUserOnboardRequest | EngagementUserOnboardRequest,
        *,
        source: str,
        participant_user_id: int,
        cabin_name: str | None = None,
    ) -> dict[str, str]:
        from modules.notifications.onboarding_notify import detail_or_hyphen

        first_name = payload.first_name or ""
        last_name = payload.last_name or ""
        name = f"{first_name} {last_name}".strip()
        return {
            "name": detail_or_hyphen(name),
            "email": detail_or_hyphen(payload.email),
            "phone": detail_or_hyphen(payload.phone),
            "age": detail_or_hyphen(payload.age),
            "gender": detail_or_hyphen(payload.gender),
            "address": detail_or_hyphen(payload.address),
            "pincode": detail_or_hyphen(payload.pincode),
            "collection_date": detail_or_hyphen(payload.blood_collection_date),
            "collection_time": detail_or_hyphen(payload.blood_collection_time_slot),
            "cabin_name": detail_or_hyphen(cabin_name),
            "engagement": detail_or_hyphen(source),
            "participant_user_id": str(participant_user_id),
        }

    async def _notify_onboarding_assistants(
        self,
        db: AsyncSession,
        *,
        engagement,
        participant_user_id: int,
        payload: PublicUserOnboardRequest | EngagementUserOnboardRequest,
        source: str,
        cabin_key: str | None = None,
    ) -> None:
        """Fire-and-forget: notify onboarding assistants using the engagement's service key."""
        if self._notifications_service is None or self._engagements_service is None:
            return

        from modules.notifications.onboarding_notify import notify_onboarding_assistants_on_enrollment

        resolved_cabin_key = cabin_key
        if resolved_cabin_key is None:
            resolved_cabin_key = getattr(payload, "blood_collection_cabin", None)
        cabin_name = await self._resolve_blood_cabin_name_for_notify(
            db,
            engagement=engagement,
            collection_date=getattr(payload, "blood_collection_date", None),
            cabin_key=resolved_cabin_key,
        )
        participant_details = self._participant_details_from_onboard_payload(
            payload,
            source=source,
            participant_user_id=participant_user_id,
            cabin_name=cabin_name,
        )
        await notify_onboarding_assistants_on_enrollment(
            db,
            notifications_service=self._notifications_service,
            notifications_repository=self._notifications_service._repo,
            engagements_repository=self._engagements_service._repository,
            engagement=engagement,
            participant_user_id=participant_user_id,
            participant_details=participant_details,
        )

    async def _notify_onboarding_assistants_for_user(
        self,
        db: AsyncSession,
        *,
        engagement,
        user: User,
        source: str,
        collection_date: str | None = None,
        collection_time: str | None = None,
        cabin_key: str | None = None,
    ) -> None:
        if self._notifications_service is None or self._engagements_service is None:
            return

        from modules.notifications.onboarding_notify import (
            notify_onboarding_assistants_on_enrollment,
            participant_details_from_user,
        )

        cabin_name = await self._resolve_blood_cabin_name_for_notify(
            db,
            engagement=engagement,
            collection_date=collection_date,
            cabin_key=cabin_key,
        )
        participant_details = participant_details_from_user(
            user,
            source=source,
            participant_user_id=int(user.user_id),
            collection_date=collection_date,
            collection_time=collection_time,
            cabin_name=cabin_name,
        )
        await notify_onboarding_assistants_on_enrollment(
            db,
            notifications_service=self._notifications_service,
            notifications_repository=self._notifications_service._repo,
            engagements_repository=self._engagements_service._repository,
            engagement=engagement,
            participant_user_id=int(user.user_id),
            participant_details=participant_details,
        )

    async def public_onboard_user(
        self,
        db: AsyncSession,
        *,
        payload: PublicUserOnboardRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> UserOnboardResponse:
        """B2C onboarding.

        Creates a new engagement for this user.
        When ``payload.user_id`` is set, loads that user and skips personal-field create/patch.
        """

        onboarded_by_user_id = payload.user_id is not None

        if onboarded_by_user_id:
            user = await self._repository.get_user_by_id(db, int(payload.user_id))
            if user is None:
                raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")
            if not user.is_participant:
                user.is_participant = True
                db.add(user)
                await db.flush()
            created = False
            from modules.engagements.participant_location import (
                address_profile_update_from_onboard_payload,
                onboard_payload_has_address,
            )

            if onboard_payload_has_address(payload):
                profile_update = address_profile_update_from_onboard_payload(payload)
                if profile_update:
                    updated = await self._repository.update_user_partial(
                        db, user.user_id, profile_update
                    )
                    if updated is not None:
                        user = updated
                    await self._sync_default_address_from_user(db, user)
        else:
            email = str(payload.email) if payload.email is not None else None

            patch_data = {
                "first_name": payload.first_name,
                "last_name": payload.last_name,
                "age": payload.age,
                "email": email,
                "date_of_birth": payload.dob,
                "gender": payload.gender,
                "address": payload.address,
                "pin_code": payload.pincode,
                "city": payload.city,
                "state": payload.state,
                "country": payload.country,
            }

            create_data = {
                **patch_data,
                "phone": payload.phone,
                "referred_by": None,
                "is_participant": True,
                "status": "active",
            }

            user, created = await self._get_or_create_user_for_onboarding(
                db,
                phone=str(payload.phone),
                email=email,
                patch_data={**patch_data, "is_participant": True},
                create_data=create_data,
            )
            await self._sync_default_address_from_user(db, user)

        if self._engagements_service is None:
            raise RuntimeError("Engagements service is required")
        if self._platform_settings_service is None:
            raise RuntimeError("Platform settings service is required")

        engagement_type_code = await _resolve_onboard_engagement_type_code(
            self._platform_settings_service,
            db,
            payload.engagement_type,
        )
        await _require_active_engagement_type_code(db, engagement_type_code)
        _validate_onboard_blood_fields(
            engagement_type_code,
            blood_collection_date=payload.blood_collection_date,
            blood_collection_time_slot=payload.blood_collection_time_slot,
        )

        onboarding_defaults = await self._platform_settings_service.resolve_b2c_onboarding_defaults(
            db, engagement_type_code
        )
        assessment_package_id = onboarding_defaults.assessment_package_id
        diagnostic_package_id = resolve_b2c_diagnostic_package_id(
            onboarding_defaults,
            override_id=payload.diagnostic_package_id,
            gender=user.gender or payload.gender,
        )
        await self._platform_settings_service.ensure_active_b2c_packages(
            db, assessment_package_id, diagnostic_package_id
        )

        # Create a new engagement for this user.
        # B2C engagements auto-assign default onboarding assistants from platform settings.
        resolved_type_id, resolved_consultations = await _resolve_consultation_from_pkg(
            db, diagnostic_package_id, engagement_type_code,
        )

        from modules.engagements.enums import BloodCollectionType
        from modules.engagements.participant_location import (
            copy_engagement_location_to_participant,
            copy_onboard_payload_location_to_participant,
            is_home_blood_collection,
            onboard_payload_has_address,
        )

        bct = onboarding_defaults.blood_collection_type
        bct_val = (bct.value if hasattr(bct, "value") else str(bct or "")).strip().lower()
        onboard_home = bct_val == BloodCollectionType.home_collection.value

        if onboarded_by_user_id:
            engagement_city = payload.city if payload.city is not None else user.city
            if onboard_home:
                engagement_address = None
                engagement_pincode = None
                engagement_state = None
                engagement_country = None
            else:
                engagement_address = payload.address if payload.address is not None else user.address
                engagement_pincode = payload.pincode if payload.pincode is not None else user.pin_code
                engagement_state = payload.state if payload.state is not None else user.state
                engagement_country = payload.country if payload.country is not None else user.country
        else:
            engagement_city = payload.city or user.city
            if onboard_home:
                engagement_address = None
                engagement_pincode = None
                engagement_state = None
                engagement_country = None
            else:
                engagement_address = payload.address
                engagement_pincode = payload.pincode
                engagement_state = getattr(payload, "state", None)
                engagement_country = getattr(payload, "country", None)

        engagement = await self._engagements_service.create_b2c_engagement(
            db,
            user_first_name=user.first_name,
            engagement_date=payload.blood_collection_date or date.today(),
            city=engagement_city,
            assessment_package_id=assessment_package_id,
            diagnostic_package_id=diagnostic_package_id,
            engagement_type=resolved_type_id,
            blood_collection_type=onboarding_defaults.blood_collection_type,
            consultations=resolved_consultations,
            address=engagement_address,
            sub_locality=getattr(payload, "sub_locality", None) if not onboard_home else None,
            landmark=getattr(payload, "landmark", None) if not onboard_home else None,
            pincode=engagement_pincode,
            state=engagement_state,
            country=engagement_country,
            latitude=getattr(payload, "latitude", None) if not onboard_home else None,
            longitude=getattr(payload, "longitude", None) if not onboard_home else None,
            create_profile_on_metsights=onboarding_defaults.create_profile_on_metsights,
            enroll_for_fitprint_full=onboarding_defaults.enroll_for_fitprint_full,
            **b2c_engagement_create_kwargs_from_defaults(
                onboarding_defaults, resolved_consultations
            ),
        )

        consultations = _validate_requested_consultations(
            payload.consultations,
            engagement.consultations,
        )

        slot_start = (
            self._parse_time_slot(payload.blood_collection_time_slot)
            if payload.blood_collection_time_slot
            else None
        )
        time_slot = await self._engagements_service.enroll_user_in_engagement(
            db,
            engagement=engagement,
            user_id=user.user_id,
            engagement_date=payload.blood_collection_date,
            slot_start_time=slot_start,
            participants_employee_id=payload.participants_employee_id,
            participant_department=payload.participant_department,
            participant_blood_group=payload.participant_blood_group,
            consultations=consultations
        )
        if is_home_blood_collection(engagement):
            if onboard_payload_has_address(payload):
                copy_onboard_payload_location_to_participant(time_slot, payload)
        else:
            copy_engagement_location_to_participant(time_slot, engagement)
        db.add(time_slot)
        await db.flush()

        if self._assessments_service is None:
            raise RuntimeError("Assessments service is required")
        assessment_instance = await self._assessments_service.ensure_instance_assigned(
            db,
            user_id=user.user_id,
            engagement_id=engagement.engagement_id,
            package_id=engagement.assessment_package_id,
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
        )

        if payload.questionnaire and self._questionnaire_service is not None:
            questionnaire_payload: dict[str, list[dict]] = {}
            for category_key, category_payload in payload.questionnaire.items():
                questionnaire_payload[category_key] = [
                    {"question_id": item.question_id, "answer": item.normalized_answer()}
                    for item in category_payload.responses
                ]
            if any(questionnaire_payload.values()):
                await self._questionnaire_service.apply_onboard_questionnaire(
                    db,
                    user_id=int(user.user_id),
                    assessment_instance_id=int(assessment_instance.assessment_instance_id),
                    questionnaire=questionnaire_payload,
                    ip_address=ip_address,
                    user_agent=user_agent,
                    endpoint=endpoint,
                )
                # Re-load in case status was auto-completed.
                refreshed = await self._assessments_service.get_instance_by_id(
                    db, assessment_instance_id=int(assessment_instance.assessment_instance_id)
                )
                if refreshed is not None:
                    assessment_instance = refreshed

        if engagement.create_profile_on_metsights:
            await self._ensure_metsights_profile_id(db, user=user)

        fresh_user = await self._repository.get_user_by_id(db, user.user_id)
        profile_id = (fresh_user.metsights_profile_id or "").strip() if fresh_user else ""
        if profile_id and engagement.enroll_for_fitprint_full and self._metsights_service is not None:
            try:
                fitprint_package = await self._assessments_service.get_package_by_assessment_type_code(
                    db,
                    assessment_type_code="7",
                )
                if fitprint_package is None:
                    raise RuntimeError("Active FitPrint Full assessment package is missing")
                await release_request_transaction(db)
                fitprint_sub = (getattr(fitprint_package, "subscription_id", None) or "").strip()
                if not fitprint_sub:
                    raise RuntimeError("FitPrint package is missing MetSights subscription_id")
                fitprint_record_id = await self._metsights_service.create_record_for_profile(
                    profile_id=profile_id,
                    subscription_id=fitprint_sub,
                )
                await self._assessments_service.ensure_instance_assigned(
                    db,
                    user_id=user.user_id,
                    engagement_id=engagement.engagement_id,
                    package_id=int(fitprint_package.package_id),
                    ip_address=ip_address,
                    user_agent=user_agent,
                    endpoint=endpoint,
                    metsights_record_id=fitprint_record_id,
                )
            except Exception as exc:
                logger.warning(
                    "Metsights FitPrint enrollment failed for user_id=%s engagement_id=%s: %s",
                    user.user_id,
                    engagement.engagement_id,
                    str(exc),
                )

        # Create the primary Metsights record whenever the user has a profile.
        if (
            profile_id
            and self._metsights_service is not None
            and not (assessment_instance.metsights_record_id or "").strip()
        ):
            try:
                package = await self._assessments_service.get_package_by_id(db, engagement.assessment_package_id)
                subscription_id = (getattr(package, "subscription_id", None) or "").strip() if package else ""
                if subscription_id:
                    await release_request_transaction(db)
                    record_id = await self._metsights_service.create_record_for_profile(
                        profile_id=profile_id,
                        subscription_id=subscription_id,
                    )
                    assessment_instance = await self._assessments_service.ensure_instance_assigned(
                        db,
                        user_id=user.user_id,
                        engagement_id=engagement.engagement_id,
                        package_id=engagement.assessment_package_id,
                        ip_address=ip_address,
                        user_agent=user_agent,
                        endpoint=endpoint,
                        metsights_record_id=record_id,
                    )
            except Exception as exc:
                logger.warning(
                    "Metsights record creation failed for user_id=%s engagement_id=%s: %s",
                    user.user_id,
                    engagement.engagement_id,
                    str(exc),
                )

        # Record onboarding audit log.
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        await self._audit_service.log_event(
            db,
            action="USER_PUBLIC_ONBOARD",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user.user_id,
            session_id=None,
        )

        if onboarded_by_user_id:
            await self._notify_onboarding_assistants_for_user(
                db,
                engagement=engagement,
                user=user,
                source="public",
                collection_date=str(payload.blood_collection_date or ""),
                collection_time=str(payload.blood_collection_time_slot or ""),
            )
        else:
            await self._notify_onboarding_assistants(
                db,
                engagement=engagement,
                participant_user_id=int(user.user_id),
                payload=payload,
                source="public",
            )

        mid = (assessment_instance.metsights_record_id or "").strip() or None
        return UserOnboardResponse(
            user_id=user.user_id,
            created=created,
            is_participant=True,
            engagement_id=engagement.engagement_id,
            engagement_code=engagement.engagement_code,
            engagement_participant_id=time_slot.engagement_participant_id,
            assessment_instance_id=int(assessment_instance.assessment_instance_id),
            metsights_record_id=mid,
        )

    async def public_vifc_quick_start(
        self,
        db: AsyncSession,
        *,
        payload: VifcQuickStartRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> VifcQuickStartResponse:
        """B2C VIFC onboard followed by Aurae face-scan start."""

        if self._aurae_service is None:
            raise RuntimeError("Aurae service is required")

        onboard_payload = PublicUserOnboardRequest(
            user_id=payload.user_id,
            age=payload.age,
            first_name=payload.first_name,
            last_name=payload.last_name,
            email=payload.email,
            phone=payload.phone,
            gender=payload.gender,
            dob=payload.dob,
            address=payload.address,
            pincode=payload.pincode,
            city=payload.city,
            state=payload.state,
            country=payload.country,
            engagement_type="vifc",
            questionnaire=payload.questionnaire,
        )
        onboard = await self.public_onboard_user(
            db,
            payload=onboard_payload,
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
        )
        if onboard.assessment_instance_id is None:
            raise AppError(
                status_code=500,
                error_code="INTERNAL_ERROR",
                message="VIFC onboard did not create an assessment instance",
            )

        face = await self._aurae_service.start_face_scan(
            db,
            assessment_instance_id=int(onboard.assessment_instance_id),
            user_id=int(onboard.user_id),
        )
        link = face["link"]
        return VifcQuickStartResponse(
            user_id=onboard.user_id,
            created=onboard.created,
            is_participant=onboard.is_participant,
            engagement_id=onboard.engagement_id,
            engagement_code=onboard.engagement_code,
            engagement_participant_id=onboard.engagement_participant_id,
            assessment_instance_id=onboard.assessment_instance_id,
            metsights_record_id=onboard.metsights_record_id,
            face_scan_link=link,
        )

    async def book_bio_ai_for_user_without_payment(
        self,
        db: AsyncSession,
        *,
        current_user: User,
        target_user_id: int,
        employee: "EmployeeContext | None",
        payload: BookBioAiRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> UserOnboardResponse:
        """Bio AI booking for ``target_user_id``: confirmed booking + immediate fulfillment; no payment tables."""

        from modules.users.schemas import BookBioAiMemberPayload

        if employee is None:
            if current_user.parent_id is not None:
                raise AppError(
                    status_code=403,
                    error_code="FORBIDDEN",
                    message="Batch booking is only available for primary accounts",
                )
            target = await self._get_user_bookable_by_primary(
                db, primary_user_id=current_user.user_id, target_user_id=target_user_id
            )
        else:
            target = await self._repository.get_user_by_id(db, target_user_id)
            if target is None:
                raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

        if self._platform_settings_service is None:
            raise RuntimeError("Platform settings service is required")

        _, default_diagnostic_id = await self._platform_settings_service.resolve_b2c_default_package_ids(db)
        resolved_diagnostic = (
            payload.diagnostic_package_id if payload.diagnostic_package_id is not None else default_diagnostic_id
        )

        member = BookBioAiMemberPayload(
            user_id=target.user_id,
            address=payload.address or target.address or "N/A",
            pincode=payload.pincode or target.pin_code or "000000",
            city=payload.city or target.city or "N/A",
            blood_collection_date=payload.blood_collection_date,
            blood_collection_time_slot=payload.blood_collection_time_slot,
            diagnostic_package_id=resolved_diagnostic,
        )

        audit_action = "EMPLOYEE_BOOK_BIO_AI" if employee is not None else "USER_BOOK_BIO_AI"

        return await self._create_bio_ai_bookings_without_payment(
            db,
            members=[member],
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
            audit_user_id=current_user.user_id,
            audit_action=audit_action,
        )

    async def onboard_user_for_engagement(
        self,
        db: AsyncSession,
        *,
        engagement_code: str,
        payload: EngagementUserOnboardRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> UserOnboardResponse:
        """B2B onboarding.

        Links user to an existing engagement.
        """

        if self._engagements_service is None:
            raise RuntimeError("Engagements service is required")

        payload_code = (payload.referred_by or "").strip()
        path_code = (engagement_code or "").strip()
        code = payload_code or path_code
        if not code:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        engagement = await self._engagements_service.get_by_code(db, code)
        if engagement is None:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        slot_start = self._parse_time_slot(payload.blood_collection_time_slot)
        slot_start = time(hour=slot_start.hour, minute=slot_start.minute)
        blood_collection_cabin = await self._engagements_service.validate_blood_collection_slot_for_onboard(
            db,
            engagement=engagement,
            collection_date=payload.blood_collection_date,
            cabin_key=payload.blood_collection_cabin,
            slot_time=slot_start,
        )

        consultations = _validate_requested_consultations(
            payload.consultations,
            engagement.consultations,
        )
        consultations = await self._engagements_service.validate_consultation_slots_for_onboard(
            db,
            engagement=engagement,
            consultations=consultations,
        )

        email = str(payload.email) if payload.email is not None else None

        patch_data = {
            "first_name": payload.first_name,
            "last_name": payload.last_name,
            "age": payload.age,
            "email": email,
            "date_of_birth": payload.dob,
            "gender": payload.gender,
            "address": payload.address,
            "pin_code": payload.pincode,
            "city": payload.city,
            "state": payload.state,
            "country": payload.country,
        }

        create_data = {
            **patch_data,
            "phone": payload.phone,
            "referred_by": code,
            "is_participant": True,
            "status": "active",
        }

        user, created = await self._get_or_create_user_for_onboarding(
            db,
            phone=payload.phone,
            email=email,
            patch_data={**patch_data, "is_participant": True, "referred_by": code},
            create_data=create_data,
        )

        # Age, email, and address fields: always overwrite when provided (even if already set).
        # If email is owned by a sub-profile of this user, reclaim it (alias the sub first).
        # Unrelated owners → 409; IntegrityError → 409 (never raw 500).
        if email is not None:
            owner = await self._repository.get_user_by_email(db, email)
            if owner is not None and int(owner.user_id) != int(user.user_id):
                if owner.parent_id is not None and int(owner.parent_id) == int(user.user_id):
                    try:
                        alias = await self._allocate_plus_alias_email(db, email)
                        await self._repository.update_user_partial(
                            db, int(owner.user_id), {"email": alias}
                        )
                    except IntegrityError as exc:
                        raise AppError(
                            status_code=409,
                            error_code="CONFLICT",
                            message=_integrity_error_reason(exc),
                        ) from exc
                else:
                    raise AppError(
                        status_code=409,
                        error_code="CONFLICT",
                        message="Email already belongs to another user",
                    )

        profile_update = {
            key: value
            for key, value in {
                "age": payload.age,
                "email": email,
                "address": payload.address,
                "pin_code": payload.pincode,
                "city": payload.city,
                "state": payload.state,
                "country": payload.country,
            }.items()
            if value is not None
        }
        if profile_update:
            try:
                updated = await self._repository.update_user_partial(
                    db, user.user_id, profile_update
                )
            except IntegrityError as exc:
                raise AppError(
                    status_code=409,
                    error_code="CONFLICT",
                    message=_integrity_error_reason(exc),
                ) from exc
            if updated is not None:
                user = updated

        await self._sync_default_address_from_user(db, user)

        validated_department = await self._engagements_service.resolve_participant_department_for_engagement(
            db,
            engagement=engagement,
            participant_department=payload.participant_department,
        )
        from modules.engagements.enums import EngagementPaymentType
        from modules.engagements.participant_eligibility import engagement_payment_type_value

        payment_type = engagement_payment_type_value(engagement)
        requires_payment = payment_type == EngagementPaymentType.paid_by_patient.value

        time_slot = await self._engagements_service.enroll_user_in_engagement(
            db,
            engagement=engagement,
            user_id=user.user_id,
            engagement_date=payload.blood_collection_date,
            slot_start_time=slot_start,
            participants_employee_id=payload.participants_employee_id,
            participant_department=validated_department,
            participant_blood_group=payload.participant_blood_group,
            blood_collection_cabin=blood_collection_cabin,
            consultations=consultations,
            paid=False if requires_payment else True,
            booker_message=payload.booker_message,
            blood_collection_time_slot_id=payload.blood_collection_time_slot_id,
        )
        from modules.engagements.participant_location import (
            copy_onboard_payload_location_to_participant,
            is_home_blood_collection,
            onboard_payload_has_address,
        )

        if is_home_blood_collection(engagement) and onboard_payload_has_address(payload):
            copy_onboard_payload_location_to_participant(time_slot, payload)
            db.add(time_slot)
            await db.flush()

        if requires_payment:
            from modules.auth.dependencies import get_auth_service

            auth_service = get_auth_service()
            otp_session_id, delivery = await auth_service.send_otp_for_user_id(
                db,
                user_id=int(user.user_id),
                ip_address=ip_address,
                user_agent=user_agent,
                endpoint=endpoint,
            )
            if delivery is not None:
                await auth_service.deliver_otp_via_notifications(db, delivery=delivery)
            if self._audit_service is not None:
                await self._audit_service.log_event(
                    db,
                    action="USER_ENGAGEMENT_ONBOARD",
                    endpoint=endpoint,
                    ip_address=ip_address,
                    user_agent=user_agent,
                    user_id=user.user_id,
                    session_id=None,
                )
            return UserOnboardResponse(
                user_id=user.user_id,
                created=created,
                is_participant=True,
                engagement_id=engagement.engagement_id,
                engagement_code=engagement.engagement_code,
                engagement_participant_id=time_slot.engagement_participant_id,
                payment_required=True,
                otp_session_id=otp_session_id,
            )

        assessment_instance = None
        preview_available = False
        if self._assessments_service is None:
            raise RuntimeError("Assessments service is required")

        # Assign local assessment instances before any Metsights I/O that releases the
        # request transaction. Otherwise a later failure leaves console enrollment without
        # journey assessments.
        await self._engagements_service.ensure_assessment_instances_for_participant(
            db,
            engagement=engagement,
            user_id=int(user.user_id),
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
        )
        if engagement.assessment_package_id is not None:
            assessment_instance = await self._assessments_service.ensure_instance_assigned(
                db,
                user_id=user.user_id,
                engagement_id=engagement.engagement_id,
                package_id=engagement.assessment_package_id,
                ip_address=ip_address,
                user_agent=user_agent,
                endpoint=endpoint,
            )

        # Commit user/enrollment/assessments before Metsights I/O. integration_sync_logs
        # is written on an isolated session and FK-references users.user_id.
        await release_request_transaction(db)

        metsights_record_id: str | None = None
        sync_context = MetsightsSyncContext(
            db=db,
            engagement_id=int(engagement.engagement_id),
            user_id=int(user.user_id),
        )
        profile_id: str | None = None
        try:
            profile_id = await self._create_metsights_profile_for_engagement(
                db,
                user=user,
                engagement=engagement,
                sync_context=sync_context,
            )
            if profile_id and bool(engagement.enroll_for_fitprint_full):
                await release_request_transaction(db)
                fitprint_package = await self._assessments_service.get_package_by_assessment_type_code(
                    db,
                    assessment_type_code="7",
                )
                fitprint_sub = (
                    (getattr(fitprint_package, "subscription_id", None) or "").strip()
                    if fitprint_package is not None
                    else ""
                )
                if fitprint_sub:
                    fitprint_record_id = await self._metsights_service.create_record_for_profile(
                        profile_id=profile_id,
                        subscription_id=fitprint_sub,
                        sync_context=sync_context,
                    )
                else:
                    fitprint_record_id = None
                if fitprint_package is not None:
                    await self._assessments_service.ensure_instance_assigned(
                        db,
                        user_id=user.user_id,
                        engagement_id=engagement.engagement_id,
                        package_id=int(fitprint_package.package_id),
                        ip_address=ip_address,
                        user_agent=user_agent,
                        endpoint=endpoint,
                        metsights_record_id=fitprint_record_id,
                    )
        except Exception as exc:
            logger.warning(
                "Metsights sync failed for user_id=%s engagement_id=%s: %s",
                user.user_id,
                engagement.engagement_id,
                str(exc),
            )

        if profile_id:
            try:
                package = await self._assessments_service.get_package_by_id(db, engagement.assessment_package_id)
                primary_subscription_id = (
                    (getattr(package, "subscription_id", None) or "").strip() if package is not None else ""
                )
                primary_record_id: str | None = None
                if primary_subscription_id:
                    await release_request_transaction(db)
                    primary_record_id = await self._metsights_service.create_record_for_profile(
                        profile_id=profile_id,
                        subscription_id=primary_subscription_id,
                        sync_context=sync_context,
                    )
                if primary_record_id and assessment_instance is not None:
                    assessment_instance = await self._assessments_service.ensure_instance_assigned(
                        db,
                        user_id=user.user_id,
                        engagement_id=engagement.engagement_id,
                        package_id=engagement.assessment_package_id,
                        ip_address=ip_address,
                        user_agent=user_agent,
                        endpoint=endpoint,
                        metsights_record_id=primary_record_id,
                    )
                    metsights_record_id = primary_record_id
            except Exception as exc:
                logger.warning(
                    "Metsights primary record sync failed for user_id=%s engagement_id=%s: %s",
                    user.user_id,
                    engagement.engagement_id,
                    str(exc),
                )

        if (
            self._metsights_service is not None
            and assessment_instance is not None
            and not (assessment_instance.metsights_record_id or "").strip()
        ):
            try:
                package = await self._assessments_service.get_package_by_id(db, engagement.assessment_package_id)
                subscription_id = (getattr(package, "subscription_id", None) or "").strip() if package else ""
                profile_id = (user.metsights_profile_id or "").strip()
                if profile_id and subscription_id:
                    await release_request_transaction(db)
                    record_id = await self._metsights_service.create_record_for_profile(
                        profile_id=profile_id,
                        subscription_id=subscription_id,
                        sync_context=sync_context,
                    )
                    assessment_instance = await self._assessments_service.ensure_instance_assigned(
                        db,
                        user_id=user.user_id,
                        engagement_id=engagement.engagement_id,
                        package_id=engagement.assessment_package_id,
                        ip_address=ip_address,
                        user_agent=user_agent,
                        endpoint=endpoint,
                        metsights_record_id=record_id,
                    )
                    metsights_record_id = record_id
            except Exception as exc:
                logger.warning(
                    "Metsights record creation failed for user_id=%s engagement_id=%s: %s",
                    user.user_id,
                    engagement.engagement_id,
                    str(exc),
                )

        if (
            bool(engagement.load_prev_assessment_questionnaires)
            and assessment_instance is not None
            and self._questionnaire_service is not None
        ):
            try:
                from modules.assessments.repository import AssessmentsRepository
                from modules.questionnaire.load_prev_defaults import resolve_load_prev_category_keys

                has_prior = await self._engagements_service.user_has_prior_engagement_participation(
                    db,
                    user_id=int(user.user_id),
                    exclude_engagement_id=int(engagement.engagement_id),
                )
                if has_prior:
                    assessments_repo = AssessmentsRepository()
                    source_instance = await assessments_repo.get_latest_metsights_instance_from_prior_engagement(
                        db,
                        user_id=int(user.user_id),
                        exclude_engagement_id=int(engagement.engagement_id),
                        exclude_assessment_instance_id=int(assessment_instance.assessment_instance_id),
                    )
                    if source_instance is not None:
                        allowed_keys = resolve_load_prev_category_keys(
                            list(engagement.load_prev_questionnaire_category_keys or [])
                        )
                        copied_count = await self._questionnaire_service.copy_responses_from_previous_instance(
                            db,
                            user_id=int(user.user_id),
                            source_assessment_instance_id=int(source_instance.assessment_instance_id),
                            dest_assessment_instance_id=int(assessment_instance.assessment_instance_id),
                            allowed_category_keys=allowed_keys,
                            ip_address=ip_address,
                            user_agent=user_agent,
                            endpoint=endpoint,
                        )
                        preview_available = copied_count > 0
            except Exception as exc:
                logger.warning(
                    "Load previous assessment questionnaires failed for user_id=%s engagement_id=%s: %s",
                    user.user_id,
                    engagement.engagement_id,
                    str(exc),
                )

        # Record onboarding audit log.
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        await self._audit_service.log_event(
            db,
            action="USER_ENGAGEMENT_ONBOARD",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user.user_id,
            session_id=None,
        )

        await self._notify_onboarding_assistants(
            db,
            engagement=engagement,
            participant_user_id=int(user.user_id),
            payload=payload,
            source=code,
            cabin_key=blood_collection_cabin,
        )

        from modules.engagements.models import BloodCollectionType

        blood_collection = engagement.blood_collection_type
        is_home_collection = (
            blood_collection == BloodCollectionType.home_collection
            or str(blood_collection or "").strip().lower() == BloodCollectionType.home_collection.value
        )
        if (
            payment_type == EngagementPaymentType.paid_by_group.value
            and bool(engagement.auto_booking)
            and is_home_collection
        ):
            from modules.bookings import service as booking_service

            await booking_service.fulfill_auto_partner_booking_for_members(
                db,
                members=[{"user_id": int(user.user_id), "engagement_id": int(engagement.engagement_id)}],
                caller_user_id=int(user.user_id),
                allow_active_engagement_status=True,
                preserve_engagement_status=True,
            )

        mid = (assessment_instance.metsights_record_id or "").strip() or None
        return UserOnboardResponse(
            user_id=user.user_id,
            created=created,
            is_participant=True,
            engagement_id=engagement.engagement_id,
            engagement_code=engagement.engagement_code,
            engagement_participant_id=time_slot.engagement_participant_id,
            assessment_instance_id=int(assessment_instance.assessment_instance_id) if assessment_instance is not None else None,
            metsights_record_id=metsights_record_id or mid,
            preview_available=preview_available,
        )

    async def verify_engagement_onboard_pay(
        self,
        db: AsyncSession,
        *,
        engagement_code: str,
        payload: EngagementOnboardVerifyPayRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
        client_ip: str | None = None,
    ) -> dict[str, Any]:
        from modules.auth.dependencies import get_auth_service
        from modules.bookings import service as booking_service

        if self._engagements_service is None:
            raise RuntimeError("Engagements service is required")

        code = (engagement_code or "").strip()
        engagement = await self._engagements_service.get_by_code(db, code)
        if engagement is None:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        auth_service = get_auth_service()
        user_id, tokens = await auth_service.verify_otp(
            db,
            phone=payload.phone,
            otp=payload.otp,
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
        )

        participant = await self._engagements_service.get_participant_for_user_engagement(
            db,
            user_id=user_id,
            engagement_id=int(engagement.engagement_id),
        )
        if participant is None:
            raise AppError(status_code=404, error_code="NOT_ENROLLED", message="User is not enrolled in this engagement")

        pay_result = await booking_service.create_pay_order_for_engagement_participant(
            db,
            user_id=user_id,
            engagement_id=int(engagement.engagement_id),
            payer_user_id=user_id,
            discount_code=payload.discount_code,
            client_ip=client_ip,
        )
        pay_result["tokens"] = {
            "access_token": tokens.access_token,
            "refresh_token": tokens.refresh_token,
        }
        return pay_result

    async def confirm_engagement_onboard_payment(
        self,
        db: AsyncSession,
        *,
        engagement_code: str,
        payload: EngagementOnboardConfirmPaymentRequest,
        caller_user_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
        onboard_payload: EngagementUserOnboardRequest | None = None,
    ) -> dict[str, Any]:
        from modules.bookings import service as booking_service

        if self._engagements_service is None:
            raise RuntimeError("Engagements service is required")

        code = (engagement_code or "").strip()
        engagement = await self._engagements_service.get_by_code(db, code)
        if engagement is None:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        pay_result = await booking_service.verify_and_finalize_engagement_code_payment(
            db,
            razorpay_payment_id=payload.razorpay_payment_id,
            razorpay_order_id=payload.razorpay_order_id,
            razorpay_signature=payload.razorpay_signature,
            caller_user_id=caller_user_id,
            engagements_service=self._engagements_service,
        )

        user_id = caller_user_id
        await self._engagements_service.ensure_assessment_instances_for_participant(
            db,
            engagement=engagement,
            user_id=user_id,
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
        )

        user = await self._repository.get_user_by_id(db, user_id)
        if user is not None:
            await self._run_post_enrollment_integrations(
                db,
                user=user,
                engagement=engagement,
                payload=onboard_payload,
                ip_address=ip_address,
                user_agent=user_agent,
                endpoint=endpoint,
                source=code,
                blood_collection_cabin=None,
            )

        pay_result["engagement_id"] = engagement.engagement_id
        pay_result["engagement_code"] = engagement.engagement_code
        return pay_result

    async def _get_or_create_sub_profile_for_booker(
        self,
        db: AsyncSession,
        *,
        booker: User,
        member: FamilyOnboardMemberPayload,
    ) -> tuple[User, bool]:
        if booker.parent_id is not None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Booker must be a primary account",
            )

        first_name, last_name = self._split_csv_name(member.name)
        phone = str(member.phone).strip()
        existing = await self.resolve_user_by_phone(db, phone)
        if existing is not None:
            if int(existing.parent_id or 0) == int(booker.user_id):
                patch = {
                    key: value
                    for key, value in {
                        "first_name": first_name,
                        "last_name": last_name,
                        "age": member.age,
                        "date_of_birth": member.dob,
                        "gender": member.gender,
                        "city": member.city,
                    }.items()
                    if value is not None
                }
                if patch:
                    updated = await self._repository.patch_missing_fields(db, user=existing, data=patch)
                    if updated is not None:
                        existing = updated
                if member.email is not None:
                    existing = await self._apply_email_if_different(
                        db, existing, str(member.email)
                    )
                if not existing.is_participant:
                    existing.is_participant = True
                    db.add(existing)
                    await db.flush()
                return existing, False

            if member.email is not None:
                existing = await self._apply_email_if_different(
                    db, existing, str(member.email)
                )
            if not existing.is_participant:
                existing.is_participant = True
                db.add(existing)
                await db.flush()
            return existing, False

        email_to_use = str(member.email) if member.email is not None else None
        if email_to_use is None:
            if not booker.email or "@" not in booker.email:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            email_to_use = await self._allocate_plus_alias_email(db, booker.email)

        created_user = await self._repository.create_sub_profile(
            db,
            booker,
            {
                "first_name": first_name,
                "last_name": last_name,
                "age": member.age,
                "date_of_birth": member.dob,
                "gender": member.gender,
                "relationship": member.relation,
                "city": member.city,
                "phone": phone,
                "email": email_to_use,
            },
        )
        if created_user is None:
            raise AppError(status_code=409, error_code="CONFLICT", message="Unable to create profile")
        created_user.is_participant = True
        db.add(created_user)
        await db.flush()
        return created_user, True

    async def _unpaid_family_pay_members_for_booker(
        self,
        db: AsyncSession,
        *,
        booker_user_id: int,
    ) -> list[dict[str, int]]:
        if self._engagements_service is None:
            raise RuntimeError("Engagements service is required")
        participants = await self._engagements_service._repository.list_unpaid_participants_booked_by(
            db,
            booker_user_id=booker_user_id,
        )
        if not participants:
            raise AppError(
                status_code=422,
                error_code="NO_UNPAID_ENROLLMENTS",
                message="No unpaid family enrollments found for this booker",
            )
        return [
            {"user_id": int(p.user_id), "engagement_id": int(p.engagement_id)}
            for p in participants
        ]

    async def _family_pay_members_for_booker_engagements(
        self,
        db: AsyncSession,
        *,
        booker_user_id: int,
        engagement_ids: list[int],
    ) -> list[dict[str, int]]:
        from modules.engagements.enums import EngagementPaymentType
        from modules.engagements.models import Engagement
        from modules.engagements.participant_eligibility import engagement_payment_type_value

        if self._engagements_service is None:
            raise RuntimeError("Engagements service is required")

        seen: set[int] = set()
        ordered_ids: list[int] = []
        for raw in engagement_ids:
            eid = int(raw)
            if eid in seen:
                continue
            seen.add(eid)
            ordered_ids.append(eid)

        if not ordered_ids:
            raise AppError(
                status_code=422,
                error_code="INVALID_INPUT",
                message="engagement_ids must not be empty",
            )

        members: list[dict[str, int]] = []
        for engagement_id in ordered_ids:
            engagement = await self._engagements_service.get_by_id(db, engagement_id)
            if engagement is None:
                raise AppError(
                    status_code=404,
                    error_code="ENGAGEMENT_NOT_FOUND",
                    message="Engagement not found",
                )
            if engagement_payment_type_value(engagement) != EngagementPaymentType.paid_by_patient.value:
                raise AppError(
                    status_code=422,
                    error_code="INVALID_STATE",
                    message="Engagement does not require participant payment",
                )
            if (engagement.status or "").strip().lower() != "draft":
                raise AppError(
                    status_code=422,
                    error_code="INVALID_STATE",
                    message="Engagement is not in draft status",
                )

            participant_result = await db.execute(
                select(EngagementParticipant)
                .where(EngagementParticipant.engagement_id == engagement_id)
                .order_by(EngagementParticipant.engagement_participant_id.desc())
            )
            participants = list(participant_result.scalars().all())
            if not participants:
                raise AppError(
                    status_code=404,
                    error_code="NOT_ENROLLED",
                    message="User is not a participant",
                )

            booker_participants = [
                p
                for p in participants
                if int(p.booked_by_user_id or 0) == int(booker_user_id)
            ]
            if not booker_participants:
                raise AppError(
                    status_code=403,
                    error_code="FORBIDDEN",
                    message="Not authorized to pay for this participant",
                )
            participant = booker_participants[0]
            if bool(participant.paid):
                raise AppError(
                    status_code=422,
                    error_code="ALREADY_PAID",
                    message="Participant has already paid",
                )
            if int(booker_user_id) not in (
                int(participant.user_id),
                int(participant.booked_by_user_id),
            ):
                raise AppError(
                    status_code=403,
                    error_code="FORBIDDEN",
                    message="Not authorized to pay for this participant",
                )

            members.append({
                "user_id": int(participant.user_id),
                "engagement_id": int(engagement_id),
            })

        return members

    async def _promote_family_engagements_to_scheduled(
        self,
        db: AsyncSession,
        *,
        engagement_ids: list[int],
    ) -> None:
        if self._engagements_service is None:
            raise RuntimeError("Engagements service is required")
        seen: set[int] = set()
        for engagement_id in engagement_ids:
            eid = int(engagement_id)
            if eid in seen:
                continue
            seen.add(eid)
            engagement = await self._engagements_service.get_by_id(db, eid)
            if engagement is None:
                continue
            status = (engagement.status or "").strip().lower()
            if status == "draft":
                engagement.status = "scheduled"
                db.add(engagement)
        await db.flush()

    async def start_family_onboard(
        self,
        db: AsyncSession,
        *,
        payload: FamilyOnboardStartRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> FamilyOnboardStartResponse:
        if self._engagements_service is None or self._platform_settings_service is None:
            raise RuntimeError("Engagements and platform settings services are required")

        booker_first, booker_last = self._split_csv_name(payload.booker.name)
        booker_email = str(payload.booker.email)
        booker_phone = str(payload.booker.phone)

        booker, _booker_created = await self._get_or_create_user_for_onboarding(
            db,
            phone=booker_phone,
            email=booker_email,
            patch_data={
                "first_name": booker_first,
                "last_name": booker_last,
                "email": booker_email,
                "is_participant": True,
            },
            create_data={
                "first_name": booker_first,
                "last_name": booker_last,
                "phone": booker_phone,
                "email": booker_email,
                "referred_by": None,
                "is_participant": True,
                "status": "active",
            },
        )
        if booker.parent_id is not None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Booker must be a primary account",
            )

        booker = await self._apply_email_if_different(db, booker, booker_email)

        member_results: list[FamilyOnboardMemberResponse] = []

        for member in payload.members:
            member_user, sub_created = await self._get_or_create_sub_profile_for_booker(
                db, booker=booker, member=member,
            )

            engagement_type_code = await _resolve_onboard_engagement_type_code(
                self._platform_settings_service,
                db,
                member.engagement_type,
            )
            await _require_active_engagement_type_code(db, engagement_type_code)
            _validate_onboard_blood_fields(
                engagement_type_code,
                blood_collection_date=member.blood_collection_date,
                blood_collection_time_slot=member.blood_collection_time_slot,
            )
            onboarding_defaults = await self._platform_settings_service.resolve_b2c_onboarding_defaults(
                db, engagement_type_code
            )
            assessment_package_id = onboarding_defaults.assessment_package_id
            diagnostic_package_id = resolve_b2c_diagnostic_package_id(
                onboarding_defaults,
                override_id=member.diagnostic_package_id,
                gender=member_user.gender or member.gender,
            )
            await self._platform_settings_service.ensure_active_b2c_packages(
                db, assessment_package_id, diagnostic_package_id
            )

            resolved_type_id, resolved_consultations = await _resolve_consultation_from_pkg(
                db, diagnostic_package_id, engagement_type_code,
            )

            engagement = await self._engagements_service.create_b2c_engagement(
                db,
                user_first_name=member_user.first_name,
                engagement_date=member.blood_collection_date or date.today(),
                city=member.city or member_user.city,
                assessment_package_id=assessment_package_id,
                diagnostic_package_id=diagnostic_package_id,
                engagement_type=resolved_type_id,
                blood_collection_type=onboarding_defaults.blood_collection_type,
                consultations=resolved_consultations,
                address=member.address,
                pincode=member.pincode,
                state=member.state,
                country=member.country,
                create_profile_on_metsights=onboarding_defaults.create_profile_on_metsights,
                enroll_for_fitprint_full=onboarding_defaults.enroll_for_fitprint_full,
                **b2c_engagement_create_kwargs_from_defaults(
                    onboarding_defaults, resolved_consultations
                ),
            )
            engagement.status = "draft"
            db.add(engagement)
            await db.flush()

            slot_start = (
                self._parse_time_slot(member.blood_collection_time_slot)
                if member.blood_collection_time_slot
                else None
            )
            participant = await self._engagements_service.enroll_user_in_engagement(
                db,
                engagement=engagement,
                user_id=int(member_user.user_id),
                engagement_date=member.blood_collection_date,
                slot_start_time=slot_start,
                booked_by_user_id=int(booker.user_id),
                paid=False,
                booker_message=member.booker_message,
                booker_relation=member.relation,
                blood_collection_time_slot_id=member.blood_collection_time_slot_id,
            )

            member_results.append(
                FamilyOnboardMemberResponse(
                    user_id=int(member_user.user_id),
                    engagement_id=int(engagement.engagement_id),
                    engagement_code=str(engagement.engagement_code),
                    engagement_participant_id=int(participant.engagement_participant_id),
                    sub_profile_created=sub_created,
                )
            )

        from modules.auth.dependencies import get_auth_service

        await release_request_transaction(db)
        auth_service = get_auth_service()
        otp_session_id, delivery = await auth_service.send_otp_for_user_id(
            db,
            user_id=int(booker.user_id),
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
        )
        if delivery is not None:
            await auth_service.deliver_otp_via_notifications(db, delivery=delivery)

        if self._audit_service is not None:
            await self._audit_service.log_event(
                db,
                action="FAMILY_ONBOARD_START",
                endpoint=endpoint,
                ip_address=ip_address,
                user_agent=user_agent,
                user_id=booker.user_id,
                session_id=None,
            )

        return FamilyOnboardStartResponse(
            booker_user_id=int(booker.user_id),
            otp_session_id=int(otp_session_id),
            payment_required=True,
            members=member_results,
        )

    async def verify_family_onboard_pay(
        self,
        db: AsyncSession,
        *,
        payload: FamilyOnboardVerifyPayRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
        client_ip: str | None = None,
    ) -> dict[str, Any]:
        from modules.auth.dependencies import get_auth_service
        from modules.bookings import service as booking_service

        auth_service = get_auth_service()
        user_id, tokens = await auth_service.verify_otp(
            db,
            phone=payload.phone,
            otp=payload.otp,
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
        )

        pay_members = await self._family_pay_members_for_booker_engagements(
            db,
            booker_user_id=int(user_id),
            engagement_ids=[int(eid) for eid in payload.engagement_ids],
        )
        pay_result = await booking_service.create_pay_order_for_family_onboard(
            db,
            members=pay_members,
            payer_user_id=int(user_id),
            discount_code=payload.discount_code,
            client_ip=client_ip,
        )

        pay_result["booker_user_id"] = int(user_id)
        pay_result["engagement_ids"] = [int(m["engagement_id"]) for m in pay_members]
        pay_result["tokens"] = {
            "access_token": tokens.access_token,
            "refresh_token": tokens.refresh_token,
        }
        return pay_result

    async def confirm_family_onboard_payment(
        self,
        db: AsyncSession,
        *,
        payload: FamilyOnboardConfirmPaymentRequest,
        caller_user_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict[str, Any]:
        from sqlalchemy import select

        from modules.bookings import service as booking_service
        from modules.payments.models import Booking, Order, OrderBooking

        if self._engagements_service is None:
            raise RuntimeError("Engagements service is required")

        order_result = await db.execute(
            select(Order).where(Order.razorpay_order_id == payload.razorpay_order_id.strip())
        )
        order = order_result.scalar_one_or_none()
        if order is None:
            raise AppError(status_code=404, error_code="ORDER_NOT_FOUND", message="Payment order not found")
        if int(order.user_id) != int(caller_user_id):
            raise AppError(status_code=403, error_code="FORBIDDEN", message="Not authorized for this payment order")

        pay_result = await booking_service.verify_and_finalize_engagement_code_payment(
            db,
            razorpay_payment_id=payload.razorpay_payment_id,
            razorpay_order_id=payload.razorpay_order_id,
            razorpay_signature=payload.razorpay_signature,
            caller_user_id=caller_user_id,
            engagements_service=self._engagements_service,
        )

        booking_ids = list(
            (
                await db.execute(
                    select(OrderBooking.booking_id).where(OrderBooking.order_id == order.order_id)
                )
            )
            .scalars()
            .all()
        )
        booker_user = await self._repository.get_user_by_id(db, int(caller_user_id))

        order_members: list[dict[str, int]] = []
        if booking_ids:
            bookings_result = await db.execute(select(Booking).where(Booking.booking_id.in_(booking_ids)))
            bookings = list(bookings_result.scalars().all())
            order_members = booking_service._members_from_order_bookings(bookings)
            pay_member_results: dict[tuple[int, int], dict[str, Any]] = {}
            for pay_member in pay_result.get("members") or []:
                uid = pay_member.get("user_id")
                eid = pay_member.get("engagement_id")
                if uid is not None and eid is not None:
                    pay_member_results[(int(uid), int(eid))] = pay_member
            await self._promote_family_engagements_to_scheduled(
                db,
                engagement_ids=[int(m["engagement_id"]) for m in order_members],
            )
            for member in order_members:
                engagement = await self._engagements_service.get_by_id(db, int(member["engagement_id"]))
                if engagement is None:
                    continue
                member_user = await self._repository.get_user_by_id(db, int(member["user_id"]))
                if member_user is None:
                    continue
                await self._engagements_service.ensure_assessment_instances_for_participant(
                    db,
                    engagement=engagement,
                    user_id=int(member["user_id"]),
                    ip_address=ip_address,
                    user_agent=user_agent,
                    endpoint=endpoint,
                )
                await self._run_post_enrollment_integrations(
                    db,
                    user=member_user,
                    engagement=engagement,
                    payload=None,
                    ip_address=ip_address,
                    user_agent=user_agent,
                    endpoint=endpoint,
                    source="family-onboard",
                    blood_collection_cabin=None,
                )
                participant = await self._engagements_service.get_participant_for_user_engagement(
                    db,
                    user_id=int(member["user_id"]),
                    engagement_id=int(member["engagement_id"]),
                )
                pay_member_result = pay_member_results.get(
                    (int(member["user_id"]), int(member["engagement_id"]))
                )
                vendor_booking_id = (
                    (pay_member_result.get("booking_id") or "").strip()
                    if pay_member_result is not None
                    else ""
                )
                vendor_already_notified = bool(engagement.auto_booking) and (
                    (pay_member_result or {}).get("status") == "success" and bool(vendor_booking_id)
                )
                if not vendor_already_notified:
                    from modules.engagements.blood_bookings_access import (
                        get_current_row,
                        read_schedule_from_row,
                    )

                    sched = read_schedule_from_row(
                        await get_current_row(db, participant) if participant is not None else None
                    )
                    coll_date = sched.get("engagement_date")
                    coll_time = sched.get("slot_start_time")
                    await self._notify_onboarding_assistants_for_user(
                        db,
                        engagement=engagement,
                        user=member_user,
                        source=engagement.engagement_code or "family-onboard",
                        collection_date=coll_date.isoformat() if coll_date is not None else None,
                        collection_time=coll_time.isoformat() if coll_time is not None else None,
                        cabin_key=sched.get("blood_collection_cabin"),
                    )
                if booker_user is not None and self._notifications_service is not None:
                    from modules.notifications.family_onboard_gift_notify import (
                        notify_family_onboard_gift,
                    )

                    await notify_family_onboard_gift(
                        db,
                        notifications_service=self._notifications_service,
                        booker=booker_user,
                        recipient=member_user,
                        engagement_id=int(member["engagement_id"]),
                        relationship=(
                            participant.booker_relation if participant is not None else None
                        ),
                        booker_message=(
                            participant.booker_message if participant is not None else None
                        ),
                    )

        pay_result["booker_user_id"] = int(caller_user_id)
        return pay_result

    async def _run_post_enrollment_integrations(
        self,
        db: AsyncSession,
        *,
        user: Any,
        engagement: Any,
        payload: EngagementUserOnboardRequest | None,
        ip_address: str,
        user_agent: str,
        endpoint: str,
        source: str,
        blood_collection_cabin: str | None,
    ) -> None:
        """Metsights, questionnaires, and OA notifications after payment or group onboard."""
        if self._assessments_service is None:
            return

        assessment_instance = None
        if engagement.assessment_package_id is not None:
            assessment_instance = await self._assessments_service.ensure_instance_assigned(
                db,
                user_id=user.user_id,
                engagement_id=engagement.engagement_id,
                package_id=engagement.assessment_package_id,
                ip_address=ip_address,
                user_agent=user_agent,
                endpoint=endpoint,
            )

        await release_request_transaction(db)
        if self._metsights_service is not None:
            sync_context = MetsightsSyncContext(
                db=db,
                engagement_id=int(engagement.engagement_id),
                user_id=int(user.user_id),
            )
            try:
                profile_id = await self._create_metsights_profile_for_engagement(
                    db,
                    user=user,
                    engagement=engagement,
                    sync_context=sync_context,
                )
                if profile_id and bool(engagement.enroll_for_fitprint_full):
                    await release_request_transaction(db)
                    fitprint_package = await self._assessments_service.get_package_by_assessment_type_code(
                        db,
                        assessment_type_code="7",
                    )
                    fitprint_sub = (
                        (getattr(fitprint_package, "subscription_id", None) or "").strip()
                        if fitprint_package is not None
                        else ""
                    )
                    if fitprint_sub:
                        fitprint_record_id = await self._metsights_service.create_record_for_profile(
                            profile_id=profile_id,
                            subscription_id=fitprint_sub,
                            sync_context=sync_context,
                        )
                        if fitprint_package is not None:
                            await self._assessments_service.ensure_instance_assigned(
                                db,
                                user_id=user.user_id,
                                engagement_id=engagement.engagement_id,
                                package_id=int(fitprint_package.package_id),
                                ip_address=ip_address,
                                user_agent=user_agent,
                                endpoint=endpoint,
                                metsights_record_id=fitprint_record_id,
                            )
            except Exception as exc:
                logger.warning(
                    "Post-payment Metsights sync failed for user_id=%s engagement_id=%s: %s",
                    user.user_id,
                    engagement.engagement_id,
                    str(exc),
                )

        if payload is not None:
            await self._notify_onboarding_assistants(
                db,
                engagement=engagement,
                participant_user_id=int(user.user_id),
                payload=payload,
                source=source,
                cabin_key=blood_collection_cabin,
            )

    def _split_csv_name(self, raw_name: str) -> tuple[str | None, str | None]:
        cleaned = (raw_name or "").strip()
        if not cleaned:
            return None, None
        parts = [p for p in cleaned.split() if p]
        if len(parts) == 1:
            return parts[0], "Unknown"
        return parts[0], " ".join(parts[1:])

    def _normalize_gender_label(self, raw_gender: str | None) -> str | None:
        g = (raw_gender or "").strip().lower()
        if not g:
            return None
        if g in {"1", "male", "m"}:
            return "Male"
        if g in {"2", "female", "f"}:
            return "Female"
        return raw_gender

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
        """Match local users when Metsights and DB store the same number in different formats (+91, 10-digit, etc.)."""

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

    async def _resolve_user_by_phone_for_import(self, db: AsyncSession, phone: str) -> User | None:
        candidates = _phone_lookup_candidates(phone)
        if not candidates:
            return None
        users = await self._repository.list_users_by_phones(db, candidates)
        return self._resolve_user_by_phone_from_index(phone, self._build_phone_lookup_index(users))

    async def _preload_phone_lookup_index(self, db: AsyncSession, raw_phones: list[str]) -> dict[str, list[User]]:
        candidates: list[str] = []
        for phone in raw_phones:
            candidates.extend(_phone_lookup_candidates(phone))
        unique = list(dict.fromkeys(c for c in candidates if c))
        if not unique:
            return {}
        users = await self._repository.list_users_by_phones(db, unique)
        return self._build_phone_lookup_index(users)

    def _build_email_owner_index(self, users: list[User]) -> dict[str, int]:
        owners: dict[str, int] = {}
        for user in users:
            email_key = (user.email or "").strip().lower()
            if email_key:
                owners[email_key] = int(user.user_id)
        return owners

    async def _email_safe_for_user_update(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        candidate: str | None,
    ) -> str | None:
        """Return *candidate* only when no other user row already owns that email."""

        if not candidate:
            return None
        owner = await self._repository.get_user_by_email(db, candidate)
        if owner is None or int(owner.user_id) == int(user_id):
            return candidate
        return None

    async def _email_for_metsights_sub_profile(
        self,
        db: AsyncSession,
        *,
        parent_user: User,
        preferred_email: str | None,
        metsights_profile_id: str,
        email_owners: dict[str, int] | None = None,
    ) -> str | None:
        def _is_free(email: str) -> bool:
            key = email.strip().lower()
            owner_id = (email_owners or {}).get(key)
            return owner_id is None

        if preferred_email and _is_free(preferred_email):
            return preferred_email

        parent_email = (parent_user.email or "").strip()
        if not parent_email or "@" not in parent_email:
            return None

        local_part, domain = parent_email.split("@", 1)
        ms_suffix = (metsights_profile_id or "").replace("-", "")[:12] or str(parent_user.user_id)
        deterministic = f"{local_part}+ms{parent_user.user_id}-{ms_suffix}@{domain}"
        if _is_free(deterministic):
            return deterministic

        for attempt in range(5):
            candidate = f"{local_part}+ms{parent_user.user_id}-{ms_suffix}-{attempt}@{domain}"
            if _is_free(candidate):
                return candidate
        return None

    async def _get_or_create_user_from_metsights_profile(
        self,
        db: AsyncSession,
        *,
        profile_id: str,
        csv_name: str,
        csv_phone: str,
        profile_data: dict[str, Any],
        engagement_code: str | None,
        phone_index: dict[str, list[User]] | None = None,
        email_owners: dict[str, int] | None = None,
    ) -> User:
        profile_phone = _normalize_import_phone(str(profile_data.get("phone") or ""))
        profile_email_raw = str(profile_data.get("email") or "").strip()
        profile_email = profile_email_raw if profile_email_raw and "@" in profile_email_raw else None
        first_name = str(profile_data.get("first_name") or "").strip() or None
        last_name = str(profile_data.get("last_name") or "").strip() or None
        if not first_name or not last_name:
            csv_first, csv_last = self._split_csv_name(csv_name)
            first_name = first_name or csv_first
            last_name = last_name or csv_last
        gender = self._normalize_gender_label(str(profile_data.get("gender") or "").strip()) or None
        age_value = profile_data.get("age")
        try:
            age = int(age_value) if age_value is not None else 30
        except (TypeError, ValueError):
            age = 30
        if age < 1 or age > 120:
            age = 30

        dob_value = profile_data.get("date_of_birth")
        dob = None
        if isinstance(dob_value, str) and dob_value.strip():
            try:
                dob = date.fromisoformat(dob_value.strip())
            except ValueError:
                dob = None

        normalized_csv_phone = _normalize_import_phone(csv_phone)
        normalized_phone = profile_phone or normalized_csv_phone
        if len(normalized_phone) < 5:
            raise AppError(status_code=422, error_code="INVALID_INPUT", message="Invalid or missing phone")

        existing = await self._repository.get_user_by_metsights_profile_id(db, profile_id)
        if existing is None:
            if phone_index is not None:
                existing = self._resolve_user_by_phone_from_index(normalized_phone, phone_index)
            else:
                existing = await self._resolve_user_by_phone_for_import(db, normalized_phone)
        if existing is None and profile_email is not None:
            owner_id = (email_owners or {}).get(profile_email.strip().lower())
            if owner_id is not None:
                existing = await self._repository.get_user_by_id(db, owner_id)
            else:
                existing = await self._repository.get_user_by_email(db, profile_email)

        patch_data: dict[str, Any] = {
            "first_name": first_name,
            "last_name": last_name,
            "age": age,
            "date_of_birth": dob,
            "gender": gender,
            "referred_by": engagement_code or None,
            "is_participant": True,
            "metsights_profile_id": profile_id,
        }

        if existing is None:
            create_email = profile_email
            if profile_email is not None:
                owner_id = (email_owners or {}).get(profile_email.strip().lower())
                if owner_id is not None:
                    create_email = None
            user = User(
                **patch_data,
                email=create_email,
                phone=normalized_phone,
                status="active",
            )
            return await self._repository.create_user(db, user)

        safe_email: str | None = None
        if profile_email:
            owner_id = (email_owners or {}).get(profile_email.strip().lower())
            if owner_id is None or int(owner_id) == int(existing.user_id):
                safe_email = profile_email
        if safe_email is not None:
            patch_data["email"] = safe_email

        existing_ms_id = (existing.metsights_profile_id or "").strip()
        if existing_ms_id and existing_ms_id != profile_id:
            parent_user = existing
            if existing.parent_id is not None:
                parent_row = await self._repository.get_user_by_id(db, int(existing.parent_id))
                if parent_row is not None:
                    parent_user = parent_row

            sub_email = await self._email_for_metsights_sub_profile(
                db,
                parent_user=parent_user,
                preferred_email=profile_email,
                metsights_profile_id=profile_id,
                email_owners=email_owners,
            )
            sub = await self._repository.create_sub_profile(
                db,
                parent_user,
                {
                    "first_name": first_name,
                    "last_name": last_name,
                    "age": age,
                    "date_of_birth": dob,
                    "gender": gender,
                    "phone": parent_user.phone,
                    "email": sub_email,
                },
            )
            # Sub-profile email is set at create time; do not overwrite with the parent's Metsights email.
            sub_patch = {k: v for k, v in patch_data.items() if k != "email"}
            updated = await self._repository.update_user_partial(db, sub.user_id, sub_patch)
            return updated if updated is not None else sub

        return await self._repository.update_user_partial(db, existing.user_id, patch_data)

    def _metsights_records_list_payload(self, data: Any) -> list[dict[str, Any]]:
        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]
        if isinstance(data, dict):
            inner = data.get("results")
            if isinstance(inner, list):
                return [r for r in inner if isinstance(r, dict)]
        return []

    def _is_female_gender(self, gender: str | None) -> bool:
        g = (gender or "").strip().lower()
        return g in ("female", "2", "f")

    async def import_metsights_profiles_by_employee(
        self,
        db: AsyncSession,
        *,
        employee,
        metsights_profile_ids: list[str],
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict[str, Any]:
        """Fetch Metsights profiles + records; upsert users; one B2C engagement per non-FitPrint record (oldest first)."""

        self._ensure_employee_access(employee)
        if self._metsights_service is None:
            raise RuntimeError("Metsights service is required")
        if self._engagements_service is None or self._assessments_service is None:
            raise RuntimeError("Engagements and assessments services are required")
        if self._platform_settings_service is None:
            raise RuntimeError("Platform settings service is required")

        _PEAK_DIAG_MALE = 17
        _PEAK_DIAG_FEMALE = 24
        default_slot = time(10, 0)
        profiles_out: list[dict[str, Any]] = []

        for profile_id in metsights_profile_ids:
            profile_block: dict[str, Any] = {
                "metsights_profile_id": profile_id,
                "user_id": None,
                "records": [],
                "error": None,
            }
            try:
                profile_detail = await self._metsights_service.get_profile_detail(profile_id=profile_id)
            except AppError as exc:
                profile_block["error"] = exc.message or "Failed to fetch Metsights profile"
                profiles_out.append(profile_block)
                continue

            if not isinstance(profile_detail, dict):
                profile_block["error"] = "Invalid Metsights profile response"
                profiles_out.append(profile_block)
                continue

            first_name = str(profile_detail.get("first_name") or "").strip()
            last_name = str(profile_detail.get("last_name") or "").strip()
            csv_name = f"{first_name} {last_name}".strip() or "Participant"
            csv_phone = str(profile_detail.get("phone") or "").strip()
            if not csv_phone:
                profile_block["error"] = "Metsights profile has no phone"
                profiles_out.append(profile_block)
                continue

            user = await self._get_or_create_user_from_metsights_profile(
                db,
                profile_id=profile_id,
                csv_name=csv_name,
                csv_phone=csv_phone,
                profile_data=profile_detail,
                engagement_code=None,
            )
            profile_block["user_id"] = int(user.user_id)

            try:
                raw_records = await self._metsights_service.list_profile_records(profile_id=profile_id)
            except AppError as exc:
                profile_block["error"] = exc.message or "Failed to list Metsights records"
                profiles_out.append(profile_block)
                continue

            rows = self._metsights_records_list_payload(raw_records)
            rows.sort(
                key=lambda r: (
                    _parse_iso_date(r.get("date")),
                    str(r.get("updated_at") or r.get("created_at") or ""),
                    str(r.get("id") or ""),
                )
            )

            for row in rows:
                mrid = str(row.get("id") or "").strip()
                rec_out: dict[str, Any] = {"metsights_record_id": mrid or None, "status": "", "reason": None}
                if not mrid:
                    rec_out["status"] = "skipped"
                    rec_out["reason"] = "missing record id"
                    profile_block["records"].append(rec_out)
                    continue

                existing_inst = await self._assessments_service.get_instance_by_metsights_record_id(
                    db, metsights_record_id=mrid
                )
                if existing_inst is not None:
                    rec_out["status"] = "skipped"
                    rec_out["reason"] = "already imported"
                    profile_block["records"].append(rec_out)
                    continue

                type_code = _normalize_metsights_type_code(row)
                if type_code == "7":
                    rec_out["status"] = "skipped"
                    rec_out["reason"] = "fitprint"
                    profile_block["records"].append(rec_out)
                    continue
                if not type_code:
                    rec_out["status"] = "skipped"
                    rec_out["reason"] = "unknown assessment type"
                    profile_block["records"].append(rec_out)
                    continue

                package = await self._assessments_service.get_package_by_assessment_type_code(
                    db,
                    assessment_type_code=type_code,
                )
                if package is None:
                    rec_out["status"] = "skipped"
                    rec_out["reason"] = f"no active package for type {type_code}"
                    profile_block["records"].append(rec_out)
                    continue

                try:
                    record_detail = await self._metsights_service.get_record_detail(record_id=mrid)
                except AppError as exc:
                    rec_out["status"] = "error"
                    rec_out["reason"] = exc.message or "Failed to fetch Metsights record"
                    profile_block["records"].append(rec_out)
                    continue

                if not isinstance(record_detail, dict):
                    rec_out["status"] = "error"
                    rec_out["reason"] = "Invalid Metsights record response"
                    profile_block["records"].append(rec_out)
                    continue

                record_profile = record_detail.get("profile")
                record_profile_id = (
                    str((record_profile or {}).get("id") or "").strip()
                    if isinstance(record_profile, dict)
                    else ""
                )
                if record_profile_id and record_profile_id != profile_id:
                    rec_out["status"] = "error"
                    rec_out["reason"] = "record does not belong to metsights_profile_id"
                    profile_block["records"].append(rec_out)
                    continue

                diag_pref = _PEAK_DIAG_FEMALE if self._is_female_gender(user.gender) else _PEAK_DIAG_MALE
                diag_id = await _resolve_active_diagnostic_package_id(db, diag_pref)
                ap_id = int(package.package_id)
                await self._platform_settings_service.ensure_active_b2c_packages(db, ap_id, diag_id)

                engagement_date = _parse_iso_date(row.get("date"))
                bio_ai_type_id = await _resolve_engagement_type_id(db, "bio_ai")
                engagement = await self._engagements_service.create_b2c_engagement(
                    db,
                    user_first_name=user.first_name,
                    engagement_date=engagement_date,
                    city=user.city,
                    assessment_package_id=ap_id,
                    diagnostic_package_id=diag_id,
                    engagement_type=bio_ai_type_id,
                    address=user.address,
                    pincode=user.pin_code,
                    state=getattr(user, "state", None),
                    country=getattr(user, "country", None),
                    create_profile_on_metsights=True,
                    enroll_for_fitprint_full=False,
                )

                participant = await self._engagements_service.enroll_user_in_engagement(
                    db,
                    engagement=engagement,
                    user_id=user.user_id,
                    engagement_date=engagement_date,
                    slot_start_time=default_slot
                )

                await self._notify_onboarding_assistants_for_user(
                    db,
                    engagement=engagement,
                    user=user,
                    source=engagement.engagement_code or "metsights-import",
                    collection_date=engagement_date.isoformat(),
                    collection_time=default_slot.isoformat(),
                )

                try:
                    instance = await self._assessments_service.create_instance_for_metsights_record(
                        db,
                        user_id=user.user_id,
                        engagement_id=engagement.engagement_id,
                        package_id=ap_id,
                        metsights_record_id=mrid,
                        metsights_is_complete=bool(record_detail.get("is_complete")),
                        ip_address=ip_address,
                        user_agent=user_agent,
                        endpoint=endpoint,
                    )
                except AppError as exc:
                    rec_out["status"] = "error"
                    rec_out["reason"] = exc.message or "Assessment assignment failed"
                    profile_block["records"].append(rec_out)
                    continue


                rec_out["status"] = "imported"
                rec_out["engagement_id"] = int(engagement.engagement_id)
                rec_out["assessment_instance_id"] = int(instance.assessment_instance_id)
                rec_out["diagnostic_package_id"] = int(diag_id)
                profile_block["records"].append(rec_out)

            profiles_out.append(profile_block)

        return {"profiles": profiles_out}

    async def get_metsights_profile_import_stats(self, db: AsyncSession) -> dict[str, int]:
        """Local user counts plus Metsights remote total from page 1."""

        if self._metsights_service is None:
            raise RuntimeError("Metsights service is required")

        local_total = await self._repository.count_users(db)
        local_with = await self._repository.count_users_with_metsights_profile_id(db)
        local_without = max(0, local_total - local_with)

        page = await self._metsights_service.list_profiles_page(page=1)
        metsights_total = int(page.count or 0)
        estimated_not_imported = max(0, metsights_total - local_with)

        return {
            "local_total_users": local_total,
            "local_with_metsights_profile_id": local_with,
            "local_without_metsights_profile_id": local_without,
            "metsights_total": metsights_total,
            "estimated_not_imported": estimated_not_imported,
        }

    async def import_metsights_profiles_page(self, db: AsyncSession, *, page: int) -> dict[str, Any]:
        """Import one Metsights profiles list page into local users (profile only, no engagements)."""

        if self._metsights_service is None:
            raise RuntimeError("Metsights service is required")

        ms_page = await self._metsights_service.list_profiles_page(page=page)
        rows = ms_page.data

        profile_ids = [str(row.get("id") or "").strip() for row in rows]
        existing_by_ms_id = await self._repository.get_users_by_metsights_profile_ids(db, profile_ids)

        pending_phones: list[str] = []
        pending_emails: list[str] = []
        for row in rows:
            profile_id = str(row.get("id") or "").strip()
            if not profile_id or profile_id in existing_by_ms_id:
                continue
            csv_phone = str(row.get("phone") or "").strip()
            if not csv_phone:
                continue
            pending_phones.append(_normalize_import_phone(csv_phone))
            email_raw = str(row.get("email") or "").strip()
            if email_raw and "@" in email_raw:
                pending_emails.append(email_raw)

        phone_index = await self._preload_phone_lookup_index(db, pending_phones)
        email_users = await self._repository.list_users_by_emails(db, pending_emails)
        email_owners = self._build_email_owner_index(email_users)

        created = 0
        linked = 0
        skipped = 0
        failed = 0
        failures: list[dict[str, str]] = []
        skipped_items: list[dict[str, str]] = []
        max_detail_items = 20

        for row in rows:
            profile_id = str(row.get("id") or "").strip()
            if not profile_id:
                failed += 1
                if len(failures) < max_detail_items:
                    failures.append({"metsights_profile_id": "", "reason": "Missing profile id"})
                continue

            if profile_id in existing_by_ms_id:
                skipped += 1
                if len(skipped_items) < max_detail_items:
                    linked_user_id = int(existing_by_ms_id[profile_id].user_id)
                    skipped_items.append(
                        {
                            "metsights_profile_id": profile_id,
                            "reason": (
                                f"Already linked — local user #{linked_user_id} "
                                "has this metsights_profile_id"
                            ),
                        }
                    )
                continue

            first_name = str(row.get("first_name") or "").strip()
            last_name = str(row.get("last_name") or "").strip()
            csv_name = f"{first_name} {last_name}".strip() or "Participant"
            csv_phone = str(row.get("phone") or "").strip()
            if not csv_phone:
                failed += 1
                if len(failures) < max_detail_items:
                    failures.append({"metsights_profile_id": profile_id, "reason": "Metsights profile has no phone"})
                continue

            profile_phone = _normalize_import_phone(csv_phone)
            profile_email_raw = str(row.get("email") or "").strip()
            profile_email = profile_email_raw if profile_email_raw and "@" in profile_email_raw else None

            existing_before: User | None = None
            if profile_phone or csv_phone:
                try:
                    existing_before = self._resolve_user_by_phone_from_index(
                        profile_phone or csv_phone,
                        phone_index,
                    )
                except AppError:
                    existing_before = None
            if existing_before is None and profile_email is not None:
                owner_id = email_owners.get(profile_email.strip().lower())
                if owner_id is not None:
                    existing_before = await self._repository.get_user_by_id(db, owner_id)

            try:
                async with db.begin_nested():
                    user = await self._get_or_create_user_from_metsights_profile(
                        db,
                        profile_id=profile_id,
                        csv_name=csv_name,
                        csv_phone=csv_phone,
                        profile_data=row,
                        engagement_code=None,
                        phone_index=phone_index,
                        email_owners=email_owners,
                    )
            except AppError as exc:
                failed += 1
                if len(failures) < max_detail_items:
                    failures.append(
                        {"metsights_profile_id": profile_id, "reason": exc.message or "Import failed"}
                    )
                continue
            except IntegrityError as exc:
                failed += 1
                if len(failures) < max_detail_items:
                    failures.append(
                        {
                            "metsights_profile_id": profile_id,
                            "reason": _integrity_error_reason(exc),
                        }
                    )
                continue
            except Exception as exc:
                failed += 1
                if len(failures) < max_detail_items:
                    failures.append({"metsights_profile_id": profile_id, "reason": str(exc)})
                continue

            existing_by_ms_id[profile_id] = user
            phone_key = (user.phone or "").strip()
            if phone_key:
                bucket = phone_index.setdefault(phone_key, [])
                if not any(int(row.user_id) == int(user.user_id) for row in bucket):
                    bucket.append(user)
            email_key = (user.email or "").strip().lower()
            if email_key:
                email_owners[email_key] = int(user.user_id)

            if existing_before is not None:
                linked += 1
            else:
                created += 1

        return {
            "page": page,
            "page_size": len(rows),
            "metsights_total": int(ms_page.count or 0),
            "metsights_next": ms_page.next,
            "metsights_previous": ms_page.previous,
            "created": created,
            "linked": linked,
            "skipped": skipped,
            "failed": failed,
            "failures": failures,
            "skipped_items": skipped_items,
        }

    async def get_engagements_sync_stats(self, db: AsyncSession) -> dict[str, int]:
        """Counts for Settings Engagements sync cards."""

        users_with = await self._repository.count_users_with_metsights_profile_id(db)
        b2c_total = await EngagementsRepository().count_engagements_with_null_organization(db)
        return {
            "users_with_metsights_profile_id": users_with,
            "b2c_engagements_total": b2c_total,
        }

    async def import_engagements_sync_page(
        self,
        db: AsyncSession,
        *,
        page: int,
        ip_address: str = "unknown",
        user_agent: str = "unknown",
        endpoint: str = "/platform-settings/engagements-sync/import-page",
    ) -> dict[str, Any]:
        """Page local users with Metsights profiles; create B2C engagements from non-FitPrint records."""

        if self._metsights_service is None:
            raise RuntimeError("Metsights service is required")
        if self._engagements_service is None or self._assessments_service is None:
            raise RuntimeError("Engagements and assessments services are required")
        if self._platform_settings_service is None:
            raise RuntimeError("Platform settings service is required")

        _PEAK_DIAG_MALE = 17
        _PEAK_DIAG_FEMALE = 24
        _PAGE_SIZE = 10
        default_slot = time(10, 0)
        today = date.today()

        users_total = await self._repository.count_users_with_metsights_profile_id(db)
        users = await self._repository.list_users_with_metsights_profile_id(
            db, page=page, limit=_PAGE_SIZE
        )

        created = 0
        skipped = 0
        failed = 0
        failures: list[dict[str, str]] = []
        skipped_items: list[dict[str, str]] = []
        max_detail_items = 20

        for user in users:
            profile_id = (user.metsights_profile_id or "").strip()
            if not profile_id:
                skipped += 1
                continue

            try:
                raw_records = await self._metsights_service.list_profile_records(profile_id=profile_id)
            except AppError as exc:
                failed += 1
                if len(failures) < max_detail_items:
                    failures.append(
                        {
                            "metsights_profile_id": profile_id,
                            "reason": exc.message or "Failed to list Metsights records",
                        }
                    )
                continue
            except Exception as exc:
                failed += 1
                if len(failures) < max_detail_items:
                    failures.append({"metsights_profile_id": profile_id, "reason": str(exc)})
                continue

            rows = self._metsights_records_list_payload(raw_records)
            rows.sort(
                key=lambda r: (
                    _parse_iso_date(r.get("date")),
                    str(r.get("updated_at") or r.get("created_at") or ""),
                    str(r.get("id") or ""),
                )
            )

            for row in rows:
                mrid = str(row.get("id") or "").strip()
                if not mrid:
                    skipped += 1
                    if len(skipped_items) < max_detail_items:
                        skipped_items.append(
                            {"metsights_profile_id": profile_id, "reason": "missing record id"}
                        )
                    continue

                existing_inst = await self._assessments_service.get_instance_by_metsights_record_id(
                    db, metsights_record_id=mrid
                )
                if existing_inst is not None:
                    skipped += 1
                    if len(skipped_items) < max_detail_items:
                        skipped_items.append(
                            {
                                "metsights_profile_id": profile_id,
                                "reason": f"already imported — record {mrid}",
                            }
                        )
                    continue

                type_code = _normalize_metsights_type_code(row)
                if type_code == "7":
                    skipped += 1
                    if len(skipped_items) < max_detail_items:
                        skipped_items.append(
                            {"metsights_profile_id": profile_id, "reason": f"fitprint — record {mrid}"}
                        )
                    continue
                if not type_code:
                    skipped += 1
                    if len(skipped_items) < max_detail_items:
                        skipped_items.append(
                            {
                                "metsights_profile_id": profile_id,
                                "reason": f"unknown assessment type — record {mrid}",
                            }
                        )
                    continue

                package = await self._assessments_service.get_package_by_assessment_type_code(
                    db,
                    assessment_type_code=type_code,
                )
                if package is None:
                    skipped += 1
                    if len(skipped_items) < max_detail_items:
                        skipped_items.append(
                            {
                                "metsights_profile_id": profile_id,
                                "reason": f"no active package for type {type_code} — record {mrid}",
                            }
                        )
                    continue

                try:
                    diag_pref = _PEAK_DIAG_FEMALE if self._is_female_gender(user.gender) else _PEAK_DIAG_MALE
                    diag_id = await _resolve_active_diagnostic_package_id(db, diag_pref)
                    ap_id = int(package.package_id)
                    await self._platform_settings_service.ensure_active_b2c_packages(db, ap_id, diag_id)

                    pkg = await _diagnostics_repo.get_package_by_id_basic(db, package_id=diag_id)
                    consultations: dict | None = None
                    if pkg is not None and isinstance(pkg.complementary_consultation, dict):
                        consultations = pkg.complementary_consultation

                    start_date = _parse_iso_date(row.get("created_at") or row.get("date"))
                    end_date = _parse_iso_date(row.get("updated_at") or row.get("date") or row.get("created_at"))

                    bio_ai_type_id = await _resolve_engagement_type_id(db, "bio_ai")
                    engagement = await self._engagements_service.create_b2c_engagement(
                        db,
                        user_first_name=user.first_name,
                        engagement_date=start_date,
                        city=user.city,
                        assessment_package_id=ap_id,
                        diagnostic_package_id=diag_id,
                        engagement_type=bio_ai_type_id,
                        blood_collection_type=BloodCollectionType.home_collection,
                        consultations=consultations,
                        address=user.address,
                        pincode=user.pin_code,
                        state=getattr(user, "state", None),
                        country=getattr(user, "country", None),
                        create_profile_on_metsights=True,
                        enroll_for_fitprint_full=False,
                    )
                    engagement.start_date = start_date
                    engagement.end_date = end_date
                    await db.flush()

                    await self._engagements_service.enroll_user_in_engagement(
                        db,
                        engagement=engagement,
                        user_id=user.user_id,
                        engagement_date=start_date,
                        slot_start_time=default_slot,
                        booked_by_user_id=int(user.user_id),
                    )

                    assigned_at = datetime.combine(start_date, time.min, tzinfo=timezone.utc)
                    completed_flag = bool(row.get("is_complete")) or end_date < today
                    instance = await self._assessments_service.create_instance_for_metsights_record(
                        db,
                        user_id=user.user_id,
                        engagement_id=engagement.engagement_id,
                        package_id=ap_id,
                        metsights_record_id=mrid,
                        metsights_is_complete=completed_flag,
                        ip_address=ip_address,
                        user_agent=user_agent,
                        endpoint=endpoint,
                        assigned_at=assigned_at,
                    )
                    if completed_flag:
                        instance.completed_at = datetime.combine(end_date, time.min, tzinfo=timezone.utc)
                        await db.flush()

                    if end_date < today:
                        engagement.status = "completed"
                        await db.flush()

                    created += 1
                except AppError as exc:
                    failed += 1
                    if len(failures) < max_detail_items:
                        failures.append(
                            {
                                "metsights_profile_id": profile_id,
                                "reason": exc.message or f"Failed to import record {mrid}",
                            }
                        )
                except Exception as exc:
                    logger.exception("engagements sync failed for record %s", mrid)
                    failed += 1
                    if len(failures) < max_detail_items:
                        failures.append(
                            {
                                "metsights_profile_id": profile_id,
                                "reason": str(exc),
                            }
                        )

        next_page: int | None = page + 1 if (page * _PAGE_SIZE) < users_total else None

        return {
            "page": page,
            "page_size": _PAGE_SIZE,
            "users_total": users_total,
            "users_on_page": len(users),
            "next_page": next_page,
            "created": created,
            "skipped": skipped,
            "failed": failed,
            "failures": failures,
            "skipped_items": skipped_items,
        }
