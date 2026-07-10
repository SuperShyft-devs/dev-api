"""Users service.

This module owns user business rules.
Auth may only use it for existence checks.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from modules.employee.service import EmployeeContext

import logging
import random
from datetime import date, datetime, time, timedelta

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from core.exceptions import AppError
from modules.employee.models import Employee
from modules.audit.service import AuditService
from modules.metsights.service import MetsightsService
from modules.metsights.sync_service import (
    _normalize_metsights_type_code,
    _parse_iso_date,
    _resolve_active_diagnostic_package_id,
)
from modules.platform_settings.service import PlatformSettingsService
from modules.users.models import User, UserPreference
from modules.users.repository import UsersRepository
from modules.engagements.models import EngagementKind
from common.phone import phone_lookup_candidates as _phone_lookup_candidates
from modules.users.schemas import (
    BookBioAiBatchRequest,
    BookBioAiRequest,
    BookBloodTestBatchRequest,
    BookingPaymentResponse,
    EmployeeCreateUserRequest,
    EmployeeUpdateUserRequest,
    EngagementUserOnboardRequest,
    UpcomingSlotResponse,
    PublicUserOnboardRequest,
    SubProfileCreate,
    SubProfileUpdate,
    UnlinkRequest,
    UserPreferencesUpdate,
    UpdateMyProfileRequest,
    UserOnboardResponse,
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

    async def _is_protected_employee_user(self, db: AsyncSession, user_id: int) -> bool:
        result = await db.execute(select(Employee.employee_id).where(Employee.user_id == user_id).limit(1))
        employee_id = result.scalar_one_or_none()
        return employee_id == _ALWAYS_ACTIVE_EMPLOYEE_ID

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
    ):
        self._repository = repository
        self._audit_service = audit_service
        self._engagements_service = engagements_service
        self._assessments_service = assessments_service
        self._platform_settings_service = platform_settings_service
        self._metsights_service = metsights_service
        self._payments_service = payments_service
        self._notifications_service = notifications_service

    async def get_existing_user_by_phone(self, db: AsyncSession, phone: str) -> Optional[User]:
        return await self._repository.get_user_by_phone(db, phone)

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

    async def get_upcoming_slots(self, db: AsyncSession, *, user_id: int) -> UpcomingSlotResponse:
        rows = await self._repository.get_upcoming_slots(db, user_id)
        if not rows:
            return UpcomingSlotResponse(has_scheduled_slot=False, slots=[])

        slots = []
        for row in rows:
            is_b2b = row.organization_id is not None
            engagement_type = "b2b" if is_b2b else "b2c"

            slot_duration_minutes = int(row.slot_duration or 0)
            slot_start_dt = datetime.combine(row.engagement_date, row.slot_start_time)
            slot_end_dt = slot_start_dt + timedelta(minutes=slot_duration_minutes)

            eng_addr = (getattr(row, "engagement_address", None) or "").strip()
            if eng_addr:
                location_display = eng_addr
            elif is_b2b:
                location_display = (row.engagement_city or "") or ""
            else:
                location_display = (row.user_address or row.user_city or "") or ""

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
                    },
                    "location": {
                        "type": "venue" if is_b2b else "home_collection",
                        "display": location_display,
                    },
                }
            )

        return UpcomingSlotResponse(has_scheduled_slot=True, slots=slots)

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
        engagement = await self._engagements_service.create_b2c_engagement(
            db,
            user_first_name=user.first_name,
            engagement_date=date.fromisoformat(meta["blood_collection_date"]),
            city=eng_city,
            assessment_package_id=assessment_package_id,
            diagnostic_package_id=diagnostic_package_id,
            engagement_type=EngagementKind.bio_ai,
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

        if profile_id:
            await self._engagements_service.update_participant_sync_flags(
                db,
                participant=time_slot,
                is_profile_created_on_metsights=True,
            )

        ip_address = "system"
        user_agent = "post-payment-fulfillment"
        endpoint = "/payments/verify"

        metsights_record_id: str | None = None
        if profile_id and self._metsights_service is not None:
            try:
                package = await self._assessments_service.get_package_by_id(db, assessment_package_id)
                assessment_type_code = (getattr(package, "assessment_type_code", None) or "").strip() if package else ""
                if assessment_type_code:
                    metsights_record_id = await self._metsights_service.create_record_for_profile(
                        profile_id=profile_id,
                        assessment_type_code=assessment_type_code,
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
            if metsights_record_id:
                await self._engagements_service.update_participant_sync_flags(
                    db,
                    participant=time_slot,
                    is_primary_record_id_synced=True,
                )

        if profile_id and self._metsights_service is not None:
            try:
                fitprint_record_id = await self._metsights_service.create_record_for_profile(
                    profile_id=profile_id,
                    assessment_type_code="7",
                )
                fitprint_package = await self._assessments_service.get_package_by_assessment_type_code(
                    db,
                    assessment_type_code="7",
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
                    await self._engagements_service.update_participant_sync_flags(
                        db,
                        participant=time_slot,
                        is_fitprint_record_id_synced=True,
                    )
            except Exception as exc:
                logger.warning(
                    "Metsights FitPrint record creation failed for user_id=%s: %s",
                    user.user_id, str(exc),
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

        engagement = await self._engagements_service.create_b2c_engagement(
            db,
            user_first_name=user.first_name,
            engagement_date=date.fromisoformat(meta["blood_collection_date"]),
            city=meta.get("city") or user.city,
            assessment_package_id=None,
            diagnostic_package_id=diagnostic_package_id,
            engagement_type=EngagementKind.diagnostic,
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

    async def _ensure_metsights_profile_id(self, db: AsyncSession, *, user: User) -> None:
        if user.metsights_profile_id:
            return
        if self._metsights_service is None:
            return

        first_name = (user.first_name or "").strip()
        last_name = (user.last_name or "").strip()
        phone = (user.phone or "").strip()
        gender = self._to_metsights_gender(user.gender)

        if not first_name or not last_name or not phone or gender is None:
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

    def _select_latest_metsights_record_id(self, records: list[dict[str, Any]]) -> str | None:
        def _sort_key(row: dict[str, Any]) -> tuple[str, str]:
            date_value = str(row.get("date") or "").strip()
            created_value = str(row.get("created_at") or "").strip()
            return (date_value, created_value)

        valid_rows = [row for row in records if isinstance(row, dict)]
        if not valid_rows:
            return None
        latest = max(valid_rows, key=_sort_key)
        record_id = str(latest.get("id") or "").strip()
        return record_id or None

    async def _create_metsights_profile_for_engagement(
        self,
        db: AsyncSession,
        *,
        user: User,
        engagement,
    ) -> str | None:
        if self._metsights_service is None:
            return None
        if not bool(engagement.create_profile_on_metsights):
            return (user.metsights_profile_id or "").strip() or None

        engagement_metsights_id = (engagement.metsights_engagement_id or "").strip()
        first_name = (user.first_name or "").strip()
        last_name = (user.last_name or "").strip()
        phone = self._normalize_phone_for_metsights(user.phone)
        gender = self._to_metsights_gender(user.gender)
        if not first_name or not last_name or not phone or gender is None:
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
            )
            if profile_id and self._assessments_service is not None and engagement.assessment_package_id:
                package = await self._assessments_service.get_package_by_id(db, engagement.assessment_package_id)
                assessment_type_code = (getattr(package, "assessment_type_code", None) or "").strip() if package else ""
                if assessment_type_code:
                    try:
                        await self._metsights_service.create_record_for_profile(
                            profile_id=profile_id,
                            assessment_type_code=assessment_type_code,
                        )
                    except Exception as exc:
                        logger.warning(
                            "Metsights record creation failed for profile_id=%s assessment_type_code=%s: %s",
                            profile_id,
                            assessment_type_code,
                            str(exc),
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
    ) -> User:
        if await self._has_phone_conflict(db, phone=payload.phone):
            raise AppError(status_code=409, error_code="CONFLICT", message="User already exists")
        if payload.email is not None:
            existing_email = await self._repository.get_user_by_email(db, str(payload.email))
            if existing_email is not None:
                raise AppError(status_code=409, error_code="CONFLICT", message="User already exists")

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

        actor_user_id = employee.user_id if employee is not None else None
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

        return created

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

    async def get_participant_metsights_stats_for_employee(self, db: AsyncSession, *, employee) -> dict:
        self._ensure_employee_access(employee)
        with_profile, total_participants = await self._repository.count_participant_metsights_stats(db)
        return {
            "with_metsights_profile": with_profile,
            "total_participants": total_participants,
        }

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

        if (payload.phone or "").strip() != (user.phone or "").strip():
            if await self._has_phone_conflict(db, phone=payload.phone, exclude_user_id=int(user_id)):
                raise AppError(status_code=409, error_code="CONFLICT", message="User already exists")

        if payload.email is not None:
            existing_email = await self._repository.get_user_by_email(db, str(payload.email))
            if existing_email is not None and existing_email.user_id != user_id:
                raise AppError(status_code=409, error_code="CONFLICT", message="User already exists")

        data = payload.model_dump()
        data["email"] = str(payload.email) if payload.email is not None else None

        updated = await self._repository.update_user_full(db, user=user, data=data)

        if self._audit_service is None:
            raise RuntimeError("Audit service is required")

        await self._audit_service.log_event(
            db,
            action="EMPLOYEE_UPDATE_USER",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
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
            user_id=employee.user_id,
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
            user_id=employee.user_id,
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
        if employee.user_id in user_ids_to_delete:
            if employee.user_id == user_id:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message="You cannot delete your own account",
                )
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="You cannot delete this user: your account is in the same family tree and would be removed.",
            )

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
            user_id=employee.user_id,
            session_id=None,
        )

        return {"deleted_user_id": user_id, "deleted_user_count": deleted_count}

    async def _get_or_create_user_for_onboarding(
        self,
        db: AsyncSession,
        *,
        phone: str,
        email: str | None,
        patch_data: dict,
        create_data: dict,
    ) -> tuple[User, bool]:
        existing = await self._repository.get_user_by_phone(db, phone)
        if existing is None and email is not None:
            existing = await self._repository.get_user_by_email(db, email)

        if existing is None:
            user = User(**create_data)
            user = await self._repository.create_user(db, user)
            return user, True

        user = await self._repository.patch_missing_fields(db, user=existing, data=patch_data)
        return user, False

    @staticmethod
    def _participant_details_from_onboard_payload(
        payload: PublicUserOnboardRequest | EngagementUserOnboardRequest,
        *,
        source: str,
        participant_user_id: int,
    ) -> dict[str, str]:
        first_name = payload.first_name or ""
        last_name = payload.last_name or ""
        name = f"{first_name} {last_name}".strip()
        email_str = str(payload.email) if payload.email is not None else ""
        age_str = str(payload.age) if payload.age is not None else ""
        return {
            "name": name,
            "email": email_str,
            "phone": str(payload.phone or ""),
            "age": age_str,
            "gender": str(payload.gender or ""),
            "address": str(payload.address or ""),
            "pincode": str(payload.pincode or ""),
            "collection_date": str(payload.blood_collection_date),
            "collection_time": str(payload.blood_collection_time_slot),
            "engagement": source,
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
    ) -> None:
        """Fire-and-forget: notify onboarding assistants using the engagement's service key."""
        if self._notifications_service is None or self._engagements_service is None:
            return

        from modules.notifications.onboarding_notify import notify_onboarding_assistants_on_enrollment

        participant_details = self._participant_details_from_onboard_payload(
            payload, source=source, participant_user_id=participant_user_id
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
    ) -> None:
        if self._notifications_service is None or self._engagements_service is None:
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
        """

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
            phone=payload.phone,
            email=email,
            patch_data={**patch_data, "is_participant": True},
            create_data=create_data,
        )
        await self._ensure_metsights_profile_id(db, user=user)

        if self._engagements_service is None:
            raise RuntimeError("Engagements service is required")
        if self._platform_settings_service is None:
            raise RuntimeError("Platform settings service is required")

        assessment_package_id, diagnostic_package_id = await self._platform_settings_service.resolve_b2c_default_package_ids(
            db
        )
        await self._platform_settings_service.ensure_active_b2c_packages(
            db, assessment_package_id, diagnostic_package_id
        )

        # Create a new engagement for this user.
        # B2C engagements auto-assign default onboarding assistants from platform settings.
        engagement = await self._engagements_service.create_b2c_engagement(
            db,
            user_first_name=user.first_name,
            engagement_date=payload.blood_collection_date,
            city=payload.city or user.city,
            assessment_package_id=assessment_package_id,
            diagnostic_package_id=diagnostic_package_id,
            engagement_type=EngagementKind.bio_ai,
            address=payload.address,
            sub_locality=getattr(payload, "sub_locality", None),
            landmark=getattr(payload, "landmark", None),
            pincode=payload.pincode,
            state=getattr(payload, "state", None),
            country=getattr(payload, "country", None),
            latitude=getattr(payload, "latitude", None),
            longitude=getattr(payload, "longitude", None),
        )

        slot_start = self._parse_time_slot(payload.blood_collection_time_slot)
        time_slot = await self._engagements_service.enroll_user_in_engagement(
            db,
            engagement=engagement,
            user_id=user.user_id,
            engagement_date=payload.blood_collection_date,
            slot_start_time=slot_start,
            participants_employee_id=payload.participants_employee_id,
            participant_department=payload.participant_department,
            participant_blood_group=payload.participant_blood_group,
            want_doctor_consultation=payload.want_doctor_consultation,
            want_nutritionist_consultation=payload.want_nutritionist_consultation,
            want_doctor_and_nutritionist_consultation=payload.want_doctor_and_nutritionist_consultation,
            is_profile_created_on_metsights=bool((user.metsights_profile_id or "").strip()),
        )

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

        # Create Metsights record for this assessment instance when integration data is available.
        if self._metsights_service is not None and not (assessment_instance.metsights_record_id or "").strip():
            try:
                package = await self._assessments_service.get_package_by_id(db, engagement.assessment_package_id)
                assessment_type_code = (getattr(package, "assessment_type_code", None) or "").strip() if package else ""
                profile_id = (user.metsights_profile_id or "").strip()
                if profile_id and assessment_type_code:
                    record_id = await self._metsights_service.create_record_for_profile(
                        profile_id=profile_id,
                        assessment_type_code=assessment_type_code,
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

        slot_start = self._parse_time_slot(payload.blood_collection_time_slot)
        validated_department = await self._engagements_service.resolve_participant_department_for_engagement(
            db,
            engagement=engagement,
            participant_department=payload.participant_department,
        )
        time_slot = await self._engagements_service.enroll_user_in_engagement(
            db,
            engagement=engagement,
            user_id=user.user_id,
            engagement_date=payload.blood_collection_date,
            slot_start_time=slot_start,
            participants_employee_id=payload.participants_employee_id,
            participant_department=validated_department,
            participant_blood_group=payload.participant_blood_group,
            want_doctor_consultation=payload.want_doctor_consultation,
            want_nutritionist_consultation=payload.want_nutritionist_consultation,
            want_doctor_and_nutritionist_consultation=payload.want_doctor_and_nutritionist_consultation,
            is_profile_created_on_metsights=False,
            is_primary_record_id_synced=False,
            is_fitprint_record_id_synced=False,
        )

        assessment_instance = None
        if self._assessments_service is None:
            raise RuntimeError("Assessments service is required")
        try:
            assessment_instance = await self._assessments_service.ensure_instance_assigned(
                db,
                user_id=user.user_id,
                engagement_id=engagement.engagement_id,
                package_id=engagement.assessment_package_id,
                ip_address=ip_address,
                user_agent=user_agent,
                endpoint=endpoint,
            )
        except Exception as exc:
            logger.warning(
                "Assessment assignment failed for user_id=%s engagement_id=%s: %s",
                user.user_id,
                engagement.engagement_id,
                str(exc),
            )

        metsights_record_id: str | None = None
        try:
            profile_id = await self._create_metsights_profile_for_engagement(db, user=user, engagement=engagement)
            if profile_id:
                await self._engagements_service.update_participant_sync_flags(
                    db,
                    participant=time_slot,
                    is_profile_created_on_metsights=True,
                )
                records_payload = await self._metsights_service.list_profile_records(profile_id=profile_id)
                records = records_payload if isinstance(records_payload, list) else []
                latest_record_id = self._select_latest_metsights_record_id(records)
                if latest_record_id and assessment_instance is not None:
                    assessment_instance = await self._assessments_service.ensure_instance_assigned(
                        db,
                        user_id=user.user_id,
                        engagement_id=engagement.engagement_id,
                        package_id=engagement.assessment_package_id,
                        ip_address=ip_address,
                        user_agent=user_agent,
                        endpoint=endpoint,
                        metsights_record_id=latest_record_id,
                    )
                    metsights_record_id = latest_record_id
                    await self._engagements_service.update_participant_sync_flags(
                        db,
                        participant=time_slot,
                        is_primary_record_id_synced=True,
                    )

                if bool(engagement.enroll_for_fitprint_full):
                    fitprint_record_id = await self._metsights_service.create_record_for_profile(
                        profile_id=profile_id,
                        assessment_type_code="7",
                    )
                    fitprint_package = await self._assessments_service.get_package_by_assessment_type_code(
                        db,
                        assessment_type_code="7",
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
                        await self._engagements_service.update_participant_sync_flags(
                            db,
                            participant=time_slot,
                            is_fitprint_record_id_synced=True,
                        )
        except Exception as exc:
            logger.warning(
                "Metsights sync failed for user_id=%s engagement_id=%s: %s",
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
                assessment_type_code = (getattr(package, "assessment_type_code", None) or "").strip() if package else ""
                profile_id = (user.metsights_profile_id or "").strip()
                if profile_id and assessment_type_code:
                    record_id = await self._metsights_service.create_record_for_profile(
                        profile_id=profile_id,
                        assessment_type_code=assessment_type_code,
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
                    await self._engagements_service.update_participant_sync_flags(
                        db,
                        participant=time_slot,
                        is_primary_record_id_synced=True,
                    )
                    metsights_record_id = record_id
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
                engagement = await self._engagements_service.create_b2c_engagement(
                    db,
                    user_first_name=user.first_name,
                    engagement_date=engagement_date,
                    city=user.city,
                    assessment_package_id=ap_id,
                    diagnostic_package_id=diag_id,
                    engagement_type=EngagementKind.bio_ai,
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
                    slot_start_time=default_slot,
                    is_profile_created_on_metsights=True,
                    is_primary_record_id_synced=False,
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

                await self._engagements_service.update_participant_sync_flags(
                    db,
                    participant=participant,
                    is_primary_record_id_synced=True,
                )

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
