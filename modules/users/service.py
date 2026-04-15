"""Users service.

This module owns user business rules.
Auth may only use it for existence checks.
"""

from __future__ import annotations

from typing import Optional

import csv
import io
import logging
import random
from datetime import date, datetime, time, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from core.exceptions import AppError
from modules.employee.models import Employee
from modules.audit.service import AuditService
from modules.metsights.service import MetsightsService
from modules.platform_settings.service import PlatformSettingsService
from modules.users.models import User, UserPreference
from modules.users.repository import UsersRepository
from modules.engagements.enums import EngagementKind
from modules.users.schemas import (
    BookBioAiBatchRequest,
    BookBioAiRequest,
    BookBloodTestBatchRequest,
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

_METSIGHTS_HEADER_ALIASES: dict[str, str] = {
    "id": "id",
    "created date": "created_date",
    "first name": "first_name",
    "last name": "last_name",
    "phone": "phone",
    "email": "email",
    "gender": "gender",
    "age": "age",
}


def _metsights_canonical_header(cell: str) -> str | None:
    raw = (cell or "").strip().lower().replace("#", " ").strip()
    key = " ".join(raw.split())
    return _METSIGHTS_HEADER_ALIASES.get(key)


def _parse_csv_age(raw: str | None) -> int | None:
    if raw is None or str(raw).strip() == "":
        return None
    try:
        v = int(float(str(raw).strip()))
    except ValueError:
        return None
    if 1 <= v <= 120:
        return v
    return None


def _normalize_import_phone(raw: str | None) -> str:
    s = (raw or "").strip().replace(" ", "").replace("-", "")
    return s


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
    ):
        self._repository = repository
        self._audit_service = audit_service
        self._engagements_service = engagements_service
        self._assessments_service = assessments_service
        self._platform_settings_service = platform_settings_service
        self._metsights_service = metsights_service

    async def get_existing_user_by_phone(self, db: AsyncSession, phone: str) -> Optional[User]:
        return await self._repository.get_user_by_phone(db, phone)

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
            existing_phone = await self._repository.get_user_by_phone(db, data.phone)
            if existing_phone is not None and existing_phone.user_id != current_user.user_id:
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
            existing_phone = await self._repository.get_user_by_phone(db, new_phone)
            if existing_phone is not None and existing_phone.user_id != user.user_id:
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

        We only store the start time in `engagement_time_slots.slot_start_time`.
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

    async def _book_bio_ai_for_user(
        self,
        db: AsyncSession,
        *,
        user: User,
        blood_collection_date: date,
        blood_collection_time_slot: str,
        diagnostic_package_id: int | None,
        address: str | None,
        pincode: str | None,
        city: str | None,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> UserOnboardResponse:
        """Bio AI booking for one user (engagement + slot + assessment + Metsights when configured)."""

        if self._engagements_service is None:
            raise RuntimeError("Engagements service is required")
        if self._platform_settings_service is None:
            raise RuntimeError("Platform settings service is required")
        if self._assessments_service is None:
            raise RuntimeError("Assessments service is required")

        await self._ensure_metsights_profile_id(db, user=user)

        if not user.is_participant:
            await self._repository.update_user_partial(db, user.user_id, {"is_participant": True})
            user.is_participant = True

        assessment_package_id, default_diagnostic_id = await self._platform_settings_service.resolve_b2c_default_package_ids(
            db
        )
        resolved_diagnostic = diagnostic_package_id if diagnostic_package_id is not None else default_diagnostic_id
        await self._platform_settings_service.ensure_active_b2c_packages(db, assessment_package_id, resolved_diagnostic)

        eng_city = (city or "").strip() or user.city
        engagement = await self._engagements_service.create_b2c_engagement(
            db,
            user_first_name=user.first_name,
            engagement_date=blood_collection_date,
            city=eng_city,
            assessment_package_id=assessment_package_id,
            diagnostic_package_id=resolved_diagnostic,
            engagement_type=EngagementKind.bio_ai,
            address=address,
            pincode=pincode,
        )

        slot_start = self._parse_time_slot(blood_collection_time_slot)
        time_slot = await self._engagements_service.enroll_user_in_engagement(
            db,
            engagement=engagement,
            user_id=user.user_id,
            engagement_date=blood_collection_date,
            slot_start_time=slot_start,
            increment_participant_count=False,
        )

        metsights_record_id: str | None = None
        if (settings.METSIGHTS_API_KEY or "").strip():
            fresh_user = await self._repository.get_user_by_id(db, user.user_id)
            if fresh_user is None:
                raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")
            profile_id = (fresh_user.metsights_profile_id or "").strip()
            if not profile_id:
                raise AppError(
                    status_code=422,
                    error_code="METSIGHTS_PROFILE_REQUIRED",
                    message="Profile is incomplete for health record sync; provide first name, last name, phone, and gender",
                )
            if self._metsights_service is None:
                raise RuntimeError("Metsights service is required")

            package = await self._assessments_service.get_package_by_id(db, assessment_package_id)
            assessment_type_code = (getattr(package, "assessment_type_code", None) or "").strip() if package else ""
            if not assessment_type_code:
                raise AppError(
                    status_code=422,
                    error_code="INVALID_STATE",
                    message="Assessment package is missing Metsights assessment type",
                )
            metsights_record_id = await self._metsights_service.create_record_for_profile(
                profile_id=profile_id,
                assessment_type_code=assessment_type_code,
            )

        ap_id = engagement.assessment_package_id
        if ap_id is None:
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Assessment package is required for Bio AI booking")

        assessment_instance = await self._assessments_service.ensure_instance_assigned(
            db,
            user_id=user.user_id,
            engagement_id=engagement.engagement_id,
            package_id=int(ap_id),
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
            metsights_record_id=metsights_record_id,
        )

        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        await self._audit_service.log_event(
            db,
            action="USER_BOOK_BIO_AI",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user.user_id,
            session_id=None,
        )

        return UserOnboardResponse(
            user_id=user.user_id,
            created=False,
            is_participant=True,
            engagement_id=engagement.engagement_id,
            engagement_code=engagement.engagement_code,
            time_slot_id=time_slot.time_slot_id,
            assessment_instance_id=assessment_instance.assessment_instance_id,
            metsights_record_id=assessment_instance.metsights_record_id,
        )

    async def _book_blood_test_for_user(
        self,
        db: AsyncSession,
        *,
        user: User,
        blood_collection_date: date,
        blood_collection_time_slot: str,
        diagnostic_package_id: int,
        address: str,
        pincode: str,
        city: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> UserOnboardResponse:
        """Diagnostic-only booking: engagement + slot (no assessment instance, no Metsights)."""

        if self._engagements_service is None:
            raise RuntimeError("Engagements service is required")
        if self._platform_settings_service is None:
            raise RuntimeError("Platform settings service is required")

        if not user.is_participant:
            await self._repository.update_user_partial(db, user.user_id, {"is_participant": True})
            user.is_participant = True

        await self._platform_settings_service.ensure_active_diagnostic_package(db, diagnostic_package_id)

        engagement = await self._engagements_service.create_b2c_engagement(
            db,
            user_first_name=user.first_name,
            engagement_date=blood_collection_date,
            city=city,
            assessment_package_id=None,
            diagnostic_package_id=diagnostic_package_id,
            engagement_type=EngagementKind.diagnostic,
            address=address,
            pincode=pincode,
        )

        slot_start = self._parse_time_slot(blood_collection_time_slot)
        time_slot = await self._engagements_service.enroll_user_in_engagement(
            db,
            engagement=engagement,
            user_id=user.user_id,
            engagement_date=blood_collection_date,
            slot_start_time=slot_start,
            increment_participant_count=False,
        )

        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        await self._audit_service.log_event(
            db,
            action="USER_BOOK_BLOOD_TEST",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user.user_id,
            session_id=None,
        )

        return UserOnboardResponse(
            user_id=user.user_id,
            created=False,
            is_participant=True,
            engagement_id=engagement.engagement_id,
            engagement_code=engagement.engagement_code,
            time_slot_id=time_slot.time_slot_id,
            assessment_instance_id=None,
            metsights_record_id=None,
        )

    async def book_bio_ai_batch_for_primary(
        self,
        db: AsyncSession,
        *,
        actor: User,
        payload: BookBioAiBatchRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> list[UserOnboardResponse]:
        if actor.parent_id is not None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="Batch booking is only available for primary accounts",
            )
        results: list[UserOnboardResponse] = []
        for m in payload.members:
            target = await self._get_user_bookable_by_primary(db, primary_user_id=actor.user_id, target_user_id=m.user_id)
            results.append(
                await self._book_bio_ai_for_user(
                    db,
                    user=target,
                    blood_collection_date=m.blood_collection_date,
                    blood_collection_time_slot=m.blood_collection_time_slot,
                    diagnostic_package_id=m.diagnostic_package_id,
                    address=m.address,
                    pincode=m.pincode,
                    city=m.city,
                    ip_address=ip_address,
                    user_agent=user_agent,
                    endpoint=endpoint,
                )
            )
        return results

    async def book_blood_test_batch_for_primary(
        self,
        db: AsyncSession,
        *,
        actor: User,
        payload: BookBloodTestBatchRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> list[UserOnboardResponse]:
        if actor.parent_id is not None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="Batch booking is only available for primary accounts",
            )
        results: list[UserOnboardResponse] = []
        for m in payload.members:
            target = await self._get_user_bookable_by_primary(db, primary_user_id=actor.user_id, target_user_id=m.user_id)
            results.append(
                await self._book_blood_test_for_user(
                    db,
                    user=target,
                    blood_collection_date=m.blood_collection_date,
                    blood_collection_time_slot=m.blood_collection_time_slot,
                    diagnostic_package_id=m.diagnostic_package_id,
                    address=m.address,
                    pincode=m.pincode,
                    city=m.city,
                    ip_address=ip_address,
                    user_agent=user_agent,
                    endpoint=endpoint,
                )
            )
        return results

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
        existing = await self._repository.get_user_by_phone(db, payload.phone)
        if existing is None and payload.email is not None:
            existing = await self._repository.get_user_by_email(db, str(payload.email))

        if existing is not None:
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
        )

        total = await self._repository.count_users(
            db,
            phone=phone,
            email=email,
            status=status,
            is_participant=is_participant,
        )

        return users, total

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

        existing = await self._repository.get_user_by_phone(db, payload.phone)
        if existing is not None and existing.user_id != user_id:
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

    async def delete_user_by_employee(
        self,
        db: AsyncSession,
        *,
        employee,
        user_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
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

        await self._repository.delete_user_related_data(db, user_ids_to_delete)
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
        # B2C engagements are created with no onboarding assistants by default.
        engagement = await self._engagements_service.create_b2c_engagement(
            db,
            user_first_name=user.first_name,
            engagement_date=payload.blood_collection_date,
            city=payload.city or user.city,
            assessment_package_id=assessment_package_id,
            diagnostic_package_id=diagnostic_package_id,
            engagement_type=EngagementKind.bio_ai,
            address=payload.address,
            pincode=payload.pincode,
        )

        # Enroll the user by booking a time slot.
        slot_start = self._parse_time_slot(payload.blood_collection_time_slot)
        time_slot = await self._engagements_service.enroll_user_in_engagement(
            db,
            engagement=engagement,
            user_id=user.user_id,
            engagement_date=payload.blood_collection_date,
            slot_start_time=slot_start,
            increment_participant_count=False,
        )

        # Create assessment instance for this user and engagement.
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
                    await self._assessments_service.ensure_instance_assigned(
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

        return UserOnboardResponse(
            user_id=user.user_id,
            created=created,
            is_participant=True,
            engagement_id=engagement.engagement_id,
            engagement_code=engagement.engagement_code,
            time_slot_id=time_slot.time_slot_id,
        )

    async def book_bio_ai_for_authenticated_user(
        self,
        db: AsyncSession,
        *,
        user: User,
        payload: BookBioAiRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> UserOnboardResponse:
        """B2C Bio AI booking for an existing JWT user (new engagement + slot + assessment instance)."""

        return await self._book_bio_ai_for_user(
            db,
            user=user,
            blood_collection_date=payload.blood_collection_date,
            blood_collection_time_slot=payload.blood_collection_time_slot,
            diagnostic_package_id=payload.diagnostic_package_id,
            address=payload.address,
            pincode=payload.pincode,
            city=payload.city,
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
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
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")

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
        await self._ensure_metsights_profile_id(db, user=user)

        slot_start = self._parse_time_slot(payload.blood_collection_time_slot)
        # Enroll the user by booking a time slot.
        time_slot = await self._engagements_service.enroll_user_in_engagement(
            db,
            engagement=engagement,
            user_id=user.user_id,
            engagement_date=payload.blood_collection_date,
            slot_start_time=slot_start,
            increment_participant_count=False,
        )

        # Create assessment instance for this user and engagement.
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
                    await self._assessments_service.ensure_instance_assigned(
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
            time_slot_id=time_slot.time_slot_id,
        )

    def _metsights_csv_cell(self, raw_row: dict[str, str], colmap: dict[str, str], key: str) -> str:
        h = colmap.get(key)
        if not h:
            return ""
        return (raw_row.get(h) or "").strip()

    async def _import_one_metsights_csv_row(
        self,
        db: AsyncSession,
        *,
        raw_row: dict[str, str],
        colmap: dict[str, str],
        engagement,
        slot_date: date,
        slot_time: time,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict[str, str]:
        """Return {status, reason} where status is imported | skipped | failed."""

        def _out(status: str, reason: str = "") -> dict[str, str]:
            return {"status": status, "reason": reason}

        metsights_id = self._metsights_csv_cell(raw_row, colmap, "id")
        if not metsights_id:
            return _out("failed", "Missing Metsights id")

        phone = _normalize_import_phone(self._metsights_csv_cell(raw_row, colmap, "phone"))
        if len(phone) < 5:
            return _out("failed", "Invalid or missing phone")

        age = _parse_csv_age(self._metsights_csv_cell(raw_row, colmap, "age"))
        if age is None:
            return _out("failed", "Invalid or missing age")

        email_raw = self._metsights_csv_cell(raw_row, colmap, "email")
        email = str(email_raw) if email_raw else None

        first_name = self._metsights_csv_cell(raw_row, colmap, "first_name") or None
        last_name = self._metsights_csv_cell(raw_row, colmap, "last_name") or None
        gender_raw = self._metsights_csv_cell(raw_row, colmap, "gender")
        gender = gender_raw or None

        if self._assessments_service is None or self._engagements_service is None:
            raise RuntimeError("Engagements and assessments services are required")

        existing_ai = await self._assessments_service.get_instance_by_metsights_record_id(db, metsights_id)
        if existing_ai is not None:
            if existing_ai.engagement_id != engagement.engagement_id:
                return _out("failed", "Metsights id already linked to another engagement")

        patch_data = {
            "first_name": first_name,
            "last_name": last_name,
            "age": age,
            "email": email,
            "date_of_birth": None,
            "gender": gender,
            "address": None,
            "pin_code": None,
            "city": None,
            "state": None,
            "country": None,
        }
        code = (engagement.engagement_code or "").strip()
        create_data = {
            **patch_data,
            "phone": phone,
            "referred_by": code or None,
            "is_participant": True,
            "status": "active",
        }

        user, _created = await self._get_or_create_user_for_onboarding(
            db,
            phone=phone,
            email=email,
            patch_data={**patch_data, "is_participant": True, "referred_by": code or None},
            create_data=create_data,
        )

        if existing_ai is not None:
            if existing_ai.user_id != user.user_id:
                return _out("failed", "Metsights id already linked to another user")
            return _out("skipped", "Already imported for this engagement")

        if await self._engagements_service.user_has_slot_for_engagement(
            db, user_id=user.user_id, engagement_id=engagement.engagement_id
        ):
            return _out("skipped", "User already enrolled in this engagement")

        await self._engagements_service.enroll_user_in_engagement(
            db,
            engagement=engagement,
            user_id=user.user_id,
            engagement_date=slot_date,
            slot_start_time=slot_time,
            increment_participant_count=True,
        )

        try:
            await self._assessments_service.ensure_instance_assigned(
                db,
                user_id=user.user_id,
                engagement_id=engagement.engagement_id,
                package_id=engagement.assessment_package_id,
                ip_address=ip_address,
                user_agent=user_agent,
                endpoint=endpoint,
                metsights_record_id=metsights_id,
            )
        except AppError as exc:
            return _out("failed", exc.message or "Assessment assignment failed")

        return _out("imported", "")

    async def import_metsights_csv_for_engagement(
        self,
        db: AsyncSession,
        *,
        employee,
        engagement_id: int,
        file_content: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        """Parse a Metsights-export CSV and enroll participants into an engagement."""

        self._ensure_employee_access(employee)
        if self._engagements_service is None or self._assessments_service is None:
            raise RuntimeError("Engagements and assessments services are required")

        engagement = await self._engagements_service.get_engagement_details_for_employee(
            db,
            employee=employee,
            engagement_id=engagement_id,
        )
        if (engagement.status or "").lower() != "active":
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Engagement is not active",
            )

        slot_date = engagement.start_date or engagement.end_date
        if slot_date is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Engagement must have start_date or end_date set before importing participants",
            )

        slot_time = time(10, 0)
        text = (file_content or "").strip()
        if not text:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="CSV file is empty")

        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="CSV has no header row")

        colmap: dict[str, str] = {}
        for h in reader.fieldnames:
            if h is None:
                continue
            canon = _metsights_canonical_header(h)
            if canon:
                colmap[canon] = h

        required = {"id", "phone", "age"}
        missing = required - colmap.keys()
        if missing:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"CSV missing required columns: {', '.join(sorted(missing))}",
            )

        rows_out: list[dict] = []
        imported = skipped = failed = 0
        line_no = 1

        for raw_row in reader:
            line_no += 1
            one = await self._import_one_metsights_csv_row(
                db,
                raw_row=raw_row,
                colmap=colmap,
                engagement=engagement,
                slot_date=slot_date,
                slot_time=slot_time,
                ip_address=ip_address,
                user_agent=user_agent,
                endpoint=endpoint,
            )
            rows_out.append({"line": line_no, "status": one["status"], "reason": one["reason"]})
            if one["status"] == "imported":
                imported += 1
            elif one["status"] == "skipped":
                skipped += 1
            else:
                failed += 1

        return {
            "imported": imported,
            "skipped": skipped,
            "failed": failed,
            "rows": rows_out,
        }
