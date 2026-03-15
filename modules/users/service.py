"""Users service.

This module owns user business rules.
Auth may only use it for existence checks.
"""

from __future__ import annotations

from typing import Optional

import logging
import random
from datetime import datetime, time, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.employee.models import Employee
from modules.audit.service import AuditService
from modules.users.models import User, UserPreference
from modules.users.repository import UsersRepository
from modules.users.schemas import (
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


class UsersService:
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
    ):
        self._repository = repository
        self._audit_service = audit_service
        self._engagements_service = engagements_service
        self._assessments_service = assessments_service

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
            engagement_type = (row.engagement_type or "").strip().lower()
            if engagement_type != "b2b":
                engagement_type = "b2c"

            slot_duration_minutes = int(row.slot_duration or 0)
            slot_start_dt = datetime.combine(row.engagement_date, row.slot_start_time)
            slot_end_dt = slot_start_dt + timedelta(minutes=slot_duration_minutes)

            is_b2b = engagement_type == "b2b"
            location_display = row.engagement_city if is_b2b else (row.user_address or row.user_city or "")

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

        if not current_user.email or "@" not in current_user.email:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        local_part, domain = current_user.email.split("@", 1)
        generated_email = None
        for _ in range(200):
            suffix = random.randint(1000, 9999)
            candidate = f"{local_part}+{suffix}@{domain}"
            existing = await self._repository.get_user_by_email(db, candidate)
            if existing is None:
                generated_email = candidate
                break

        if generated_email is None:
            raise AppError(status_code=409, error_code="CONFLICT", message="Unable to create profile")

        created = await self._repository.create_sub_profile(
            db,
            current_user,
            {
                "first_name": data.first_name,
                "last_name": data.last_name,
                "date_of_birth": data.date_of_birth,
                "gender": data.gender,
                "relationship": data.relationship,
                "city": data.city,
                "email": generated_email,
            },
        )

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

        updated = await self._repository.update_user_partial(db, target_user_id, data.model_dump(exclude_unset=True))
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
        if current_user.parent_id is None:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Already a primary user")

        parent_user = await self._repository.get_user_by_id(db, current_user.parent_id)
        if parent_user is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

        if current_user.phone == parent_user.phone:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Cannot unlink without an independent phone number. Update your phone number first before unlinking.",
            )

        local_part = str(data.email).split("@", 1)[0]
        if "+" in local_part:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Please provide a real email address")

        existing = await self._repository.get_user_by_email(db, str(data.email))
        if existing is not None and existing.user_id != current_user.user_id:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Email already exists")

        updated = await self._repository.update_user_partial(
            db,
            current_user.user_id,
            {
                "parent_id": None,
                "relationship": "self",
                "email": str(data.email),
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
            user_id=current_user.user_id,
            session_id=None,
        )

        return updated

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

        updated = await self._repository.update_user_profile(db, user=user, payload=payload)

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

    def _create_metsights_profile(self, user: User) -> None:
        """Legacy placeholder.

        This module does not call external services during onboarding.
        """

        _ = user
        return None

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
            phone=payload.phone,
            email=str(payload.email) if payload.email is not None else None,
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

        if self._engagements_service is None:
            raise RuntimeError("Engagements service is required")

        # Create a new engagement for this user.
        # B2C engagements are created with no onboarding assistants by default.
        # diagnostic_package_id is set to None as it's not required for now.
        engagement = await self._engagements_service.create_b2c_engagement(
            db,
            user_first_name=user.first_name,
            engagement_date=payload.blood_collection_date,
            city=user.city,
            assessment_package_id=1,
            diagnostic_package_id=None,
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
        await self._assessments_service.ensure_instance_assigned(
            db,
            user_id=user.user_id,
            engagement_id=engagement.engagement_id,
            package_id=engagement.assessment_package_id,
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
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
        await self._assessments_service.ensure_instance_assigned(
            db,
            user_id=user.user_id,
            engagement_id=engagement.engagement_id,
            package_id=engagement.assessment_package_id,
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
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
