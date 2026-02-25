"""Engagements service.

Business rules:
- Engagement creation
- Enrolling users by creating `engagement_time_slots`
- Updating `participant_count`
"""

from __future__ import annotations

import secrets
import string
from datetime import date, time

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.employee.service import EmployeeContext
from modules.engagements.models import Engagement, EngagementTimeSlot
from modules.engagements.repository import EngagementsRepository
from modules.engagements.schemas import EngagementCreateRequest, EngagementUpdateRequest
from modules.organizations.repository import OrganizationsRepository


def _generate_engagement_code(length: int = 8) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


_ALLOWED_ENGAGEMENT_STATUS = {"active", "inactive", "archived"}


def _normalize_status(value: str | None) -> str:
    return (value or "").strip().lower()


class EngagementsService:
    def __init__(
        self,
        repository: EngagementsRepository,
        audit_service: AuditService | None = None,
        organizations_repository: OrganizationsRepository | None = None,
    ):
        self._repository = repository
        self._audit_service = audit_service
        self._organizations_repository = organizations_repository

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
            metsights_engagement_id=None,
            organization_id=payload.organization_id,
            engagement_code=code,
            engagement_type=payload.engagement_type,
            assessment_package_id=payload.assessment_package_id,
            diagnostic_package_id=payload.diagnostic_package_id,
            city=payload.city,
            slot_duration=payload.slot_duration,
            start_date=payload.start_date,
            end_date=payload.end_date,
            status="active",
            participant_count=0,
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
    ) -> tuple[list[Engagement], int]:
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

        return engagements, total

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
        engagement.slot_duration = payload.slot_duration
        engagement.start_date = payload.start_date
        engagement.end_date = payload.end_date
        engagement.metsights_engagement_id = payload.metsights_engagement_id

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
        assessment_package_id: int,
        diagnostic_package_id: int | None = None,
    ) -> Engagement:
        """Create a B2C engagement with no onboarding assistants assigned by default.
        
        B2C engagements are created automatically during public user onboarding.
        Onboarding assistant assignments can be added later if needed.
        
        diagnostic_package_id is optional and can be None for engagements that don't require diagnostics.
        """
        name_part = (user_first_name or "user").strip() or "user"
        engagement_name = f"{name_part}-{engagement_date.isoformat()}"

        engagement = Engagement(
            engagement_name=engagement_name,
            metsights_engagement_id=None,
            organization_id=None,
            engagement_code=_generate_engagement_code(),
            engagement_type="healthcamp",
            assessment_package_id=assessment_package_id,
            diagnostic_package_id=diagnostic_package_id,
            city=city,
            slot_duration=20,
            start_date=engagement_date,
            end_date=engagement_date,
            status="active",
            participant_count=0,
        )
        return await self._repository.create_engagement(db, engagement)

    async def enroll_user_in_engagement(
        self,
        db: AsyncSession,
        *,
        engagement: Engagement,
        user_id: int,
        engagement_date: date,
        slot_start_time: time,
        increment_participant_count: bool = False,
    ) -> EngagementTimeSlot:
        if (engagement.status or "").lower() != "active":
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Engagement is no longer active")

        if increment_participant_count:
            engagement.participant_count = int(engagement.participant_count or 0) + 1
            await self._repository.update_engagement(db, engagement)

        slot = EngagementTimeSlot(
            engagement_id=engagement.engagement_id,
            user_id=user_id,
            engagement_date=engagement_date,
            slot_start_time=slot_start_time,
        )
        return await self._repository.create_time_slot(db, slot)

    async def list_participants_for_engagement_code(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_code: str,
        page: int,
        limit: int,
    ) -> tuple[list[dict], int]:
        """Fetch all distinct users enrolled in a specific engagement by code.
        
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

        # Count total distinct participants
        total = await self._repository.count_participants_by_engagement_code(
            db,
            engagement_code=engagement_code,
        )

        # Transform tuple results to dictionary format
        result = []
        for row in participants:
            user_id, first_name, last_name, phone, email, city, status = row
            result.append({
                "user_id": user_id,
                "first_name": first_name,
                "last_name": last_name,
                "phone": phone,
                "email": email,
                "city": city,
                "status": status,
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
        """Fetch all distinct users enrolled in all B2C engagements.
        
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

        # Count total distinct participants
        total = await self._repository.count_participants_for_b2c_engagements(db)

        # Transform tuple results to dictionary format
        result = []
        for row in participants:
            user_id, first_name, last_name, phone, email, city, status = row
            result.append({
                "user_id": user_id,
                "first_name": first_name,
                "last_name": last_name,
                "phone": phone,
                "email": email,
                "city": city,
                "status": status,
            })

        return result, total
