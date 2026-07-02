"""Engagement console service."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.employee.access_control import ensure_console_access, ensure_employee_present
from modules.employee.models import EmployeeRole
from modules.employee.service import EmployeeContext
from modules.engagements.models import Engagement
from modules.engagements.repository import EngagementsRepository
from modules.engagements.service import _participant_enrollment_to_dict


class ConsoleService:
    def __init__(self, repository: EngagementsRepository):
        self._repository = repository

    @staticmethod
    def _engagement_to_console_dict(engagement: Engagement) -> dict:
        return {
            "engagement_id": engagement.engagement_id,
            "engagement_name": engagement.engagement_name,
            "engagement_code": engagement.engagement_code,
            "start_date": engagement.start_date,
            "end_date": engagement.end_date,
            "status": engagement.status,
            "participant_count": engagement.participant_count,
        }

    async def list_console_engagements(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
    ) -> list[dict]:
        ensure_employee_present(employee)
        if employee.role == EmployeeRole.admin:
            engagements = await self._repository.list_running_engagements(db)
        elif employee.role == EmployeeRole.onboarding_assistant:
            engagements = await self._repository.list_running_engagements_for_assigned_employee(
                db, employee_id=employee.employee_id
            )
        else:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )
        return [self._engagement_to_console_dict(e) for e in engagements]

    async def get_engagement_for_console(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
    ) -> dict:
        await ensure_console_access(db, employee, engagement_id, repository=self._repository)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        return self._engagement_to_console_dict(engagement)

    async def list_participants_for_console(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        page: int,
        limit: int,
    ) -> tuple[list[dict], int]:
        await ensure_console_access(db, employee, engagement_id, repository=self._repository)

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
