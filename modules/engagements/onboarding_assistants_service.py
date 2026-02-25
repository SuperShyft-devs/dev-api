"""Engagement onboarding assistant assignment service."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.employee.service import EmployeeContext, EmployeeService
from modules.engagements.models import OnboardingAssistantAssignment
from modules.engagements.repository import EngagementsRepository


def _normalize_int(value: int) -> int:
    """Normalize and validate integer input."""
    if not isinstance(value, int) or value <= 0:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
    return value


class OnboardingAssistantsService:
    """Business logic for onboarding assistant assignment to engagements."""

    def __init__(
        self,
        repository: EngagementsRepository,
        employee_service: EmployeeService,
        audit_service: AuditService | None = None,
    ):
        self._repository = repository
        self._employee_service = employee_service
        self._audit_service = audit_service

    def _ensure_employee_access(self, employee: EmployeeContext | None) -> None:
        """Verify that the current user is an employee."""
        if employee is None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

    def _require_audit_service(self) -> AuditService:
        """Ensure audit service is available."""
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        return self._audit_service

    async def list_onboarding_assistants_for_engagement(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
    ) -> list[dict]:
        """List all employees assigned as onboarding assistants to an engagement."""
        self._ensure_employee_access(employee)

        engagement_id = _normalize_int(engagement_id)

        # Verify engagement exists
        engagement = await self._repository.get_engagement_by_id(db, engagement_id=engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        # Get all assignments for this engagement
        assignments = await self._repository.list_onboarding_assistant_assignments(db, engagement_id=engagement_id)
        employee_ids = [assignment.employee_id for assignment in assignments]

        # Get employee details via employee service
        # This keeps module boundaries strict
        employees: list[dict] = []
        for employee_id in employee_ids:
            try:
                emp = await self._employee_service.get_employee_details(
                    db,
                    employee=employee,
                    employee_id=employee_id,
                )
            except AppError as exc:
                # If an employee was removed, we skip it
                if exc.status_code == 404:
                    continue
                raise

            employees.append(
                {
                    "employee_id": emp.employee_id,
                    "user_id": emp.user_id,
                    "role": emp.role,
                    "status": emp.status,
                }
            )

        return employees

    async def assign_onboarding_assistants_to_engagement(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        employee_ids: list[int],
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        """Assign one or more employees as onboarding assistants to an engagement."""
        self._ensure_employee_access(employee)

        engagement_id = _normalize_int(engagement_id)

        # Verify engagement exists
        engagement = await self._repository.get_engagement_by_id(db, engagement_id=engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        if not isinstance(employee_ids, list) or len(employee_ids) == 0:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        # Normalize and deduplicate employee IDs
        normalized_ids: list[int] = []
        seen: set[int] = set()
        for raw in employee_ids:
            emp_id = _normalize_int(raw)
            if emp_id in seen:
                continue
            seen.add(emp_id)
            normalized_ids.append(emp_id)

        added: list[int] = []
        skipped: list[int] = []

        for emp_id in normalized_ids:
            # Validate employee exists via employee service
            await self._employee_service.get_employee_details(
                db,
                employee=employee,
                employee_id=emp_id,
            )

            # Check if already assigned
            existing = await self._repository.get_onboarding_assistant_assignment(
                db,
                engagement_id=engagement_id,
                employee_id=emp_id,
            )
            if existing is not None:
                skipped.append(emp_id)
                continue

            # Create assignment
            assignment = OnboardingAssistantAssignment(engagement_id=engagement_id, employee_id=emp_id)
            await self._repository.create_onboarding_assistant_assignment(db, assignment)
            added.append(emp_id)

        # Audit logging
        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_ASSIGN_ONBOARDING_ASSISTANTS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return {
            "engagement_id": engagement_id,
            "added_employee_ids": added,
            "skipped_employee_ids": skipped,
        }

    async def remove_onboarding_assistant_from_engagement(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        employee_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        """Remove an employee's assignment from an engagement."""
        self._ensure_employee_access(employee)

        engagement_id = _normalize_int(engagement_id)
        employee_id = _normalize_int(employee_id)

        # Verify engagement exists
        engagement = await self._repository.get_engagement_by_id(db, engagement_id=engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        # Delete the assignment
        deleted = await self._repository.delete_onboarding_assistant_assignment(
            db,
            engagement_id=engagement_id,
            employee_id=employee_id,
        )
        if deleted == 0:
            raise AppError(
                status_code=404,
                error_code="ONBOARDING_ASSISTANT_ASSIGNMENT_NOT_FOUND",
                message="Employee is not assigned to this engagement",
            )

        # Audit logging
        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_REMOVE_ONBOARDING_ASSISTANT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return {"engagement_id": engagement_id, "removed_employee_id": employee_id}
