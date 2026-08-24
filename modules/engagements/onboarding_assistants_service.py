"""Engagement onboarding assistant assignment service."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.employee.access_control import (
    ONBOARDING_ASSISTANT_ASSIGNEE_ROLES,
    ensure_admin,
    ensure_org_manager_assignable_to_engagement,
    ensure_valid_onboarding_assistant_assignee_role,
)
from modules.employee.models import Employee, EmployeeRole
from modules.employee.repository import EmployeeRepository
from modules.employee.schemas import EmployeeCreateRequest
from modules.employee.service import EmployeeContext, EmployeeService
from modules.engagements.models import OnboardingAssistantAssignment
from modules.engagements.repository import EngagementsRepository
from modules.users.models import User
from modules.users.schemas import EmployeeCreateUserRequest
from modules.users.service import UsersService


def _normalize_int(value: int) -> int:
    """Normalize and validate integer input."""
    if not isinstance(value, int) or value <= 0:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
    return value


def _existing_user_payload(user: User, employee: Employee | None) -> dict:
    payload: dict = {
        "user_id": user.user_id,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "phone": user.phone,
        "employee": None,
    }
    if employee is not None:
        payload["employee"] = {
            "employee_id": employee.employee_id,
            "role": employee.role.value if isinstance(employee.role, EmployeeRole) else employee.role,
            "status": employee.status,
        }
    return payload


class OnboardingAssistantsService:
    """Business logic for onboarding assistant assignment to engagements."""

    def __init__(
        self,
        repository: EngagementsRepository,
        employee_service: EmployeeService,
        employee_repository: EmployeeRepository,
        users_service: UsersService,
        audit_service: AuditService | None = None,
    ):
        self._repository = repository
        self._employee_service = employee_service
        self._employee_repository = employee_repository
        self._users_service = users_service
        self._audit_service = audit_service

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
        ensure_admin(employee)

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
                emp, first_name, last_name = await self._employee_service.get_employee_details(
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
                    "first_name": first_name,
                    "last_name": last_name,
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
        ensure_admin(employee)

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
            emp, _first_name, _last_name = await self._employee_service.get_employee_details(
                db,
                employee=employee,
                employee_id=emp_id,
            )
            ensure_valid_onboarding_assistant_assignee_role(emp.role)
            await ensure_org_manager_assignable_to_engagement(
                db,
                assignee_user_id=emp.user_id,
                assignee_role=emp.role,
                engagement_id=engagement_id,
                repository=self._repository,
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

    async def create_and_assign_phlebo(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        engagement_id: int,
        name: str,
        phone: str,
        confirm_existing: bool,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        """Create or reuse a phlebo (onboarding assistant) and assign them to an engagement."""
        ensure_admin(employee)

        engagement_id = _normalize_int(engagement_id)

        engagement = await self._repository.get_engagement_by_id(db, engagement_id=engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        existing_user = await self._users_service.resolve_user_by_phone(db, phone)
        if existing_user is not None and not confirm_existing:
            existing_employee = await self._employee_repository.get_by_user_id(db, existing_user.user_id)
            return {
                "status": "confirmation_required",
                "existing_user": _existing_user_payload(existing_user, existing_employee),
            }

        user_created = False
        employee_created = False

        if existing_user is not None:
            user = existing_user
            emp_row = await self._employee_repository.get_by_user_id(db, user.user_id)
            if emp_row is None:
                emp_row = await self._employee_service.create_employee(
                    db,
                    employee=employee,
                    payload=EmployeeCreateRequest(
                        user_id=user.user_id,
                        role=EmployeeRole.onboarding_assistant,
                        status="active",
                    ),
                    ip_address=ip_address,
                    user_agent=user_agent,
                    endpoint=endpoint,
                )
                employee_created = True
            else:
                role = emp_row.role if isinstance(emp_row.role, EmployeeRole) else EmployeeRole(emp_row.role)
                if role not in ONBOARDING_ASSISTANT_ASSIGNEE_ROLES:
                    raise AppError(
                        status_code=422,
                        error_code="INVALID_STATE",
                        message="Employee role cannot be assigned as an onboarding assistant",
                    )
            result_status = "assigned"
        else:
            first_name = name.strip() or None
            user = await self._users_service.create_user_by_employee(
                db,
                employee=employee,
                payload=EmployeeCreateUserRequest(
                    first_name=first_name,
                    phone=phone,
                    status="active",
                ),
                ip_address=ip_address,
                user_agent=user_agent,
                endpoint=endpoint,
            )
            user_created = True
            emp_row = await self._employee_service.create_employee(
                db,
                employee=employee,
                payload=EmployeeCreateRequest(
                    user_id=user.user_id,
                    role=EmployeeRole.onboarding_assistant,
                    status="active",
                ),
                ip_address=ip_address,
                user_agent=user_agent,
                endpoint=endpoint,
            )
            employee_created = True
            result_status = "created"

        assign_result = await self.assign_onboarding_assistants_to_engagement(
            db,
            employee=employee,
            engagement_id=engagement_id,
            employee_ids=[emp_row.employee_id],
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
        )

        return {
            "status": result_status,
            "user_id": user.user_id,
            "employee_id": emp_row.employee_id,
            "user_created": user_created,
            "employee_created": employee_created,
            "engagement_id": engagement_id,
            "added_employee_ids": assign_result["added_employee_ids"],
            "skipped_employee_ids": assign_result["skipped_employee_ids"],
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
        ensure_admin(employee)

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
