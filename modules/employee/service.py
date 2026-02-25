"""Employee service.

This module owns employee business rules.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.employee.models import Employee
from modules.employee.repository import EmployeeRepository
from modules.employee.schemas import EmployeeCreateRequest, EmployeeUpdateRequest


@dataclass(frozen=True)
class EmployeeContext:
    """Authenticated employee context."""

    employee_id: int
    user_id: int
    role: str


_ALLOWED_EMPLOYEE_STATUS = {"active", "inactive", "archived"}
_ALLOWED_EMPLOYEE_STATUS_UPDATE = {"active", "inactive"}


def _normalize_status(value: str | None) -> str:
    return (value or "").strip().lower()


class EmployeeService:
    """Employee service layer."""

    def __init__(self, repository: EmployeeRepository, audit_service: AuditService | None = None):
        self._repository = repository
        self._audit_service = audit_service

    def _ensure_employee_access(self, employee: EmployeeContext | None) -> None:
        if employee is None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

    def _ensure_admin(self, employee: EmployeeContext | None) -> None:
        self._ensure_employee_access(employee)
        if (employee.role or "").strip().lower() != "admin":
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

    def _require_audit_service(self) -> AuditService:
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        return self._audit_service

    async def get_active_employee_by_user_id(self, db: AsyncSession, user_id: int) -> EmployeeContext:
        employee = await self._repository.get_by_user_id(db, user_id)
        if employee is None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

        if (employee.status or "").lower() != "active":
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

        return EmployeeContext(
            employee_id=employee.employee_id,
            user_id=employee.user_id,
            role=employee.role,
        )

    async def create_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        payload: EmployeeCreateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Employee:
        self._ensure_admin(employee)

        status_value = _normalize_status(payload.status)
        if status_value not in _ALLOWED_EMPLOYEE_STATUS:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        existing = await self._repository.get_by_user_id(db, payload.user_id)
        if existing is not None:
            raise AppError(status_code=409, error_code="EMPLOYEE_ALREADY_EXISTS", message="Employee already exists")

        row = Employee(user_id=payload.user_id, role=payload.role, status=status_value)
        row = await self._repository.create(db, row)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_CREATE_EMPLOYEE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return row

    async def list_employees(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        page: int,
        limit: int,
        status: str | None,
        role: str | None,
        user_id: int | None,
    ) -> tuple[list[Employee], int]:
        self._ensure_admin(employee)

        status_value = None
        if status is not None:
            normalized = _normalize_status(status)
            if normalized not in _ALLOWED_EMPLOYEE_STATUS:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            status_value = normalized

        employees = await self._repository.list_employees(
            db,
            page=page,
            limit=limit,
            status=status_value,
            role=role,
            user_id=user_id,
        )
        total = await self._repository.count_employees(
            db,
            status=status_value,
            role=role,
            user_id=user_id,
        )

        return employees, total

    async def get_employee_details(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        employee_id: int,
    ) -> Employee:
        self._ensure_admin(employee)

        row = await self._repository.get_by_id(db, employee_id)
        if row is None:
            raise AppError(status_code=404, error_code="EMPLOYEE_NOT_FOUND", message="Employee does not exist")

        return row

    async def update_employee(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        employee_id: int,
        payload: EmployeeUpdateRequest,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Employee:
        self._ensure_admin(employee)

        row = await self._repository.get_by_id(db, employee_id)
        if row is None:
            raise AppError(status_code=404, error_code="EMPLOYEE_NOT_FOUND", message="Employee does not exist")

        if payload.user_id != row.user_id:
            existing = await self._repository.get_by_user_id(db, payload.user_id)
            if existing is not None and existing.employee_id != row.employee_id:
                raise AppError(status_code=409, error_code="EMPLOYEE_ALREADY_EXISTS", message="Employee already exists")
            row.user_id = payload.user_id

        row.role = payload.role
        row = await self._repository.update(db, row)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_UPDATE_EMPLOYEE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return row

    async def change_employee_status(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        employee_id: int,
        status: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Employee:
        self._ensure_admin(employee)

        row = await self._repository.get_by_id(db, employee_id)
        if row is None:
            raise AppError(status_code=404, error_code="EMPLOYEE_NOT_FOUND", message="Employee does not exist")

        normalized = _normalize_status(status)
        if normalized not in _ALLOWED_EMPLOYEE_STATUS_UPDATE:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        row.status = normalized
        row = await self._repository.update(db, row)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_UPDATE_EMPLOYEE_STATUS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return row
