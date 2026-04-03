"""Employee module dependencies."""

from __future__ import annotations

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from core.dependencies import get_current_user, get_current_user_bearer_or_query
from core.exceptions import AppError
from db.session import get_db
from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.employee.repository import EmployeeRepository
from modules.employee.service import EmployeeContext, EmployeeService


def get_employee_service() -> EmployeeService:
    """Service used by auth dependencies (no audit needed)."""

    return EmployeeService(EmployeeRepository())


def get_employee_management_service() -> EmployeeService:
    """Service used by employee management routes (audit is mandatory)."""

    audit_service = AuditService(AuditRepository())
    return EmployeeService(repository=EmployeeRepository(), audit_service=audit_service)


async def get_current_employee(
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    employee_service: EmployeeService = Depends(get_employee_service),
) -> EmployeeContext:
    """Return the authenticated active employee.

    This is authentication and identity lookup only.
    Role checks must happen in services.
    """

    return await employee_service.get_active_employee_by_user_id(db, current_user.user_id)


async def get_optional_employee(
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    employee_service: EmployeeService = Depends(get_employee_service),
) -> EmployeeContext | None:
    """Return employee context if the user is an active employee, else None."""

    try:
        return await employee_service.get_active_employee_by_user_id(db, current_user.user_id)
    except AppError:
        return None


async def get_current_employee_bearer_or_query(
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user_bearer_or_query),
    employee_service: EmployeeService = Depends(get_employee_service),
) -> EmployeeContext:
    """Active employee context; JWT via Authorization header or ?access_token= (for browser downloads)."""

    return await employee_service.get_active_employee_by_user_id(db, current_user.user_id)
