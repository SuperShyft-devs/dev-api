"""Employee module dependencies."""

from __future__ import annotations

from fastapi import Depends, Query, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from core.dependencies import get_current_employee_from_token
from core.exceptions import AppError
from db.session import get_db
from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.employee.auth_service import EmployeeAuthService
from modules.employee.repository import EmployeeRepository
from modules.employee.service import EmployeeContext, EmployeeService


_optional_http_bearer = HTTPBearer(auto_error=False)
_http_bearer = HTTPBearer(auto_error=False)


def get_employee_service() -> EmployeeService:
    """Service used by auth dependencies (no audit needed)."""

    return EmployeeService(EmployeeRepository())


def get_employee_management_service() -> EmployeeService:
    """Service used by employee management routes (audit is mandatory)."""

    audit_service = AuditService(AuditRepository())
    return EmployeeService(repository=EmployeeRepository(), audit_service=audit_service)


def get_employee_auth_service() -> EmployeeAuthService:
    # Local import avoids circular dependency with notifications.dependencies.
    from modules.notifications.dependencies import get_notifications_service

    return EmployeeAuthService(
        repository=EmployeeRepository(),
        audit_service=AuditService(AuditRepository()),
        notifications_service=get_notifications_service(),
    )


async def get_current_employee(
    request: Request,
    db: AsyncSession = Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(_http_bearer),
    employee_service: EmployeeService = Depends(get_employee_service),
) -> EmployeeContext:
    """Return the authenticated active employee from an employee JWT."""

    employee_row = await get_current_employee_from_token(db, credentials, access_token=None)
    return await employee_service.get_active_employee_by_id(
        db,
        employee_row.employee_id,
        capability=getattr(request.state, "rbac_capability", None),
    )


async def get_optional_employee(
    request: Request,
    db: AsyncSession = Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(_http_bearer),
    employee_service: EmployeeService = Depends(get_employee_service),
) -> EmployeeContext | None:
    """Return employee context if the token is an active employee JWT, else None."""

    try:
        employee_row = await get_current_employee_from_token(db, credentials, access_token=None)
        return await employee_service.get_active_employee_by_id(
            db,
            employee_row.employee_id,
            capability=getattr(request.state, "rbac_capability", None),
        )
    except AppError:
        return None


async def get_current_employee_bearer_or_query(
    request: Request,
    db: AsyncSession = Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(_http_bearer),
    access_token: str | None = Query(
        default=None,
        description="JWT for browser download links (prefer Authorization header).",
    ),
    employee_service: EmployeeService = Depends(get_employee_service),
) -> EmployeeContext:
    """Active employee context; JWT via Authorization header or ?access_token=."""

    employee_row = await get_current_employee_from_token(
        db, credentials, access_token=access_token
    )
    return await employee_service.get_active_employee_by_id(
        db,
        employee_row.employee_id,
        capability=getattr(request.state, "rbac_capability", None),
    )


async def get_optional_employee_if_authenticated(
    request: Request,
    db: AsyncSession = Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(_optional_http_bearer),
    employee_service: EmployeeService = Depends(get_employee_service),
) -> EmployeeContext | None:
    """Bearer optional: active employee if token is typ=employee, else None (no 401)."""

    if credentials is None or credentials.scheme.lower() != "bearer":
        return None
    try:
        employee_row = await get_current_employee_from_token(db, credentials, access_token=None)
        return await employee_service.get_active_employee_by_id(
            db,
            employee_row.employee_id,
            capability=getattr(request.state, "rbac_capability", None),
        )
    except AppError:
        return None
