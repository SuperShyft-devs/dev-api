"""Shared actor for organization-scoped routes: employee or organization_manager partner."""

from __future__ import annotations

from dataclasses import dataclass

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from core.dependencies import get_current_employee_from_token, get_current_partner_from_token
from core.exceptions import AppError
from core.security import decode_and_verify_jwt
from core.subject_auth import jwt_subject_typ
from db.session import get_db
from modules.employee.access_control import is_organization_manager_partner
from modules.employee.dependencies import get_employee_service
from modules.employee.service import EmployeeContext, EmployeeService
from modules.partners.models import Partner

_http_bearer = HTTPBearer(auto_error=False)


@dataclass(frozen=True)
class OrgScopedActor:
    employee: EmployeeContext | None = None
    partner: Partner | None = None


async def get_org_scoped_actor(
    request: Request,
    db: AsyncSession = Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(_http_bearer),
    employee_service: EmployeeService = Depends(get_employee_service),
) -> OrgScopedActor:
    """Accept employee JWT or partner JWT with role=organization_manager."""
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

    try:
        payload = decode_and_verify_jwt(credentials.credentials)
        typ = jwt_subject_typ(payload)
    except Exception as exc:
        raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed") from exc

    if typ == "partner":
        partner = await get_current_partner_from_token(db, credentials)
        if not is_organization_manager_partner(partner):
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )
        return OrgScopedActor(partner=partner)

    if typ == "employee":
        employee_row = await get_current_employee_from_token(db, credentials)
        employee = await employee_service.get_active_employee_by_id(
            db,
            employee_row.employee_id,
            capability=getattr(request.state, "rbac_capability", None),
        )
        return OrgScopedActor(employee=employee)

    raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")
