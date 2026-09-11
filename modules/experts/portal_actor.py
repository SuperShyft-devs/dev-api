"""Expert portal actor: partner (expert) or employee (admin)."""

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
from modules.employee.access_control import ensure_expert_portal_access
from modules.employee.dependencies import get_employee_service
from modules.employee.service import EmployeeContext, EmployeeService
from modules.partners.models import Partner

_http_bearer = HTTPBearer(auto_error=False)


@dataclass(frozen=True)
class ExpertPortalActor:
    employee: EmployeeContext | None = None
    partner: Partner | None = None


async def get_expert_portal_actor(
    request: Request,
    db: AsyncSession = Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(_http_bearer),
    employee_service: EmployeeService = Depends(get_employee_service),
) -> ExpertPortalActor:
    """Accept partner JWT (expert) or employee JWT (admin)."""
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

    try:
        payload = decode_and_verify_jwt(credentials.credentials)
        typ = jwt_subject_typ(payload)
    except Exception as exc:
        raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed") from exc

    if typ == "partner":
        partner = await get_current_partner_from_token(db, credentials)
        ensure_expert_portal_access(partner=partner)
        return ExpertPortalActor(partner=partner)

    if typ == "employee":
        employee_row = await get_current_employee_from_token(db, credentials)
        employee = await employee_service.get_active_employee_by_id(
            db,
            employee_row.employee_id,
            capability=getattr(request.state, "rbac_capability", None),
        )
        ensure_expert_portal_access(employee)
        return ExpertPortalActor(employee=employee)

    raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")
