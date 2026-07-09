"""Notifications module dependencies."""

from __future__ import annotations

from fastapi import Depends, Header
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from core.dependencies import authenticate_bearer_user
from core.exceptions import AppError
from db.session import get_db
from modules.employee.access_control import ensure_internal_employee
from modules.employee.dependencies import get_employee_service
from modules.employee.service import EmployeeContext, EmployeeService
from modules.metsights.client import MetsightsClient
from modules.metsights.service import MetsightsService
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService


_optional_http_bearer = HTTPBearer(auto_error=False)


def get_notifications_service() -> NotificationsService:
    return NotificationsService(
        repository=NotificationsRepository(),
        metsights_service=MetsightsService(client=MetsightsClient()),
    )


async def authenticate_notification_endpoint(
    db: AsyncSession = Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(_optional_http_bearer),
    x_api_key: str | None = Header(None),
    employee_service: EmployeeService = Depends(get_employee_service),
) -> EmployeeContext | None:
    """Accept either a valid internal employee JWT or a matching x-api-key header.

    Returns EmployeeContext when authenticated via JWT, None when via API key.
    Raises 401 if neither method succeeds.
    """
    if credentials is not None and credentials.scheme.lower() == "bearer":
        try:
            user = await authenticate_bearer_user(db, credentials, access_token=None)
            employee = await employee_service.get_active_employee_by_user_id(db, user.user_id)
            ensure_internal_employee(employee)
            return employee
        except AppError:
            pass

    if x_api_key and settings.NOTIFICATION_API_KEY and x_api_key == settings.NOTIFICATION_API_KEY:
        return None

    raise AppError(
        status_code=401,
        error_code="AUTH_FAILED",
        message="Authentication required: provide a valid employee token or x-api-key header",
    )
