"""Shared FastAPI dependencies.

These dependencies are safe to import from any router.
They must not contain business rules.
"""

from __future__ import annotations

from typing import Optional

from fastapi import Depends, Query
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from core.security import decode_and_verify_jwt
from core.subject_auth import jwt_subject_typ, parse_subject_id
from db.session import get_db


_http_bearer = HTTPBearer(auto_error=False)


def _extract_bearer_token(
    credentials: HTTPAuthorizationCredentials | None,
    *,
    access_token: str | None = None,
) -> str:
    token: str | None = None
    if credentials is not None and credentials.scheme.lower() == "bearer":
        token = credentials.credentials
    elif access_token is not None and access_token.strip():
        token = access_token.strip()

    if token is None:
        raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")
    return token


def _decode_payload(token: str) -> dict:
    try:
        return decode_and_verify_jwt(token)
    except Exception as exc:
        raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed") from exc


async def authenticate_bearer_user(
    db: AsyncSession,
    credentials: HTTPAuthorizationCredentials | None,
    *,
    access_token: str | None = None,
):
    """Validate Bearer JWT (header or optional query token) and return the active user.

    Requires ``typ`` missing or ``\"user\"`` (backward compatible).
    """

    token = _extract_bearer_token(credentials, access_token=access_token)
    payload = _decode_payload(token)
    try:
        typ = jwt_subject_typ(payload)
        if typ != "user":
            raise ValueError("Not a user token")
        user_id = parse_subject_id(payload.get("sub"))
    except Exception as exc:
        raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed") from exc

    # Import locally to avoid hard coupling during app boot.
    from modules.users.repository import UsersRepository

    user = await UsersRepository().get_user_by_id(db, user_id)
    if user is None:
        raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

    if (user.status or "").lower() != "active":
        raise AppError(status_code=403, error_code="FORBIDDEN", message="You do not have permission to perform this action")

    return user


async def get_current_employee_from_token(
    db: AsyncSession,
    credentials: HTTPAuthorizationCredentials | None,
    *,
    access_token: str | None = None,
):
    """Validate Bearer JWT with ``typ=employee`` and return the active Employee row."""

    token = _extract_bearer_token(credentials, access_token=access_token)
    payload = _decode_payload(token)
    try:
        typ = jwt_subject_typ(payload)
        employee_id = parse_subject_id(payload.get("sub"))
    except Exception as exc:
        raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed") from exc

    if typ != "employee":
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )

    from modules.employee.models import Employee

    employee = await db.get(Employee, employee_id)
    if employee is None:
        raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

    if (employee.status or "").lower() != "active":
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )

    return employee


async def get_current_partner(
    db: AsyncSession = Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(_http_bearer),
):
    """Validate Bearer JWT with ``typ=partner`` and return the active Partner row."""

    return await get_current_partner_from_token(db, credentials, access_token=None)


async def get_current_partner_from_token(
    db: AsyncSession,
    credentials: HTTPAuthorizationCredentials | None,
    *,
    access_token: str | None = None,
):
    """Validate Bearer JWT with ``typ=partner`` and return the active Partner row."""

    token = _extract_bearer_token(credentials, access_token=access_token)
    payload = _decode_payload(token)
    try:
        typ = jwt_subject_typ(payload)
        if typ != "partner":
            raise ValueError("Not a partner token")
        partner_id = parse_subject_id(payload.get("sub"))
    except Exception as exc:
        raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed") from exc

    from modules.partners.models import Partner

    partner = await db.get(Partner, partner_id)
    if partner is None:
        raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

    if (partner.status or "").lower() != "active":
        raise AppError(
            status_code=403,
            error_code="FORBIDDEN",
            message="You do not have permission to perform this action",
        )

    return partner


async def get_current_user(
    db: AsyncSession = Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(_http_bearer),
):
    """Return the authenticated active user."""

    return await authenticate_bearer_user(db, credentials, access_token=None)


async def get_optional_user(
    db: AsyncSession = Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(_http_bearer),
):
    """Return active user if Bearer token is valid; otherwise None (no 401)."""

    if credentials is None or credentials.scheme.lower() != "bearer":
        return None
    try:
        return await authenticate_bearer_user(db, credentials, access_token=None)
    except AppError:
        return None


async def get_current_user_bearer_or_query(
    db: AsyncSession = Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(_http_bearer),
    access_token: str | None = Query(default=None, description="JWT for browser download links (prefer Authorization header)."),
):
    """Same as get_current_user, but also accepts ?access_token= for browser file downloads."""

    return await authenticate_bearer_user(db, credentials, access_token=access_token)
