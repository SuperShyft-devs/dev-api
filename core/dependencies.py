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
from db.session import get_db


_http_bearer = HTTPBearer(auto_error=False)


def _parse_user_id(subject: Optional[str]) -> int:
    if subject is None:
        raise ValueError("Missing subject")

    user_id = int(subject)
    if user_id <= 0:
        raise ValueError("Invalid subject")

    return user_id


async def authenticate_bearer_user(
    db: AsyncSession,
    credentials: HTTPAuthorizationCredentials | None,
    *,
    access_token: str | None = None,
):
    """Validate Bearer JWT (header or optional query token) and return the active user."""

    token: str | None = None
    if credentials is not None and credentials.scheme.lower() == "bearer":
        token = credentials.credentials
    elif access_token is not None and access_token.strip():
        token = access_token.strip()

    if token is None:
        raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

    try:
        payload = decode_and_verify_jwt(token)
        user_id = _parse_user_id(payload.get("sub"))
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


async def get_current_user(
    db: AsyncSession = Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(_http_bearer),
):
    """Return the authenticated active user."""

    return await authenticate_bearer_user(db, credentials, access_token=None)


async def get_current_user_bearer_or_query(
    db: AsyncSession = Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(_http_bearer),
    access_token: str | None = Query(default=None, description="JWT for browser download links (prefer Authorization header)."),
):
    """Same as get_current_user, but also accepts ?access_token= for browser file downloads."""

    return await authenticate_bearer_user(db, credentials, access_token=access_token)
