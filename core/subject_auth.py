"""Shared JWT subject helpers for user / employee / partner tokens."""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Literal

from core.config import settings
from core.security import create_jwt_token

JwtSubjectTyp = Literal["user", "employee", "partner"]

_VALID_TYPS = frozenset({"user", "employee", "partner"})


def create_access_token(subject_id: int, typ: JwtSubjectTyp) -> str:
    """Issue an access JWT with ``sub`` and ``typ`` claims."""
    if subject_id <= 0:
        raise ValueError("subject_id must be positive")
    if typ not in _VALID_TYPS:
        raise ValueError("Invalid JWT subject typ")
    return create_jwt_token(
        {"sub": str(subject_id), "typ": typ},
        timedelta(minutes=settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES),
    )


def parse_subject_id(subject: Any) -> int:
    if subject is None:
        raise ValueError("Missing subject")
    subject_id = int(subject)
    if subject_id <= 0:
        raise ValueError("Invalid subject")
    return subject_id


def jwt_subject_typ(payload: dict[str, Any]) -> JwtSubjectTyp:
    """Return JWT ``typ``. Missing typ is treated as ``user`` for backward compatibility."""
    typ = payload.get("typ")
    if typ is None:
        return "user"
    if typ not in _VALID_TYPS:
        raise ValueError("Invalid JWT subject typ")
    return typ  # type: ignore[return-value]
