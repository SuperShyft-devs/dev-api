"""Rate limiting helpers and misuse / lockout controls for discount APIs."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.discounts.models import DiscountValidationAttempt
from modules.discounts.schemas import normalize_code

logger = logging.getLogger(__name__)

CODE_PATTERN = re.compile(r"^[A-Z0-9_-]{3,32}$")
PUBLIC_INVALID = "This code is not valid for this order"
LOCKOUT_MESSAGE = "Too many discount attempts, try again later"

FAIL_OUTCOMES = ("invalid", "ineligible")
LOCKOUT_FAIL_LIMIT = 10
LOCKOUT_WINDOW_MINUTES = 15
VALIDATE_PER_USER_HOUR = 20
DISCOUNTED_CREATE_PER_MINUTE = 10


class AbuseError(Exception):
    def __init__(self, message: str, *, outcome: str = "locked", status_code: int = 429):
        super().__init__(message)
        self.message = message
        self.outcome = outcome
        self.status_code = status_code


def sanitize_code(raw: str | None) -> str | None:
    if raw is None:
        return None
    code = normalize_code(raw)
    if not CODE_PATTERN.fullmatch(code):
        return None
    return code


async def record_attempt(
    db: AsyncSession,
    *,
    user_id: int | None,
    client_ip: str | None,
    code_submitted: str | None,
    outcome: str,
    endpoint: str | None,
    detail: str | None = None,
) -> None:
    db.add(
        DiscountValidationAttempt(
            user_id=user_id,
            client_ip=(client_ip or "")[:64] or None,
            code_submitted=(code_submitted or "")[:64] or None,
            outcome=outcome,
            endpoint=(endpoint or "")[:128] or None,
            detail=(detail or "")[:255] or None,
        )
    )


async def assert_not_locked(
    db: AsyncSession,
    *,
    user_id: int | None,
    client_ip: str | None,
) -> None:
    since = datetime.now(timezone.utc) - timedelta(minutes=LOCKOUT_WINDOW_MINUTES)
    clauses = [
        DiscountValidationAttempt.created_at >= since,
        DiscountValidationAttempt.outcome.in_(FAIL_OUTCOMES),
    ]
    subject = []
    if user_id is not None:
        subject.append(DiscountValidationAttempt.user_id == user_id)
    if client_ip:
        subject.append(DiscountValidationAttempt.client_ip == client_ip)
    if not subject:
        return
    result = await db.execute(
        select(func.count(DiscountValidationAttempt.attempt_id)).where(
            and_(*clauses, or_(*subject))
        )
    )
    failures = int(result.scalar() or 0)
    if failures >= LOCKOUT_FAIL_LIMIT:
        # Confirm most recent failure is still within window (soft lock)
        raise AbuseError(LOCKOUT_MESSAGE, outcome="locked", status_code=429)


async def assert_validate_hourly_cap(
    db: AsyncSession,
    *,
    user_id: int,
) -> None:
    since = datetime.now(timezone.utc) - timedelta(hours=1)
    result = await db.execute(
        select(func.count(DiscountValidationAttempt.attempt_id)).where(
            DiscountValidationAttempt.user_id == user_id,
            DiscountValidationAttempt.created_at >= since,
            DiscountValidationAttempt.endpoint.in_(("/discounts/validate", "validate")),
        )
    )
    count = int(result.scalar() or 0)
    if count >= VALIDATE_PER_USER_HOUR:
        raise AbuseError(LOCKOUT_MESSAGE, outcome="rate_limited", status_code=429)


async def assert_discounted_create_cap(
    db: AsyncSession,
    *,
    user_id: int,
    client_ip: str | None,
) -> None:
    since = datetime.now(timezone.utc) - timedelta(minutes=1)
    clauses = [
        DiscountValidationAttempt.created_at >= since,
        DiscountValidationAttempt.endpoint.in_(("/payments/create-order", "create-order")),
        DiscountValidationAttempt.outcome == "ok",
    ]
    subject = [DiscountValidationAttempt.user_id == user_id]
    if client_ip:
        subject.append(DiscountValidationAttempt.client_ip == client_ip)
    result = await db.execute(
        select(func.count(DiscountValidationAttempt.attempt_id)).where(
            and_(*clauses, or_(*subject))
        )
    )
    if int(result.scalar() or 0) >= DISCOUNTED_CREATE_PER_MINUTE:
        raise AbuseError(LOCKOUT_MESSAGE, outcome="rate_limited", status_code=429)


async def count_abuse_events_24h(db: AsyncSession) -> int:
    since = datetime.now(timezone.utc) - timedelta(hours=24)
    result = await db.execute(
        select(func.count(DiscountValidationAttempt.attempt_id)).where(
            DiscountValidationAttempt.created_at >= since,
            DiscountValidationAttempt.outcome.in_(("locked", "rate_limited")),
        )
    )
    return int(result.scalar() or 0)


def map_engine_outcome(ok: bool, reason: str | None) -> str:
    if ok:
        return "ok"
    if reason in ("not_found", "invalid_format"):
        return "invalid"
    return "ineligible"
