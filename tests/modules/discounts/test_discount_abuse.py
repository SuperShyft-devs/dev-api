"""API-ish tests for discount abuse helpers."""

from __future__ import annotations

import pytest
from datetime import datetime, timedelta, timezone

from modules.discounts.abuse import (
    AbuseError,
    assert_not_locked,
    LOCKOUT_FAIL_LIMIT,
)
from modules.discounts.models import DiscountValidationAttempt


@pytest.mark.asyncio
async def test_lockout_after_failures(test_db_session):
    ip = "203.0.113.10"
    now = datetime.now(timezone.utc)
    for _ in range(LOCKOUT_FAIL_LIMIT):
        test_db_session.add(
            DiscountValidationAttempt(
                user_id=None,
                client_ip=ip,
                code_submitted="BADCODE",
                outcome="invalid",
                endpoint="/discounts/validate",
                created_at=now - timedelta(minutes=1),
            )
        )
    await test_db_session.commit()

    with pytest.raises(AbuseError) as exc:
        await assert_not_locked(test_db_session, user_id=None, client_ip=ip)
    assert exc.value.status_code == 429
