"""Unit tests for auth service."""

from __future__ import annotations

import pytest

from core.exceptions import AppError
from modules.users.models import User


@pytest.mark.asyncio
async def test_verify_otp_fails_with_wrong_code(auth_service, test_db_session):
    test_db_session.add(User(user_id=1010, phone="5555555555", status="active"))
    await test_db_session.commit()

    session_id = await auth_service.send_otp(
        test_db_session,
        phone="5555555555",
        ip_address="1.1.1.1",
        user_agent="pytest",
        endpoint="/auth/send-otp",
    )

    with pytest.raises(AppError):
        await auth_service.verify_otp(
            test_db_session,
            phone="5555555555",
            otp="000000",
            ip_address="1.1.1.1",
            user_agent="pytest",
            endpoint="/auth/verify-otp",
        )

    assert session_id > 0
