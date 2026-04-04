"""Unit tests for auth service."""

from __future__ import annotations

import pytest

from core.config import settings
from core.exceptions import AppError
from modules.users.models import User


@pytest.mark.asyncio
async def test_verify_otp_fails_with_wrong_code(auth_service, test_db_session):
    test_db_session.add(User(user_id=1010, age=30, phone="5555555555", status="active"))
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


@pytest.mark.asyncio
async def test_verify_otp_allows_bypass_code_when_enabled(auth_service, test_db_session):
    test_db_session.add(User(user_id=1011, age=30, phone="5555555556", status="active"))
    await test_db_session.commit()

    await auth_service.send_otp(
        test_db_session,
        phone="5555555556",
        ip_address="1.1.1.1",
        user_agent="pytest",
        endpoint="/auth/send-otp",
    )

    previous = settings.ALLOW_BYPASS_OTP
    settings.ALLOW_BYPASS_OTP = True
    try:
        user_id, tokens = await auth_service.verify_otp(
            test_db_session,
            phone="5555555556",
            otp="654321",
            ip_address="1.1.1.1",
            user_agent="pytest",
            endpoint="/auth/verify-otp",
        )
    finally:
        settings.ALLOW_BYPASS_OTP = previous

    assert user_id == 1011
    assert tokens.access_token
    assert tokens.refresh_token


@pytest.mark.asyncio
async def test_verify_otp_rejects_bypass_code_when_disabled(auth_service, test_db_session):
    test_db_session.add(User(user_id=1012, age=30, phone="5555555557", status="active"))
    await test_db_session.commit()

    await auth_service.send_otp(
        test_db_session,
        phone="5555555557",
        ip_address="1.1.1.1",
        user_agent="pytest",
        endpoint="/auth/send-otp",
    )

    previous = settings.ALLOW_BYPASS_OTP
    settings.ALLOW_BYPASS_OTP = False
    try:
        with pytest.raises(AppError):
            await auth_service.verify_otp(
                test_db_session,
                phone="5555555557",
                otp="654321",
                ip_address="1.1.1.1",
                user_agent="pytest",
                endpoint="/auth/verify-otp",
            )
    finally:
        settings.ALLOW_BYPASS_OTP = previous
