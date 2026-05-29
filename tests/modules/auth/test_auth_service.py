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

    session_id, _, _ = await auth_service.send_otp(
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


@pytest.mark.asyncio
async def test_verify_otp_per_phone_bypass_when_global_disabled(auth_service, test_db_session):
    test_db_session.add(User(user_id=1020, age=30, phone="5555555560", status="active"))
    await test_db_session.commit()

    await auth_service.send_otp(
        test_db_session,
        phone="5555555560",
        ip_address="1.1.1.1",
        user_agent="pytest",
        endpoint="/auth/send-otp",
    )

    previous_allow = settings.ALLOW_BYPASS_OTP
    previous_by_phone = settings.BYPASS_OTP_BY_PHONE
    settings.ALLOW_BYPASS_OTP = False
    settings.BYPASS_OTP_BY_PHONE = "5555555560:424242"
    settings._bypass_otp_cache_key = None
    settings._bypass_otp_by_phone_index = {}
    try:
        user_id, tokens = await auth_service.verify_otp(
            test_db_session,
            phone="5555555560",
            otp="424242",
            ip_address="1.1.1.1",
            user_agent="pytest",
            endpoint="/auth/verify-otp",
        )
    finally:
        settings.ALLOW_BYPASS_OTP = previous_allow
        settings.BYPASS_OTP_BY_PHONE = previous_by_phone
        settings._bypass_otp_cache_key = None
        settings._bypass_otp_by_phone_index = {}

    assert user_id == 1020
    assert tokens.access_token
    assert tokens.refresh_token


@pytest.mark.asyncio
async def test_verify_otp_per_phone_bypass_wrong_otp_fails(auth_service, test_db_session):
    test_db_session.add(User(user_id=1021, age=30, phone="5555555561", status="active"))
    await test_db_session.commit()

    await auth_service.send_otp(
        test_db_session,
        phone="5555555561",
        ip_address="1.1.1.1",
        user_agent="pytest",
        endpoint="/auth/send-otp",
    )

    previous_allow = settings.ALLOW_BYPASS_OTP
    previous_by_phone = settings.BYPASS_OTP_BY_PHONE
    settings.ALLOW_BYPASS_OTP = False
    settings.BYPASS_OTP_BY_PHONE = "5555555561:424242"
    settings._bypass_otp_cache_key = None
    settings._bypass_otp_by_phone_index = {}
    try:
        with pytest.raises(AppError):
            await auth_service.verify_otp(
                test_db_session,
                phone="5555555561",
                otp="000000",
                ip_address="1.1.1.1",
                user_agent="pytest",
                endpoint="/auth/verify-otp",
            )
    finally:
        settings.ALLOW_BYPASS_OTP = previous_allow
        settings.BYPASS_OTP_BY_PHONE = previous_by_phone
        settings._bypass_otp_cache_key = None
        settings._bypass_otp_by_phone_index = {}


@pytest.mark.asyncio
async def test_verify_otp_per_phone_bypass_wrong_phone_fails(auth_service, test_db_session):
    test_db_session.add(User(user_id=1022, age=30, phone="5555555562", status="active"))
    await test_db_session.commit()

    await auth_service.send_otp(
        test_db_session,
        phone="5555555562",
        ip_address="1.1.1.1",
        user_agent="pytest",
        endpoint="/auth/send-otp",
    )

    previous_allow = settings.ALLOW_BYPASS_OTP
    previous_by_phone = settings.BYPASS_OTP_BY_PHONE
    settings.ALLOW_BYPASS_OTP = False
    settings.BYPASS_OTP_BY_PHONE = "5555555599:424242"
    settings._bypass_otp_cache_key = None
    settings._bypass_otp_by_phone_index = {}
    try:
        with pytest.raises(AppError):
            await auth_service.verify_otp(
                test_db_session,
                phone="5555555562",
                otp="424242",
                ip_address="1.1.1.1",
                user_agent="pytest",
                endpoint="/auth/verify-otp",
            )
    finally:
        settings.ALLOW_BYPASS_OTP = previous_allow
        settings.BYPASS_OTP_BY_PHONE = previous_by_phone
        settings._bypass_otp_cache_key = None
        settings._bypass_otp_by_phone_index = {}
