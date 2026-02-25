"""Integration tests for auth routes."""

from __future__ import annotations

import pytest
from sqlalchemy import select

from modules.audit.models import DataAuditLog
from modules.auth.models import AuthOtpSession, AuthToken
from modules.users.models import User

@pytest.mark.asyncio
async def test_send_otp_requires_existing_user(async_client, test_db_session):
    test_db_session.add(User(user_id=1001, phone="9999999999", status="active"))
    await test_db_session.commit()

    response = await async_client.post("/auth/send-otp", json={"phone": "9999999999"})
    assert response.status_code == 200

    response2 = await async_client.post("/auth/send-otp", json={"phone": "9999999999"})
    assert response2.status_code == 200

    # Only the latest OTP session should remain.
    session_id = response2.json()["data"]["session_id"]
    assert isinstance(session_id, int)

    otp_session = (
        await test_db_session.execute(
            select(AuthOtpSession).where(AuthOtpSession.session_id == session_id)
        )
    ).scalar_one_or_none()
    assert otp_session is not None

    sessions = (
        await test_db_session.execute(select(AuthOtpSession).where(AuthOtpSession.user_id == 1001))
    ).scalars().all()
    assert len(sessions) == 1

    audit_rows = (
        await test_db_session.execute(
            select(DataAuditLog).where(DataAuditLog.user_id == 1001, DataAuditLog.action == "AUTH_SEND_OTP")
        )
    ).scalars().all()
    assert len(audit_rows) >= 1


@pytest.mark.asyncio
async def test_send_otp_returns_404_for_unknown_user(async_client):
    response = await async_client.post("/auth/send-otp", json={"phone": "1111111111"})

    assert response.status_code == 404
    assert response.json() == {"error_code": "USER_NOT_FOUND", "message": "User does not exist"}


@pytest.mark.asyncio
async def test_verify_otp_issues_tokens_and_consumes_session(async_client, test_db_session):
    test_db_session.add(User(user_id=1002, phone="8888888888", status="active"))
    await test_db_session.commit()

    send = await async_client.post("/auth/send-otp", json={"phone": "8888888888"})
    session_id = send.json()["data"]["session_id"]

    otp = async_client._transport.app.state.otp_sender.last_otp
    assert otp is not None

    verify = await async_client.post(
        "/auth/verify-otp",
        json={"phone": "8888888888", "otp": otp},
    )

    assert verify.status_code == 200
    data = verify.json()["data"]
    assert data["user_id"] == 1002
    assert data["tokens"]["access_token"]
    assert data["tokens"]["refresh_token"]

    token_count = (
        await test_db_session.execute(select(AuthToken).where(AuthToken.user_id == 1002))
    ).scalars().all()
    assert len(token_count) == 1

    audit_row = (
        await test_db_session.execute(
            select(DataAuditLog).where(DataAuditLog.user_id == 1002, DataAuditLog.action == "AUTH_LOGIN")
        )
    ).scalar_one_or_none()
    assert audit_row is not None

    # OTP must be single use.
    should_fail = await async_client.post(
        "/auth/verify-otp",
        json={"phone": "8888888888", "otp": otp},
    )
    assert should_fail.status_code == 401


@pytest.mark.asyncio
async def test_refresh_token_rotates_refresh_token(async_client, test_db_session):
    test_db_session.add(User(user_id=1003, phone="7777777777", status="active"))
    await test_db_session.commit()

    send = await async_client.post("/auth/send-otp", json={"phone": "7777777777"})
    session_id = send.json()["data"]["session_id"]

    otp = async_client._transport.app.state.otp_sender.last_otp
    assert otp is not None

    verify = await async_client.post("/auth/verify-otp", json={"phone": "7777777777", "otp": otp})
    refresh_token = verify.json()["data"]["tokens"]["refresh_token"]

    refreshed = await async_client.post("/auth/refresh-token", json={"refresh_token": refresh_token})
    assert refreshed.status_code == 200

    new_refresh = refreshed.json()["data"]["tokens"]["refresh_token"]
    assert new_refresh != refresh_token

    should_fail = await async_client.post("/auth/refresh-token", json={"refresh_token": refresh_token})
    assert should_fail.status_code == 401

    audit_row = (
        await test_db_session.execute(
            select(DataAuditLog).where(DataAuditLog.user_id == 1003, DataAuditLog.action == "AUTH_REFRESH")
        )
    ).scalar_one_or_none()
    assert audit_row is not None


@pytest.mark.asyncio
async def test_logout_invalidates_refresh_token(async_client, test_db_session):
    test_db_session.add(User(user_id=1004, phone="6666666666", status="active"))
    await test_db_session.commit()

    send = await async_client.post("/auth/send-otp", json={"phone": "6666666666"})
    session_id = send.json()["data"]["session_id"]

    otp = async_client._transport.app.state.otp_sender.last_otp
    assert otp is not None

    verify = await async_client.post("/auth/verify-otp", json={"phone": "6666666666", "otp": otp})
    refresh_token = verify.json()["data"]["tokens"]["refresh_token"]

    out = await async_client.post("/auth/logout", json={"refresh_token": refresh_token})
    assert out.status_code == 200

    should_fail = await async_client.post("/auth/refresh-token", json={"refresh_token": refresh_token})
    assert should_fail.status_code == 401

    audit_row = (
        await test_db_session.execute(
            select(DataAuditLog).where(DataAuditLog.user_id == 1004, DataAuditLog.action == "AUTH_LOGOUT")
        )
    ).scalar_one_or_none()
    assert audit_row is not None
