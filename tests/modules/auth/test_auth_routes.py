"""Integration tests for auth routes."""

from __future__ import annotations

import pytest
from sqlalchemy import select

from modules.audit.models import DataAuditLog
from modules.auth.models import AuthOtpSession, AuthToken
from modules.users.models import User

@pytest.mark.asyncio
async def test_send_otp_requires_existing_user(async_client, test_db_session):
    test_db_session.add(User(user_id=1001, age=30, phone="9999999999", status="active"))
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
async def test_send_otp_with_dontsendotp_suffix_skips_dispatch(async_client, test_db_session):
    test_db_session.add(User(user_id=1013, age=30, phone="7000000137", status="active"))
    await test_db_session.commit()

    sender = async_client._transport.app.state.capturing_notifications_service
    sender.last_dispatch = None
    sender.last_otp = None

    response = await async_client.post("/auth/send-otp", json={"phone": "7000000137dontsendotp"})

    assert response.status_code == 200
    assert isinstance(response.json()["data"]["session_id"], int)
    assert sender.last_dispatch is None
    assert sender.last_otp is None


@pytest.mark.asyncio
async def test_send_otp_with_dontsendotp_suffix_accepts_international_e164(async_client, test_db_session):
    test_db_session.add(
        User(user_id=1015, age=30, phone="+66961275268", status="active", email="thai1015@example.com")
    )
    await test_db_session.commit()

    sender = async_client._transport.app.state.capturing_notifications_service
    sender.last_dispatch = None
    sender.last_otp = None

    response = await async_client.post(
        "/auth/send-otp",
        json={"phone": "+66961275268dontsendotp"},
    )

    assert response.status_code == 200
    assert isinstance(response.json()["data"]["session_id"], int)
    assert sender.last_dispatch is None
    assert sender.last_otp is None


@pytest.mark.asyncio
async def test_send_and_verify_otp_accepts_thailand_plus66(async_client, test_db_session):
    test_db_session.add(
        User(user_id=1016, age=30, phone="+66961275269", status="active", email="thai1016@example.com")
    )
    await test_db_session.commit()

    send = await async_client.post("/auth/send-otp", json={"phone": "+66961275269"})
    assert send.status_code == 200

    otp = async_client._transport.app.state.capturing_notifications_service.last_otp
    assert otp is not None

    verify = await async_client.post(
        "/auth/verify-otp",
        json={"phone": "+66961275269", "otp": otp},
    )
    assert verify.status_code == 200
    assert verify.json()["data"]["user_id"] == 1016


@pytest.mark.asyncio
async def test_send_otp_rejects_non_supported_phone_suffix(async_client, test_db_session):
    test_db_session.add(User(user_id=1014, age=30, phone="7000000138", status="active"))
    await test_db_session.commit()

    response = await async_client.post("/auth/send-otp", json={"phone": "7000000138abc"})

    assert response.status_code == 400
    assert response.json() == {"error_code": "INVALID_INPUT", "message": "Invalid request"}


@pytest.mark.asyncio
async def test_send_and_verify_otp_sub_profile_with_unique_phone(async_client, test_db_session):
    parent = User(
        user_id=9201,
        age=40,
        phone="6100000001",
        status="active",
        email="parent9201@example.com",
        relationship="self",
    )
    sub = User(
        user_id=9202,
        age=10,
        phone="6100000002",
        status="active",
        email="sub9202+1@example.com",
        parent_id=9201,
        relationship="child",
    )
    test_db_session.add_all([parent, sub])
    await test_db_session.commit()

    send = await async_client.post("/auth/send-otp", json={"phone": "6100000002"})
    assert send.status_code == 200

    otp = async_client._transport.app.state.capturing_notifications_service.last_otp
    assert otp is not None

    verify = await async_client.post(
        "/auth/verify-otp",
        json={"phone": "6100000002", "otp": otp},
    )
    assert verify.status_code == 200
    assert verify.json()["data"]["user_id"] == 9202


@pytest.mark.asyncio
async def test_send_and_verify_otp_shared_phone_authenticates_parent(async_client, test_db_session):
    parent = User(
        user_id=9203,
        age=40,
        phone="6100000003",
        status="active",
        email="parent9203@example.com",
        relationship="self",
    )
    sub = User(
        user_id=9204,
        age=10,
        phone="6100000003",
        status="active",
        email="sub9204+1@example.com",
        parent_id=9203,
        relationship="child",
    )
    test_db_session.add_all([parent, sub])
    await test_db_session.commit()

    send = await async_client.post("/auth/send-otp", json={"phone": "6100000003"})
    assert send.status_code == 200

    otp = async_client._transport.app.state.capturing_notifications_service.last_otp
    verify = await async_client.post(
        "/auth/verify-otp",
        json={"phone": "6100000003", "otp": otp},
    )
    assert verify.status_code == 200
    assert verify.json()["data"]["user_id"] == 9203


@pytest.mark.asyncio
async def test_send_and_verify_otp_accepts_plus91_and_local_forms(async_client, test_db_session):
    user = User(
        user_id=9301,
        age=31,
        phone="+918103946120",
        status="active",
        email="user9301@example.com",
        relationship="self",
    )
    test_db_session.add(user)
    await test_db_session.commit()

    send = await async_client.post("/auth/send-otp", json={"phone": "8103946120"})
    assert send.status_code == 200

    otp = async_client._transport.app.state.capturing_notifications_service.last_otp
    assert otp is not None

    verify = await async_client.post(
        "/auth/verify-otp",
        json={"phone": "+918103946120", "otp": otp},
    )
    assert verify.status_code == 200
    assert verify.json()["data"]["user_id"] == 9301


@pytest.mark.asyncio
async def test_send_otp_ambiguous_when_multiple_linked_share_phone(async_client, test_db_session):
    parent = User(
        user_id=9205,
        age=40,
        phone="6100000004",
        status="active",
        email="parent9205@example.com",
        relationship="self",
    )
    sub_a = User(
        user_id=9206,
        age=8,
        phone="6100000005",
        status="active",
        email="suba9206@example.com",
        parent_id=9205,
        relationship="child",
    )
    sub_b = User(
        user_id=9207,
        age=6,
        phone="6100000005",
        status="active",
        email="subb9207@example.com",
        parent_id=9205,
        relationship="child",
    )
    test_db_session.add_all([parent, sub_a, sub_b])
    await test_db_session.commit()

    response = await async_client.post("/auth/send-otp", json={"phone": "6100000005"})
    assert response.status_code == 409
    assert response.json()["error_code"] == "AMBIGUOUS_PHONE"


@pytest.mark.asyncio
async def test_verify_otp_issues_tokens_and_consumes_session(async_client, test_db_session):
    test_db_session.add(User(user_id=1002, age=30, phone="8888888888", status="active"))
    await test_db_session.commit()

    send = await async_client.post("/auth/send-otp", json={"phone": "8888888888"})
    session_id = send.json()["data"]["session_id"]

    otp = async_client._transport.app.state.capturing_notifications_service.last_otp
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
    test_db_session.add(User(user_id=1003, age=30, phone="7777777777", status="active"))
    await test_db_session.commit()

    send = await async_client.post("/auth/send-otp", json={"phone": "7777777777"})
    session_id = send.json()["data"]["session_id"]

    otp = async_client._transport.app.state.capturing_notifications_service.last_otp
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
    test_db_session.add(User(user_id=1004, age=30, phone="6666666666", status="active"))
    await test_db_session.commit()

    send = await async_client.post("/auth/send-otp", json={"phone": "6666666666"})
    session_id = send.json()["data"]["session_id"]

    otp = async_client._transport.app.state.capturing_notifications_service.last_otp
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


@pytest.mark.asyncio
async def test_send_otp_dispatches_via_whatapi_service_key(async_client, test_db_session):
    test_db_session.add(
        User(user_id=9401, age=30, phone="9401000001", status="active", email="user9401@example.com")
    )
    await test_db_session.commit()

    capture = async_client._transport.app.state.capturing_notifications_service
    capture.last_dispatch = None

    response = await async_client.post("/auth/send-otp", json={"phone": "9401000001"})
    assert response.status_code == 200
    assert capture.last_dispatch is not None
    assert capture.last_dispatch["service_key"] == "whatapi-otp"
    assert capture.last_dispatch["user_ids"] == [9401]
    assert capture.last_otp is not None


@pytest.mark.asyncio
async def test_send_and_verify_otp_via_email(async_client, test_db_session):
    test_db_session.add(
        User(
            user_id=9402,
            age=30,
            phone="9402000002",
            status="active",
            email="user9402@example.com",
        )
    )
    await test_db_session.commit()

    capture = async_client._transport.app.state.capturing_notifications_service
    capture.last_dispatch = None

    send = await async_client.post("/auth/send-otp", json={"email": "user9402@example.com"})
    assert send.status_code == 200
    assert capture.last_dispatch is not None
    assert capture.last_dispatch["service_key"] == "email-otp"
    assert capture.last_dispatch["user_ids"] == [9402]

    otp = capture.last_otp
    assert otp is not None

    verify = await async_client.post(
        "/auth/verify-otp",
        json={"email": "user9402@example.com", "otp": otp},
    )
    assert verify.status_code == 200
    assert verify.json()["data"]["user_id"] == 9402


@pytest.mark.asyncio
async def test_send_otp_returns_404_for_unknown_email(async_client):
    response = await async_client.post("/auth/send-otp", json={"email": "unknown@example.com"})
    assert response.status_code == 404
    assert response.json()["error_code"] == "USER_NOT_FOUND"


@pytest.mark.asyncio
async def test_send_otp_rejects_both_phone_and_email(async_client, test_db_session):
    test_db_session.add(User(user_id=9403, age=30, phone="9403000003", status="active"))
    await test_db_session.commit()

    response = await async_client.post(
        "/auth/send-otp",
        json={"phone": "9403000003", "email": "user9403@example.com"},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_resend_otp_requires_phone_or_email(async_client):
    response = await async_client.post("/auth/resend-otp", json={})
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_resend_otp_phone_only_dispatches_whatsapp_and_email(async_client, test_db_session):
    test_db_session.add(
        User(
            user_id=9501,
            age=30,
            phone="9501000001",
            status="active",
            email="user9501@example.com",
        )
    )
    await test_db_session.commit()

    capture = async_client._transport.app.state.capturing_notifications_service
    capture.reset_captures()

    response = await async_client.post("/auth/resend-otp", json={"phone": "9501000001"})
    assert response.status_code == 200
    assert isinstance(response.json()["data"]["session_id"], int)
    assert len(capture.dispatches) == 2
    service_keys = {d["service_key"] for d in capture.dispatches}
    assert service_keys == {"whatapi-otp", "email-otp"}
    assert capture.dispatches[0]["otp"] == capture.dispatches[1]["otp"]

    audit_rows = (
        await test_db_session.execute(
            select(DataAuditLog).where(
                DataAuditLog.user_id == 9501,
                DataAuditLog.action == "AUTH_RESEND_OTP",
            )
        )
    ).scalars().all()
    assert len(audit_rows) == 1


@pytest.mark.asyncio
async def test_resend_otp_email_only_dispatches_whatsapp_and_email(async_client, test_db_session):
    test_db_session.add(
        User(
            user_id=9502,
            age=30,
            phone="9502000002",
            status="active",
            email="user9502@example.com",
        )
    )
    await test_db_session.commit()

    capture = async_client._transport.app.state.capturing_notifications_service
    capture.reset_captures()

    response = await async_client.post(
        "/auth/resend-otp",
        json={"email": "user9502@example.com"},
    )
    assert response.status_code == 200
    assert len(capture.dispatches) == 2
    service_keys = {d["service_key"] for d in capture.dispatches}
    assert service_keys == {"whatapi-otp", "email-otp"}


@pytest.mark.asyncio
async def test_resend_otp_via_whatsapp_only(async_client, test_db_session):
    test_db_session.add(
        User(
            user_id=9503,
            age=30,
            phone="9503000003",
            status="active",
            email="user9503@example.com",
        )
    )
    await test_db_session.commit()

    capture = async_client._transport.app.state.capturing_notifications_service
    capture.reset_captures()

    response = await async_client.post(
        "/auth/resend-otp",
        json={"phone": "9503000003", "via": "whatsapp"},
    )
    assert response.status_code == 200
    assert len(capture.dispatches) == 1
    assert capture.last_dispatch["service_key"] == "whatapi-otp"


@pytest.mark.asyncio
async def test_resend_otp_via_email_only_from_phone_lookup(async_client, test_db_session):
    test_db_session.add(
        User(
            user_id=9504,
            age=30,
            phone="9504000004",
            status="active",
            email="user9504@example.com",
        )
    )
    await test_db_session.commit()

    capture = async_client._transport.app.state.capturing_notifications_service
    capture.reset_captures()

    response = await async_client.post(
        "/auth/resend-otp",
        json={"phone": "9504000004", "via": "email"},
    )
    assert response.status_code == 200
    assert len(capture.dispatches) == 1
    assert capture.last_dispatch["service_key"] == "email-otp"


@pytest.mark.asyncio
async def test_resend_otp_via_whatsapp_from_email_lookup(async_client, test_db_session):
    test_db_session.add(
        User(
            user_id=9505,
            age=30,
            phone="9505000005",
            status="active",
            email="user9505@example.com",
        )
    )
    await test_db_session.commit()

    capture = async_client._transport.app.state.capturing_notifications_service
    capture.reset_captures()

    response = await async_client.post(
        "/auth/resend-otp",
        json={"email": "user9505@example.com", "via": "whatsapp"},
    )
    assert response.status_code == 200
    assert len(capture.dispatches) == 1
    assert capture.last_dispatch["service_key"] == "whatapi-otp"


@pytest.mark.asyncio
async def test_resend_otp_phone_only_without_email_sends_whatsapp_only(async_client, test_db_session):
    test_db_session.add(User(user_id=9506, age=30, phone="9506000006", status="active"))
    await test_db_session.commit()

    capture = async_client._transport.app.state.capturing_notifications_service
    capture.reset_captures()

    response = await async_client.post("/auth/resend-otp", json={"phone": "9506000006"})
    assert response.status_code == 200
    assert len(capture.dispatches) == 1
    assert capture.last_dispatch["service_key"] == "whatapi-otp"


@pytest.mark.asyncio
async def test_resend_otp_via_email_fails_when_user_has_no_email(async_client, test_db_session):
    test_db_session.add(User(user_id=9507, age=30, phone="9507000007", status="active"))
    await test_db_session.commit()

    response = await async_client.post(
        "/auth/resend-otp",
        json={"phone": "9507000007", "via": "email"},
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_resend_otp_returns_404_for_unknown_user(async_client):
    response = await async_client.post("/auth/resend-otp", json={"phone": "9507999999"})
    assert response.status_code == 404
    assert response.json()["error_code"] == "USER_NOT_FOUND"


@pytest.mark.asyncio
async def test_resend_otp_replaces_existing_session(async_client, test_db_session):
    test_db_session.add(
        User(
            user_id=9508,
            age=30,
            phone="9508000008",
            status="active",
            email="user9508@example.com",
        )
    )
    await test_db_session.commit()

    first = await async_client.post("/auth/send-otp", json={"phone": "9508000008"})
    first_session_id = first.json()["data"]["session_id"]

    second = await async_client.post("/auth/resend-otp", json={"phone": "9508000008"})
    second_session_id = second.json()["data"]["session_id"]
    assert second_session_id != first_session_id

    sessions = (
        await test_db_session.execute(select(AuthOtpSession).where(AuthOtpSession.user_id == 9508))
    ).scalars().all()
    assert len(sessions) == 1
    assert sessions[0].session_id == second_session_id
