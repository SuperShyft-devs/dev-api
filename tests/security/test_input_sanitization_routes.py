"""HTTP-level input sanitization and injection rejection tests."""

from __future__ import annotations

from datetime import timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.users.models import User

_XSS = "<script>alert(1)</script>"
_INVALID_NAME = "John3"
_INVALID_PIN = "12345"


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token(
        {"sub": str(user_id)},
        timedelta(minutes=5),
        secret_key=settings.JWT_SECRET_KEY,
    )
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int = 1):
    user = User(user_id=user_id, age=30, phone=f"{user_id:010d}", status="active")
    test_db_session.add(user)
    await test_db_session.flush()
    test_db_session.add(
        Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active")
    )
    await test_db_session.commit()


@pytest.mark.parametrize(
    "payload",
    [
        {"phone": "123", "email": None},
        {"phone": None, "email": "not-an-email"},
        {"phone": "8103946120", "email": "a@b.com"},
    ],
)
@pytest.mark.asyncio
async def test_auth_send_otp_rejects_invalid_identifiers(async_client, payload):
    response = await async_client.post("/auth/send-otp", json=payload)
    assert response.status_code == 422


@pytest.mark.parametrize(
    "field,bad_value",
    [
        ("name", _XSS),
        ("address", _XSS),
        ("pin_code", _INVALID_PIN),
        ("pin_code", "abcdef"),
    ],
)
@pytest.mark.asyncio
async def test_create_organization_rejects_unsafe_fields(
    async_client, test_db_session, field, bad_value
):
    await _seed_employee(test_db_session, user_id=92001, employee_id=9201)
    payload = {"name": "Acme Corp"}
    payload[field] = bad_value
    response = await async_client.post(
        "/organizations",
        headers=_auth_header(92001),
        json=payload,
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_support_ticket_rejects_xss_in_query(async_client, test_db_session):
    test_db_session.add(User(user_id=92002, age=30, phone="9200200000", status="active"))
    await test_db_session.commit()
    response = await async_client.post(
        "/support/tickets",
        headers=_auth_header(92002),
        json={"user_id": 92002, "query_text": _XSS},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_support_ticket_rejects_overlong_query(async_client, test_db_session):
    test_db_session.add(User(user_id=92003, age=30, phone="9200300000", status="active"))
    await test_db_session.commit()
    response = await async_client.post(
        "/support/tickets",
        headers=_auth_header(92003),
        json={"user_id": 92003, "query_text": "a" * 1001},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_checklist_template_rejects_xss_description(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=92004, employee_id=9204)
    response = await async_client.post(
        "/checklist-templates",
        headers=_auth_header(92004),
        json={"name": "Onsite Prep", "description": _XSS, "audience": "internal"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_public_onboard_rejects_invalid_person_name(async_client):
    payload = {
        "age": 30,
        "phone": "8103946120",
        "first_name": _INVALID_NAME,
        "engagement_type": "bio_ai",
        "blood_collection_date": "2026-12-01",
        "blood_collection_time_slot": "09:00",
    }
    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_public_onboard_rejects_invalid_pincode(async_client):
    payload = {
        "age": 30,
        "phone": "8103946121",
        "first_name": "Rahul",
        "pincode": _INVALID_PIN,
        "engagement_type": "bio_ai",
        "blood_collection_date": "2026-12-01",
        "blood_collection_time_slot": "09:00",
    }
    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_public_onboard_rejects_xss_in_address(async_client):
    payload = {
        "age": 30,
        "phone": "8103946122",
        "first_name": "Rahul",
        "address": _XSS,
        "engagement_type": "bio_ai",
        "blood_collection_date": "2026-12-01",
        "blood_collection_time_slot": "09:00",
    }
    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_verify_otp_rejects_non_digit_otp(async_client):
    response = await async_client.post(
        "/auth/verify-otp",
        json={"phone": "8103946123", "otp": "abcd", "email": None},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_engagement_type_create_rejects_invalid_code(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=92005, employee_id=9205)
    response = await async_client.post(
        "/engagement-types",
        headers=_auth_header(92005),
        json={"code": "BAD CODE!", "display_name": "Test Type", "is_active": True},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_create_organization_rejects_negative_bd_employee_id(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=92006, employee_id=9206)
    response = await async_client.post(
        "/organizations",
        headers=_auth_header(92006),
        json={"name": "Valid Org", "bd_employee_id": -1},
    )
    assert response.status_code == 422
