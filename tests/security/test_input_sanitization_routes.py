"""HTTP-level input sanitization and injection rejection tests."""

from __future__ import annotations

import pytest

from modules.users.models import User
from tests.helpers.auth import employee_auth_header, seed_employee, user_auth_header

_XSS = "<script>alert(1)</script>"
_INVALID_NAME = "John3"
_INVALID_PIN = "12345"


def _auth_header(employee_id: int) -> dict[str, str]:
    return employee_auth_header(employee_id)


async def _seed_employee(test_db_session, *, employee_id: int = 1):
    await seed_employee(test_db_session, employee_id=employee_id, role="admin")


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
    assert response.status_code in (400, 422)


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
    await _seed_employee(test_db_session, employee_id=9201)
    payload = {"name": "Acme Corp"}
    payload[field] = bad_value
    response = await async_client.post(
        "/organizations",
        headers=_auth_header(9201),
        json=payload,
    )
    assert response.status_code in (400, 422)


@pytest.mark.asyncio
async def test_support_ticket_rejects_xss_in_query(async_client, test_db_session):
    test_db_session.add(User(user_id=92002, age=30, phone="9200200000", status="active"))
    await test_db_session.commit()
    response = await async_client.post(
        "/support/tickets",
        headers=user_auth_header(92002),
        json={"user_id": 92002, "query_text": _XSS},
    )
    assert response.status_code in (400, 422)


@pytest.mark.asyncio
async def test_support_ticket_rejects_overlong_query(async_client, test_db_session):
    test_db_session.add(User(user_id=92003, age=30, phone="9200300000", status="active"))
    await test_db_session.commit()
    response = await async_client.post(
        "/support/tickets",
        headers=user_auth_header(92003),
        json={"user_id": 92003, "query_text": "a" * 1001},
    )
    assert response.status_code in (400, 422)


@pytest.mark.asyncio
async def test_checklist_template_rejects_xss_description(async_client, test_db_session):
    await _seed_employee(test_db_session, employee_id=9204)
    response = await async_client.post(
        "/checklist-templates",
        headers=_auth_header(9204),
        json={"name": "Onsite Prep", "description": _XSS, "audience": "internal"},
    )
    assert response.status_code in (400, 422)


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
    assert response.status_code in (400, 422)


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
    assert response.status_code in (400, 422)


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
    assert response.status_code in (400, 422)


@pytest.mark.asyncio
async def test_verify_otp_rejects_non_digit_otp(async_client):
    response = await async_client.post(
        "/auth/verify-otp",
        json={"phone": "8103946123", "otp": "abcd", "email": None},
    )
    assert response.status_code in (400, 422)


@pytest.mark.asyncio
async def test_engagement_type_create_rejects_invalid_code(async_client, test_db_session):
    await _seed_employee(test_db_session, employee_id=9205)
    response = await async_client.post(
        "/engagement-types",
        headers=_auth_header(9205),
        json={"code": "BAD CODE!", "display_name": "Test Type", "is_active": True},
    )
    assert response.status_code in (400, 422)


@pytest.mark.asyncio
async def test_create_organization_rejects_negative_bd_employee_id(async_client, test_db_session):
    await _seed_employee(test_db_session, employee_id=9206)
    response = await async_client.post(
        "/organizations",
        headers=_auth_header(9206),
        json={"name": "Valid Org", "bd_employee_id": -1},
    )
    assert response.status_code in (400, 422)
