"""Onboard slot_detail validation for B2B cabin bookings."""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import text

from core.config import settings
from core.rate_limit import limiter
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


async def _seed_organization(test_db_session, *, organization_id: int, name: str):
    from modules.organizations.models import Organization

    test_db_session.add(
        Organization(
            organization_id=organization_id,
            name=name,
            organization_type="corporate",
            status="active",
        )
    )
    await test_db_session.commit()


async def _seed_packages(test_db_session, *, package_id: int):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (:pid, :pcode, :dname, 'active') ON CONFLICT (package_id) DO UPDATE SET "
            "package_code = EXCLUDED.package_code, display_name = EXCLUDED.display_name, status = EXCLUDED.status"
        ),
        {"pid": package_id, "pcode": f"PKG{package_id}", "dname": f"Package {package_id}"},
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status, bookings_count) "
            "VALUES (:did, :ref, :pname, 'test_provider', 'active', 0) ON CONFLICT (diagnostic_package_id) DO UPDATE SET "
            "reference_id = EXCLUDED.reference_id, package_name = EXCLUDED.package_name, "
            "diagnostic_provider = EXCLUDED.diagnostic_provider, status = EXCLUDED.status"
        ),
        {"did": package_id, "ref": f"REF{package_id}", "pname": f"Diag {package_id}"},
    )
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, require_bio_ai_report_url, require_participant_detail) "
            "VALUES ('booking-alert-whatsapp', 'Booking Alert', 'whatsapp', 'booking-alert', true, false, false, false) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true"
        )
    )
    await test_db_session.commit()


async def _engagement_type_id(test_db_session, code: str = "bio_ai") -> int:
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_types (code, display_name, is_active) "
            "VALUES (:code, :dn, true) "
            "ON CONFLICT (code) DO UPDATE SET is_active = true"
        ),
        {"code": code, "dn": code},
    )
    await test_db_session.commit()
    row = (
        await test_db_session.execute(
            text("SELECT id FROM engagement_types WHERE code = :code"),
            {"code": code},
        )
    ).one()
    return int(row[0])


def _slot_detail(*, capacity: int = 6, is_active: bool = True) -> dict:
    return {
        "blood_collection": {
            "2026-08-20": [
                {
                    "cabin_name": "Blood Test Cabin 1",
                    "cabin_key": "btc-001",
                    "start_time": "09:00",
                    "end_time": "17:00",
                    "slot_duration": 30,
                    "capacity_per_slot": capacity,
                    "breaks": [{"start_time": "13:00", "end_time": "14:00"}],
                    "is_active": is_active,
                }
            ]
        }
    }


async def _create_slot_engagement(
    async_client,
    test_db_session,
    *,
    user_id: int,
    employee_id: int,
    organization_id: int,
    code: str,
    capacity: int = 6,
    is_active: bool = True,
):
    await _seed_employee(test_db_session, user_id=user_id, employee_id=employee_id)
    await _seed_organization(test_db_session, organization_id=organization_id, name=f"Org {organization_id}")
    await _seed_packages(test_db_session, package_id=organization_id)
    type_id = await _engagement_type_id(test_db_session, "bio_ai")
    response = await async_client.post(
        "/engagements",
        headers=_auth_header(user_id),
        json={
            "engagement_name": f"Camp {code}",
            "organization_id": organization_id,
            "engagement_type": type_id,
            "assessment_package_id": organization_id,
            "diagnostic_package_id": organization_id,
            "city": "BLR",
            "slot_duration": 30,
            "start_date": "2026-08-20",
            "end_date": "2026-08-21",
            "engagement_code": code,
            "slot_detail": _slot_detail(capacity=capacity, is_active=is_active),
        },
    )
    assert response.status_code == 201, response.text
    return response.json()["data"]["engagement_id"]


def _onboard_payload(*, phone: str, slot: str = "09:00", cabin: str = "btc-001", date: str = "2026-08-20") -> dict:
    return {
        "age": 30,
        "first_name": "On",
        "last_name": "Board",
        "phone": phone,
        "email": f"{phone}@example.com",
        "city": "BLR",
        "blood_collection_date": date,
        "blood_collection_time_slot": slot,
        "blood_collection_cabin": cabin,
    }


@pytest.mark.asyncio
async def test_onboard_without_slot_detail_still_succeeds(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7420, employee_id=420)
    await _seed_organization(test_db_session, organization_id=9201, name="Legacy Org")
    await _seed_packages(test_db_session, package_id=9201)
    type_id = await _engagement_type_id(test_db_session, "bio_ai")
    created = await async_client.post(
        "/engagements",
        headers=_auth_header(7420),
        json={
            "engagement_name": "Legacy Camp",
            "organization_id": 9201,
            "engagement_type": type_id,
            "assessment_package_id": 9201,
            "diagnostic_package_id": 9201,
            "city": "BLR",
            "slot_duration": 30,
            "start_date": "2026-08-20",
            "end_date": "2026-08-21",
            "engagement_code": "SLOTLEG1",
        },
    )
    assert created.status_code == 201, created.text
    payload = _onboard_payload(phone="9201000001")
    payload.pop("blood_collection_cabin")
    response = await async_client.post("/users/code/SLOTLEG1/onboard", json=payload)
    assert response.status_code == 200, response.text


@pytest.mark.asyncio
async def test_onboard_persists_blood_collection_cabin(async_client, test_db_session):
    engagement_id = await _create_slot_engagement(
        async_client,
        test_db_session,
        user_id=7421,
        employee_id=421,
        organization_id=9202,
        code="SLOTOK01",
    )
    response = await async_client.post(
        "/users/code/SLOTOK01/onboard",
        json=_onboard_payload(phone="9202000001", slot="09:30"),
    )
    assert response.status_code == 200, response.text
    pid = response.json()["data"]["engagement_participant_id"]
    row = (
        await test_db_session.execute(
            text(
                "SELECT blood_collection_cabin, engagement_date, slot_start_time "
                "FROM engagement_participants WHERE engagement_participant_id = :pid"
            ),
            {"pid": pid},
        )
    ).first()
    assert row.blood_collection_cabin == "btc-001"
    assert str(row.engagement_date) == "2026-08-20"
    assert str(row.slot_start_time)[:5] == "09:30"
    assert engagement_id == response.json()["data"]["engagement_id"]


@pytest.mark.asyncio
async def test_onboard_rejects_unavailable_slot_variants(async_client, test_db_session):
    await _create_slot_engagement(
        async_client,
        test_db_session,
        user_id=7422,
        employee_id=422,
        organization_id=9203,
        code="SLOTBAD1",
    )
    cases = [
        _onboard_payload(phone="9203000001", date="2026-08-21"),
        _onboard_payload(phone="9203000002", cabin="missing"),
        _onboard_payload(phone="9203000003", slot="13:00"),
        _onboard_payload(phone="9203000004", slot="17:00"),
    ]
    for payload in cases:
        limiter.reset()
        response = await async_client.post("/users/code/SLOTBAD1/onboard", json=payload)
        assert response.status_code == 400, payload
        assert response.json() == {
            "error_code": "SLOT_UNAVAILABLE",
            "message": "No such Slot Available",
        }


@pytest.mark.asyncio
async def test_onboard_rejects_when_slot_capacity_is_full(async_client, test_db_session):
    await _create_slot_engagement(
        async_client,
        test_db_session,
        user_id=7423,
        employee_id=423,
        organization_id=9204,
        code="SLOTFULL",
        capacity=1,
    )
    first = await async_client.post(
        "/users/code/SLOTFULL/onboard",
        json=_onboard_payload(phone="9204000001"),
    )
    assert first.status_code == 200, first.text
    second = await async_client.post(
        "/users/code/SLOTFULL/onboard",
        json=_onboard_payload(phone="9204000002"),
    )
    assert second.status_code == 400
    assert second.json()["error_code"] == "SLOT_UNAVAILABLE"
    assert second.json()["message"] == "No such Slot Available"
