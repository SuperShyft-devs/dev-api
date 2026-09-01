"""Tests for PATCH participant schedule fields (date, slot, cabin)."""

from __future__ import annotations

from datetime import date, time, timedelta

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token
from modules.engagements.models import BloodCollectionType, Engagement, EngagementParticipant
from modules.users.models import User
from tests.modules.users.test_users_onboard_slot_routes import (
    _create_slot_engagement,
    _engagement_type_id,
    _onboard_payload,
    _seed_employee,
    _seed_organization,
)


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_packages_for_engagement(test_db_session, *, package_id: int):
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
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_patch_participant_schedule_updates_fields(async_client, test_db_session):
    engagement_id = await _create_slot_engagement(
        async_client,
        test_db_session,
        user_id=79811,
        employee_id=811,
        organization_id=9811,
        code="SCHPATCH1",
    )
    onboard = await async_client.post(
        "/users/code/SCHPATCH1/onboard",
        json=_onboard_payload(phone="9811000001", slot="09:00"),
    )
    assert onboard.status_code == 200, onboard.text
    user_id = onboard.json()["data"]["user_id"]

    response = await async_client.patch(
        f"/engagements/{engagement_id}/participants/{user_id}",
        headers=_auth_header(79811),
        json={
            "engagement_date": "2026-08-20",
            "slot_start_time": "09:30",
            "blood_collection_cabin": "blood_test_cabin_1",
        },
    )
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert data["engagement_date"] == "2026-08-20"
    assert data["slot_start_time"] == "09:30:00"
    assert data["blood_collection_cabin"] == "blood_test_cabin_1"


@pytest.mark.asyncio
async def test_patch_participant_schedule_allows_same_slot_when_at_capacity(async_client, test_db_session):
    engagement_id = await _create_slot_engagement(
        async_client,
        test_db_session,
        user_id=79812,
        employee_id=812,
        organization_id=9812,
        code="SCHPATCH2",
        capacity=1,
    )
    onboard = await async_client.post(
        "/users/code/SCHPATCH2/onboard",
        json=_onboard_payload(phone="9812000001", slot="09:00"),
    )
    assert onboard.status_code == 200, onboard.text
    user_id = onboard.json()["data"]["user_id"]

    response = await async_client.patch(
        f"/engagements/{engagement_id}/participants/{user_id}",
        headers=_auth_header(79812),
        json={
            "engagement_date": "2026-08-20",
            "slot_start_time": "09:00",
            "blood_collection_cabin": "blood_test_cabin_1",
        },
    )
    assert response.status_code == 200, response.text


@pytest.mark.asyncio
async def test_patch_participant_schedule_rejects_full_slot(async_client, test_db_session):
    engagement_id = await _create_slot_engagement(
        async_client,
        test_db_session,
        user_id=79813,
        employee_id=813,
        organization_id=9813,
        code="SCHPATCH3",
        capacity=1,
    )
    first = await async_client.post(
        "/users/code/SCHPATCH3/onboard",
        json=_onboard_payload(phone="9813000001", slot="09:00"),
    )
    assert first.status_code == 200, first.text
    second = await async_client.post(
        "/users/code/SCHPATCH3/onboard",
        json=_onboard_payload(phone="9813000002", slot="09:30"),
    )
    assert second.status_code == 200, second.text

    user_b = (
        await test_db_session.execute(
            text(
                "SELECT user_id FROM engagement_participants "
                "WHERE engagement_id = :eid AND slot_start_time = '09:30:00'"
            ),
            {"eid": engagement_id},
        )
    ).scalar_one()

    response = await async_client.patch(
        f"/engagements/{engagement_id}/participants/{user_b}",
        headers=_auth_header(79813),
        json={
            "engagement_date": "2026-08-20",
            "slot_start_time": "09:00",
            "blood_collection_cabin": "blood_test_cabin_1",
        },
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "SLOT_UNAVAILABLE"


@pytest.mark.asyncio
async def test_patch_participant_schedule_rejects_invalid_slot(async_client, test_db_session):
    engagement_id = await _create_slot_engagement(
        async_client,
        test_db_session,
        user_id=79814,
        employee_id=814,
        organization_id=9814,
        code="SCHPATCH4",
    )
    onboard = await async_client.post(
        "/users/code/SCHPATCH4/onboard",
        json=_onboard_payload(phone="9814000001", slot="09:00"),
    )
    assert onboard.status_code == 200, onboard.text
    user_id = onboard.json()["data"]["user_id"]

    response = await async_client.patch(
        f"/engagements/{engagement_id}/participants/{user_id}",
        headers=_auth_header(79814),
        json={
            "engagement_date": "2026-08-21",
            "slot_start_time": "09:00",
            "blood_collection_cabin": "blood_test_cabin_1",
        },
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "SLOT_UNAVAILABLE"


@pytest.mark.asyncio
async def test_patch_participant_schedule_rejects_home_collection(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=79815, employee_id=815)
    await _seed_organization(test_db_session, organization_id=9815, name="Home Org")
    await _seed_packages_for_engagement(test_db_session, package_id=9815)
    type_id = await _engagement_type_id(test_db_session, "bio_ai")

    test_db_session.add(User(user_id=98151, age=30, phone="98151000000", status="active"))
    await test_db_session.flush()

    test_db_session.add(
        Engagement(
            engagement_id=9815,
            engagement_name="Home Collection Engagement",
            organization_id=9815,
            engagement_code="SCH9815",
            engagement_type=type_id,
            assessment_package_id=9815,
            diagnostic_package_id=9815,
            city="BLR",
            slot_duration=30,
            start_date=date(2026, 8, 20),
            end_date=date(2026, 8, 20),
            status="running",
            blood_collection_type=BloodCollectionType.home_collection,
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=98151,
            engagement_id=9815,
            user_id=98151,
            engagement_date=date(2026, 8, 20),
            slot_start_time=time(9, 0),
        )
    )
    await test_db_session.commit()

    response = await async_client.patch(
        "/engagements/9815/participants/98151",
        headers=_auth_header(79815),
        json={
            "engagement_date": "2026-08-20",
            "slot_start_time": "09:30",
            "blood_collection_cabin": "blood_test_cabin_1",
        },
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "SCHEDULE_UPDATE_NOT_ALLOWED"


@pytest.mark.asyncio
async def test_get_engagement_includes_public_slot_detail(async_client, test_db_session):
    engagement_id = await _create_slot_engagement(
        async_client,
        test_db_session,
        user_id=79816,
        employee_id=816,
        organization_id=9816,
        code="SCHPATCH5",
    )

    response = await async_client.get(
        f"/engagements/{engagement_id}",
        headers=_auth_header(79816),
    )
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert data["slot_detail"] is not None
    assert data["public_slot_detail"] is not None
    assert "blood_collection" in data["public_slot_detail"]
    cabins = data["public_slot_detail"]["blood_collection"]["2026-08-20"]["cabins"]
    assert cabins[0]["available_slots"][0]["spot_left"] >= 0
