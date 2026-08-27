"""Integration tests for users routes."""

from __future__ import annotations

from datetime import date, time, timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.engagements.models import Engagement, EngagementParticipant, EngagementSlotInfo
from modules.organizations.models import Organization
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_get_me_requires_auth(async_client):
    response = await async_client.get("/users/me")
    assert response.status_code == 401
    assert response.json() == {"error_code": "AUTH_FAILED", "message": "Authentication failed"}


@pytest.mark.asyncio
async def test_get_me_returns_profile(async_client, test_db_session):
    user = User(user_id=1010, age=30, phone="5555555555", status="active", first_name="A")
    test_db_session.add(user)
    await test_db_session.commit()

    headers = _auth_header(1010)
    response = await async_client.get("/users/me", headers=headers)

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["user_id"] == 1010
    assert data["phone"] == "5555555555"
    assert data["first_name"] == "A"


@pytest.mark.asyncio
async def test_update_me_updates_editable_fields(async_client, test_db_session):
    user = User(user_id=1010, age=30, phone="5555555555", status="active", first_name="A")
    test_db_session.add(user)
    await test_db_session.commit()

    headers = _auth_header(1010)
    payload = {"age": 30, "first_name": "New", "city": "Pune"}

    response = await async_client.put("/users/me", headers=headers, json=payload)
    assert response.status_code == 200

    data = response.json()["data"]
    assert data["first_name"] == "New"
    assert data["city"] == "Pune"


@pytest.mark.asyncio
async def test_update_me_allows_same_phone_when_subprofile_shares_number(async_client, test_db_session):
    """Parent and sub-profile may share phone; updating other fields must not 500."""
    parent = User(
        user_id=1020,
        age=30,
        phone="+919876543210",
        status="active",
        first_name="Parent",
    )
    sub = User(
        user_id=1021,
        age=28,
        phone="+919876543210",
        status="active",
        first_name="Sub",
        parent_id=1020,
    )
    test_db_session.add_all([parent, sub])
    await test_db_session.commit()

    headers = _auth_header(1021)
    response = await async_client.put(
        "/users/me",
        headers=headers,
        json={"age": 28, "phone": "+919876543210", "first_name": "Sub Updated"},
    )
    assert response.status_code == 200
    assert response.json()["data"]["first_name"] == "Sub Updated"


@pytest.mark.asyncio
async def test_update_me_updates_phone(async_client, test_db_session):
    user = User(user_id=1010, age=30, phone="5555555555", status="active", first_name="A")
    test_db_session.add(user)
    await test_db_session.commit()

    headers = _auth_header(1010)
    payload = {"age": 30, "phone": "5999999999"}

    response = await async_client.put("/users/me", headers=headers, json=payload)
    assert response.status_code == 200
    assert response.json()["data"]["phone"] == "5999999999"


@pytest.mark.asyncio
async def test_get_me_status_returns_active_flag(async_client, test_db_session):
    user = User(user_id=1010, age=30, phone="5555555555", status="active")
    test_db_session.add(user)
    await test_db_session.commit()

    headers = _auth_header(1010)
    response = await async_client.get("/users/me/status", headers=headers)

    assert response.status_code == 200
    assert response.json()["data"] == {"user_id": 1010, "status": "active", "is_active": True}


@pytest.mark.asyncio
async def test_inactive_user_is_forbidden(async_client, test_db_session):
    user = User(user_id=1010, age=30, phone="5555555555", status="inactive")
    test_db_session.add(user)
    await test_db_session.commit()

    headers = _auth_header(1010)
    response = await async_client.get("/users/me", headers=headers)

    assert response.status_code == 403
    assert response.json() == {
        "error_code": "FORBIDDEN",
        "message": "You do not have permission to perform this action",
    }


@pytest.mark.asyncio
async def test_update_sub_profile_persists_email(async_client, test_db_session):
    parent = User(
        user_id=9501,
        age=40,
        phone="6222222221",
        status="active",
        email="parent9501@example.com",
        relationship="self",
    )
    child = User(
        user_id=9502,
        age=20,
        phone="6222222222",
        status="active",
        email="child+old@example.com",
        parent_id=9501,
        relationship="child",
    )
    test_db_session.add_all([parent, child])
    await test_db_session.commit()

    headers = _auth_header(9501)
    response = await async_client.put(
        "/users/me/profiles/9502",
        headers=headers,
        json={
            "age": 20,
            "email": "pratheek@gmail.com",
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["email"] == "pratheek@gmail.com"


@pytest.mark.asyncio
async def test_child_can_unlink_own_profile(async_client, test_db_session):
    parent = User(
        user_id=9301,
        age=40,
        phone="6111111111",
        status="active",
        email="parent9301@example.com",
        relationship="self",
    )
    child = User(
        user_id=9302,
        age=10,
        phone="6111111112",
        status="active",
        email="child9302@example.com",
        parent_id=9301,
        relationship="child",
    )
    test_db_session.add_all([parent, child])
    await test_db_session.commit()

    headers = _auth_header(9302)
    response = await async_client.post(
        "/users/me/unlink",
        headers=headers,
        json={},
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["user_id"] == 9302
    assert data["parent_id"] is None
    assert data["email"] == "child9302@example.com"


@pytest.mark.asyncio
async def test_child_unlink_rejects_child_user_id(async_client, test_db_session):
    parent = User(
        user_id=9401,
        age=40,
        phone="6111111111",
        status="active",
        email="parent9401@example.com",
        relationship="self",
    )
    child = User(
        user_id=9402,
        age=10,
        phone="6111111112",
        status="active",
        email="child9402@example.com",
        parent_id=9401,
        relationship="child",
    )
    test_db_session.add_all([parent, child])
    await test_db_session.commit()

    headers = _auth_header(9402)
    response = await async_client.post(
        "/users/me/unlink",
        headers=headers,
        json={"child_user_id": 9402},
    )

    assert response.status_code == 400
    assert response.json()["error_code"] == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_primary_unlink_requires_child_user_id(async_client, test_db_session):
    primary = User(
        user_id=9303,
        age=35,
        phone="6111111113",
        status="active",
        email="primary9303@example.com",
        relationship="self",
    )
    test_db_session.add(primary)
    await test_db_session.commit()

    headers = _auth_header(9303)
    response = await async_client.post(
        "/users/me/unlink",
        headers=headers,
        json={},
    )

    assert response.status_code == 400
    assert response.json()["error_code"] == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_primary_can_unlink_sub_profile(async_client, test_db_session):
    parent = User(
        user_id=9310,
        age=40,
        phone="6111111111",
        status="active",
        email="parent9310@example.com",
        relationship="self",
    )
    child = User(
        user_id=9311,
        age=12,
        phone="6111111114",
        status="active",
        email="child9311@example.com",
        parent_id=9310,
        relationship="child",
    )
    test_db_session.add_all([parent, child])
    await test_db_session.commit()

    headers = _auth_header(9310)
    response = await async_client.post(
        "/users/me/unlink",
        headers=headers,
        json={"child_user_id": 9311},
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["user_id"] == 9311
    assert data["parent_id"] is None
    assert data["email"] == "child9311@example.com"


@pytest.mark.asyncio
async def test_primary_cannot_unlink_unrelated_sub_profile(async_client, test_db_session):
    actual_parent = User(
        user_id=9320,
        age=40,
        phone="6111111112",
        status="active",
        email="parent9320@example.com",
        relationship="self",
    )
    child = User(
        user_id=9321,
        age=11,
        phone="6111111113",
        status="active",
        email="child9321@example.com",
        parent_id=9320,
        relationship="child",
    )
    other_primary = User(
        user_id=9330,
        age=38,
        phone="6111111114",
        status="active",
        email="other9330@example.com",
        relationship="self",
    )
    test_db_session.add_all([actual_parent, child, other_primary])
    await test_db_session.commit()

    headers = _auth_header(9330)
    response = await async_client.post(
        "/users/me/unlink",
        headers=headers,
        json={"child_user_id": 9321},
    )

    assert response.status_code == 403
    assert response.json()["error_code"] == "FORBIDDEN"


def _blood_collection_slot_detail(slot_date: str) -> dict:
    return {
        "blood_collection": {
            slot_date: {
                "is_enable": True,
                "cabins": [
                    {
                        "cabin_name": "Blood Test Cabin 1",
                        "cabin_key": "blood_test_cabin_1",
                        "start_time": "09:00",
                        "end_time": "17:00",
                        "slot_duration": 30,
                        "capacity_per_slot": 2,
                        "breaks": [],
                        "is_active": True,
                    }
                ],
            }
        }
    }


@pytest.mark.asyncio
async def test_upcoming_slot_empty(async_client, test_db_session):
    user = User(user_id=99001, age=30, phone="990010000000", status="active", first_name="Empty")
    test_db_session.add(user)
    await test_db_session.commit()

    response = await async_client.get("/users/me/upcoming-slot", headers=_auth_header(99001))

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["has_scheduled_slot"] is False
    assert data["slots"] == []


@pytest.mark.asyncio
async def test_upcoming_slot_uses_cabin_duration(async_client, test_db_session):
    slot_date = (date.today() + timedelta(days=30)).isoformat()
    user = User(user_id=99002, age=30, phone="990020000000", status="active", first_name="Camp")
    org = Organization(
        organization_id=99002,
        name="NVIDIA",
        organization_type="corporate",
        status="active",
    )
    slot_info = EngagementSlotInfo(slot_detail_id=99002, slot_detail=_blood_collection_slot_detail(slot_date))
    engagement = Engagement(
        engagement_id=99002,
        engagement_name="Camp Slot Test",
        organization_id=99002,
        engagement_code="UPCOMING-CAMP-99002",
        city="BLR",
        slot_duration=60,
        slot_detail_id=99002,
        start_date=date.fromisoformat(slot_date),
        end_date=date.fromisoformat(slot_date),
        status="running",
    )
    participant = EngagementParticipant(
        engagement_participant_id=99002,
        engagement_id=99002,
        user_id=99002,
        booked_by_user_id=99002,
        engagement_date=date.fromisoformat(slot_date),
        slot_start_time=time(13, 40),
        blood_collection_cabin="blood_test_cabin_1",
    )
    test_db_session.add(user)
    await test_db_session.flush()
    test_db_session.add(org)
    await test_db_session.flush()
    test_db_session.add(slot_info)
    await test_db_session.flush()
    test_db_session.add(engagement)
    await test_db_session.flush()
    test_db_session.add(participant)
    await test_db_session.commit()

    response = await async_client.get("/users/me/upcoming-slot", headers=_auth_header(99002))

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["has_scheduled_slot"] is True
    assert len(data["slots"]) == 1
    slot = data["slots"][0]["slot"]
    assert slot["slot_start_time"] == "1:40 PM"
    assert slot["slot_end_time"] == "2:10 PM"
    assert slot["engagement_date"] == slot_date
    assert slot["cabin"] == "blood_test_cabin_1"
    assert data["slots"][0]["engagement"]["engagement_type"] == "b2b"
    assert data["slots"][0]["engagement"]["organization_name"] == "NVIDIA"


@pytest.mark.asyncio
async def test_upcoming_slot_home_collection_fallback(async_client, test_db_session):
    slot_date = (date.today() + timedelta(days=31)).isoformat()
    user = User(user_id=99003, age=30, phone="990030000000", status="active", first_name="Home")
    engagement = Engagement(
        engagement_id=99003,
        engagement_name="Home Collection Test",
        organization_id=None,
        engagement_code="UPCOMING-HOME-99003",
        city="BLR",
        slot_duration=60,
        start_date=date.fromisoformat(slot_date),
        end_date=date.fromisoformat(slot_date),
        status="running",
    )
    participant = EngagementParticipant(
        engagement_participant_id=99003,
        engagement_id=99003,
        user_id=99003,
        booked_by_user_id=99003,
        engagement_date=date.fromisoformat(slot_date),
        slot_start_time=time(10, 0),
        blood_collection_cabin=None,
    )
    test_db_session.add(user)
    await test_db_session.flush()
    test_db_session.add(engagement)
    await test_db_session.flush()
    test_db_session.add(participant)
    await test_db_session.commit()

    response = await async_client.get("/users/me/upcoming-slot", headers=_auth_header(99003))

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["has_scheduled_slot"] is True
    slot = data["slots"][0]["slot"]
    assert slot["slot_start_time"] == "10:00 AM"
    assert slot["slot_end_time"] == "11:00 AM"
    assert slot["cabin"] is None
    assert data["slots"][0]["engagement"]["engagement_type"] == "b2c"
