"""Integration tests for users routes."""

from __future__ import annotations

from datetime import date, time, timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.engagements.enums import BloodCollectionType
from modules.engagements.models import Engagement, EngagementParticipant, EngagementSlotInfo, ParticipantBloodBooking
from modules.engagements.blood_booking_enums import BloodBookingRelation, BloodBookingStatus
from modules.organizations.models import Organization
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    from tests.helpers.auth import user_auth_header
    return user_auth_header(user_id)


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


async def _seed_upcoming_slot_pbb(
    test_db_session,
    *,
    participant_id: int,
    slot_date: date,
    slot_time: time,
    cabin: str | None = None,
    booking_id: str | None = None,
) -> None:
    test_db_session.add(
        ParticipantBloodBooking(
            engagement_participant_id=participant_id,
            collection_date=slot_date,
            collection_time=slot_time,
            collection_cabin=cabin,
            booking_id=booking_id,
            relation=BloodBookingRelation.primary.value,
            status=BloodBookingStatus.active.value,
        )
    )
    await test_db_session.commit()


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
        blood_collection_type=BloodCollectionType.camp_collection,
        start_date=date.fromisoformat(slot_date),
        end_date=date.fromisoformat(slot_date),
        status="running",
    )
    participant = EngagementParticipant(
        engagement_participant_id=99002,
        engagement_id=99002,
        user_id=99002,
        booked_by_user_id=99002,
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
    await test_db_session.flush()
    await _seed_upcoming_slot_pbb(
        test_db_session,
        participant_id=99002,
        slot_date=date.fromisoformat(slot_date),
        slot_time=time(13, 40),
        cabin="blood_test_cabin_1",
    )

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
    user = User(user_id=99003, age=30, phone="990030000000", status="active", first_name="Home", address="User Home Addr")
    engagement = Engagement(
        engagement_id=99003,
        engagement_name="Home Collection Test",
        organization_id=None,
        engagement_code="UPCOMING-HOME-99003",
        city="BLR",
        slot_duration=60,
        blood_collection_type=BloodCollectionType.home_collection,
        start_date=date.fromisoformat(slot_date),
        end_date=date.fromisoformat(slot_date),
        status="running",
    )
    participant = EngagementParticipant(
        engagement_participant_id=99003,
        engagement_id=99003,
        user_id=99003,
        booked_by_user_id=99003,
    )
    test_db_session.add(user)
    await test_db_session.flush()
    test_db_session.add(engagement)
    await test_db_session.flush()
    test_db_session.add(participant)
    await test_db_session.flush()
    await _seed_upcoming_slot_pbb(
        test_db_session,
        participant_id=99003,
        slot_date=date.fromisoformat(slot_date),
        slot_time=time(10, 0),
    )

    response = await async_client.get("/users/me/upcoming-slot", headers=_auth_header(99003))

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["has_scheduled_slot"] is True
    slot = data["slots"][0]["slot"]
    assert slot["slot_start_time"] == "10:00 AM"
    assert slot["slot_end_time"] == "11:00 AM"
    assert slot["cabin"] is None
    assert data["slots"][0]["engagement"]["engagement_type"] == "b2c"
    assert data["slots"][0]["location"]["type"] == "home_collection"
    assert data["slots"][0]["location"]["display"] == "User Home Addr"


@pytest.mark.asyncio
async def test_upcoming_slot_home_collection_participant_address_wins(async_client, test_db_session):
    slot_date = (date.today() + timedelta(days=32)).isoformat()
    user = User(
        user_id=99004,
        age=30,
        phone="990040000000",
        status="active",
        first_name="HomeP",
        address="User Addr",
    )
    engagement = Engagement(
        engagement_id=99004,
        engagement_name="Home Participant Addr",
        organization_id=None,
        engagement_code="UPCOMING-HOME-99004",
        city="BLR",
        address="Engagement Venue Should Not Show",
        slot_duration=60,
        blood_collection_type=BloodCollectionType.home_collection,
        start_date=date.fromisoformat(slot_date),
        end_date=date.fromisoformat(slot_date),
        status="running",
    )
    participant = EngagementParticipant(
        engagement_participant_id=99004,
        engagement_id=99004,
        user_id=99004,
        booked_by_user_id=99004,
        address="Participant Home Addr",
    )
    test_db_session.add(user)
    await test_db_session.flush()
    test_db_session.add(engagement)
    await test_db_session.flush()
    test_db_session.add(participant)
    await test_db_session.flush()
    await _seed_upcoming_slot_pbb(
        test_db_session,
        participant_id=99004,
        slot_date=date.fromisoformat(slot_date),
        slot_time=time(11, 0),
    )

    response = await async_client.get("/users/me/upcoming-slot", headers=_auth_header(99004))
    data = response.json()["data"]
    assert data["slots"][0]["location"]["display"] == "Participant Home Addr"
    assert data["slots"][0]["location"]["type"] == "home_collection"


@pytest.mark.asyncio
async def test_upcoming_slot_home_collection_b2c_single_participant_seeds_engagement(async_client, test_db_session):
    slot_date = (date.today() + timedelta(days=33)).isoformat()
    user = User(user_id=99005, age=30, phone="990050000000", status="active", first_name="Seed")
    engagement = Engagement(
        engagement_id=99005,
        engagement_name="Home Seed Eng",
        organization_id=None,
        engagement_code="UPCOMING-HOME-99005",
        city="BLR",
        address="Seeded Engagement Addr",
        slot_duration=60,
        blood_collection_type=BloodCollectionType.home_collection,
        start_date=date.fromisoformat(slot_date),
        end_date=date.fromisoformat(slot_date),
        status="running",
    )
    participant = EngagementParticipant(
        engagement_participant_id=99005,
        engagement_id=99005,
        user_id=99005,
        booked_by_user_id=99005,
    )
    test_db_session.add(user)
    await test_db_session.flush()
    test_db_session.add(engagement)
    await test_db_session.flush()
    test_db_session.add(participant)
    await test_db_session.flush()
    await _seed_upcoming_slot_pbb(
        test_db_session,
        participant_id=99005,
        slot_date=date.fromisoformat(slot_date),
        slot_time=time(9, 30),
    )

    response = await async_client.get("/users/me/upcoming-slot", headers=_auth_header(99005))
    loc = response.json()["data"]["slots"][0]["location"]
    assert loc["display"] == "Seeded Engagement Addr"


@pytest.mark.asyncio
async def test_upcoming_slot_home_collection_two_participants_uses_user_not_engagement(
    async_client, test_db_session
):
    slot_date = (date.today() + timedelta(days=34)).isoformat()
    user = User(
        user_id=99006,
        age=30,
        phone="990060000000",
        status="active",
        first_name="Multi",
        address="User Addr Multi",
    )
    other = User(user_id=99007, age=28, phone="990070000000", status="active", first_name="Other")
    engagement = Engagement(
        engagement_id=99006,
        engagement_name="Home Multi",
        organization_id=None,
        engagement_code="UPCOMING-HOME-99006",
        city="BLR",
        address="Eng Addr Multi",
        slot_duration=60,
        blood_collection_type=BloodCollectionType.home_collection,
        start_date=date.fromisoformat(slot_date),
        end_date=date.fromisoformat(slot_date),
        status="running",
    )
    participant = EngagementParticipant(
        engagement_participant_id=99006,
        engagement_id=99006,
        user_id=99006,
        booked_by_user_id=99006,
    )
    other_participant = EngagementParticipant(
        engagement_participant_id=99007,
        engagement_id=99006,
        user_id=99007,
        booked_by_user_id=99007,
    )
    test_db_session.add_all([user, other])
    await test_db_session.flush()
    test_db_session.add(engagement)
    await test_db_session.flush()
    test_db_session.add_all([participant, other_participant])
    await test_db_session.flush()
    await _seed_upcoming_slot_pbb(
        test_db_session,
        participant_id=99006,
        slot_date=date.fromisoformat(slot_date),
        slot_time=time(8, 0),
    )

    response = await async_client.get("/users/me/upcoming-slot", headers=_auth_header(99006))
    assert response.json()["data"]["slots"][0]["location"]["display"] == "User Addr Multi"


@pytest.mark.asyncio
async def test_upcoming_slot_home_collection_b2b_does_not_seed_engagement(async_client, test_db_session):
    slot_date = (date.today() + timedelta(days=35)).isoformat()
    org = Organization(
        organization_id=99008,
        name="Corp",
        organization_type="corporate",
        status="active",
    )
    user = User(
        user_id=99008,
        age=30,
        phone="990080000000",
        status="active",
        first_name="B2BHome",
        address="B2B User Addr",
    )
    engagement = Engagement(
        engagement_id=99008,
        engagement_name="B2B Home",
        organization_id=99008,
        engagement_code="UPCOMING-HOME-99008",
        city="BLR",
        address="B2B Eng Addr",
        slot_duration=60,
        blood_collection_type=BloodCollectionType.home_collection,
        start_date=date.fromisoformat(slot_date),
        end_date=date.fromisoformat(slot_date),
        status="running",
    )
    participant = EngagementParticipant(
        engagement_participant_id=99008,
        engagement_id=99008,
        user_id=99008,
        booked_by_user_id=99008,
    )
    test_db_session.add(user)
    await test_db_session.flush()
    test_db_session.add(org)
    await test_db_session.flush()
    test_db_session.add(engagement)
    await test_db_session.flush()
    test_db_session.add(participant)
    await test_db_session.flush()
    await _seed_upcoming_slot_pbb(
        test_db_session,
        participant_id=99008,
        slot_date=date.fromisoformat(slot_date),
        slot_time=time(14, 0),
    )

    response = await async_client.get("/users/me/upcoming-slot", headers=_auth_header(99008))
    loc = response.json()["data"]["slots"][0]["location"]
    assert loc["type"] == "home_collection"
    assert loc["display"] == "B2B User Addr"


@pytest.mark.asyncio
async def test_upcoming_slot_camp_collection_uses_engagement_address(async_client, test_db_session):
    slot_date = (date.today() + timedelta(days=36)).isoformat()
    user = User(user_id=99009, age=30, phone="990090000000", status="active", first_name="CampLoc")
    engagement = Engagement(
        engagement_id=99009,
        engagement_name="Camp Loc",
        organization_id=None,
        engagement_code="UPCOMING-CAMP-99009",
        city="BLR",
        address="Camp Venue Address",
        slot_duration=60,
        blood_collection_type=BloodCollectionType.camp_collection,
        start_date=date.fromisoformat(slot_date),
        end_date=date.fromisoformat(slot_date),
        status="running",
    )
    participant = EngagementParticipant(
        engagement_participant_id=99009,
        engagement_id=99009,
        user_id=99009,
        booked_by_user_id=99009,
        address="Participant Home Ignored",
    )
    test_db_session.add(user)
    await test_db_session.flush()
    test_db_session.add(engagement)
    await test_db_session.flush()
    test_db_session.add(participant)
    await test_db_session.flush()
    await _seed_upcoming_slot_pbb(
        test_db_session,
        participant_id=99009,
        slot_date=date.fromisoformat(slot_date),
        slot_time=time(12, 0),
    )

    response = await async_client.get("/users/me/upcoming-slot", headers=_auth_header(99009))
    loc = response.json()["data"]["slots"][0]["location"]
    assert loc["type"] == "venue"
    assert loc["display"] == "Camp Venue Address"
