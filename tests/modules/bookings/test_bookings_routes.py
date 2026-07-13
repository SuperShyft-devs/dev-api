"""Tests for batch booking endpoints under `/book`."""

from __future__ import annotations

import hashlib
import hmac
from datetime import date, time, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


def _mock_razorpay_order(*, amount_paise: int, receipt: str):
    return {"id": f"order_test_{receipt}", "amount": amount_paise, "currency": "INR"}


def _razorpay_signature(*, order_id: str, payment_id: str) -> str:
    message = f"{order_id}|{payment_id}"
    return hmac.new(
        key=settings.RAZORPAY_KEY_SECRET.encode(),
        msg=message.encode(),
        digestmod=hashlib.sha256,
    ).hexdigest()


_VERIFY_PAYLOAD = {
    "razorpay_order_id": "order_test_x",
    "razorpay_payment_id": "pay_test_x",
    "razorpay_signature": "sig",
}


@pytest.mark.asyncio
async def test_book_bio_ai_batch_requires_auth(async_client):
    response = await async_client.post("/book/bio-ai", json=_VERIFY_PAYLOAD)
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_book_pay_requires_auth(async_client):
    response = await async_client.post(
        "/book/pay",
        json={"members": [{"user_id": 1, "engagement_id": 1}]},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_book_pay_rejects_duplicate_member_user_id(async_client, test_db_session):
    await _seed_healthians_diagnostic_package(test_db_session)
    u = User(
        user_id=940001,
        age=30,
        phone="9400010000",
        status="active",
        first_name="Dup",
        last_name="Pay",
        gender="male",
        city="City",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()

    member = {"user_id": 940001, "engagement_id": 940101}
    response = await async_client.post(
        "/book/pay",
        headers=_auth_header(940001),
        json={"members": [member, member]},
    )
    assert response.status_code == 400

# --- Booking flow API tests (drafts, serviceability, slots, lock) ---


async def _seed_healthians_diagnostic_package(test_db_session, *, package_id: int = 1) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package "
            "(diagnostic_package_id, reference_id, package_name, diagnostic_provider, status, price, external_package_id) "
            "VALUES (:id, 'REF-H', 'Healthians Package', 'healthians', 'active', 500, 101) "
            "ON CONFLICT (diagnostic_package_id) DO UPDATE SET "
            "diagnostic_provider = EXCLUDED.diagnostic_provider, "
            "external_package_id = EXCLUDED.external_package_id, status = EXCLUDED.status"
        ),
        {"id": package_id},
    )
    await test_db_session.commit()


async def _seed_draft_engagement(
    test_db_session,
    *,
    engagement_id: int,
    user_id: int,
    booked_by_user_id: int,
    address: str | None = "Flat 1, Block A, Near Park, Mumbai - 400001",
    locked: bool = False,
) -> None:
    from modules.engagements.models import BloodCollectionType, Engagement, EngagementParticipant

    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="test-draft",
            organization_id=None,
            engagement_code=f"DRF{engagement_id}",
            diagnostic_package_id=1,
            city="Mumbai",
            address=address,
            sub_locality="Block A",
            pincode="400001",
            latitude=19.0760,
            longitude=72.8777,
            healthians_zone_id="440",
            slot_duration=20,
            status="draft",
            blood_collection_type=BloodCollectionType.home_collection,
        )
    )
    participant_kwargs: dict = {
        "engagement_id": engagement_id,
        "user_id": user_id,
        "booked_by_user_id": booked_by_user_id,
    }
    if locked:
        participant_kwargs.update({
            "engagement_date": date(2026, 7, 15),
            "slot_start_time": time(6, 0),
            "blood_collection_time_slot_id": "slot-123",
        })
    else:
        participant_kwargs.update({
            "engagement_date": None,
            "slot_start_time": None,
        })
    test_db_session.add(EngagementParticipant(**participant_kwargs))
    await test_db_session.commit()


_CHECK_SERVICE_PAYLOAD = {
    "members": [
        {
            "user_id": 930001,
            "house_flat_no": "Flat 12",
            "building_area": "Green Park",
            "landmark": "Near Mall",
            "city": "Mumbai",
            "pincode": "400001",
            "diagnostic_package_id": 1,
        }
    ]
}


@pytest.mark.asyncio
async def test_get_my_drafts_returns_engagements(async_client, test_db_session):
    await _seed_healthians_diagnostic_package(test_db_session)
    u = User(
        user_id=930001,
        age=30,
        phone="9300010000",
        status="active",
        first_name="Draft",
        last_name="User",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()
    await _seed_draft_engagement(test_db_session, engagement_id=930101, user_id=930001, booked_by_user_id=930001)

    response = await async_client.get("/book/me/drafts", headers=_auth_header(930001))
    assert response.status_code == 200
    engagements = response.json()["data"]["engagements"]
    assert len(engagements) == 1
    assert engagements[0]["engagement_id"] == 930101
    assert engagements[0]["status"] == "draft"
    assert engagements[0]["resume_step"] == "booking_date"
    assert engagements[0]["address"] is not None


@pytest.mark.asyncio
async def test_get_my_drafts_visible_to_booker(async_client, test_db_session):
    await _seed_healthians_diagnostic_package(test_db_session)
    parent = User(
        user_id=930010,
        age=40,
        phone="9300100000",
        status="active",
        first_name="Parent",
        last_name="Booker",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    child = User(
        user_id=930011,
        age=10,
        phone="9300100000",
        status="active",
        first_name="Child",
        last_name="Member",
        gender="male",
        city="Mumbai",
        parent_id=930010,
    )
    test_db_session.add_all([parent, child])
    await test_db_session.commit()
    await _seed_draft_engagement(
        test_db_session,
        engagement_id=930110,
        user_id=930011,
        booked_by_user_id=930010,
    )

    response = await async_client.get("/book/me/drafts", headers=_auth_header(930010))
    assert response.status_code == 200
    engagements = response.json()["data"]["engagements"]
    assert len(engagements) == 1
    assert engagements[0]["engagement_id"] == 930110


@pytest.mark.asyncio
async def test_get_my_drafts_resume_step_address_when_no_address(async_client, test_db_session):
    await _seed_healthians_diagnostic_package(test_db_session)
    u = User(
        user_id=930002,
        age=30,
        phone="9300020000",
        status="active",
        first_name="No",
        last_name="Addr",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()
    await _seed_draft_engagement(
        test_db_session,
        engagement_id=930102,
        user_id=930002,
        booked_by_user_id=930002,
        address=None,
    )

    response = await async_client.get("/book/me/drafts", headers=_auth_header(930002))
    assert response.status_code == 200
    engagements = response.json()["data"]["engagements"]
    assert engagements[0]["resume_step"] == "address"
    assert engagements[0]["address"] is None


@pytest.mark.asyncio
async def test_check_service_availability_creates_engagement_before_healthians(
    async_client, test_db_session,
):
    await _seed_healthians_diagnostic_package(test_db_session)
    u = User(
        user_id=930001,
        age=30,
        phone="9300010000",
        status="active",
        first_name="Svc",
        last_name="User",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()

    geocode_result = [{"latitude": 19.0760, "longitude": 72.8777, "state": "Maharashtra", "country": "India"}]
    healthians_resp = {"status": True, "data": {"zone_id": "440"}, "message": "Serviceable"}

    with (
        patch("modules.bookings.service.search_places", new_callable=AsyncMock, return_value=geocode_result),
        patch("modules.bookings.service.healthians_client.get_access_token", new_callable=AsyncMock, return_value="tok"),
        patch(
            "modules.bookings.service.healthians_client.check_serviceability_by_location_v2",
            new_callable=AsyncMock,
            return_value=healthians_resp,
        ) as mock_check,
    ):
        response = await async_client.post(
            "/book/check-service-availability",
            headers=_auth_header(930001),
            json=_CHECK_SERVICE_PAYLOAD,
        )

    assert response.status_code == 200
    member = response.json()["data"]["members"][0]
    assert member["status"] == "serviceable"
    assert member["engagement_id"] is not None
    assert member["zone_id"] == "440"
    mock_check.assert_awaited_once()

    eng_row = (
        await test_db_session.execute(
            text("SELECT status, healthians_zone_id, address, pincode FROM engagements WHERE engagement_id = :eid"),
            {"eid": member["engagement_id"]},
        )
    ).one()
    assert eng_row.status == "draft"
    assert eng_row.healthians_zone_id == "440"
    assert "Flat 12" in eng_row.address
    assert eng_row.pincode == "400001"

    part_row = (
        await test_db_session.execute(
            text(
                "SELECT booked_by_user_id, engagement_date, slot_start_time "
                "FROM engagement_participants WHERE engagement_id = :eid"
            ),
            {"eid": member["engagement_id"]},
        )
    ).one()
    assert part_row.booked_by_user_id == 930001
    assert part_row.engagement_date is None
    assert part_row.slot_start_time is None


@pytest.mark.asyncio
async def test_check_service_availability_cancels_on_not_serviceable(async_client, test_db_session):
    await _seed_healthians_diagnostic_package(test_db_session)
    u = User(
        user_id=930003,
        age=30,
        phone="9300030000",
        status="active",
        first_name="Not",
        last_name="Svc",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()

    geocode_result = [{"latitude": 19.0, "longitude": 72.0, "state": "Maharashtra", "country": "India"}]
    healthians_resp = {"status": False, "message": "Not serviceable"}

    payload = {
        "members": [
            {
                "user_id": 930003,
                "house_flat_no": "A",
                "building_area": "B",
                "city": "Mumbai",
                "pincode": "400002",
                "diagnostic_package_id": 1,
            }
        ]
    }

    with (
        patch("modules.bookings.service.search_places", new_callable=AsyncMock, return_value=geocode_result),
        patch("modules.bookings.service.healthians_client.get_access_token", new_callable=AsyncMock, return_value="tok"),
        patch(
            "modules.bookings.service.healthians_client.check_serviceability_by_location_v2",
            new_callable=AsyncMock,
            return_value=healthians_resp,
        ),
    ):
        response = await async_client.post(
            "/book/check-service-availability",
            headers=_auth_header(930003),
            json=payload,
        )

    assert response.status_code == 200
    member = response.json()["data"]["members"][0]
    assert member["status"] == "not_serviceable"
    assert member["engagement_id"] is not None

    status_row = (
        await test_db_session.execute(
            text("SELECT status FROM engagements WHERE engagement_id = :eid"),
            {"eid": member["engagement_id"]},
        )
    ).scalar_one()
    assert status_row == "cancelled"


@pytest.mark.asyncio
async def test_available_slots_returns_slim_slots(async_client, test_db_session):
    await _seed_healthians_diagnostic_package(test_db_session)
    u = User(
        user_id=930004,
        age=30,
        phone="9300040000",
        status="active",
        first_name="Slot",
        last_name="User",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()
    await _seed_draft_engagement(
        test_db_session,
        engagement_id=930104,
        user_id=930004,
        booked_by_user_id=930004,
    )

    healthians_slots = {
        "status": True,
        "data": [
            {
                "booked_count_slot_count": "0",
                "end_time": "07:00:00",
                "free_count_slot_count": "0",
                "is_peak_hours": "1",
                "sample_type_id": "1",
                "slot_date": "2026-07-15",
                "slot_time": "06:00:00",
                "stm_id": "45418464",
                "total_count_slot_count": "0",
                "state_id": "440",
                "state": "",
                "city_id": "1563",
                "city": "Mumbai West Zone 2 - Malad",
            }
        ],
    }

    with (
        patch("modules.bookings.service.healthians_client.get_access_token", new_callable=AsyncMock, return_value="tok"),
        patch(
            "modules.bookings.service.healthians_client.get_slots_by_location",
            new_callable=AsyncMock,
            return_value=healthians_slots,
        ),
    ):
        response = await async_client.post(
            "/book/available-slots",
            headers=_auth_header(930004),
            json={
                "members": [
                    {
                        "user_id": 930004,
                        "engagement_id": 930104,
                        "blood_collection_date": "2026-07-15",
                    }
                ]
            },
        )

    assert response.status_code == 200
    member = response.json()["data"]["members"][0]
    assert member["status"] == "success"
    assert member["slots"] == [
        {
            "end_time": "07:00:00",
            "slot_date": "2026-07-15",
            "slot_time": "06:00:00",
            "stm_id": "45418464",
        }
    ]


@pytest.mark.asyncio
async def test_lock_uses_booked_by_for_vendor_billing(async_client, test_db_session):
    await _seed_healthians_diagnostic_package(test_db_session)
    parent = User(
        user_id=930020,
        age=40,
        phone="9300200000",
        status="active",
        first_name="Lock",
        last_name="Parent",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    child = User(
        user_id=930021,
        age=10,
        phone="9300200000",
        status="active",
        first_name="Lock",
        last_name="Child",
        gender="male",
        city="Mumbai",
        parent_id=930020,
    )
    test_db_session.add_all([parent, child])
    await test_db_session.commit()
    await _seed_draft_engagement(
        test_db_session,
        engagement_id=930120,
        user_id=930021,
        booked_by_user_id=930020,
    )

    freeze_resp = {"status": True, "resCode": "RES0001", "message": "Slot locked", "data": {"slot_id": "34235263"}}

    with (
        patch("modules.bookings.service.healthians_client.get_access_token", new_callable=AsyncMock, return_value="tok"),
        patch(
            "modules.bookings.service.healthians_client.freeze_slot_v1",
            new_callable=AsyncMock,
            return_value=freeze_resp,
        ) as mock_freeze,
    ):
        response = await async_client.post(
            "/book/lock",
            headers=_auth_header(930020),
            json={
                "members": [
                    {
                        "user_id": 930021,
                        "engagement_id": 930120,
                        "blood_collection_date": "2026-07-15",
                        "blood_collection_time_slot_id": "34235263",
                        "blood_collection_time_slot": "06:00:00",
                    }
                ]
            },
        )

    assert response.status_code == 200
    assert response.json()["data"]["members"][0]["status"] == "success"
    mock_freeze.assert_awaited_once()
    assert mock_freeze.await_args.kwargs["vendor_billing_user_id"] == "930020"


@pytest.mark.asyncio
async def test_lock_updates_slot_start_time(async_client, test_db_session):
    await _seed_healthians_diagnostic_package(test_db_session)
    u = User(
        user_id=930005,
        age=30,
        phone="9300050000",
        status="active",
        first_name="Lock",
        last_name="Self",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()
    await _seed_draft_engagement(
        test_db_session,
        engagement_id=930105,
        user_id=930005,
        booked_by_user_id=930005,
    )

    freeze_resp = {"status": True, "resCode": "RES0001", "message": "Slot locked", "data": {"slot_id": "999"}}

    with (
        patch("modules.bookings.service.healthians_client.get_access_token", new_callable=AsyncMock, return_value="tok"),
        patch(
            "modules.bookings.service.healthians_client.freeze_slot_v1",
            new_callable=AsyncMock,
            return_value=freeze_resp,
        ),
    ):
        response = await async_client.post(
            "/book/lock",
            headers=_auth_header(930005),
            json={
                "members": [
                    {
                        "user_id": 930005,
                        "engagement_id": 930105,
                        "blood_collection_date": "2026-07-16",
                        "blood_collection_time_slot_id": "999",
                        "blood_collection_time_slot": "09:30:00",
                    }
                ]
            },
        )

    assert response.status_code == 200
    part_row = (
        await test_db_session.execute(
            text(
                "SELECT engagement_date, slot_start_time, blood_collection_time_slot_id "
                "FROM engagement_participants WHERE engagement_id = 930105"
            )
        )
    ).one()
    assert str(part_row.engagement_date) == "2026-07-16"
    assert str(part_row.slot_start_time) == "09:30:00"
    assert part_row.blood_collection_time_slot_id == "999"


@pytest.mark.asyncio
async def test_book_pay_creates_razorpay_order(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "RAZORPAY_KEY_SECRET", "test_razorpay_secret")
    await _seed_healthians_diagnostic_package(test_db_session)
    u = User(
        user_id=940010,
        age=30,
        phone="9400100000",
        status="active",
        first_name="Pay",
        last_name="User",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()
    await _seed_draft_engagement(
        test_db_session,
        engagement_id=940110,
        user_id=940010,
        booked_by_user_id=940010,
        locked=True,
    )

    with patch(
        "modules.payments.services._create_razorpay_order_sync",
        side_effect=_mock_razorpay_order,
    ):
        response = await async_client.post(
            "/book/pay",
            headers=_auth_header(940010),
            json={"members": [{"user_id": 940010, "engagement_id": 940110}]},
        )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["razorpay_order_id"]
    assert data["amount_paise"] == 50000
    assert data["key_id"]

    meta_row = (
        await test_db_session.execute(
            text("SELECT metadata FROM bookings WHERE booking_id = :bid"),
            {"bid": data["booking_ids"][0]},
        )
    ).scalar_one()
    assert meta_row["engagement_id"] == 940110


@pytest.mark.asyncio
async def test_book_pay_rejects_unlocked_draft(async_client, test_db_session):
    await _seed_healthians_diagnostic_package(test_db_session)
    u = User(
        user_id=940011,
        age=30,
        phone="9400110000",
        status="active",
        first_name="Un",
        last_name="Locked",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()
    await _seed_draft_engagement(
        test_db_session,
        engagement_id=940111,
        user_id=940011,
        booked_by_user_id=940011,
        locked=False,
    )

    response = await async_client.post(
        "/book/pay",
        headers=_auth_header(940011),
        json={"members": [{"user_id": 940011, "engagement_id": 940111}]},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_book_bio_ai_verifies_and_finalizes(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "RAZORPAY_KEY_SECRET", "test_razorpay_secret")
    monkeypatch.setattr(settings, "HEALTHIANS_CHECKSUM_KEY", "test-checksum")
    await _seed_healthians_diagnostic_package(test_db_session)
    u = User(
        user_id=940020,
        age=30,
        phone="9400200000",
        status="active",
        first_name="Bio",
        last_name="Final",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()
    await _seed_draft_engagement(
        test_db_session,
        engagement_id=940120,
        user_id=940020,
        booked_by_user_id=940020,
        locked=True,
    )

    with patch(
        "modules.payments.services._create_razorpay_order_sync",
        side_effect=_mock_razorpay_order,
    ):
        pay_resp = await async_client.post(
            "/book/pay",
            headers=_auth_header(940020),
            json={"members": [{"user_id": 940020, "engagement_id": 940120}]},
        )
    razorpay_order_id = pay_resp.json()["data"]["razorpay_order_id"]
    payment_id = "pay_test_940020"
    signature = _razorpay_signature(order_id=razorpay_order_id, payment_id=payment_id)

    healthians_resp = {"status": True, "booking_id": "HI940020", "message": "Booking placed"}

    with patch(
        "modules.bookings.service.healthians_client.get_access_token",
        new_callable=AsyncMock,
        return_value="tok",
    ), patch(
        "modules.bookings.service.healthians_client.create_booking_v3",
        new_callable=AsyncMock,
        return_value=healthians_resp,
    ):
        response = await async_client.post(
            "/book/bio-ai",
            headers=_auth_header(940020),
            json={
                "razorpay_order_id": razorpay_order_id,
                "razorpay_payment_id": payment_id,
                "razorpay_signature": signature,
            },
        )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["payment_verified"] is True
    assert data["members"][0]["status"] == "success"
    assert data["members"][0]["booking_id"] == "HI940020"

    eng_row = (
        await test_db_session.execute(
            text("SELECT status, engagement_type FROM engagements WHERE engagement_id = 940120")
        )
    ).one()
    assert eng_row.status == "scheduled"
    assert eng_row.engagement_type == "bio_ai"


@pytest.mark.asyncio
async def test_book_blood_test_sets_engagement_type_diagnostic(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "RAZORPAY_KEY_SECRET", "test_razorpay_secret")
    monkeypatch.setattr(settings, "HEALTHIANS_CHECKSUM_KEY", "test-checksum")
    await _seed_healthians_diagnostic_package(test_db_session)
    u = User(
        user_id=940021,
        age=30,
        phone="9400210000",
        status="active",
        first_name="Blood",
        last_name="Test",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()
    await _seed_draft_engagement(
        test_db_session,
        engagement_id=940121,
        user_id=940021,
        booked_by_user_id=940021,
        locked=True,
    )

    with patch(
        "modules.payments.services._create_razorpay_order_sync",
        side_effect=_mock_razorpay_order,
    ):
        pay_resp = await async_client.post(
            "/book/pay",
            headers=_auth_header(940021),
            json={"members": [{"user_id": 940021, "engagement_id": 940121}]},
        )
    razorpay_order_id = pay_resp.json()["data"]["razorpay_order_id"]
    payment_id = "pay_test_940021"
    signature = _razorpay_signature(order_id=razorpay_order_id, payment_id=payment_id)

    with patch(
        "modules.bookings.service.healthians_client.get_access_token",
        new_callable=AsyncMock,
        return_value="tok",
    ), patch(
        "modules.bookings.service.healthians_client.create_booking_v3",
        new_callable=AsyncMock,
        return_value={"status": True, "booking_id": "HI940021", "message": "OK"},
    ):
        response = await async_client.post(
            "/book/blood-test",
            headers=_auth_header(940021),
            json={
                "razorpay_order_id": razorpay_order_id,
                "razorpay_payment_id": payment_id,
                "razorpay_signature": signature,
            },
        )

    assert response.status_code == 200
    eng_type = (
        await test_db_session.execute(
            text("SELECT engagement_type FROM engagements WHERE engagement_id = 940121")
        )
    ).scalar_one()
    assert eng_type == "diagnostic"


@pytest.mark.asyncio
async def test_book_bio_ai_idempotent_when_already_paid(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "RAZORPAY_KEY_SECRET", "test_razorpay_secret")
    monkeypatch.setattr(settings, "HEALTHIANS_CHECKSUM_KEY", "test-checksum")
    await _seed_healthians_diagnostic_package(test_db_session)
    u = User(
        user_id=940022,
        age=30,
        phone="9400220000",
        status="active",
        first_name="Idem",
        last_name="Potent",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()
    await _seed_draft_engagement(
        test_db_session,
        engagement_id=940122,
        user_id=940022,
        booked_by_user_id=940022,
        locked=True,
    )

    with patch(
        "modules.payments.services._create_razorpay_order_sync",
        side_effect=_mock_razorpay_order,
    ):
        pay_resp = await async_client.post(
            "/book/pay",
            headers=_auth_header(940022),
            json={"members": [{"user_id": 940022, "engagement_id": 940122}]},
        )
    razorpay_order_id = pay_resp.json()["data"]["razorpay_order_id"]
    payment_id = "pay_test_940022"
    signature = _razorpay_signature(order_id=razorpay_order_id, payment_id=payment_id)
    verify_payload = {
        "razorpay_order_id": razorpay_order_id,
        "razorpay_payment_id": payment_id,
        "razorpay_signature": signature,
    }

    healthians_mock = AsyncMock(return_value={"status": True, "booking_id": "HI940022", "message": "OK"})

    with patch(
        "modules.bookings.service.healthians_client.get_access_token",
        new_callable=AsyncMock,
        return_value="tok",
    ), patch(
        "modules.bookings.service.healthians_client.create_booking_v3",
        healthians_mock,
    ):
        first = await async_client.post("/book/bio-ai", headers=_auth_header(940022), json=verify_payload)
        second = await async_client.post("/book/bio-ai", headers=_auth_header(940022), json=verify_payload)

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["data"]["members"][0]["status"] == "success"
    assert healthians_mock.await_count == 1
