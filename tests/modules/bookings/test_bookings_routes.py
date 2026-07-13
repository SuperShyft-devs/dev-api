"""Tests for batch booking endpoints under `/book`."""

from __future__ import annotations

from datetime import timedelta
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


@pytest.mark.asyncio
async def test_book_bio_ai_batch_requires_auth(async_client):
    response = await async_client.post(
        "/book/bio-ai",
        json={
            "members": [
                {
                    "user_id": 1,
                    "address": "A",
                    "pincode": "1",
                    "city": "C",
                    "blood_collection_date": "2026-06-01",
                    "blood_collection_time_slot": "10:00",
                    "diagnostic_package_id": 1,
                }
            ]
        },
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_book_bio_ai_batch_rejects_sub_profile_actor(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "")

    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status) "
            "VALUES (1, 'PK1', 'Package', '1', 'active') ON CONFLICT (package_id) DO UPDATE SET "
            "assessment_type_code = EXCLUDED.assessment_type_code, status = EXCLUDED.status"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status, price) "
            "VALUES (1, 'REF1', 'Diag Package', 'test_provider', 'active', 500) ON CONFLICT (diagnostic_package_id) DO UPDATE SET "
            "reference_id = EXCLUDED.reference_id, package_name = EXCLUDED.package_name, "
            "diagnostic_provider = EXCLUDED.diagnostic_provider, status = EXCLUDED.status, price = EXCLUDED.price"
        )
    )
    await test_db_session.commit()

    parent = User(
        user_id=920001,
        age=40,
        phone="9200010000",
        status="active",
        first_name="Par",
        last_name="Ent",
        gender="male",
        city="City",
        parent_id=None,
    )
    child = User(
        user_id=920002,
        age=10,
        phone="9200010000",
        status="active",
        first_name="Chi",
        last_name="Ld",
        gender="male",
        city="City",
        parent_id=920001,
    )
    test_db_session.add_all([parent, child])
    await test_db_session.commit()

    payload = {
        "members": [
            {
                "user_id": 920002,
                "address": "A1",
                "pincode": "111111",
                "city": "City",
                "blood_collection_date": "2026-06-01",
                "blood_collection_time_slot": "10:00",
                "diagnostic_package_id": 1,
            }
        ]
    }
    response = await async_client.post("/book/bio-ai", headers=_auth_header(920002), json=payload)
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_book_bio_ai_batch_returns_payment_info(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "")

    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status) "
            "VALUES (1, 'PK1', 'Package', '1', 'active') ON CONFLICT (package_id) DO UPDATE SET "
            "assessment_type_code = EXCLUDED.assessment_type_code, status = EXCLUDED.status"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status, price) VALUES "
            "(1, 'REF1', 'Diag 1', 'test_provider', 'active', 1000) ON CONFLICT (diagnostic_package_id) DO UPDATE SET "
            "reference_id = EXCLUDED.reference_id, package_name = EXCLUDED.package_name, "
            "diagnostic_provider = EXCLUDED.diagnostic_provider, status = EXCLUDED.status, price = EXCLUDED.price"
        )
    )
    await test_db_session.commit()

    parent = User(
        user_id=920010,
        age=40,
        phone="9200100000",
        status="active",
        first_name="Par",
        last_name="Ent",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    child = User(
        user_id=920011,
        age=12,
        phone="9200100000",
        status="active",
        first_name="Chi",
        last_name="Ld",
        gender="male",
        city="Mumbai",
        parent_id=920010,
    )
    test_db_session.add_all([parent, child])
    await test_db_session.commit()

    payload = {
        "members": [
            {
                "user_id": 920010,
                "address": "Addr P",
                "pincode": "400001",
                "city": "Mumbai",
                "blood_collection_date": "2026-06-10",
                "blood_collection_time_slot": "09:00",
                "diagnostic_package_id": 1,
            },
            {
                "user_id": 920011,
                "address": "Addr C",
                "pincode": "400002",
                "city": "Mumbai",
                "blood_collection_date": "2026-06-10",
                "blood_collection_time_slot": "09:30",
                "diagnostic_package_id": 1,
            },
        ]
    }

    with patch(
        "modules.payments.services._create_razorpay_order_sync",
        side_effect=_mock_razorpay_order,
    ):
        response = await async_client.post("/book/bio-ai", headers=_auth_header(920010), json=payload)

    assert response.status_code == 200
    data = response.json()["data"]
    assert "razorpay_order_id" in data
    assert len(data["booking_ids"]) == 2
    assert data["amount_paise"] == 200000
    assert data["currency"] == "INR"


@pytest.mark.asyncio
async def test_book_bio_ai_batch_forbidden_for_unrelated_user(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "")

    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status) "
            "VALUES (1, 'PK1', 'Package', '1', 'active') ON CONFLICT (package_id) DO UPDATE SET "
            "assessment_type_code = EXCLUDED.assessment_type_code, status = EXCLUDED.status"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status, price) "
            "VALUES (1, 'REF1', 'Diag Package', 'test_provider', 'active', 500) ON CONFLICT (diagnostic_package_id) DO UPDATE SET "
            "reference_id = EXCLUDED.reference_id, package_name = EXCLUDED.package_name, "
            "diagnostic_provider = EXCLUDED.diagnostic_provider, status = EXCLUDED.status, price = EXCLUDED.price"
        )
    )
    await test_db_session.commit()

    a = User(
        user_id=920020,
        age=30,
        phone="9200200000",
        status="active",
        first_name="A",
        last_name="A",
        gender="male",
        city="X",
        parent_id=None,
    )
    b = User(
        user_id=920021,
        age=30,
        phone="9200210000",
        status="active",
        first_name="B",
        last_name="B",
        gender="female",
        city="Y",
        parent_id=None,
    )
    test_db_session.add_all([a, b])
    await test_db_session.commit()

    payload = {
        "members": [
            {
                "user_id": 920021,
                "address": "Addr",
                "pincode": "111111",
                "city": "Y",
                "blood_collection_date": "2026-07-01",
                "blood_collection_time_slot": "10:00",
                "diagnostic_package_id": 1,
            }
        ]
    }
    response = await async_client.post("/book/bio-ai", headers=_auth_header(920020), json=payload)
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_book_blood_test_batch_returns_payment_info(async_client, test_db_session):
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status, price) "
            "VALUES (1, 'REF1', 'Diag Package', 'test_provider', 'active', 750) ON CONFLICT (diagnostic_package_id) DO UPDATE SET "
            "reference_id = EXCLUDED.reference_id, package_name = EXCLUDED.package_name, "
            "diagnostic_provider = EXCLUDED.diagnostic_provider, status = EXCLUDED.status, price = EXCLUDED.price"
        )
    )
    await test_db_session.commit()

    u = User(
        user_id=920030,
        age=25,
        phone="9200300000",
        status="active",
        first_name="S",
        last_name="olo",
        gender="male",
        city="Pune",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()

    payload = {
        "members": [
            {
                "user_id": 920030,
                "address": "Lab St",
                "pincode": "411001",
                "city": "Pune",
                "blood_collection_date": "2026-08-01",
                "blood_collection_time_slot": "08:00",
                "diagnostic_package_id": 1,
            }
        ]
    }

    with patch(
        "modules.payments.services._create_razorpay_order_sync",
        side_effect=_mock_razorpay_order,
    ):
        response = await async_client.post("/book/blood-test", headers=_auth_header(920030), json=payload)

    assert response.status_code == 200
    data = response.json()["data"]
    assert "razorpay_order_id" in data
    assert data["amount_paise"] == 75000
    assert len(data["booking_ids"]) == 1

    booking_row = (
        await test_db_session.execute(
            text("SELECT booking_type, metadata FROM bookings WHERE booking_id = :bid"),
            {"bid": data["booking_id"]},
        )
    ).first()
    assert booking_row.booking_type == "blood_test"
    assert booking_row.metadata is not None


@pytest.mark.asyncio
async def test_book_bio_ai_batch_rejects_duplicate_member_user_id(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "")

    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status, price) "
            "VALUES (1, 'REF1', 'Diag Package', 'test_provider', 'active', 500) ON CONFLICT (diagnostic_package_id) DO UPDATE SET "
            "reference_id = EXCLUDED.reference_id, package_name = EXCLUDED.package_name, "
            "diagnostic_provider = EXCLUDED.diagnostic_provider, status = EXCLUDED.status, price = EXCLUDED.price"
        )
    )
    await test_db_session.commit()

    u = User(
        user_id=920040,
        age=30,
        phone="9200400000",
        status="active",
        first_name="Dup",
        last_name="User",
        gender="male",
        city="City",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()

    member = {
        "user_id": 920040,
        "address": "Addr",
        "pincode": "111111",
        "city": "City",
        "blood_collection_date": "2026-06-01",
        "blood_collection_time_slot": "10:00",
        "diagnostic_package_id": 1,
    }
    payload = {"members": [member, member]}
    response = await async_client.post("/book/bio-ai", headers=_auth_header(920040), json=payload)
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_book_bio_ai_batch_rejects_more_than_ten_members(async_client, test_db_session):
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status, price) "
            "VALUES (1, 'REF1', 'Diag Package', 'test_provider', 'active', 500) ON CONFLICT (diagnostic_package_id) DO UPDATE SET "
            "reference_id = EXCLUDED.reference_id, package_name = EXCLUDED.package_name, "
            "diagnostic_provider = EXCLUDED.diagnostic_provider, status = EXCLUDED.status, price = EXCLUDED.price"
        )
    )
    await test_db_session.commit()

    u = User(
        user_id=920050,
        age=30,
        phone="9200500000",
        status="active",
        first_name="Big",
        last_name="Batch",
        gender="male",
        city="City",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()

    members = [
        {
            "user_id": 920050 + i,
            "address": f"Addr {i}",
            "pincode": "111111",
            "city": "City",
            "blood_collection_date": "2026-06-01",
            "blood_collection_time_slot": "10:00",
            "diagnostic_package_id": 1,
        }
        for i in range(11)
    ]
    response = await async_client.post("/book/bio-ai", headers=_auth_header(920050), json={"members": members})
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_book_bio_ai_batch_writes_audit_log(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "")

    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status, price) "
            "VALUES (1, 'REF1', 'Diag Package', 'test_provider', 'active', 500) ON CONFLICT (diagnostic_package_id) DO UPDATE SET "
            "reference_id = EXCLUDED.reference_id, package_name = EXCLUDED.package_name, "
            "diagnostic_provider = EXCLUDED.diagnostic_provider, status = EXCLUDED.status, price = EXCLUDED.price"
        )
    )
    await test_db_session.commit()

    u = User(
        user_id=920060,
        age=30,
        phone="9200600000",
        status="active",
        first_name="Aud",
        last_name="It",
        gender="male",
        city="City",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()

    payload = {
        "members": [
            {
                "user_id": 920060,
                "address": "Addr",
                "pincode": "111111",
                "city": "City",
                "blood_collection_date": "2026-06-01",
                "blood_collection_time_slot": "10:00",
                "diagnostic_package_id": 1,
            }
        ]
    }

    with patch(
        "modules.payments.services._create_razorpay_order_sync",
        side_effect=_mock_razorpay_order,
    ):
        response = await async_client.post("/book/bio-ai", headers=_auth_header(920060), json=payload)

    assert response.status_code == 200

    audit_count = (
        await test_db_session.execute(
            text(
                "SELECT COUNT(*) FROM data_audit_logs "
                "WHERE user_id = 920060 AND action = 'USER_BOOK_BIO_AI'"
            )
        )
    ).scalar_one()
    assert int(audit_count) >= 1


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
    test_db_session.add(
        EngagementParticipant(
            engagement_id=engagement_id,
            user_id=user_id,
            booked_by_user_id=booked_by_user_id,
            engagement_date=None,
            slot_start_time=None,
        )
    )
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
