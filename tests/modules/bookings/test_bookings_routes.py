"""Tests for batch booking endpoints under `/book`."""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import date, time, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select, text

from core.config import settings
from core.security import create_jwt_token
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    from tests.helpers.auth import user_auth_header
    return user_auth_header(user_id)


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


async def _seed_healthians_diagnostic_package(
    test_db_session,
    *,
    package_id: int = 1,
    complementary_consultation: dict | None = None,
) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package "
            "(diagnostic_package_id, reference_id, package_name, diagnostic_provider, status, price, external_package_code, complementary_consultation) "
            "VALUES (:id, 'REF-H', 'Healthians Package', 'healthians', 1, 500, '101', CAST(:cc AS json)) "
            "ON CONFLICT (diagnostic_package_id) DO UPDATE SET "
            "diagnostic_provider = EXCLUDED.diagnostic_provider, "
            "external_package_code = EXCLUDED.external_package_code, status = EXCLUDED.status, "
            "complementary_consultation = EXCLUDED.complementary_consultation"
        ),
        {
            "id": package_id,
            "cc": json.dumps(complementary_consultation) if complementary_consultation is not None else None,
        },
    )
    await test_db_session.commit()


async def _seed_notification_service(test_db_session, *, service_key: str) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, "
            "require_blood_report_url, require_bio_ai_report_url, require_participant_detail) "
            "VALUES (:sk, :dn, 'whatsapp', 'test-webhook', true, false, false, false) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true"
        ),
        {"sk": service_key, "dn": service_key},
    )
    await test_db_session.commit()


async def _seed_book_finalize_platform_defaults(
    test_db_session,
    *,
    assistant_employee_ids: str | None = None,
) -> None:
    """Platform notification defaults + optional default OA employee ids for /book finalize."""
    for key in (
        "booking-alert-whatsapp",
        "pretest-alert-whatsapp",
        "qr1-alert-whatsapp",
        "qr2-alert-whatsapp",
        "blood-report-alert-whatsapp",
        "bioai-report-alert-whatsapp",
        "consultation-alert-whatsapp",
    ):
        await _seed_notification_service(test_db_session, service_key=key)

    await test_db_session.execute(text("DELETE FROM platform_settings"))
    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings ("
            "settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id, "
            "default_onboarding_notification, default_pretest_guidelines_notification, "
            "default_questionnaire_reminder_1, default_questionnaire_reminder_2, "
            "default_blood_report_notification, default_bioai_report_notification, "
            "default_notify_users_for_consultation, default_onboarding_assistant_employee_ids"
            ") VALUES ("
            "1, 1, 1, "
            "'booking-alert-whatsapp', 'pretest-alert-whatsapp', "
            "'qr1-alert-whatsapp', 'qr2-alert-whatsapp', "
            "'blood-report-alert-whatsapp', 'bioai-report-alert-whatsapp', "
            "'consultation-alert-whatsapp', :oa_ids"
            ")"
        ),
        {"oa_ids": assistant_employee_ids},
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
    status: str = "draft",
    healthians_zone_id: str | None = "440",
    participant_location: bool = True,
    enforce_metsights_pro_gate: bool = False,
) -> None:
    from modules.engagements.blood_booking_enums import BloodBookingRelation, BloodBookingStatus
    from modules.engagements.models import BloodCollectionType, Engagement, EngagementParticipant, ParticipantBloodBooking

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
            healthians_zone_id=healthians_zone_id,
            slot_duration=20,
            status=status,
            blood_collection_type=BloodCollectionType.home_collection,
            enforce_metsights_pro_gate=enforce_metsights_pro_gate,
        )
    )
    participant = EngagementParticipant(
        engagement_id=engagement_id,
        user_id=user_id,
        booked_by_user_id=booked_by_user_id,
    )
    test_db_session.add(participant)
    await test_db_session.flush()
    engagement_row = (
        await test_db_session.execute(
            select(Engagement).where(Engagement.engagement_id == engagement_id)
        )
    ).scalar_one()
    if participant_location:
        from modules.engagements.participant_location import copy_engagement_location_to_participant

        copy_engagement_location_to_participant(participant, engagement_row)
        test_db_session.add(participant)
        await test_db_session.flush()
    if locked:
        test_db_session.add(
            ParticipantBloodBooking(
                engagement_participant_id=participant.engagement_participant_id,
                relation=BloodBookingRelation.primary.value,
                status=BloodBookingStatus.active.value,
                collection_date=date(2026, 7, 15),
                collection_time=time(6, 0),
                collection_time_slot_id="slot-123",
            )
        )
    await test_db_session.commit()


_CHECK_SERVICE_PAYLOAD = {
    "members": [
        {
            "user_id": 930001,
            "address_line": "Flat 12, Green Park",
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
        patch(
            "modules.bookings.service.search_places_for_booking",
            new_callable=AsyncMock,
            return_value=geocode_result,
        ) as mock_geocode,
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
    mock_geocode.assert_awaited_once()
    assert mock_geocode.await_args.kwargs["address_line"] == "Flat 12, Green Park"
    assert mock_geocode.await_args.kwargs["landmark"] == "Near Mall"
    assert mock_geocode.await_args.kwargs["city"] == "Mumbai"
    assert mock_geocode.await_args.kwargs["pincode"] == "400001"
    assert mock_geocode.await_args.kwargs["limit"] == 1
    assert mock_geocode.await_args.kwargs["primary"].value == "google"

    eng_row = (
        await test_db_session.execute(
            text("SELECT status, healthians_zone_id, address, sub_locality, pincode FROM engagements WHERE engagement_id = :eid"),
            {"eid": member["engagement_id"]},
        )
    ).one()
    assert eng_row.status in ("draft", 1)
    assert eng_row.healthians_zone_id == "440"
    assert eng_row.address == "Flat 12, Green Park"
    assert eng_row.sub_locality == "Flat 12, Green Park"
    assert eng_row.pincode == "400001"

    part_row = (
        await test_db_session.execute(
            text(
                "SELECT booked_by_user_id, address, pincode, latitude, longitude, healthians_zone_id "
                "FROM engagement_participants WHERE engagement_id = :eid"
            ),
            {"eid": member["engagement_id"]},
        )
    ).one()
    assert part_row.booked_by_user_id == 930001
    assert part_row.address == "Flat 12, Green Park"
    assert part_row.pincode == "400001"
    assert part_row.latitude is not None
    assert part_row.longitude is not None
    assert part_row.healthians_zone_id == "440"

    blood_count = (
        await test_db_session.execute(
            text(
                "SELECT COUNT(*) AS n FROM participant_blood_bookings pbb "
                "JOIN engagement_participants ep ON ep.engagement_participant_id = pbb.engagement_participant_id "
                "WHERE ep.engagement_id = :eid"
            ),
            {"eid": member["engagement_id"]},
        )
    ).scalar_one()
    assert blood_count == 0


@pytest.mark.asyncio
async def test_check_service_availability_token_failure_returns_502(async_client, test_db_session):
    await _seed_healthians_diagnostic_package(test_db_session)
    u = User(
        user_id=930099,
        age=30,
        phone="9300990000",
        status="active",
        first_name="Tok",
        last_name="Fail",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()

    payload = {
        "members": [
            {
                "user_id": 930099,
                "address_line": "Flat 12, Green Park",
                "landmark": "Near Mall",
                "city": "Mumbai",
                "pincode": "400001",
                "diagnostic_package_id": 1,
            }
        ]
    }

    with patch(
        "modules.bookings.service.healthians_client.get_access_token",
        new_callable=AsyncMock,
        side_effect=RuntimeError("Healthians blocked the request (403 Forbidden)"),
    ):
        response = await async_client.post(
            "/book/check-service-availability",
            headers=_auth_header(930099),
            json=payload,
        )

    assert response.status_code == 502
    body = response.json()
    assert body["error_code"] == "HEALTHIANS_IP_BLOCKED"


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
                "address_line": "A, B",
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
async def test_available_slots_allows_scheduled_home_collection(async_client, test_db_session):
    await _seed_healthians_diagnostic_package(test_db_session)
    u = User(
        user_id=930014,
        age=30,
        phone="9300140000",
        status="active",
        first_name="Reslot",
        last_name="User",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()
    await _seed_draft_engagement(
        test_db_session,
        engagement_id=930114,
        user_id=930014,
        booked_by_user_id=930014,
        locked=True,
        status="scheduled",
    )

    healthians_slots = {
        "status": True,
        "data": [
            {
                "end_time": "11:30:00",
                "slot_date": "2026-07-20",
                "slot_time": "11:00:00",
                "stm_id": "STM_RESCHED",
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
            headers=_auth_header(930014),
            json={
                "members": [
                    {
                        "user_id": 930014,
                        "engagement_id": 930114,
                        "blood_collection_date": "2026-07-20",
                    }
                ]
            },
        )

    assert response.status_code == 200
    member = response.json()["data"]["members"][0]
    assert member["status"] == "success"
    assert member["slots"][0]["stm_id"] == "STM_RESCHED"


@pytest.mark.asyncio
async def test_available_slots_resolves_missing_zone_id(async_client, test_db_session):
    await _seed_healthians_diagnostic_package(test_db_session)
    u = User(
        user_id=930017,
        age=30,
        phone="9300170000",
        status="active",
        first_name="Nozone",
        last_name="User",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()
    await _seed_draft_engagement(
        test_db_session,
        engagement_id=930117,
        user_id=930017,
        booked_by_user_id=930017,
        locked=True,
        status="scheduled",
        healthians_zone_id=None,
    )

    healthians_slots = {
        "status": True,
        "data": [
            {
                "end_time": "11:30:00",
                "slot_date": "2026-07-20",
                "slot_time": "11:00:00",
                "stm_id": "STM_ZONE",
            }
        ],
    }
    mock_slots = AsyncMock(return_value=healthians_slots)

    with (
        patch("modules.bookings.service.healthians_client.get_access_token", new_callable=AsyncMock, return_value="tok"),
        patch(
            "modules.bookings.service.healthians_client.check_serviceability_by_location_v2",
            new_callable=AsyncMock,
            return_value={"status": True, "message": "Serviceable", "data": {"zone_id": 999}},
        ) as mock_zone,
        patch(
            "modules.bookings.service.healthians_client.get_slots_by_location",
            mock_slots,
        ),
    ):
        response = await async_client.post(
            "/book/available-slots",
            headers=_auth_header(930017),
            json={
                "members": [
                    {
                        "user_id": 930017,
                        "engagement_id": 930117,
                        "blood_collection_date": "2026-07-20",
                    }
                ]
            },
        )

    assert response.status_code == 200
    member = response.json()["data"]["members"][0]
    assert member["status"] == "success"
    assert member["slots"][0]["stm_id"] == "STM_ZONE"
    mock_zone.assert_awaited_once()
    sent = mock_slots.await_args.args[1]
    assert sent["zone_id"] == "999"


@pytest.mark.asyncio
async def test_available_slots_seeds_legacy_b2c_participant_from_engagement_address(
    async_client, test_db_session,
):
    await _seed_healthians_diagnostic_package(test_db_session)
    u = User(
        user_id=930018,
        age=30,
        phone="9300180000",
        status="active",
        first_name="Legacy",
        last_name="Home",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()
    await _seed_draft_engagement(
        test_db_session,
        engagement_id=930118,
        user_id=930018,
        booked_by_user_id=930018,
        locked=True,
        status="scheduled",
        healthians_zone_id=None,
        participant_location=False,
    )

    geocode_result = [{"latitude": 19.0760, "longitude": 72.8777, "state": "Maharashtra", "country": "India"}]
    healthians_slots = {
        "status": True,
        "data": [{"end_time": "11:30:00", "slot_date": "2026-07-20", "slot_time": "11:00:00", "stm_id": "STM_HEAL"}],
    }

    with (
        patch(
            "modules.bookings.service.search_places_for_booking",
            new_callable=AsyncMock,
            return_value=geocode_result,
        ) as mock_geocode,
        patch("modules.bookings.service.healthians_client.get_access_token", new_callable=AsyncMock, return_value="tok"),
        patch(
            "modules.bookings.service.healthians_client.check_serviceability_by_location_v2",
            new_callable=AsyncMock,
            return_value={"status": True, "message": "Serviceable", "data": {"zone_id": 264}},
        ),
        patch(
            "modules.bookings.service.healthians_client.get_slots_by_location",
            new_callable=AsyncMock,
            return_value=healthians_slots,
        ),
    ):
        response = await async_client.post(
            "/book/available-slots",
            headers=_auth_header(930018),
            json={
                "members": [
                    {
                        "user_id": 930018,
                        "engagement_id": 930118,
                        "blood_collection_date": "2026-07-20",
                    }
                ]
            },
        )

    assert response.status_code == 200
    member = response.json()["data"]["members"][0]
    assert member["status"] == "success"
    mock_geocode.assert_awaited_once()
    part_row = (
        await test_db_session.execute(
            text(
                "SELECT address, pincode, latitude, longitude, healthians_zone_id "
                "FROM engagement_participants WHERE engagement_id = 930118"
            ),
        )
    ).one()
    assert part_row.address == "Flat 1, Block A, Near Park, Mumbai - 400001"
    assert part_row.pincode == "400001"
    assert part_row.latitude is not None
    assert part_row.healthians_zone_id == "264"


@pytest.mark.asyncio
async def test_available_slots_home_multi_participant_cannot_seed_from_engagement(
    async_client, test_db_session,
):
    await _seed_healthians_diagnostic_package(test_db_session)
    u1 = User(
        user_id=930019,
        age=30,
        phone="9300190000",
        status="active",
        first_name="Multi",
        last_name="One",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    u2 = User(
        user_id=930020,
        age=28,
        phone="9300200000",
        status="active",
        first_name="Multi",
        last_name="Two",
        gender="female",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add_all([u1, u2])
    await test_db_session.commit()
    await _seed_draft_engagement(
        test_db_session,
        engagement_id=930119,
        user_id=930019,
        booked_by_user_id=930019,
        status="scheduled",
        participant_location=False,
    )
    from modules.engagements.models import EngagementParticipant

    test_db_session.add(
        EngagementParticipant(
            engagement_id=930119,
            user_id=930020,
            booked_by_user_id=930019,
        )
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/book/available-slots",
        headers=_auth_header(930019),
        json={
            "members": [
                {
                    "user_id": 930019,
                    "engagement_id": 930119,
                    "blood_collection_date": "2026-07-20",
                }
            ]
        },
    )
    assert response.status_code == 200
    member = response.json()["data"]["members"][0]
    assert member["status"] == "error"
    assert "Re-check service availability" in member["message"]


@pytest.mark.asyncio
async def test_available_slots_rejects_completed_engagement(async_client, test_db_session):
    await _seed_healthians_diagnostic_package(test_db_session)
    u = User(
        user_id=930015,
        age=30,
        phone="9300150000",
        status="active",
        first_name="Done",
        last_name="User",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()
    await _seed_draft_engagement(
        test_db_session,
        engagement_id=930115,
        user_id=930015,
        booked_by_user_id=930015,
        status="completed",
    )

    response = await async_client.post(
        "/book/available-slots",
        headers=_auth_header(930015),
        json={
            "members": [
                {
                    "user_id": 930015,
                    "engagement_id": 930115,
                    "blood_collection_date": "2026-07-20",
                }
            ]
        },
    )

    assert response.status_code == 200
    member = response.json()["data"]["members"][0]
    assert member["status"] == "error"
    assert member["message"] == "Engagement is not in draft status"


@pytest.mark.asyncio
async def test_lock_allows_scheduled_home_collection(async_client, test_db_session):
    await _seed_healthians_diagnostic_package(test_db_session)
    u = User(
        user_id=930016,
        age=30,
        phone="9300160000",
        status="active",
        first_name="Relock",
        last_name="User",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()
    await _seed_draft_engagement(
        test_db_session,
        engagement_id=930116,
        user_id=930016,
        booked_by_user_id=930016,
        locked=True,
        status="scheduled",
    )

    freeze_resp = {"status": True, "resCode": "RES0001", "message": "Slot locked", "data": {"slot_id": "STM_NEW"}}

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
            headers=_auth_header(930016),
            json={
                "members": [
                    {
                        "user_id": 930016,
                        "engagement_id": 930116,
                        "blood_collection_date": "2026-07-20",
                        "blood_collection_time_slot_id": "STM_NEW",
                        "blood_collection_time_slot": "11:00 AM",
                    }
                ]
            },
        )

    assert response.status_code == 200
    member = response.json()["data"]["members"][0]
    assert member["status"] == "success"
    assert member["slot_id"] == "STM_NEW"


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
                "SELECT pbb.collection_date, pbb.collection_time, pbb.collection_time_slot_id "
                "FROM participant_blood_bookings pbb "
                "JOIN engagement_participants ep ON ep.engagement_participant_id = pbb.engagement_participant_id "
                "WHERE ep.engagement_id = 930105"
            )
        )
    ).one()
    assert str(part_row.collection_date) == "2026-07-16"
    assert str(part_row.collection_time) == "09:30:00"
    assert part_row.collection_time_slot_id == "999"


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
async def test_book_pay_rejects_metsights_pro_without_hormones(async_client, test_db_session):
    await _seed_healthians_diagnostic_package(test_db_session, package_id=1)
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (902, 'METSIGHTS_PRO', 'Metsights Pro', 1) "
            "ON CONFLICT (package_id) DO UPDATE SET package_code = EXCLUDED.package_code, status = EXCLUDED.status"
        )
    )
    by_type = {
        "bio_ai": {
            "assessment_package_id": 902,
            "diagnostic_package_id": 1,
            "blood_collection_type": "home_collection",
            "create_profile_on_metsights": True,
            "enroll_for_fitprint_full": False,
            "auto_booking": False,
            "slot_duration": 20,
            "payment_type": "paid_by_patient",
        }
    }
    await test_db_session.execute(text("DELETE FROM platform_settings"))
    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings ("
            "settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id, "
            "b2c_default_engagement_type, b2c_onboarding_by_engagement_type"
            ") VALUES (1, 902, 1, 'bio_ai', CAST(:by_type AS jsonb))"
        ),
        {"by_type": json.dumps(by_type)},
    )
    u = User(
        user_id=940012,
        age=30,
        phone="9400120000",
        status="active",
        first_name="Pro",
        last_name="Gate",
        gender="female",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()
    await _seed_draft_engagement(
        test_db_session,
        engagement_id=940112,
        user_id=940012,
        booked_by_user_id=940012,
        locked=True,
        enforce_metsights_pro_gate=True,
    )

    response = await async_client.post(
        "/book/pay",
        headers=_auth_header(940012),
        json={"members": [{"user_id": 940012, "engagement_id": 940112}]},
    )
    assert response.status_code == 400
    body = response.json()
    assert body["error_code"] == "INVALID_INPUT"
    assert "Metsights Pro" in body["message"] or "missing" in body["message"].lower()


@pytest.mark.asyncio
async def test_book_bio_ai_verifies_and_finalizes(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "RAZORPAY_KEY_SECRET", "test_razorpay_secret")
    monkeypatch.setattr(settings, "HEALTHIANS_CHECKSUM_KEY", "test-checksum")
    await _seed_healthians_diagnostic_package(test_db_session)
    await _seed_notification_service(test_db_session, service_key="booking-alert-whatsapp")
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
    freeze_resp = {"status": True, "resCode": "RES0001", "message": "ok", "data": {"slot_id": "slot-123"}}

    with patch(
        "modules.bookings.service.healthians_client.get_access_token",
        new_callable=AsyncMock,
        return_value="tok",
    ), patch(
        "modules.bookings.service.healthians_client.freeze_slot_v1",
        new_callable=AsyncMock,
        return_value=freeze_resp,
    ) as mock_freeze, patch(
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
    mock_freeze.assert_awaited_once()

    eng_row = (
        await test_db_session.execute(
            text(
                "SELECT status, engagement_type, start_date, end_date, engagement_name, "
                "onboarding_notification FROM engagements WHERE engagement_id = 940120"
            )
        )
    ).one()
    assert eng_row.status == "scheduled"
    assert eng_row.engagement_type == "bio_ai"
    assert str(eng_row.start_date) == "2026-07-15"
    assert str(eng_row.end_date) == "2026-07-18"
    assert eng_row.engagement_name == "Bio-2026-07-15"
    assert eng_row.onboarding_notification == "booking-alert-whatsapp"


@pytest.mark.asyncio
async def test_book_bio_ai_applies_defaults_assigns_assistants_and_notifies(
    async_client, test_db_session, monkeypatch
):
    monkeypatch.setattr(settings, "RAZORPAY_KEY_SECRET", "test_razorpay_secret")
    monkeypatch.setattr(settings, "HEALTHIANS_CHECKSUM_KEY", "test-checksum")
    await _seed_healthians_diagnostic_package(test_db_session)

    await test_db_session.execute(
        text("INSERT INTO users (user_id, age, phone, status) VALUES (940030, 30, '9400300000', 1)")
    )
    await test_db_session.execute(
        text(
            "INSERT INTO employee (employee_id, user_id, role, status) "
            "VALUES (940110, 940030, 'onboarding_assistant', 1)"
        )
    )
    await _seed_book_finalize_platform_defaults(test_db_session, assistant_employee_ids="940110")

    u = User(
        user_id=940023,
        age=30,
        phone="9400230000",
        status="active",
        first_name="Defaults",
        last_name="User",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()
    await _seed_draft_engagement(
        test_db_session,
        engagement_id=940123,
        user_id=940023,
        booked_by_user_id=940023,
        locked=True,
    )

    with patch(
        "modules.payments.services._create_razorpay_order_sync",
        side_effect=_mock_razorpay_order,
    ):
        pay_resp = await async_client.post(
            "/book/pay",
            headers=_auth_header(940023),
            json={"members": [{"user_id": 940023, "engagement_id": 940123}]},
        )
    razorpay_order_id = pay_resp.json()["data"]["razorpay_order_id"]
    payment_id = "pay_test_940023"
    signature = _razorpay_signature(order_id=razorpay_order_id, payment_id=payment_id)

    notify_mock = AsyncMock()
    with patch(
        "modules.bookings.service.healthians_client.get_access_token",
        new_callable=AsyncMock,
        return_value="tok",
    ), patch(
        "modules.bookings.service.healthians_client.freeze_slot_v1",
        new_callable=AsyncMock,
        return_value={"status": True, "resCode": "RES0001", "message": "ok", "data": {"slot_id": "slot-123"}},
    ), patch(
        "modules.bookings.service.healthians_client.create_booking_v3",
        new_callable=AsyncMock,
        return_value={"status": True, "booking_id": "HI940023", "message": "OK"},
    ), patch(
        "modules.notifications.onboarding_notify.notify_onboarding_assistants_on_enrollment",
        notify_mock,
    ):
        response = await async_client.post(
            "/book/bio-ai",
            headers=_auth_header(940023),
            json={
                "razorpay_order_id": razorpay_order_id,
                "razorpay_payment_id": payment_id,
                "razorpay_signature": signature,
            },
        )

    assert response.status_code == 200
    assert response.json()["data"]["members"][0]["status"] == "success"

    eng_row = (
        await test_db_session.execute(
            text(
                "SELECT start_date, end_date, onboarding_notification, pretest_guidelines_notification, "
                "questionnaire_reminder_1, questionnaire_reminder_2, blood_report_notification, "
                "bioai_report_notification, notify_users_for_consultation "
                "FROM engagements WHERE engagement_id = 940123"
            )
        )
    ).one()
    assert str(eng_row.start_date) == "2026-07-15"
    assert str(eng_row.end_date) == "2026-07-18"
    assert eng_row.onboarding_notification == "booking-alert-whatsapp"
    assert eng_row.pretest_guidelines_notification == "pretest-alert-whatsapp"
    assert eng_row.questionnaire_reminder_1 == "qr1-alert-whatsapp"
    assert eng_row.questionnaire_reminder_2 == "qr2-alert-whatsapp"
    assert eng_row.blood_report_notification == "blood-report-alert-whatsapp"
    assert eng_row.bioai_report_notification == "bioai-report-alert-whatsapp"
    assert eng_row.notify_users_for_consultation == "consultation-alert-whatsapp"

    oa_ids = (
        await test_db_session.execute(
            text(
                "SELECT employee_id FROM onboarding_assistant_assignment "
                "WHERE engagement_id = 940123 ORDER BY employee_id"
            )
        )
    ).scalars().all()
    assert list(oa_ids) == [940110]

    notify_mock.assert_awaited()
    notify_kwargs = notify_mock.await_args.kwargs
    assert notify_kwargs["participant_user_id"] == 940023
    assert notify_kwargs["participant_details"]["collection_date"] == "2026-07-15"
    assert notify_kwargs["participant_details"]["collection_time"] == "06:00:00"

    eng_clinical = (
        await test_db_session.execute(
            text(
                "SELECT assessment_package_id, create_profile_on_metsights, enroll_for_fitprint_full "
                "FROM engagements WHERE engagement_id = 940123"
            )
        )
    ).one()
    assert eng_clinical.assessment_package_id == 1
    assert eng_clinical.create_profile_on_metsights is True
    assert eng_clinical.enroll_for_fitprint_full is False

    instance_count = (
        await test_db_session.execute(
            text(
                "SELECT COUNT(*) FROM assessment_instances "
                "WHERE user_id = 940023 AND engagement_id = 940123"
            )
        )
    ).scalar_one()
    assert int(instance_count) >= 1


@pytest.mark.asyncio
async def test_book_bio_ai_finalize_attempts_metsights_and_survives_failure(
    async_client, test_db_session, monkeypatch
):
    monkeypatch.setattr(settings, "RAZORPAY_KEY_SECRET", "test_razorpay_secret")
    monkeypatch.setattr(settings, "HEALTHIANS_CHECKSUM_KEY", "test-checksum")
    await _seed_healthians_diagnostic_package(test_db_session)
    await _seed_book_finalize_platform_defaults(test_db_session)

    u = User(
        user_id=940024,
        age=30,
        phone="9400240000",
        status="active",
        first_name="Mets",
        last_name="Fail",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()
    await _seed_draft_engagement(
        test_db_session,
        engagement_id=940124,
        user_id=940024,
        booked_by_user_id=940024,
        locked=True,
    )

    with patch(
        "modules.payments.services._create_razorpay_order_sync",
        side_effect=_mock_razorpay_order,
    ):
        pay_resp = await async_client.post(
            "/book/pay",
            headers=_auth_header(940024),
            json={"members": [{"user_id": 940024, "engagement_id": 940124}]},
        )
    razorpay_order_id = pay_resp.json()["data"]["razorpay_order_id"]
    payment_id = "pay_test_940024"
    signature = _razorpay_signature(order_id=razorpay_order_id, payment_id=payment_id)

    metsights_mock = AsyncMock(side_effect=RuntimeError("metsights down"))
    with patch(
        "modules.bookings.service.healthians_client.get_access_token",
        new_callable=AsyncMock,
        return_value="tok",
    ), patch(
        "modules.bookings.service.healthians_client.freeze_slot_v1",
        new_callable=AsyncMock,
        return_value={"status": True, "resCode": "RES0001", "message": "ok", "data": {"slot_id": "slot-123"}},
    ), patch(
        "modules.bookings.service.healthians_client.create_booking_v3",
        new_callable=AsyncMock,
        return_value={"status": True, "booking_id": "HI940024", "message": "OK"},
    ), patch(
        "modules.metsights.service.MetsightsService.get_or_create_profile_id",
        metsights_mock,
    ):
        response = await async_client.post(
            "/book/bio-ai",
            headers=_auth_header(940024),
            json={
                "razorpay_order_id": razorpay_order_id,
                "razorpay_payment_id": payment_id,
                "razorpay_signature": signature,
            },
        )

    assert response.status_code == 200
    assert response.json()["data"]["members"][0]["status"] == "success"
    metsights_mock.assert_awaited()

    instance_count = (
        await test_db_session.execute(
            text(
                "SELECT COUNT(*) FROM assessment_instances "
                "WHERE user_id = 940024 AND engagement_id = 940124"
            )
        )
    ).scalar_one()
    assert int(instance_count) >= 1
    monkeypatch.setattr(settings, "RAZORPAY_KEY_SECRET", "test_razorpay_secret")
    monkeypatch.setattr(settings, "HEALTHIANS_CHECKSUM_KEY", "test-checksum")
    await _seed_healthians_diagnostic_package(test_db_session)
    await _seed_notification_service(test_db_session, service_key="booking-alert-whatsapp")
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
        "modules.bookings.service.healthians_client.freeze_slot_v1",
        new_callable=AsyncMock,
        return_value={"status": True, "resCode": "RES0001", "message": "ok", "data": {"slot_id": "slot-123"}},
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
    eng_row = (
        await test_db_session.execute(
            text(
                "SELECT engagement_type, start_date, end_date FROM engagements "
                "WHERE engagement_id = 940121"
            )
        )
    ).one()
    assert eng_row.engagement_type == "blood_test"
    assert str(eng_row.start_date) == "2026-07-15"
    assert str(eng_row.end_date) == "2026-07-18"


@pytest.mark.asyncio
async def test_book_bio_ai_idempotent_when_already_paid(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "RAZORPAY_KEY_SECRET", "test_razorpay_secret")
    monkeypatch.setattr(settings, "HEALTHIANS_CHECKSUM_KEY", "test-checksum")
    await _seed_healthians_diagnostic_package(test_db_session)
    await _seed_notification_service(test_db_session, service_key="booking-alert-whatsapp")
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
    freeze_mock = AsyncMock(
        return_value={"status": True, "resCode": "RES0001", "message": "ok", "data": {"slot_id": "slot-123"}},
    )

    with patch(
        "modules.bookings.service.healthians_client.get_access_token",
        new_callable=AsyncMock,
        return_value="tok",
    ), patch(
        "modules.bookings.service.healthians_client.freeze_slot_v1",
        freeze_mock,
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
    assert freeze_mock.await_count == 1


@pytest.mark.asyncio
async def test_book_bio_ai_applies_complementary_consultation(
    async_client, test_db_session, monkeypatch,
):
    monkeypatch.setattr(settings, "RAZORPAY_KEY_SECRET", "test_razorpay_secret")
    monkeypatch.setattr(settings, "HEALTHIANS_CHECKSUM_KEY", "test-checksum")
    cc = {"doctor": True, "nutritionist": False}
    await _seed_healthians_diagnostic_package(test_db_session, complementary_consultation=cc)
    await _seed_notification_service(test_db_session, service_key="booking-alert-whatsapp")
    u = User(
        user_id=940024,
        age=30,
        phone="9400240000",
        status="active",
        first_name="Comp",
        last_name="Consult",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()
    await _seed_draft_engagement(
        test_db_session,
        engagement_id=940124,
        user_id=940024,
        booked_by_user_id=940024,
        locked=True,
    )

    with patch(
        "modules.payments.services._create_razorpay_order_sync",
        side_effect=_mock_razorpay_order,
    ):
        pay_resp = await async_client.post(
            "/book/pay",
            headers=_auth_header(940024),
            json={"members": [{"user_id": 940024, "engagement_id": 940124}]},
        )
    razorpay_order_id = pay_resp.json()["data"]["razorpay_order_id"]
    payment_id = "pay_test_940024"
    signature = _razorpay_signature(order_id=razorpay_order_id, payment_id=payment_id)

    with patch(
        "modules.bookings.service.healthians_client.get_access_token",
        new_callable=AsyncMock,
        return_value="tok",
    ), patch(
        "modules.bookings.service.healthians_client.freeze_slot_v1",
        new_callable=AsyncMock,
        return_value={"status": True, "resCode": "RES0001", "message": "ok", "data": {"slot_id": "slot-123"}},
    ), patch(
        "modules.bookings.service.healthians_client.create_booking_v3",
        new_callable=AsyncMock,
        return_value={"status": True, "booking_id": "HI940024", "message": "OK"},
    ):
        response = await async_client.post(
            "/book/bio-ai",
            headers=_auth_header(940024),
            json={
                "razorpay_order_id": razorpay_order_id,
                "razorpay_payment_id": payment_id,
                "razorpay_signature": signature,
            },
        )

    assert response.status_code == 200
    eng_row = (
        await test_db_session.execute(
            text(
                "SELECT engagement_type, consultations FROM engagements "
                "WHERE engagement_id = 940124"
            )
        )
    ).one()
    assert eng_row.engagement_type == "bio_ai_with_consultation"
    assert eng_row.consultations == cc


@pytest.mark.asyncio
async def test_book_blood_test_applies_complementary_consultation(
    async_client, test_db_session, monkeypatch,
):
    monkeypatch.setattr(settings, "RAZORPAY_KEY_SECRET", "test_razorpay_secret")
    monkeypatch.setattr(settings, "HEALTHIANS_CHECKSUM_KEY", "test-checksum")
    cc = {"doctor": True}
    await _seed_healthians_diagnostic_package(test_db_session, complementary_consultation=cc)
    await _seed_notification_service(test_db_session, service_key="booking-alert-whatsapp")
    u = User(
        user_id=940025,
        age=30,
        phone="9400250000",
        status="active",
        first_name="Blood",
        last_name="Consult",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()
    await _seed_draft_engagement(
        test_db_session,
        engagement_id=940125,
        user_id=940025,
        booked_by_user_id=940025,
        locked=True,
    )

    with patch(
        "modules.payments.services._create_razorpay_order_sync",
        side_effect=_mock_razorpay_order,
    ):
        pay_resp = await async_client.post(
            "/book/pay",
            headers=_auth_header(940025),
            json={"members": [{"user_id": 940025, "engagement_id": 940125}]},
        )
    razorpay_order_id = pay_resp.json()["data"]["razorpay_order_id"]
    payment_id = "pay_test_940025"
    signature = _razorpay_signature(order_id=razorpay_order_id, payment_id=payment_id)

    with patch(
        "modules.bookings.service.healthians_client.get_access_token",
        new_callable=AsyncMock,
        return_value="tok",
    ), patch(
        "modules.bookings.service.healthians_client.freeze_slot_v1",
        new_callable=AsyncMock,
        return_value={"status": True, "resCode": "RES0001", "message": "ok", "data": {"slot_id": "slot-123"}},
    ), patch(
        "modules.bookings.service.healthians_client.create_booking_v3",
        new_callable=AsyncMock,
        return_value={"status": True, "booking_id": "HI940025", "message": "OK"},
    ):
        response = await async_client.post(
            "/book/blood-test",
            headers=_auth_header(940025),
            json={
                "razorpay_order_id": razorpay_order_id,
                "razorpay_payment_id": payment_id,
                "razorpay_signature": signature,
            },
        )

    assert response.status_code == 200
    eng_row = (
        await test_db_session.execute(
            text(
                "SELECT engagement_type, consultations FROM engagements "
                "WHERE engagement_id = 940125"
            )
        )
    ).one()
    assert eng_row.engagement_type == "blood_test_with_consultation"
    assert eng_row.consultations == cc
