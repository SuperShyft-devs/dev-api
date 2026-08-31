"""Tests for console book participant (`POST .../console/participants/{user_id}/book`)."""

from __future__ import annotations

from datetime import date, time, timedelta
from unittest.mock import AsyncMock, patch

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import AssessmentPackage
from modules.diagnostics.models import DiagnosticPackage
from modules.employee.models import Employee
from modules.engagements.models import Engagement, EngagementParticipant
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_console_book_participant_uses_engagement_external_camp_id(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "HEALTHIANS_CHECKSUM_KEY", "test-checksum-key")

    existing_pkg = await test_db_session.get(AssessmentPackage, 1)
    if existing_pkg is None:
        test_db_session.add(
            AssessmentPackage(
                package_id=1,
                package_code="PKG001",
                display_name="Test Package",
                status="active",
            )
        )
    existing_diag = await test_db_session.get(DiagnosticPackage, 51)
    if existing_diag is None:
        test_db_session.add(
            DiagnosticPackage(
                diagnostic_package_id=51,
                reference_id="REF51",
                package_name="Healthians Camp",
                diagnostic_provider="healthians",
                external_package_id=2002,
                status="active",
                bookings_count=0,
            )
        )
    else:
        existing_diag.diagnostic_provider = "healthians"
        existing_diag.external_package_id = 2002

    test_db_session.add(
        User(user_id=93101, age=30, phone="9310100000", status="active", first_name="Admin", last_name="User")
    )
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=601, user_id=93101, role="admin", status="active"))

    existing_eng = await test_db_session.get(Engagement, 7101)
    if existing_eng is None:
        test_db_session.add(
            Engagement(
                engagement_id=7101,
                engagement_name="Camp Eng Book",
                engagement_code="CAMP7101",
                engagement_type=None,
                assessment_package_id=1,
                diagnostic_package_id=51,
                external_camp_id=3003,
                status="running",
                start_date=date.today(),
                end_date=date.today(),
                city="Delhi",
                latitude=28.6,
                longitude=77.2,
                pincode="110001",
                address="Camp Address",
            )
        )
    else:
        existing_eng.diagnostic_package_id = 51
        existing_eng.external_camp_id = 3003
        existing_eng.status = "running"
        existing_eng.latitude = 28.6
        existing_eng.longitude = 77.2
        existing_eng.pincode = "110001"
        existing_eng.address = "Camp Address"

    test_db_session.add(
        User(
            user_id=93102,
            age=35,
            phone="9310200000",
            email="pat.participant@example.com",
            status="active",
            first_name="Pat",
            last_name="Participant",
            gender="male",
            relationship="self",
        )
    )
    await test_db_session.flush()

    existing_participant = await test_db_session.get(EngagementParticipant, 96001)
    if existing_participant is None:
        test_db_session.add(
            EngagementParticipant(
                engagement_participant_id=96001,
                engagement_id=7101,
                user_id=93102,
                booked_by_user_id=93102,
                engagement_date=date.today(),
                slot_start_time=time(10, 0),
            )
        )
    else:
        existing_participant.booking_id = None
        existing_participant.barcode = None
        existing_participant.booked_by_user_id = 93102
    await test_db_session.commit()

    mock_serviceability = AsyncMock(
        return_value={"status": True, "data": {"zone_id": 42}, "message": "ok"}
    )
    mock_create_booking = AsyncMock(
        return_value={
            "status": True,
            "message": "Booking created",
            "booking_id": "1715622999",
            "lead_id": 99,
        }
    )

    with patch(
        "modules.engagements.console.service.healthians_client.check_serviceability_by_location_v2",
        mock_serviceability,
    ):
        with patch(
            "modules.engagements.console.service.healthians_client.create_booking_v3",
            mock_create_booking,
        ):
            with patch(
                "modules.engagements.console.service.healthians_client.get_access_token",
                AsyncMock(return_value="token"),
            ):
                response = await async_client.post(
                    "/engagements/7101/console/participants/93102/book",
                    json={"barcode": "BC96001"},
                    headers=_auth_header(93101),
                )

    assert response.status_code == 200
    assert response.json()["data"]["booking_id"] == "1715622999"
    mock_create_booking.assert_awaited_once()
    booking_payload = mock_create_booking.await_args.args[1]
    assert booking_payload["camp_id"] == 3003
    assert booking_payload["package"] == [{"deal_id": ["package_2002"]}]
    customer = booking_payload["customer"][0]
    assert customer["age"] == 35
    assert customer["email"] == "pat.participant@example.com"
    assert booking_payload["email"] == "pat.participant@example.com"

    participant = await test_db_session.get(EngagementParticipant, 96001)
    assert participant.booking_id == "1715622999"
    assert participant.barcode == "BC96001"


@pytest.mark.asyncio
async def test_console_home_collection_book_flow(async_client, test_db_session, monkeypatch):
    """Regression: check -> slots -> lock -> book must survive release_request_transaction."""
    monkeypatch.setattr(settings, "HEALTHIANS_CHECKSUM_KEY", "test-checksum-key")

    existing_pkg = await test_db_session.get(AssessmentPackage, 1)
    if existing_pkg is None:
        test_db_session.add(
            AssessmentPackage(
                package_id=1,
                package_code="PKG001",
                display_name="Test Package",
                status="active",
            )
        )

    existing_diag = await test_db_session.get(DiagnosticPackage, 52)
    if existing_diag is None:
        test_db_session.add(
            DiagnosticPackage(
                diagnostic_package_id=52,
                reference_id="REF52",
                package_name="Healthians Home",
                diagnostic_provider="healthians",
                external_package_id=2003,
                original_price=999,
                status="active",
                bookings_count=0,
            )
        )
    else:
        existing_diag.diagnostic_provider = "healthians"
        existing_diag.external_package_id = 2003
        existing_diag.original_price = 999

    test_db_session.add(
        User(user_id=93111, age=30, phone="9311100000", status="active", first_name="Admin", last_name="User")
    )
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=611, user_id=93111, role="admin", status="active"))

    existing_eng = await test_db_session.get(Engagement, 7102)
    if existing_eng is None:
        test_db_session.add(
            Engagement(
                engagement_id=7102,
                engagement_name="Home Collection Eng",
                engagement_code="HOME7102",
                engagement_type=None,
                assessment_package_id=1,
                diagnostic_package_id=52,
                status="running",
                start_date=date.today(),
                end_date=date.today(),
                city="Bangalore",
            )
        )
    else:
        existing_eng.diagnostic_package_id = 52
        existing_eng.status = "running"

    test_db_session.add(
        User(
            user_id=93112,
            age=28,
            phone="9311200000",
            email="home.participant@example.com",
            status="active",
            first_name="Home",
            last_name="Participant",
            gender="female",
            relationship="self",
        )
    )
    await test_db_session.flush()

    existing_participant = await test_db_session.get(EngagementParticipant, 96002)
    if existing_participant is None:
        test_db_session.add(
            EngagementParticipant(
                engagement_participant_id=96002,
                engagement_id=7102,
                user_id=93112,
                booked_by_user_id=93112,
                engagement_date=date.today(),
                slot_start_time=time(10, 0),
            )
        )
    else:
        existing_participant.booking_id = None
        existing_participant.barcode = None
        existing_participant.blood_collection_time_slot_id = None
        existing_participant.healthians_zone_id = None
        existing_participant.latitude = None
        existing_participant.longitude = None
        existing_participant.address = None
        existing_participant.pincode = None
        existing_participant.booked_by_user_id = 93112
    await test_db_session.commit()

    auth = _auth_header(93111)
    base = "/engagements/7102/console/participants/93112/book-home-collection"
    slot_date = date.today().isoformat()

    mock_geocode = AsyncMock(
        return_value=[
            {
                "latitude": 12.97,
                "longitude": 77.59,
                "state": "Karnataka",
                "country": "India",
            }
        ]
    )
    mock_serviceability = AsyncMock(
        return_value={"status": True, "data": {"zone_id": 77}, "message": "ok"}
    )
    mock_slots = AsyncMock(
        return_value={
            "status": True,
            "data": [
                {
                    "end_time": "12:00:00",
                    "slot_date": slot_date,
                    "slot_time": "11:30:00",
                    "stm_id": "STM96002",
                }
            ],
        }
    )
    mock_freeze = AsyncMock(
        return_value={
            "status": True,
            "resCode": "RES0001",
            "message": "Slot locked",
            "data": {"slot_id": "STM96002", "freeze_time": "2026-08-31T11:00:00"},
        }
    )
    mock_create_booking = AsyncMock(
        return_value={
            "status": True,
            "message": "Booking created",
            "booking_id": "1715623000",
            "lead_id": 100,
        }
    )

    with patch("modules.engagements.console.service.search_places", mock_geocode):
        with patch(
            "modules.engagements.console.service.healthians_client.check_serviceability_by_location_v2",
            mock_serviceability,
        ):
            with patch(
                "modules.engagements.console.service.healthians_client.get_access_token",
                AsyncMock(return_value="token"),
            ):
                check_resp = await async_client.post(
                    f"{base}/check-service-availability",
                    json={
                        "address_line": "123 MG Road",
                        "city": "Bangalore",
                        "pincode": "560001",
                    },
                    headers=auth,
                )

    assert check_resp.status_code == 200
    assert check_resp.json()["data"]["zone_id"] == 77

    with patch(
        "modules.engagements.console.service.healthians_client.get_slots_by_location",
        mock_slots,
    ):
        with patch(
            "modules.engagements.console.service.healthians_client.get_access_token",
            AsyncMock(return_value="token"),
        ):
            slots_resp = await async_client.post(
                f"{base}/available-slots",
                json={"blood_collection_date": slot_date},
                headers=auth,
            )

    assert slots_resp.status_code == 200
    assert slots_resp.json()["data"]["slots"][0]["stm_id"] == "STM96002"

    with patch(
        "modules.engagements.console.service.healthians_client.freeze_slot_v1",
        mock_freeze,
    ):
        with patch(
            "modules.engagements.console.service.healthians_client.get_access_token",
            AsyncMock(return_value="token"),
        ):
            lock_resp = await async_client.post(
                f"{base}/lock",
                json={
                    "blood_collection_date": slot_date,
                    "blood_collection_time_slot_id": "STM96002",
                    "blood_collection_time_slot": "11:30:00",
                },
                headers=auth,
            )

    assert lock_resp.status_code == 200

    with patch(
        "modules.engagements.console.service.healthians_client.create_booking_v3",
        mock_create_booking,
    ):
        with patch(
            "modules.engagements.console.service.healthians_client.get_access_token",
            AsyncMock(return_value="token"),
        ):
            book_resp = await async_client.post(f"{base}/book", headers=auth)

    assert book_resp.status_code == 200
    assert book_resp.json()["data"]["booking_id"] == "1715623000"
    mock_create_booking.assert_awaited_once()
    booking_payload = mock_create_booking.await_args.args[1]
    assert booking_payload["customer"][0]["age"] == 28
    assert booking_payload["customer"][0]["email"] == "home.participant@example.com"
    assert booking_payload["slot"] == {"slot_id": "STM96002"}

    participant = await test_db_session.get(EngagementParticipant, 96002)
    assert participant.booking_id == "1715623000"
    assert participant.healthians_zone_id == "77"
    assert participant.blood_collection_time_slot_id == "STM96002"
