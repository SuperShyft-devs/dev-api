"""E2E: home collection auto-book resolves zone/lat/long on participant before createBooking_v3."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import text

from core.config import settings
from modules.engagements.enums import EngagementPaymentType
from modules.engagements.models import BloodCollectionType, Engagement


@pytest.mark.asyncio
async def test_b2b_home_auto_book_resolves_zone_on_participant_before_create_booking(
    async_client, test_db_session, monkeypatch,
):
    monkeypatch.setattr(settings, "HEALTHIANS_CHECKSUM_KEY", "test-checksum")

    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package "
            "(diagnostic_package_id, reference_id, package_name, diagnostic_provider, status, external_package_code) "
            "VALUES (1, 'REF1', 'Diag', 'healthians', 1, '12992') "
            "ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO organizations (organization_id, name, status) "
            "VALUES (8201, 'Org Zone', 1)"
        )
    )
    test_db_session.add(
        Engagement(
            engagement_id=3201,
            engagement_name="Home Auto",
            engagement_code="ENGZONE01",
            organization_id=8201,
            engagement_type=1,
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            address="Corporate HQ Pune",
            pincode="411001",
            healthians_zone_id="470",
            latitude=20.91,
            longitude=74.76,
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="running",
            payment_type=EngagementPaymentType.paid_by_group,
            auto_booking=True,
            blood_collection_type=BloodCollectionType.home_collection,
            create_profile_on_metsights=False,
        )
    )
    await test_db_session.commit()

    geocode_mock = AsyncMock(
        return_value={
            "latitude": 28.5886109,
            "longitude": 77.39560039999999,
            "pincode": "201316",
            "state": "UP",
            "country": "IN",
        }
    )
    serviceability_mock = AsyncMock(
        return_value={"status": True, "data": {"zone_id": 50}, "message": "ok"},
    )
    freeze_mock = AsyncMock(
        return_value={"status": True, "resCode": "RES0001", "message": "ok"},
    )
    create_mock = AsyncMock(
        return_value={
            "status": True,
            "booking_id": "21079463874",
            "message": "Booking placed successfully.",
        },
    )

    with patch(
        "modules.bookings.service._geocode_for_booking",
        geocode_mock,
    ), patch(
        "modules.bookings.service.healthians_client.get_access_token",
        new_callable=AsyncMock,
        return_value="tok",
    ), patch(
        "modules.bookings.service.healthians_client.check_serviceability_by_location_v2",
        serviceability_mock,
    ), patch(
        "modules.bookings.service.healthians_client.freeze_slot_v1",
        freeze_mock,
    ), patch(
        "modules.bookings.service.healthians_client.create_booking_v3",
        create_mock,
    ), patch(
        "modules.users.service.UsersService._create_metsights_profile_for_engagement",
        new_callable=AsyncMock,
        return_value=None,
    ):
        response = await async_client.post(
            "/users/code/ENGZONE01/onboard",
            json={
                "age": 42,
                "first_name": "Utpal",
                "last_name": "Trip",
                "phone": "7447756123",
                "email": "utpal@example.com",
                "address": "B 1606 Prateek Wisteria, Sector 77, Noida",
                "pincode": "201316",
                "city": "Noida",
                "blood_collection_date": "2026-02-01",
                "blood_collection_time_slot": "11:00",
                "blood_collection_time_slot_id": "49207144",
            },
        )

    assert response.status_code == 200
    create_mock.assert_awaited_once()
    booking_payload = create_mock.await_args.args[1]
    assert booking_payload["zone_id"] == 50
    assert booking_payload["latitude"] == "28.5886109"
    assert booking_payload["longitude"] == "77.39560039999999"
    assert booking_payload["customer"][0]["contact_number"] == "7447756123"
    assert booking_payload["customer"][0]["email"] == "utpal@example.com"

    user_id = response.json()["data"]["user_id"]
    participant_row = (
        await test_db_session.execute(
            text(
                "SELECT healthians_zone_id, latitude, longitude, address "
                "FROM engagement_participants WHERE user_id = :uid AND engagement_id = 3201"
            ),
            {"uid": user_id},
        )
    ).first()
    assert str(participant_row.healthians_zone_id) == "50"
    assert participant_row.latitude is not None
    assert participant_row.longitude is not None

    engagement_row = (
        await test_db_session.execute(
            text(
                "SELECT healthians_zone_id, latitude, longitude, address "
                "FROM engagements WHERE engagement_id = 3201"
            )
        )
    ).first()
    assert str(engagement_row.healthians_zone_id) == "470"
    assert float(engagement_row.latitude) == 20.91
    assert engagement_row.address == "Corporate HQ Pune"
