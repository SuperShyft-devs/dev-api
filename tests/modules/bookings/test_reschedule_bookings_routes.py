"""Tests for POST /book/reschedule/blood-test."""

from __future__ import annotations

from datetime import date, time, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import text

from core.config import settings
from modules.diagnostics.models import DiagnosticPackage
from modules.engagements.models import BloodCollectionType, Engagement, EngagementParticipant
from modules.users.models import User
from tests.helpers.auth import user_auth_header


def _auth_header(user_id: int) -> dict[str, str]:
    return user_auth_header(user_id)


async def _seed_reschedule_fixture(
    test_db_session,
    *,
    engagement_id: int,
    participant_user_id: int,
    booked_by_user_id: int,
):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PKG1', 'Test Package', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    existing_diag = await test_db_session.get(DiagnosticPackage, 61)
    if existing_diag is None:
        test_db_session.add(
            DiagnosticPackage(
                diagnostic_package_id=61,
                reference_id="REF61",
                package_name="Healthians Home",
                diagnostic_provider="healthians",
                external_package_code=1002,
                status="active",
                bookings_count=0,
            )
        )
    existing_eng = await test_db_session.get(Engagement, engagement_id)
    if existing_eng is None:
        test_db_session.add(
            Engagement(
                engagement_id=engagement_id,
                engagement_name=f"Eng {engagement_id}",
                engagement_code=f"ENG{engagement_id}",
                engagement_type=None,
                assessment_package_id=1,
                diagnostic_package_id=61,
                status="scheduled",
                start_date=date.today(),
                end_date=date.today() + timedelta(days=7),
                blood_collection_type=BloodCollectionType.home_collection,
            )
        )
    else:
        existing_eng.blood_collection_type = BloodCollectionType.home_collection

    if booked_by_user_id != participant_user_id:
        existing_primary = await test_db_session.get(User, booked_by_user_id)
        if existing_primary is None:
            test_db_session.add(
                User(
                    user_id=booked_by_user_id,
                    age=40,
                    phone=f"{booked_by_user_id}000000",
                    status="active",
                    first_name="Primary",
                    last_name="User",
                    relationship="self",
                )
            )
    existing_participant_user = await test_db_session.get(User, participant_user_id)
    if existing_participant_user is None:
        test_db_session.add(
            User(
                user_id=participant_user_id,
                age=30,
                phone=f"{participant_user_id}000000",
                status="active",
                first_name="Part",
                last_name="icipant",
                relationship="child",
                parent_id=booked_by_user_id if booked_by_user_id != participant_user_id else None,
            )
        )
    await test_db_session.flush()
    slot_date = date.today() + timedelta(days=3)
    from modules.engagements.blood_booking_enums import BloodBookingRelation, BloodBookingStatus
    from modules.engagements.models import ParticipantBloodBooking

    participant = EngagementParticipant(
        engagement_id=engagement_id,
        user_id=participant_user_id,
        booked_by_user_id=booked_by_user_id,
    )
    test_db_session.add(participant)
    await test_db_session.flush()
    test_db_session.add(
        ParticipantBloodBooking(
            engagement_participant_id=participant.engagement_participant_id,
            relation=BloodBookingRelation.primary.value,
            status=BloodBookingStatus.active.value,
            collection_date=slot_date,
            collection_time=time(11, 30),
            booking_id=f"BK{engagement_id}",
            barcode=f"BK{engagement_id}",
            collection_time_slot_id="STM_BATCH",
        )
    )
    await test_db_session.commit()
    return slot_date


@pytest.mark.asyncio
async def test_reschedule_blood_test_success(async_client, test_db_session):
    slot_date = await _seed_reschedule_fixture(
        test_db_session,
        engagement_id=8101,
        participant_user_id=95001,
        booked_by_user_id=95001,
    )

    mock_reschedule = AsyncMock(
        return_value={
            "status": True,
            "message": "Booking Successfully Rescheduled.",
            "data": {"new_booking_id": "NEW8101", "refresh": ["list"]},
            "resCode": "RES0001",
            "code": 200,
        }
    )

    with patch(
        "modules.bookings.service.healthians_client.reschedule_booking_by_customer_v1",
        mock_reschedule,
    ):
        with patch(
            "modules.bookings.service.healthians_client.get_access_token",
            AsyncMock(return_value="token"),
        ):
            response = await async_client.post(
                "/book/reschedule/blood-test",
                headers=_auth_header(95001),
                json={
                    "members": [
                        {
                            "user_id": 95001,
                            "engagement_id": 8101,
                            "blood_collection_date": slot_date.isoformat(),
                            "blood_collection_time_slot_id": "STM_BATCH",
                            "blood_collection_time_slot": "11:30 AM",
                            "reschedule_reason": "Customer request",
                        }
                    ]
                },
            )

    assert response.status_code == 200
    member = response.json()["data"]["members"][0]
    assert member["status"] == "success"
    assert member["booking_id"] == "NEW8101"
    mock_reschedule.assert_awaited_once()
    assert mock_reschedule.await_args.kwargs["checksum_key"] == settings.HEALTHIANS_CHECKSUM_KEY


@pytest.mark.asyncio
async def test_reschedule_blood_test_forbidden_unrelated_user(async_client, test_db_session):
    await _seed_reschedule_fixture(
        test_db_session,
        engagement_id=8102,
        participant_user_id=95010,
        booked_by_user_id=95011,
    )
    test_db_session.add(User(user_id=95099, age=25, phone="9509900000", status="active"))
    await test_db_session.flush()
    await test_db_session.commit()

    response = await async_client.post(
        "/book/reschedule/blood-test",
        headers=_auth_header(95099),
        json={
            "members": [
                {
                    "user_id": 95010,
                    "engagement_id": 8102,
                    "blood_collection_date": date.today().isoformat(),
                    "blood_collection_time_slot_id": "STM_BATCH",
                    "blood_collection_time_slot": "11:30 AM",
                    "reschedule_reason": "Nope",
                }
            ]
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["members"][0]["status"] == "error"
