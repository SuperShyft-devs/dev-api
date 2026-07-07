"""Tests for console cancel booking (`DELETE .../console/participants/{user_id}/book`)."""

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
async def test_console_cancel_booking_success(async_client, test_db_session):
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
    existing_diag = await test_db_session.get(DiagnosticPackage, 50)
    if existing_diag is None:
        test_db_session.add(
            DiagnosticPackage(
                diagnostic_package_id=50,
                reference_id="REF50",
                package_name="Healthians Camp",
                diagnostic_provider="healthians",
                external_package_id=1001,
                status="active",
                bookings_count=0,
            )
        )
    else:
        existing_diag.diagnostic_provider = "healthians"
        existing_diag.external_package_id = 1001
    test_db_session.add(User(user_id=93001, age=30, phone="9300100000", status="active", first_name="Admin", last_name="User"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=501, user_id=93001, role="admin", status="active"))

    existing_eng = await test_db_session.get(Engagement, 7001)
    if existing_eng is None:
        test_db_session.add(
            Engagement(
                engagement_id=7001,
                engagement_name="Camp Eng",
                engagement_code="CAMP7001",
                engagement_type="diagnostic",
                assessment_package_id=1,
                diagnostic_package_id=50,
                external_camp_id=1001,
                status="running",
                start_date=date.today(),
                end_date=date.today(),
                latitude=28.6,
                longitude=77.2,
                pincode="110001",
                address="Camp Address",
            )
        )
    else:
        existing_eng.diagnostic_package_id = 50
        existing_eng.external_camp_id = 1001
        existing_eng.status = "running"
        existing_eng.latitude = 28.6
        existing_eng.longitude = 77.2
        existing_eng.pincode = "110001"
        existing_eng.address = "Camp Address"
    test_db_session.add(
        User(
            user_id=93002,
            age=35,
            phone="9300200000",
            status="active",
            first_name="Pat",
            last_name="Participant",
            gender="male",
            relationship="self",
        )
    )
    await test_db_session.flush()
    existing_participant = await test_db_session.get(EngagementParticipant, 95001)
    if existing_participant is None:
        test_db_session.add(
            EngagementParticipant(
                engagement_participant_id=95001,
                engagement_id=7001,
                user_id=93002,
                booked_by_user_id=93002,
                engagement_date=date.today(),
                slot_start_time=time(10, 0),
                booking_id="1715622590",
                barcode="BC95001",
            )
        )
    else:
        existing_participant.booking_id = "1715622590"
        existing_participant.barcode = "BC95001"
        existing_participant.booked_by_user_id = 93002
    await test_db_session.commit()

    mock_cancel = AsyncMock(
        return_value={"status": True, "message": "Order Cancelled Successfully!", "data": None, "code": 200}
    )

    with patch("modules.bookings.service.healthians_client.cancel_booking", mock_cancel):
        with patch("modules.bookings.service.healthians_client.get_access_token", AsyncMock(return_value="token")):
            response = await async_client.delete(
                "/engagements/7001/console/participants/93002/book",
                params={"remarks": "Testing cancel"},
                headers=_auth_header(93001),
            )

    assert response.status_code == 200
    assert response.json()["data"]["status"] is True
    mock_cancel.assert_awaited_once()
    call_kwargs = mock_cancel.await_args.kwargs
    assert call_kwargs["booking_id"] == "1715622590"
    assert call_kwargs["vendor_billing_user_id"] == "93002"
    assert call_kwargs["vendor_customer_id"] == "93002"
    assert call_kwargs["remarks"] == "Testing cancel"

    participant = await test_db_session.get(EngagementParticipant, 95001)
    assert participant.booking_id is None
    assert participant.barcode is None


@pytest.mark.asyncio
async def test_console_cancel_booking_requires_remarks(async_client, test_db_session):
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
    test_db_session.add(User(user_id=93011, age=30, phone="9301100000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=511, user_id=93011, role="admin", status="active"))
    test_db_session.add(
        Engagement(
            engagement_id=7002,
            engagement_name="Camp Eng 2",
            engagement_code="CAMP7002",
            engagement_type="diagnostic",
            assessment_package_id=1,
            diagnostic_package_id=1,
            status="running",
            start_date=date.today(),
            end_date=date.today(),
        )
    )
    await test_db_session.commit()

    response = await async_client.delete(
        "/engagements/7002/console/participants/1/book",
        params={"remarks": "  "},
        headers=_auth_header(93011),
    )
    assert response.status_code == 400
