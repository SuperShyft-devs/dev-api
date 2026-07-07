"""Tests for cancel booking endpoints under `/book/cancel`."""

from __future__ import annotations

from datetime import date, time, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token
from modules.diagnostics.models import DiagnosticPackage
from modules.engagements.models import Engagement, EngagementParticipant
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_cancel_fixture(test_db_session, *, engagement_id: int, participant_user_id: int, booked_by_user_id: int):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PKG1', 'Test Package', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    existing_diag = await test_db_session.get(DiagnosticPackage, 60)
    if existing_diag is None:
        test_db_session.add(
            DiagnosticPackage(
                diagnostic_package_id=60,
                reference_id="REF60",
                package_name="Healthians Home",
                diagnostic_provider="healthians",
                external_package_id=1001,
                status="active",
                bookings_count=0,
            )
        )
    else:
        existing_diag.diagnostic_provider = "healthians"
        existing_diag.external_package_id = 1001
    existing_eng = await test_db_session.get(Engagement, engagement_id)
    if existing_eng is None:
        test_db_session.add(
            Engagement(
                engagement_id=engagement_id,
                engagement_name=f"Eng {engagement_id}",
                engagement_code=f"ENG{engagement_id}",
                engagement_type="diagnostic",
                assessment_package_id=1,
                diagnostic_package_id=60,
                status="scheduled",
                start_date=date.today(),
                end_date=date.today(),
            )
        )
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
    test_db_session.add(
        EngagementParticipant(
            engagement_id=engagement_id,
            user_id=participant_user_id,
            booked_by_user_id=booked_by_user_id,
            engagement_date=date.today(),
            slot_start_time=time(10, 0),
            booking_id=f"BK{engagement_id}",
            barcode=f"BK{engagement_id}",
        )
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_cancel_bio_ai_booked_by_user_success(async_client, test_db_session):
    await _seed_cancel_fixture(
        test_db_session,
        engagement_id=8001,
        participant_user_id=94001,
        booked_by_user_id=94000,
    )

    mock_cancel = AsyncMock(
        return_value={"status": True, "message": "Order Cancelled Successfully!", "data": None, "code": 200}
    )

    with patch("modules.bookings.service.healthians_client.cancel_booking", mock_cancel):
        with patch("modules.bookings.service.healthians_client.get_access_token", AsyncMock(return_value="token")):
            response = await async_client.post(
                "/book/cancel/bio-ai",
                headers=_auth_header(94000),
                json={
                    "members": [
                        {"user_id": 94001, "engagement_id": 8001, "remarks": "Family cancel test"}
                    ]
                },
            )

    assert response.status_code == 200
    member = response.json()["data"]["members"][0]
    assert member["status"] == "success"
    mock_cancel.assert_awaited_once()
    assert mock_cancel.await_args.kwargs["vendor_billing_user_id"] == "94000"
    assert mock_cancel.await_args.kwargs["vendor_customer_id"] == "94001"
    assert mock_cancel.await_args.kwargs["booking_id"] == "BK8001"


@pytest.mark.asyncio
async def test_cancel_blood_test_participant_user_success(async_client, test_db_session):
    await _seed_cancel_fixture(
        test_db_session,
        engagement_id=8002,
        participant_user_id=94010,
        booked_by_user_id=94010,
    )

    mock_cancel = AsyncMock(
        return_value={"status": True, "message": "Order Cancelled Successfully!", "data": None, "code": 200}
    )

    with patch("modules.bookings.service.healthians_client.cancel_booking", mock_cancel):
        with patch("modules.bookings.service.healthians_client.get_access_token", AsyncMock(return_value="token")):
            response = await async_client.post(
                "/book/cancel/blood-test",
                headers=_auth_header(94010),
                json={
                    "members": [
                        {"user_id": 94010, "engagement_id": 8002, "remarks": "Self cancel"}
                    ]
                },
            )

    assert response.status_code == 200
    assert response.json()["data"]["members"][0]["status"] == "success"


@pytest.mark.asyncio
async def test_cancel_booking_forbidden_unrelated_user(async_client, test_db_session):
    await _seed_cancel_fixture(
        test_db_session,
        engagement_id=8003,
        participant_user_id=94020,
        booked_by_user_id=94021,
    )
    test_db_session.add(User(user_id=94099, age=25, phone="9409900000", status="active"))
    await test_db_session.flush()
    await test_db_session.commit()

    response = await async_client.post(
        "/book/cancel/bio-ai",
        headers=_auth_header(94099),
        json={
            "members": [
                {"user_id": 94020, "engagement_id": 8003, "remarks": "Should fail"}
            ]
        },
    )
    assert response.status_code == 200
    assert response.json()["data"]["members"][0]["status"] == "error"
    assert "authorized" in response.json()["data"]["members"][0]["message"].lower()
