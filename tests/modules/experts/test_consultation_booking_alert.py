"""Tests for consultation booking alert trigger on POST /experts/consultations/book."""

from __future__ import annotations

from datetime import date, time, timedelta
from unittest.mock import AsyncMock

import pytest

from modules.diagnostics.models import DiagnosticPackage
from modules.engagements.enums import ConsultationMode
from modules.engagements.models import Engagement, EngagementParticipant
from modules.experts.repository import (
    ExpertAvailabilityOverrideRepository,
    ExpertAvailabilityRepository,
    ExpertsRepository,
)
from modules.experts.schemas import ConsultationBookRequest
from modules.experts.service import ExpertAvailabilityService
from modules.users.models import User


@pytest.mark.asyncio
async def test_book_consultation_slot_triggers_booking_alert(test_db_session, monkeypatch):
    existing_diag = await test_db_session.get(DiagnosticPackage, 1)
    if existing_diag is None:
        test_db_session.add(
            DiagnosticPackage(
                diagnostic_package_id=1,
                reference_id="REF1",
                package_name="Diag Package",
                diagnostic_provider="test_provider",
                status="active",
                bookings_count=0,
            )
        )

    participant_user_id = 78601
    engagement_id = 78601
    participant_id = 78601

    test_db_session.add(
        User(
            user_id=participant_user_id,
            age=30,
            phone="786010000000",
            first_name="Book",
            last_name="User",
            email="book@example.com",
            status="active",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Book Alert",
            engagement_code="BK78601",
            engagement_type="consultation",
            consultations={"doctor": True},
            consultation_mode=ConsultationMode.online,
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date.today(),
            end_date=date.today() + timedelta(days=7),
            status="running",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=participant_id,
            engagement_id=engagement_id,
            user_id=participant_user_id,
            engagement_date=date.today(),
            slot_start_time=time(9, 0),
        )
    )
    await test_db_session.commit()

    notify_mock = AsyncMock()
    monkeypatch.setattr(
        "modules.notifications.consultation_booking_alert_notify.notify_onboarding_assistants_on_consultation_booking",
        notify_mock,
    )

    service = ExpertAvailabilityService(
        experts_repository=ExpertsRepository(),
        availability_repository=ExpertAvailabilityRepository(),
        override_repository=ExpertAvailabilityOverrideRepository(),
    )
    monkeypatch.setattr(service, "_slot_is_available", AsyncMock(return_value=True))

    book_date = date.today() + timedelta(days=1)
    payload = ConsultationBookRequest(
        engagement_id=engagement_id,
        expert_type="doctor",
        date=book_date,
        slot="10:00",
    )

    result = await service.book_consultation_slot(
        test_db_session,
        user_id=participant_user_id,
        payload=payload,
    )

    assert "received your slot" in result["message"].lower()
    notify_mock.assert_awaited_once()
    kwargs = notify_mock.await_args.kwargs
    assert kwargs["participant_user_id"] == participant_user_id
    assert kwargs["expert_type"] == "doctor"
    assert kwargs["consultation_date"] == book_date
    assert kwargs["consultation_slot"] == "10:00"
