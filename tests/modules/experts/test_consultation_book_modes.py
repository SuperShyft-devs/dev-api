"""Tests for consultation booking modes (online vs offline)."""

from __future__ import annotations

from datetime import date, time, timedelta
from unittest.mock import AsyncMock

import pytest

from core.exceptions import AppError
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


def _consultation_slot_detail(*, expert_type: str = "nutritionist", cabin_key: str = "consultation_cabin_1") -> dict:
    return {
        "consultation": {
            "2026-08-20": [
                {
                    "cabin_name": "Consultation Cabin 1",
                    "cabin_key": cabin_key,
                    "start_time": "09:00",
                    "end_time": "17:00",
                    "expert_type": expert_type,
                    "slot_duration": 30,
                    "capacity_per_slot": 1,
                    "breaks": [],
                    "is_active": True,
                }
            ]
        }
    }


async def _seed_booking_fixture(
    test_db_session,
    *,
    engagement_id: int,
    participant_user_id: int,
    participant_id: int,
    consultation_mode: ConsultationMode | None,
    organization_id: int | None,
    slot_detail: dict | None = None,
    expert_type: str = "nutritionist",
):
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

    test_db_session.add(
        User(
            user_id=participant_user_id,
            age=30,
            phone=f"{participant_user_id}000000000",
            first_name="Book",
            last_name="User",
            email=f"book{participant_user_id}@example.com",
            status="active",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Mode Book Test",
            engagement_code=f"BK{engagement_id}",
            organization_id=organization_id,
            engagement_type=1,
            consultations={expert_type: True},
            consultation_mode=consultation_mode,
            slot_detail=slot_detail,
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=30,
            start_date=date(2026, 8, 20),
            end_date=date(2026, 8, 21),
            status="running",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=participant_id,
            engagement_id=engagement_id,
            user_id=participant_user_id,
            booked_by_user_id=participant_user_id,
            engagement_date=date(2026, 8, 20),
            slot_start_time=time(9, 0),
        )
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_book_offline_requires_cabin_and_uses_slot_detail(test_db_session, monkeypatch):
    engagement_id = 78610
    user_id = 78610
    await _seed_booking_fixture(
        test_db_session,
        engagement_id=engagement_id,
        participant_user_id=user_id,
        participant_id=78610,
        consultation_mode=ConsultationMode.offline,
        organization_id=100,
        slot_detail=_consultation_slot_detail(),
    )

    service = ExpertAvailabilityService(
        experts_repository=ExpertsRepository(),
        availability_repository=ExpertAvailabilityRepository(),
        override_repository=ExpertAvailabilityOverrideRepository(),
    )
    monkeypatch.setattr(service, "_slot_is_available", AsyncMock(return_value=False))

    with pytest.raises(AppError) as exc:
        await service.book_consultation_slot(
            test_db_session,
            user_id=user_id,
            payload=ConsultationBookRequest(
                engagement_id=engagement_id,
                expert_type="nutritionist",
                date=date(2026, 8, 20),
                slot="09:00",
            ),
        )
    assert exc.value.error_code == "INVALID_INPUT"
    assert "cabin" in exc.value.message.lower()

    result = await service.book_consultation_slot(
        test_db_session,
        user_id=user_id,
        payload=ConsultationBookRequest(
            engagement_id=engagement_id,
            expert_type="nutritionist",
            date=date(2026, 8, 20),
            cabin="consultation_cabin_1",
            slot="09:00",
        ),
    )
    assert result["slot"] == "09:00"
    assert result["date"] == "2026-08-20"


@pytest.mark.asyncio
async def test_book_offline_rejects_slot_not_in_engagement_slot_detail(test_db_session, monkeypatch):
    engagement_id = 78611
    user_id = 78611
    await _seed_booking_fixture(
        test_db_session,
        engagement_id=engagement_id,
        participant_user_id=user_id,
        participant_id=78611,
        consultation_mode=ConsultationMode.offline,
        organization_id=100,
        slot_detail=_consultation_slot_detail(),
    )

    service = ExpertAvailabilityService(
        experts_repository=ExpertsRepository(),
        availability_repository=ExpertAvailabilityRepository(),
        override_repository=ExpertAvailabilityOverrideRepository(),
    )
    monkeypatch.setattr(service, "_slot_is_available", AsyncMock(return_value=True))

    with pytest.raises(AppError) as exc:
        await service.book_consultation_slot(
            test_db_session,
            user_id=user_id,
            payload=ConsultationBookRequest(
                engagement_id=engagement_id,
                expert_type="nutritionist",
                date=date(2026, 8, 20),
                cabin="consultation_cabin_1",
                slot="07:00",
            ),
        )
    assert exc.value.error_code == "SLOT_UNAVAILABLE"


@pytest.mark.asyncio
async def test_book_online_uses_expert_availability_and_ignores_cabin(test_db_session, monkeypatch):
    engagement_id = 78612
    user_id = 78612
    await _seed_booking_fixture(
        test_db_session,
        engagement_id=engagement_id,
        participant_user_id=user_id,
        participant_id=78612,
        consultation_mode=ConsultationMode.online,
        organization_id=None,
        slot_detail=_consultation_slot_detail(),
    )

    service = ExpertAvailabilityService(
        experts_repository=ExpertsRepository(),
        availability_repository=ExpertAvailabilityRepository(),
        override_repository=ExpertAvailabilityOverrideRepository(),
    )
    slot_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(service, "_slot_is_available", slot_mock)

    result = await service.book_consultation_slot(
        test_db_session,
        user_id=user_id,
        payload=ConsultationBookRequest(
            engagement_id=engagement_id,
            expert_type="nutritionist",
            date=date.today() + timedelta(days=1),
            cabin="consultation_cabin_1",
            slot="07:00",
        ),
    )
    assert result["slot"] == "07:00"
    slot_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_book_online_rejects_unavailable_expert_slot(test_db_session, monkeypatch):
    engagement_id = 78613
    user_id = 78613
    await _seed_booking_fixture(
        test_db_session,
        engagement_id=engagement_id,
        participant_user_id=user_id,
        participant_id=78613,
        consultation_mode=ConsultationMode.online,
        organization_id=None,
    )

    service = ExpertAvailabilityService(
        experts_repository=ExpertsRepository(),
        availability_repository=ExpertAvailabilityRepository(),
        override_repository=ExpertAvailabilityOverrideRepository(),
    )
    monkeypatch.setattr(service, "_slot_is_available", AsyncMock(return_value=False))

    with pytest.raises(AppError) as exc:
        await service.book_consultation_slot(
            test_db_session,
            user_id=user_id,
            payload=ConsultationBookRequest(
                engagement_id=engagement_id,
                expert_type="nutritionist",
                date=date.today() + timedelta(days=1),
                slot="09:00",
            ),
        )
    assert exc.value.error_code == "INVALID_INPUT"
    assert "not available" in exc.value.message.lower()
