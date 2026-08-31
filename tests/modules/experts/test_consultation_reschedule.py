"""Tests for POST /experts/consultations/reschedule flow."""

from __future__ import annotations

from datetime import date, time, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from core.exceptions import AppError
from modules.diagnostics.models import DiagnosticPackage
from modules.engagements.enums import ConsultationMode
from modules.engagements.models import Engagement, EngagementParticipant
from modules.experts.models import ConsultationBooking, Expert, ExpertAvailabilityOverrideModel
from modules.experts.repository import (
    ExpertAvailabilityOverrideRepository,
    ExpertAvailabilityRepository,
    ExpertsRepository,
)
from modules.experts.schemas import ConsultationRescheduleRequest
from modules.experts.service import ExpertAvailabilityService
from modules.users.models import User


async def _seed_reschedule_fixture(
    test_db_session,
    *,
    engagement_id: int,
    participant_user_id: int,
    participant_id: int,
    consultation_mode: ConsultationMode | None = ConsultationMode.online,
    expert_type: str = "nutritionist",
    want: bool = True,
    done: bool = False,
    consultation_date: date | None = None,
    consultation_slot: str | None = "09:00",
    expert_id: int | None = None,
    consultations: dict | None = None,
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
            first_name="Reschedule",
            last_name="User",
            email=f"reschedule{participant_user_id}@example.com",
            status="active",
        )
    )
    await test_db_session.flush()

    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Reschedule Test",
            engagement_code=f"RS{engagement_id}",
            organization_id=None,
            engagement_type=1,
            consultations=consultations if consultations is not None else {expert_type: True},
            consultation_mode=consultation_mode,
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

    participant = EngagementParticipant(
        engagement_participant_id=participant_id,
        engagement_id=engagement_id,
        user_id=participant_user_id,
        booked_by_user_id=participant_user_id,
        engagement_date=date(2026, 8, 20),
        slot_start_time=time(9, 0),
    )
    test_db_session.add(participant)
    await test_db_session.flush()

    booking = ConsultationBooking(
        engagement_participant_id=participant_id,
        expert_type=expert_type,
        want=want,
        done=done,
        consultation_date=consultation_date or date(2026, 8, 20),
        consultation_slot=consultation_slot,
        expert_id=expert_id,
    )
    test_db_session.add(booking)
    await test_db_session.flush()
    participant.consultation_booking_ids = [booking.consultation_id]
    test_db_session.add(participant)
    await test_db_session.commit()
    return booking.consultation_id


def _service() -> ExpertAvailabilityService:
    return ExpertAvailabilityService(
        experts_repository=ExpertsRepository(),
        availability_repository=ExpertAvailabilityRepository(),
        override_repository=ExpertAvailabilityOverrideRepository(),
    )


@pytest.mark.asyncio
async def test_reschedule_online_nutritionist_success(test_db_session, monkeypatch):
    engagement_id = 78701
    user_id = 78701
    await _seed_reschedule_fixture(
        test_db_session,
        engagement_id=engagement_id,
        participant_user_id=user_id,
        participant_id=78701,
    )

    service = _service()
    slot_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(service, "_slot_is_available", slot_mock)

    new_date = date.today() + timedelta(days=2)
    result = await service.reschedule_consultation_slot(
        test_db_session,
        user_id=user_id,
        payload=ConsultationRescheduleRequest(
            engagement_id=engagement_id,
            consultation_date=new_date,
            consultation_slot="10:30",
            expert_type="nutritionist",
        ),
    )

    assert result["message"] == "Consultation rescheduled"
    assert result["date"] == new_date.isoformat()
    assert result["slot"] == "10:30"
    assert result["expert_id"] is None
    slot_mock.assert_awaited_once()

    booking = (
        await test_db_session.execute(
            select(ConsultationBooking).where(ConsultationBooking.engagement_participant_id == 78701)
        )
    ).scalar_one()
    assert booking.consultation_date == new_date
    assert booking.consultation_slot == "10:30"
    assert booking.expert_id is None


@pytest.mark.asyncio
async def test_reschedule_rejects_offline_mode(test_db_session, monkeypatch):
    engagement_id = 78702
    user_id = 78702
    await _seed_reschedule_fixture(
        test_db_session,
        engagement_id=engagement_id,
        participant_user_id=user_id,
        participant_id=78702,
        consultation_mode=ConsultationMode.offline,
    )

    service = _service()
    monkeypatch.setattr(service, "_slot_is_available", AsyncMock(return_value=True))

    with pytest.raises(AppError) as exc:
        await service.reschedule_consultation_slot(
            test_db_session,
            user_id=user_id,
            payload=ConsultationRescheduleRequest(
                engagement_id=engagement_id,
                consultation_date=date.today() + timedelta(days=1),
                consultation_slot="10:00",
                expert_type="nutritionist",
            ),
        )
    assert exc.value.error_code == "INVALID_INPUT"
    assert "online" in exc.value.message.lower()


@pytest.mark.asyncio
async def test_reschedule_rejects_non_nutritionist(test_db_session, monkeypatch):
    engagement_id = 78703
    user_id = 78703
    await _seed_reschedule_fixture(
        test_db_session,
        engagement_id=engagement_id,
        participant_user_id=user_id,
        participant_id=78703,
        expert_type="doctor",
        consultations={"doctor": True},
    )

    service = _service()
    monkeypatch.setattr(service, "_slot_is_available", AsyncMock(return_value=True))

    with pytest.raises(AppError) as exc:
        await service.reschedule_consultation_slot(
            test_db_session,
            user_id=user_id,
            payload=ConsultationRescheduleRequest(
                engagement_id=engagement_id,
                consultation_date=date.today() + timedelta(days=1),
                consultation_slot="10:00",
                expert_type="doctor",
            ),
        )
    assert exc.value.error_code == "INVALID_INPUT"
    assert "nutritionist" in exc.value.message.lower()


@pytest.mark.asyncio
async def test_reschedule_rejects_want_false(test_db_session, monkeypatch):
    engagement_id = 78704
    user_id = 78704
    await _seed_reschedule_fixture(
        test_db_session,
        engagement_id=engagement_id,
        participant_user_id=user_id,
        participant_id=78704,
        want=False,
    )

    service = _service()
    monkeypatch.setattr(service, "_slot_is_available", AsyncMock(return_value=True))

    with pytest.raises(AppError) as exc:
        await service.reschedule_consultation_slot(
            test_db_session,
            user_id=user_id,
            payload=ConsultationRescheduleRequest(
                engagement_id=engagement_id,
                consultation_date=date.today() + timedelta(days=1),
                consultation_slot="10:00",
                expert_type="nutritionist",
            ),
        )
    assert exc.value.error_code == "INVALID_INPUT"
    assert "did not request" in exc.value.message.lower()


@pytest.mark.asyncio
async def test_reschedule_rejects_unavailable_slot(test_db_session, monkeypatch):
    engagement_id = 78705
    user_id = 78705
    await _seed_reschedule_fixture(
        test_db_session,
        engagement_id=engagement_id,
        participant_user_id=user_id,
        participant_id=78705,
    )

    service = _service()
    monkeypatch.setattr(service, "_slot_is_available", AsyncMock(return_value=False))

    with pytest.raises(AppError) as exc:
        await service.reschedule_consultation_slot(
            test_db_session,
            user_id=user_id,
            payload=ConsultationRescheduleRequest(
                engagement_id=engagement_id,
                consultation_date=date.today() + timedelta(days=1),
                consultation_slot="10:00",
                expert_type="nutritionist",
            ),
        )
    assert exc.value.error_code == "INVALID_INPUT"
    assert "not available" in exc.value.message.lower()


@pytest.mark.asyncio
async def test_reschedule_clears_expert_and_booked_override(test_db_session, monkeypatch):
    engagement_id = 78706
    user_id = 78706
    expert_id = 78706

    test_db_session.add(
        User(
            user_id=user_id + 1000,
            age=40,
            phone=f"{user_id + 1000}000000000",
            first_name="Expert",
            last_name="Nutri",
            email=f"expert{user_id}@example.com",
            status="active",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        Expert(
            expert_id=expert_id,
            user_id=user_id + 1000,
            expert_type="nutritionist",
            specialization="Diet",
            status="active",
            session_duration_mins=30,
        )
    )
    await test_db_session.flush()

    old_date = date(2026, 8, 20)
    await _seed_reschedule_fixture(
        test_db_session,
        engagement_id=engagement_id,
        participant_user_id=user_id,
        participant_id=78706,
        consultation_date=old_date,
        consultation_slot="09:00",
        expert_id=expert_id,
    )

    override = ExpertAvailabilityOverrideModel(
        expert_id=expert_id,
        override_date=old_date,
        status="booked",
        start_time=time(9, 0),
        end_time=None,
        buffer_time=None,
    )
    test_db_session.add(override)
    await test_db_session.commit()
    override_id = override.id

    service = _service()
    monkeypatch.setattr(service, "_slot_is_available", AsyncMock(return_value=True))

    new_date = date.today() + timedelta(days=3)
    result = await service.reschedule_consultation_slot(
        test_db_session,
        user_id=user_id,
        payload=ConsultationRescheduleRequest(
            engagement_id=engagement_id,
            consultation_date=new_date,
            consultation_slot="11:00",
            expert_type="nutritionist",
        ),
    )

    assert result["expert_id"] is None
    booking = (
        await test_db_session.execute(
            select(ConsultationBooking).where(ConsultationBooking.engagement_participant_id == 78706)
        )
    ).scalar_one()
    assert booking.expert_id is None
    assert booking.consultation_date == new_date
    assert booking.consultation_slot == "11:00"

    deleted = await test_db_session.get(ExpertAvailabilityOverrideModel, override_id)
    assert deleted is None


@pytest.mark.asyncio
async def test_reschedule_rejects_done_consultation(test_db_session, monkeypatch):
    engagement_id = 78707
    user_id = 78707
    await _seed_reschedule_fixture(
        test_db_session,
        engagement_id=engagement_id,
        participant_user_id=user_id,
        participant_id=78707,
        done=True,
    )

    service = _service()
    monkeypatch.setattr(service, "_slot_is_available", AsyncMock(return_value=True))

    with pytest.raises(AppError) as exc:
        await service.reschedule_consultation_slot(
            test_db_session,
            user_id=user_id,
            payload=ConsultationRescheduleRequest(
                engagement_id=engagement_id,
                consultation_date=date.today() + timedelta(days=1),
                consultation_slot="10:00",
                expert_type="nutritionist",
            ),
        )
    assert exc.value.error_code == "INVALID_INPUT"
    assert "completed" in exc.value.message.lower()
