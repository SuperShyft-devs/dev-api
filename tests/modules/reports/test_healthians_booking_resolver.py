"""Unit tests for Healthians booking id resolver."""

from __future__ import annotations

from datetime import date, time

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.engagements.models import Engagement, EngagementParticipant
from modules.reports.healthians_booking_resolver import (
    HealthiansBookingSource,
    is_healthians_diagnostic_provider,
    resolve_healthians_booking_id,
    try_participant_booking_id,
)
from modules.users.models import User


class _FakeMetsightsService:
    def __init__(self, *, payload: dict | None = None, should_fail: bool = False):
        self.payload = payload or {}
        self.should_fail = should_fail
        self.calls = 0

    async def get_fetch_collections(self, *, record_id: str):
        self.calls += 1
        if self.should_fail:
            raise RuntimeError("fetch-collections unavailable")
        return self.payload


async def _seed_participant_context(
    db: AsyncSession,
    *,
    user_id: int = 50101,
    engagement_id: int = 40101,
    diagnostic_package_id: int = 30101,
    diagnostic_provider: str = "healthians",
    booking_id: str | None = "1387716654555",
    record_id: str = "REC50101",
) -> None:
    await db.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status, bookings_count) "
            "VALUES (:pid, :ref, :pname, :provider, 'active', 0) "
            "ON CONFLICT (diagnostic_package_id) DO UPDATE SET diagnostic_provider = EXCLUDED.diagnostic_provider"
        ),
        {
            "pid": diagnostic_package_id,
            "ref": f"REF{diagnostic_package_id}",
            "pname": "Healthians Package",
            "provider": diagnostic_provider,
        },
    )
    db.add(
        User(
            user_id=user_id,
            phone=f"{user_id}000000",
            first_name="Jane",
            last_name="Doe",
            age=30,
            gender="female",
            status="active",
        )
    )
    await db.flush()
    await db.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status) "
            "VALUES (901, 'P50101', 'MetSights Pro', '1', 'active') "
            "ON CONFLICT (package_id) DO NOTHING"
        )
    )
    db.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Camp",
            engagement_code=f"ENG{engagement_id}",
            engagement_type="bio_ai",
            assessment_package_id=901,
            diagnostic_package_id=diagnostic_package_id,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 1),
            status="running",
            participant_count=0,
        )
    )
    await db.flush()
    db.add(
        EngagementParticipant(
            engagement_participant_id=user_id + 500000,
            engagement_id=engagement_id,
            user_id=user_id,
            engagement_date=date(2026, 1, 1),
            slot_start_time=time(10, 0),
            booking_id=booking_id,
        )
    )
    await db.execute(
        text(
            "INSERT INTO assessment_instances "
            "(assessment_instance_id, user_id, package_id, engagement_id, status, metsights_record_id, assigned_at, completed_at) "
            "VALUES (:aid, :uid, 901, :eid, 'completed', :record_id, NOW(), NOW()) "
            "ON CONFLICT (assessment_instance_id) DO UPDATE SET metsights_record_id = EXCLUDED.metsights_record_id"
        ),
        {
            "aid": user_id,
            "uid": user_id,
            "eid": engagement_id,
            "record_id": record_id,
        },
    )
    await db.flush()


def test_is_healthians_diagnostic_provider_case_insensitive():
    assert is_healthians_diagnostic_provider("healthians") is True
    assert is_healthians_diagnostic_provider("Healthians") is True
    assert is_healthians_diagnostic_provider("other_lab") is False
    assert is_healthians_diagnostic_provider(None) is False


def test_try_participant_booking_id_requires_healthians_provider():
    assert try_participant_booking_id("12345", "healthians") == "12345"
    assert try_participant_booking_id("12345", "other_lab") is None
    assert try_participant_booking_id(None, "healthians") is None
    assert try_participant_booking_id("  ", "healthians") is None


@pytest.mark.asyncio
async def test_resolve_prefers_participant_booking_without_metsights(test_db_session):
    await _seed_participant_context(test_db_session)
    await test_db_session.commit()

    fake_metsights = _FakeMetsightsService(
        payload={
            "reference_id": "metsights-booking",
            "provider": {"code": "Healthians"},
        }
    )
    resolved = await resolve_healthians_booking_id(
        test_db_session,
        user_id=50101,
        engagement_id=40101,
        record_id="REC50101",
        metsights_service=fake_metsights,
    )

    assert resolved.booking_id == "1387716654555"
    assert resolved.source == HealthiansBookingSource.PARTICIPANT
    assert resolved.collection_data is None
    assert fake_metsights.calls == 0


@pytest.mark.asyncio
async def test_resolve_falls_back_to_metsights_when_booking_id_missing(test_db_session):
    await _seed_participant_context(test_db_session, booking_id=None)
    await test_db_session.commit()

    fake_metsights = _FakeMetsightsService(
        payload={
            "reference_id": "metsights-booking",
            "provider": {"code": "Healthians"},
        }
    )
    resolved = await resolve_healthians_booking_id(
        test_db_session,
        user_id=50101,
        engagement_id=40101,
        record_id="REC50101",
        metsights_service=fake_metsights,
    )

    assert resolved.booking_id == "metsights-booking"
    assert resolved.source == HealthiansBookingSource.METSIGHTS
    assert resolved.collection_data is not None
    assert fake_metsights.calls == 1


@pytest.mark.asyncio
async def test_resolve_falls_back_when_provider_not_healthians(test_db_session):
    await _seed_participant_context(
        test_db_session,
        booking_id="participant-booking",
        diagnostic_provider="other_lab",
    )
    await test_db_session.commit()

    fake_metsights = _FakeMetsightsService(
        payload={
            "reference_id": "metsights-booking",
            "provider": {"code": "Healthians"},
        }
    )
    resolved = await resolve_healthians_booking_id(
        test_db_session,
        user_id=50101,
        engagement_id=40101,
        record_id="REC50101",
        metsights_service=fake_metsights,
    )

    assert resolved.booking_id == "metsights-booking"
    assert resolved.source == HealthiansBookingSource.METSIGHTS
    assert fake_metsights.calls == 1


@pytest.mark.asyncio
async def test_resolve_metsights_rejects_non_healthians_provider(test_db_session):
    await _seed_participant_context(test_db_session, booking_id=None)
    await test_db_session.commit()

    fake_metsights = _FakeMetsightsService(
        payload={
            "reference_id": "metsights-booking",
            "provider": {"code": "OtherLab"},
        }
    )
    with pytest.raises(AppError) as exc_info:
        await resolve_healthians_booking_id(
            test_db_session,
            user_id=50101,
            engagement_id=40101,
            record_id="REC50101",
            metsights_service=fake_metsights,
        )

    assert exc_info.value.error_code == "INVALID_STATE"
    assert "only Healthians is supported" in exc_info.value.message


@pytest.mark.asyncio
async def test_resolve_metsights_requires_reference_id(test_db_session):
    await _seed_participant_context(test_db_session, booking_id=None)
    await test_db_session.commit()

    fake_metsights = _FakeMetsightsService(
        payload={
            "reference_id": "",
            "provider": {"code": "Healthians"},
        }
    )
    with pytest.raises(AppError) as exc_info:
        await resolve_healthians_booking_id(
            test_db_session,
            user_id=50101,
            engagement_id=40101,
            record_id="REC50101",
            metsights_service=fake_metsights,
        )

    assert exc_info.value.error_code == "INVALID_STATE"
    assert "missing the provider reference id" in exc_info.value.message
