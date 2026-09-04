"""Tests for sync_diagnostic_report_urls job."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

import pytest
from sqlalchemy import select

from modules.engagements.models import Engagement, EngagementParticipant
from modules.reports.blood_report_archival import is_archived_blood_report_url
from modules.reports.models import IndividualHealthReport
from modules.reports.sync_diagnostic_report_urls import (
    _get_eligible_participants,
    sync_diagnostic_report_urls,
)
from modules.users.models import User

_REPORT_URL = "https://example.com/blood-report.pdf"
_ARCHIVED_REPORT_URL = "https://supershyft.com/reports/AbCdEfGhIjKlMnOp.pdf"
_VERIFIED_AT = "2025-12-04 19:55:56"
_VERIFIED_AT_DT = datetime(2025, 12, 4, 19, 55, 56, tzinfo=timezone.utc)


def _report_payload(
    *,
    full_report: int = 1,
    verified_at: str = _VERIFIED_AT,
    cust_name: str = "John Doe",
    report_url: str = _REPORT_URL,
) -> dict:
    return {
        "data": [
            {
                "cust_name": cust_name,
                "report_url": report_url,
                "full_report": full_report,
                "verified_at": verified_at,
            }
        ]
    }


async def _seed_participant_with_booking(
    test_db_session,
    *,
    user_id: int = 198001,
    engagement_id: int = 198001,
    assessment_id: int | None = None,
    diagnostic_package_id: int = 17,
    booking_id: str | None = None,
    engagement_status: str = "completed",
    existing_diag_url: str | None = None,
    existing_full_report: bool | None = None,
    existing_verified_at=None,
) -> None:
    resolved_booking_id = booking_id or f"BOOK-{engagement_id}"
    test_db_session.add(
        User(
            user_id=user_id,
            first_name="John",
            last_name="Doe",
            phone=f"{user_id}000000",
            age=30,
            status="active",
        )
    )
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Sync Diagnostic URL Engagement",
            engagement_code=f"ENG-SYNC-DIAG-{engagement_id}",
            engagement_type=None,
            assessment_package_id=1,
            diagnostic_package_id=diagnostic_package_id,
            city="Bengaluru",
            slot_duration=20,
            start_date=date.today() - timedelta(days=30),
            end_date=date.today() - timedelta(days=1),
            status=engagement_status,
        )
    )
    await test_db_session.flush()

    test_db_session.add(
        EngagementParticipant(
            engagement_id=engagement_id,
            user_id=user_id,
            engagement_date=date.today() - timedelta(days=10),
            slot_start_time=time(9, 0),
            booking_id=resolved_booking_id,
        )
    )
    if assessment_id is not None:
        from modules.assessments.models import AssessmentInstance

        test_db_session.add(
            AssessmentInstance(
                assessment_instance_id=assessment_id,
                user_id=user_id,
                package_id=1,
                engagement_id=engagement_id,
                status="active",
                metsights_record_id="MS-OPTIONAL",
            )
        )
    if existing_diag_url is not None or existing_full_report is not None or existing_verified_at is not None:
        test_db_session.add(
            IndividualHealthReport(
                report_id=1980000 + engagement_id,
                user_id=user_id,
                engagement_id=engagement_id,
                assessment_instance_id=assessment_id,
                diagnostic_report_url=existing_diag_url,
                blood_parameters_full_report=existing_full_report,
                blood_parameters_verified_at=existing_verified_at,
            )
        )
    await test_db_session.commit()


async def _fake_resolve_persistable_diagnostic_report_url(
    healthians_url: str,
    *,
    is_full_report: bool,
    existing_url: str | None,
    assessment_instance_id: int,
) -> str | None:
    healthians = (healthians_url or "").strip()
    existing = (existing_url or "").strip()
    if not is_full_report:
        if existing and is_archived_blood_report_url(existing):
            return existing
        return None
    if existing and is_archived_blood_report_url(existing):
        return existing
    if not healthians:
        return None
    return _ARCHIVED_REPORT_URL


def _patch_healthians(monkeypatch, *, report_payload: dict | None = None) -> None:
    payload = report_payload or _report_payload()

    async def _fake_token():
        return "token"

    async def _fake_report(_token, _booking_id):
        return payload

    monkeypatch.setattr(
        "modules.reports.sync_diagnostic_report_urls.healthians_client.get_access_token",
        _fake_token,
    )
    monkeypatch.setattr(
        "modules.reports.sync_diagnostic_report_urls.healthians_client.get_booking_report",
        _fake_report,
    )
    monkeypatch.setattr(
        "modules.reports.sync_diagnostic_report_urls.resolve_persistable_diagnostic_report_url",
        _fake_resolve_persistable_diagnostic_report_url,
    )


@pytest.mark.asyncio
async def test_get_eligible_participants_includes_all_statuses_with_booking_id(
    test_db_session,
):
    await _seed_participant_with_booking(
        test_db_session,
        user_id=198010,
        engagement_id=198010,
        engagement_status="completed",
    )
    await _seed_participant_with_booking(
        test_db_session,
        user_id=198011,
        engagement_id=198011,
        engagement_status="running",
    )

    rows = await _get_eligible_participants(test_db_session, engagement_id=198010)
    pairs = {(row[0], row[1], row[4]) for row in rows}
    assert (198010, 198010, "BOOK-198010") in pairs

    rows_running = await _get_eligible_participants(test_db_session, engagement_id=198011)
    pairs_running = {(row[0], row[1], row[4]) for row in rows_running}
    assert (198011, 198011, "BOOK-198011") in pairs_running


@pytest.mark.asyncio
async def test_sync_diagnostic_report_urls_dry_run_does_not_write(test_db_session, monkeypatch):
    await _seed_participant_with_booking(
        test_db_session,
        user_id=198020,
        engagement_id=198020,
        assessment_id=198020,
    )
    _patch_healthians(monkeypatch)

    result = await sync_diagnostic_report_urls(
        test_db_session, dry_run=True, engagement_id=198020
    )

    assert result["matched"] == 1
    assert result["updated"] == 0
    assert result["details"][0]["action"] == "dry_run"
    ihr = (
        await test_db_session.execute(
            select(IndividualHealthReport).where(
                IndividualHealthReport.user_id == 198020,
                IndividualHealthReport.engagement_id == 198020,
            )
        )
    ).scalar_one_or_none()
    assert ihr is None


@pytest.mark.asyncio
async def test_sync_diagnostic_report_urls_persists_archived_url(test_db_session, monkeypatch):
    await _seed_participant_with_booking(
        test_db_session,
        user_id=198030,
        engagement_id=198030,
        assessment_id=198030,
    )
    _patch_healthians(monkeypatch)

    result = await sync_diagnostic_report_urls(
        test_db_session, engagement_id=198030
    )

    assert result["updated"] == 1
    ihr = (
        await test_db_session.execute(
            select(IndividualHealthReport).where(
                IndividualHealthReport.user_id == 198030,
                IndividualHealthReport.engagement_id == 198030,
            )
        )
    ).scalar_one()
    assert ihr.diagnostic_report_url == _ARCHIVED_REPORT_URL
    assert ihr.blood_parameters_full_report is True
    assert ihr.blood_parameters_verified_at == _VERIFIED_AT_DT


@pytest.mark.asyncio
async def test_sync_diagnostic_report_urls_skips_already_archived_without_force(
    test_db_session, monkeypatch
):
    await _seed_participant_with_booking(
        test_db_session,
        user_id=198040,
        engagement_id=198040,
        assessment_id=198040,
        existing_diag_url=_ARCHIVED_REPORT_URL,
        existing_full_report=True,
        existing_verified_at=_VERIFIED_AT_DT,
    )
    report_calls: list[str] = []

    async def _fake_report(_token, booking_id):
        report_calls.append(booking_id)
        return _report_payload()

    _patch_healthians(monkeypatch)
    monkeypatch.setattr(
        "modules.reports.sync_diagnostic_report_urls.healthians_client.get_booking_report",
        _fake_report,
    )

    result = await sync_diagnostic_report_urls(
        test_db_session, engagement_id=198040
    )
    assert result["skipped"] == 1
    assert report_calls == []
    assert result["details"][0]["reason"] == "diagnostic_report_url already archived"


@pytest.mark.asyncio
async def test_sync_diagnostic_report_urls_force_refetches_archived_url(
    test_db_session, monkeypatch
):
    await _seed_participant_with_booking(
        test_db_session,
        user_id=198050,
        engagement_id=198050,
        assessment_id=198050,
        existing_diag_url=_ARCHIVED_REPORT_URL,
        existing_full_report=True,
        existing_verified_at=_VERIFIED_AT_DT,
    )
    report_calls: list[str] = []

    async def _fake_report(_token, booking_id):
        report_calls.append(booking_id)
        return _report_payload()

    _patch_healthians(monkeypatch)
    monkeypatch.setattr(
        "modules.reports.sync_diagnostic_report_urls.healthians_client.get_booking_report",
        _fake_report,
    )

    result = await sync_diagnostic_report_urls(
        test_db_session, force=True, engagement_id=198050
    )

    assert result["updated"] == 1
    assert report_calls == ["BOOK-198050"]
    ihr = (
        await test_db_session.execute(
            select(IndividualHealthReport).where(
                IndividualHealthReport.user_id == 198050,
                IndividualHealthReport.engagement_id == 198050,
            )
        )
    ).scalar_one()
    assert ihr.diagnostic_report_url == _ARCHIVED_REPORT_URL


@pytest.mark.asyncio
async def test_sync_diagnostic_report_urls_does_not_require_metsights_record(
    test_db_session, monkeypatch
):
    await _seed_participant_with_booking(
        test_db_session,
        user_id=198060,
        engagement_id=198060,
        assessment_id=None,
    )
    _patch_healthians(monkeypatch)

    result = await sync_diagnostic_report_urls(
        test_db_session, engagement_id=198060
    )

    assert result["updated"] == 1
    ihr = (
        await test_db_session.execute(
            select(IndividualHealthReport).where(
                IndividualHealthReport.user_id == 198060,
                IndividualHealthReport.engagement_id == 198060,
            )
        )
    ).scalar_one()
    assert ihr.diagnostic_report_url == _ARCHIVED_REPORT_URL
    assert ihr.assessment_instance_id is None
