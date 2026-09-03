"""Tests for load-bioai-reports participant endpoint."""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.engagements.models import Engagement, EngagementParticipant
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token(
        {"sub": str(user_id)},
        timedelta(minutes=5),
        secret_key=settings.JWT_SECRET_KEY,
    )
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id:010d}", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


async def _engagement_type_id(test_db_session, code: str) -> int:
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_types (code, display_name, is_active) "
            "VALUES (:code, :dn, true) "
            "ON CONFLICT (code) DO UPDATE SET is_active = true"
        ),
        {"code": code, "dn": code},
    )
    await test_db_session.commit()
    row = (
        await test_db_session.execute(
            text("SELECT id FROM engagement_types WHERE code = :code"),
            {"code": code},
        )
    ).one()
    return int(row[0])


async def _seed_engagement(test_db_session, *, engagement_id: int, type_code: str):
    type_id = await _engagement_type_id(test_db_session, type_code)
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (:pid, :pcode, :dname, 'active') ON CONFLICT (package_id) DO NOTHING"
        ),
        {"pid": engagement_id, "pcode": f"PKG{engagement_id}", "dname": f"Package {engagement_id}"},
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status, bookings_count) "
            "VALUES (:did, :ref, :pname, 'test_provider', 'active', 0) ON CONFLICT (diagnostic_package_id) DO NOTHING"
        ),
        {"did": engagement_id, "ref": f"REF{engagement_id}", "pname": f"Diag {engagement_id}"},
    )
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_code=f"ENG{engagement_id}",
            engagement_type=type_id,
            assessment_package_id=engagement_id,
            diagnostic_package_id=engagement_id,
            slot_duration=20,
            start_date=date(2026, 9, 1),
            end_date=date(2026, 9, 30),
            status="running",
        )
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_load_bioai_reports_route_requires_auth(async_client):
    response = await async_client.post(
        "/engagements/1/participants/load-bioai-reports",
        json={"user_ids": [1]},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_load_bioai_reports_route_rejects_unknown_participants(async_client, test_db_session):
    engagement_id = 883101
    admin_id = 883101
    await _seed_employee(test_db_session, user_id=admin_id, employee_id=admin_id)
    await _seed_engagement(test_db_session, engagement_id=engagement_id, type_code="load_bioai_reject")

    response = await async_client.post(
        f"/engagements/{engagement_id}/participants/load-bioai-reports",
        headers=_auth_header(admin_id),
        json={"user_ids": [999883101]},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_load_bioai_reports_route_calls_job_without_notifications(
    async_client,
    test_db_session,
    monkeypatch,
):
    engagement_id = 883102
    admin_id = 883102
    participant_id = 883103
    await _seed_employee(test_db_session, user_id=admin_id, employee_id=admin_id)
    await _seed_engagement(test_db_session, engagement_id=engagement_id, type_code="load_bioai_ok")
    test_db_session.add(
        User(
            user_id=participant_id,
            age=30,
            phone=f"{participant_id:010d}",
            status="active",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=883102,
            engagement_id=engagement_id,
            user_id=participant_id,
        )
    )
    await test_db_session.commit()

    captured: dict = {}

    async def _fake_load(db, **kwargs):
        captured.update(kwargs)
        return {
            "as_of": "2026-09-02",
            "engagement_id": engagement_id,
            "matched": 1,
            "loaded": 1,
            "notified": 0,
            "skipped": 0,
            "failed": 0,
            "dry_run": False,
            "details": [],
        }

    monkeypatch.setattr(
        "modules.notifications.load_bioai_reports.load_bioai_reports",
        AsyncMock(side_effect=_fake_load),
    )

    response = await async_client.post(
        f"/engagements/{engagement_id}/participants/load-bioai-reports",
        headers=_auth_header(admin_id),
        json={"user_ids": [participant_id]},
    )

    assert response.status_code == 200
    body = response.json()["data"]
    assert body["loaded"] == 1
    assert body["notified"] == 0
    assert captured["send_notifications"] is False
    assert captured["user_ids"] == {participant_id}
    assert captured["ignore_engagement_date"] is True
    assert captured["all_engagements"] is True
    assert captured["engagement_id"] == engagement_id
