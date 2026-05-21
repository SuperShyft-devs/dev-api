"""Tests for POST /engagements/{id}/create-metsights-profiles."""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.metsights.service import MetsightsService
from modules.users.models import User

NEW_PROFILE_ID = "019e49eb-efce-d5ce-444b-154b18231133"


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int | None = None):
    eid = employee_id if employee_id is not None else user_id
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}0000000001", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=eid, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


async def _seed_engagement(test_db_session, *, engagement_id: int = 9101):
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'Test Diagnostic', 'test_provider', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, "
            "assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, "
            "status, participant_count, organization_id) "
            "VALUES (:eid, 'Camp', 'ENG9101', 'bio_ai', 1, 1, 'BLR', 20, "
            "'2026-02-01', '2026-02-28', 'active', 0, NULL)"
        ),
        {"eid": engagement_id},
    )
    await test_db_session.commit()


async def _seed_participant(
    test_db_session,
    *,
    engagement_id: int,
    user_id: int,
    metsights_profile_id: str | None = None,
):
    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, first_name, last_name, age, phone, email, gender, date_of_birth, "
            "status, metsights_profile_id) "
            "VALUES (:uid, 'Riya', 'Sharma', 33, '+919876543210', 'riya@example.com', 'Female', "
            "'1992-06-15', 'active', :pid)"
        ),
        {"uid": user_id, "pid": metsights_profile_id},
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_participants (engagement_id, user_id, engagement_date, slot_start_time, "
            "is_profile_created_on_metsights) "
            "VALUES (:eid, :uid, '2026-02-10', '09:00:00', false)"
        ),
        {"eid": engagement_id, "uid": user_id},
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_create_metsights_profiles_requires_auth(async_client):
    response = await async_client.post("/engagements/9101/create-metsights-profiles")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_create_metsights_profiles_skips_existing_id(async_client, test_db_session, monkeypatch):
    await _seed_employee(test_db_session, user_id=9101)
    await _seed_engagement(test_db_session)
    existing_id = "01961d4b-3cb1-cfae-f876-2957ef9acf18"
    await _seed_participant(test_db_session, engagement_id=9101, user_id=5101, metsights_profile_id=existing_id)

    called = {"count": 0}

    async def _get_or_create_profile_id(self, **kwargs):
        called["count"] += 1
        return NEW_PROFILE_ID

    monkeypatch.setattr(MetsightsService, "get_or_create_profile_id", _get_or_create_profile_id)
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")

    response = await async_client.post(
        "/engagements/9101/create-metsights-profiles",
        headers=_auth_header(9101),
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["skipped"] == 1
    assert data["created"] == 0
    assert data["failed"] == 0
    assert called["count"] == 0

    row = (
        await test_db_session.execute(
            text("SELECT metsights_profile_id FROM users WHERE user_id = 5101")
        )
    ).first()
    assert row.metsights_profile_id == existing_id


@pytest.mark.asyncio
async def test_create_metsights_profiles_creates_and_stores_id(async_client, test_db_session, monkeypatch):
    await _seed_employee(test_db_session, user_id=9102)
    await _seed_engagement(test_db_session, engagement_id=9102)
    await _seed_participant(test_db_session, engagement_id=9102, user_id=5102, metsights_profile_id=None)

    async def _get_or_create_profile_id(self, **kwargs):
        assert kwargs["first_name"] == "Riya"
        assert kwargs["gender"] == "2"
        return NEW_PROFILE_ID

    monkeypatch.setattr(MetsightsService, "get_or_create_profile_id", _get_or_create_profile_id)
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")

    response = await async_client.post(
        "/engagements/9102/create-metsights-profiles",
        headers=_auth_header(9102),
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["created"] == 1
    assert data["skipped"] == 0
    assert data["failed"] == 0
    assert data["results"][0]["status"] == "created"
    assert data["results"][0]["metsights_profile_id"] == NEW_PROFILE_ID

    user_row = (
        await test_db_session.execute(
            text("SELECT metsights_profile_id FROM users WHERE user_id = 5102")
        )
    ).first()
    assert user_row.metsights_profile_id == NEW_PROFILE_ID

    participant_row = (
        await test_db_session.execute(
            text(
                "SELECT is_profile_created_on_metsights FROM engagement_participants "
                "WHERE engagement_id = 9102 AND user_id = 5102"
            )
        )
    ).first()
    assert participant_row.is_profile_created_on_metsights is True


@pytest.mark.asyncio
async def test_create_metsights_profiles_fails_missing_fields(async_client, test_db_session, monkeypatch):
    await _seed_employee(test_db_session, user_id=9103)
    await _seed_engagement(test_db_session, engagement_id=9103)

    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, first_name, last_name, age, phone, status) "
            "VALUES (5103, '', 'Sharma', 33, '+919876543211', 'active')"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_participants (engagement_id, user_id, engagement_date, slot_start_time) "
            "VALUES (9103, 5103, '2026-02-10', '09:00:00')"
        )
    )
    await test_db_session.commit()

    async def _get_or_create_profile_id(self, **kwargs):
        return NEW_PROFILE_ID

    monkeypatch.setattr(MetsightsService, "get_or_create_profile_id", _get_or_create_profile_id)
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")

    response = await async_client.post(
        "/engagements/9103/create-metsights-profiles",
        headers=_auth_header(9103),
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["failed"] == 1
    assert data["results"][0]["reason"] == "missing_required_user_fields"
