"""Tests for Engagements sync on platform settings."""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.metsights.service import MetsightsService
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id:011d}", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


async def _seed_assessment_and_diagnostic_packages(test_db_session):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status) "
            "VALUES (901, 'MB', 'Met Basic', '1', 'active'), (902, 'MP', 'Met Pro', '2', 'active') "
            "ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status, complementary_consultation) "
            "VALUES (17, 'MEN', 'Men Peak Performance', 'active', CAST('{\"nutritionist\": true}' AS json)), "
            "(24, 'WMN', 'Women Peak Performance', 'active', CAST('{}' AS json)) "
            "ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_engagements_sync_stats_requires_auth(async_client):
    response = await async_client.get("/platform-settings/engagements-sync/stats")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_engagements_sync_import_page_requires_auth(async_client):
    response = await async_client.post("/platform-settings/engagements-sync/import-page", json={"page": 1})
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_engagements_sync_stats_returns_counts(async_client, test_db_session):
    uid = 9301
    await _seed_employee(test_db_session, user_id=uid, employee_id=9301)
    test_db_session.add(
        User(
            user_id=9302,
            age=28,
            phone="93020000001",
            status="active",
            gender="Female",
            metsights_profile_id="ms-eg-stats-001",
            city="Pune",
        )
    )
    await test_db_session.commit()

    response = await async_client.get(
        "/platform-settings/engagements-sync/stats",
        headers=_auth_header(uid),
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["users_with_metsights_profile_id"] >= 1
    assert "b2c_engagements_total" in data


@pytest.mark.asyncio
async def test_engagements_sync_import_page_creates_engagement_and_skips_fitprint(
    async_client, test_db_session, monkeypatch
):
    uid = 9310
    await _seed_employee(test_db_session, user_id=uid, employee_id=9310)
    await _seed_assessment_and_diagnostic_packages(test_db_session)

    pid = "ms-eg-sync-prof-01"
    test_db_session.add(
        User(
            user_id=9311,
            first_name="Ada",
            last_name="Lovelace",
            age=35,
            phone="93110000001",
            status="active",
            gender="Female",
            metsights_profile_id=pid,
            city="Mumbai",
            address="12 Marine Drive",
            pin_code="400001",
            state="MH",
            country="IN",
        )
    )
    await test_db_session.commit()

    async def _list_records(self, *, profile_id: str, completed=None, code=None, search=None):
        return [
            {
                "id": "EGFP1",
                "date": "2026-05-07",
                "assessment_type": "FitPrint Full",
                "assessment_code": "MY_FITNESS_PRINT",
                "created_at": "2026-05-07T10:00:00Z",
                "updated_at": "2026-05-07T15:00:00Z",
                "is_complete": True,
            },
            {
                "id": "EGPRO1",
                "date": "2026-04-29",
                "assessment_type": "MetSights Pro",
                "assessment_code": "MET_PRO",
                "created_at": "2026-04-29T09:00:00Z",
                "updated_at": "2026-04-30T11:00:00Z",
                "is_complete": True,
            },
        ]

    monkeypatch.setattr(MetsightsService, "list_profile_records", _list_records)
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")

    response = await async_client.post(
        "/platform-settings/engagements-sync/import-page",
        headers=_auth_header(uid),
        json={"page": 1},
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["created"] >= 1
    assert body["skipped"] >= 1
    assert body["users_total"] >= 1

    eng = (
        await test_db_session.execute(
            text(
                "SELECT e.engagement_id, e.organization_id, e.diagnostic_package_id, e.blood_collection_type, "
                "e.consultations, e.start_date, e.end_date, ai.metsights_record_id, ai.status, "
                "ep.is_primary_record_id_synced, ep.is_fitprint_record_id_synced "
                "FROM engagements e "
                "JOIN assessment_instances ai ON ai.engagement_id = e.engagement_id "
                "JOIN engagement_participants ep ON ep.engagement_id = e.engagement_id AND ep.user_id = ai.user_id "
                "WHERE ai.user_id = 9311 AND e.organization_id IS NULL "
                "ORDER BY ai.metsights_record_id ASC"
            )
        )
    ).all()
    assert len(eng) == 1
    assert eng[0].metsights_record_id == "EGPRO1"
    assert eng[0].organization_id is None
    assert int(eng[0].diagnostic_package_id) == 24
    assert eng[0].blood_collection_type == "home_collection"
    assert eng[0].status == "completed"
    assert eng[0].is_primary_record_id_synced is True
    assert eng[0].is_fitprint_record_id_synced is False

    # Second pass should skip already-imported record
    response2 = await async_client.post(
        "/platform-settings/engagements-sync/import-page",
        headers=_auth_header(uid),
        json={"page": 1},
    )
    assert response2.status_code == 200
    body2 = response2.json()["data"]
    assert body2["created"] == 0
    assert body2["skipped"] >= 2
