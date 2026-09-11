"""Tests for POST /engagements/{id}/connect-metsights-records."""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import text

from modules.metsights.service import MetsightsService
from modules.users.models import User
from tests.helpers.auth import employee_auth_header, make_employee, seed_employee, user_auth_header

METSIGHTS_PROFILE_ID = "01961d4b-3cb1-cfae-f876-2957ef9acf18"
NEW_RECORD_ID = "08AB25490686"


def _auth_header(employee_id: int) -> dict[str, str]:
    return employee_auth_header(employee_id)


async def _seed_employee(test_db_session, *, employee_id: int, role: str = "admin"):
    await seed_employee(test_db_session, employee_id=employee_id, role=role)


async def _seed_engagement_with_package(test_db_session, *, engagement_id: int = 9201, package_id: int = 2):
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'Test Diagnostic', 'test_provider', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status) "
            "VALUES (:pid, 'METSIGHTS_PRO', 'Metsights Pro', '2', 'active') "
            "ON CONFLICT (package_id) DO UPDATE SET assessment_type_code = EXCLUDED.assessment_type_code"
        ),
        {"pid": package_id},
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, "
            "assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, "
            "status, organization_id) "
            "VALUES (:eid, 'Camp', 'ENG9201', 'bio_ai', :pid, 1, 'BLR', 20, "
            "'2026-02-01', '2026-02-28', 'running', NULL)"
        ),
        {"eid": engagement_id, "pid": package_id},
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_connect_metsights_records_requires_auth(async_client):
    response = await async_client.post(
        "/engagements/9201/connect-metsights-records",
        json={"package_id": 2},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_connect_metsights_records_links_existing_instances(async_client, test_db_session, monkeypatch):
    await _seed_employee(test_db_session, employee_id=1)
    await _seed_engagement_with_package(test_db_session)

    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, first_name, last_name, age, phone, status, metsights_profile_id) "
            "VALUES (5201, 'Riya', 'Sharma', 33, '+919876543210', 'active', :pid)"
        ),
        {"pid": METSIGHTS_PROFILE_ID},
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_participants (engagement_id, user_id, engagement_date, slot_start_time) "
            "VALUES (9201, 5201, '2026-02-10', '09:00:00')"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_instances (user_id, engagement_id, package_id, status, metsights_record_id) "
            "VALUES (5201, 9201, 2, 'assigned', NULL)"
        )
    )
    await test_db_session.commit()

    async def _create_record_for_profile(self, *, profile_id: str, assessment_type_code: str):
        assert profile_id == METSIGHTS_PROFILE_ID
        assert assessment_type_code == "2"
        return NEW_RECORD_ID

    monkeypatch.setattr(MetsightsService, "create_record_for_profile", _create_record_for_profile)
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")

    response = await async_client.post(
        "/engagements/9201/connect-metsights-records",
        headers=_auth_header(1),
        json={"package_id": 2},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["connected"] == 1
    assert data["skipped"] == 0
    assert data["failed"] == 0
    assert data["results"][0]["metsights_record_id"] == NEW_RECORD_ID

    row = (
        await test_db_session.execute(
            text(
                "SELECT metsights_record_id FROM assessment_instances "
                "WHERE engagement_id = 9201 AND user_id = 5201 AND package_id = 2"
            )
        )
    ).first()
    assert row.metsights_record_id == NEW_RECORD_ID


@pytest.mark.asyncio
async def test_connect_metsights_records_skips_already_connected(async_client, test_db_session, monkeypatch):
    await _seed_employee(test_db_session, employee_id=1)
    await _seed_engagement_with_package(test_db_session, engagement_id=9202)

    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, age, phone, status, metsights_profile_id) "
            "VALUES (5202, 30, '+919876543211', 'active', :pid)"
        ),
        {"pid": METSIGHTS_PROFILE_ID},
    )
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_instances (user_id, engagement_id, package_id, status, metsights_record_id) "
            "VALUES (5202, 9202, 2, 'assigned', 'EXISTINGREC1')"
        )
    )
    await test_db_session.commit()

    called = {"n": 0}

    async def _create_record_for_profile(self, **kwargs):
        called["n"] += 1
        return NEW_RECORD_ID

    monkeypatch.setattr(MetsightsService, "create_record_for_profile", _create_record_for_profile)
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")

    response = await async_client.post(
        "/engagements/9202/connect-metsights-records",
        headers=_auth_header(1),
        json={"package_id": 2},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["connected"] == 0
    assert data["skipped"] == 1
    assert data["results"][0]["reason"] == "already_connected"
    assert called["n"] == 0


@pytest.mark.asyncio
async def test_connect_metsights_records_skips_no_profile_id(async_client, test_db_session, monkeypatch):
    await _seed_employee(test_db_session, employee_id=1)
    await _seed_engagement_with_package(test_db_session, engagement_id=9203)

    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, age, phone, status, metsights_profile_id) "
            "VALUES (5203, 30, '+919876543212', 'active', NULL)"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_instances (user_id, engagement_id, package_id, status, metsights_record_id) "
            "VALUES (5203, 9203, 2, 'assigned', NULL)"
        )
    )
    await test_db_session.commit()

    called = {"n": 0}

    async def _create_record_for_profile(self, **kwargs):
        called["n"] += 1
        return NEW_RECORD_ID

    monkeypatch.setattr(MetsightsService, "create_record_for_profile", _create_record_for_profile)
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")

    response = await async_client.post(
        "/engagements/9203/connect-metsights-records",
        headers=_auth_header(1),
        json={"package_id": 2},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["connected"] == 0
    assert data["skipped"] == 1
    assert data["results"][0]["reason"] == "no_metsights_profile_id"
    assert called["n"] == 0
