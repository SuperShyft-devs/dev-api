"""Tests for Metsights sync and questionnaire import routes."""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_sync_metsights_records_requires_auth(async_client):
    response = await async_client.post("/users/1/metsights/sync-records", json={})
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_sync_metsights_records_forbidden_non_employee_other_user(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")

    async def _fake_list(self, **kwargs):
        return {"detail": "ok", "data": []}

    monkeypatch.setattr("modules.metsights.client.MetsightsClient.list_profile_records", _fake_list)

    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, first_name, last_name, age, phone, email, status, is_participant, gender) "
            "VALUES (50101, 'P', 'User', 28, '+1555010101', 'pu@example.com', 'active', true, 'male') "
            "ON CONFLICT (user_id) DO NOTHING"
        )
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/users/1/metsights/sync-records",
        headers=_auth_header(50101),
        json={},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_sync_metsights_records_idempotent(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")

    rid = "MSYNCIDEM01"
    payload = {
        "detail": "ok",
        "data": [
            {
                "id": rid,
                "date": "2026-03-10",
                "assessment_code": "MET_BASIC",
                "assessment_type": "MetSights Basic",
                "is_complete": True,
            }
        ],
    }

    async def _fake_list(self, **kwargs):
        return payload

    monkeypatch.setattr("modules.metsights.client.MetsightsClient.list_profile_records", _fake_list)

    await test_db_session.execute(
        text("UPDATE users SET metsights_profile_id = '550e8400-e29b-41d4-a716-446655440099' WHERE user_id = 1")
    )
    await test_db_session.commit()

    r1 = await async_client.post("/users/1/metsights/sync-records", headers=_auth_header(1), json={})
    assert r1.status_code == 200
    d1 = r1.json()["data"]
    assert len(d1["created"]) == 1

    r2 = await async_client.post("/users/1/metsights/sync-records", headers=_auth_header(1), json={})
    assert r2.status_code == 200
    d2 = r2.json()["data"]
    assert len(d2["created"]) == 0
    assert any(s.get("metsights_record_id") == rid for s in d2["skipped"])


@pytest.mark.asyncio
async def test_sync_then_import_stores_metsights_codes(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")

    rid = "MSIMP01"
    list_payload = {
        "detail": "ok",
        "data": [
            {
                "id": rid,
                "date": "2026-04-01",
                "assessment_code": "MET_BASIC",
                "assessment_type": "MetSights Basic",
                "is_complete": True,
            }
        ],
    }

    detail = {
        "id": rid,
        "physical_measurement": None,
        "vital_parameter": None,
        "diet_lifestyle_parameter": {
            "living_region": "1",
            "diet_preference": "0",
            "food_groups": ["0", "3"],
        },
        "fitness_parameter": None,
    }

    async def _fake_list(self, **kwargs):
        return list_payload

    async def _fake_detail(self, **kwargs):
        return {"detail": "ok", "data": detail}

    monkeypatch.setattr("modules.metsights.client.MetsightsClient.list_profile_records", _fake_list)
    monkeypatch.setattr("modules.metsights.client.MetsightsClient.get_record_detail", _fake_detail)

    await test_db_session.execute(
        text("UPDATE users SET metsights_profile_id = '550e8400-e29b-41d4-a716-446655440088' WHERE user_id = 1")
    )
    await test_db_session.commit()

    sync_r = await async_client.post("/users/1/metsights/sync-records", headers=_auth_header(1), json={})
    assert sync_r.status_code == 200
    created = sync_r.json()["data"]["created"]
    assert len(created) == 1
    aid = int(created[0]["assessment_instance_id"])

    imp_r = await async_client.post(f"/assessments/{aid}/metsights/import-answers", headers=_auth_header(1))
    assert imp_r.status_code == 200
    assert imp_r.json()["data"]["responses_upserted"] >= 3

    row = (
        await test_db_session.execute(
            text(
                "SELECT qr.answer FROM questionnaire_responses qr "
                "JOIN questionnaire_definitions qd ON qd.question_id = qr.question_id "
                "WHERE qr.assessment_instance_id = :aid AND qd.question_key = 'living_region'"
            ),
            {"aid": aid},
        )
    ).first()
    assert row is not None
    ans = row[0]
    assert ans == "1" or ans == '"1"' or (isinstance(ans, dict) and ans.get("value") is None)
