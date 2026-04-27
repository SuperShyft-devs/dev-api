"""Tests for authenticated B2C Bio AI booking (`POST /users/me/book-bio-ai`)."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_book_bio_ai_requires_auth(async_client):
    response = await async_client.post(
        "/users/me/book-bio-ai",
        json={"blood_collection_date": "2026-02-01", "blood_collection_time_slot": "10:00"},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_book_bio_ai_creates_engagement_slot_instance(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "")

    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status) "
            "VALUES (1, 'PK1', 'Package', '1', 'active') ON CONFLICT (package_id) DO UPDATE SET "
            "assessment_type_code = EXCLUDED.assessment_type_code, status = EXCLUDED.status"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.commit()

    user = User(
        user_id=915010,
        age=30,
        phone="9150100000",
        status="active",
        first_name="Book",
        last_name="User",
        gender="male",
        city="Mumbai",
        is_participant=False,
    )
    test_db_session.add(user)
    await test_db_session.commit()

    headers = _auth_header(915010)
    payload = {"blood_collection_date": "2026-02-01", "blood_collection_time_slot": "10:30"}

    response = await async_client.post("/users/me/book-bio-ai", headers=headers, json=payload)
    assert response.status_code == 200

    data = response.json()["data"]
    assert data["user_id"] == 915010
    assert data["created"] is False
    assert data["is_participant"] is True
    assert data["engagement_id"] is not None
    assert data["engagement_participant_id"] is not None
    assert data["assessment_instance_id"] is not None
    assert data.get("metsights_record_id") in (None, "")

    eid = data["engagement_id"]
    eng = (
        await test_db_session.execute(
            text("SELECT diagnostic_package_id, assessment_package_id FROM engagements WHERE engagement_id = :eid"),
            {"eid": eid},
        )
    ).first()
    assert eng.diagnostic_package_id == 6  # seeded platform_settings B2C default
    assert eng.assessment_package_id == 1

    inst = (
        await test_db_session.execute(
            text(
                "SELECT metsights_record_id, package_id FROM assessment_instances "
                "WHERE assessment_instance_id = :aid"
            ),
            {"aid": data["assessment_instance_id"]},
        )
    ).first()
    assert inst.package_id == 1
    assert inst.metsights_record_id is None


@pytest.mark.asyncio
async def test_book_bio_ai_diagnostic_package_override(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "")

    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) VALUES "
            "(1, 'REF1', 'Diag 1', 'active'), (2, 'REF2', 'Diag 2', 'active') "
            "ON CONFLICT (diagnostic_package_id) DO UPDATE SET "
            "reference_id = EXCLUDED.reference_id, package_name = EXCLUDED.package_name, status = EXCLUDED.status"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings (settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id) "
            "VALUES (1, 1, 1) ON CONFLICT (settings_id) DO UPDATE SET "
            "b2c_default_assessment_package_id = EXCLUDED.b2c_default_assessment_package_id, "
            "b2c_default_diagnostic_package_id = EXCLUDED.b2c_default_diagnostic_package_id"
        )
    )
    await test_db_session.commit()

    user = User(
        user_id=915011,
        age=28,
        phone="9150110000",
        status="active",
        first_name="Diag",
        last_name="Pick",
        gender="female",
        city="Pune",
        is_participant=True,
    )
    test_db_session.add(user)
    await test_db_session.commit()

    headers = _auth_header(915011)
    payload = {
        "blood_collection_date": "2026-03-01",
        "blood_collection_time_slot": "09:00",
        "diagnostic_package_id": 2,
    }

    response = await async_client.post("/users/me/book-bio-ai", headers=headers, json=payload)
    assert response.status_code == 200
    eid = response.json()["data"]["engagement_id"]

    eng = (
        await test_db_session.execute(
            text("SELECT diagnostic_package_id, assessment_package_id FROM engagements WHERE engagement_id = :eid"),
            {"eid": eid},
        )
    ).first()
    assert eng.diagnostic_package_id == 2
    assert eng.assessment_package_id == 1


@pytest.mark.asyncio
async def test_book_bio_ai_stores_metsights_record_when_configured(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-metsights-key")
    monkeypatch.setattr(
        "modules.metsights.service.MetsightsService.create_record_for_profile",
        AsyncMock(return_value="ABC123DEF456"),
    )

    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status) "
            "VALUES (1, 'PK1', 'Package', '1', 'active') ON CONFLICT (package_id) DO UPDATE SET "
            "assessment_type_code = EXCLUDED.assessment_type_code, status = EXCLUDED.status"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.commit()

    user = User(
        user_id=915012,
        age=35,
        phone="9150120000",
        status="active",
        first_name="Meta",
        last_name="Sights",
        gender="male",
        city="Delhi",
        is_participant=True,
        metsights_profile_id="550e8400-e29b-41d4-a716-446655440000",
    )
    test_db_session.add(user)
    await test_db_session.commit()

    headers = _auth_header(915012)
    payload = {"blood_collection_date": "2026-04-01", "blood_collection_time_slot": "11:00"}

    response = await async_client.post("/users/me/book-bio-ai", headers=headers, json=payload)
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["metsights_record_id"] == "ABC123DEF456"

    inst = (
        await test_db_session.execute(
            text("SELECT metsights_record_id FROM assessment_instances WHERE assessment_instance_id = :aid"),
            {"aid": data["assessment_instance_id"]},
        )
    ).first()
    assert inst.metsights_record_id == "ABC123DEF456"


@pytest.mark.asyncio
async def test_book_bio_ai_metsights_profile_required_when_key_configured(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-metsights-key")

    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status) "
            "VALUES (1, 'PK1', 'Package', '1', 'active') ON CONFLICT (package_id) DO UPDATE SET "
            "assessment_type_code = EXCLUDED.assessment_type_code, status = EXCLUDED.status"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.commit()

    # No gender -> _ensure_metsights_profile_id cannot sync; no metsights_profile_id on row.
    user = User(
        user_id=915013,
        age=40,
        phone="9150130000",
        status="active",
        first_name="No",
        last_name="Profile",
        gender=None,
        city="Goa",
        is_participant=True,
        metsights_profile_id=None,
    )
    test_db_session.add(user)
    await test_db_session.commit()

    headers = _auth_header(915013)
    payload = {"blood_collection_date": "2026-05-01", "blood_collection_time_slot": "08:00"}

    response = await async_client.post("/users/me/book-bio-ai", headers=headers, json=payload)
    assert response.status_code == 422
    assert response.json()["error_code"] == "METSIGHTS_PROFILE_REQUIRED"
