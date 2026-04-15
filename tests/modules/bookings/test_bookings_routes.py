"""Tests for batch booking endpoints under `/book`."""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_book_bio_ai_batch_requires_auth(async_client):
    response = await async_client.post(
        "/book/bio-ai",
        json={
            "members": [
                {
                    "user_id": 1,
                    "address": "A",
                    "pincode": "1",
                    "city": "C",
                    "blood_collection_date": "2026-06-01",
                    "blood_collection_time_slot": "10:00",
                    "diagnostic_package_id": 1,
                }
            ]
        },
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_book_bio_ai_batch_rejects_sub_profile_actor(async_client, test_db_session, monkeypatch):
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
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'test_provider', 'active') ON CONFLICT (diagnostic_package_id) DO UPDATE SET "
            "reference_id = EXCLUDED.reference_id, package_name = EXCLUDED.package_name, "
            "diagnostic_provider = EXCLUDED.diagnostic_provider, status = EXCLUDED.status"
        )
    )
    await test_db_session.commit()

    parent = User(
        user_id=920001,
        age=40,
        phone="9200010000",
        status="active",
        first_name="Par",
        last_name="Ent",
        gender="male",
        city="City",
        parent_id=None,
    )
    child = User(
        user_id=920002,
        age=10,
        phone="9200010000",
        status="active",
        first_name="Chi",
        last_name="Ld",
        gender="male",
        city="City",
        parent_id=920001,
    )
    test_db_session.add_all([parent, child])
    await test_db_session.commit()

    payload = {
        "members": [
            {
                "user_id": 920002,
                "address": "A1",
                "pincode": "111111",
                "city": "City",
                "blood_collection_date": "2026-06-01",
                "blood_collection_time_slot": "10:00",
                "diagnostic_package_id": 1,
            }
        ]
    }
    response = await async_client.post("/book/bio-ai", headers=_auth_header(920002), json=payload)
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_book_bio_ai_batch_primary_books_self_and_child(async_client, test_db_session, monkeypatch):
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
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status) VALUES "
            "(1, 'REF1', 'Diag 1', 'test_provider', 'active') ON CONFLICT (diagnostic_package_id) DO UPDATE SET "
            "reference_id = EXCLUDED.reference_id, package_name = EXCLUDED.package_name, "
            "diagnostic_provider = EXCLUDED.diagnostic_provider, status = EXCLUDED.status"
        )
    )
    await test_db_session.commit()

    parent = User(
        user_id=920010,
        age=40,
        phone="9200100000",
        status="active",
        first_name="Par",
        last_name="Ent",
        gender="male",
        city="Mumbai",
        parent_id=None,
    )
    child = User(
        user_id=920011,
        age=12,
        phone="9200100000",
        status="active",
        first_name="Chi",
        last_name="Ld",
        gender="male",
        city="Mumbai",
        parent_id=920010,
    )
    test_db_session.add_all([parent, child])
    await test_db_session.commit()

    payload = {
        "members": [
            {
                "user_id": 920010,
                "address": "Addr P",
                "pincode": "400001",
                "city": "Mumbai",
                "blood_collection_date": "2026-06-10",
                "blood_collection_time_slot": "09:00",
                "diagnostic_package_id": 1,
            },
            {
                "user_id": 920011,
                "address": "Addr C",
                "pincode": "400002",
                "city": "Mumbai",
                "blood_collection_date": "2026-06-10",
                "blood_collection_time_slot": "09:30",
                "diagnostic_package_id": 1,
            },
        ]
    }
    response = await async_client.post("/book/bio-ai", headers=_auth_header(920010), json=payload)
    assert response.status_code == 200
    body = response.json()["data"]["results"]
    assert len(body) == 2
    assert body[0]["user_id"] == 920010
    assert body[1]["user_id"] == 920011
    assert body[0]["assessment_instance_id"] is not None
    assert body[1]["assessment_instance_id"] is not None


@pytest.mark.asyncio
async def test_book_bio_ai_batch_forbidden_for_unrelated_user(async_client, test_db_session, monkeypatch):
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
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'test_provider', 'active') ON CONFLICT (diagnostic_package_id) DO UPDATE SET "
            "reference_id = EXCLUDED.reference_id, package_name = EXCLUDED.package_name, "
            "diagnostic_provider = EXCLUDED.diagnostic_provider, status = EXCLUDED.status"
        )
    )
    await test_db_session.commit()

    a = User(
        user_id=920020,
        age=30,
        phone="9200200000",
        status="active",
        first_name="A",
        last_name="A",
        gender="male",
        city="X",
        parent_id=None,
    )
    b = User(
        user_id=920021,
        age=30,
        phone="9200210000",
        status="active",
        first_name="B",
        last_name="B",
        gender="female",
        city="Y",
        parent_id=None,
    )
    test_db_session.add_all([a, b])
    await test_db_session.commit()

    payload = {
        "members": [
            {
                "user_id": 920021,
                "address": "Addr",
                "pincode": "111111",
                "city": "Y",
                "blood_collection_date": "2026-07-01",
                "blood_collection_time_slot": "10:00",
                "diagnostic_package_id": 1,
            }
        ]
    }
    response = await async_client.post("/book/bio-ai", headers=_auth_header(920020), json=payload)
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_book_blood_test_batch_no_assessment_instance(async_client, test_db_session):
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'test_provider', 'active') ON CONFLICT (diagnostic_package_id) DO UPDATE SET "
            "reference_id = EXCLUDED.reference_id, package_name = EXCLUDED.package_name, "
            "diagnostic_provider = EXCLUDED.diagnostic_provider, status = EXCLUDED.status"
        )
    )
    await test_db_session.commit()

    u = User(
        user_id=920030,
        age=25,
        phone="9200300000",
        status="active",
        first_name="S",
        last_name="olo",
        gender="male",
        city="Pune",
        parent_id=None,
    )
    test_db_session.add(u)
    await test_db_session.commit()

    payload = {
        "members": [
            {
                "user_id": 920030,
                "address": "Lab St",
                "pincode": "411001",
                "city": "Pune",
                "blood_collection_date": "2026-08-01",
                "blood_collection_time_slot": "08:00",
                "diagnostic_package_id": 1,
            }
        ]
    }
    response = await async_client.post("/book/blood-test", headers=_auth_header(920030), json=payload)
    assert response.status_code == 200
    row = response.json()["data"]["results"][0]
    assert row["assessment_instance_id"] is None
    assert row["metsights_record_id"] is None
    assert row["engagement_id"] is not None

    eng = (
        await test_db_session.execute(
            text(
                "SELECT engagement_type, assessment_package_id FROM engagements WHERE engagement_id = :eid"
            ),
            {"eid": row["engagement_id"]},
        )
    ).first()
    et = eng.engagement_type
    assert (getattr(et, "value", et) == "diagnostic")
    assert eng.assessment_package_id is None
