from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import text


@pytest.mark.asyncio
async def test_public_onboard_requires_blood_fields(async_client, test_db_session):
    payload = {
        "age": 30,
        "first_name": "A",
        "last_name": "B",
        "email": "ab@example.com",
        "phone": "1111111111",
        "gender": "male",
        "dob": "1990-01-01",
        "address": "addr",
        "pincode": "123456",
        "city": "Mumbai",
        "state": "MH",
        "country": "IN",
        "referred_by": "",
    }

    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 400
    assert response.json() == {"error_code": "INVALID_INPUT", "message": "Invalid request"}


@pytest.mark.asyncio
async def test_public_onboard_updates_only_missing_fields(async_client, test_db_session):
    # Seed active assessment package used by B2C onboarding.
    await test_db_session.execute(
        text("INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES (1, 'PK1', 'Package', 'active')")
    )
    # Seed required diagnostic package used by B2C onboarding.
    await test_db_session.execute(
        text("INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) VALUES (1, 'REF1', 'Diag Package', 'active')")
    )
    await test_db_session.commit()
    # Create a user with first_name already set, last_name missing.
    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, first_name, last_name, age, phone, email, status) VALUES (2001, 'Existing', NULL, 30, '2222222222', 'ex@example.com', 'active')"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 30,
        "first_name": "NewFirst",
        "last_name": "NewLast",
        "email": "ex@example.com",
        "phone": "2222222222",
        "city": "Pune",
    }

    payload["blood_collection_date"] = "2026-02-01"
    payload["blood_collection_time_slot"] = "10:00"

    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 200

    result = await test_db_session.execute(
        text("SELECT first_name, last_name, city FROM users WHERE user_id = 2001")
    )
    row = result.first()
    assert row.first_name == "Existing"  # not overwritten
    assert row.last_name == "NewLast"  # filled
    assert row.city == "Pune"  # filled


@pytest.mark.asyncio
async def test_public_onboard_creates_engagement_time_slot_and_assessment_instance(async_client, test_db_session):
    # Seed active assessment package used by B2C onboarding.
    await test_db_session.execute(
        text("INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES (1, 'PK1', 'Package', 'active')")
    )
    # Seed required diagnostic package used by B2C onboarding.
    await test_db_session.execute(
        text("INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) VALUES (1, 'REF1', 'Diag Package', 'active')")
    )
    await test_db_session.commit()

    payload = {
        "age": 30,
        "first_name": "C",
        "last_name": "D",
        "email": "cd@example.com",
        "phone": "3333333333",
        "city": "Delhi",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "10:00",
        "referred_by": "",
    }

    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 200

    data = response.json()["data"]
    assert data["is_participant"] is True
    assert data["engagement_id"] is not None
    assert data["time_slot_id"] is not None

    engagement_id = data["engagement_id"]

    engagement_row = (
        await test_db_session.execute(
            text(
                "SELECT engagement_type, diagnostic_package_id, participant_count, city, start_date, end_date "
                "FROM engagements WHERE engagement_id = :eid"
            ),
            {"eid": engagement_id},
        )
    ).first()

    assert engagement_row.engagement_type == "healthcamp"
    assert engagement_row.diagnostic_package_id == 1
    assert engagement_row.participant_count == 0
    assert engagement_row.city == "Delhi"
    assert str(engagement_row.start_date) == "2026-02-01"
    assert str(engagement_row.end_date) == "2026-02-01"

    slot_row = (
        await test_db_session.execute(
            text(
                "SELECT engagement_id, user_id, engagement_date, slot_start_time FROM engagement_time_slots WHERE time_slot_id = :tid"
            ),
            {"tid": data["time_slot_id"]},
        )
    ).first()

    assert slot_row.engagement_id == engagement_id
    assert str(slot_row.engagement_date) == "2026-02-01"
    assert str(slot_row.slot_start_time)[:5] == "10:00"

    instance_row = (
        await test_db_session.execute(
            text(
                "SELECT user_id, engagement_id, package_id, status, assigned_at FROM assessment_instances WHERE user_id = :uid AND engagement_id = :eid"
            ),
            {"uid": data["user_id"], "eid": engagement_id},
        )
    ).first()

    assert instance_row.user_id == data["user_id"]
    assert instance_row.engagement_id == engagement_id
    assert instance_row.package_id == 1
    assert (instance_row.status or "").lower() == "active"
    assert instance_row.assigned_at is not None


@pytest.mark.asyncio
async def test_engagement_onboard_attaches_by_engagement_code(async_client, test_db_session):
    """Backwards compatible path param flow.

    If payload.referred_by is missing, the path param engagement_code is used.
    The engagement_code is also stored in users.referred_by.
    """
    # Seed active assessment package used by this engagement.
    await test_db_session.execute(
        text("INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES (1, 'PK1', 'Package', 'active')")
    )
    await test_db_session.execute(
        text("INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) VALUES (1, 'REF1', 'Diag Package', 'active')")
    )

    # Prepare an existing engagement with participant_count=0
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status, participant_count) "
            "VALUES (3001, 'Camp', 'ENG12345', 'healthcamp', 1, 1, 'BLR', 20, '2026-02-01', '2026-02-01', 'active', 0)"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 30,
        "first_name": "E",
        "last_name": "F",
        "phone": "4444444444",
        "email": "ef@example.com",
        "city": "BLR",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "11:00",
    }

    response = await async_client.post("/users/code/ENG12345/onboard", json=payload)
    assert response.status_code == 200

    data = response.json()["data"]
    assert data["engagement_id"] == 3001

    engagement_row = (
        await test_db_session.execute(
            text("SELECT participant_count FROM engagements WHERE engagement_id = 3001")
        )
    ).first()
    assert engagement_row.participant_count == 0

    instance_row = (
        await test_db_session.execute(
            text(
                "SELECT user_id, engagement_id, package_id, status, assigned_at FROM assessment_instances WHERE engagement_id = 3001"
            )
        )
    ).first()

    assert instance_row.user_id == data["user_id"]
    assert instance_row.engagement_id == 3001
    assert instance_row.package_id == 1
    assert (instance_row.status or "").lower() == "active"
    assert instance_row.assigned_at is not None

    user_row = (
        await test_db_session.execute(
            text("SELECT referred_by FROM users WHERE user_id = :uid"),
            {"uid": data["user_id"]},
        )
    ).first()
    assert user_row.referred_by == "ENG12345"


@pytest.mark.asyncio
async def test_engagement_onboard_prefers_payload_referred_by(async_client, test_db_session):
    # Seed active assessment package used by engagements.
    await test_db_session.execute(
        text("INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES (1, 'PK1', 'Package', 'active')")
    )
    await test_db_session.execute(
        text("INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) VALUES (1, 'REF1', 'Diag Package', 'active')")
    )

    # Two engagements.
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status, participant_count) "
            "VALUES (3201, 'Camp-A', 'ENGA', 'healthcamp', 1, 1, 'BLR', 20, '2026-02-01', '2026-02-01', 'active', 0)"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status, participant_count) "
            "VALUES (3202, 'Camp-B', 'ENGB', 'healthcamp', 1, 1, 'BLR', 20, '2026-02-01', '2026-02-01', 'active', 0)"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 30,
        "first_name": "Payload",
        "phone": "6666666666",
        "city": "BLR",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "11:00",
        "referred_by": "ENGB",
    }

    response = await async_client.post("/users/code/ENGA/onboard", json=payload)
    assert response.status_code == 200

    data = response.json()["data"]
    assert data["engagement_id"] == 3202

    user_row = (
        await test_db_session.execute(
            text("SELECT referred_by FROM users WHERE user_id = :uid"),
            {"uid": data["user_id"]},
        )
    ).first()
    assert user_row.referred_by == "ENGB"


@pytest.mark.asyncio
async def test_engagement_onboard_requires_active_engagement(async_client, test_db_session):
    await test_db_session.execute(
        text("INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES (1, 'PK1', 'Package', 'active')")
    )
    await test_db_session.execute(
        text("INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) VALUES (1, 'REF1', 'Diag Package', 'active')")
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status, participant_count) "
            "VALUES (3101, 'Camp', 'ENGINACT', 'healthcamp', 1, 1, 'BLR', 20, '2026-02-01', '2026-02-01', 'inactive', 0)"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 30,
        "first_name": "G",
        "phone": "5555555555",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "09:00",
        "city": "Hyd",
    }

    response = await async_client.post("/users/code/ENGINACT/onboard", json=payload)
    assert response.status_code == 422
    assert response.json() == {"error_code": "INVALID_STATE", "message": "Engagement is no longer active"}


@pytest.mark.asyncio
async def test_public_onboard_uses_platform_settings_package_ids(async_client, test_db_session):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES "
            "(1, 'PK1', 'Package 1', 'active'), (2, 'PK2', 'Package 2', 'active')"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) VALUES "
            "(1, 'REF1', 'Diag 1', 'active'), (2, 'REF2', 'Diag 2', 'active')"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings (settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id) "
            "VALUES (1, 2, 2)"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 30,
        "first_name": "B2C",
        "last_name": "Cfg",
        "email": "b2c_cfg@example.com",
        "phone": "4444444444",
        "city": "Chennai",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "10:00",
        "referred_by": "",
    }

    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 200
    data = response.json()["data"]
    engagement_id = data["engagement_id"]

    engagement_row = (
        await test_db_session.execute(
            text(
                "SELECT assessment_package_id, diagnostic_package_id FROM engagements WHERE engagement_id = :eid"
            ),
            {"eid": engagement_id},
        )
    ).first()

    assert engagement_row.assessment_package_id == 2
    assert engagement_row.diagnostic_package_id == 2

    instance_row = (
        await test_db_session.execute(
            text(
                "SELECT package_id FROM assessment_instances WHERE user_id = :uid AND engagement_id = :eid"
            ),
            {"uid": data["user_id"], "eid": engagement_id},
        )
    ).first()
    assert instance_row.package_id == 2


@pytest.mark.asyncio
async def test_public_onboard_fails_when_fallback_packages_inactive(async_client, test_db_session):
    """With no platform_settings row, defaults 1/1 must be active."""
    await test_db_session.execute(
        text("INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES (1, 'PK1', 'Package', 'inactive')")
    )
    await test_db_session.execute(
        text("INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) VALUES (1, 'REF1', 'Diag', 'active')")
    )
    await test_db_session.commit()

    payload = {
        "age": 30,
        "first_name": "X",
        "phone": "4444444445",
        "city": "Goa",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "10:00",
    }

    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 422
    assert response.json()["error_code"] == "INVALID_B2C_ASSESSMENT_PACKAGE"
