from __future__ import annotations

import json
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
async def test_public_onboard_requires_phone_and_age_without_user_id(async_client, test_db_session):
    payload = {
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "10:00",
        "engagement_type": "bio_ai",
    }
    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 400
    assert response.json()["error_code"] == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_public_onboard_by_user_id_skips_personal_fields(async_client, test_db_session):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, first_name, last_name, age, phone, email, city, address, pin_code, "
            "state, country, is_participant, status) "
            "VALUES (2101, 'Existing', 'User', 35, '9988776655', 'exist@example.com', 'Pune', "
            "'12 Main', '411001', 'MH', 'IN', false, 'active')"
        )
    )
    await test_db_session.commit()

    payload = {
        "user_id": 2101,
        "engagement_type": "bio_ai",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "10:00",
        "participant_blood_group": "B+",
    }
    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["user_id"] == 2101
    assert data["created"] is False
    assert data["is_participant"] is True
    assert data["engagement_id"] is not None

    user_row = (
        await test_db_session.execute(
            text("SELECT is_participant, first_name, city FROM users WHERE user_id = 2101")
        )
    ).first()
    assert user_row.is_participant is True
    assert user_row.first_name == "Existing"

    engagement_row = (
        await test_db_session.execute(
            text("SELECT city, address FROM engagements WHERE engagement_id = :eid"),
            {"eid": data["engagement_id"]},
        )
    ).first()
    assert engagement_row.city == "Pune"
    assert engagement_row.address == "12 Main"


@pytest.mark.asyncio
async def test_public_onboard_by_unknown_user_id_returns_404(async_client, test_db_session):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.commit()

    payload = {
        "user_id": 999999,
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "10:00",
    }
    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 404
    assert response.json()["error_code"] == "USER_NOT_FOUND"


@pytest.mark.asyncio
async def test_public_onboard_updates_only_missing_fields(async_client, test_db_session):
    # Seed active assessment package used by B2C onboarding.
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    # Seed required diagnostic package used by B2C onboarding.
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
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
async def test_public_onboard_creates_engagement_participant_and_assessment_instance(async_client, test_db_session):
    # Seed active assessment package used by B2C onboarding.
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    # Seed required diagnostic package used by B2C onboarding.
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
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
        "participant_department": "Engineering",
        "participant_blood_group": "O+",
        "referred_by": "",
    }

    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 200

    data = response.json()["data"]
    assert data["is_participant"] is True
    assert data["engagement_id"] is not None
    assert data["engagement_participant_id"] is not None

    engagement_id = data["engagement_id"]

    engagement_row = (
        await test_db_session.execute(
            text(
                "SELECT engagement_type, diagnostic_package_id, city, start_date, end_date "
                "FROM engagements WHERE engagement_id = :eid"
            ),
            {"eid": engagement_id},
        )
    ).first()

    et = engagement_row.engagement_type
    assert (getattr(et, "value", et) == "bio_ai")
    assert engagement_row.diagnostic_package_id == 6  # db.seed platform_settings B2C default (active diagnostic)
    assert engagement_row.city == "Delhi"
    assert str(engagement_row.start_date) == "2026-02-01"
    assert str(engagement_row.end_date) == "2026-02-01"

    slot_row = (
        await test_db_session.execute(
            text(
                "SELECT engagement_id, user_id, engagement_date, slot_start_time, participant_department, participant_blood_group, is_profile_created_on_metsights "
                "FROM engagement_participants WHERE engagement_participant_id = :tid"
            ),
            {"tid": data["engagement_participant_id"]},
        )
    ).first()

    assert slot_row.engagement_id == engagement_id
    assert str(slot_row.engagement_date) == "2026-02-01"
    assert str(slot_row.slot_start_time)[:5] == "10:00"
    assert slot_row.participant_department == "Engineering"
    assert slot_row.participant_blood_group == "O+"
    assert slot_row.is_profile_created_on_metsights is False

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
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )

    # Prepare an existing engagement with participant_count=0
    await test_db_session.execute(
        text(
            "INSERT INTO organizations (organization_id, name, status, departments) "
            "VALUES (8001, 'Camp Org', 'active', "
            "'[{\"department\": \"HR\", \"slug\": \"hr\"}]'::json)"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, organization_id, engagement_type, assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status, participant_count) "
            "VALUES (3001, 'Camp', 'ENG12345', 8001, 'bio_ai', 1, 1, 'BLR', 20, '2026-02-01', '2026-02-01', 'running')"
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
        "participant_department": "hr",
        "participant_blood_group": "B+",
    }

    response = await async_client.post("/users/code/ENG12345/onboard", json=payload)
    assert response.status_code == 200

    data = response.json()["data"]
    assert data["engagement_id"] == 3001

    slot_row = (
        await test_db_session.execute(
            text(
                "SELECT participant_department, participant_blood_group, is_profile_created_on_metsights "
                "FROM engagement_participants WHERE engagement_participant_id = :pid"
            ),
            {"pid": data["engagement_participant_id"]},
        )
    ).first()
    assert slot_row.participant_department == "hr"
    assert slot_row.participant_blood_group == "B+"
    assert slot_row.is_profile_created_on_metsights is False

    participant_count = (
        await test_db_session.execute(
            text(
                "SELECT COUNT(DISTINCT user_id) FROM engagement_participants WHERE engagement_id = 3001"
            )
        )
    ).scalar_one()
    assert participant_count == 1

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
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )

    # Two engagements.
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status, participant_count) "
            "VALUES (3201, 'Camp-A', 'ENGA', 'bio_ai', 1, 1, 'BLR', 20, '2026-02-01', '2026-02-01', 'running')"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status, participant_count) "
            "VALUES (3202, 'Camp-B', 'ENGB', 'bio_ai', 1, 1, 'BLR', 20, '2026-02-01', '2026-02-01', 'running')"
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
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status, participant_count) "
            "VALUES (3101, 'Camp', 'ENGINACT', 'bio_ai', 1, 1, 'BLR', 20, '2026-02-01', '2026-02-01', 'completed')"
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
    assert response.json() == {"error_code": "INVALID_STATE", "message": "Engagement is not running"}


@pytest.mark.asyncio
async def test_public_onboard_uses_platform_settings_package_ids(async_client, test_db_session):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES "
            "(1, 'PK1', 'Package 1', 'active'), (2, 'PK2', 'Package 2', 'active') "
            "ON CONFLICT (package_id) DO UPDATE SET "
            "package_code = EXCLUDED.package_code, display_name = EXCLUDED.display_name, status = EXCLUDED.status"
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
    # Per-type map: bio_ai uses packages 2/2 (used when engagement_type omitted).
    by_type = {
        "bio_ai": {
            "assessment_package_id": 2,
            "diagnostic_package_id": 2,
            "blood_collection_type": "home_collection",
            "create_profile_on_metsights": False,
            "enroll_for_fitprint_full": False,
        },
        "blood_test": {
            "assessment_package_id": 1,
            "diagnostic_package_id": 1,
            "blood_collection_type": None,
            "create_profile_on_metsights": True,
            "enroll_for_fitprint_full": False,
        },
        "consultation": {
            "assessment_package_id": 1,
            "diagnostic_package_id": 1,
            "blood_collection_type": None,
            "create_profile_on_metsights": True,
            "enroll_for_fitprint_full": False,
        },
        "blood_test_with_consultation": {
            "assessment_package_id": 1,
            "diagnostic_package_id": 1,
            "blood_collection_type": None,
            "create_profile_on_metsights": True,
            "enroll_for_fitprint_full": False,
        },
        "bio_ai_with_consultation": {
            "assessment_package_id": 1,
            "diagnostic_package_id": 1,
            "blood_collection_type": None,
            "create_profile_on_metsights": True,
            "enroll_for_fitprint_full": False,
        },
    }

    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings "
            "(settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id, "
            "b2c_default_engagement_type, b2c_default_blood_collection_type, "
            "b2c_default_create_profile_on_metsights, b2c_default_enroll_for_fitprint_full, "
            "b2c_onboarding_by_engagement_type) "
            "VALUES (1, 2, 2, 'bio_ai', 'home_collection', false, false, CAST(:by_type AS jsonb)) "
            "ON CONFLICT (settings_id) DO UPDATE SET "
            "b2c_default_assessment_package_id = EXCLUDED.b2c_default_assessment_package_id, "
            "b2c_default_diagnostic_package_id = EXCLUDED.b2c_default_diagnostic_package_id, "
            "b2c_default_engagement_type = EXCLUDED.b2c_default_engagement_type, "
            "b2c_default_blood_collection_type = EXCLUDED.b2c_default_blood_collection_type, "
            "b2c_default_create_profile_on_metsights = EXCLUDED.b2c_default_create_profile_on_metsights, "
            "b2c_default_enroll_for_fitprint_full = EXCLUDED.b2c_default_enroll_for_fitprint_full, "
            "b2c_onboarding_by_engagement_type = EXCLUDED.b2c_onboarding_by_engagement_type"
        ),
        {"by_type": json.dumps(by_type)},
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
                "SELECT assessment_package_id, diagnostic_package_id, engagement_type, blood_collection_type, "
                "create_profile_on_metsights, enroll_for_fitprint_full "
                "FROM engagements WHERE engagement_id = :eid"
            ),
            {"eid": engagement_id},
        )
    ).first()

    assert engagement_row.assessment_package_id == 2
    assert engagement_row.diagnostic_package_id == 2
    bio_ai_type_id = (
        await test_db_session.execute(
            text("SELECT id FROM engagement_types WHERE code = 'bio_ai'")
        )
    ).scalar_one()
    assert engagement_row.engagement_type == bio_ai_type_id
    assert getattr(engagement_row.blood_collection_type, "value", engagement_row.blood_collection_type) == "home_collection"
    assert engagement_row.create_profile_on_metsights is False
    assert engagement_row.enroll_for_fitprint_full is False

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
async def test_public_onboard_uses_engagement_type_and_its_defaults(async_client, test_db_session):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES "
            "(1, 'PK1', 'Package 1', 'active'), (2, 'PK2', 'Package 2', 'active') "
            "ON CONFLICT (package_id) DO UPDATE SET "
            "package_code = EXCLUDED.package_code, display_name = EXCLUDED.display_name, status = EXCLUDED.status"
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
    by_type = {
        "bio_ai": {
            "assessment_package_id": 1,
            "diagnostic_package_id": 1,
            "blood_collection_type": None,
            "create_profile_on_metsights": True,
            "enroll_for_fitprint_full": False,
        },
        "blood_test": {
            "assessment_package_id": 2,
            "diagnostic_package_id": 2,
            "blood_collection_type": "camp_collection",
            "create_profile_on_metsights": False,
            "enroll_for_fitprint_full": False,
        },
        "consultation": {
            "assessment_package_id": 1,
            "diagnostic_package_id": 1,
            "blood_collection_type": None,
            "create_profile_on_metsights": True,
            "enroll_for_fitprint_full": False,
        },
        "blood_test_with_consultation": {
            "assessment_package_id": 1,
            "diagnostic_package_id": 1,
            "blood_collection_type": None,
            "create_profile_on_metsights": True,
            "enroll_for_fitprint_full": False,
        },
        "bio_ai_with_consultation": {
            "assessment_package_id": 1,
            "diagnostic_package_id": 1,
            "blood_collection_type": None,
            "create_profile_on_metsights": True,
            "enroll_for_fitprint_full": False,
        },
    }
    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings "
            "(settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id, "
            "b2c_onboarding_by_engagement_type) "
            "VALUES (1, 1, 1, CAST(:by_type AS jsonb)) "
            "ON CONFLICT (settings_id) DO UPDATE SET "
            "b2c_default_assessment_package_id = EXCLUDED.b2c_default_assessment_package_id, "
            "b2c_default_diagnostic_package_id = EXCLUDED.b2c_default_diagnostic_package_id, "
            "b2c_onboarding_by_engagement_type = EXCLUDED.b2c_onboarding_by_engagement_type"
        ),
        {"by_type": json.dumps(by_type)},
    )
    await test_db_session.commit()

    payload = {
        "age": 30,
        "first_name": "Blood",
        "last_name": "Test",
        "email": "blood_type@example.com",
        "phone": "4444444455",
        "city": "Chennai",
        "engagement_type": "blood_test",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "10:00",
    }

    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 200
    data = response.json()["data"]

    engagement_row = (
        await test_db_session.execute(
            text(
                "SELECT assessment_package_id, diagnostic_package_id, engagement_type, blood_collection_type, "
                "create_profile_on_metsights "
                "FROM engagements WHERE engagement_id = :eid"
            ),
            {"eid": data["engagement_id"]},
        )
    ).first()

    assert engagement_row.assessment_package_id == 2
    assert engagement_row.diagnostic_package_id == 2
    blood_test_type_id = (
        await test_db_session.execute(
            text("SELECT id FROM engagement_types WHERE code = 'blood_test'")
        )
    ).scalar_one()
    assert engagement_row.engagement_type == blood_test_type_id
    assert getattr(engagement_row.blood_collection_type, "value", engagement_row.blood_collection_type) == "camp_collection"
    assert engagement_row.create_profile_on_metsights is False


@pytest.mark.asyncio
async def test_public_onboard_rejects_unknown_engagement_type(async_client, test_db_session):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 30,
        "first_name": "Bad",
        "phone": "4444444499",
        "city": "Pune",
        "engagement_type": "not_a_real_type",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "10:00",
    }

    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 422
    assert response.json()["error_code"] == "INVALID_ENGAGEMENT_TYPE"


@pytest.mark.asyncio
async def test_public_onboard_rejects_inactive_engagement_type(async_client, test_db_session):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_types (code, display_name, is_active) "
            "VALUES ('inactive_onboard_type', 'Inactive', false) "
            "ON CONFLICT (code) DO UPDATE SET is_active = false"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 30,
        "first_name": "Inactive",
        "phone": "4444444488",
        "city": "Pune",
        "engagement_type": "inactive_onboard_type",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "10:00",
    }

    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 422
    assert response.json()["error_code"] == "INVALID_ENGAGEMENT_TYPE"


@pytest.mark.asyncio
async def test_public_onboard_fails_when_fallback_packages_inactive(async_client, test_db_session):
    """With no platform_settings row, defaults 1/1 must be active."""
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 'inactive') "
            "ON CONFLICT (package_id) DO UPDATE SET status = EXCLUDED.status"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
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


@pytest.mark.asyncio
async def test_engagement_onboard_rejects_invalid_department_slug(async_client, test_db_session):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO organizations (organization_id, name, status, departments) "
            "VALUES (8101, 'Dept Org', 'active', "
            "'[{\"department\": \"Sales\", \"slug\": \"sales\"}]'::json)"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, organization_id, engagement_type, assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status, participant_count) "
            "VALUES (3101, 'Dept Camp', 'DEPT01', 8101, 'bio_ai', 1, 1, 'BLR', 20, '2026-02-01', '2026-02-01', 'running')"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 30,
        "first_name": "Bad",
        "phone": "5555555555",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "11:00",
        "participant_department": "marketing",
    }

    response = await async_client.post("/users/code/DEPT01/onboard", json=payload)
    assert response.status_code == 400
    assert response.json()["error_code"] == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_engagement_onboard_rejects_department_without_organization(async_client, test_db_session):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status, participant_count) "
            "VALUES (3102, 'B2C Camp', 'B2CDEPT', 'bio_ai', 1, 1, 'BLR', 20, '2026-02-01', '2026-02-01', 'running')"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 30,
        "first_name": "NoOrg",
        "phone": "5555555556",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "11:00",
        "participant_department": "sales",
    }

    response = await async_client.post("/users/code/B2CDEPT/onboard", json=payload)
    assert response.status_code == 400
    assert response.json()["error_code"] == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_engagement_onboard_me_returns_tokens_and_logs_in(async_client, test_db_session):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO organizations (organization_id, name, status, departments) "
            "VALUES (8101, 'Onboard Me Org', 'active', "
            "'[{\"department\": \"HR\", \"slug\": \"hr\"}]'::json)"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, organization_id, "
            "engagement_type, assessment_package_id, diagnostic_package_id, city, slot_duration, "
            "start_date, end_date, status) "
            "VALUES (3301, 'Camp Me', 'ENGME01', 8101, 'bio_ai', 1, 1, 'BLR', 20, "
            "'2026-02-01', '2026-02-01', 'running')"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 30,
        "first_name": "Me",
        "last_name": "User",
        "phone": "7777777771",
        "email": "onboard_me@example.com",
        "city": "BLR",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "11:00",
        "participant_department": "hr",
        "participant_blood_group": "A+",
    }

    response = await async_client.post("/users/code/ENGME01/onboard/me", json=payload)
    assert response.status_code == 200

    data = response.json()["data"]
    assert "user_id" in data
    tokens = data["tokens"]
    assert tokens["token_type"] == "bearer"
    assert isinstance(tokens["access_token"], str) and tokens["access_token"]
    assert isinstance(tokens["refresh_token"], str) and tokens["refresh_token"]

    user_id = data["user_id"]

    participant_count = (
        await test_db_session.execute(
            text(
                "SELECT COUNT(DISTINCT user_id) FROM engagement_participants "
                "WHERE engagement_id = 3301 AND user_id = :uid"
            ),
            {"uid": user_id},
        )
    ).scalar_one()
    assert participant_count == 1

    instance_row = (
        await test_db_session.execute(
            text(
                "SELECT user_id, engagement_id, package_id, status FROM assessment_instances "
                "WHERE engagement_id = 3301 AND user_id = :uid"
            ),
            {"uid": user_id},
        )
    ).first()
    assert instance_row is not None
    assert instance_row.package_id == 1
    assert (instance_row.status or "").lower() == "active"

    token_count = (
        await test_db_session.execute(
            text("SELECT COUNT(*) FROM auth_tokens WHERE user_id = :uid"),
            {"uid": user_id},
        )
    ).scalar_one()
    assert token_count == 1

    me_response = await async_client.get(
        "/users/me",
        headers={"Authorization": f"Bearer {tokens['access_token']}"},
    )
    assert me_response.status_code == 200
    assert me_response.json()["data"]["user_id"] == user_id


@pytest.mark.asyncio
async def test_engagement_onboard_me_invalid_engagement_code_issues_no_tokens(
    async_client, test_db_session
):
    payload = {
        "age": 30,
        "first_name": "Bad",
        "phone": "7777777772",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "11:00",
        "city": "BLR",
    }

    response = await async_client.post("/users/code/NOSUCHCODE/onboard/me", json=payload)
    assert response.status_code == 400
    assert response.json()["error_code"] == "INVALID_INPUT"

    token_count = (await test_db_session.execute(text("SELECT COUNT(*) FROM auth_tokens"))).scalar_one()
    assert token_count == 0


@pytest.mark.asyncio
async def test_engagement_onboard_me_inactive_engagement_issues_no_tokens(
    async_client, test_db_session
):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, "
            "assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, "
            "status) "
            "VALUES (3302, 'Camp Done', 'ENGMEINACT', 'bio_ai', 1, 1, 'BLR', 20, "
            "'2026-02-01', '2026-02-01', 'completed')"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 30,
        "first_name": "Inactive",
        "phone": "7777777773",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "09:00",
        "city": "Hyd",
    }

    response = await async_client.post("/users/code/ENGMEINACT/onboard/me", json=payload)
    assert response.status_code == 422
    assert response.json()["error_code"] == "INVALID_STATE"

    token_count = (await test_db_session.execute(text("SELECT COUNT(*) FROM auth_tokens"))).scalar_one()
    assert token_count == 0
