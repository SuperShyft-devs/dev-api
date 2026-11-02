from __future__ import annotations

import json
import re
from datetime import date

import pytest
from sqlalchemy import text

from core.config import settings


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
    body = response.json()
    assert body["error_code"] == "INVALID_INPUT"
    assert "blood_collection_date" in body["message"]
    assert "Field required" in body["message"]


@pytest.mark.asyncio
async def test_public_onboard_vifc_allows_missing_blood_fields(async_client, test_db_session):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 1) ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_types (code, display_name, is_active) "
            "VALUES ('vifc', 'Aurae - Face Scan', true) "
            "ON CONFLICT (code) DO UPDATE SET display_name = EXCLUDED.display_name, is_active = true"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 22,
        "first_name": "Vifc",
        "last_name": "User",
        "email": "vifc.user@example.com",
        "phone": "8762830757",
        "gender": "male",
        "city": "Mumbai",
        "engagement_type": "vifc",
    }

    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 200
    data = response.json()["data"]

    participant_row = (
        await test_db_session.execute(
            text(
                "SELECT engagement_date, slot_start_time "
                "FROM engagement_participants WHERE engagement_participant_id = :epid"
            ),
            {"epid": data["engagement_participant_id"]},
        )
    ).first()
    assert participant_row.engagement_date is None
    assert participant_row.slot_start_time is None


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
            "VALUES (1, 'PK1', 'Package', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 1) ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, first_name, last_name, age, phone, email, city, address, pin_code, "
            "state, country, is_participant, status) "
            "VALUES (2101, 'Existing', 'User', 35, '9988776655', 'exist@example.com', 'Pune', "
            "'12 Main', '411001', 'MH', 'IN', false, 1)"
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
            "VALUES (1, 'PK1', 'Package', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 1) ON CONFLICT (diagnostic_package_id) DO NOTHING"
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
            "VALUES (1, 'PK1', 'Package', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    # Seed required diagnostic package used by B2C onboarding.
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 1) ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.commit()
    # Create a user with first_name already set, last_name missing.
    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, first_name, last_name, age, phone, email, status) VALUES (2001, 'Existing', NULL, 30, '2222222222', 'ex@example.com', 1)"
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
            "VALUES (1, 'PK1', 'Package', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    # Seed required diagnostic package used by B2C onboarding.
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 1) ON CONFLICT (diagnostic_package_id) DO NOTHING"
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
                "SELECT engagement_id, user_id, engagement_date, slot_start_time, participant_department, participant_blood_group "
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
            "VALUES (1, 'PK1', 'Package', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 1) ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )

    # Prepare an existing engagement with participant_count=0
    await test_db_session.execute(
        text(
            "INSERT INTO organizations (organization_id, name, status, departments) "
            "VALUES (8001, 'Camp Org', 1, "
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
                "SELECT participant_department, participant_blood_group "
                "FROM engagement_participants WHERE engagement_participant_id = :pid"
            ),
            {"pid": data["engagement_participant_id"]},
        )
    ).first()
    assert slot_row.participant_department == "hr"
    assert slot_row.participant_blood_group == "B+"

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
async def test_engagement_onboard_reuses_user_when_phone_format_differs(async_client, test_db_session):
    """10-digit payload must attach to an existing +91 primary, not create a second account."""
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 1) ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO organizations (organization_id, name, status, departments) "
            "VALUES (8011, 'Phone Format Org', 1, "
            "'[{\"department\": \"HR\", \"slug\": \"hr\"}]'::json)"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, organization_id, "
            "engagement_type, assessment_package_id, diagnostic_package_id, city, slot_duration, "
            "start_date, end_date, status, participant_count) "
            "VALUES (3011, 'Camp', 'ENGPLUS91', 8011, 'bio_ai', 1, 1, 'BLR', 20, "
            "'2026-02-01', '2026-02-01', 'running', 0)"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, age, first_name, last_name, phone, email, status, is_participant) "
            "VALUES (88101, 30, 'Format', 'User', '+917000008899', 'format88101@example.com', 1, true)"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 30,
        "first_name": "Format",
        "last_name": "User",
        "phone": "7000008899",
        "email": "format88101-onboard@example.com",
        "city": "BLR",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "11:00",
        "participant_department": "hr",
        "participant_blood_group": "B+",
    }
    response = await async_client.post("/users/code/ENGPLUS91/onboard", json=payload)
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert data["created"] is False
    assert data["user_id"] == 88101

    count = (
        await test_db_session.execute(
            text(
                "SELECT count(*) FROM users WHERE right(regexp_replace(phone, '[^0-9]', '', 'g'), 10) = '7000008899'"
            )
        )
    ).scalar_one()
    assert count == 1


@pytest.mark.asyncio
async def test_engagement_onboard_prefers_payload_referred_by(async_client, test_db_session):
    # Seed active assessment package used by engagements.
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 1) ON CONFLICT (diagnostic_package_id) DO NOTHING"
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
async def test_engagement_onboard_overwrites_existing_address_fields(async_client, test_db_session):
    """B2B onboard overwrites address/city/state/country/pincode even when already set."""
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 1) ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    from datetime import date

    from modules.engagements.models import BloodCollectionType, Engagement

    test_db_session.add(
        Engagement(
            engagement_id=3301,
            engagement_name="Camp-Addr",
            engagement_code="ENGADDR",
            engagement_type=1,
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="running",
            blood_collection_type=BloodCollectionType.home_collection,
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, first_name, age, phone, email, status, address, pin_code, city, state, country) "
            "VALUES (2101, 'Existing', 30, '7777777777', 'addr@example.com', 1, "
            "'Old Street', '110001', 'Delhi', 'DL', 'India')"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 30,
        "first_name": "Existing",
        "phone": "7777777777",
        "email": "addr@example.com",
        "address": "New Street 42",
        "pincode": "560001",
        "city": "Bengaluru",
        "state": "KA",
        "country": "IN",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "11:00",
    }

    response = await async_client.post("/users/code/ENGADDR/onboard", json=payload)
    assert response.status_code == 200

    row = (
        await test_db_session.execute(
            text(
                "SELECT address, pin_code, city, state, country FROM users WHERE user_id = 2101"
            )
        )
    ).first()
    assert row.address == "New Street 42"
    assert row.pin_code == "560001"
    assert row.city == "Bengaluru"
    assert row.state == "KA"
    assert row.country == "IN"

    data = response.json()["data"]
    participant_row = (
        await test_db_session.execute(
            text(
                "SELECT address, pincode, city, state, country FROM engagement_participants "
                "WHERE engagement_participant_id = :epid"
            ),
            {"epid": data["engagement_participant_id"]},
        )
    ).first()
    assert participant_row.address == "New Street 42"
    assert participant_row.pincode == "560001"
    assert participant_row.city == "Bengaluru"
    assert participant_row.state == "KA"
    assert participant_row.country == "IN"

    engagement_row = (
        await test_db_session.execute(
            text("SELECT address, pincode, city FROM engagements WHERE engagement_id = 3301")
        )
    ).first()
    assert engagement_row.address is None
    assert engagement_row.pincode is None
    assert engagement_row.city == "BLR"


@pytest.mark.asyncio
async def test_public_onboard_persists_address_on_user_engagement_and_participant(
    async_client, test_db_session
):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 1) ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings "
            "(settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id, "
            "b2c_default_engagement_type, b2c_default_blood_collection_type) "
            "VALUES (1, 1, 1, 'bio_ai', 'home_collection') "
            "ON CONFLICT (settings_id) DO UPDATE SET "
            "b2c_default_blood_collection_type = EXCLUDED.b2c_default_blood_collection_type"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 31,
        "first_name": "Addr",
        "last_name": "User",
        "email": "addruser@example.com",
        "phone": "8888888881",
        "address": "221B Baker Street",
        "pincode": "411001",
        "city": "Pune",
        "state": "MH",
        "country": "IN",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "10:00",
    }

    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 200
    data = response.json()["data"]

    user_row = (
        await test_db_session.execute(
            text(
                "SELECT address, pin_code, city, state, country FROM users WHERE user_id = :uid"
            ),
            {"uid": data["user_id"]},
        )
    ).first()
    assert user_row.address == "221B Baker Street"
    assert user_row.pin_code == "411001"
    assert user_row.city == "Pune"

    engagement_row = (
        await test_db_session.execute(
            text(
                "SELECT address, pincode, city, state, country, organization_id "
                "FROM engagements WHERE engagement_id = :eid"
            ),
            {"eid": data["engagement_id"]},
        )
    ).first()
    assert engagement_row.organization_id is None
    assert engagement_row.address is None
    assert engagement_row.pincode is None
    assert engagement_row.city == "Pune"

    participant_row = (
        await test_db_session.execute(
            text(
                "SELECT address, pincode, city, state, country FROM engagement_participants "
                "WHERE engagement_participant_id = :epid"
            ),
            {"epid": data["engagement_participant_id"]},
        )
    ).first()
    assert participant_row.address == "221B Baker Street"
    assert participant_row.pincode == "411001"
    assert participant_row.city == "Pune"


@pytest.mark.asyncio
async def test_engagement_onboard_overwrites_existing_age_and_email(async_client, test_db_session):
    """B2B onboard overwrites age and email even when already set."""
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 1) ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, "
            "assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, status, participant_count) "
            "VALUES (3302, 'Camp-AgeEmail', 'ENGAGEEM', 'bio_ai', 1, 1, 'BLR', 20, '2026-02-01', '2026-02-01', 'running')"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, first_name, age, phone, email, status) "
            "VALUES (2102, 'Existing', 28, '7777777778', 'old@example.com', 1)"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 35,
        "first_name": "Existing",
        "phone": "7777777778",
        "email": "new@example.com",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "11:00",
    }

    response = await async_client.post("/users/code/ENGAGEEM/onboard", json=payload)
    assert response.status_code == 200

    row = (
        await test_db_session.execute(
            text("SELECT age, email FROM users WHERE user_id = 2102")
        )
    ).first()
    assert row.age == 35
    assert row.email == "new@example.com"


@pytest.mark.asyncio
async def test_engagement_onboard_reclaims_email_from_own_sub_profile(async_client, test_db_session):
    """When email is on a sub-profile of the phone-matched primary, reclaim it and alias the sub."""
    await _seed_onboard_packages(test_db_session)
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_types (code, display_name, is_active) "
            "VALUES ('bio_ai', 'Bio AI', true) "
            "ON CONFLICT (code) DO UPDATE SET is_active = true"
        )
    )
    bio_ai_type_id = (
        await test_db_session.execute(
            text("SELECT id FROM engagement_types WHERE code = 'bio_ai'")
        )
    ).scalar_one()
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, "
            "assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, "
            "status) "
            "VALUES (3393, 'Camp-SubEmail', 'ENGSUBEM', :etype, 1, 1, 'BLR', 20, "
            "'2026-02-01', '2026-02-01', 'running')"
        ),
        {"etype": bio_ai_type_id},
    )
    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, first_name, age, phone, email, status, parent_id, relationship) "
            "VALUES (2103, 'Primary', 40, '7777777779', 'primary-old@example.com', 1, NULL, 'self')"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, first_name, age, phone, email, status, parent_id, relationship) "
            "VALUES (2104, 'Child', 12, '7777777779', 'target@example.com', 1, 2103, 'child')"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 41,
        "first_name": "Primary",
        "phone": "7777777779",
        "email": "target@example.com",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "11:00",
    }

    response = await async_client.post("/users/code/ENGSUBEM/onboard", json=payload)
    assert response.status_code == 200, response.text

    primary = (
        await test_db_session.execute(
            text("SELECT email FROM users WHERE user_id = 2103")
        )
    ).first()
    child = (
        await test_db_session.execute(
            text("SELECT email FROM users WHERE user_id = 2104")
        )
    ).first()
    assert primary.email == "target@example.com"
    assert re.fullmatch(r"target\+\d{4}@example\.com", child.email)


@pytest.mark.asyncio
async def test_engagement_onboard_email_owned_by_unrelated_user_returns_409(async_client, test_db_session):
    """Phone-matched user cannot steal an email owned by an unrelated account."""
    await _seed_onboard_packages(test_db_session)
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_types (code, display_name, is_active) "
            "VALUES ('bio_ai', 'Bio AI', true) "
            "ON CONFLICT (code) DO UPDATE SET is_active = true"
        )
    )
    bio_ai_type_id = (
        await test_db_session.execute(
            text("SELECT id FROM engagement_types WHERE code = 'bio_ai'")
        )
    ).scalar_one()
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, "
            "assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, "
            "status) "
            "VALUES (3394, 'Camp-EmailConflict', 'ENGEMCONF', :etype, 1, 1, 'BLR', 20, "
            "'2026-02-01', '2026-02-01', 'running')"
        ),
        {"etype": bio_ai_type_id},
    )
    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, first_name, age, phone, email, status) "
            "VALUES (2105, 'PhoneUser', 30, '7777777780', 'phone-user@example.com', 1)"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, first_name, age, phone, email, status) "
            "VALUES (2106, 'EmailOwner', 30, '7777777781', 'taken@example.com', 1)"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 31,
        "first_name": "PhoneUser",
        "phone": "7777777780",
        "email": "taken@example.com",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "11:00",
    }

    response = await async_client.post("/users/code/ENGEMCONF/onboard", json=payload)
    assert response.status_code == 409
    body = response.json()
    assert body["error_code"] == "CONFLICT"
    assert "email" in body["message"].lower()

    phone_user = (
        await test_db_session.execute(
            text("SELECT email FROM users WHERE user_id = 2105")
        )
    ).first()
    email_owner = (
        await test_db_session.execute(
            text("SELECT email FROM users WHERE user_id = 2106")
        )
    ).first()
    assert phone_user.email == "phone-user@example.com"
    assert email_owner.email == "taken@example.com"


@pytest.mark.asyncio
async def test_engagement_onboard_requires_active_engagement(async_client, test_db_session):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 1) ON CONFLICT (diagnostic_package_id) DO NOTHING"
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
            "(1, 'PK1', 'Package 1', 1), (2, 'PK2', 'Package 2', 1) "
            "ON CONFLICT (package_id) DO UPDATE SET "
            "package_code = EXCLUDED.package_code, display_name = EXCLUDED.display_name, status = EXCLUDED.status"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) VALUES "
            "(1, 'REF1', 'Diag 1', 1), (2, 'REF2', 'Diag 2', 1) "
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
            "(1, 'PK1', 'Package 1', 1), (2, 'PK2', 'Package 2', 1) "
            "ON CONFLICT (package_id) DO UPDATE SET "
            "package_code = EXCLUDED.package_code, display_name = EXCLUDED.display_name, status = EXCLUDED.status"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) VALUES "
            "(1, 'REF1', 'Diag 1', 1), (2, 'REF2', 'Diag 2', 1) "
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
async def test_public_onboard_diagnostic_package_id_overrides_platform_default(async_client, test_db_session):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES "
            "(1, 'PK1', 'Package 1', 1) "
            "ON CONFLICT (package_id) DO UPDATE SET status = EXCLUDED.status"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) VALUES "
            "(1, 'REF1', 'Diag Default', 1), (2, 'REF2', 'Diag Override', 1) "
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
    }
    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings "
            "(settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id, "
            "b2c_default_engagement_type, b2c_default_blood_collection_type, "
            "b2c_default_create_profile_on_metsights, b2c_default_enroll_for_fitprint_full, "
            "b2c_onboarding_by_engagement_type) "
            "VALUES (1, 1, 1, 'bio_ai', NULL, true, false, CAST(:by_type AS jsonb)) "
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
        "first_name": "Override",
        "phone": "4444444401",
        "city": "Pune",
        "engagement_type": "bio_ai",
        "diagnostic_package_id": 2,
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "10:00",
    }

    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 200
    data = response.json()["data"]

    engagement_row = (
        await test_db_session.execute(
            text(
                "SELECT diagnostic_package_id FROM engagements WHERE engagement_id = :eid"
            ),
            {"eid": data["engagement_id"]},
        )
    ).first()
    assert engagement_row.diagnostic_package_id == 2


@pytest.mark.asyncio
async def test_public_onboard_omitted_diagnostic_package_id_uses_platform_default(
    async_client, test_db_session
):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES "
            "(1, 'PK1', 'Package 1', 1) "
            "ON CONFLICT (package_id) DO UPDATE SET status = EXCLUDED.status"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) VALUES "
            "(1, 'REF1', 'Diag Default', 1), (2, 'REF2', 'Diag Other', 1) "
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
    }
    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings "
            "(settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id, "
            "b2c_default_engagement_type, b2c_default_blood_collection_type, "
            "b2c_default_create_profile_on_metsights, b2c_default_enroll_for_fitprint_full, "
            "b2c_onboarding_by_engagement_type) "
            "VALUES (1, 1, 1, 'bio_ai', NULL, true, false, CAST(:by_type AS jsonb)) "
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
        "first_name": "DefaultPkg",
        "phone": "4444444402",
        "city": "Pune",
        "engagement_type": "bio_ai",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "10:00",
    }

    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 200
    data = response.json()["data"]

    engagement_row = (
        await test_db_session.execute(
            text(
                "SELECT diagnostic_package_id FROM engagements WHERE engagement_id = :eid"
            ),
            {"eid": data["engagement_id"]},
        )
    ).first()
    assert engagement_row.diagnostic_package_id == 1


@pytest.mark.asyncio
async def test_public_onboard_rejects_inactive_diagnostic_package_id(async_client, test_db_session):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES "
            "(1, 'PK1', 'Package 1', 1) "
            "ON CONFLICT (package_id) DO UPDATE SET status = EXCLUDED.status"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) VALUES "
            "(1, 'REF1', 'Diag Default', 1), (99, 'REF99', 'Diag Inactive', 'inactive') "
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
    }
    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings "
            "(settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id, "
            "b2c_default_engagement_type, b2c_default_blood_collection_type, "
            "b2c_default_create_profile_on_metsights, b2c_default_enroll_for_fitprint_full, "
            "b2c_onboarding_by_engagement_type) "
            "VALUES (1, 1, 1, 'bio_ai', NULL, true, false, CAST(:by_type AS jsonb)) "
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
        "first_name": "BadPkg",
        "phone": "4444444403",
        "city": "Pune",
        "engagement_type": "bio_ai",
        "diagnostic_package_id": 99,
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "10:00",
    }

    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 422
    body = response.json()
    assert body["error_code"] == "INVALID_B2C_DIAGNOSTIC_PACKAGE"


@pytest.mark.asyncio
async def test_public_onboard_rejects_unknown_engagement_type(async_client, test_db_session):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 1) ON CONFLICT (diagnostic_package_id) DO NOTHING"
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
            "VALUES (1, 'PK1', 'Package', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 1) ON CONFLICT (diagnostic_package_id) DO NOTHING"
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
            "VALUES (1, 'REF1', 'Diag', 1) ON CONFLICT (diagnostic_package_id) DO NOTHING"
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
            "VALUES (1, 'PK1', 'Package', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 1) ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO organizations (organization_id, name, status, departments) "
            "VALUES (8101, 'Dept Org', 1, "
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
            "VALUES (1, 'PK1', 'Package', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 1) ON CONFLICT (diagnostic_package_id) DO NOTHING"
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
            "VALUES (1, 'PK1', 'Package', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 1) ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO organizations (organization_id, name, status, departments) "
            "VALUES (8101, 'Onboard Me Org', 1, "
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
            "VALUES (1, 'PK1', 'Package', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 1) ON CONFLICT (diagnostic_package_id) DO NOTHING"
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


async def _seed_onboard_packages(test_db_session) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PK1', 'Package', 1) ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 1) ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )


@pytest.mark.asyncio
async def test_engagement_onboard_rejects_same_camp_no_across_engagements(
    async_client, test_db_session
):
    """Same user cannot enroll in two engagements that share camp_no."""
    await _seed_onboard_packages(test_db_session)
    await test_db_session.execute(
        text(
            "INSERT INTO organizations (organization_id, name, status, departments) "
            "VALUES (8801, 'Same Camp Org', 1, "
            "'[{\"department\": \"HR\", \"slug\": \"hr\"}]'::json)"
        )
    )
    # org 8801 + start 2026-02-01 → camp_no 8801010226
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, organization_id, "
            "camp_no, engagement_type, assessment_package_id, diagnostic_package_id, city, slot_duration, "
            "start_date, end_date, status) "
            "VALUES (8801, 'Camp City A', 'CAMPA01', 8801, 8801010226, 'bio_ai', 1, 1, 'BLR', 20, "
            "'2026-02-01', '2026-02-01', 'running')"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, organization_id, "
            "camp_no, engagement_type, assessment_package_id, diagnostic_package_id, city, slot_duration, "
            "start_date, end_date, status) "
            "VALUES (8802, 'Camp City B', 'CAMPB01', 8801, 8801010226, 'bio_ai', 1, 1, 'HYD', 20, "
            "'2026-02-01', '2026-02-01', 'running')"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 30,
        "first_name": "Same",
        "last_name": "Camp",
        "phone": "8801000001",
        "email": "same.camp@example.com",
        "city": "BLR",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "10:00",
        "participant_department": "hr",
    }

    first = await async_client.post("/users/code/CAMPA01/onboard", json=payload)
    assert first.status_code == 200
    assert first.json()["data"]["engagement_id"] == 8801

    second = await async_client.post("/users/code/CAMPB01/onboard", json=payload)
    assert second.status_code == 409
    body = second.json()
    assert body["error_code"] == "ALREADY_ENROLLED_SAME_CAMP"
    assert "camp" in body["message"].lower()


@pytest.mark.asyncio
async def test_engagement_onboard_allows_different_camp_no(
    async_client, test_db_session
):
    """Same user may enroll in engagements with different camp_no values."""
    await _seed_onboard_packages(test_db_session)
    await test_db_session.execute(
        text(
            "INSERT INTO organizations (organization_id, name, status, departments) "
            "VALUES (8802, 'Diff Camp Org', 1, "
            "'[{\"department\": \"HR\", \"slug\": \"hr\"}]'::json)"
        )
    )
    # Different start dates → different camp_no
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, organization_id, "
            "camp_no, engagement_type, assessment_package_id, diagnostic_package_id, city, slot_duration, "
            "start_date, end_date, status) "
            "VALUES (8811, 'Camp Feb', 'DIFFC01', 8802, 8802010226, 'bio_ai', 1, 1, 'BLR', 20, "
            "'2026-02-01', '2026-02-01', 'running')"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, organization_id, "
            "camp_no, engagement_type, assessment_package_id, diagnostic_package_id, city, slot_duration, "
            "start_date, end_date, status) "
            "VALUES (8812, 'Camp Mar', 'DIFFC02', 8802, 8802010326, 'bio_ai', 1, 1, 'BLR', 20, "
            "'2026-03-01', '2026-03-01', 'running')"
        )
    )
    await test_db_session.commit()

    payload_a = {
        "age": 28,
        "first_name": "Diff",
        "last_name": "Camp",
        "phone": "8802000001",
        "email": "diff.camp@example.com",
        "city": "BLR",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "10:00",
        "participant_department": "hr",
    }
    payload_b = {
        **payload_a,
        "blood_collection_date": "2026-03-01",
        "blood_collection_time_slot": "11:00",
    }

    first = await async_client.post("/users/code/DIFFC01/onboard", json=payload_a)
    assert first.status_code == 200
    assert first.json()["data"]["engagement_id"] == 8811

    second = await async_client.post("/users/code/DIFFC02/onboard", json=payload_b)
    assert second.status_code == 200
    assert second.json()["data"]["engagement_id"] == 8812
    assert second.json()["data"]["user_id"] == first.json()["data"]["user_id"]


@pytest.mark.asyncio
async def test_engagement_onboard_allows_b2c_null_camp_across_engagements(
    async_client, test_db_session
):
    """B2C engagements (camp_no null) do not block cross-engagement enroll."""
    await _seed_onboard_packages(test_db_session)
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, "
            "assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, "
            "status) "
            "VALUES (8821, 'B2C-A', 'B2CNULLA', 'bio_ai', 1, 1, 'BLR', 20, "
            "'2026-02-01', '2026-02-01', 'running')"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, "
            "assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, "
            "status) "
            "VALUES (8822, 'B2C-B', 'B2CNULLB', 'bio_ai', 1, 1, 'HYD', 20, "
            "'2026-02-01', '2026-02-01', 'running')"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 32,
        "first_name": "B2C",
        "last_name": "User",
        "phone": "8803000001",
        "email": "b2c.null@example.com",
        "city": "BLR",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "09:00",
    }

    first = await async_client.post("/users/code/B2CNULLA/onboard", json=payload)
    assert first.status_code == 200
    assert first.json()["data"]["engagement_id"] == 8821

    second = await async_client.post("/users/code/B2CNULLB/onboard", json=payload)
    assert second.status_code == 200
    assert second.json()["data"]["engagement_id"] == 8822


@pytest.mark.asyncio
async def test_engagement_onboard_same_engagement_still_already_enrolled(
    async_client, test_db_session
):
    """Re-onboarding the same engagement still returns ALREADY_ENROLLED."""
    await _seed_onboard_packages(test_db_session)
    await test_db_session.execute(
        text(
            "INSERT INTO organizations (organization_id, name, status, departments) "
            "VALUES (8803, 'Dup Eng Org', 1, "
            "'[{\"department\": \"HR\", \"slug\": \"hr\"}]'::json)"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, organization_id, "
            "camp_no, engagement_type, assessment_package_id, diagnostic_package_id, city, slot_duration, "
            "start_date, end_date, status) "
            "VALUES (8831, 'Dup Eng', 'DUPENG01', 8803, 8803010226, 'bio_ai', 1, 1, 'BLR', 20, "
            "'2026-02-01', '2026-02-01', 'running')"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 29,
        "first_name": "Dup",
        "last_name": "Eng",
        "phone": "8804000001",
        "email": "dup.eng@example.com",
        "city": "BLR",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "10:00",
        "participant_department": "hr",
    }

    first = await async_client.post("/users/code/DUPENG01/onboard", json=payload)
    assert first.status_code == 200

    second = await async_client.post("/users/code/DUPENG01/onboard", json=payload)
    assert second.status_code == 409
    assert second.json()["error_code"] == "ALREADY_ENROLLED"


@pytest.mark.asyncio
async def test_engagement_onboard_logs_metsights_integration_sync(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    monkeypatch.setattr(settings, "METSIGHTS_BASE_URL", "https://api.metsights.test")

    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status, subscription_id) "
            "VALUES (8801, 'PK8801', 'Mets Pro', '2', 1, '01975457-63d4-c3eb-52f7-15bd67a7da7a') "
            "ON CONFLICT (package_id) DO UPDATE SET subscription_id = EXCLUDED.subscription_id"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 1) ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_types (id, code, display_name, is_active) "
            "VALUES (8801, 'bio_ai_mets8801', 'Bio AI', true) ON CONFLICT (code) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO organizations (organization_id, name, status) "
            "VALUES (8801, 'Mets Org', 1)"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, organization_id, "
            "engagement_type, assessment_package_id, diagnostic_package_id, city, slot_duration, "
            "start_date, end_date, status, create_profile_on_metsights, "
            "metsights_engagement_id, enroll_for_fitprint_full) "
            "VALUES (8801, 'Mets Camp', 'METS8801', 8801, 8801, 8801, 1, 'BLR', 20, "
            "'2026-02-01', '2026-02-28', 3, true, 'ms-eng-8801', false)"
        )
    )
    await test_db_session.commit()

    profile_id = "ms-profile-8801"
    record_id = "ms-record-8801"

    async def _fake_create_profile(self, *, data: dict):
        assert data.get("gender") == "1"
        assert data.get("age") == 30
        assert "first_name" not in data
        assert "phone" not in data
        return {"data": {"id": profile_id}}

    async def _fake_list_records(self, *, profile_id: str, completed=None, code=None, search=None):
        assert profile_id == "ms-profile-8801"
        return {"data": [{"id": record_id, "date": "2026-02-01", "created_at": "2026-02-01T10:00:00Z"}]}

    async def _fake_create_record(self, *, profile_id: str, data: dict):
        assert profile_id == "ms-profile-8801"
        return {"data": {"id": record_id}}

    monkeypatch.setattr("modules.metsights.client.MetsightsClient.create_profile", _fake_create_profile)
    monkeypatch.setattr("modules.metsights.client.MetsightsClient.list_profile_records", _fake_list_records)
    monkeypatch.setattr("modules.metsights.client.MetsightsClient.create_profile_record", _fake_create_record)

    payload = {
        "age": 30,
        "first_name": "Mets",
        "last_name": "Onboard",
        "phone": "8801880188",
        "email": "mets.onboard@example.com",
        "gender": "male",
        "city": "BLR",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "11:00",
    }

    response = await async_client.post("/users/code/METS8801/onboard", json=payload)
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert data["metsights_record_id"] == record_id

    rows = (
        await test_db_session.execute(
            text(
                "SELECT engagement_id, user_id, api_endpoint_url, status, request_payload, response_payload "
                "FROM integration_sync_logs WHERE provider = 'metsights' AND engagement_id = 8801 "
                "ORDER BY sync_log_id"
            )
        )
    ).mappings().all()

    assert len(rows) >= 2
    profile_row = next(r for r in rows if r["api_endpoint_url"].rstrip("/").endswith("/external/profiles"))
    assert profile_row["status"] in ("success", 2)
    assert profile_row["user_id"] == data["user_id"]
    assert profile_row["request_payload"]["gender"] == "1"
    assert profile_row["request_payload"]["age"] == 30
    assert "first_name" not in profile_row["request_payload"]

    records_row = next(r for r in rows if "/profiles/ms-profile-8801/records/" in r["api_endpoint_url"])
    assert records_row["status"] in ("success", 2)
    assert records_row["user_id"] == data["user_id"]


@pytest.mark.asyncio
async def test_engagement_onboard_commits_before_metsights_sync_log(
    async_client, test_db_session, monkeypatch
):
    """Regression: isolated integration_sync_logs must not run before user row is committed."""
    from db.transaction import release_request_transaction
    from modules.audit.cron_sync_logging import persist_integration_sync_log_isolated

    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    monkeypatch.setattr(settings, "METSIGHTS_BASE_URL", "https://api.metsights.test")

    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status, subscription_id) "
            "VALUES (8802, 'PK8802', 'Mets Pro', '2', 1, '01975457-63d4-c3eb-52f7-15bd67a7da7a') "
            "ON CONFLICT (package_id) DO UPDATE SET subscription_id = EXCLUDED.subscription_id"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 1) ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_types (id, code, display_name, is_active) "
            "VALUES (8802, 'bio_ai_mets8802', 'Bio AI', true) ON CONFLICT (code) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO organizations (organization_id, name, status) "
            "VALUES (8802, 'Mets Org 2', 1)"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, organization_id, "
            "engagement_type, assessment_package_id, diagnostic_package_id, city, slot_duration, "
            "start_date, end_date, status, create_profile_on_metsights, "
            "metsights_engagement_id, enroll_for_fitprint_full) "
            "VALUES (8802, 'Mets Camp 2', 'METS8802', 8802, 8802, 8802, 1, 'BLR', 20, "
            "'2026-02-01', '2026-02-28', 3, true, 'ms-eng-8802', false)"
        )
    )
    await test_db_session.commit()

    profile_id = "ms-profile-8802"
    record_id = "ms-record-8802"
    call_order: list[str] = []

    async def _tracking_release(db):
        call_order.append("release")
        return await release_request_transaction(db)

    async def _tracking_persist(**kwargs):
        if kwargs.get("provider") == "metsights":
            call_order.append("metsights_sync_log")
        return await persist_integration_sync_log_isolated(**kwargs)

    async def _fake_create_profile(self, *, data: dict):
        return {"data": {"id": profile_id}}

    async def _fake_list_records(self, *, profile_id: str, completed=None, code=None, search=None):
        return {"data": [{"id": record_id, "date": "2026-02-01", "created_at": "2026-02-01T10:00:00Z"}]}

    async def _fake_create_record(self, *, profile_id: str, data: dict):
        return {"data": {"id": record_id}}

    monkeypatch.setattr("modules.users.service.release_request_transaction", _tracking_release)
    monkeypatch.setattr(
        "modules.metsights.integration_logging.persist_integration_sync_log_isolated",
        _tracking_persist,
    )
    monkeypatch.setattr("modules.metsights.client.MetsightsClient.create_profile", _fake_create_profile)
    monkeypatch.setattr("modules.metsights.client.MetsightsClient.list_profile_records", _fake_list_records)
    monkeypatch.setattr("modules.metsights.client.MetsightsClient.create_profile_record", _fake_create_record)

    payload = {
        "age": 30,
        "first_name": "Mets",
        "last_name": "Commit",
        "phone": "8802880288",
        "email": "mets.commit@example.com",
        "gender": "male",
        "city": "BLR",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "11:00",
    }

    response = await async_client.post("/users/code/METS8802/onboard", json=payload)
    assert response.status_code == 200, response.text
    assert response.json()["data"]["metsights_record_id"] == record_id

    assert "metsights_sync_log" in call_order
    first_log_idx = call_order.index("metsights_sync_log")
    assert any(call_order[i] == "release" for i in range(first_log_idx))
