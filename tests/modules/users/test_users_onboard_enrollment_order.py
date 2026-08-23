from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from core.config import settings
from modules.users.dependencies import get_users_service


def _patch_metsights_record_order(monkeypatch) -> list[str]:
    order: list[str] = []

    async def _fake_create_record(self, *, profile_id, assessment_type_code, sync_context=None):
        order.append(assessment_type_code)
        return f"record-{assessment_type_code}"

    monkeypatch.setattr(
        "modules.metsights.service.MetsightsService.create_record_for_profile",
        _fake_create_record,
    )
    return order


async def _seed_primary_and_fitprint_packages(test_db_session) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status) "
            "VALUES "
            "(9911, 'PK9911', 'Primary Pro', '2', 'active'), "
            "(9912, 'PK9912', 'FitPrint Full', '7', 'active') "
            "ON CONFLICT (package_id) DO UPDATE SET "
            "package_code = EXCLUDED.package_code, "
            "display_name = EXCLUDED.display_name, "
            "assessment_type_code = EXCLUDED.assessment_type_code, "
            "status = EXCLUDED.status"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (9911, 'REF9911', 'Diag Package', 'active') "
            "ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )


@pytest.mark.asyncio
async def test_engagement_onboard_enrolls_fitprint_before_primary(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    monkeypatch.setattr(settings, "METSIGHTS_BASE_URL", "https://api.metsights.test")
    record_order = _patch_metsights_record_order(monkeypatch)

    async def _fake_register(self, *, engagement_id: str, data: dict):
        return {"data": {"id": "ms-profile-fp-order"}}

    monkeypatch.setattr("modules.metsights.client.MetsightsClient.create_profile_for_engagement", _fake_register)

    await _seed_primary_and_fitprint_packages(test_db_session)
    await test_db_session.execute(
        text(
            "INSERT INTO organizations (organization_id, name, status) "
            "VALUES (9911, 'FitPrint Order Org', 'active') "
            "ON CONFLICT (organization_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, organization_id, "
            "engagement_type, assessment_package_id, diagnostic_package_id, city, slot_duration, "
            "start_date, end_date, status, participant_count, create_profile_on_metsights, "
            "metsights_engagement_id, enroll_for_fitprint_full) "
            "VALUES (9911, 'FitPrint Order Camp', 'FPORDER01', 9911, 'bio_ai', 9911, 9911, 'BLR', 20, "
            "'2026-02-01', '2026-02-28', 'running', 0, true, 'ms-eng-fp-order', true)"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 30,
        "first_name": "Fit",
        "last_name": "Order",
        "phone": "9911001100",
        "email": "fit.order@example.com",
        "gender": "male",
        "city": "BLR",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "11:00",
    }

    response = await async_client.post("/users/code/FPORDER01/onboard", json=payload)
    assert response.status_code == 200, response.text
    assert "7" in record_order
    assert "2" in record_order
    assert record_order.index("7") < record_order.index("2")

    participant_row = (
        await test_db_session.execute(
            text(
                "SELECT is_fitprint_record_id_synced, is_primary_record_id_synced "
                "FROM engagement_participants WHERE engagement_id = 9911"
            )
        )
    ).one()
    assert participant_row.is_fitprint_record_id_synced is True
    assert participant_row.is_primary_record_id_synced is True


@pytest.mark.asyncio
async def test_engagement_onboard_null_metsights_engagement_enrolls_fitprint_before_primary(
    async_client, test_db_session, monkeypatch
):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    monkeypatch.setattr(settings, "METSIGHTS_BASE_URL", "https://api.metsights.test")
    record_order = _patch_metsights_record_order(monkeypatch)

    async def _fake_get_or_create_profile_id(self, **kwargs):
        return "ms-profile-null-eng-order"

    monkeypatch.setattr(
        "modules.metsights.service.MetsightsService.get_or_create_profile_id",
        _fake_get_or_create_profile_id,
    )

    await _seed_primary_and_fitprint_packages(test_db_session)
    await test_db_session.execute(
        text(
            "INSERT INTO organizations (organization_id, name, status) "
            "VALUES (9913, 'Null Metsights Org', 'active') "
            "ON CONFLICT (organization_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, organization_id, "
            "engagement_type, assessment_package_id, diagnostic_package_id, city, slot_duration, "
            "start_date, end_date, status, participant_count, create_profile_on_metsights, "
            "metsights_engagement_id, enroll_for_fitprint_full) "
            "VALUES (9913, 'Null Metsights Camp', 'FPNULL01', 9913, 'bio_ai', 9911, 9911, 'BLR', 20, "
            "'2026-02-01', '2026-02-28', 'running', 0, true, NULL, true)"
        )
    )
    await test_db_session.commit()

    payload = {
        "age": 30,
        "first_name": "Null",
        "last_name": "EngOrder",
        "phone": "9911003300",
        "email": "null.eng.order@example.com",
        "gender": "male",
        "city": "BLR",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "11:00",
    }

    response = await async_client.post("/users/code/FPNULL01/onboard", json=payload)
    assert response.status_code == 200, response.text
    assert "7" in record_order
    assert "2" in record_order
    assert record_order.index("7") < record_order.index("2")
    assert record_order.count("2") == 1


@pytest.mark.asyncio
async def test_public_onboard_enrolls_fitprint_before_primary(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    monkeypatch.setattr(settings, "METSIGHTS_BASE_URL", "https://api.metsights.test")
    record_order = _patch_metsights_record_order(monkeypatch)

    async def _fake_get_or_create_profile_id(self, **kwargs):
        return "ms-profile-public-fp-order"

    monkeypatch.setattr(
        "modules.metsights.service.MetsightsService.get_or_create_profile_id",
        _fake_get_or_create_profile_id,
    )

    await _seed_primary_and_fitprint_packages(test_db_session)
    by_type = {
        "bio_ai": {
            "assessment_package_id": 9911,
            "diagnostic_package_id": 9911,
            "blood_collection_type": "home_collection",
            "create_profile_on_metsights": True,
            "enroll_for_fitprint_full": True,
        },
        "blood_test": {
            "assessment_package_id": 9911,
            "diagnostic_package_id": 9911,
            "blood_collection_type": None,
            "create_profile_on_metsights": True,
            "enroll_for_fitprint_full": False,
        },
        "consultation": {
            "assessment_package_id": 9911,
            "diagnostic_package_id": 9911,
            "blood_collection_type": None,
            "create_profile_on_metsights": True,
            "enroll_for_fitprint_full": False,
        },
        "blood_test_with_consultation": {
            "assessment_package_id": 9911,
            "diagnostic_package_id": 9911,
            "blood_collection_type": None,
            "create_profile_on_metsights": True,
            "enroll_for_fitprint_full": False,
        },
        "bio_ai_with_consultation": {
            "assessment_package_id": 9911,
            "diagnostic_package_id": 9911,
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
            "VALUES (1, 9911, 9911, 'bio_ai', 'home_collection', true, true, CAST(:by_type AS jsonb)) "
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
        "first_name": "Public",
        "last_name": "FitOrder",
        "email": "public.fit.order@example.com",
        "phone": "9911002200",
        "gender": "male",
        "city": "Chennai",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "10:00",
        "referred_by": "",
    }

    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 200, response.text
    assert "7" in record_order
    assert "2" in record_order
    assert record_order.index("7") < record_order.index("2")


@pytest.mark.asyncio
async def test_fulfill_bio_ai_booking_enrolls_fitprint_before_primary(test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    monkeypatch.setattr(settings, "METSIGHTS_BASE_URL", "https://api.metsights.test")
    record_order = _patch_metsights_record_order(monkeypatch)

    async def _fake_get_or_create_profile_id(self, **kwargs):
        return "ms-profile-fulfill-fp-order"

    monkeypatch.setattr(
        "modules.metsights.service.MetsightsService.get_or_create_profile_id",
        _fake_get_or_create_profile_id,
    )

    await _seed_primary_and_fitprint_packages(test_db_session)
    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, age, phone, status, first_name, last_name, gender, city) "
            "VALUES (991130, 30, '9911300000', 'active', 'Fulfill', 'FitOrder', 'male', 'Mumbai')"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings "
            "(settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id, "
            "b2c_default_engagement_type, b2c_default_blood_collection_type, "
            "b2c_default_create_profile_on_metsights, b2c_default_enroll_for_fitprint_full) "
            "VALUES (1, 9911, 9911, 'bio_ai', 'home_collection', true, true) "
            "ON CONFLICT (settings_id) DO UPDATE SET "
            "b2c_default_assessment_package_id = EXCLUDED.b2c_default_assessment_package_id, "
            "b2c_default_diagnostic_package_id = EXCLUDED.b2c_default_diagnostic_package_id, "
            "b2c_default_engagement_type = EXCLUDED.b2c_default_engagement_type, "
            "b2c_default_blood_collection_type = EXCLUDED.b2c_default_blood_collection_type, "
            "b2c_default_create_profile_on_metsights = EXCLUDED.b2c_default_create_profile_on_metsights, "
            "b2c_default_enroll_for_fitprint_full = EXCLUDED.b2c_default_enroll_for_fitprint_full"
        )
    )
    await test_db_session.commit()

    booking = SimpleNamespace(
        user_id=991130,
        metadata_={
            "blood_collection_date": "2026-07-15",
            "blood_collection_time_slot": "09:30",
            "city": "Mumbai",
        },
    )

    service = get_users_service()
    result = await service.fulfill_bio_ai_booking(test_db_session, booking=booking)
    await test_db_session.commit()

    assert result is not None
    assert "7" in record_order
    assert "2" in record_order
    assert record_order.index("7") < record_order.index("2")
