"""Integration tests for user onboarding payload fields and camp configuration."""

from __future__ import annotations

import pytest
from sqlalchemy import delete, select, text

from modules.engagements.models import CampUserOnboardingPayloadField, Engagement, UserOnboardingPayloadField
from modules.experts.models import ExpertTypeModel
from tests.helpers.auth import employee_auth_header
from tests.modules.engagements.test_engagements_routes import (
    _seed_assessment_package,
    _seed_diagnostic_package,
    _seed_employee,
    _seed_organization,
)


def _auth_header(employee_id: int) -> dict[str, str]:
    return employee_auth_header(employee_id)


def _assert_valid_camp_code(camp_code: str | None) -> None:
    assert camp_code is not None
    assert len(camp_code) == 12
    assert camp_code.isalnum()


async def _seed_platform_settings(test_db_session, *, employee_id: int) -> None:
    await test_db_session.execute(text("DELETE FROM platform_settings"))
    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings "
            "(settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id, "
            "default_onboarding_assistant_employee_ids) "
            "VALUES (1, 1, 1, :ids)"
        ),
        {"ids": str(employee_id)},
    )
    await test_db_session.commit()


from tests.helpers.engagement_types import engagement_type_id


async def _create_b2b_engagement(async_client, test_db_session, *, employee_id: int = 10) -> tuple[int, int]:
    type_id = await engagement_type_id(test_db_session, "bio_ai")
    org_id = employee_id
    await _seed_organization(test_db_session, organization_id=org_id, name=f"Org {org_id}")
    day = min(28, max(1, employee_id))
    start_date = f"2026-03-{day:02d}"
    payload = {
        "engagement_name": "Camp",
        "organization_id": org_id,
        "engagement_type": type_id,
        "assessment_package_id": 1,
        "diagnostic_package_id": 1,
        "city": "BLR",
        "slot_duration": 20,
        "start_date": start_date,
        "end_date": start_date,
    }
    response = await async_client.post("/engagements", headers=_auth_header(employee_id), json=payload)
    assert response.status_code == 201
    engagement_id = response.json()["data"]["engagement_id"]
    details = await async_client.get(f"/engagements/{engagement_id}", headers=_auth_header(employee_id))
    camp_no = details.json()["data"]["camp_no"]
    assert camp_no is not None
    return int(engagement_id), int(camp_no)


async def _seed_catalog_field(async_client, *, employee_id: int, payload_key: str, input_type: str = "text"):
    response = await async_client.post(
        "/user-onboarding-payload-fields",
        headers=_auth_header(employee_id),
        json={
            "payload_key": payload_key,
            "display_name": payload_key.replace("_", " ").title(),
            "section": "personal_information",
            "input_type": input_type,
        },
    )
    assert response.status_code == 201
    return response.json()["data"]["id"]


@pytest.mark.asyncio
async def test_user_onboarding_payload_fields_crud(async_client, test_db_session):
    await _seed_employee(test_db_session, employee_id=51)
    headers = _auth_header(51)

    create_res = await async_client.post(
        "/user-onboarding-payload-fields",
        headers=headers,
        json={
            "payload_key": "height",
            "display_name": "Height",
            "section": "personal_information",
            "input_type": "number",
        },
    )
    assert create_res.status_code == 201
    field_id = create_res.json()["data"]["id"]

    dup_res = await async_client.post(
        "/user-onboarding-payload-fields",
        headers=headers,
        json={
            "payload_key": "height",
            "display_name": "Duplicate",
            "section": "personal_information",
            "input_type": "number",
        },
    )
    assert dup_res.status_code == 409

    list_res = await async_client.get("/user-onboarding-payload-fields?page=1&limit=10", headers=headers)
    assert list_res.status_code == 200
    assert any(row["id"] == field_id for row in list_res.json()["data"])

    update_res = await async_client.put(
        f"/user-onboarding-payload-fields/{field_id}",
        headers=headers,
        json={"display_name": "Height (cm)"},
    )
    assert update_res.status_code == 200
    assert update_res.json()["data"]["display_name"] == "Height (cm)"

    delete_res = await async_client.delete(f"/user-onboarding-payload-fields/{field_id}", headers=headers)
    assert delete_res.status_code == 200
    assert delete_res.json()["data"]["deleted"] is True


@pytest.mark.asyncio
async def test_b2b_create_inserts_camp_onboarding_row(async_client, test_db_session):
    await _seed_employee(test_db_session, employee_id=10)
    await _seed_assessment_package(test_db_session, package_id=1)
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)
    await _seed_platform_settings(test_db_session, employee_id=10)

    _, camp_no = await _create_b2b_engagement(async_client, test_db_session, employee_id=10)

    result = await test_db_session.execute(
        select(CampUserOnboardingPayloadField).where(CampUserOnboardingPayloadField.camp_no == camp_no)
    )
    row = result.scalar_one_or_none()
    assert row is not None
    assert row.onboarding_payload == {}
    _assert_valid_camp_code(row.camp_code)


@pytest.mark.asyncio
async def test_existing_camp_init_and_update_payload(async_client, test_db_session):
    await _seed_employee(test_db_session, employee_id=12)
    await _seed_assessment_package(test_db_session, package_id=1)
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)
    await _seed_platform_settings(test_db_session, employee_id=12)

    _, camp_no = await _create_b2b_engagement(async_client, test_db_session, employee_id=12)
    await test_db_session.execute(
        delete(CampUserOnboardingPayloadField).where(CampUserOnboardingPayloadField.camp_no == camp_no)
    )
    await test_db_session.commit()

    get_res = await async_client.get(
        f"/organizations/camps/{camp_no}/onboarding-payload",
        headers=_auth_header(12),
    )
    assert get_res.status_code == 200
    assert get_res.json()["data"]["initialized"] is False
    assert get_res.json()["data"]["onboarding_payload"] is None

    put_before_init = await async_client.put(
        f"/organizations/camps/{camp_no}/onboarding-payload",
        headers=_auth_header(12),
        json={"onboarding_payload": {}},
    )
    assert put_before_init.status_code == 404

    init_res = await async_client.post(
        f"/organizations/camps/{camp_no}/onboarding-payload",
        headers=_auth_header(12),
    )
    assert init_res.status_code == 201
    assert init_res.json()["data"]["initialized"] is True
    _assert_valid_camp_code(init_res.json()["data"]["camp_code"])

    second_init = await async_client.post(
        f"/organizations/camps/{camp_no}/onboarding-payload",
        headers=_auth_header(12),
    )
    assert second_init.status_code == 409

    await _seed_catalog_field(async_client, employee_id=12, payload_key="weight")

    update_res = await async_client.put(
        f"/organizations/camps/{camp_no}/onboarding-payload",
        headers=_auth_header(12),
        json={
            "onboarding_payload": {
                "weight": {"input_type": "text", "required": True, "sub_payload": {}},
            }
        },
    )
    assert update_res.status_code == 200
    assert update_res.json()["data"]["onboarding_payload"]["weight"]["required"] is True
    _assert_valid_camp_code(update_res.json()["data"]["camp_code"])


@pytest.mark.asyncio
async def test_public_camp_onboarding(async_client, test_db_session):
    await _seed_employee(test_db_session, employee_id=14)
    await _seed_assessment_package(test_db_session, package_id=1)
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)
    await _seed_platform_settings(test_db_session, employee_id=14)

    engagement_id, camp_no = await _create_b2b_engagement(async_client, test_db_session, employee_id=14)

    camp_row_result = await test_db_session.execute(
        select(CampUserOnboardingPayloadField).where(CampUserOnboardingPayloadField.camp_no == camp_no)
    )
    camp_row = camp_row_result.scalar_one()
    _assert_valid_camp_code(camp_row.camp_code)
    camp_code = camp_row.camp_code

    public_res = await async_client.get(f"/engagements/camps/code/{camp_code}/onboarding")
    assert public_res.status_code == 200
    body = public_res.json()["data"]
    assert body["organization_name"] == "Org 14"
    assert body["year"] == 2026
    assert isinstance(body["onboarding_payload"], dict)
    assert len(body["engagements"]) == 1
    assert body["engagements"][0]["city"] == "BLR"
    assert isinstance(body["engagements"][0]["code"], str)

    missing = await async_client.get("/engagements/camps/code/invalidCode12/onboarding")
    assert missing.status_code == 404

    await test_db_session.execute(
        delete(CampUserOnboardingPayloadField).where(CampUserOnboardingPayloadField.camp_no == camp_no)
    )
    await test_db_session.commit()

    public_after_delete = await async_client.get(f"/engagements/camps/code/{camp_code}/onboarding")
    assert public_after_delete.status_code == 404

    # Sanity: engagement still exists
    row = await test_db_session.get(Engagement, engagement_id)
    assert row is not None


@pytest.mark.asyncio
async def test_json_field_requires_all_expert_types(async_client, test_db_session):
    await _seed_employee(test_db_session, employee_id=16)
    await _seed_assessment_package(test_db_session, package_id=1)
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)
    await _seed_platform_settings(test_db_session, employee_id=16)
    await test_db_session.execute(delete(UserOnboardingPayloadField))
    await test_db_session.commit()

    existing = await test_db_session.execute(select(ExpertTypeModel))
    if not existing.scalars().first():
        test_db_session.add(ExpertTypeModel(type_key="doctor", type="Doctor"))
        test_db_session.add(ExpertTypeModel(type_key="nutritionist", type="Nutritionist"))
        await test_db_session.commit()

    _, camp_no = await _create_b2b_engagement(async_client, test_db_session, employee_id=16)
    await _seed_catalog_field(async_client, employee_id=16, payload_key="consultations", input_type="json")

    invalid = await async_client.put(
        f"/organizations/camps/{camp_no}/onboarding-payload",
        headers=_auth_header(16),
        json={
            "onboarding_payload": {
                "consultations": {
                    "input_type": "json",
                    "required": True,
                    "sub_payload": {"doctor": True},
                }
            }
        },
    )
    assert invalid.status_code == 400

    expert_keys = {
        row.type_key
        for row in (await test_db_session.execute(select(ExpertTypeModel))).scalars().all()
    }
    valid = await async_client.put(
        f"/organizations/camps/{camp_no}/onboarding-payload",
        headers=_auth_header(16),
        json={
            "onboarding_payload": {
                "consultations": {
                    "input_type": "json",
                    "required": True,
                    "sub_payload": {key: False for key in expert_keys},
                }
            }
        },
    )
    assert valid.status_code == 200
