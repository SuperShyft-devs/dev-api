"""Tests for integration_sync_logs on Metsights questionnaire push/pull paths."""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import AssessmentInstance, AssessmentPackage, AssessmentPackageCategory
from modules.employee.models import Employee
from modules.questionnaire.models import QuestionnaireCategory, QuestionnaireResponse
from modules.users.models import User
from tests.modules.questionnaire.test_questionnaire_user_routes import _ensure_test_engagement, _seed_user


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}0000000001", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=user_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


async def _seed_push_engagement(test_db_session, *, engagement_id: int = 9701, package_id: int = 9702):
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'Test Diagnostic', 'test_provider', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    test_db_session.add(
        AssessmentPackage(
            package_id=package_id,
            package_code="METSIGHTS_PRO_LOG",
            display_name="Metsights Pro",
            assessment_type_code="2",
            status="active",
        )
    )
    await test_db_session.flush()
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, "
            "assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, "
            "status, participant_count, organization_id) "
            "VALUES (:eid, 'Sync Log Camp', 'ENG9701', 'bio_ai', :pid, 1, 'BLR', 20, "
            "'2026-02-01', '2026-02-28', 'running', 0, NULL) "
            "ON CONFLICT (engagement_id) DO NOTHING"
        ),
        {"eid": engagement_id, "pid": package_id},
    )
    test_db_session.add(AssessmentPackageCategory(package_id=package_id, category_id=1))
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_push_questionnaires_creates_integration_sync_logs(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    await _seed_employee(test_db_session, user_id=9701)
    await _seed_push_engagement(test_db_session)

    await _seed_user(test_db_session, user_id=5701)
    await _seed_user(test_db_session, user_id=5702)

    pushed_rid = "MS-PUSH-LOG-01"
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=9703,
            user_id=5701,
            package_id=9702,
            engagement_id=9701,
            status="active",
            metsights_record_id=pushed_rid,
        )
    )
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=9703,
            question_id=1,
            category_id=1,
            answer={"value": 175.0, "unit": "0"},
            submitted_at=None,
        )
    )
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=9704,
            user_id=5702,
            package_id=9702,
            engagement_id=9701,
            status="active",
            metsights_record_id=None,
        )
    )
    await test_db_session.commit()

    async def _fake_upsert(self, *, record_id: str, resource: str, body: dict):
        return {}

    async def _fake_options(self, *, record_id: str, resource: str):
        return {}

    monkeypatch.setattr("modules.metsights.service.MetsightsService.upsert_record_subresource", _fake_upsert)
    monkeypatch.setattr("modules.metsights.service.MetsightsService.options_record_subresource", _fake_options)

    response = await async_client.post(
        "/engagements/9701/push-questionnaires",
        headers=_auth_header(9701),
        json={"package_id": 9702},
    )
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert data["pushed"] == 1
    assert data["skipped"] == 1

    rows = (
        await test_db_session.execute(
            text(
                "SELECT engagement_id, user_id, api_endpoint_url, status, response_payload, request_payload "
                "FROM integration_sync_logs WHERE provider = 'metsights' "
                "AND engagement_id = 9701 ORDER BY sync_log_id"
            )
        )
    ).mappings().all()

    assert len(rows) >= 2

    skipped_rows = [r for r in rows if r["status"] == "skipped"]
    assert any(r["user_id"] == 5702 for r in skipped_rows)
    assert any(
        r["response_payload"] and r["response_payload"].get("reason") == "no_metsights_record_id"
        for r in skipped_rows
    )

    push_rows = [r for r in rows if r["user_id"] == 5701 and r["status"] == "success"]
    assert push_rows
    assert any("/physical-measurement/" in r["api_endpoint_url"] for r in push_rows)
    assert any(r["response_payload"] == {"pushed": True} for r in push_rows)
    assert any(r["request_payload"] and "height" in r["request_payload"] for r in push_rows)


@pytest.mark.asyncio
async def test_import_answers_legacy_creates_integration_sync_logs(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    await _ensure_test_engagement(test_db_session)

    uid = 5710
    await _seed_user(test_db_session, user_id=uid)
    pkg_id = 9710
    rid = "MS-IMPORT-LOG-01"

    test_db_session.add(
        AssessmentPackage(
            package_id=pkg_id,
            package_code="MET_IMPORT_LOG",
            display_name="Import Log Test",
            assessment_type_code="1",
            status="active",
        )
    )
    await test_db_session.flush()
    test_db_session.add(AssessmentPackageCategory(package_id=pkg_id, category_id=1))
    aid = 9711
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=aid,
            user_id=uid,
            package_id=pkg_id,
            engagement_id=1,
            status="active",
            metsights_record_id=rid,
        )
    )
    await test_db_session.commit()

    diet_payload = {"living_region": "1", "diet_preference": "0"}

    async def _fake_get_sub(self, *, record_id: str, resource: str):
        resource = str(resource).strip().strip("/")
        if resource == "diet-lifestyle-parameters":
            return diet_payload
        return None

    async def _fake_options(self, *, record_id: str, resource: str):
        resource = str(resource).strip().strip("/")
        if resource == "diet-lifestyle-parameters":
            return {
                "living_region": {"choices": {"1": "Inland region", "0": "Coastal region"}},
                "diet_preference": {"choices": {"0": "Veg", "1": "Non-Veg"}},
            }
        return {}

    async def _fake_record_detail(self, *, record_id: str):
        return {"id": record_id, "vital_parameter": None, "physical_measurement": None}

    monkeypatch.setattr("modules.metsights.service.MetsightsService.get_record_subresource_or_none", _fake_get_sub)
    monkeypatch.setattr("modules.metsights.service.MetsightsService.options_record_subresource", _fake_options)
    monkeypatch.setattr("modules.metsights.service.MetsightsService.get_record_detail", _fake_record_detail)

    response = await async_client.post(
        f"/assessments/{aid}/metsights/import-answers-legacy",
        headers=_auth_header(uid),
    )
    assert response.status_code == 200, response.text

    rows = (
        await test_db_session.execute(
            text(
                "SELECT api_endpoint_url, status, response_payload "
                "FROM integration_sync_logs WHERE provider = 'metsights' "
                "AND user_id = :uid ORDER BY sync_log_id"
            ),
            {"uid": uid},
        )
    ).mappings().all()

    assert len(rows) == 3
    diet_row = next(r for r in rows if "diet-lifestyle-parameters" in r["api_endpoint_url"])
    assert diet_row["status"] == "success"
    assert "imported" in diet_row["response_payload"]

    skipped_rows = [r for r in rows if r["status"] == "skipped"]
    assert len(skipped_rows) == 2


@pytest.mark.asyncio
async def test_import_category_reload_zero_creates_skipped_sync_log(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    await _ensure_test_engagement(test_db_session)

    uid = 5720
    await _seed_user(test_db_session, user_id=uid)
    pkg_id = 9720
    rid = "MS-SKIP-IMPORT-01"

    test_db_session.add(
        AssessmentPackage(
            package_id=pkg_id,
            package_code="MET_SKIP_IMPORT",
            display_name="Skip Import Test",
            assessment_type_code="2",
            status="active",
        )
    )
    await test_db_session.flush()
    category = QuestionnaireCategory(
        category_id=9721,
        category_key="physical-measurement",
        category_of="metsights",
        display_name="Physical Measurement",
        status="active",
    )
    test_db_session.add(category)
    await test_db_session.flush()
    test_db_session.add(AssessmentPackageCategory(package_id=pkg_id, category_id=9721))
    aid = 9722
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=aid,
            user_id=uid,
            package_id=pkg_id,
            engagement_id=1,
            status="active",
            metsights_record_id=rid,
        )
    )
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=aid,
            question_id=1,
            category_id=9721,
            answer={"value": 170.0, "unit": "0"},
            submitted_at=None,
        )
    )
    await test_db_session.commit()

    response = await async_client.post(
        f"/assessments/{aid}/metsights/import-answers",
        headers=_auth_header(uid),
        json={"category": "physical-measurement", "category_of": "metsights", "reload": 0},
    )
    assert response.status_code == 200, response.text
    assert response.json()["data"]["status"] == "skipped"

    row = (
        await test_db_session.execute(
            text(
                "SELECT status, response_payload, api_endpoint_url "
                "FROM integration_sync_logs WHERE provider = 'metsights' "
                "AND user_id = :uid ORDER BY sync_log_id DESC LIMIT 1"
            ),
            {"uid": uid},
        )
    ).mappings().one()
    assert row["status"] == "skipped"
    assert row["response_payload"]["skipped"] is True
    assert "physical-measurement" in row["api_endpoint_url"]


@pytest.mark.asyncio
async def test_submit_legacy_creates_integration_sync_logs(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    await _ensure_test_engagement(test_db_session)

    uid = 5730
    await _seed_user(test_db_session, user_id=uid)
    pkg_id = 9730
    rid = "MS-SUBMIT-LEG-01"

    test_db_session.add(
        AssessmentPackage(
            package_id=pkg_id,
            package_code="MET_SUBMIT_LEG",
            display_name="Submit Legacy Test",
            assessment_type_code="1",
            status="active",
        )
    )
    await test_db_session.flush()
    test_db_session.add(AssessmentPackageCategory(package_id=pkg_id, category_id=1))
    aid = 9731
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=aid,
            user_id=uid,
            package_id=pkg_id,
            engagement_id=1,
            status="active",
            metsights_record_id=rid,
        )
    )
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=aid,
            question_id=1,
            category_id=1,
            answer={"value": 180.0, "unit": "0"},
            submitted_at=None,
        )
    )
    await test_db_session.commit()

    async def _fake_upsert(self, *, record_id: str, resource: str, body: dict):
        return {}

    async def _fake_options(self, *, record_id: str, resource: str):
        return {}

    monkeypatch.setattr("modules.metsights.service.MetsightsService.upsert_record_subresource", _fake_upsert)
    monkeypatch.setattr("modules.metsights.service.MetsightsService.options_record_subresource", _fake_options)

    response = await async_client.post(
        f"/assessments/{aid}/submit-legacy",
        headers=_auth_header(uid),
        json={},
    )
    assert response.status_code == 200, response.text

    rows = (
        await test_db_session.execute(
            text(
                "SELECT status, api_endpoint_url, response_payload "
                "FROM integration_sync_logs WHERE provider = 'metsights' "
                "AND user_id = :uid AND status = 'success'"
            ),
            {"uid": uid},
        )
    ).mappings().all()

    assert any("/physical-measurement/" in r["api_endpoint_url"] for r in rows)
    assert any(r["response_payload"] == {"pushed": True} for r in rows)
