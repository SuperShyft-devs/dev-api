"""Tests for POST /assessments/{assessment_instance_id}/submit and Metsights push."""

from __future__ import annotations

from datetime import timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import AssessmentInstance, AssessmentPackage, AssessmentPackageCategory
from modules.questionnaire.models import QuestionnaireResponse
from tests.modules.questionnaire.test_questionnaire_user_routes import _ensure_test_engagement, _seed_user


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_submit_metsights_basic_patches_physical_measurement(async_client, test_db_session, monkeypatch):
    await _ensure_test_engagement(test_db_session)
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")

    calls: list[tuple[str, str, dict]] = []

    async def _fake_upsert(self, *, record_id: str, resource: str, body: dict):
        calls.append((record_id, resource, dict(body)))
        return {}

    monkeypatch.setattr("modules.metsights.service.MetsightsService.upsert_record_subresource", _fake_upsert)

    uid = 55201
    await _seed_user(test_db_session, user_id=uid)
    pkg_id = 5551
    test_db_session.add(
        AssessmentPackage(
            package_id=pkg_id,
            package_code="MET_SUBMIT_T",
            display_name="Submit Test",
            assessment_type_code="1",
            status="active",
        )
    )
    test_db_session.add(AssessmentPackageCategory(package_id=pkg_id, category_id=1))
    await test_db_session.commit()

    aid = 5552
    rid = "MS-SUBMIT-01"
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
            answer={"value": 175.0, "unit": "0"},
            submitted_at=None,
        )
    )
    await test_db_session.commit()

    r = await async_client.post(f"/assessments/{aid}/submit", headers=_auth_header(uid))
    assert r.status_code == 200
    assert r.json()["data"]["message"]

    phys = [c for c in calls if c[1] == "physical-measurement"]
    assert len(phys) == 1
    assert phys[0][0] == rid
    assert phys[0][2].get("height") == 175.0
    assert phys[0][2].get("height_unit") == "0"
    assert phys[0][2].get("is_complete") is True


@pytest.mark.asyncio
async def test_submit_metsights_basic_requires_record_id(async_client, test_db_session, monkeypatch):
    await _ensure_test_engagement(test_db_session)
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")

    uid = 55202
    await _seed_user(test_db_session, user_id=uid)
    pkg_id = 5553
    test_db_session.add(
        AssessmentPackage(
            package_id=pkg_id,
            package_code="MET_SUBMIT_T2",
            display_name="Submit Test 2",
            assessment_type_code="1",
            status="active",
        )
    )
    test_db_session.add(AssessmentPackageCategory(package_id=pkg_id, category_id=1))
    await test_db_session.commit()

    aid = 5554
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=aid,
            user_id=uid,
            package_id=pkg_id,
            engagement_id=1,
            status="active",
            metsights_record_id=None,
        )
    )
    await test_db_session.commit()

    r = await async_client.post(f"/assessments/{aid}/submit", headers=_auth_header(uid))
    assert r.status_code == 422
    assert "Metsights record id" in r.json()["message"]
