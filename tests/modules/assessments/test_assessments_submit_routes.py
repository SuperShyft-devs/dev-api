"""Tests for POST /assessments/{assessment_instance_id}/submit-legacy and Metsights push."""

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

    r = await async_client.post(f"/assessments/{aid}/submit-legacy", headers=_auth_header(uid))
    assert r.status_code == 200
    assert r.json()["data"]["message"]

    phys = [c for c in calls if c[1] == "physical-measurement"]
    assert len(phys) == 1
    assert phys[0][0] == rid
    assert phys[0][2].get("height") == 175.0
    assert phys[0][2].get("height_unit") == "0"
    assert phys[0][2].get("is_complete") is True


@pytest.mark.asyncio
async def test_submit_legacy_metsights_basic_requires_record_id(async_client, test_db_session, monkeypatch):
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

    r = await async_client.post(f"/assessments/{aid}/submit-legacy", headers=_auth_header(uid))
    assert r.status_code == 422
    assert "Metsights record id" in r.json()["message"]


@pytest.mark.asyncio
async def test_submit_multi_source_merges_answers(async_client, test_db_session, monkeypatch):
    """Submitting with source_assessment_instance_ids aggregates responses from multiple instances."""
    await _ensure_test_engagement(test_db_session)
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")

    calls: list[tuple[str, str, dict]] = []

    async def _fake_upsert(self, *, record_id: str, resource: str, body: dict):
        calls.append((record_id, resource, dict(body)))
        return {}

    monkeypatch.setattr("modules.metsights.service.MetsightsService.upsert_record_subresource", _fake_upsert)

    uid = 55210
    await _seed_user(test_db_session, user_id=uid)

    # Source package (Metsights Pro, type "2") with height answer
    src_pkg_id = 5560
    test_db_session.add(
        AssessmentPackage(
            package_id=src_pkg_id,
            package_code="MET_MULTI_SRC",
            display_name="Multi Source",
            assessment_type_code="2",
            status="active",
        )
    )
    test_db_session.add(AssessmentPackageCategory(package_id=src_pkg_id, category_id=1))

    # Target package (FitPrint Full, type "7")
    tgt_pkg_id = 5561
    test_db_session.add(
        AssessmentPackage(
            package_id=tgt_pkg_id,
            package_code="FP_MULTI_TGT",
            display_name="Multi Target",
            assessment_type_code="7",
            status="active",
        )
    )
    test_db_session.add(AssessmentPackageCategory(package_id=tgt_pkg_id, category_id=1))
    await test_db_session.commit()

    src_aid = 5570
    tgt_aid = 5571
    tgt_rid = "MS-MULTI-01"

    # Source instance (already completed, has height response)
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=src_aid,
            user_id=uid,
            package_id=src_pkg_id,
            engagement_id=1,
            status="completed",
            metsights_record_id="MS-SRC-01",
        )
    )
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=src_aid,
            question_id=1,
            category_id=1,
            answer={"value": 180.0, "unit": "0"},
            submitted_at=None,
        )
    )
    # Target instance (active, has weight response)
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=tgt_aid,
            user_id=uid,
            package_id=tgt_pkg_id,
            engagement_id=1,
            status="active",
            metsights_record_id=tgt_rid,
        )
    )
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=tgt_aid,
            question_id=2,
            category_id=1,
            answer={"value": 72.5, "unit": "0"},
            submitted_at=None,
        )
    )
    await test_db_session.commit()

    r = await async_client.post(
        f"/assessments/{tgt_aid}/submit-legacy",
        headers=_auth_header(uid),
        json={"source_assessment_instance_ids": [src_aid, tgt_aid]},
    )
    assert r.status_code == 200
    assert r.json()["data"]["message"]

    # FitPrint (type 7) pushes to fitness-parameters
    fit = [c for c in calls if c[1] == "fitness-parameters"]
    assert len(fit) == 1
    assert fit[0][0] == tgt_rid
    assert fit[0][2].get("height") == 180.0
    assert fit[0][2].get("weight") == 72.5
    assert fit[0][2].get("is_complete") is True


@pytest.mark.asyncio
async def test_submit_multi_source_last_wins_on_duplicate(async_client, test_db_session, monkeypatch):
    """When the same question appears in multiple sources, the later source wins."""
    await _ensure_test_engagement(test_db_session)
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")

    calls: list[tuple[str, str, dict]] = []

    async def _fake_upsert(self, *, record_id: str, resource: str, body: dict):
        calls.append((record_id, resource, dict(body)))
        return {}

    monkeypatch.setattr("modules.metsights.service.MetsightsService.upsert_record_subresource", _fake_upsert)

    uid = 55220
    await _seed_user(test_db_session, user_id=uid)

    pkg_id = 5580
    test_db_session.add(
        AssessmentPackage(
            package_id=pkg_id,
            package_code="MET_DUP_T",
            display_name="Dup Test",
            assessment_type_code="1",
            status="active",
        )
    )
    test_db_session.add(AssessmentPackageCategory(package_id=pkg_id, category_id=1))
    await test_db_session.commit()

    aid1 = 5581
    aid2 = 5582
    rid = "MS-DUP-01"

    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=aid1,
            user_id=uid,
            package_id=pkg_id,
            engagement_id=1,
            status="completed",
            metsights_record_id="MS-DUP-SRC",
        )
    )
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=aid1,
            question_id=1,
            category_id=1,
            answer={"value": 170.0, "unit": "0"},
            submitted_at=None,
        )
    )
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=aid2,
            user_id=uid,
            package_id=pkg_id,
            engagement_id=1,
            status="active",
            metsights_record_id=rid,
        )
    )
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=aid2,
            question_id=1,
            category_id=1,
            answer={"value": 185.0, "unit": "2"},
            submitted_at=None,
        )
    )
    await test_db_session.commit()

    # aid2 is later in the list, so its answer should win
    r = await async_client.post(
        f"/assessments/{aid2}/submit-legacy",
        headers=_auth_header(uid),
        json={"source_assessment_instance_ids": [aid1, aid2]},
    )
    assert r.status_code == 200

    phys = [c for c in calls if c[1] == "physical-measurement"]
    assert len(phys) == 1
    assert phys[0][2].get("height") == 185.0
    assert phys[0][2].get("height_unit") == "2"


@pytest.mark.asyncio
async def test_submit_multi_source_rejects_different_user(async_client, test_db_session, monkeypatch):
    """Source instance belonging to a different user should be rejected."""
    await _ensure_test_engagement(test_db_session)
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")

    uid1 = 55230
    uid2 = 55231
    await _seed_user(test_db_session, user_id=uid1)
    await _seed_user(test_db_session, user_id=uid2)

    pkg_id = 5590
    test_db_session.add(
        AssessmentPackage(
            package_id=pkg_id,
            package_code="MET_XUSER",
            display_name="Cross User",
            assessment_type_code="1",
            status="active",
        )
    )
    test_db_session.add(AssessmentPackageCategory(package_id=pkg_id, category_id=1))
    await test_db_session.commit()

    other_aid = 5591
    target_aid = 5592

    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=other_aid,
            user_id=uid2,
            package_id=pkg_id,
            engagement_id=1,
            status="completed",
            metsights_record_id="MS-OTHER-01",
        )
    )
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=target_aid,
            user_id=uid1,
            package_id=pkg_id,
            engagement_id=1,
            status="active",
            metsights_record_id="MS-TGT-01",
        )
    )
    await test_db_session.commit()

    r = await async_client.post(
        f"/assessments/{target_aid}/submit-legacy",
        headers=_auth_header(uid1),
        json={"source_assessment_instance_ids": [other_aid, target_aid]},
    )
    assert r.status_code == 422
    assert "different user" in r.json()["message"]


@pytest.mark.asyncio
async def test_submit_legacy_without_body_still_works(async_client, test_db_session, monkeypatch):
    """Backward compat: submitting with no body behaves the same as before."""
    await _ensure_test_engagement(test_db_session)
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")

    calls: list[tuple[str, str, dict]] = []

    async def _fake_upsert(self, *, record_id: str, resource: str, body: dict):
        calls.append((record_id, resource, dict(body)))
        return {}

    monkeypatch.setattr("modules.metsights.service.MetsightsService.upsert_record_subresource", _fake_upsert)

    uid = 55240
    await _seed_user(test_db_session, user_id=uid)
    pkg_id = 5595
    test_db_session.add(
        AssessmentPackage(
            package_id=pkg_id,
            package_code="MET_NOBD",
            display_name="No Body Test",
            assessment_type_code="1",
            status="active",
        )
    )
    test_db_session.add(AssessmentPackageCategory(package_id=pkg_id, category_id=1))
    await test_db_session.commit()

    aid = 5596
    rid = "MS-NOBD-01"
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
            answer={"value": 165.0, "unit": "0"},
            submitted_at=None,
        )
    )
    await test_db_session.commit()

    r = await async_client.post(f"/assessments/{aid}/submit-legacy", headers=_auth_header(uid))
    assert r.status_code == 200

    phys = [c for c in calls if c[1] == "physical-measurement"]
    assert len(phys) == 1
    assert phys[0][2].get("height") == 165.0


def test_answer_to_metsights_field_conversions():
    from modules.metsights.sync_service import _answer_to_metsights_fields

    assert _answer_to_metsights_fields("iodized_salt_status", "single_choice", "true") == {
        "iodized_salt_status": "true"
    }
    hp = _answer_to_metsights_fields("health_priorities", "single_choice", "2")
    assert hp["health_priorities"][0] == "2"
    assert len(hp["health_priorities"]) == 2
    assert len(set(hp["health_priorities"])) == 2
    hp_multi = _answer_to_metsights_fields("health_priorities", "multiple_choice", ["3"])
    assert hp_multi["health_priorities"][0] == "3"
    assert len(hp_multi["health_priorities"]) == 2
    assert len(set(hp_multi["health_priorities"])) == 2
    hp_label = _answer_to_metsights_fields(
        "health_priorities",
        "multiple_choice",
        ["Increasing Energy Levels"],
    )
    assert hp_label["health_priorities"][0] == "3"
    assert len(hp_label["health_priorities"]) == 2
    assert _answer_to_metsights_fields("tobacco_frequency", "single_choice", "5") == {
        "tobacco_frequency": "1"
    }
    assert _answer_to_metsights_fields("daily_active_duration", "single_choice", "2") == {
        "daily_active_duration": 1.0,
        "daily_active_duration_unit": "1",
    }
    assert _answer_to_metsights_fields("family_health_history", "multiple_choice", ["none"]) == {}


@pytest.mark.asyncio
async def test_submit_legacy_pro_pushes_fitprint_sibling_fitness(async_client, test_db_session, monkeypatch):
    await _ensure_test_engagement(test_db_session)
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")

    calls: list[tuple[str, str, dict]] = []

    async def _fake_upsert(self, *, record_id: str, resource: str, body: dict):
        calls.append((record_id, resource, dict(body)))
        return {}

    async def _fake_options(self, *, record_id: str, resource: str):
        if resource == "fitness-parameters":
            return {
                "actions": {
                    "POST": {
                        "exercise_frequency_week": {
                            "required": True,
                            "choices": [{"value": "2"}],
                        },
                    }
                }
            }
        return {}

    monkeypatch.setattr("modules.metsights.service.MetsightsService.upsert_record_subresource", _fake_upsert)
    monkeypatch.setattr("modules.metsights.service.MetsightsService.options_record_subresource", _fake_options)

    uid = 55270
    await _seed_user(test_db_session, user_id=uid)
    pro_pkg = 5630
    fp_pkg = 5631
    test_db_session.add(
        AssessmentPackage(
            package_id=pro_pkg,
            package_code="MET_PRO_FP",
            display_name="Pro FP",
            assessment_type_code="2",
            status="active",
        )
    )
    test_db_session.add(
        AssessmentPackage(
            package_id=fp_pkg,
            package_code="FP_SIB",
            display_name="FitPrint Sib",
            assessment_type_code="7",
            status="active",
        )
    )
    test_db_session.add(AssessmentPackageCategory(package_id=pro_pkg, category_id=1))
    test_db_session.add(AssessmentPackageCategory(package_id=fp_pkg, category_id=1))
    await test_db_session.commit()

    pro_aid = 5632
    fp_rid = "MS-FP-SIB"
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=pro_aid,
            user_id=uid,
            package_id=pro_pkg,
            engagement_id=1,
            status="active",
            metsights_record_id="MS-PRO-SIB",
        )
    )
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=5633,
            user_id=uid,
            package_id=fp_pkg,
            engagement_id=1,
            status="active",
            metsights_record_id=fp_rid,
        )
    )
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=pro_aid,
            question_id=15,
            category_id=3,
            answer="2",
            submitted_at=None,
        )
    )
    await test_db_session.commit()

    r = await async_client.post(f"/assessments/{pro_aid}/submit-legacy", headers=_auth_header(uid))
    assert r.status_code == 200

    fit = [c for c in calls if c[0] == fp_rid and c[1] == "fitness-parameters"]
    assert len(fit) == 1
    assert fit[0][2].get("exercise_frequency_week") == "2"
    assert fit[0][2].get("is_complete") is True
