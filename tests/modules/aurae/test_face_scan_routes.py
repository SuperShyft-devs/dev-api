"""Tests for POST /assessments/{id}/start-face-scan and VIFC quick-start."""

from __future__ import annotations

import json
from datetime import date, timedelta

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.aurae import client as aurae_client
from modules.engagements.models import EngagementParticipant
from modules.questionnaire.models import QuestionnaireResponse
from modules.users.models import User
from tests.modules.questionnaire.test_questionnaire_user_routes import _ensure_test_engagement


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_vifc_user(test_db_session, *, user_id: int, phone: str | None = None):
    test_db_session.add(
        User(
            user_id=user_id,
            age=30,
            phone=phone or f"{user_id}000000000"[:15],
            email=f"user{user_id}@example.com",
            first_name="Face",
            last_name="Scan",
            gender="male",
            date_of_birth=date(1995, 5, 15),
            status="active",
            is_participant=True,
        )
    )
    await test_db_session.commit()


async def _seed_vifc_package(test_db_session, *, package_id: int = 8801):
    test_db_session.add(
        AssessmentPackage(
            package_id=package_id,
            package_code="vifc",
            display_name="Aurae Face Scan",
            assessment_type_code="vifc",
            status="active",
        )
    )
    await test_db_session.commit()
    return package_id


async def _seed_anthropometry(test_db_session, *, assessment_instance_id: int):
    # Seeded Metsights questions: 1=height, 2=weight, 3=waist_circumference (category 1).
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=assessment_instance_id,
            question_id=1,
            category_id=1,
            answer={"value": 175.0, "unit": "0"},
        )
    )
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=assessment_instance_id,
            question_id=2,
            category_id=1,
            answer={"value": 70.0, "unit": "0"},
        )
    )
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=assessment_instance_id,
            question_id=3,
            category_id=1,
            answer={"value": 32.0, "unit": "1"},
        )
    )
    await test_db_session.commit()


def _configure_aurae(monkeypatch):
    monkeypatch.setattr(settings, "AURAE_BASE_URL", "https://aurae.test/dev")
    monkeypatch.setattr(settings, "AURAE_API_KEY", "test-api-key")
    monkeypatch.setattr(settings, "AURAE_ORG_CODE", "ORG123")
    aurae_client.clear_token_cache()


@pytest.mark.asyncio
async def test_start_face_scan_not_found_for_other_user(async_client, test_db_session):
    await _ensure_test_engagement(test_db_session)
    uid = 88101
    other = 88102
    await _seed_vifc_user(test_db_session, user_id=uid)
    await _seed_vifc_user(test_db_session, user_id=other, phone="88102000000000")
    pkg_id = await _seed_vifc_package(test_db_session, package_id=8811)

    aid = 8812
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=aid,
            user_id=uid,
            package_id=pkg_id,
            engagement_id=1,
            status="active",
        )
    )
    await test_db_session.commit()

    r = await async_client.post(f"/assessments/{aid}/start-face-scan", headers=_auth_header(other))
    assert r.status_code == 404
    assert r.json()["error_code"] == "ASSESSMENT_NOT_FOUND"


@pytest.mark.asyncio
async def test_start_face_scan_rejects_non_vifc_package(async_client, test_db_session):
    await _ensure_test_engagement(test_db_session)
    uid = 88103
    await _seed_vifc_user(test_db_session, user_id=uid)
    pkg_id = 8813
    test_db_session.add(
        AssessmentPackage(
            package_id=pkg_id,
            package_code="MET_BASIC",
            display_name="Not VIFC",
            assessment_type_code="1",
            status="active",
        )
    )
    await test_db_session.commit()

    aid = 8814
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=aid,
            user_id=uid,
            package_id=pkg_id,
            engagement_id=1,
            status="active",
        )
    )
    await test_db_session.commit()

    r = await async_client.post(f"/assessments/{aid}/start-face-scan", headers=_auth_header(uid))
    assert r.status_code == 400
    assert "vifc" in r.json()["message"].lower()


@pytest.mark.asyncio
async def test_start_face_scan_returns_cached_link(async_client, test_db_session, monkeypatch):
    await _ensure_test_engagement(test_db_session)
    _configure_aurae(monkeypatch)
    uid = 88105
    await _seed_vifc_user(test_db_session, user_id=uid)
    pkg_id = await _seed_vifc_package(test_db_session, package_id=8815)

    aid = 8816
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=aid,
            user_id=uid,
            package_id=pkg_id,
            engagement_id=1,
            status="active",
        )
    )
    test_db_session.add(
        EngagementParticipant(
            engagement_id=1,
            user_id=uid,
            booked_by_user_id=uid,
            face_scan_link="https://integration.auraehealth.com?token=cached",
        )
    )
    await test_db_session.commit()

    called = {"token": 0, "onboard": 0}

    async def _fake_token(**kwargs):
        called["token"] += 1
        return "tok"

    async def _fake_onboard(**kwargs):
        called["onboard"] += 1
        return {"link": "https://should-not-call"}

    monkeypatch.setattr(aurae_client, "get_token", _fake_token)
    monkeypatch.setattr(aurae_client, "onboard_user", _fake_onboard)

    r = await async_client.post(f"/assessments/{aid}/start-face-scan", headers=_auth_header(uid))
    assert r.status_code == 200
    assert r.json()["data"]["link"] == "https://integration.auraehealth.com?token=cached"
    assert called["token"] == 0
    assert called["onboard"] == 0


@pytest.mark.asyncio
async def test_start_face_scan_calls_aurae_and_logs(async_client, test_db_session, monkeypatch):
    await _ensure_test_engagement(test_db_session)
    _configure_aurae(monkeypatch)
    uid = 88107
    await _seed_vifc_user(test_db_session, user_id=uid)
    pkg_id = await _seed_vifc_package(test_db_session, package_id=8817)

    aid = 8818
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=aid,
            user_id=uid,
            package_id=pkg_id,
            engagement_id=1,
            status="active",
        )
    )
    test_db_session.add(
        EngagementParticipant(
            engagement_id=1,
            user_id=uid,
            booked_by_user_id=uid,
        )
    )
    await test_db_session.commit()
    await _seed_anthropometry(test_db_session, assessment_instance_id=aid)

    onboard_payloads: list[dict] = []

    async def _fake_token(**kwargs):
        return "aurae-jwt-token"

    async def _fake_onboard(*, token: str, payload: dict):
        onboard_payloads.append(dict(payload))
        assert token == "aurae-jwt-token"
        return {
            "message": "User onboarded successfully",
            "link": "https://integration.auraehealth.com?token=newlink",
        }

    monkeypatch.setattr(aurae_client, "get_token", _fake_token)
    monkeypatch.setattr(aurae_client, "onboard_user", _fake_onboard)

    r = await async_client.post(f"/assessments/{aid}/start-face-scan", headers=_auth_header(uid))
    assert r.status_code == 200
    assert r.json()["data"]["link"] == "https://integration.auraehealth.com?token=newlink"

    assert len(onboard_payloads) == 1
    body = onboard_payloads[0]
    assert body["components"] == ["VIFC"]
    assert body["api_customer_id"] == str(aid)
    assert body["height"] == 175
    assert body["weight"] == 70
    assert body["waist"] == 32
    assert body["gender"] == "male"
    assert body["org_code"] == "ORG123"

    link_row = (
        await test_db_session.execute(
            text(
                "SELECT face_scan_link FROM engagement_participants "
                "WHERE user_id = :uid AND engagement_id = 1"
            ),
            {"uid": uid},
        )
    ).first()
    assert link_row.face_scan_link == "https://integration.auraehealth.com?token=newlink"

    logs = (
        await test_db_session.execute(
            text(
                "SELECT provider, status, api_endpoint_url FROM integration_sync_logs "
                "WHERE user_id = :uid AND provider = 'aurae' ORDER BY sync_log_id"
            ),
            {"uid": uid},
        )
    ).fetchall()
    assert len(logs) == 2
    assert all(row.status == "success" for row in logs)
    assert "token" in logs[0].api_endpoint_url
    assert "onboard" in logs[1].api_endpoint_url


@pytest.mark.asyncio
async def test_vifc_quick_start_returns_link(async_client, test_db_session, monkeypatch):
    _configure_aurae(monkeypatch)

    pkg_id = 8820
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status) "
            "VALUES (:pid, 'vifc', 'Aurae Face Scan', 'vifc', 'active') "
            "ON CONFLICT (package_id) DO UPDATE SET "
            "package_code = 'vifc', assessment_type_code = 'vifc', status = 'active'"
        ),
        {"pid": pkg_id},
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'active') ON CONFLICT (diagnostic_package_id) DO UPDATE SET status = 'active'"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_types (code, display_name, is_active) "
            "VALUES ('vifc', 'Aurae - Face Scan', true) "
            "ON CONFLICT (code) DO UPDATE SET display_name = EXCLUDED.display_name, is_active = true"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_package_categories (package_id, category_id) "
            "SELECT :pid, 1 WHERE NOT EXISTS ("
            "  SELECT 1 FROM assessment_package_categories "
            "  WHERE package_id = :pid AND category_id = 1"
            ")"
        ),
        {"pid": pkg_id},
    )
    by_type = {
        "vifc": {
            "assessment_package_id": pkg_id,
            "diagnostic_package_id": 1,
            "blood_collection_type": None,
            "create_profile_on_metsights": False,
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
            "VALUES (1, :pid, 1, 'bio_ai', NULL, false, false, CAST(:by_type AS jsonb)) "
            "ON CONFLICT (settings_id) DO UPDATE SET "
            "b2c_default_assessment_package_id = EXCLUDED.b2c_default_assessment_package_id, "
            "b2c_default_diagnostic_package_id = EXCLUDED.b2c_default_diagnostic_package_id, "
            "b2c_default_engagement_type = EXCLUDED.b2c_default_engagement_type, "
            "b2c_default_create_profile_on_metsights = EXCLUDED.b2c_default_create_profile_on_metsights, "
            "b2c_default_enroll_for_fitprint_full = EXCLUDED.b2c_default_enroll_for_fitprint_full, "
            "b2c_onboarding_by_engagement_type = EXCLUDED.b2c_onboarding_by_engagement_type"
        ),
        {"pid": pkg_id, "by_type": json.dumps(by_type)},
    )
    await test_db_session.commit()

    async def _fake_token(**kwargs):
        return "quick-token"

    async def _fake_onboard(*, token: str, payload: dict):
        return {"message": "ok", "link": "https://integration.auraehealth.com?token=quick"}

    monkeypatch.setattr(aurae_client, "get_token", _fake_token)
    monkeypatch.setattr(aurae_client, "onboard_user", _fake_onboard)

    payload = {
        "age": 28,
        "first_name": "Quick",
        "last_name": "Start",
        "email": "vifc.quick@example.com",
        "phone": "9876500123",
        "gender": "female",
        "dob": "1998-01-20",
        "city": "Mumbai",
        "questionnaire": {
            "anthropometry": {
                "responses": [
                    {"question_id": 1, "answer": {"value": 165, "unit": "0"}},
                    {"question_id": 2, "answer": {"value": 60, "unit": "0"}},
                    {"question_id": 3, "answer": {"value": 28, "unit": "1"}},
                ]
            }
        },
    }

    response = await async_client.post("/users/public/vifc/quick-start", json=payload)
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert data["link"] == "https://integration.auraehealth.com?token=quick"
    assert data["face_scan_link"] == data["link"]
    assert data["assessment_instance_id"] is not None
    assert data["engagement_participant_id"] is not None

    link_row = (
        await test_db_session.execute(
            text(
                "SELECT face_scan_link FROM engagement_participants "
                "WHERE engagement_participant_id = :epid"
            ),
            {"epid": data["engagement_participant_id"]},
        )
    ).first()
    assert link_row.face_scan_link == "https://integration.auraehealth.com?token=quick"


@pytest.mark.asyncio
async def test_vifc_quick_start_by_user_id(async_client, test_db_session, monkeypatch):
    _configure_aurae(monkeypatch)

    pkg_id = 8821
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status) "
            "VALUES (:pid, 'vifc', 'Aurae Face Scan', 'vifc', 'active') "
            "ON CONFLICT (package_id) DO UPDATE SET "
            "package_code = 'vifc', assessment_type_code = 'vifc', status = 'active'"
        ),
        {"pid": pkg_id},
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'active') ON CONFLICT (diagnostic_package_id) DO UPDATE SET status = 'active'"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_types (code, display_name, is_active) "
            "VALUES ('vifc', 'Aurae - Face Scan', true) "
            "ON CONFLICT (code) DO UPDATE SET is_active = true"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_package_categories (package_id, category_id) "
            "SELECT :pid, 1 WHERE NOT EXISTS ("
            "  SELECT 1 FROM assessment_package_categories "
            "  WHERE package_id = :pid AND category_id = 1"
            ")"
        ),
        {"pid": pkg_id},
    )
    by_type = {
        "vifc": {
            "assessment_package_id": pkg_id,
            "diagnostic_package_id": 1,
            "blood_collection_type": None,
            "create_profile_on_metsights": False,
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
            "VALUES (1, :pid, 1, 'bio_ai', NULL, false, false, CAST(:by_type AS jsonb)) "
            "ON CONFLICT (settings_id) DO UPDATE SET "
            "b2c_onboarding_by_engagement_type = EXCLUDED.b2c_onboarding_by_engagement_type, "
            "b2c_default_assessment_package_id = EXCLUDED.b2c_default_assessment_package_id, "
            "b2c_default_diagnostic_package_id = EXCLUDED.b2c_default_diagnostic_package_id, "
            "b2c_default_create_profile_on_metsights = false, "
            "b2c_default_enroll_for_fitprint_full = false"
        ),
        {"pid": pkg_id, "by_type": json.dumps(by_type)},
    )
    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, first_name, last_name, age, phone, email, gender, date_of_birth, "
            "city, is_participant, status) "
            "VALUES (88250, 'Existing', 'User', 35, '9876500999', 'existing.vifc@example.com', 'male', "
            "'1990-03-01', 'Pune', true, 'active') "
            "ON CONFLICT (user_id) DO NOTHING"
        )
    )
    await test_db_session.commit()

    async def _fake_token(**kwargs):
        return "uid-token"

    async def _fake_onboard(*, token: str, payload: dict):
        assert payload["email"] == "existing.vifc@example.com"
        return {"message": "ok", "link": "https://integration.auraehealth.com?token=byuid"}

    monkeypatch.setattr(aurae_client, "get_token", _fake_token)
    monkeypatch.setattr(aurae_client, "onboard_user", _fake_onboard)

    payload = {
        "user_id": 88250,
        "questionnaire": {
            "anthropometry": {
                "responses": [
                    {"question_id": 1, "answer": {"value": 180, "unit": "0"}},
                    {"question_id": 2, "answer": {"value": 75, "unit": "0"}},
                    {"question_id": 3, "answer": {"value": 34, "unit": "1"}},
                ]
            }
        },
    }

    response = await async_client.post("/users/public/vifc/quick-start", json=payload)
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert data["user_id"] == 88250
    assert data["created"] is False
    assert data["link"] == "https://integration.auraehealth.com?token=byuid"
