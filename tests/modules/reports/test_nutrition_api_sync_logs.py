"""Tests for integration_sync_logs on nutrition API calls."""

from __future__ import annotations

import pytest
import httpx
from datetime import date
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from core.config import settings
from core.exceptions import AppError
from modules.assessments.repository import AssessmentsRepository
from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.questionnaire.repository import QuestionnaireRepository
from modules.reports.repository import ReportsRepository
from modules.reports.service import ReportsService
from tests.modules.questionnaire.test_questionnaire_user_routes import _seed_user


class _FakeMetsightsService:
    pass


async def _seed_engagement(test_db_session, *, engagement_id: int):
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'Test Diagnostic', 'test_provider', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status) "
            "VALUES (1, 'FITPRINT', 'FitPrint', '7', 'active') "
            "ON CONFLICT (package_id) DO UPDATE SET assessment_type_code = EXCLUDED.assessment_type_code"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements (engagement_id, engagement_name, engagement_code, engagement_type, "
            "assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, "
            "status, organization_id) "
            "VALUES (:eid, 'Nutrition Log Camp', 'ENG-NUT-LOG', 'bio_ai', 1, 1, 'BLR', 20, "
            "'2026-02-01', '2026-02-28', 'running', 0, NULL) "
            "ON CONFLICT (engagement_id) DO NOTHING"
        ),
        {"eid": engagement_id},
    )
    await test_db_session.commit()


def _build_reports_service(session_factory=None) -> ReportsService:
    return ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=_FakeMetsightsService(),  # type: ignore[arg-type]
        diagnostics_service=None,  # type: ignore[arg-type]
        audit_service=AuditService(AuditRepository()),
        questionnaire_repository=QuestionnaireRepository(),
        session_factory=session_factory,
    )


def _fake_httpx_client_success(*, nutrition_score: int = 75):
    class _FakeResponse:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {"nutrition_score": nutrition_score}

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, json=None, headers=None):
            return _FakeResponse()

    return _FakeClient


def _fake_httpx_client_client_error():
    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, json=None, headers=None):
            request = httpx.Request("POST", url)
            response = httpx.Response(
                400,
                request=request,
                json={"detail": "invalid literal for int() with base 10: 'Moderate-intensity'"},
            )
            raise httpx.HTTPStatusError("bad request", request=request, response=response)

    return _FakeClient


@pytest.mark.asyncio
async def test_call_nutrition_api_creates_integration_sync_log_on_success(
    test_db_session, test_engine, monkeypatch
):
    await _seed_user(test_db_session, user_id=8801)
    await _seed_engagement(test_db_session, engagement_id=9901)
    payload = {"diet_preference": "vegetarian", "water_intake_frequency": "often"}
    monkeypatch.setattr(
        "modules.reports.service.httpx.AsyncClient",
        _fake_httpx_client_success(nutrition_score=82),
    )

    session_factory = async_sessionmaker(bind=test_engine, class_=AsyncSession, expire_on_commit=False)
    service = _build_reports_service(session_factory=session_factory)
    response = await service._call_nutrition_api(
        test_db_session,
        payload,
        user_id=8801,
        engagement_id=9901,
    )
    assert response == {"nutrition_score": 82}

    result = await test_db_session.execute(
        text(
            "SELECT provider, engagement_id, user_id, api_endpoint_url, request_payload, "
            "response_payload, status, error_message "
            "FROM integration_sync_logs WHERE provider = 'nutrition_api' "
            "ORDER BY sync_log_id DESC LIMIT 1"
        )
    )
    row = result.mappings().one()
    assert row["provider"] == "nutrition_api"
    assert row["engagement_id"] == 9901
    assert row["user_id"] == 8801
    assert row["api_endpoint_url"] == settings.NUTRITION_API_URL
    assert row["request_payload"] == payload
    assert row["status"] == "success"
    assert row["response_payload"] == {"nutrition_score": 82}
    assert row["error_message"] is None


@pytest.mark.asyncio
async def test_call_nutrition_api_creates_integration_sync_log_on_failure_without_route_commit(
    test_db_session, test_engine, monkeypatch
):
    """Failed nutrition calls must persist sync logs even when the request session rolls back."""
    await _seed_user(test_db_session, user_id=8802)
    await _seed_engagement(test_db_session, engagement_id=9902)
    payload = {"exercise_level": "Moderate-intensity"}
    monkeypatch.setattr(
        "modules.reports.service.httpx.AsyncClient",
        _fake_httpx_client_client_error(),
    )

    session_factory = async_sessionmaker(bind=test_engine, class_=AsyncSession, expire_on_commit=False)
    service = _build_reports_service(session_factory=session_factory)
    with pytest.raises(AppError) as exc_info:
        await service._call_nutrition_api(
            test_db_session,
            payload,
            user_id=8802,
            engagement_id=9902,
        )
    assert exc_info.value.error_code == "INVALID_INPUT"
    assert "[exercise_level]" in exc_info.value.message

    # Simulate route rollback of the request session (do NOT commit test_db_session).
    await test_db_session.rollback()

    result = await test_db_session.execute(
        text(
            "SELECT status, error_message, response_payload, request_payload "
            "FROM integration_sync_logs WHERE provider = 'nutrition_api' "
            "AND user_id = 8802 "
            "ORDER BY sync_log_id DESC LIMIT 1"
        )
    )
    row = result.mappings().one()
    assert row["status"] == "failed"
    assert "Moderate-intensity" in (row["error_message"] or "")
    assert row["response_payload"] is None
    assert row["request_payload"] == payload


def test_resolve_nutrition_choice_maps_label_variants_to_option_value():
    key_map = {
        "0": "0",
        "1": "1",
        "2": "2",
        "Low": "0",
        "Moderate": "1",
        "High": "2",
        "low": "0",
        "moderate": "1",
        "high": "2",
    }

    assert ReportsService._resolve_nutrition_choice_value("1", key_map) == "1"
    assert ReportsService._resolve_nutrition_choice_value("Moderate", key_map) == "1"
    assert ReportsService._resolve_nutrition_choice_value("Moderate-intensity", key_map) == "1"
    assert ReportsService._resolve_nutrition_choice_value("Low-intensity", key_map) == "0"
    assert ReportsService._resolve_nutrition_choice_value("High-intensity", key_map) == "2"


def test_build_nutrition_api_payload_remaps_legacy_intensity_labels():
    service = _build_reports_service()
    reverse_map = {
        "exercise_level": {
            "0": "0",
            "1": "1",
            "2": "2",
            "Low": "0",
            "Moderate": "1",
            "High": "2",
            "low": "0",
            "moderate": "1",
            "high": "2",
        },
        "food_groups": {
            "3": "3",
            "Fresh vegetables": "3",
            "freshvegetables": "3",
        },
    }
    lookup = {
        "exercise_level": "Moderate-intensity",
        "food_groups": ["Fresh vegetables"],
        "height": {"value": 175.0, "unit": "cm"},
    }
    payload = service._build_nutrition_api_payload(
        lookup,
        user_gender="female",
        user_age=32,
        option_reverse_map=reverse_map,
    )
    assert payload["exercise_level"] == "1"
    assert payload["food_groups"] == ["3"]
    assert payload["height"] == 175
    assert payload["height_unit"] == "cm"
    assert payload["gender"] == "female"
    assert payload["age"] == 32


def test_resolve_nutrition_age_prefers_stored_age():
    assert ReportsService._resolve_nutrition_age(user_age=40, user_date_of_birth=date(1990, 1, 1)) == 40


def test_resolve_nutrition_age_from_date_of_birth():
    dob = date(1990, 6, 15)
    ref = date(2026, 8, 24)
    assert ReportsService._resolve_nutrition_age(
        user_age=None,
        user_date_of_birth=dob,
        reference_date=ref,
    ) == 36


def test_resolve_nutrition_age_returns_none_when_missing():
    assert ReportsService._resolve_nutrition_age(user_age=None, user_date_of_birth=None) is None


def test_build_nutrition_api_payload_omits_age_when_unresolvable():
    service = _build_reports_service()
    payload = service._build_nutrition_api_payload(
        {},
        user_gender="male",
        user_age=None,
        user_date_of_birth=None,
    )
    assert "age" not in payload


def test_build_nutrition_api_payload_computes_age_from_date_of_birth():
    service = _build_reports_service()
    dob = date(1990, 6, 15)
    payload = service._build_nutrition_api_payload(
        {},
        user_date_of_birth=dob,
    )
    expected = ReportsService._resolve_nutrition_age(user_age=None, user_date_of_birth=dob)
    assert payload["age"] == expected
