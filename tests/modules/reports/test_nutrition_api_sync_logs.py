"""Tests for integration_sync_logs on local Nutrition Intelligence calls."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

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


@pytest.mark.asyncio
async def test_call_nutrition_api_creates_integration_sync_log_on_success(
    test_db_session, test_engine
):
    await _seed_user(test_db_session, user_id=8801)
    payload = {
        "diet_preference": "1",
        "food_groups": ["0", "1", "2", "5"],
        "fresh_fruit_frequency": "0",
        "fresh_vegetable_frequency": "0",
        "water_intake_frequency": "4",
        "health_priorities": ["0"],
    }

    session_factory = async_sessionmaker(bind=test_engine, class_=AsyncSession, expire_on_commit=False)
    service = _build_reports_service(session_factory=session_factory)
    response = await service._call_nutrition_api(
        test_db_session,
        payload,
        user_id=8801,
        engagement_id=None,
    )
    assert isinstance(response.get("nutrition_score"), (int, float))
    assert "carbs" in response
    assert "protein" in response
    assert "fats" in response
    assert "fibre" in response
    assert "water" in response

    result = await test_db_session.execute(
        text(
            "SELECT provider, engagement_id, user_id, api_endpoint_url, request_payload, "
            "response_payload, status, error_message "
            "FROM integration_sync_logs WHERE provider = 'nutrition_intelligence' "
            "ORDER BY sync_log_id DESC LIMIT 1"
        )
    )
    row = result.mappings().one()
    assert row["provider"] == "nutrition_intelligence"
    assert row["engagement_id"] is None
    assert row["user_id"] == 8801
    assert row["api_endpoint_url"] == "local://nutrition_intelligence"
    assert row["request_payload"] == payload
    assert row["status"] == "success"
    assert row["response_payload"]["nutrition_score"] == response["nutrition_score"]
    assert row["error_message"] is None


@pytest.mark.asyncio
async def test_call_nutrition_api_creates_integration_sync_log_on_failure_without_route_commit(
    test_db_session, test_engine, monkeypatch
):
    """Failed nutrition calls must persist sync logs even when the request session rolls back."""
    await _seed_user(test_db_session, user_id=8802)
    payload = {"diet_preference": "1"}

    def _boom(*args, **kwargs):
        raise RuntimeError("engine exploded")

    monkeypatch.setattr(
        "modules.reports.nutrition_intelligence.engine.run_nutrition_intelligence_from_lookup",
        _boom,
    )

    session_factory = async_sessionmaker(bind=test_engine, class_=AsyncSession, expire_on_commit=False)
    service = _build_reports_service(session_factory=session_factory)
    with pytest.raises(AppError) as exc_info:
        await service._call_nutrition_api(
            test_db_session,
            payload,
            user_id=8802,
            engagement_id=None,
        )
    assert exc_info.value.error_code == "INTERNAL_ERROR"

    await test_db_session.rollback()

    result = await test_db_session.execute(
        text(
            "SELECT status, error_message, response_payload, request_payload "
            "FROM integration_sync_logs WHERE provider = 'nutrition_intelligence' "
            "AND user_id = 8802 "
            "ORDER BY sync_log_id DESC LIMIT 1"
        )
    )
    row = result.mappings().one()
    assert row["status"] == "failed"
    assert "engine exploded" in (row["error_message"] or "")
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
        option_reverse_map=reverse_map,
    )
    assert payload["exercise_level"] == "1"
    assert payload["food_groups"] == ["3"]
    assert payload["height"] == 175
    assert payload["height_unit"] == "cm"
    assert payload["gender"] == "female"
