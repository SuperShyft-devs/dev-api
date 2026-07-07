"""Tests for integration_sync_logs on nutrition API calls."""

from __future__ import annotations

import pytest
import httpx
from sqlalchemy import text

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


def _build_reports_service() -> ReportsService:
    return ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=_FakeMetsightsService(),  # type: ignore[arg-type]
        diagnostics_service=None,  # type: ignore[arg-type]
        audit_service=AuditService(AuditRepository()),
        questionnaire_repository=QuestionnaireRepository(),
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
                json={"detail": "invalid payload"},
            )
            raise httpx.HTTPStatusError("bad request", request=request, response=response)

    return _FakeClient


@pytest.mark.asyncio
async def test_call_nutrition_api_creates_integration_sync_log_on_success(
    test_db_session, monkeypatch
):
    await _seed_user(test_db_session, user_id=8801)
    await _seed_engagement(test_db_session, engagement_id=9901)
    payload = {"diet_preference": "vegetarian", "water_intake_frequency": "often"}
    monkeypatch.setattr(
        "modules.reports.service.httpx.AsyncClient",
        _fake_httpx_client_success(nutrition_score=82),
    )

    service = _build_reports_service()
    response = await service._call_nutrition_api(
        test_db_session,
        payload,
        user_id=8801,
        engagement_id=9901,
    )
    assert response == {"nutrition_score": 82}
    await test_db_session.commit()

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
async def test_call_nutrition_api_creates_integration_sync_log_on_failure(
    test_db_session, monkeypatch
):
    await _seed_user(test_db_session, user_id=8802)
    await _seed_engagement(test_db_session, engagement_id=9902)
    payload = {"diet_preference": "invalid"}
    monkeypatch.setattr(
        "modules.reports.service.httpx.AsyncClient",
        _fake_httpx_client_client_error(),
    )

    service = _build_reports_service()
    with pytest.raises(AppError) as exc_info:
        await service._call_nutrition_api(
            test_db_session,
            payload,
            user_id=8802,
            engagement_id=9902,
        )
    assert exc_info.value.error_code == "INVALID_INPUT"
    await test_db_session.commit()

    result = await test_db_session.execute(
        text(
            "SELECT status, error_message, response_payload, request_payload "
            "FROM integration_sync_logs WHERE provider = 'nutrition_api' "
            "ORDER BY sync_log_id DESC LIMIT 1"
        )
    )
    row = result.mappings().one()
    assert row["status"] == "failed"
    assert row["error_message"] == "invalid payload"
    assert row["response_payload"] is None
    assert row["request_payload"] == payload
