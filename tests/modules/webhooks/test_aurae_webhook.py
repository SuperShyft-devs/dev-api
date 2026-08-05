"""Tests for POST /webhooks/aurae."""

from __future__ import annotations

import pytest
from sqlalchemy import select, text

from core.config import settings
from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.reports.models import IndividualHealthReport
from modules.users.models import User
from tests.modules.questionnaire.test_questionnaire_user_routes import _ensure_test_engagement

_WEBHOOK_KEY = "aurae-webhook-test-key"
_REPORT_URL = (
    "https://reportsauraehealth.s3.ap-south-1.amazonaws.com/auraeaireports/af1b-9ae41d1b9b05.pdf"
)


def _auth_headers(*, api_key: str = _WEBHOOK_KEY) -> dict[str, str]:
    return {"x-api-key": api_key}


def _configure_webhook_auth(monkeypatch, *, webhook_key: str = _WEBHOOK_KEY, api_key: str = ""):
    monkeypatch.setattr(settings, "AURAE_WEBHOOK_API_KEY", webhook_key)
    monkeypatch.setattr(settings, "AURAE_API_KEY", api_key)


def _results_payload(*, api_customer_id: str, hr: int = 79) -> dict:
    return {
        "api_customer_id": api_customer_id,
        "data": {
            "HR": hr,
            "BP": "100/74",
            "Spo2": 96,
            "wellnessScore": 74,
            "scores": {"heartRateScore": 80, "spo2Score": 90},
            "categories": {"heartRateCategory": "High", "spo2Category": "High"},
            "scanType": "face",
            "statusCode": 200,
            "timestamp": "23 January 2025 at 13:58:07 IST+0530",
        },
    }


def _report_payload(*, api_customer_id: str, urls: list[str] | None = None) -> dict:
    return {
        "api_customer_id": api_customer_id,
        "orgCode": "AH1234",
        "requested_components": ["VIFC"],
        "reports": {"VIFC": urls or [_REPORT_URL]},
    }


async def _seed_vifc_user(test_db_session, *, user_id: int):
    test_db_session.add(
        User(
            user_id=user_id,
            age=30,
            phone=f"{user_id}000000000"[:15],
            email=f"user{user_id}@example.com",
            first_name="Aurae",
            last_name="Webhook",
            gender="male",
            status="active",
            is_participant=True,
        )
    )
    await test_db_session.commit()


async def _seed_vifc_package(test_db_session, *, package_id: int = 9901) -> int:
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


async def _seed_vifc_instance(
    test_db_session,
    *,
    assessment_instance_id: int,
    user_id: int,
    package_id: int,
    engagement_id: int = 1,
) -> None:
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=assessment_instance_id,
            user_id=user_id,
            package_id=package_id,
            engagement_id=engagement_id,
            status="active",
        )
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_results_webhook_creates_ihr_reports(async_client, test_db_session, monkeypatch):
    _configure_webhook_auth(monkeypatch)
    await _ensure_test_engagement(test_db_session)

    uid = 99101
    aid = 99102
    pkg_id = await _seed_vifc_package(test_db_session, package_id=9910)
    await _seed_vifc_user(test_db_session, user_id=uid)
    await _seed_vifc_instance(
        test_db_session,
        assessment_instance_id=aid,
        user_id=uid,
        package_id=pkg_id,
    )

    payload = _results_payload(api_customer_id=str(aid))
    response = await async_client.post("/webhooks/aurae", json=payload, headers=_auth_headers())
    assert response.status_code == 200, response.text

    body = response.json()["data"]
    assert body["received"] is True
    assert body["event"] == "results"
    assert body["assessment_instance_id"] == aid
    assert body["report_id"] is not None
    assert body["sync_log_id"] is not None

    ihr = (
        await test_db_session.execute(
            select(IndividualHealthReport).where(IndividualHealthReport.assessment_instance_id == aid)
        )
    ).scalar_one()
    assert ihr.user_id == uid
    assert ihr.engagement_id == 1
    assert ihr.reports["HR"] == 79
    assert ihr.reports["wellnessScore"] == 74
    assert ihr.report_url is None

    log_row = (
        await test_db_session.execute(
            text(
                "SELECT provider, status, user_id, engagement_id, api_endpoint_url "
                "FROM integration_sync_logs WHERE sync_log_id = :sid"
            ),
            {"sid": body["sync_log_id"]},
        )
    ).mappings().one()
    assert log_row["provider"] == "aurae"
    assert log_row["status"] == "success"
    assert log_row["user_id"] == uid
    assert log_row["engagement_id"] == 1
    assert log_row["api_endpoint_url"] == "/webhooks/aurae"


@pytest.mark.asyncio
async def test_report_webhook_sets_report_url(async_client, test_db_session, monkeypatch):
    _configure_webhook_auth(monkeypatch)
    await _ensure_test_engagement(test_db_session)

    uid = 99111
    aid = 99112
    pkg_id = await _seed_vifc_package(test_db_session, package_id=9911)
    await _seed_vifc_user(test_db_session, user_id=uid)
    await _seed_vifc_instance(
        test_db_session,
        assessment_instance_id=aid,
        user_id=uid,
        package_id=pkg_id,
    )

    payload = _report_payload(api_customer_id=str(aid))
    response = await async_client.post("/webhooks/aurae", json=payload, headers=_auth_headers())
    assert response.status_code == 200, response.text

    body = response.json()["data"]
    assert body["event"] == "report"
    assert body["assessment_instance_id"] == aid

    ihr = (
        await test_db_session.execute(
            select(IndividualHealthReport).where(IndividualHealthReport.assessment_instance_id == aid)
        )
    ).scalar_one()
    assert ihr.user_id == uid
    assert ihr.engagement_id == 1
    assert ihr.report_url == _REPORT_URL
    assert ihr.reports is None


@pytest.mark.asyncio
async def test_results_then_report_preserves_both_fields(async_client, test_db_session, monkeypatch):
    _configure_webhook_auth(monkeypatch)
    await _ensure_test_engagement(test_db_session)

    uid = 99121
    aid = 99122
    pkg_id = await _seed_vifc_package(test_db_session, package_id=9912)
    await _seed_vifc_user(test_db_session, user_id=uid)
    await _seed_vifc_instance(
        test_db_session,
        assessment_instance_id=aid,
        user_id=uid,
        package_id=pkg_id,
    )

    r1 = await async_client.post(
        "/webhooks/aurae",
        json=_results_payload(api_customer_id=str(aid), hr=88),
        headers=_auth_headers(),
    )
    assert r1.status_code == 200, r1.text

    r2 = await async_client.post(
        "/webhooks/aurae",
        json=_report_payload(api_customer_id=str(aid)),
        headers=_auth_headers(),
    )
    assert r2.status_code == 200, r2.text

    assert r1.json()["data"]["report_id"] == r2.json()["data"]["report_id"]

    ihr = (
        await test_db_session.execute(
            select(IndividualHealthReport).where(IndividualHealthReport.assessment_instance_id == aid)
        )
    ).scalar_one()
    assert ihr.reports["HR"] == 88
    assert ihr.report_url == _REPORT_URL


@pytest.mark.asyncio
async def test_report_then_results_preserves_both_fields(async_client, test_db_session, monkeypatch):
    _configure_webhook_auth(monkeypatch)
    await _ensure_test_engagement(test_db_session)

    uid = 99131
    aid = 99132
    pkg_id = await _seed_vifc_package(test_db_session, package_id=9913)
    await _seed_vifc_user(test_db_session, user_id=uid)
    await _seed_vifc_instance(
        test_db_session,
        assessment_instance_id=aid,
        user_id=uid,
        package_id=pkg_id,
    )

    r1 = await async_client.post(
        "/webhooks/aurae",
        json=_report_payload(api_customer_id=str(aid)),
        headers=_auth_headers(),
    )
    assert r1.status_code == 200, r1.text

    r2 = await async_client.post(
        "/webhooks/aurae",
        json=_results_payload(api_customer_id=str(aid), hr=72),
        headers=_auth_headers(),
    )
    assert r2.status_code == 200, r2.text

    ihr = (
        await test_db_session.execute(
            select(IndividualHealthReport).where(IndividualHealthReport.assessment_instance_id == aid)
        )
    ).scalar_one()
    assert ihr.reports["HR"] == 72
    assert ihr.report_url == _REPORT_URL


@pytest.mark.asyncio
async def test_missing_api_customer_id_returns_422(async_client, test_db_session, monkeypatch):
    _configure_webhook_auth(monkeypatch)

    payload = _results_payload(api_customer_id="")
    response = await async_client.post("/webhooks/aurae", json=payload, headers=_auth_headers())
    assert response.status_code == 422, response.text
    assert response.json()["error_code"] == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_non_numeric_api_customer_id_returns_422(async_client, test_db_session, monkeypatch):
    _configure_webhook_auth(monkeypatch)

    payload = _results_payload(api_customer_id="AH12345")
    response = await async_client.post("/webhooks/aurae", json=payload, headers=_auth_headers())
    assert response.status_code == 422, response.text
    assert response.json()["error_code"] == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_unknown_assessment_instance_returns_404(async_client, test_db_session, monkeypatch):
    _configure_webhook_auth(monkeypatch)

    payload = _results_payload(api_customer_id="99999901")
    response = await async_client.post("/webhooks/aurae", json=payload, headers=_auth_headers())
    assert response.status_code == 404, response.text
    assert response.json()["error_code"] == "ASSESSMENT_NOT_FOUND"


@pytest.mark.asyncio
async def test_missing_x_api_key_returns_401(async_client, test_db_session, monkeypatch):
    _configure_webhook_auth(monkeypatch)

    payload = _results_payload(api_customer_id="1")
    response = await async_client.post("/webhooks/aurae", json=payload)
    assert response.status_code == 401, response.text
    assert response.json()["error_code"] == "AUTH_FAILED"


@pytest.mark.asyncio
async def test_wrong_x_api_key_returns_401(async_client, test_db_session, monkeypatch):
    _configure_webhook_auth(monkeypatch)

    payload = _results_payload(api_customer_id="1")
    response = await async_client.post(
        "/webhooks/aurae",
        json=payload,
        headers=_auth_headers(api_key="wrong-key"),
    )
    assert response.status_code == 401, response.text
    assert response.json()["error_code"] == "AUTH_FAILED"


@pytest.mark.asyncio
async def test_falls_back_to_aurae_api_key(async_client, test_db_session, monkeypatch):
    _configure_webhook_auth(monkeypatch, webhook_key="", api_key="outbound-aurae-key")
    await _ensure_test_engagement(test_db_session)

    uid = 99141
    aid = 99142
    pkg_id = await _seed_vifc_package(test_db_session, package_id=9914)
    await _seed_vifc_user(test_db_session, user_id=uid)
    await _seed_vifc_instance(
        test_db_session,
        assessment_instance_id=aid,
        user_id=uid,
        package_id=pkg_id,
    )

    response = await async_client.post(
        "/webhooks/aurae",
        json=_report_payload(api_customer_id=str(aid)),
        headers=_auth_headers(api_key="outbound-aurae-key"),
    )
    assert response.status_code == 200, response.text
    assert response.json()["data"]["event"] == "report"


@pytest.mark.asyncio
async def test_empty_payload_returns_422(async_client, test_db_session, monkeypatch):
    _configure_webhook_auth(monkeypatch)

    response = await async_client.post(
        "/webhooks/aurae",
        json={"api_customer_id": "1"},
        headers=_auth_headers(),
    )
    assert response.status_code == 422, response.text
    assert response.json()["error_code"] == "INVALID_INPUT"
