"""Tests for POST /notifications/dispatch report URL validation."""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.reports.models import IndividualHealthReport
from modules.users.models import User

TEST_NOTIFICATION_API_KEY = "test-notif-dispatch-key"


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token(
        {"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY
    )
    return {"Authorization": f"Bearer {token}"}


def _api_key_header() -> dict[str, str]:
    return {"x-api-key": TEST_NOTIFICATION_API_KEY}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int = 1):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


async def _seed_metsights_basic_package(test_db_session, *, package_id: int = 1):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status) "
            "VALUES (:pid, 'METSIGHTS_BASIC', 'Metsights Basic', '1', 'active') "
            "ON CONFLICT (package_id) DO UPDATE SET assessment_type_code = EXCLUDED.assessment_type_code"
        ),
        {"pid": package_id},
    )
    await test_db_session.commit()


async def _seed_diagnostic_package(test_db_session, *, diagnostic_package_id: int = 1):
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status, bookings_count) "
            "VALUES (:did, :ref, :pname, 'test_provider', 'active', 0) ON CONFLICT (diagnostic_package_id) DO NOTHING"
        ),
        {"did": diagnostic_package_id, "ref": f"REF{diagnostic_package_id}", "pname": "Diag"},
    )
    await test_db_session.commit()


async def _seed_engagement(test_db_session, *, engagement_id: int, engagement_code: str):
    from modules.engagements.models import Engagement

    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Camp",
            engagement_code=engagement_code,
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 5, 1),
            end_date=date(2026, 5, 1),
            status="active",
        )
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_dispatch_succeeds_when_bio_ai_report_url_present(
    async_client, test_db_session, monkeypatch
):
    """Bio AI dispatch should succeed when individual_health_report has report_url."""
    await _seed_employee(test_db_session, user_id=9501, employee_id=951)
    await _seed_metsights_basic_package(test_db_session)
    await _seed_diagnostic_package(test_db_session)
    await _seed_engagement(test_db_session, engagement_id=9501, engagement_code="ENG-NOTIF-9501")

    test_db_session.add(User(user_id=9502, age=30, phone="9502000000", status="active"))
    await test_db_session.flush()

    from modules.assessments.models import AssessmentInstance

    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=9508,
            user_id=9502,
            package_id=1,
            engagement_id=9501,
            status="active",
            metsights_record_id="25D4C413C7D3",
        )
    )
    test_db_session.add(
        IndividualHealthReport(
            report_id=9508,
            user_id=9502,
            engagement_id=9501,
            assessment_instance_id=9508,
            report_url="https://example.com/bio-ai-report/9508",
        )
    )
    service_key = "bio_ai_report_whatsapp_test"
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, require_bio_ai_report_url, require_participant_detail, require_otp) "
            "VALUES (:sk, 'BioAI Report | Whatsapp', 'whatsapp', 'bio-ai-report', true, false, true, false, false) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true, require_bio_ai_report_url = true"
        ),
        {"sk": service_key},
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []

    class _FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"message": "ok"}

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, json=None):
            webhook_calls.append({"url": url, "json": json})
            return _FakeResponse()

    monkeypatch.setattr("modules.notifications.service.httpx.AsyncClient", _FakeClient)

    response = await async_client.post(
        "/notifications/dispatch",
        headers=_auth_header(9501),
        json={
            "service_key": service_key,
            "user_ids": [9502],
            "engagement_id": 9501,
        },
    )
    assert response.status_code == 201, response.text
    assert webhook_calls


@pytest.mark.asyncio
async def test_dispatch_validates_bio_ai_report_url_for_correct_engagement(
    async_client, test_db_session, monkeypatch
):
    """When engagement_id is set, report_url must come from that engagement's health report."""
    await _seed_employee(test_db_session, user_id=9521, employee_id=971)
    await _seed_metsights_basic_package(test_db_session)
    await _seed_diagnostic_package(test_db_session)
    await _seed_engagement(test_db_session, engagement_id=9521, engagement_code="ENG-NOTIF-9521")
    await _seed_engagement(test_db_session, engagement_id=9522, engagement_code="ENG-NOTIF-9522")

    test_db_session.add(User(user_id=9523, age=30, phone="9523000000", status="active"))
    await test_db_session.flush()

    from modules.assessments.models import AssessmentInstance

    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=9520,
            user_id=9523,
            package_id=1,
            engagement_id=9522,
            status="active",
            metsights_record_id="OTHER-ENG-RECORD",
        )
    )
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=9521,
            user_id=9523,
            package_id=1,
            engagement_id=9521,
            status="active",
            metsights_record_id="TARGET-ENG-RECORD",
        )
    )
    test_db_session.add(
        IndividualHealthReport(
            report_id=9521,
            user_id=9523,
            engagement_id=9521,
            assessment_instance_id=9521,
            report_url="https://example.com/bio-ai-report/target",
        )
    )
    service_key = "bio_ai_report_whatsapp_eng"
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, require_bio_ai_report_url, require_participant_detail, require_otp) "
            "VALUES (:sk, 'BioAI Report | Whatsapp', 'whatsapp', 'bio-ai-report', true, false, true, false, false) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true, require_bio_ai_report_url = true"
        ),
        {"sk": service_key},
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []

    class _FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"message": "ok"}

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, json=None):
            webhook_calls.append({"url": url, "json": json})
            return _FakeResponse()

    monkeypatch.setattr("modules.notifications.service.httpx.AsyncClient", _FakeClient)

    response = await async_client.post(
        "/notifications/dispatch",
        headers=_auth_header(9521),
        json={
            "service_key": service_key,
            "user_ids": [9523],
            "engagement_id": 9521,
        },
    )
    assert response.status_code == 201, response.text
    assert webhook_calls


@pytest.mark.asyncio
async def test_dispatch_requires_auth(async_client, test_db_session, monkeypatch):
    """POST /notifications/dispatch requires authentication (API key or employee JWT)."""
    response = await async_client.post(
        "/notifications/dispatch",
        json={
            "service_key": "any_service",
            "user_ids": [9531],
            "engagement_id": None,
        },
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_dispatch_with_api_key_auth(async_client, test_db_session, monkeypatch):
    """POST /notifications/dispatch works with x-api-key header."""
    monkeypatch.setattr(settings, "NOTIFICATION_API_KEY", TEST_NOTIFICATION_API_KEY)
    test_db_session.add(User(user_id=9531, age=30, phone="9531000000", status="active"))
    service_key = "api_key_dispatch_test"
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, require_bio_ai_report_url, require_participant_detail, require_otp) "
            "VALUES (:sk, 'API Key Dispatch', 'whatsapp', 'api-key-dispatch', true, false, false, false, false) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true"
        ),
        {"sk": service_key},
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []

    class _FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"message": "ok"}

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, json=None):
            webhook_calls.append({"url": url, "json": json})
            return _FakeResponse()

    monkeypatch.setattr("modules.notifications.service.httpx.AsyncClient", _FakeClient)

    response = await async_client.post(
        "/notifications/dispatch",
        headers=_api_key_header(),
        json={
            "service_key": service_key,
            "user_ids": [9531],
            "engagement_id": None,
        },
    )
    assert response.status_code == 201, response.text
    assert webhook_calls


@pytest.mark.asyncio
async def test_dispatch_without_bio_ai_report_url_returns_400_when_required(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9511, employee_id=961)
    await _seed_metsights_basic_package(test_db_session)
    await _seed_diagnostic_package(test_db_session)
    await _seed_engagement(test_db_session, engagement_id=9511, engagement_code="ENG-NOTIF-9511")
    test_db_session.add(User(user_id=9512, age=30, phone="9512000000", status="active"))
    await test_db_session.flush()

    from modules.assessments.models import AssessmentInstance

    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=9513,
            user_id=9512,
            package_id=1,
            engagement_id=9511,
            status="active",
            metsights_record_id="MISSING-REPORT-RECORD",
        )
    )
    missing_service_key = "bio_ai_report_whatsapp_missing"
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, require_bio_ai_report_url, require_participant_detail, require_otp) "
            "VALUES (:sk, 'BioAI Report | Whatsapp', 'whatsapp', 'bio-ai-report', true, false, true, false, false) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true, require_bio_ai_report_url = true"
        ),
        {"sk": missing_service_key},
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/notifications/dispatch",
        headers=_auth_header(9511),
        json={
            "service_key": missing_service_key,
            "user_ids": [9512],
            "engagement_id": 9511,
        },
    )
    assert response.status_code == 400
    assert "report" in response.json()["message"].lower() or "url" in response.json()["message"].lower()


@pytest.mark.asyncio
async def test_dispatch_requires_otp_when_service_flag_set(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "NOTIFICATION_API_KEY", TEST_NOTIFICATION_API_KEY)
    test_db_session.add(User(user_id=9541, age=30, phone="9541000000", status="active"))
    service_key = "otp_required_test"
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, require_bio_ai_report_url, require_participant_detail, require_otp) "
            "VALUES (:sk, 'OTP Required', 'whatsapp', 'otp-required', true, false, false, false, true) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true, require_otp = true"
        ),
        {"sk": service_key},
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/notifications/dispatch",
        headers=_api_key_header(),
        json={"service_key": service_key, "user_ids": [9541]},
    )
    assert response.status_code == 400
    assert "otp" in response.json()["message"].lower()


@pytest.mark.asyncio
async def test_dispatch_includes_otp_in_members_when_required(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "NOTIFICATION_API_KEY", TEST_NOTIFICATION_API_KEY)
    test_db_session.add(
        User(
            user_id=9542,
            age=30,
            phone="9542000000",
            status="active",
            email="user9542@example.com",
            first_name="John",
            last_name="Doe",
        )
    )
    service_key = "otp_dispatch_test"
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, require_bio_ai_report_url, require_participant_detail, require_otp) "
            "VALUES (:sk, 'OTP Dispatch', 'whatsapp', 'otp-dispatch', true, false, false, false, true) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true, require_otp = true"
        ),
        {"sk": service_key},
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []

    class _FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"message": "ok"}

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, json=None):
            webhook_calls.append({"url": url, "json": json})
            return _FakeResponse()

    monkeypatch.setattr("modules.notifications.service.httpx.AsyncClient", _FakeClient)

    response = await async_client.post(
        "/notifications/dispatch",
        headers=_api_key_header(),
        json={"service_key": service_key, "user_ids": [9542], "otp": "787878"},
    )
    assert response.status_code == 201, response.text
    assert webhook_calls
    member = webhook_calls[0]["json"]["members"][0]
    assert member["otp"] == "787878"
    assert member["first_name"] == "John"
    assert member["last_name"] == "Doe"
    assert member["phone"] == "9542000000"
    assert member["email"] == "user9542@example.com"


@pytest.mark.asyncio
async def test_dispatch_omits_otp_in_members_when_not_required(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "NOTIFICATION_API_KEY", TEST_NOTIFICATION_API_KEY)
    test_db_session.add(User(user_id=9543, age=30, phone="9543000000", status="active"))
    service_key = "otp_not_required_test"
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, require_bio_ai_report_url, require_participant_detail, require_otp) "
            "VALUES (:sk, 'No OTP', 'whatsapp', 'no-otp', true, false, false, false, false) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true, require_otp = false"
        ),
        {"sk": service_key},
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []

    class _FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"message": "ok"}

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, json=None):
            webhook_calls.append({"url": url, "json": json})
            return _FakeResponse()

    monkeypatch.setattr("modules.notifications.service.httpx.AsyncClient", _FakeClient)

    response = await async_client.post(
        "/notifications/dispatch",
        headers=_api_key_header(),
        json={"service_key": service_key, "user_ids": [9543], "otp": "787878"},
    )
    assert response.status_code == 201, response.text
    member = webhook_calls[0]["json"]["members"][0]
    assert "otp" not in member


async def _seed_simple_dispatch_service(
    test_db_session, *, user_id: int, service_key: str, webhook_path: str = "test-webhook"
):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}0000000", status="active"))
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, require_bio_ai_report_url, require_participant_detail, require_otp) "
            "VALUES (:sk, 'Sync Log Test', 'email', :wp, true, false, false, false, false) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true, webhook_path = EXCLUDED.webhook_path"
        ),
        {"sk": service_key, "wp": webhook_path},
    )
    await test_db_session.commit()


def _fake_httpx_client(*, succeed: bool = True):
    class _FakeResponse:
        status_code = 200
        text = "ok"

        def raise_for_status(self):
            if not succeed:
                raise RuntimeError("webhook failed")

        def json(self):
            return {"message": "ok"}

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, json=None):
            return _FakeResponse()

    return _FakeClient


@pytest.mark.asyncio
async def test_dispatch_creates_n8n_sync_log_on_success(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "NOTIFICATION_API_KEY", TEST_NOTIFICATION_API_KEY)
    service_key = "sync_log_success_test"
    await _seed_simple_dispatch_service(
        test_db_session, user_id=9601, service_key=service_key, webhook_path="welcome-whatsapp"
    )

    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(succeed=True),
    )

    response = await async_client.post(
        "/notifications/dispatch",
        headers=_api_key_header(),
        json={"service_key": service_key, "user_ids": [9601]},
    )
    assert response.status_code == 201, response.text
    notification_id = response.json()["data"]["notification_id"]

    result = await test_db_session.execute(
        text(
            "SELECT provider, engagement_id, user_id, api_endpoint_url, request_payload, "
            "response_payload, status, error_message "
            "FROM integration_sync_logs WHERE provider = 'n8n' "
            "ORDER BY sync_log_id DESC LIMIT 1"
        )
    )
    row = result.mappings().one()
    assert row["provider"] == "n8n"
    assert row["engagement_id"] is None
    assert row["user_id"] is None
    assert row["api_endpoint_url"].endswith("/welcome-whatsapp")
    assert row["request_payload"]["notification_id"] == notification_id
    assert row["request_payload"]["members"]
    assert row["status"] == "success"
    assert row["response_payload"] == {"message": "ok"}
    assert row["error_message"] is None


@pytest.mark.asyncio
async def test_dispatch_creates_n8n_sync_log_on_failure(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(settings, "NOTIFICATION_API_KEY", TEST_NOTIFICATION_API_KEY)
    service_key = "sync_log_failure_test"
    await _seed_simple_dispatch_service(
        test_db_session, user_id=9602, service_key=service_key, webhook_path="failed-webhook"
    )

    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(succeed=False),
    )

    response = await async_client.post(
        "/notifications/dispatch",
        headers=_api_key_header(),
        json={"service_key": service_key, "user_ids": [9602]},
    )
    assert response.status_code == 201, response.text
    assert "Webhook call failed" in response.json()["data"]["message"]

    result = await test_db_session.execute(
        text(
            "SELECT status, error_message, response_payload "
            "FROM integration_sync_logs WHERE provider = 'n8n' "
            "ORDER BY sync_log_id DESC LIMIT 1"
        )
    )
    row = result.mappings().one()
    assert row["status"] == "failed"
    assert "webhook failed" in row["error_message"]
    assert row["response_payload"] is None


async def _seed_fitprint_package(test_db_session, *, package_id: int = 2):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status) "
            "VALUES (:pid, 'FITPRINT', 'FitPrint', '7', 'active') "
            "ON CONFLICT (package_id) DO UPDATE SET assessment_type_code = EXCLUDED.assessment_type_code"
        ),
        {"pid": package_id},
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_dispatch_uses_assessment_instance_id_over_engagement_fallback(
    async_client, test_db_session, monkeypatch
):
    """Selecting FitPrint instance must send FitPrint report URL, not Basic."""
    await _seed_employee(test_db_session, user_id=9701, employee_id=971)
    await _seed_metsights_basic_package(test_db_session, package_id=1)
    await _seed_fitprint_package(test_db_session, package_id=2)
    await _seed_diagnostic_package(test_db_session)
    await _seed_engagement(test_db_session, engagement_id=9701, engagement_code="ENG-NOTIF-9701")

    test_db_session.add(User(user_id=9702, age=30, phone="9702000000", status="active"))
    await test_db_session.flush()

    from modules.assessments.models import AssessmentInstance

    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=9703,
            user_id=9702,
            package_id=1,
            engagement_id=9701,
            status="active",
            metsights_record_id="BASIC-RECORD",
        )
    )
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=9704,
            user_id=9702,
            package_id=2,
            engagement_id=9701,
            status="completed",
            metsights_record_id="FITPRINT-RECORD",
        )
    )
    test_db_session.add(
        IndividualHealthReport(
            report_id=9703,
            user_id=9702,
            engagement_id=9701,
            assessment_instance_id=9703,
            report_url="https://example.com/bio-ai/basic",
        )
    )
    test_db_session.add(
        IndividualHealthReport(
            report_id=9704,
            user_id=9702,
            engagement_id=9701,
            assessment_instance_id=9704,
            report_url="https://example.com/bio-ai/fitprint",
        )
    )
    service_key = "bio_ai_fitprint_test"
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, require_bio_ai_report_url, require_participant_detail, require_otp) "
            "VALUES (:sk, 'BioAI FitPrint', 'whatsapp', 'bio-ai-fitprint', true, false, true, false, false) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true, require_bio_ai_report_url = true"
        ),
        {"sk": service_key},
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []

    class _FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"message": "ok"}

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, json=None):
            webhook_calls.append({"url": url, "json": json})
            return _FakeResponse()

    monkeypatch.setattr("modules.notifications.service.httpx.AsyncClient", _FakeClient)

    response = await async_client.post(
        "/notifications/dispatch",
        headers=_auth_header(9701),
        json={
            "service_key": service_key,
            "user_ids": [9702],
            "engagement_id": 9701,
            "assessment_instance_id": 9704,
        },
    )
    assert response.status_code == 201, response.text
    assert webhook_calls
    member = webhook_calls[0]["json"]["members"][0]
    assert member["bio_ai_report_url"] == "https://example.com/bio-ai/fitprint"


@pytest.mark.asyncio
async def test_dispatch_rejects_assessment_instance_id_for_wrong_user(
    async_client, test_db_session, monkeypatch
):
    await _seed_employee(test_db_session, user_id=9711, employee_id=981)
    await _seed_metsights_basic_package(test_db_session)
    await _seed_diagnostic_package(test_db_session)
    await _seed_engagement(test_db_session, engagement_id=9711, engagement_code="ENG-NOTIF-9711")

    test_db_session.add(User(user_id=9712, age=30, phone="9712000000", status="active"))
    test_db_session.add(User(user_id=9713, age=30, phone="9713000000", status="active"))
    await test_db_session.flush()

    from modules.assessments.models import AssessmentInstance

    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=9715,
            user_id=9713,
            package_id=1,
            engagement_id=9711,
            status="active",
            metsights_record_id="OTHER-USER-RECORD",
        )
    )
    service_key = "bio_ai_wrong_user_test"
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, require_bio_ai_report_url, require_participant_detail, require_otp) "
            "VALUES (:sk, 'BioAI Wrong User', 'whatsapp', 'bio-ai-wrong', true, false, true, false, false) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true, require_bio_ai_report_url = true"
        ),
        {"sk": service_key},
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/notifications/dispatch",
        headers=_auth_header(9711),
        json={
            "service_key": service_key,
            "user_ids": [9712],
            "engagement_id": 9711,
            "assessment_instance_id": 9715,
        },
    )
    assert response.status_code == 400
    assert "not found" in response.json()["message"].lower()


@pytest.mark.asyncio
async def test_dispatch_rejects_assessment_instance_id_with_multiple_users(
    async_client, test_db_session, monkeypatch
):
    monkeypatch.setattr(settings, "NOTIFICATION_API_KEY", TEST_NOTIFICATION_API_KEY)
    test_db_session.add(User(user_id=9721, age=30, phone="9721000000", status="active"))
    test_db_session.add(User(user_id=9722, age=30, phone="9722000000", status="active"))
    service_key = "multi_user_assessment_test"
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, require_bio_ai_report_url, require_participant_detail, require_otp) "
            "VALUES (:sk, 'Multi User', 'whatsapp', 'multi-user', true, false, false, false, false) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true"
        ),
        {"sk": service_key},
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/notifications/dispatch",
        headers=_api_key_header(),
        json={
            "service_key": service_key,
            "user_ids": [9721, 9722],
            "assessment_instance_id": 9999,
        },
    )
    assert response.status_code == 400
    assert "single-user" in response.json()["message"].lower()


@pytest.mark.asyncio
async def test_dispatch_non_report_without_scope_leaves_engagement_null(
    async_client, test_db_session, monkeypatch
):
    """Welcome-style dispatch with no engagement/assessment must not auto-resolve engagement."""
    monkeypatch.setattr(settings, "NOTIFICATION_API_KEY", TEST_NOTIFICATION_API_KEY)
    await _seed_metsights_basic_package(test_db_session)
    await _seed_diagnostic_package(test_db_session)
    await _seed_engagement(test_db_session, engagement_id=9731, engagement_code="ENG-NOTIF-9731")

    test_db_session.add(User(user_id=9732, age=30, phone="9732000000", status="active"))
    await test_db_session.flush()

    from modules.assessments.models import AssessmentInstance

    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=9733,
            user_id=9732,
            package_id=1,
            engagement_id=9731,
            status="active",
            metsights_record_id="AUTO-ENG-RECORD",
        )
    )
    service_key = "welcome_no_scope_test"
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, require_bio_ai_report_url, require_participant_detail, require_otp) "
            "VALUES (:sk, 'Welcome No Scope', 'email', 'welcome-no-scope', true, false, false, false, false) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true"
        ),
        {"sk": service_key},
    )
    await test_db_session.commit()

    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(succeed=True),
    )

    response = await async_client.post(
        "/notifications/dispatch",
        headers=_api_key_header(),
        json={"service_key": service_key, "user_ids": [9732]},
    )
    assert response.status_code == 201, response.text
    notification_id = response.json()["data"]["notification_id"]

    result = await test_db_session.execute(
        text(
            "SELECT engagement_id, assessment_instance_id FROM notifications "
            "WHERE notification_id = :nid"
        ),
        {"nid": notification_id},
    )
    row = result.mappings().one()
    assert row["engagement_id"] is None
    assert row["assessment_instance_id"] is None


@pytest.mark.asyncio
async def test_dispatch_report_service_without_scope_returns_400(
    async_client, test_db_session, monkeypatch
):
    monkeypatch.setattr(settings, "NOTIFICATION_API_KEY", TEST_NOTIFICATION_API_KEY)
    test_db_session.add(User(user_id=9741, age=30, phone="9741000000", status="active"))
    service_key = "report_no_scope_test"
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, require_bio_ai_report_url, require_participant_detail, require_otp) "
            "VALUES (:sk, 'Report No Scope', 'email', 'report-no-scope', true, true, true, false, false) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true, require_blood_report_url = true, require_bio_ai_report_url = true"
        ),
        {"sk": service_key},
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/notifications/dispatch",
        headers=_api_key_header(),
        json={"service_key": service_key, "user_ids": [9741]},
    )
    assert response.status_code == 400
    assert "assessment_instance_id" in response.json()["message"].lower() or "engagement_id" in response.json()["message"].lower()
