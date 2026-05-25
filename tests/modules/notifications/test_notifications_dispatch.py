"""Tests for POST /notifications/dispatch record_id resolution."""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token(
        {"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY
    )
    return {"Authorization": f"Bearer {token}"}


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
            participant_count=0,
        )
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_dispatch_resolves_record_id_from_metsights_basic_instance(
    async_client, test_db_session, monkeypatch
):
    """Bio AI dispatch should auto-resolve record_id when type code is '1' (METSIGHTS_BASIC)."""
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
    service_key = "bio_ai_report_whatsapp_test"
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_record_id, require_participant_detail) "
            "VALUES (:sk, 'BioAI Report | Whatsapp', 'whatsapp', 'bio-ai-report', true, true, false) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true, require_record_id = true"
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
            "user_id": 9502,
            "engagement_id": 9501,
        },
    )
    assert response.status_code == 201, response.text
    assert webhook_calls
    assert webhook_calls[0]["json"]["record_id"] == "25D4C413C7D3"


@pytest.mark.asyncio
async def test_dispatch_prefers_engagement_instance_over_other_engagements(
    async_client, test_db_session, monkeypatch
):
    """When engagement_id is set, record_id must come from that engagement's Metsights instance."""
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
    service_key = "bio_ai_report_whatsapp_eng"
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_record_id, require_participant_detail) "
            "VALUES (:sk, 'BioAI Report | Whatsapp', 'whatsapp', 'bio-ai-report', true, true, false) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true, require_record_id = true"
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
            "user_id": 9523,
            "engagement_id": 9521,
        },
    )
    assert response.status_code == 201, response.text
    assert webhook_calls[0]["json"]["record_id"] == "TARGET-ENG-RECORD"


@pytest.mark.asyncio
async def test_dispatch_without_record_id_returns_400_when_required(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9511, employee_id=961)
    test_db_session.add(User(user_id=9512, age=30, phone="9512000000", status="active"))
    missing_service_key = "bio_ai_report_whatsapp_missing"
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_record_id, require_participant_detail) "
            "VALUES (:sk, 'BioAI Report | Whatsapp', 'whatsapp', 'bio-ai-report', true, true, false) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true, require_record_id = true"
        ),
        {"sk": missing_service_key},
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/notifications/dispatch",
        headers=_auth_header(9511),
        json={
            "service_key": missing_service_key,
            "user_id": 9512,
            "engagement_id": None,
        },
    )
    assert response.status_code == 400
    assert "record_id" in response.json()["message"].lower()
