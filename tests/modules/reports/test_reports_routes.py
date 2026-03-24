"""Integration tests for reports routes."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.assessments.repository import AssessmentsRepository
from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.engagements.models import Engagement
from modules.reports.dependencies import get_reports_service
from modules.reports.models import IndividualHealthReport
from modules.reports.repository import ReportsRepository
from modules.reports.service import ReportsService
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


class _FakeMetsightsService:
    def __init__(self, payload, should_fail: bool = False):
        self.payload = payload
        self.should_fail = should_fail
        self.calls = 0

    async def get_blood_parameters(self, *, record_id: str):
        self.calls += 1
        if self.should_fail:
            raise AssertionError("Metsights should not be called in this test")
        return self.payload


async def _seed_assessment(test_db_session, *, assessment_id: int, user_id: int, engagement_id: int, record_id: str | None):
    test_db_session.add(User(user_id=user_id, phone=f"{user_id}000000", age=30, status="active"))
    test_db_session.add(
        AssessmentPackage(
            package_id=assessment_id % 1000,
            package_code=f"P{assessment_id}",
            display_name=f"Package {assessment_id}",
            status="active",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_code=f"ENG{engagement_id}",
            assessment_package_id=assessment_id % 1000,
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=assessment_id,
            user_id=user_id,
            package_id=assessment_id % 1000,
            engagement_id=engagement_id,
            status="completed",
            metsights_record_id=record_id,
            assigned_at=datetime.now(timezone.utc),
            completed_at=datetime.now(timezone.utc),
        )
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_get_blood_parameters_returns_cached_without_metsights(
    async_client,
    fastapi_app,
    test_db_session,
):
    await _seed_assessment(
        test_db_session,
        assessment_id=98001,
        user_id=3801,
        engagement_id=4801,
        record_id="ABC1234DEF56",
    )
    test_db_session.add(
        IndividualHealthReport(
            report_id=70001,
            user_id=3801,
            engagement_id=4801,
            assessment_instance_id=98001,
            blood_parameters={"haemoglobin": 13.2, "haemoglobin_unit": "0"},
        )
    )
    await test_db_session.commit()

    fake_metsights = _FakeMetsightsService(payload={"haemoglobin": 99}, should_fail=True)
    reports_service = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=fake_metsights,
        audit_service=AuditService(AuditRepository()),
    )
    fastapi_app.dependency_overrides[get_reports_service] = lambda: reports_service

    response = await async_client.get("/reports/98001/blood-parameters", headers=_auth_header(3801))
    assert response.status_code == 200
    assert response.json()["data"]["blood_parameters"]["haemoglobin"] == 13.2
    assert fake_metsights.calls == 0
    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_get_blood_parameters_fetches_and_caches_on_miss(
    async_client,
    fastapi_app,
    test_db_session,
):
    await _seed_assessment(
        test_db_session,
        assessment_id=98002,
        user_id=3802,
        engagement_id=4802,
        record_id="XYZ1234DEF56",
    )

    fake_metsights = _FakeMetsightsService(payload={"glucose_fasting": 91, "glucose_fasting_unit": "0"})
    reports_service = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=fake_metsights,
        audit_service=AuditService(AuditRepository()),
    )
    fastapi_app.dependency_overrides[get_reports_service] = lambda: reports_service

    response = await async_client.get("/reports/98002/blood-parameters", headers=_auth_header(3802))
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["assessment_id"] == 98002
    assert body["blood_parameters"]["glucose_fasting"] == 91
    assert fake_metsights.calls == 1

    saved = await test_db_session.execute(
        select(IndividualHealthReport).where(IndividualHealthReport.assessment_instance_id == 98002)
    )
    report = saved.scalar_one_or_none()
    assert report is not None
    assert report.blood_parameters["glucose_fasting"] == 91
    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_get_blood_parameters_requires_metsights_record_id(
    async_client,
    test_db_session,
):
    await _seed_assessment(
        test_db_session,
        assessment_id=98003,
        user_id=3803,
        engagement_id=4803,
        record_id=None,
    )

    response = await async_client.get("/reports/98003/blood-parameters", headers=_auth_header(3803))
    assert response.status_code == 422
    assert response.json() == {
        "error_code": "INVALID_STATE",
        "message": "Metsights record id is missing for this assessment",
    }


@pytest.mark.asyncio
async def test_get_blood_parameter_trends_returns_ordered_points(async_client, test_db_session):
    test_db_session.add(User(user_id=3811, phone="3811000000", age=30, status="active"))
    test_db_session.add(
        AssessmentPackage(
            package_id=811,
            package_code="P811",
            display_name="Package 811",
            status="active",
        )
    )
    await test_db_session.flush()
    test_db_session.add(Engagement(engagement_id=4811, engagement_code="ENG4811", assessment_package_id=811))
    test_db_session.add(Engagement(engagement_id=4812, engagement_code="ENG4812", assessment_package_id=811))
    test_db_session.add(Engagement(engagement_id=4813, engagement_code="ENG4813", assessment_package_id=811))
    await test_db_session.flush()
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=98011,
            user_id=3811,
            package_id=811,
            engagement_id=4811,
            status="completed",
            metsights_record_id="TREND1",
            assigned_at=datetime.now(timezone.utc),
            completed_at=datetime(2024, 6, 6, tzinfo=timezone.utc),
        )
    )
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=98012,
            user_id=3811,
            package_id=811,
            engagement_id=4812,
            status="completed",
            metsights_record_id="TREND2",
            assigned_at=datetime.now(timezone.utc),
            completed_at=datetime(2024, 12, 26, tzinfo=timezone.utc),
        )
    )
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=98013,
            user_id=3811,
            package_id=811,
            engagement_id=4813,
            status="completed",
            metsights_record_id="TREND3",
            assigned_at=datetime.now(timezone.utc),
            completed_at=datetime(2025, 4, 18, tzinfo=timezone.utc),
        )
    )

    test_db_session.add(
        IndividualHealthReport(
            report_id=70111,
            user_id=3811,
            engagement_id=4811,
            assessment_instance_id=98011,
            blood_parameters={"albumin": 3.8, "albumin_unit": "g/dL"},
        )
    )
    test_db_session.add(
        IndividualHealthReport(
            report_id=70112,
            user_id=3811,
            engagement_id=4812,
            assessment_instance_id=98012,
            blood_parameters={"albumin": 4.1, "albumin_unit": "g/dL"},
        )
    )
    test_db_session.add(
        IndividualHealthReport(
            report_id=70113,
            user_id=3811,
            engagement_id=4813,
            assessment_instance_id=98013,
            blood_parameters={"albumin": 4.05, "albumin_unit": "g/dL"},
        )
    )
    await test_db_session.commit()

    response = await async_client.get(
        "/reports/trends?blood_parameter=albumin",
        headers=_auth_header(3811),
    )
    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["parameter"] == "albumin"
    assert payload["unit"] == "g/dL"
    assert payload["data_points"] == [
        {"date": "2024-06-06", "value": 3.8, "engagement_id": 4811},
        {"date": "2024-12-26", "value": 4.1, "engagement_id": 4812},
        {"date": "2025-04-18", "value": 4.05, "engagement_id": 4813},
    ]


@pytest.mark.asyncio
async def test_get_blood_parameter_trends_returns_empty_when_no_points(async_client, test_db_session):
    await _seed_assessment(
        test_db_session,
        assessment_id=98021,
        user_id=3821,
        engagement_id=4821,
        record_id="TREND4",
    )
    test_db_session.add(
        IndividualHealthReport(
            report_id=70221,
            user_id=3821,
            engagement_id=4821,
            assessment_instance_id=98021,
            blood_parameters={"haemoglobin": 13.2, "haemoglobin_unit": "g/dL"},
        )
    )
    await test_db_session.commit()

    response = await async_client.get(
        "/reports/trends?blood_parameter=albumin",
        headers=_auth_header(3821),
    )
    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["parameter"] == "albumin"
    assert payload["unit"] is None
    assert payload["data_points"] == []
