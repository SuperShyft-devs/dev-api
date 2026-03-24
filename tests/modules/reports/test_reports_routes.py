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
