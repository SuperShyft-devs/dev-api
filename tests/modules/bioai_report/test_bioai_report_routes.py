"""Integration tests for GET /bioai-report/{assessment_instance_id}."""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy import text

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import AssessmentInstance
from modules.bioai_report.report_engine.api.dependencies import get_bioreport_service
from modules.bioai_report.report_engine.models.report import (
    BioReport,
    ExecutiveSummary,
    PatientInfo,
    ReportMetadata,
)
from modules.employee.models import Employee
from modules.engagements.models import Engagement
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


class _FakeBioReportService:
    async def generate_for_assessment_instance(self, *, assessment_instance_id: int, db):
        return BioReport(
            patient=PatientInfo(record_id="REC-1", name="Test User"),
            executive_summary=ExecutiveSummary(),
            disease_sections=[],
            report_metadata=ReportMetadata(
                record_id="REC-1",
                engine_version="test",
                template_version="test",
            ),
        )


async def _seed_assessment(test_db_session, *, assessment_id: int, user_id: int):
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'Test Diagnostic', 'test_provider', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status) "
            "VALUES (1, 'PRO', 'Pro', '2', 'active') "
            "ON CONFLICT (package_id) DO UPDATE SET assessment_type_code = EXCLUDED.assessment_type_code"
        )
    )
    test_db_session.add(
        User(
            user_id=user_id,
            first_name="Test",
            last_name="User",
            phone=f"{user_id}000000",
            age=30,
            status="active",
        )
    )
    test_db_session.add(
        Engagement(
            engagement_id=assessment_id,
            engagement_name="BioAI Route Engagement",
            engagement_code=f"ENG-BIOAI-ROUTE-{assessment_id}",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="Bengaluru",
            slot_duration=20,
            start_date=date.today() - timedelta(days=7),
            end_date=date.today() + timedelta(days=7),
            status="running",
        )
    )
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=assessment_id,
            user_id=user_id,
            package_id=1,
            engagement_id=assessment_id,
            status="active",
            metsights_record_id="REC-1",
        )
    )
    await test_db_session.commit()


async def _seed_admin_employee(test_db_session, *, user_id: int, employee_id: int):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_get_bioreport_requires_internal_employee(async_client, test_db_session):
    await _seed_assessment(test_db_session, assessment_id=99501, user_id=89501)

    response = await async_client.get("/bioai-report/99501", headers=_auth_header(89501))
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_get_bioreport_onboarding_assistant_allowed(async_client, fastapi_app, test_db_session):
    await _seed_assessment(test_db_session, assessment_id=99502, user_id=89502)
    test_db_session.add(User(user_id=89512, age=30, phone="8951200000", status="active"))
    await test_db_session.flush()
    test_db_session.add(
        Employee(employee_id=99512, user_id=89512, role="onboarding_assistant", status="active")
    )
    await test_db_session.commit()

    fastapi_app.dependency_overrides[get_bioreport_service] = lambda: _FakeBioReportService()

    response = await async_client.get("/bioai-report/99502", headers=_auth_header(89512))
    assert response.status_code == 200
    body = response.json()
    assert body["patient"]["name"] == "Test User"
    assert body["report_metadata"]["record_id"] == "REC-1"

    fastapi_app.dependency_overrides.pop(get_bioreport_service, None)


@pytest.mark.asyncio
async def test_get_bioreport_admin_allowed(async_client, fastapi_app, test_db_session):
    await _seed_assessment(test_db_session, assessment_id=99503, user_id=89503)
    await _seed_admin_employee(test_db_session, user_id=89513, employee_id=99513)
    fastapi_app.dependency_overrides[get_bioreport_service] = lambda: _FakeBioReportService()

    response = await async_client.get("/bioai-report/99503", headers=_auth_header(89513))
    assert response.status_code == 200
    assert response.json()["patient"]["record_id"] == "REC-1"

    fastapi_app.dependency_overrides.pop(get_bioreport_service, None)
