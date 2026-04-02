"""Integration tests for reports routes."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.assessments.repository import AssessmentsRepository
from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.engagements.models import Engagement
from modules.diagnostics.models import DiagnosticPackage
from modules.diagnostics.schemas import (
    HealthParameterResponse,
    PackageTestsResponse,
    ParameterType,
    TestGroupResponse as DiagnosticTestGroupResponse,
)
from modules.reports.dependencies import get_reports_service
from modules.reports.models import IndividualHealthReport, ReportsUserSyncState
from modules.reports.repository import ReportsRepository
from modules.reports.service import ReportsService
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


class _FakeMetsightsService:
    def __init__(self, payload, should_fail: bool = False, report_payload=None):
        self.payload = payload
        self.should_fail = should_fail
        self.calls = 0
        self.report_payload = report_payload
        self.report_calls = 0

    async def get_blood_parameters(self, *, record_id: str):
        self.calls += 1
        if self.should_fail:
            raise AssertionError("Metsights should not be called in this test")
        return self.payload

    async def get_report(self, *, record_id: str, assessment_type_code: str | None = None):
        self.report_calls += 1
        if self.report_payload is None:
            raise AssertionError("get_report was not expected in this test")
        return self.report_payload


class _FailingMetsightsService:
    async def get_blood_parameters(self, *, record_id: str):
        raise RuntimeError("simulated metsights failure")


class _FakeDiagnosticsService:
    def __init__(self):
        self._payload = PackageTestsResponse(
            diagnostic_package_id=1,
            groups=[
                DiagnosticTestGroupResponse(
                    group_id=10,
                    group_name="Blood parameters",
                    test_count=2,
                    display_order=1,
                    tests=[
                        HealthParameterResponse(
                            test_id=1,
                            parameter_type=ParameterType.TEST,
                            test_name="Haemoglobin (Hb)",
                            parameter_key="haemoglobin",
                            unit="g/dL",
                            meaning=None,
                            lower_range_male=13.0,
                            higher_range_male=17.0,
                            lower_range_female=12.0,
                            higher_range_female=16.0,
                            causes_when_high=None,
                            causes_when_low=None,
                            effects_when_high=None,
                            effects_when_low=None,
                            what_to_do_when_low=None,
                            what_to_do_when_high=None,
                            is_available=True,
                            display_order=1,
                        ),
                        HealthParameterResponse(
                            test_id=2,
                            parameter_type=ParameterType.TEST,
                            test_name="Glucose (fasting)",
                            parameter_key="glucose_fasting",
                            unit="mg/dL",
                            meaning=None,
                            lower_range_male=70.0,
                            higher_range_male=110.0,
                            lower_range_female=70.0,
                            higher_range_female=110.0,
                            causes_when_high=None,
                            causes_when_low=None,
                            effects_when_high=None,
                            effects_when_low=None,
                            what_to_do_when_low=None,
                            what_to_do_when_high=None,
                            is_available=True,
                            display_order=2,
                        ),
                    ],
                )
            ],
        )

    async def get_package_tests(self, db, *, package_id: int) -> PackageTestsResponse:
        # Keep response static for tests; package_id is irrelevant here.
        return self._payload

    async def get_health_parameter_by_parameter_key(self, db, *, parameter_key: str):
        return None


async def _seed_assessment(
    test_db_session,
    *,
    assessment_id: int,
    user_id: int,
    engagement_id: int,
    record_id: str | None,
    diagnostic_package_id: int = 1,
    user_gender: str = "male",
    package_code: str | None = None,
    assessment_type_code: str | None = None,
):
    test_db_session.add(
        DiagnosticPackage(
            diagnostic_package_id=diagnostic_package_id,
            reference_id=f"REF{diagnostic_package_id}",
            package_name=f"Diag Package {diagnostic_package_id}",
            diagnostic_provider="test_provider",
            status="active",
        )
    )
    test_db_session.add(User(user_id=user_id, phone=f"{user_id}000000", age=30, gender=user_gender, status="active"))
    pkg_code = package_code if package_code is not None else f"P{assessment_id}"
    pkg = AssessmentPackage(
        package_id=assessment_id % 1000,
        package_code=pkg_code,
        display_name=f"Package {assessment_id}",
        status="active",
    )
    if assessment_type_code is not None:
        pkg.assessment_type_code = assessment_type_code
    test_db_session.add(pkg)
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_code=f"ENG{engagement_id}",
            assessment_package_id=assessment_id % 1000,
            diagnostic_package_id=diagnostic_package_id,
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
            blood_parameters={"haemoglobin": 13.2, "haemoglobin_unit": "g/dL"},
        )
    )
    await test_db_session.commit()

    fake_metsights = _FakeMetsightsService(payload={"haemoglobin": 99}, should_fail=True)
    reports_service = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=fake_metsights,
        diagnostics_service=_FakeDiagnosticsService(),
        audit_service=AuditService(AuditRepository()),
    )
    fastapi_app.dependency_overrides[get_reports_service] = lambda: reports_service

    response = await async_client.get("/reports/98001/blood-parameters", headers=_auth_header(3801))
    assert response.status_code == 200
    data = response.json()["data"]
    haemoglobin_test = next(t for g in data for t in g["tests"] if t["parameter_key"] == "haemoglobin")
    assert haemoglobin_test["value"] == 13.2
    assert haemoglobin_test["unit"] == "g/dL"
    assert haemoglobin_test["lower_range"] == 13.0
    assert haemoglobin_test["higher_range"] == 17.0
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

    fake_metsights = _FakeMetsightsService(payload={"glucose_fasting": 91, "glucose_fasting_unit": "mg/dL"})
    reports_service = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=fake_metsights,
        diagnostics_service=_FakeDiagnosticsService(),
        audit_service=AuditService(AuditRepository()),
    )
    fastapi_app.dependency_overrides[get_reports_service] = lambda: reports_service

    response = await async_client.get("/reports/98002/blood-parameters", headers=_auth_header(3802))
    assert response.status_code == 200
    body = response.json()["data"]
    glucose_test = next(t for g in body for t in g["tests"] if t["parameter_key"] == "glucose_fasting")
    assert glucose_test["value"] == 91
    assert glucose_test["unit"] == "mg/dL"
    assert glucose_test["lower_range"] == 70.0
    assert glucose_test["higher_range"] == 110.0
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
        DiagnosticPackage(
            diagnostic_package_id=1,
            reference_id="REF1",
            package_name="Diag Package 1",
            diagnostic_provider="test_provider",
            status="active",
        )
    )
    test_db_session.add(
        AssessmentPackage(
            package_id=811,
            package_code="P811",
            display_name="Package 811",
            status="active",
        )
    )
    await test_db_session.flush()
    test_db_session.add(Engagement(engagement_id=4811, engagement_code="ENG4811", assessment_package_id=811, diagnostic_package_id=1))
    test_db_session.add(Engagement(engagement_id=4812, engagement_code="ENG4812", assessment_package_id=811, diagnostic_package_id=1))
    test_db_session.add(Engagement(engagement_id=4813, engagement_code="ENG4813", assessment_package_id=811, diagnostic_package_id=1))
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
    test_db_session.add(
        ReportsUserSyncState(
            user_id=3811,
            last_synced_assessment_instance_id=98013,
            sync_status="idle",
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
    test_db_session.add(
        ReportsUserSyncState(
            user_id=3821,
            last_synced_assessment_instance_id=98021,
            sync_status="idle",
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


@pytest.mark.asyncio
async def test_get_blood_parameter_trends_stale_returns_cached_and_meta(
    async_client,
    fastapi_app,
    test_db_session,
):
    await _seed_assessment(
        test_db_session,
        assessment_id=98031,
        user_id=3831,
        engagement_id=4831,
        record_id="TREND_STALE",
    )
    test_db_session.add(
        IndividualHealthReport(
            report_id=70331,
            user_id=3831,
            engagement_id=4831,
            assessment_instance_id=98031,
            blood_parameters={"albumin": 3.9, "albumin_unit": "g/dL"},
        )
    )
    await test_db_session.commit()

    reports_service = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=_FakeMetsightsService(payload={"albumin": 3.9, "albumin_unit": "g/dL"}),
        diagnostics_service=_FakeDiagnosticsService(),
        audit_service=AuditService(AuditRepository()),
    )
    reports_service.trigger_user_blood_parameters_refresh = lambda *, user_id: None
    fastapi_app.dependency_overrides[get_reports_service] = lambda: reports_service

    response = await async_client.get(
        "/reports/trends?blood_parameter=albumin",
        headers=_auth_header(3831),
    )
    assert response.status_code == 200
    body = response.json()
    payload = body["data"]
    assert payload["parameter"] == "albumin"
    assert payload["unit"] == "g/dL"
    assert payload["data_points"] == [{"date": payload["data_points"][0]["date"], "value": 3.9, "engagement_id": 4831}]
    assert body["meta"]["is_stale"] is True
    assert body["meta"]["sync_status"] == "in_progress"
    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_get_blood_parameter_trends_ignores_fitprint_for_staleness(
    async_client,
    test_db_session,
):
    """FitPrint records have no blood-parameters API; they must not drive trends staleness."""
    test_db_session.add(
        DiagnosticPackage(
            diagnostic_package_id=1,
            reference_id="REF1",
            package_name="Diag Package 1",
            diagnostic_provider="test_provider",
            status="active",
        )
    )
    test_db_session.add(User(user_id=3855, phone="3855000000", age=30, gender="male", status="active"))
    test_db_session.add(
        AssessmentPackage(
            package_id=851,
            package_code="MET_PRO",
            display_name="MetSights Pro",
            status="active",
            assessment_type_code="2",
        )
    )
    test_db_session.add(
        AssessmentPackage(
            package_id=852,
            package_code="MY_FITNESS_PRINT",
            display_name="FitPrint",
            status="active",
            assessment_type_code="7",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=5851,
            engagement_code="ENG5851",
            assessment_package_id=851,
            diagnostic_package_id=1,
        )
    )
    test_db_session.add(
        Engagement(
            engagement_id=5852,
            engagement_code="ENG5852",
            assessment_package_id=852,
            diagnostic_package_id=1,
        )
    )
    await test_db_session.flush()
    now = datetime.now(timezone.utc)
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=98511,
            user_id=3855,
            package_id=851,
            engagement_id=5851,
            status="completed",
            metsights_record_id="MSREC1",
            assigned_at=now,
            completed_at=now,
        )
    )
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=98512,
            user_id=3855,
            package_id=852,
            engagement_id=5852,
            status="completed",
            metsights_record_id="FPREC1",
            assigned_at=now,
            completed_at=now,
        )
    )
    test_db_session.add(
        IndividualHealthReport(
            report_id=70855,
            user_id=3855,
            engagement_id=5851,
            assessment_instance_id=98511,
            blood_parameters={"albumin": 4.05, "albumin_unit": "g/dL"},
        )
    )
    test_db_session.add(
        ReportsUserSyncState(
            user_id=3855,
            last_synced_assessment_instance_id=98511,
            sync_status="idle",
        )
    )
    await test_db_session.commit()

    response = await async_client.get(
        "/reports/trends?blood_parameter=albumin",
        headers=_auth_header(3855),
    )
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["latest_assessment_instance_id"] == 98511
    assert body["meta"]["is_stale"] is False
    assert body["meta"]["sync_status"] == "idle"


@pytest.mark.asyncio
async def test_refresh_user_blood_parameters_success_advances_cursor(test_db_session):
    await _seed_assessment(
        test_db_session,
        assessment_id=98041,
        user_id=3841,
        engagement_id=4841,
        record_id="REC1",
    )
    test_db_session.add(
        Engagement(
            engagement_id=4842,
            engagement_code="ENG4842",
            assessment_package_id=41,
            diagnostic_package_id=1,
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=98042,
            user_id=3841,
            package_id=41,
            engagement_id=4842,
            status="completed",
            metsights_record_id="REC2",
            assigned_at=datetime.now(timezone.utc),
            completed_at=datetime.now(timezone.utc),
        )
    )
    await test_db_session.commit()
    test_db_session.add(
        ReportsUserSyncState(
            user_id=3841,
            last_synced_assessment_instance_id=98040,
            sync_status="in_progress",
        )
    )
    await test_db_session.commit()

    session_factory = async_sessionmaker(bind=test_db_session.bind, expire_on_commit=False)
    service = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=_FakeMetsightsService(payload={"albumin": 4.2, "albumin_unit": "g/dL"}),
        diagnostics_service=_FakeDiagnosticsService(),
        audit_service=AuditService(AuditRepository()),
        session_factory=session_factory,
    )
    await service._refresh_user_blood_parameters(user_id=3841)

    state_row = (
        await test_db_session.execute(select(ReportsUserSyncState).where(ReportsUserSyncState.user_id == 3841))
    ).scalar_one()
    assert state_row.sync_status == "idle"
    assert state_row.last_synced_assessment_instance_id == 98042
    assert state_row.last_sync_error is None


@pytest.mark.asyncio
async def test_refresh_user_blood_parameters_failure_sets_failed(test_db_session):
    await _seed_assessment(
        test_db_session,
        assessment_id=98051,
        user_id=3851,
        engagement_id=4851,
        record_id="REC_FAIL",
    )
    test_db_session.add(
        ReportsUserSyncState(
            user_id=3851,
            last_synced_assessment_instance_id=98050,
            sync_status="in_progress",
        )
    )
    await test_db_session.commit()

    session_factory = async_sessionmaker(bind=test_db_session.bind, expire_on_commit=False)
    service = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=_FailingMetsightsService(),
        diagnostics_service=_FakeDiagnosticsService(),
        audit_service=AuditService(AuditRepository()),
        session_factory=session_factory,
    )
    await service._refresh_user_blood_parameters(user_id=3851)

    state_row = (
        await test_db_session.execute(select(ReportsUserSyncState).where(ReportsUserSyncState.user_id == 3851))
    ).scalar_one()
    assert state_row.sync_status == "failed"
    assert state_row.last_sync_error is not None


@pytest.mark.asyncio
async def test_get_overview_met_cached_skips_metsights(
    async_client,
    fastapi_app,
    test_db_session,
):
    await _seed_assessment(
        test_db_session,
        assessment_id=99001,
        user_id=3901,
        engagement_id=5901,
        record_id="REC99001",
        package_code="MET_PRO",
        assessment_type_code="2",
    )
    test_db_session.add(
        IndividualHealthReport(
            report_id=79001,
            user_id=3901,
            engagement_id=5901,
            assessment_instance_id=99001,
            reports={
                "metabolic_age": 42.5,
                "diseases": [
                    {
                        "code": "A",
                        "name": "A",
                        "risk_status": "Healthy",
                        "risk_score_scaled": 10,
                        "healthy_percentile": 90,
                    },
                    {
                        "code": "B",
                        "name": "B",
                        "risk_status": "Moderate",
                        "risk_score_scaled": 55,
                        "healthy_percentile": 40,
                    },
                ],
            },
            blood_parameters={"haemoglobin": 12.0},
        )
    )
    await test_db_session.commit()

    fake_metsights = _FakeMetsightsService(payload={}, should_fail=True)
    reports_service = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=fake_metsights,
        diagnostics_service=_FakeDiagnosticsService(),
        audit_service=AuditService(AuditRepository()),
    )
    fastapi_app.dependency_overrides[get_reports_service] = lambda: reports_service

    response = await async_client.get("/reports/99001/overview", headers=_auth_header(3901))
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["metabolic_age"] == 42.5
    assert len(body["positive_wins"]["low_risk"]) == 1
    assert body["positive_wins"]["low_risk"][0]["code"] == "A"
    assert body["positive_wins"]["healthy_profiles"] == []
    assert [x["code"] for x in body["risk_analysis"]] == ["B", "A"]
    assert fake_metsights.report_calls == 0
    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_get_overview_top_three_low_and_high_risk_scores(
    async_client,
    fastapi_app,
    test_db_session,
):
    """low_risk: at most 3 Healthy, lowest risk_score_scaled first. risk_analysis: at most 3, highest score first."""
    await _seed_assessment(
        test_db_session,
        assessment_id=99010,
        user_id=3920,
        engagement_id=5920,
        record_id="REC99010",
        package_code="MET_PRO",
        assessment_type_code="2",
    )
    test_db_session.add(
        IndividualHealthReport(
            report_id=79010,
            user_id=3920,
            engagement_id=5920,
            assessment_instance_id=99010,
            reports={
                "metabolic_age": 35.0,
                "diseases": [
                    {
                        "code": "high_score",
                        "name": "High Score Healthy",
                        "risk_status": "Healthy",
                        "risk_score_scaled": 21,
                        "healthy_percentile": 80,
                    },
                    {
                        "code": "low_a",
                        "name": "Low A",
                        "risk_status": "Healthy",
                        "risk_score_scaled": 12,
                        "healthy_percentile": 22,
                    },
                    {
                        "code": "low_b",
                        "name": "Low B",
                        "risk_status": "Healthy",
                        "risk_score_scaled": 12,
                        "healthy_percentile": 60,
                    },
                    {
                        "code": "mid",
                        "name": "Mid",
                        "risk_status": "Healthy",
                        "risk_score_scaled": 16,
                        "healthy_percentile": 72,
                    },
                    {
                        "code": "nafld",
                        "name": "NAFLD",
                        "risk_status": "Increased",
                        "risk_score_scaled": 49,
                        "healthy_percentile": 86,
                    },
                ],
            },
            blood_parameters={"haemoglobin": 12.0},
        )
    )
    await test_db_session.commit()

    fake_metsights = _FakeMetsightsService(payload={}, should_fail=True)
    reports_service = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=fake_metsights,
        diagnostics_service=_FakeDiagnosticsService(),
        audit_service=AuditService(AuditRepository()),
    )
    fastapi_app.dependency_overrides[get_reports_service] = lambda: reports_service

    response = await async_client.get("/reports/99010/overview", headers=_auth_header(3920))
    assert response.status_code == 200
    low_risk = response.json()["data"]["positive_wins"]["low_risk"]
    assert len(low_risk) == 3
    assert [x["code"] for x in low_risk] == ["low_a", "low_b", "mid"]
    assert [x["risk_score_scaled"] for x in low_risk] == [12, 12, 16]

    ra = response.json()["data"]["risk_analysis"]
    assert len(ra) == 3
    assert [x["code"] for x in ra] == ["nafld", "high_score", "mid"]
    assert [x["risk_score_scaled"] for x in ra] == [49, 21, 16]
    assert response.json()["data"]["positive_wins"]["healthy_profiles"] == []

    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_get_overview_healthy_profiles_lists_top_groups_by_in_range_count(
    async_client,
    fastapi_app,
    test_db_session,
):
    """Groups ranked by count of tests with value in [lower, higher]; at most three names."""

    def _hp(tid: int, tname: str, pkey: str) -> HealthParameterResponse:
        return HealthParameterResponse(
            test_id=tid,
            parameter_type=ParameterType.TEST,
            test_name=tname,
            parameter_key=pkey,
            unit="u",
            meaning=None,
            lower_range_male=1.0,
            higher_range_male=10.0,
            lower_range_female=1.0,
            higher_range_female=10.0,
            causes_when_high=None,
            causes_when_low=None,
            effects_when_high=None,
            effects_when_low=None,
            what_to_do_when_low=None,
            what_to_do_when_high=None,
            is_available=True,
            display_order=tid,
        )

    class _DiagMultiGroup:
        async def get_package_tests(self, db, *, package_id: int) -> PackageTestsResponse:
            return PackageTestsResponse(
                diagnostic_package_id=1,
                groups=[
                    DiagnosticTestGroupResponse(
                        group_id=1,
                        group_name="Beta",
                        test_count=3,
                        display_order=2,
                        tests=[_hp(1, "b1", "b1"), _hp(2, "b2", "b2"), _hp(3, "b3", "b3")],
                    ),
                    DiagnosticTestGroupResponse(
                        group_id=2,
                        group_name="Alpha",
                        test_count=2,
                        display_order=1,
                        tests=[_hp(4, "a1", "a1"), _hp(5, "a2", "a2")],
                    ),
                    DiagnosticTestGroupResponse(
                        group_id=3,
                        group_name="Gamma",
                        test_count=1,
                        display_order=3,
                        tests=[_hp(6, "g1", "g1")],
                    ),
                    DiagnosticTestGroupResponse(
                        group_id=4,
                        group_name="Delta",
                        test_count=1,
                        display_order=4,
                        tests=[_hp(7, "d1", "d1")],
                    ),
                ],
            )

        async def get_health_parameter_by_parameter_key(self, db, *, parameter_key: str):
            return None

    await _seed_assessment(
        test_db_session,
        assessment_id=99011,
        user_id=3921,
        engagement_id=5921,
        record_id="REC99011",
        package_code="MET_PRO",
        assessment_type_code="2",
    )
    test_db_session.add(
        IndividualHealthReport(
            report_id=79011,
            user_id=3921,
            engagement_id=5921,
            assessment_instance_id=99011,
            reports={"metabolic_age": 30.0, "diseases": []},
            blood_parameters={
                "b1": 5.0,
                "b2": 5.0,
                "b3": 5.0,
                "a1": 5.0,
                "a2": 5.0,
                "g1": 5.0,
                "d1": 99.0,
            },
        )
    )
    await test_db_session.commit()

    reports_service = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=_FakeMetsightsService(payload={}, should_fail=True),
        diagnostics_service=_DiagMultiGroup(),
        audit_service=AuditService(AuditRepository()),
    )
    fastapi_app.dependency_overrides[get_reports_service] = lambda: reports_service

    response = await async_client.get("/reports/99011/overview", headers=_auth_header(3921))
    assert response.status_code == 200
    assert response.json()["data"]["positive_wins"]["healthy_profiles"] == ["Beta", "Alpha", "Gamma"]

    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_get_overview_healthy_profiles_when_blood_values_in_range(
    async_client,
    fastapi_app,
    test_db_session,
):
    await _seed_assessment(
        test_db_session,
        assessment_id=99012,
        user_id=3922,
        engagement_id=5922,
        record_id="REC99012",
        package_code="MET_PRO",
        assessment_type_code="2",
    )
    test_db_session.add(
        IndividualHealthReport(
            report_id=79012,
            user_id=3922,
            engagement_id=5922,
            assessment_instance_id=99012,
            reports={"metabolic_age": 28.0, "diseases": []},
            blood_parameters={"haemoglobin": 14.0, "glucose_fasting": 90.0},
        )
    )
    await test_db_session.commit()

    reports_service = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=_FakeMetsightsService(payload={}, should_fail=True),
        diagnostics_service=_FakeDiagnosticsService(),
        audit_service=AuditService(AuditRepository()),
    )
    fastapi_app.dependency_overrides[get_reports_service] = lambda: reports_service

    response = await async_client.get("/reports/99012/overview", headers=_auth_header(3922))
    assert response.status_code == 200
    assert response.json()["data"]["positive_wins"]["healthy_profiles"] == ["Blood parameters"]

    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_get_overview_met_fetches_then_uses_cache(
    async_client,
    fastapi_app,
    test_db_session,
):
    await _seed_assessment(
        test_db_session,
        assessment_id=99002,
        user_id=3902,
        engagement_id=5902,
        record_id="REC99002",
        package_code="MET_PRO",
        assessment_type_code="2",
    )
    met_payload = {"metabolic_age": 33.0, "diseases": []}
    fake_metsights = _FakeMetsightsService(payload={"haemoglobin": 1}, report_payload=met_payload)
    reports_service = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=fake_metsights,
        diagnostics_service=_FakeDiagnosticsService(),
        audit_service=AuditService(AuditRepository()),
    )
    fastapi_app.dependency_overrides[get_reports_service] = lambda: reports_service

    r1 = await async_client.get("/reports/99002/overview", headers=_auth_header(3902))
    assert r1.status_code == 200
    assert r1.json()["data"]["metabolic_age"] == 33.0
    assert fake_metsights.report_calls == 1

    r2 = await async_client.get("/reports/99002/overview", headers=_auth_header(3902))
    assert r2.status_code == 200
    assert r2.json()["data"]["metabolic_age"] == 33.0
    assert fake_metsights.report_calls == 1

    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_get_overview_fitprint_not_allowed(
    async_client,
    fastapi_app,
    test_db_session,
):
    await _seed_assessment(
        test_db_session,
        assessment_id=99003,
        user_id=3903,
        engagement_id=5903,
        record_id="REC99003",
        package_code="MY_FITNESS_PRINT",
        assessment_type_code="7",
    )
    fake_metsights = _FakeMetsightsService(payload={}, should_fail=True)
    reports_service = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=fake_metsights,
        diagnostics_service=_FakeDiagnosticsService(),
        audit_service=AuditService(AuditRepository()),
    )
    fastapi_app.dependency_overrides[get_reports_service] = lambda: reports_service

    response = await async_client.get("/reports/99003/overview", headers=_auth_header(3903))
    assert response.status_code == 403
    assert response.json() == {
        "error_code": "FORBIDDEN",
        "message": "FitPrint report overview is not allowed",
    }
    assert fake_metsights.report_calls == 0
    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_get_overview_wrong_user_returns_404(
    async_client,
    fastapi_app,
    test_db_session,
):
    await _seed_assessment(
        test_db_session,
        assessment_id=99004,
        user_id=3910,
        engagement_id=5904,
        record_id="REC99004",
        package_code="MET_PRO",
        assessment_type_code="2",
    )
    test_db_session.add(User(user_id=3911, phone="3911000000", age=30, gender="male", status="active"))
    await test_db_session.commit()
    fake_metsights = _FakeMetsightsService(payload={}, report_payload={})
    reports_service = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=fake_metsights,
        diagnostics_service=_FakeDiagnosticsService(),
        audit_service=AuditService(AuditRepository()),
    )
    fastapi_app.dependency_overrides[get_reports_service] = lambda: reports_service

    response = await async_client.get("/reports/99004/overview", headers=_auth_header(3911))
    assert response.status_code == 404
    assert response.json()["error_code"] == "ASSESSMENT_NOT_FOUND"
    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_get_overview_missing_metsights_record_returns_422(
    async_client,
    fastapi_app,
    test_db_session,
):
    await _seed_assessment(
        test_db_session,
        assessment_id=99005,
        user_id=3905,
        engagement_id=5905,
        record_id=None,
        package_code="MET_PRO",
        assessment_type_code="2",
    )
    fake_metsights = _FakeMetsightsService(payload={}, report_payload={})
    reports_service = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=fake_metsights,
        diagnostics_service=_FakeDiagnosticsService(),
        audit_service=AuditService(AuditRepository()),
    )
    fastapi_app.dependency_overrides[get_reports_service] = lambda: reports_service

    response = await async_client.get("/reports/99005/overview", headers=_auth_header(3905))
    assert response.status_code == 422
    assert response.json() == {
        "error_code": "INVALID_STATE",
        "message": "Metsights record id is missing for this assessment",
    }
    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_get_risk_analysis_cached_skips_metsights(
    async_client,
    fastapi_app,
    test_db_session,
):
    await _seed_assessment(
        test_db_session,
        assessment_id=99101,
        user_id=39101,
        engagement_id=59101,
        record_id="REC99101",
        package_code="MET_PRO",
        assessment_type_code="2",
    )
    test_db_session.add(
        IndividualHealthReport(
            report_id=79101,
            user_id=39101,
            engagement_id=59101,
            assessment_instance_id=99101,
            reports={
                "metabolic_score": 72.5,
                "diseases": [
                    {"code": "oxidative_stress", "name": "Oxidative stress", "risk_score_scaled": 40},
                ],
            },
            blood_parameters=None,
        )
    )
    await test_db_session.commit()

    fake_metsights = _FakeMetsightsService(payload={}, should_fail=True)
    reports_service = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=fake_metsights,
        diagnostics_service=_FakeDiagnosticsService(),
        audit_service=AuditService(AuditRepository()),
    )
    fastapi_app.dependency_overrides[get_reports_service] = lambda: reports_service

    response = await async_client.get("/reports/99101/risk-analysis", headers=_auth_header(39101))
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["assessment_id"] == 99101
    assert body["metabolic_score"] == 72.5
    assert body["diseases"] == [
        {"code": "oxidative_stress", "name": "Oxidative stress", "risk_score_scaled": 40},
    ]
    assert fake_metsights.report_calls == 0
    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_get_risk_analysis_fetches_then_second_call_uses_cache(
    async_client,
    fastapi_app,
    test_db_session,
):
    await _seed_assessment(
        test_db_session,
        assessment_id=99102,
        user_id=39102,
        engagement_id=59102,
        record_id="REC99102",
        package_code="MET_PRO",
        assessment_type_code="2",
    )
    met_report = {
        "metabolic_score": 65.0,
        "diseases": [{"code": "A", "name": "A", "risk_score_scaled": 5}],
    }
    fake_metsights = _FakeMetsightsService(payload={}, report_payload=met_report)
    reports_service = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=fake_metsights,
        diagnostics_service=_FakeDiagnosticsService(),
        audit_service=AuditService(AuditRepository()),
    )
    fastapi_app.dependency_overrides[get_reports_service] = lambda: reports_service

    r1 = await async_client.get("/reports/99102/risk-analysis", headers=_auth_header(39102))
    assert r1.status_code == 200
    assert r1.json()["data"]["metabolic_score"] == 65.0
    assert fake_metsights.report_calls == 1

    r2 = await async_client.get("/reports/99102/risk-analysis", headers=_auth_header(39102))
    assert r2.status_code == 200
    assert r2.json()["data"]["metabolic_score"] == 65.0
    assert fake_metsights.report_calls == 1

    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_get_risk_analysis_disease_query_returns_detail(
    async_client,
    fastapi_app,
    test_db_session,
):
    await _seed_assessment(
        test_db_session,
        assessment_id=99103,
        user_id=39103,
        engagement_id=59103,
        record_id="REC99103",
        package_code="MET_PRO",
        assessment_type_code="2",
    )
    test_db_session.add(
        IndividualHealthReport(
            report_id=79103,
            user_id=39103,
            engagement_id=59103,
            assessment_instance_id=99103,
            reports={
                "metabolic_score": 70.0,
                "diseases": [
                    {
                        "code": "oxidative_stress",
                        "name": "Oxidative stress",
                        "risk_score_scaled": 40,
                        "lifestyle_contribution": 12,
                        "disease_percentile": 55,
                    },
                ],
            },
            blood_parameters=None,
        )
    )
    await test_db_session.commit()

    fake_metsights = _FakeMetsightsService(payload={}, should_fail=True)
    reports_service = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=fake_metsights,
        diagnostics_service=_FakeDiagnosticsService(),
        audit_service=AuditService(AuditRepository()),
    )
    fastapi_app.dependency_overrides[get_reports_service] = lambda: reports_service

    response = await async_client.get(
        "/reports/99103/risk-analysis?disease=oxidative_stress",
        headers=_auth_header(39103),
    )
    assert response.status_code == 200
    assert response.json()["data"] == {
        "code": "oxidative_stress",
        "name": "Oxidative stress",
        "meaning": None,
        "unit": None,
        "risk_score_scaled": 40,
        "lifestyle_contribution": 12,
        "disease_percentile": 55,
        "lower_range_male": None,
        "higher_range_male": None,
        "lower_range_female": None,
        "higher_range_female": None,
        "causes_when_high": None,
        "causes_when_low": None,
        "effects_when_high": None,
        "effects_when_low": None,
        "what_to_do_when_low": None,
        "what_to_do_when_high": None,
    }
    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_get_risk_analysis_disease_not_found(
    async_client,
    fastapi_app,
    test_db_session,
):
    await _seed_assessment(
        test_db_session,
        assessment_id=99104,
        user_id=39104,
        engagement_id=59104,
        record_id="REC99104",
        package_code="MET_PRO",
        assessment_type_code="2",
    )
    test_db_session.add(
        IndividualHealthReport(
            report_id=79104,
            user_id=39104,
            engagement_id=59104,
            assessment_instance_id=99104,
            reports={"diseases": [{"code": "A", "name": "A", "risk_score_scaled": 1}]},
            blood_parameters=None,
        )
    )
    await test_db_session.commit()

    fake_metsights = _FakeMetsightsService(payload={}, should_fail=True)
    reports_service = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=fake_metsights,
        diagnostics_service=_FakeDiagnosticsService(),
        audit_service=AuditService(AuditRepository()),
    )
    fastapi_app.dependency_overrides[get_reports_service] = lambda: reports_service

    response = await async_client.get(
        "/reports/99104/risk-analysis?disease=invalid_code",
        headers=_auth_header(39104),
    )
    assert response.status_code == 404
    assert response.json()["error_code"] == "DISEASE_NOT_FOUND"
    assert "invalid_code" in response.json()["message"]
    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_get_risk_analysis_wrong_user_returns_404(
    async_client,
    fastapi_app,
    test_db_session,
):
    await _seed_assessment(
        test_db_session,
        assessment_id=99105,
        user_id=39105,
        engagement_id=59105,
        record_id="REC99105",
        package_code="MET_PRO",
        assessment_type_code="2",
    )
    test_db_session.add(User(user_id=39106, phone="3910600000", age=30, gender="male", status="active"))
    await test_db_session.commit()

    fake_metsights = _FakeMetsightsService(payload={}, report_payload={})
    reports_service = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=fake_metsights,
        diagnostics_service=_FakeDiagnosticsService(),
        audit_service=AuditService(AuditRepository()),
    )
    fastapi_app.dependency_overrides[get_reports_service] = lambda: reports_service

    response = await async_client.get("/reports/99105/risk-analysis", headers=_auth_header(39106))
    assert response.status_code == 404
    assert response.json()["error_code"] == "ASSESSMENT_NOT_FOUND"
    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_get_risk_analysis_missing_metsights_record_returns_422(
    async_client,
    fastapi_app,
    test_db_session,
):
    await _seed_assessment(
        test_db_session,
        assessment_id=99106,
        user_id=39107,
        engagement_id=59106,
        record_id=None,
        package_code="MET_PRO",
        assessment_type_code="2",
    )
    fake_metsights = _FakeMetsightsService(payload={}, report_payload={})
    reports_service = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=fake_metsights,
        diagnostics_service=_FakeDiagnosticsService(),
        audit_service=AuditService(AuditRepository()),
    )
    fastapi_app.dependency_overrides[get_reports_service] = lambda: reports_service

    response = await async_client.get("/reports/99106/risk-analysis", headers=_auth_header(39107))
    assert response.status_code == 422
    assert response.json() == {
        "error_code": "INVALID_STATE",
        "message": "Metsights record id is missing for this assessment",
    }
    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_get_risk_analysis_fitprint_allowed_empty_diseases(
    async_client,
    fastapi_app,
    test_db_session,
):
    """FitPrint has no diseases in Metsights payload; endpoint still returns 200."""
    await _seed_assessment(
        test_db_session,
        assessment_id=99107,
        user_id=39108,
        engagement_id=59107,
        record_id="REC99107",
        package_code="MY_FITNESS_PRINT",
        assessment_type_code="7",
    )
    fitprint_report = {"metabolic_score": None}
    fake_metsights = _FakeMetsightsService(payload={}, report_payload=fitprint_report)
    reports_service = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=fake_metsights,
        diagnostics_service=_FakeDiagnosticsService(),
        audit_service=AuditService(AuditRepository()),
    )
    fastapi_app.dependency_overrides[get_reports_service] = lambda: reports_service

    response = await async_client.get("/reports/99107/risk-analysis", headers=_auth_header(39108))
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["assessment_id"] == 99107
    assert data["metabolic_score"] is None
    assert data["diseases"] == []
    fastapi_app.dependency_overrides.pop(get_reports_service, None)
