"""Regression tests for MissingGreenlet during camp report section refresh."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from db.transaction import release_request_transaction
from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.assessments.repository import AssessmentsRepository
from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.diagnostics.repository import DiagnosticsRepository
from modules.engagements.models import Engagement
from modules.organizations.models import Organization
from modules.organizations.repository import OrganizationsRepository
from modules.reports.camp_report_sections_repository import CampReportSectionsRepository
from modules.reports.camp_reports_repository import CampReportsRepository, EnrolledAssessmentContext
from modules.reports.camp_reports_service import CampReportsService
from modules.reports.dependencies import get_reports_service
from modules.reports.models import CampReport, CampReportSection
from tests.modules.reports.test_refresh_camp_reports_job import _ensure_section, _service


def _camp_reports_service() -> CampReportsService:
    return CampReportsService(
        repository=CampReportsRepository(),
        sections_repository=CampReportSectionsRepository(),
        organizations_repository=OrganizationsRepository(),
        audit_service=AuditService(AuditRepository()),
        reports_service=get_reports_service(),
        assessments_repository=AssessmentsRepository(),
        diagnostics_repository=DiagnosticsRepository(),
    )


async def _seed_kpis_refresh_rows(test_db_session, *, camp_no: int = 96001) -> CampReport:
    org_id = 96001
    existing_org = (
        await test_db_session.execute(
            select(Organization).where(Organization.organization_id == org_id)
        )
    ).scalar_one_or_none()
    if existing_org is None:
        test_db_session.add(
            Organization(
                organization_id=org_id,
                name="Greenlet KPI Org",
                organization_type="corporate",
                status="active",
                departments=[{"department": "Sales", "slug": "sales"}],
            )
        )

    await _ensure_section(
        test_db_session,
        report_sections=96001,
        section_key="kpis",
        section="KPIs",
        description="Camp enrollment and health KPI summary",
    )

    existing_report = (
        await test_db_session.execute(
            select(CampReport).where(
                CampReport.camp_no == camp_no,
                CampReport.department.is_(None),
                CampReport.city.is_(None),
            )
        )
    ).scalar_one_or_none()
    if existing_report is not None:
        return existing_report

    row = CampReport(
        camp_no=camp_no,
        organization_id=org_id,
        department=None,
        city=None,
        report={
            "meta": {
                "camp_name": "Greenlet KPI Camp",
                "summary_available": False,
                "refreshed_at": None,
                "next_refresh": None,
                "camp_start_date": "2026-06-23",
                "camp_end_date": "2026-06-30",
            }
        },
    )
    test_db_session.add(row)
    await test_db_session.commit()
    await test_db_session.refresh(row)
    return row


@pytest.mark.asyncio
async def test_refresh_kpis_survives_release_expire(test_db_session, monkeypatch):
    """KPI refresh must not read expired section_row after blood Metsights release."""
    row = await _seed_kpis_refresh_rows(test_db_session)
    service = _service()

    async def _mock_kpis(db, **kwargs):
        await release_request_transaction(db)
        return (
            {"data": {"employees_enrolled": 0}},
            {"blood_details": {}, "kpi_bts_details": {}},
        )

    monkeypatch.setattr(service, "_build_kpis_payload_with_metrics", _mock_kpis)

    result = await service._refresh_camp_report_section_core(
        test_db_session,
        camp_no=row.camp_no,
        section="kpis",
        department=None,
        city=None,
        context={"camp_start_date": date(2026, 6, 23), "camp_end_date": date(2026, 6, 30)},
        audit_action="test.refresh.kpis",
        ip_address="127.0.0.1",
        user_agent="pytest",
        endpoint="test:refresh_kpis",
    )

    assert result["section"]["name"] == "KPIs"
    assert result["section"]["description"] == "Camp enrollment and health KPI summary"

    stored = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == row.report_id))
    ).scalar_one()
    assert stored.report["kpis"]["name"] == "KPIs"


@pytest.mark.asyncio
async def test_compute_company_average_scores_survives_release_expire(test_db_session, monkeypatch):
    """CAS must process every participant after resolve releases the shared session."""
    service = _camp_reports_service()

    pkg = AssessmentPackage(
        package_id=96011,
        package_code="FITPRINT96011",
        display_name="FitPrint",
        assessment_type_code="7",
        status="active",
    )
    eng = Engagement(
        engagement_id=96011,
        engagement_name="CAS Greenlet Eng",
        organization_id=96011,
        camp_no=96011062326,
        engagement_code="CASMG1",
        engagement_type=None,
        assessment_package_id=96011,
        diagnostic_package_id=None,
        city="BLR",
        slot_duration=20,
        start_date=date(2026, 6, 23),
        end_date=date(2026, 6, 30),
        status="running",
    )
    instances = [
        AssessmentInstance(
            assessment_instance_id=960111,
            user_id=960101,
            package_id=96011,
            engagement_id=96011,
            status="completed",
            metsights_record_id="REC960111",
        ),
        AssessmentInstance(
            assessment_instance_id=960112,
            user_id=960102,
            package_id=96011,
            engagement_id=96011,
            status="completed",
            metsights_record_id="REC960112",
        ),
    ]

    contexts = [
        EnrolledAssessmentContext(
            assessment_instance=instances[0],
            package=pkg,
            engagement=eng,
            individual_report=None,
            user_gender="male",
            user_age=30,
            user_date_of_birth=date(1996, 1, 1),
        ),
        EnrolledAssessmentContext(
            assessment_instance=instances[1],
            package=pkg,
            engagement=eng,
            individual_report=None,
            user_gender="female",
            user_age=28,
            user_date_of_birth=date(1998, 1, 1),
        ),
    ]

    service._repository.list_fitprint_assessment_contexts = AsyncMock(return_value=contexts)
    service._repository.list_enrolled_users_without_fitprint = AsyncMock(return_value=[])
    service._repository.count_enrolled_users = AsyncMock(return_value=2)
    service._assessments_repository.list_instances_for_user_engagement = AsyncMock(return_value=[])

    resolve_calls = 0
    fitprint_report = {
        "fitness_specification": {"score": 60.0},
        "activity_specification": {"score": 50.0},
    }

    async def _resolve_with_release(db, **kwargs):
        nonlocal resolve_calls
        resolve_calls += 1
        await release_request_transaction(db)
        return fitprint_report

    monkeypatch.setattr(
        service._reports_service,
        "_resolve_report_dict_for_instance",
        _resolve_with_release,
    )

    payload, details = await service._compute_company_average_scores_with_details(
        test_db_session,
        camp_no=96011062326,
        department=None,
        city=None,
    )

    assert resolve_calls == 2
    assert payload["data"]["lifestyle"]["score"] == 60.0
    assert len(details["participants"]) == 2
