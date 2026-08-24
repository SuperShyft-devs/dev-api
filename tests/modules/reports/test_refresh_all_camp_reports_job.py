"""Tests for initialize_and_refresh_all_camp_reports backfill job."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from modules.assessments.repository import AssessmentsRepository
from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.diagnostics.repository import DiagnosticsRepository
from modules.engagements.camp_no import compute_camp_no
from modules.engagements.models import Engagement
from modules.organizations.models import Organization
from modules.organizations.repository import OrganizationsRepository
from modules.reports.camp_report_sections_repository import CampReportSectionsRepository
from modules.reports.camp_reports_repository import CampReportsRepository
from modules.reports.camp_reports_service import CampReportsService
from modules.reports.dependencies import get_reports_service
from modules.reports.models import CampReport, CampReportSection
from modules.reports.refresh_all_camp_reports_job import initialize_and_refresh_all_camp_reports


def _service() -> CampReportsService:
    return CampReportsService(
        repository=CampReportsRepository(),
        sections_repository=CampReportSectionsRepository(),
        organizations_repository=OrganizationsRepository(),
        audit_service=AuditService(AuditRepository()),
        reports_service=get_reports_service(),
        assessments_repository=AssessmentsRepository(),
        diagnostics_repository=DiagnosticsRepository(),
    )


async def _seed_report(
    test_db_session,
    *,
    camp_no: int,
    organization_id: int,
    department: str | None = None,
    city: str | None = None,
) -> CampReport:
    row = CampReport(
        report={
            "meta": {
                "camp_name": "Test Camp",
                "summary_available": False,
                "refreshed_at": None,
                "next_refresh": None,
                "camp_start_date": "2026-06-23",
                "camp_end_date": "2026-06-30",
            }
        },
        camp_no=camp_no,
        department=department,
        city=city,
        organization_id=organization_id,
    )
    test_db_session.add(row)
    await test_db_session.commit()
    await test_db_session.refresh(row)
    return row


async def _seed_org_and_engagement(
    test_db_session,
    *,
    organization_id: int,
    engagement_id: int,
    status: str,
    engagement_code: str,
) -> int:
    test_db_session.add(
        Organization(
            organization_id=organization_id,
            name=f"All Camps Org {organization_id}",
            organization_type="corporate",
            status="active",
            departments=[{"department": "Sales", "slug": "sales"}],
        )
    )
    await test_db_session.flush()

    start = date(2026, 6, 23)
    end = date(2026, 6, 30)
    camp_no = compute_camp_no(organization_id, start)
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name=f"Eng {engagement_id}",
            organization_id=organization_id,
            camp_no=camp_no,
            engagement_code=engagement_code,
            engagement_type="bio_ai",
            assessment_package_id=None,
            diagnostic_package_id=None,
            city="BLR",
            slot_duration=20,
            start_date=start,
            end_date=end,
            status=status,
        )
    )
    await test_db_session.commit()
    return camp_no


async def _ensure_section(
    test_db_session,
    *,
    report_sections: int,
    section_key: str,
    section: str,
    description: str = "test",
) -> CampReportSection:
    existing = (
        await test_db_session.execute(
            select(CampReportSection).where(CampReportSection.section_key == section_key)
        )
    ).scalar_one_or_none()
    if existing is not None:
        existing.section = section
        existing.description = description
        await test_db_session.commit()
        return existing

    row = CampReportSection(
        report_sections=report_sections,
        section=section,
        section_key=section_key,
        description=description,
    )
    test_db_session.add(row)
    await test_db_session.commit()
    return row


@pytest.mark.asyncio
async def test_purge_orphaned_camp_reports_deletes_rows_without_engagements(test_db_session):
    orphan_camp_no = 999990001
    await _seed_report(test_db_session, camp_no=orphan_camp_no, organization_id=99001)

    service = _service()
    result = await service.purge_orphaned_camp_reports(test_db_session, dry_run=False)
    await test_db_session.commit()

    assert orphan_camp_no in result["orphan_camp_nos"]
    assert result["orphan_rows_deleted"] == 1
    remaining = (
        await test_db_session.execute(
            select(CampReport).where(CampReport.camp_no == orphan_camp_no)
        )
    ).scalar_one_or_none()
    assert remaining is None


@pytest.mark.asyncio
async def test_purge_orphaned_dry_run_does_not_delete(test_db_session):
    orphan_camp_no = 999990002
    await _seed_report(test_db_session, camp_no=orphan_camp_no, organization_id=99002)

    service = _service()
    result = await service.purge_orphaned_camp_reports(test_db_session, dry_run=True)

    assert result["orphan_row_count"] == 1
    assert result["orphan_rows_deleted"] == 0
    remaining = (
        await test_db_session.execute(
            select(CampReport).where(CampReport.camp_no == orphan_camp_no)
        )
    ).scalar_one_or_none()
    assert remaining is not None


@pytest.mark.asyncio
async def test_refresh_all_purges_orphan_rows_before_refresh(test_db_session):
    orphan_camp_no = 999990003
    await _seed_report(test_db_session, camp_no=orphan_camp_no, organization_id=99003)

    camp_no = await _seed_org_and_engagement(
        test_db_session,
        organization_id=99004,
        engagement_id=99004,
        status="completed",
        engagement_code="ALLPURGE1",
    )
    await _seed_report(test_db_session, camp_no=camp_no, organization_id=99004)
    await _ensure_section(
        test_db_session,
        report_sections=99004,
        section_key="participation_by_age",
        section="Participation by Age",
        description="Enrollment distribution across age groups",
    )

    service = _service()
    service.refresh_camp_report_section_for_cron = AsyncMock(return_value={"report_id": 1})

    result = await initialize_and_refresh_all_camp_reports(
        test_db_session,
        service=service,
        dry_run=False,
        camp_no=camp_no,
    )
    await test_db_session.commit()

    assert result["orphan_rows_deleted"] >= 1
    orphan_remaining = (
        await test_db_session.execute(
            select(CampReport).where(CampReport.camp_no == orphan_camp_no)
        )
    ).scalar_one_or_none()
    assert orphan_remaining is None
    assert result["refreshed"] >= 1


@pytest.mark.asyncio
async def test_refresh_all_continues_after_section_failure_across_rows(
    test_db_session,
    monkeypatch,
):
    camp_no = await _seed_org_and_engagement(
        test_db_session,
        organization_id=99005,
        engagement_id=99005,
        status="completed",
        engagement_code="ALLROW1",
    )
    await _seed_report(test_db_session, camp_no=camp_no, organization_id=99005)
    await _seed_report(
        test_db_session,
        camp_no=camp_no,
        organization_id=99005,
        department="sales",
    )
    await _ensure_section(
        test_db_session,
        report_sections=99005,
        section_key="participation_by_age",
        section="Participation by Age",
        description="Enrollment distribution across age groups",
    )
    await _ensure_section(
        test_db_session,
        report_sections=99006,
        section_key="kpis",
        section="KPIs",
        description="Camp enrollment and health KPI summary",
    )

    service = _service()
    original = service.refresh_camp_report_section_for_cron

    async def _flaky(db, *, camp_no, section, department=None, city=None):
        if department is None and city is None and section == "participation_by_age":
            raise RuntimeError("boom")
        return await original(
            db,
            camp_no=camp_no,
            section=section,
            department=department,
            city=city,
        )

    monkeypatch.setattr(service, "refresh_camp_report_section_for_cron", _flaky)

    result = await initialize_and_refresh_all_camp_reports(
        test_db_session,
        service=service,
        dry_run=False,
        camp_no=camp_no,
    )

    assert result["failed"] >= 1
    assert result["refreshed"] >= 1

    dept_row = (
        await test_db_session.execute(
            select(CampReport).where(
                CampReport.camp_no == camp_no,
                CampReport.department == "sales",
            )
        )
    ).scalar_one()
    assert "kpis" in (dept_row.report or {})
