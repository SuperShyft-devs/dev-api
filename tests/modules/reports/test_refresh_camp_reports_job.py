"""Tests for the refresh_camp_reports cron orchestration."""

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
from modules.reports.camp_report_section_builders import SECTION_BUILDERS
from modules.reports.camp_report_sections_repository import CampReportSectionsRepository
from modules.reports.camp_reports_repository import CampReportsRepository
from modules.reports.camp_reports_service import CampReportsService
from modules.reports.dependencies import get_reports_service
from modules.reports.models import CampReport, CampReportSection
from modules.reports.refresh_camp_reports_job import refresh_camp_reports


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
            name=f"Refresh Cron Org {organization_id}",
            organization_type="corporate",
            status="active",
            departments=[
                {"department": "Sales", "slug": "sales"},
            ],
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


@pytest.mark.asyncio
async def test_refresh_camp_reports_skips_non_running_camp(test_db_session):
    camp_no = await _seed_org_and_engagement(
        test_db_session,
        organization_id=9301,
        engagement_id=9301,
        status="completed",
        engagement_code="CRONSKIP1",
    )
    await _seed_report(test_db_session, camp_no=camp_no, organization_id=9301)
    await _ensure_section(
        test_db_session,
        report_sections=9301,
        section_key="participation_by_age",
        section="Participation by Age",
        description="Enrollment distribution across age groups",
    )

    progress: list[tuple[int, int, int, int, int]] = []
    result = await refresh_camp_reports(
        test_db_session,
        service=_service(),
        dry_run=False,
        camp_no=camp_no,
        on_progress=lambda *args: progress.append(args),
    )

    assert result["camps_total"] == 1
    assert result["camps_running"] == 0
    assert result["camps_skipped"] == 1
    assert result["refreshed"] == 0
    assert result["failed"] == 0

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.camp_no == camp_no))
    ).scalar_one()
    assert "participation_by_age" not in (row.report or {})


@pytest.mark.asyncio
async def test_refresh_camp_reports_refreshes_all_scopes_and_sections(test_db_session):
    camp_no = await _seed_org_and_engagement(
        test_db_session,
        organization_id=9302,
        engagement_id=9302,
        status="running",
        engagement_code="CRONRUN1",
    )
    await _seed_report(test_db_session, camp_no=camp_no, organization_id=9302)
    await _seed_report(
        test_db_session,
        camp_no=camp_no,
        organization_id=9302,
        department="sales",
    )
    await _seed_report(
        test_db_session,
        camp_no=camp_no,
        organization_id=9302,
        city="BLR",
    )
    await _seed_report(
        test_db_session,
        camp_no=camp_no,
        organization_id=9302,
        department="sales",
        city="BLR",
    )
    await _ensure_section(
        test_db_session,
        report_sections=9302,
        section_key="participation_by_age",
        section="Participation by Age",
        description="Enrollment distribution across age groups",
    )
    await _ensure_section(
        test_db_session,
        report_sections=9303,
        section_key="kpis",
        section="KPIs",
        description="Camp enrollment and health KPI summary",
    )

    result = await refresh_camp_reports(
        test_db_session,
        service=_service(),
        dry_run=False,
        camp_no=camp_no,
    )

    assert result["camps_running"] == 1
    assert result["camps_skipped"] == 0
    assert result["failed"] == 0
    assert result["sections"] >= 2
    assert result["refreshed"] == 4 * result["sections"]

    rows = (
        await test_db_session.execute(select(CampReport).where(CampReport.camp_no == camp_no))
    ).scalars().all()
    assert len(rows) == 4
    for row in rows:
        report = row.report or {}
        assert report["meta"]["summary_available"] is True
        assert report["meta"]["refreshed_at"] is not None
        assert "participation_by_age" in report
        assert "kpis" in report


@pytest.mark.asyncio
async def test_refresh_camp_reports_skips_unimplemented_catalog_section(test_db_session):
    camp_no = await _seed_org_and_engagement(
        test_db_session,
        organization_id=9303,
        engagement_id=9303,
        status="running",
        engagement_code="CRONUNIMP1",
    )
    await _seed_report(test_db_session, camp_no=camp_no, organization_id=9303)
    await _ensure_section(
        test_db_session,
        report_sections=9304,
        section_key="participation_by_age",
        section="Participation by Age",
        description="Enrollment distribution across age groups",
    )
    await _ensure_section(
        test_db_session,
        report_sections=9399,
        section_key="not_a_real_builder_section",
        section="Unimplemented",
        description="Should be skipped",
    )
    assert "not_a_real_builder_section" not in SECTION_BUILDERS

    result = await refresh_camp_reports(
        test_db_session,
        service=_service(),
        dry_run=False,
        camp_no=camp_no,
    )

    assert result["failed"] == 0
    assert all(d["section"] != "not_a_real_builder_section" for d in result["details"])

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.camp_no == camp_no))
    ).scalar_one()
    assert "not_a_real_builder_section" not in (row.report or {})
    assert "participation_by_age" in (row.report or {})


@pytest.mark.asyncio
async def test_refresh_camp_reports_dry_run_writes_nothing(test_db_session):
    camp_no = await _seed_org_and_engagement(
        test_db_session,
        organization_id=9304,
        engagement_id=9304,
        status="running",
        engagement_code="CRONDRY1",
    )
    await _seed_report(test_db_session, camp_no=camp_no, organization_id=9304)
    await _ensure_section(
        test_db_session,
        report_sections=9305,
        section_key="participation_by_age",
        section="Participation by Age",
        description="Enrollment distribution across age groups",
    )

    result = await refresh_camp_reports(
        test_db_session,
        service=_service(),
        dry_run=True,
        camp_no=camp_no,
    )

    assert result["dry_run"] is True
    assert result["refreshed"] >= 1
    assert result["failed"] == 0
    assert all(d["action"] == "would_refresh" for d in result["details"])

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.camp_no == camp_no))
    ).scalar_one()
    assert "participation_by_age" not in (row.report or {})
    assert (row.report or {}).get("meta", {}).get("summary_available") is False


@pytest.mark.asyncio
async def test_refresh_camp_reports_continues_after_section_failure(test_db_session, monkeypatch):
    camp_no = await _seed_org_and_engagement(
        test_db_session,
        organization_id=9305,
        engagement_id=9305,
        status="running",
        engagement_code="CRONFAIL1",
    )
    await _seed_report(test_db_session, camp_no=camp_no, organization_id=9305)
    await _ensure_section(
        test_db_session,
        report_sections=9306,
        section_key="participation_by_age",
        section="Participation by Age",
        description="Enrollment distribution across age groups",
    )
    await _ensure_section(
        test_db_session,
        report_sections=9307,
        section_key="kpis",
        section="KPIs",
        description="Camp enrollment and health KPI summary",
    )

    service = _service()
    original = service.refresh_camp_report_section_for_cron

    async def _flaky(db, *, camp_no, section, department=None, city=None):
        if section == "participation_by_age":
            raise RuntimeError("boom")
        return await original(
            db,
            camp_no=camp_no,
            section=section,
            department=department,
            city=city,
        )

    monkeypatch.setattr(service, "refresh_camp_report_section_for_cron", _flaky)

    result = await refresh_camp_reports(
        test_db_session,
        service=service,
        dry_run=False,
        camp_no=camp_no,
    )

    assert result["failed"] >= 1
    assert result["refreshed"] >= 1
    assert any(e["section"] == "participation_by_age" for e in result["errors"])

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.camp_no == camp_no))
    ).scalar_one()
    assert "kpis" in (row.report or {})
    assert "participation_by_age" not in (row.report or {})


@pytest.mark.asyncio
async def test_refresh_camp_reports_progress_callback(test_db_session):
    camp_no = await _seed_org_and_engagement(
        test_db_session,
        organization_id=9306,
        engagement_id=9306,
        status="running",
        engagement_code="CRONPROG1",
    )
    await _seed_report(test_db_session, camp_no=camp_no, organization_id=9306)
    await _ensure_section(
        test_db_session,
        report_sections=9308,
        section_key="participation_by_age",
        section="Participation by Age",
        description="Enrollment distribution across age groups",
    )

    # Limit to one implemented section by stubbing SECTION_BUILDERS intersection via mock service
    service = _service()
    service.refresh_camp_report_section_for_cron = AsyncMock(
        return_value={"report_id": 1, "section": {}}
    )

    progress: list[tuple[int, int, int, int, int]] = []
    result = await refresh_camp_reports(
        test_db_session,
        service=service,
        dry_run=False,
        camp_no=camp_no,
        on_progress=lambda *args: progress.append(args),
    )

    assert progress
    assert progress[0][0] == 0  # done starts at 0
    assert progress[-1][0] == progress[-1][1]  # done == total at end
    assert result["refreshed"] == progress[-1][2]


@pytest.mark.asyncio
async def test_refresh_camp_reports_emits_detailed_events(test_db_session):
    camp_no = await _seed_org_and_engagement(
        test_db_session,
        organization_id=9307,
        engagement_id=9307,
        status="running",
        engagement_code="CRONEVT1",
    )
    await _seed_report(test_db_session, camp_no=camp_no, organization_id=9307)
    await _ensure_section(
        test_db_session,
        report_sections=9309,
        section_key="participation_by_age",
        section="Participation by Age",
        description="Enrollment distribution across age groups",
    )

    service = _service()
    service.refresh_camp_report_section_for_cron = AsyncMock(
        return_value={"report_id": 1, "section": {}}
    )
    events: list[dict] = []
    await refresh_camp_reports(
        test_db_session,
        service=service,
        dry_run=False,
        camp_no=camp_no,
        on_event=events.append,
    )

    kinds = [e["event"] for e in events]
    assert "plan" in kinds
    assert "start" in kinds
    assert "finish" in kinds
    plan = next(e for e in events if e["event"] == "plan")
    assert plan["camps_running"] == 1
    assert "participation_by_age" in plan["section_keys"]
    start = next(e for e in events if e["event"] == "start")
    assert start["camp_no"] == camp_no
    assert start["scope"] == "overall"
    assert start["section"] == "participation_by_age" or start["section"] in SECTION_BUILDERS
    finish = next(e for e in events if e["event"] == "finish")
    assert finish["action"] == "refreshed"
