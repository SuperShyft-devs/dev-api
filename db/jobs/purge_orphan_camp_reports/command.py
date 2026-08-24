"""Delete camp_reports rows whose camp_no has no engagements.

Entrypoint: ``python -m db.jobs.purge_orphan_camp_reports --yes``
"""

from __future__ import annotations

import argparse
import asyncio

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from core.config import settings
from modules.assessments.repository import AssessmentsRepository
from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.diagnostics.repository import DiagnosticsRepository
from modules.organizations.repository import OrganizationsRepository
from modules.reports.camp_report_sections_repository import CampReportSectionsRepository
from modules.reports.camp_reports_repository import CampReportsRepository
from modules.reports.camp_reports_service import CampReportsService
from modules.reports.dependencies import get_reports_service


def _build_camp_reports_service() -> CampReportsService:
    return CampReportsService(
        repository=CampReportsRepository(),
        sections_repository=CampReportSectionsRepository(),
        organizations_repository=OrganizationsRepository(),
        audit_service=AuditService(AuditRepository()),
        reports_service=get_reports_service(),
        assessments_repository=AssessmentsRepository(),
        diagnostics_repository=DiagnosticsRepository(),
    )


async def run_purge(*, yes: bool, dry_run: bool) -> dict:
    settings.validate()

    if not yes and not dry_run:
        raise SystemExit(
            "Refusing to run without explicit confirmation. Re-run with --yes to apply changes, "
            "or --dry-run to preview."
        )

    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    service = _build_camp_reports_service()

    async with session_factory() as session:
        result = await service.purge_orphaned_camp_reports(session, dry_run=dry_run)
        if not dry_run and result.get("orphan_rows_deleted", 0) > 0:
            await session.commit()

    await engine.dispose()
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Delete camp_reports rows whose camp_no has no matching engagements."
        )
    )
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = asyncio.run(run_purge(yes=args.yes, dry_run=args.dry_run))
    mode = "dry-run" if result.get("dry_run") else "applied"
    orphan_camp_nos = result.get("orphan_camp_nos") or []
    print(f"\nPurge orphan camp reports ({mode}):")
    print(f"  orphan_camps={len(orphan_camp_nos)} orphan_row_count={result.get('orphan_row_count', 0)}")
    if orphan_camp_nos:
        print(f"  camp_nos: {', '.join(str(c) for c in orphan_camp_nos)}")
    if result.get("dry_run"):
        if result.get("orphan_row_count", 0) > 0:
            print(f"  would delete {result['orphan_row_count']} row(s)")
        else:
            print("  nothing to delete")
    else:
        print(f"  deleted={result.get('orphan_rows_deleted', 0)}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
