"""Refresh camp report sections for camps with a running engagement.

Rebuilds every implemented section on every camp_reports row (overall,
department, city, city×department) when at least one engagement for that
camp_no has status=running.

Entrypoint: ``python -m db.jobs.refresh_camp_reports --yes``

Production example (Linux cron, every 6 hours):

    0 */6 * * * cd /path/to/dev-api && /path/to/venv/bin/python -m db.jobs.refresh_camp_reports --yes >> /var/log/supershyft/refresh-camp-reports.log 2>&1
"""

from __future__ import annotations

import argparse
import asyncio
import sys

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
from modules.reports.refresh_camp_reports_job import refresh_camp_reports

_PROGRESS_BAR_WIDTH = 30


def _format_progress(
    done: int,
    total: int,
    refreshed: int,
    skipped: int,
    failed: int,
) -> str:
    pct = 100 if total == 0 else int(100 * done / total)
    filled = _PROGRESS_BAR_WIDTH if total == 0 else int(_PROGRESS_BAR_WIDTH * done / total)
    bar = "█" * filled + "░" * (_PROGRESS_BAR_WIDTH - filled)
    return (
        f"[{bar}] {done}/{total} ({pct}%)  "
        f"refreshed={refreshed} skipped={skipped} failed={failed}"
    )


def _make_progress_printer():
    """Return a progress callback that redraws in-place on a TTY, or logs lines otherwise."""
    is_tty = sys.stdout.isatty()
    last_pct = -1

    def on_progress(
        done: int,
        total: int,
        refreshed: int,
        skipped: int,
        failed: int,
    ) -> None:
        nonlocal last_pct
        line = _format_progress(done, total, refreshed, skipped, failed)
        if is_tty:
            print(f"\r{line}", end="", flush=True)
            if done >= total:
                print(flush=True)
            return

        pct = 100 if total == 0 else int(100 * done / total)
        if done == 0 or done >= total or pct != last_pct:
            print(line, flush=True)
            last_pct = pct

    return on_progress


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


async def run_refresh(
    *,
    yes: bool,
    dry_run: bool,
    camp_no: int | None,
) -> dict:
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
    on_progress = _make_progress_printer()
    print("Refreshing camp report sections...", flush=True)

    async with session_factory() as session:
        result = await refresh_camp_reports(
            session,
            service=service,
            dry_run=dry_run,
            camp_no=camp_no,
            on_progress=on_progress,
        )

    await engine.dispose()
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Refresh all implemented camp report sections for camps that have at least "
            "one running engagement. Safe to re-run (idempotent rebuild)."
        )
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Apply changes. Without this flag (and without --dry-run), the command exits without writing.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report which camps/sections would refresh without writing.",
    )
    parser.add_argument(
        "--camp-no",
        type=int,
        default=None,
        metavar="CAMP_NO",
        help="Limit to a single camp_no (useful for testing).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = asyncio.run(
        run_refresh(
            yes=args.yes,
            dry_run=args.dry_run,
            camp_no=args.camp_no,
        )
    )
    mode = "dry-run" if result["dry_run"] else "applied"
    print(
        f"\nRefresh camp reports ({mode}):\n"
        f"  camps_total={result['camps_total']} camps_running={result['camps_running']} "
        f"camps_skipped={result['camps_skipped']}\n"
        f"  sections={result['sections']} refreshed={result['refreshed']} "
        f"failed={result['failed']}"
    )
    errors = result.get("errors") or []
    if errors:
        print(f"\n  {'CAMP':>12}  {'SCOPE':<28}  {'SECTION':<40}  REASON")
        print(f"  {'─' * 12}  {'─' * 28}  {'─' * 40}  {'─' * 40}")
        for err in errors:
            print(
                f"  {err['camp_no']:>12}  {err['scope']:<28}  "
                f"{err['section']:<40}  {err['reason']}"
            )
    print()
    return 0 if result.get("failed", 0) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
