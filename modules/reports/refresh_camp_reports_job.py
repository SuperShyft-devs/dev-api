"""Refresh all camp report sections for camps with a running engagement.

Intended for CLI cron: ``python -m db.jobs.refresh_camp_reports --yes``.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Callable
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.reports.camp_report_section_builders import SECTION_BUILDERS
from modules.reports.camp_report_sections_repository import CampReportSectionsRepository
from modules.reports.camp_reports_repository import CampReportsRepository
from modules.reports.camp_reports_service import CampReportsService
from modules.reports.models import CampReport

logger = logging.getLogger(__name__)

# on_progress(done, total, refreshed, skipped, failed)
ProgressCallback = Callable[[int, int, int, int, int], None]


def _scope_label(*, department: str | None, city: str | None) -> str:
    if city is None and department is None:
        return "overall"
    if city is None and department is not None:
        return f"department={department}"
    if city is not None and department is None:
        return f"city={city}"
    return f"city={city}/department={department}"


def _error_reason(exc: BaseException) -> str:
    if isinstance(exc, AppError):
        return f"{exc.error_code}: {exc.message}"
    return str(exc) or exc.__class__.__name__


async def refresh_camp_reports(
    db: AsyncSession,
    *,
    service: CampReportsService,
    repository: CampReportsRepository | None = None,
    sections_repository: CampReportSectionsRepository | None = None,
    dry_run: bool = False,
    camp_no: int | None = None,
    on_progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Refresh every implemented section for every camp_reports row on running camps.

    Progress ``total`` is the number of ``(report_row × section)`` attempts among
    camps that have at least one running engagement. Skipped camps are counted in
    the summary only.
    """
    reports_repo = repository or CampReportsRepository()
    sections_repo = sections_repository or CampReportSectionsRepository()

    all_rows = await reports_repo.list_all(db)
    if camp_no is not None:
        all_rows = [row for row in all_rows if row.camp_no == camp_no]

    section_rows = await sections_repo.list_all(db)
    section_keys = [
        row.section_key
        for row in section_rows
        if row.section_key in SECTION_BUILDERS
    ]

    by_camp: dict[int, list[CampReport]] = defaultdict(list)
    for row in all_rows:
        by_camp[int(row.camp_no)].append(row)

    camps_total = len(by_camp)
    camps_running = 0
    camps_skipped = 0

    eligible: list[tuple[int, list[CampReport]]] = []
    for camp, rows in sorted(by_camp.items()):
        if await reports_repo.has_running_engagement(db, camp_no=camp):
            camps_running += 1
            eligible.append((camp, rows))
        else:
            camps_skipped += 1

    total = sum(len(rows) * len(section_keys) for _, rows in eligible)
    done = 0
    refreshed = 0
    skipped = 0
    failed = 0
    errors: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []

    def _emit() -> None:
        if on_progress is not None:
            on_progress(done, total, refreshed, skipped, failed)

    _emit()

    if not section_keys:
        return {
            "dry_run": dry_run,
            "camps_total": camps_total,
            "camps_running": camps_running,
            "camps_skipped": camps_skipped,
            "sections": 0,
            "refreshed": 0,
            "skipped": 0,
            "failed": 0,
            "errors": [],
            "details": [],
        }

    for camp, rows in eligible:
        for row in rows:
            scope = _scope_label(department=row.department, city=row.city)
            for section_key in section_keys:
                if dry_run:
                    refreshed += 1
                    done += 1
                    details.append(
                        {
                            "camp_no": camp,
                            "report_id": row.report_id,
                            "scope": scope,
                            "section": section_key,
                            "action": "would_refresh",
                            "reason": "",
                        }
                    )
                    _emit()
                    continue

                try:
                    await service.refresh_camp_report_section_for_cron(
                        db,
                        camp_no=camp,
                        section=section_key,
                        department=row.department,
                        city=row.city,
                    )
                    await db.commit()
                    refreshed += 1
                    details.append(
                        {
                            "camp_no": camp,
                            "report_id": row.report_id,
                            "scope": scope,
                            "section": section_key,
                            "action": "refreshed",
                            "reason": "",
                        }
                    )
                except Exception as exc:
                    await db.rollback()
                    failed += 1
                    reason = _error_reason(exc)
                    logger.exception(
                        "Failed refreshing camp_no=%s %s section=%s: %s",
                        camp,
                        scope,
                        section_key,
                        reason,
                    )
                    error_entry = {
                        "camp_no": camp,
                        "report_id": row.report_id,
                        "scope": scope,
                        "section": section_key,
                        "reason": reason,
                    }
                    errors.append(error_entry)
                    details.append(
                        {
                            **error_entry,
                            "action": "failed",
                        }
                    )
                done += 1
                _emit()

    return {
        "dry_run": dry_run,
        "camps_total": camps_total,
        "camps_running": camps_running,
        "camps_skipped": camps_skipped,
        "sections": len(section_keys),
        "refreshed": refreshed,
        "skipped": skipped,
        "failed": failed,
        "errors": errors,
        "details": details,
    }
