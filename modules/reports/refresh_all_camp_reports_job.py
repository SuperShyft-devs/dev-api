"""Initialize and refresh camp report sections for every available camp.

Intended for CLI backfill: ``python -m db.jobs.all_camps.refresh_camp_reports --yes``.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Callable
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from modules.reports.camp_report_section_builders import SECTION_BUILDERS
from modules.reports.camp_report_sections_repository import CampReportSectionsRepository
from modules.reports.camp_reports_repository import CampReportsRepository
from modules.reports.camp_reports_service import CampReportsService
from modules.reports.models import CampReport
from modules.reports.refresh_camp_reports_job import _error_reason, _scope_label

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[int, int, int, int, int], None]
EventCallback = Callable[[dict[str, Any]], None]


async def initialize_and_refresh_all_camp_reports(
    db: AsyncSession,
    *,
    service: CampReportsService,
    repository: CampReportsRepository | None = None,
    sections_repository: CampReportSectionsRepository | None = None,
    dry_run: bool = False,
    camp_no: int | None = None,
    on_progress: ProgressCallback | None = None,
    on_event: EventCallback | None = None,
) -> dict[str, Any]:
    """Initialize missing camp report rows, then refresh every implemented section.

    Unlike the running-camp cron job, this processes every camp referenced by at
    least one engagement, regardless of engagement status.
    """
    reports_repo = repository or CampReportsRepository()
    sections_repo = sections_repository or CampReportSectionsRepository()

    available_camp_nos = await reports_repo.list_distinct_camp_nos(db)
    if camp_no is not None:
        available_camp_nos = [camp for camp in available_camp_nos if camp == camp_no]

    camps_total = len(available_camp_nos)
    camps_initialized = 0
    camps_init_failed = 0
    init_errors: list[dict[str, Any]] = []

    if on_event is not None:
        on_event(
            {
                "event": "init_plan",
                "dry_run": dry_run,
                "camps_total": camps_total,
                "camp_nos": list(available_camp_nos),
            }
        )

    for camp in available_camp_nos:
        if on_event is not None:
            on_event(
                {
                    "event": "init_start",
                    "camp_no": camp,
                    "dry_run": dry_run,
                }
            )

        if dry_run:
            camps_initialized += 1
            if on_event is not None:
                on_event(
                    {
                        "event": "init_finish",
                        "camp_no": camp,
                        "action": "would_initialize",
                        "reason": "",
                    }
                )
            continue

        try:
            await service.init_all_camp_reports_for_cron(db, camp_no=camp)
            await db.commit()
            camps_initialized += 1
            if on_event is not None:
                on_event(
                    {
                        "event": "init_finish",
                        "camp_no": camp,
                        "action": "initialized",
                        "reason": "",
                    }
                )
        except Exception as exc:
            await db.rollback()
            camps_init_failed += 1
            reason = _error_reason(exc)
            logger.exception(
                "Failed initializing camp reports for camp_no=%s: %s",
                camp,
                reason,
            )
            init_errors.append({"camp_no": camp, "reason": reason})
            if on_event is not None:
                on_event(
                    {
                        "event": "init_finish",
                        "camp_no": camp,
                        "action": "failed",
                        "reason": reason,
                    }
                )

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

    eligible = sorted(by_camp.items(), key=lambda item: item[0])
    camps_with_reports = len(eligible)
    camps_without_reports = max(camps_total - camps_with_reports, 0)

    total = sum(len(rows) * len(section_keys) for _, rows in eligible)
    done = 0
    refreshed = 0
    skipped = 0
    failed = 0
    errors: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []

    def _emit_progress() -> None:
        if on_progress is not None:
            on_progress(done, total, refreshed, skipped, failed)

    if on_event is not None:
        on_event(
            {
                "event": "plan",
                "dry_run": dry_run,
                "camps_total": camps_total,
                "camps_initialized": camps_initialized,
                "camps_init_failed": camps_init_failed,
                "camps_with_reports": camps_with_reports,
                "camps_without_reports": camps_without_reports,
                "sections": len(section_keys),
                "section_keys": list(section_keys),
                "work_items": total,
                "eligible_camps": [
                    {
                        "camp_no": camp,
                        "report_rows": len(rows),
                        "scopes": [
                            _scope_label(department=r.department, city=r.city) for r in rows
                        ],
                    }
                    for camp, rows in eligible
                ],
            }
        )

    _emit_progress()

    if not section_keys:
        return {
            "dry_run": dry_run,
            "camps_total": camps_total,
            "camps_initialized": camps_initialized,
            "camps_init_failed": camps_init_failed,
            "camps_with_reports": camps_with_reports,
            "camps_without_reports": camps_without_reports,
            "sections": 0,
            "section_keys": [],
            "refreshed": 0,
            "skipped": 0,
            "failed": 0,
            "init_errors": init_errors,
            "errors": [],
            "details": [],
        }

    for camp, rows in eligible:
        for row in rows:
            report_id = int(row.report_id)
            department = row.department
            city = row.city
            scope = _scope_label(department=department, city=city)
            for section_key in section_keys:
                step_index = done + 1
                if on_event is not None:
                    on_event(
                        {
                            "event": "start",
                            "index": step_index,
                            "total": total,
                            "camp_no": camp,
                            "report_id": report_id,
                            "scope": scope,
                            "section": section_key,
                            "dry_run": dry_run,
                        }
                    )

                if dry_run:
                    refreshed += 1
                    done += 1
                    detail = {
                        "camp_no": camp,
                        "report_id": report_id,
                        "scope": scope,
                        "section": section_key,
                        "action": "would_refresh",
                        "reason": "",
                    }
                    details.append(detail)
                    if on_event is not None:
                        on_event(
                            {
                                "event": "finish",
                                "index": step_index,
                                "total": total,
                                **detail,
                            }
                        )
                    _emit_progress()
                    continue

                try:
                    await service.refresh_camp_report_section_for_cron(
                        db,
                        camp_no=camp,
                        section=section_key,
                        department=department,
                        city=city,
                    )
                    await db.commit()
                    refreshed += 1
                    detail = {
                        "camp_no": camp,
                        "report_id": report_id,
                        "scope": scope,
                        "section": section_key,
                        "action": "refreshed",
                        "reason": "",
                    }
                    details.append(detail)
                    if on_event is not None:
                        on_event(
                            {
                                "event": "finish",
                                "index": step_index,
                                "total": total,
                                **detail,
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
                        "report_id": report_id,
                        "scope": scope,
                        "section": section_key,
                        "reason": reason,
                    }
                    errors.append(error_entry)
                    detail = {
                        **error_entry,
                        "action": "failed",
                    }
                    details.append(detail)
                    if on_event is not None:
                        on_event(
                            {
                                "event": "finish",
                                "index": step_index,
                                "total": total,
                                **detail,
                            }
                        )
                done += 1
                _emit_progress()

    return {
        "dry_run": dry_run,
        "camps_total": camps_total,
        "camps_initialized": camps_initialized,
        "camps_init_failed": camps_init_failed,
        "camps_with_reports": camps_with_reports,
        "camps_without_reports": camps_without_reports,
        "sections": len(section_keys),
        "section_keys": list(section_keys),
        "refreshed": refreshed,
        "skipped": skipped,
        "failed": failed,
        "init_errors": init_errors,
        "errors": errors,
        "details": details,
    }
