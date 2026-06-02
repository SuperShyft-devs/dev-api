"""Dispatch questionnaire reminder notifications.

Finds participants in running engagements whose engagement_date is tomorrow
or yesterday (IST), checks whether their questionnaire is complete on both
Metsights and the internal DB, and dispatches reminder notifications for
those who have not completed.

Production example (Linux cron, IST):

    # 11:30am IST daily
    30 11 * * * cd /path/to/dev-api && TZ=Asia/Kolkata /path/to/venv/bin/python -m db.jobs.dispatch_questionnaire_reminders --yes >> /var/log/supershyft/questionnaire-reminders.log 2>&1

Entrypoint: ``python -m db.jobs.dispatch_questionnaire_reminders --yes``
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import date

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from core.config import settings
from modules.assessments.dependencies import get_assessment_package_categories_service
from modules.engagements.repository import EngagementsRepository
from modules.metsights.client import MetsightsClient
from modules.metsights.service import MetsightsService
from modules.notifications.questionnaire_reminders import dispatch_questionnaire_reminders
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService


async def run_dispatch(
    *,
    yes: bool,
    dry_run: bool,
    as_of: date | None,
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

    notifications_service = NotificationsService(NotificationsRepository())
    engagements_repository = EngagementsRepository()
    metsights_service = MetsightsService(client=MetsightsClient())
    categories_service = get_assessment_package_categories_service()

    async with session_factory() as session:
        async with session.begin():
            result = await dispatch_questionnaire_reminders(
                session,
                notifications_service=notifications_service,
                engagements_repository=engagements_repository,
                metsights_service=metsights_service,
                categories_service=categories_service,
                as_of=as_of,
                dry_run=dry_run,
            )

    await engine.dispose()
    return result


def _parse_as_of(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Expected YYYY-MM-DD.") from exc


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Dispatch questionnaire reminder notifications for participants with "
            "engagement_date tomorrow or yesterday (IST) in running engagements."
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
        help="Report how many participants would be notified without dispatching.",
    )
    parser.add_argument(
        "--as-of",
        type=_parse_as_of,
        default=None,
        metavar="YYYY-MM-DD",
        help="Reference date for tomorrow/yesterday calculation (default: today in IST). Useful for testing.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = asyncio.run(
        run_dispatch(
            yes=args.yes,
            dry_run=args.dry_run,
            as_of=args.as_of,
        )
    )
    mode = "dry-run" if result["dry_run"] else "applied"
    print(
        f"\nQuestionnaire reminders ({mode}):\n"
        f"  as_of={result['as_of']}, tomorrow={result['tomorrow']}, yesterday={result['yesterday']}\n"
        f"  matched={result['matched']}, sent={result['sent']}, skipped={result['skipped']}, failed={result['failed']}"
    )
    details = result.get("details", [])
    if details:
        print(f"\n  {'USER':>8}  {'ENG':>6}  {'DATE':>12}  {'TYPE':>12}  {'SERVICE_KEY':>30}  {'ACTION':>8}  REASON")
        print(f"  {'─' * 8}  {'─' * 6}  {'─' * 12}  {'─' * 12}  {'─' * 30}  {'─' * 8}  {'─' * 40}")
        for d in details:
            print(
                f"  {d['user_id']:>8}  {d['engagement_id']:>6}  {d['engagement_date']:>12}  "
                f"{d.get('reminder_type', ''):>12}  {(d.get('service_key') or '—'):>30}  "
                f"{d['action']:>8}  {d['reason']}"
            )
    print()
    return 0
