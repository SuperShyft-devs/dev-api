"""Dispatch pretest blood-collection reminder notifications.

Finds participants in running engagements whose collection date is tomorrow (IST),
filtered by morning vs afternoon slot window, and dispatches pretest WhatsApp and
email notifications. Intended to run twice daily via an external scheduler.

Production example (Linux cron, IST):

    # 6pm IST — early slots (collection tomorrow, slot <= 09:00)
    0 18 * * * cd /path/to/dev-api && TZ=Asia/Kolkata /path/to/venv/bin/python -m db.jobs.dispatch_pretest_reminders --window early --yes >> /var/log/supershyft/pretest-reminders-early.log 2>&1

    # 9pm IST — late slots (collection tomorrow, slot > 09:00)
    0 21 * * * cd /path/to/dev-api && TZ=Asia/Kolkata /path/to/venv/bin/python -m db.jobs.dispatch_pretest_reminders --window late --yes >> /var/log/supershyft/pretest-reminders-late.log 2>&1

Entrypoint: ``python -m db.jobs.dispatch_pretest_reminders --window early|late --yes``
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import date

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from core.config import settings
from modules.engagements.repository import EngagementsRepository
from modules.notifications.pretest_reminders import PretestWindow, dispatch_pretest_reminders
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService


async def run_dispatch_pretest_reminders(
    *,
    yes: bool,
    dry_run: bool,
    window: PretestWindow,
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

    async with session_factory() as session:
        async with session.begin():
            result = await dispatch_pretest_reminders(
                session,
                notifications_service=notifications_service,
                notifications_repository=NotificationsRepository(),
                engagements_repository=engagements_repository,
                window=window,
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
            "Dispatch pretest WhatsApp and email reminders for participants with blood collection "
            "tomorrow (IST) in running engagements."
        )
    )
    parser.add_argument(
        "--window",
        required=True,
        choices=("early", "late"),
        help=(
            "early: collection tomorrow with slot <= 09:00 (6pm cron). "
            "late: collection tomorrow with slot > 09:00 (9pm cron)."
        ),
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
        help="Reference date for tomorrow calculation (default: today in IST). Useful for testing.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = asyncio.run(
        run_dispatch_pretest_reminders(
            yes=args.yes,
            dry_run=args.dry_run,
            window=args.window,
            as_of=args.as_of,
        )
    )
    mode = "dry-run" if result["dry_run"] else "applied"
    print(
        f"Dispatch pretest reminders ({mode}): window={result['window']}, "
        f"as_of={result['as_of']}, collection_date={result['collection_date']}, "
        f"matched={result['matched']}, whatsapp_sent={result['whatsapp_sent']}, "
        f"email_sent={result['email_sent']}, failed={result['failed']}"
    )
    return 0
