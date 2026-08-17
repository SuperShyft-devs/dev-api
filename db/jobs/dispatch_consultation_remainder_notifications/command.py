"""Dispatch consultation remainder notifications.

Finds participants in scheduled/running engagements with a consultation scheduled
for today (IST) and dispatches using each engagement's consultation_remainder
notification service keys. Intended to run once daily.

Production example (Linux cron, IST):

    # 8:00 AM IST — morning of consultation day
    0 8 * * * cd /path/to/dev-api && TZ=Asia/Kolkata /path/to/venv/bin/python -m db.jobs.dispatch_consultation_remainder_notifications --yes >> /var/log/supershyft/consultation-remainder-notifications.log 2>&1

Entrypoint: ``python -m db.jobs.dispatch_consultation_remainder_notifications --yes``
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import date

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from core.config import settings
from modules.engagements.repository import EngagementsRepository
from modules.notifications.consultation_remainder_notifications import (
    dispatch_consultation_remainder_notifications,
)
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService


async def run_dispatch_consultation_remainder_notifications(
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

    async with session_factory() as session:
        async with session.begin():
            result = await dispatch_consultation_remainder_notifications(
                session,
                notifications_service=notifications_service,
                engagements_repository=engagements_repository,
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
            "Dispatch consultation remainder notifications for participants with a "
            "consultation scheduled today (IST), using each engagement's "
            "consultation_remainder notification service keys."
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
        help="Reference date for today calculation (default: today in IST). Useful for testing.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = asyncio.run(
        run_dispatch_consultation_remainder_notifications(
            yes=args.yes,
            dry_run=args.dry_run,
            as_of=args.as_of,
        )
    )
    mode = "dry-run" if result["dry_run"] else "applied"
    print(
        f"Dispatch consultation remainder notifications ({mode}): "
        f"as_of={result['as_of']}, consultation_date={result['consultation_date']}, "
        f"matched={result['matched']}, sent={result['sent']}, "
        f"skipped={result['skipped']}, failed={result['failed']}"
    )
    return 0
