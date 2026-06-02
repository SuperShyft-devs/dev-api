"""Load blood reports from Healthians for eligible participants.

Fetches blood parameters and diagnostic report URLs from Healthians for
participants in running engagements where today >= engagement_date.
Sends notifications via engagement.blood_report_notification.

Entrypoint: ``python -m db.jobs.load_blood_reports --yes``
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import date

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from core.config import settings
from modules.metsights.client import MetsightsClient
from modules.metsights.service import MetsightsService
from modules.notifications.load_blood_reports import load_blood_reports
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService


async def run_load(
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

    metsights_service = MetsightsService(client=MetsightsClient())
    notifications_service = NotificationsService(NotificationsRepository())

    async with session_factory() as session:
        async with session.begin():
            result = await load_blood_reports(
                session,
                metsights_service=metsights_service,
                notifications_service=notifications_service,
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
        description="Load blood reports from Healthians and send notifications."
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Apply changes. Without this flag (and without --dry-run), the command exits without writing.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be loaded without making changes.",
    )
    parser.add_argument(
        "--as-of",
        type=_parse_as_of,
        default=None,
        metavar="YYYY-MM-DD",
        help="Override 'today' date. Useful for testing.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = asyncio.run(
        run_load(
            yes=args.yes,
            dry_run=args.dry_run,
            as_of=args.as_of,
        )
    )
    mode = "dry-run" if result["dry_run"] else "applied"
    print(
        f"\nLoad blood reports ({mode}):\n"
        f"  as_of={result['as_of']}\n"
        f"  matched={result['matched']}, loaded={result['loaded']}, "
        f"notified={result['notified']}, skipped={result['skipped']}, failed={result['failed']}"
    )
    details = result.get("details", [])
    if details:
        print(f"\n  {'USER':>8}  {'ENG':>6}  {'ACTION':>10}  REASON")
        print(f"  {'─' * 8}  {'─' * 6}  {'─' * 10}  {'─' * 40}")
        for d in details:
            print(
                f"  {d['user_id']:>8}  {d['engagement_id']:>6}  "
                f"{d['action']:>10}  {d['reason']}"
            )
    print()
    return 0
